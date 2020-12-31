package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/szaydel/ratelimit"
)

const (
	DefaultReportInterval             = 10 * time.Second
	DefaultLogIntervalReports         = true
	DefaultAddress                    = ""
	DefaultPort                       = 5002
	DefaultPprofPort                  = 6060
	DefaultMetricsPort                = 9094
	MegaByte                  int64   = 1024 * 1024
	MegaByteF                 float64 = 1024 * 1024
)

type Configuration struct {
	reportInterval     time.Duration
	logIntervalReports bool
	rateLimitMBps      float64
	maxBurstCapMB      int64
	address            string
	port               uint
	pprofPort          uint
	metricsPort        uint
}

func makeAddrString(addr string, port uint) string {
	if len(addr) == 0 {
		return fmt.Sprintf(":%d", port)
	}
	return fmt.Sprintf("%s:%d", addr, port)
}

func (c *Configuration) sinkAddrString() string {
	return makeAddrString(c.address, c.port)
}

func (c *Configuration) metricsAddrString() string {
	return makeAddrString(c.address, c.metricsPort)
}

func (c *Configuration) pprofAddrString() string {
	return makeAddrString(c.address, c.pprofPort)
}

func (c *Configuration) rateLimit() float64 {
	return c.rateLimitMBps * MegaByteF
}

func (c *Configuration) maxBurstCap() int64 {
	return c.maxBurstCapMB * MegaByte
}

func (c *Configuration) isRateLimited() bool {
	return c.rateLimitMBps > 0 && c.maxBurstCapMB > 0
}

// Configuration parameters passed from the CLI via flags
var config Configuration

// Global counter channel to track lines across all connections
var linecounter = make(chan WriterStats)
var pCounterRecords = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "received_records_total",
	Help: "The total number of records received and subsequently discarded",
})
var pCounterBytes = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "received_bytes_total",
	Help: "The total number of bytes received and subsequently discarded",
})

var pCounterActiveSecs = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "active_seconds_total",
	Help: "The total number of seconds spent in reading and counting records",
})

func init() {
	flag.BoolVar(&config.logIntervalReports, "log.interval.reports", DefaultLogIntervalReports, "Should interval reports be generated")
	flag.DurationVar(&config.reportInterval, "report.interval", DefaultReportInterval, "Interval between reporting periods")
	flag.Float64Var(&config.rateLimitMBps, "rate.limit", 0, "Rate-limiting to this many MB/s, zero means unlimited")
	flag.Int64Var(&config.maxBurstCapMB, "max.burst.capacity", 0, "Peak rate-limited capacity in MB allowed, zero means unlimited")
	flag.StringVar(&config.address, "address", DefaultAddress, "Listen address")
	flag.UintVar(&config.port, "port", DefaultPort, "Port used for incoming data")
	flag.UintVar(&config.metricsPort, "metrics.port", DefaultMetricsPort, "Port used to query metrics")
	flag.UintVar(&config.pprofPort, "pprof.port", DefaultPprofPort, "Port used to obtain profiling data")

	flag.Parse()

	// If maximum burst capacity was not set, but the rate limit was set, adjust
	// capacity to be equal to rate limit. This means that the bucket will at
	// most hold tokens equal to number of bytes in the chosen rate.
	if config.maxBurstCap() < int64(config.rateLimit()) {
		fmt.Fprint(os.Stderr,
			"Setting maximum burst capacity equal to rate limit\n")
		config.maxBurstCapMB = int64(config.rateLimitMBps)
	}
}

func checkErr(err error) bool {
	if err == nil {
		log.Print("Ok")
		return true
	} else if netError, ok := err.(net.Error); ok && netError.Timeout() {
		log.Print("Timeout")
		return false
	} else if err == io.ErrShortWrite {
		log.Print("Short")
		return true
	}

	switch t := err.(type) {
	case *net.OpError:
		if t.Op == "dial" {
			log.Print("Unknown host")
		} else if t.Op == "read" {
			log.Print("Connection reset by peer")
		}
		return false

	case syscall.Errno:
		if t == syscall.ECONNREFUSED {
			log.Print("Connection refused")
		}
		return false

	default:
		log.Printf("Unhandled error case: %v type: %T", err, t)
		return false
	}
}

// WriterStats tracks the number of lines and bytes "written" during a single
// invocation of the Write method on MockWriter.
type WriterStats struct {
	nbytes   int
	nlines   int
	duration time.Duration
}

// MockWriter is an implemention of writer lacking a persistence layer.
type MockWriter struct {
	statsChan chan WriterStats
}

// Write implements the Writer interface, but does not actually persist data,
// instead just counting lines and bytes.
func (m *MockWriter) Write(p []byte) (n int, err error) {
	var start time.Time
	var stats WriterStats
	start = time.Now()
	for _, v := range p {
		stats.nbytes++ // count bytes
		if v == '\n' {
			stats.nlines++ // count newlines (records)
		}
	}

	stats.duration = time.Now().Sub(start)
	m.statsChan <- stats
	return stats.nbytes, nil
}

type Counter struct {
	prev           int // cur - prev ==> records / interval
	prevBytes      int // curBytes - prevBytes ==> bytes / interval
	cur            int
	curBytes       int
	curActiveSecs  float64
	prevActiveSecs float64
	t              time.Time
	pRecords       prometheus.Counter
	pBytes         prometheus.Counter
	pActiveSecs    prometheus.Counter
}

func (c *Counter) IncrBy(nlines, nbytes int, active float64) {
	c.cur += nlines
	c.curBytes += nbytes
	c.curActiveSecs += active
	c.pRecords.Add(float64(nlines))
	c.pBytes.Add(float64(nbytes))
	c.pActiveSecs.Add(active)
}

func (c *Counter) UpdatePrev() {
	c.prev = c.cur
	c.prevBytes = c.curBytes
	c.prevActiveSecs = c.curActiveSecs
	c.t = time.Now()
}

func aggregator(
	config Configuration,
	c chan WriterStats,
	shutdown chan struct{}) {

	// Every config.reportInterval produce a summary with totals and rates
	reportTicker := time.NewTicker(config.reportInterval)

	// This structure is persistent, only one instance exists and it is mutated
	// each time through the loop. The *pCounter* variables are globals.
	var counter = &Counter{
		t:           time.Now(),
		pRecords:    pCounterRecords,
		pBytes:      pCounterBytes,
		pActiveSecs: pCounterActiveSecs,
	}
	for {
		select {
		case v := <-c:
			counter.IncrBy(v.nlines, v.nbytes, v.duration.Seconds())
		case <-reportTicker.C:
			prev := counter.prev
			prevBytes := counter.prevBytes
			cur := counter.cur
			curBytes := counter.curBytes
			prevActiveSecs := counter.prevActiveSecs
			curActiveSecs := counter.curActiveSecs
			delta := time.Now().Sub(counter.t)
			rateLines := float64(cur-prev) / delta.Seconds()
			rateBytes := float64(curBytes-prevBytes) / delta.Seconds()
			rateActiveTime := (curActiveSecs - prevActiveSecs) / delta.Seconds()
			counter.UpdatePrev()
			if config.logIntervalReports {
				log.Printf(
					"lines/s: %f\tbytes/s: %f\ttotal\tlines: %d\tbytes: %d\tactive secs/s: %.9f",
					rateLines,
					rateBytes,
					cur,
					curBytes,
					rateActiveTime)
			}
		case <-shutdown:
			return
		}
	}
}

func main() {
	// setLimit()

	signalChan := make(chan os.Signal, 1)
	shutdown := make(chan struct{})
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-signalChan
		log.Printf("Server is shutting down")
		shutdown <- struct{}{}
		os.Exit(0)
	}()

	// Register counters with the Prometheus registry
	prometheus.MustRegister(pCounterBytes)
	prometheus.MustRegister(pCounterRecords)
	prometheus.MustRegister(pCounterActiveSecs)

	go aggregator(config, linecounter, shutdown)

	ln, err := net.Listen("tcp", config.sinkAddrString())
	if err != nil {
		panic(err)
	}

	go func() {
		if err := http.ListenAndServe(config.pprofAddrString(), nil); err != nil {
			log.Fatalf("pprof failed: %v", err)
		}
	}()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(config.metricsAddrString(), nil); err != nil {
			log.Fatalf("prom failed: %v", err)
		}
	}()

	for {
		conn, e := ln.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				log.Printf("accept temp err: %v", ne)
				continue
			}

			log.Printf("accept err: %v", e)
			return
		}

		go handleConn(conn, &config)
	}
}

func handleConn(conn net.Conn, config *Configuration) {
	defer func() {
		log.Printf("Closing connection %v -> %v", conn.RemoteAddr(), conn.LocalAddr())
		conn.Close()
	}()

	var w io.Writer
	// Optionally create a rate-limited writer, if the command line arguments
	// were passed specifying rate limit (and possibly maximum capacity).
	if config.isRateLimited() {
		bucket := ratelimit.NewBucketWithRate(
			config.rateLimit(), config.maxBurstCap())
		log.Printf("Connection %v -> %v [rate limit = %f MB/s | burst = %d MB]",
			conn.RemoteAddr(), conn.LocalAddr(),
			config.rateLimitMBps, config.maxBurstCapMB)
		w = ratelimit.Writer(&MockWriter{statsChan: linecounter}, bucket)
	} else {
		log.Printf("Connection %v -> %v", conn.RemoteAddr(), conn.LocalAddr())
		w = &MockWriter{statsChan: linecounter}
	}

	for {
		n, err := io.Copy(w, conn)
		switch {
		case n == 0:
			if err != nil {
				if !checkErr(err) {
					return
				}
			} else { // n == 0 && err == nil
				return
			}
		case err != nil:
			{
				if !checkErr(err) {
					return
				}
			}
		default:
			// There is nothing to do here for the time being
		}
	}
}

func setLimit() {
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		panic(err)
	}
	rLimit.Cur = rLimit.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		panic(err)
	}

	log.Printf("set cur limit: %d", rLimit.Cur)
}
