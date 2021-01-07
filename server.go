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
	rateLimitRdrMBps   float64
	maxBurstCapRdrMB   int64
	rateLimitWrMBps    float64
	maxBurstCapWrMB    int64
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

func (c *Configuration) rateLimitRdr() float64 {
	return c.rateLimitRdrMBps * MegaByteF
}

func (c *Configuration) maxBurstCapRdr() int64 {
	return c.maxBurstCapRdrMB * MegaByte
}

func (c *Configuration) rateLimitWr() float64 {
	return c.rateLimitWrMBps * MegaByteF
}

func (c *Configuration) maxBurstCapWr() int64 {
	return c.maxBurstCapWrMB * MegaByte
}

func (c *Configuration) isRateLimitedRdr() bool {
	return c.rateLimitRdrMBps > 0 && c.maxBurstCapRdrMB > 0
}

func (c *Configuration) isRateLimitedWr() bool {
	return c.rateLimitWrMBps > 0 && c.maxBurstCapWrMB > 0
}

// Configuration parameters passed from the CLI via flags
var config Configuration

// Global counter channel to track lines across all connections
var linecounter = make(chan WriterStats, 100)
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
	flag.Float64Var(&config.rateLimitRdrMBps, "rate.limit.in", 0, "Rate-limiting inbound to this many MB/s, zero means unlimited")
	flag.Int64Var(&config.maxBurstCapRdrMB, "max.burst.cap.in", 0, "Burst capacity in MB allowed inbound, (0 means use rate.limit.in)")
	flag.Float64Var(&config.rateLimitWrMBps, "rate.limit.out", 0, "Rate-limiting outbound to this many MB/s, zero means unlimited")
	flag.Int64Var(&config.maxBurstCapWrMB, "max.burst.cap.out", 0, "Burst capacity in MB allowed outbound, (0 means use rate.limit.out)")
	flag.StringVar(&config.address, "address", DefaultAddress, "Listen address")
	flag.UintVar(&config.port, "port", DefaultPort, "Port used for incoming data")
	flag.UintVar(&config.metricsPort, "metrics.port", DefaultMetricsPort, "Port used to query metrics")
	flag.UintVar(&config.pprofPort, "pprof.port", DefaultPprofPort, "Port used to obtain profiling data")

	flag.Parse()

	// If maximum burst capacity was not set, but the rate limit was set, adjust
	// capacity to be equal to rate limit. This means that the bucket will at
	// most hold tokens equal to number of bytes in the chosen rate.
	if config.maxBurstCapRdrMB < int64(config.rateLimitRdr()) {
		fmt.Fprint(os.Stderr,
			"Setting maximum burst capacity inbound equal to rate limit\n")
		config.maxBurstCapRdrMB = int64(config.rateLimitRdrMBps)
	}
	if config.maxBurstCapWrMB < int64(config.rateLimitWr()) {
		fmt.Fprint(os.Stderr,
			"Setting maximum burst capacity outbound equal to rate limit\n")
		config.maxBurstCapWrMB = int64(config.rateLimitWrMBps)
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

	stats.duration = time.Since(start)
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

// IncrBy increments the lines, bytes and active seconds counters.
func (c *Counter) IncrBy(nlines, nbytes int, active float64) {
	c.cur += nlines
	c.curBytes += nbytes
	c.curActiveSecs += active
	c.pRecords.Add(float64(nlines))
	c.pBytes.Add(float64(nbytes))
	c.pActiveSecs.Add(active)
}

// UpdatePrev sets the previous values to current values, which happens before
// the current values are updated. The delta between current and previous is
// effectively the amount of work during the last interval.
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
			delta := time.Since(counter.t)
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

// RateLimitedReader wraps an io.Reader in a new rate-limited io.Reader.
func RateLimitedReader(r io.Reader, config *Configuration) io.Reader {
	if !config.isRateLimitedRdr() {
		return r
	}
	return ratelimit.Reader(r,
		ratelimit.NewBucketWithRate(
			config.rateLimitRdr(),
			config.maxBurstCapRdr()))
}

// RateLimitedWriter wraps an io.Writer in a new rate-limited io.Writer.
func RateLimitedWriter(w io.Writer, config *Configuration) io.Writer {
	if !config.isRateLimitedWr() {
		return w
	}
	return ratelimit.Writer(w,
		ratelimit.NewBucketWithRate(
			config.rateLimitWr(),
			config.maxBurstCapWr()))
}

// WriteConnInfo puts out a connection information message with optional rate
// limiting details.
func WriteConnInfo(conn net.Conn, config *Configuration) {
	if config.isRateLimitedRdr() || config.isRateLimitedWr() {
		log.Printf("Connection rate-limited %v -> %v [inbound %f MB/s | %d MB] [outbound = %f MB/s | %d MB]",
			conn.RemoteAddr(), conn.LocalAddr(),
			config.rateLimitRdrMBps, config.maxBurstCapRdrMB,
			config.rateLimitWrMBps, config.maxBurstCapWrMB)
	} else {
		log.Printf("Connection %v -> %v", conn.RemoteAddr(), conn.LocalAddr())
	}
}

func handleConn(conn net.Conn, config *Configuration) {
	defer func() {
		log.Printf("Closing connection %v -> %v", conn.RemoteAddr(), conn.LocalAddr())
		conn.Close()
	}()

	// Optionally create a rate-limited reader and/or writer, if the command
	// line arguments were passed specifying rate limit (and possibly maximum
	// capacity).
	var r io.Reader = RateLimitedReader(conn, config)
	var w io.Writer = RateLimitedWriter(
		&MockWriter{statsChan: linecounter}, config)

	WriteConnInfo(conn, config)

	for {
		n, err := io.Copy(w, r)
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
