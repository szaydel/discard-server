package main

import (
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
)

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
	nbytes int
	nlines int
}

// MockWriter is an implemention of writer lacking a persistence layer.
type MockWriter struct {
	statsChan chan WriterStats
}

// Write implements the Writer interface, but does not actually persist data,
// instead just counting lines and bytes.
func (m *MockWriter) Write(p []byte) (n int, err error) {
	var stats WriterStats

	for _, v := range p {
		stats.nbytes++ // count bytes
		if v == '\n' {
			stats.nlines++ // count newlines (records)
		}
	}

	m.statsChan <- stats
	return stats.nbytes, nil
}

func aggregator(c chan WriterStats, shutdown chan struct{}) {
	// Counter is private to aggregator, since it is not used anywhere else.
	// This structure is persistent, only one instance exists and it is mutated
	// each time through the loop.
	type Counter struct {
		prev      int
		prevBytes int
		cur       int
		curBytes  int
		t         time.Time
	}
	// Every 10 seconds produce a summary with totals and rates
	ticker := time.NewTicker(10 * time.Second)
	var counter = Counter{
		t: time.Now(),
	}
	for {
		select {
		case v := <-c:
			counter.cur += v.nlines
			counter.curBytes += v.nbytes
			pCounterRecords.Add(float64(v.nlines))
			pCounterBytes.Add(float64(v.nbytes))
		case <-ticker.C:
			prev := counter.prev
			prevBytes := counter.prevBytes
			cur := counter.cur
			curBytes := counter.curBytes
			delta := time.Now().Sub(counter.t)
			rateLines := float64(cur-prev) / delta.Seconds()
			rateBytes := float64(curBytes-prevBytes) / delta.Seconds()
			log.Printf("lines/s: %f bytes/s: %f total lines: %d bytes: %d", rateLines, rateBytes, cur, curBytes)
			counter.prev = counter.cur
			counter.prevBytes = counter.curBytes
			counter.t = time.Now()
		case <-shutdown:
			return
		}
	}
}

func main() {
	// setLimit()
	var connections []net.Conn
	signalChan := make(chan os.Signal, 1)
	shutdown := make(chan struct{})
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-signalChan
		log.Printf("Server is shutting down")
		shutdown <- struct{}{}
		for _, conn := range connections {
			log.Printf("Closing conn to: %v", conn.RemoteAddr().String())
			conn.Close()
		}
		os.Exit(0)
	}()

	// Register counters with the Prometheus registry
	prometheus.MustRegister(pCounterBytes)
	prometheus.MustRegister(pCounterRecords)

	go aggregator(linecounter, shutdown)

	ln, err := net.Listen("tcp", ":5002")
	if err != nil {
		panic(err)
	}

	go func() {
		if err := http.ListenAndServe(":6060", nil); err != nil {
			log.Fatalf("pprof failed: %v", err)
		}
	}()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":9094", nil); err != nil {
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

		go handleConn(conn)
		connections = append(connections, conn)
		if len(connections)%100 == 0 {
			log.Printf("Connection Count: %v", len(connections))
		}
	}
}

func handleConn(conn net.Conn) {
	w := &MockWriter{statsChan: linecounter}
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
