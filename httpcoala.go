package httpcoala

import (
	"bytes"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var timeout time.Duration = 60000 * time.Microsecond

// Route middleware handler will coalesce multiple requests for the same URI
// (and routed methods) to be processed as a single request.
func Route(methods []string, keytypes []KeyTypes, t time.Duration) func(next http.Handler) http.Handler {
	timeout = t
	coalescer := newCoalescer(methods, keytypes)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			bw, sw, ok := coalescer.Route(w, r)

			if !ok {
				next.ServeHTTP(w, r)
				return
			}

			if sw != nil {
				<-sw.Flushed()
				return
			}

			defer coalescer.Flush(bw, r)
			next.ServeHTTP(bw, r)

		})
	}
}

func newCoalescer(methods []string, keytypes []KeyTypes) *coalescer {
	methodMap := make(map[string]struct{})
	for _, m := range methods {
		if m == "*" {
			break
		}
		methodMap[strings.ToUpper(m)] = struct{}{}
	}
	return &coalescer{
		methodMap: methodMap,
		requests:  make(map[request]*batchWriter),
		keys:      keytypes,
	}
}

type KeyTypes int

const (
	Method KeyTypes = 1 + iota
	URI
	HOST
	UA
)

type coalescer struct {
	mu        sync.Mutex
	methodMap map[string]struct{}
	requests  map[request]*batchWriter
	keys      []KeyTypes
}

func (c *coalescer) Route(w http.ResponseWriter, r *http.Request) (*batchWriter, *standbyWriter, bool) {
	if _, ok := c.methodMap[r.Method]; !ok && len(c.methodMap) > 0 {
		return nil, nil, false
	}

	reqKey := request{}
	for _, v := range c.keys {
		switch v {
		case Method:
			reqKey.Method = r.Method
		case URI:
			reqKey.URI = r.URL.RequestURI()
		case HOST:
			reqKey.HOST = r.Host
		case UA:
			reqKey.UA = r.UserAgent()
		default:
		}
	}
	var bw *batchWriter
	var sw *standbyWriter
	var found bool

	c.mu.Lock()
	defer c.mu.Unlock()

	bw, found = c.requests[reqKey]
	if found {
		sw, found = bw.AddWriter(w)
	}
	if !found {
		bw = newBatchWriter(w)
		c.requests[reqKey] = bw
	}

	return bw, sw, true
}

func (c *coalescer) Flush(bw *batchWriter, r *http.Request) {
	c.mu.Lock()
	reqKey := request{}
	for _, v := range c.keys {
		switch v {
		case Method:
			reqKey.Method = r.Method
		case URI:
			reqKey.URI = r.URL.RequestURI()
		case HOST:
			reqKey.HOST = r.Host
		case UA:
			reqKey.UA = r.UserAgent()
		default:
		}
	}
	delete(c.requests, reqKey)
	c.mu.Unlock()

	bw.Flush()
	<-bw.Flushed()
}

type request struct {
	Method string
	URI    string
	HOST   string
	UA     string
}

type batchWriter struct {
	writers []*standbyWriter
	header  http.Header
	bufw    *bytes.Buffer

	wroteHeader uint32
	flushed     uint32

	mu sync.Mutex
	wg sync.WaitGroup
}

func newBatchWriter(w http.ResponseWriter) *batchWriter {
	return &batchWriter{
		writers: []*standbyWriter{newStandbyWriter(w)},
		header:  http.Header{},
		bufw:    &bytes.Buffer{},
	}
}

func (bw *batchWriter) AddWriter(w http.ResponseWriter) (*standbyWriter, bool) {
	// Synchronize writers and wroteHeader
	bw.mu.Lock()
	defer bw.mu.Unlock()

	if atomic.LoadUint32(&bw.wroteHeader) > 0 {
		return nil, false
	}

	sw := newStandbyWriter(w)
	bw.writers = append(bw.writers, sw)
	return sw, true
}

func (bw *batchWriter) Header() http.Header {
	return bw.header
}

func (bw *batchWriter) Write(p []byte) (int, error) {
	if atomic.CompareAndSwapUint32(&bw.wroteHeader, 0, 1) {
		bw.writeHeader(http.StatusOK)
	}
	return bw.bufw.Write(p)
}

func (bw *batchWriter) WriteHeader(status int) {
	if !atomic.CompareAndSwapUint32(&bw.wroteHeader, 0, 1) {
		return
	}
	bw.writeHeader(status)
}

func (bw *batchWriter) writeHeader(status int) {
	// Synchronize writers and wroteHeader
	bw.mu.Lock()
	defer bw.mu.Unlock()

	isCoalesce := false
	// Broadcast WriteHeader to standby writers
	for _, sw := range bw.writers {
		bw.wg.Add(1)
		go write(bw, sw, status, bw.header, &isCoalesce)
	}
	bw.wg.Wait()
}

func write(bw *batchWriter, sw *standbyWriter, status int, header http.Header, isCoalesce *bool) {
	defer bw.wg.Done()
	done := make(chan struct{}, 0)
	go func(sw *standbyWriter, status int, header http.Header, isCoalesce *bool) {
		defer close(done)

		h := map[string][]string(sw.Header())
		for k, v := range header {
			h[k] = v
		}
		// Write hit headers to standby writers
		if *isCoalesce {
			h["X-Coalesce"] = []string{"HIT"}
		}
		*isCoalesce = true
		sw.WriteHeader(status)
		close(sw.wroteHeaderCh)
	}(sw, status, bw.header, isCoalesce)

	select {
	case <-done:
	case <-time.After(timeout):
	}
}

func (bw *batchWriter) Flush() {
	if !atomic.CompareAndSwapUint32(&bw.flushed, 0, 1) {
		return
	}
	if atomic.CompareAndSwapUint32(&bw.wroteHeader, 0, 1) {
		bw.writeHeader(http.StatusOK)
	}

	data := bw.bufw.Bytes()

	for _, sw := range bw.writers {
		go func(sw *standbyWriter, data []byte) {
			// Block until the header has been written
			<-sw.wroteHeaderCh

			// Write the data to the writer and signal the
			// flush to be finished by closing the channel.
			sw.Write(data)
			close(sw.flushedCh)
		}(sw, data)
	}
}

func (bw *batchWriter) Flushed() <-chan struct{} {
	return bw.writers[0].flushedCh
}

type standbyWriter struct {
	http.ResponseWriter
	wroteHeaderCh chan struct{}
	flushedCh     chan struct{}
}

func newStandbyWriter(w http.ResponseWriter) *standbyWriter {
	return &standbyWriter{
		ResponseWriter: w,
		wroteHeaderCh:  make(chan struct{}, 0),
		flushedCh:      make(chan struct{}, 0),
	}
}

func (sw *standbyWriter) Flushed() <-chan struct{} {
	return sw.flushedCh
}
