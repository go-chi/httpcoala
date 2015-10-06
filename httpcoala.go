package httpcoala

import (
	"bytes"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
)

// Route middleware handler will coalesce multiple requests for the same URI
// (and routed methods) to be processed as a single request.
func Route(methods ...string) func(next http.Handler) http.Handler {
	var requestsMu sync.Mutex
	requests := make(map[request]*coalescer)

	methodTest := make(map[string]struct{}, len(methods))
	for _, m := range methods {
		methodTest[strings.ToUpper(m)] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			// Only route specified methods through the coalescer
			if _, ok := methodTest[r.Method]; !ok {
				next.ServeHTTP(w, r)
				return
			}

			// Check if an existing request is in-flight
			var reqKey = request{r.Method, r.URL.RequestURI()}
			var cw *coalescer
			var ww *writer
			var exists bool

			// TODO: code can be improved to synchronize adding a writer
			// and waiting for the header to be written at first.
			// The approach would be to make a type for the requests map
			// with Get() and Set() methods, and remove the a request
			// once the header of the coalescer has been sent.
			requestsMu.Lock()
			cw, exists = requests[reqKey]
			if exists {
				ww, exists = cw.AddWriter(w)
			}
			if !exists {
				cw = newCoalescer(w)
				requests[reqKey] = cw
			}
			requestsMu.Unlock()

			if ww != nil {
				// Wait until stand-by request has flushed
				<-ww.Flushed()
				return
			}

			// First occurence of the request, setup a defer to flush the
			// coalescer in case of panic and call next handler in the chain.
			defer func() {
				// Remove the request key from the map in case a request comes
				// in while we're writing to the listeners
				requestsMu.Lock()
				delete(requests, reqKey)
				requestsMu.Unlock()

				// Flushing the coalescer will broadcast to all writers
				cw.Flush()
				<-cw.Flushed()
			}()

			next.ServeHTTP(cw, r)
		})
	}
}

type coalescer struct {
	writers []*writer
	header  http.Header
	bufw    *bytes.Buffer

	wroteHeader uint32
	flushed     uint32

	mu sync.Mutex
}

func newCoalescer(w http.ResponseWriter) *coalescer {
	return &coalescer{
		writers: []*writer{newWriter(w)},
		header:  http.Header{},
		bufw:    &bytes.Buffer{},
	}
}

func (cw *coalescer) AddWriter(w http.ResponseWriter) (*writer, bool) {
	if atomic.LoadUint32(&cw.wroteHeader) > 0 {
		return nil, false
	}

	// Synchronize writers and wroteHeader
	cw.mu.Lock()
	defer cw.mu.Unlock()

	ww := newWriter(w)
	cw.writers = append(cw.writers, ww)
	return ww, true
}

func (cw *coalescer) Header() http.Header {
	return cw.header
}

func (cw *coalescer) Write(p []byte) (int, error) {
	if atomic.CompareAndSwapUint32(&cw.wroteHeader, 0, 1) {
		cw.writeHeader(http.StatusOK)
	}
	return cw.bufw.Write(p)
}

func (cw *coalescer) WriteHeader(status int) {
	if !atomic.CompareAndSwapUint32(&cw.wroteHeader, 0, 1) {
		return
	}
	cw.writeHeader(status)
}

func (cw *coalescer) writeHeader(status int) {
	// Synchronize writers and wroteHeader
	cw.mu.Lock()
	defer cw.mu.Unlock()

	for _, ww := range cw.writers {
		go func(ww *writer, status int, header http.Header) {
			h := map[string][]string(ww.Header())
			for k, v := range header {
				h[k] = v
			}
			// h["X-Coalesce"] = []string{"hit"}

			ww.WriteHeader(status)
			close(ww.wroteHeaderCh)
		}(ww, status, cw.header)
	}
}

func (cw *coalescer) Flush() {
	if !atomic.CompareAndSwapUint32(&cw.flushed, 0, 1) {
		return
	}
	if atomic.CompareAndSwapUint32(&cw.wroteHeader, 0, 1) {
		cw.writeHeader(http.StatusOK)
	}

	data := cw.bufw.Bytes()

	for _, ww := range cw.writers {
		go func(ww *writer, data []byte) {
			// Block until the header has been written
			<-ww.wroteHeaderCh

			// Write the data to the writer and signal the
			// flush to be finished by closing the channel.
			ww.Write(data)
			close(ww.flushedCh)
		}(ww, data)
	}
}

func (cw *coalescer) Flushed() <-chan struct{} {
	return cw.writers[0].flushedCh
}

type request struct {
	Method string
	URI    string
}

type writer struct {
	http.ResponseWriter
	wroteHeaderCh chan struct{}
	flushedCh     chan struct{}
}

func newWriter(w http.ResponseWriter) *writer {
	return &writer{
		ResponseWriter: w,
		wroteHeaderCh:  make(chan struct{}, 1),
		flushedCh:      make(chan struct{}, 1),
	}
}

func (ww *writer) Flushed() <-chan struct{} {
	return ww.flushedCh
}
