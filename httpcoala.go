package httpcoala

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	counter int64 = 1
)

func Route(methods ...string) func(next http.Handler) http.Handler {
	var requestsMu sync.Mutex
	requests := make(map[request]*coalescer)

	methodTest := make(map[string]struct{}, len(methods))
	for _, m := range methods {
		methodTest[strings.ToUpper(m)] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.Println("broadcast handler..")

			if _, ok := methodTest[r.Method]; !ok {
				next.ServeHTTP(w, r)
				return
			}

			var reqKey = request{r.Method, r.URL.RequestURI()}
			var cw *coalescer
			var ww *writer
			var exists bool

			requestsMu.Lock()
			cw, exists = requests[reqKey]
			if exists {
				ww, exists = cw.AddWriter(w)
			}
			if !exists {
				log.Println("!!!!! SET requests[reqKey] !!!!!") //, len(cw.writers))
				cw = newCoalescer(w)
				requests[reqKey] = cw
			}
			requestsMu.Unlock()

			if ww != nil {
				// existing request listening for the stuff..
				log.Printf("waiting for existing request id:%d", ww.ID)

				<-ww.Flushed()
				return
			}

			defer func() {
				// Remove the request key from the map in case a request comes
				// in while we're writing to the listeners
				requestsMu.Lock()
				delete(requests, reqKey)
				requestsMu.Unlock()

				cw.Flush()
				<-cw.Flushed()
			}()

			log.Println("sending request to next.ServeHTTP(cw,r)")
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
	id := atomic.LoadInt64(&counter)
	atomic.AddInt64(&counter, 1)

	return &coalescer{
		writers: []*writer{newWriter(w, id)},
		header:  http.Header{},
		bufw:    &bytes.Buffer{},
	}
}

func (cw *coalescer) AddWriter(w http.ResponseWriter) (*writer, bool) {
	if atomic.LoadUint32(&cw.wroteHeader) > 0 {
		return nil, false
	}

	id := atomic.LoadInt64(&counter)
	atomic.AddInt64(&counter, 1)

	cw.mu.Lock()
	defer cw.mu.Unlock()

	ww := newWriter(w, id)
	cw.writers = append(cw.writers, ww)
	return ww, true
}

func (cw *coalescer) Header() http.Header {
	return cw.header
}

func (cw *coalescer) Write(p []byte) (int, error) {
	log.Println("broadcastwriter: Write(), wroteHeader:", atomic.LoadUint32(&cw.wroteHeader))
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
	cw.mu.Lock()
	defer cw.mu.Unlock()

	log.Println("broadcastwriter: WriterHeader(), wroteHeader:", atomic.LoadUint32(&cw.wroteHeader))

	log.Println("listeners...?", len(cw.writers))

	for _, ww := range cw.writers {
		log.Printf("=====> writeHeader() id:%d", ww.ID)
		go func(ww *writer, status int, header http.Header) {
			h := map[string][]string(ww.Header())
			for k, v := range header {
				h[k] = v
			}
			// h["X-Coalesce"] = []string{"hit"}
			h["X-ID"] = []string{fmt.Sprintf("%d", ww.ID)}

			ww.WriteHeader(status)
			close(ww.wroteHeaderCh)
		}(ww, status, cw.header)
	}
}

// how does http streaming work...? can we broadcast streaming...?
// best is to make a test case really..
// but first, how does Flush() operate normally?
// what happens to connections normally after a request..? etc.?
func (cw *coalescer) Flush() {
	if !atomic.CompareAndSwapUint32(&cw.flushed, 0, 1) {
		return
	}

	if atomic.CompareAndSwapUint32(&cw.wroteHeader, 0, 1) {
		cw.writeHeader(http.StatusOK)
	}

	log.Println("flushing..")

	data := cw.bufw.Bytes()

	for _, ww := range cw.writers {
		go func(ww *writer, data []byte) {
			// Block until the header has been written
			<-ww.wroteHeaderCh

			// Write the data to the original response writer
			// and signal to the flush channel once complete.
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
	ID int64
	http.ResponseWriter
	wroteHeaderCh chan struct{}
	flushedCh     chan struct{}
}

func newWriter(w http.ResponseWriter, id int64) *writer {
	log.Printf("newWriter, id:%d", id)
	return &writer{
		ID:             id,
		ResponseWriter: w,
		wroteHeaderCh:  make(chan struct{}, 1),
		flushedCh:      make(chan struct{}, 1),
	}
}

func (ww *writer) Flushed() <-chan struct{} {
	return ww.flushedCh
}
