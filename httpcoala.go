package httpcoala

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// TODO: unexport the writers here...?

// TODO: will this work with consistentrd ..?

// TODO: closing these connections...?

var (
	counter int64 = 1
)

func Route(methods ...string) func(next http.Handler) http.Handler {
	var requestsMu sync.Mutex
	requests := make(map[string]*coalescer)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.Println("broadcast handler..")

			// TODO: take a hash of the method+path to generate the key instead

			var reqKey = fmt.Sprintf("%s %s", r.Method, r.URL.RequestURI())
			var cw *coalescer
			var ww *writer
			var exists bool

			requestsMu.Lock()
			cw, exists = requests[reqKey]
			if exists {
				ww, exists = cw.AddWriter(w)
			}
			if !exists {
				log.Println("!!!!! SET requests[reqKey] !!!!!")
				cw = newCoalescer(w)
				requests[reqKey] = cw
			}
			requestsMu.Unlock()

			if ww != nil { // or lw != nil
				// existing request listening for the stuff..
				log.Println("waiting for existing request..")

				// TODO: hmm.. do we have a listener timeout..?
				// after that point, we close it up.. etc..?
				// ie. what if the first handler never responds..?
				// .. we should probably use context.Context ..
				// or, have a options with a timeout..
				// or, leave it up to other middlewares
				// to stop the first handler, which will broadcast
				// all others.

				for {
					select {
					case <-ww.Flushed():
						return
					case <-time.After(3 * time.Second):
						log.Println("********************************** FORCE CLOSE *******************", ww.ID)
						ww.TmpCloseFlushCh()
					}
				}
				// <-ww.Flushed()
				// return
			}

			log.Println("sending request to next.ServeHTTP(cw,r)")
			next.ServeHTTP(cw, r)

			// Remove the request key from the map in case a request comes
			// in while we're writing to the listeners
			requestsMu.Lock()
			delete(requests, reqKey)
			requestsMu.Unlock()

			cw.Flush()
			<-cw.Flushed()
			return
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

	// note: we need to synchronize the listeners..
	// needed....? i think so..
	cw.mu.Lock()
	defer cw.mu.Unlock()

	id := atomic.LoadInt64(&counter)
	atomic.AddInt64(&counter, 1)

	ww := newWriter(w, id)
	cw.writers = append(cw.writers, ww)
	return ww, true
}

func (cw *coalescer) Header() http.Header {
	return cw.header
}

func (cw *coalescer) Write(p []byte) (int, error) {
	log.Println("broadcastwriter: Write(), wroteHeader:", atomic.LoadUint32(&cw.wroteHeader))
	if atomic.LoadUint32(&cw.wroteHeader) == 0 {
		cw.WriteHeader(http.StatusOK)
	}
	return cw.bufw.Write(p)
}

func (cw *coalescer) WriteHeader(status int) {
	log.Println("broadcastwriter: WriterHeader(), wroteHeader:", atomic.LoadUint32(&cw.wroteHeader))
	if atomic.LoadUint32(&cw.wroteHeader) > 0 {
		return
	}
	atomic.AddUint32(&cw.wroteHeader, 1)

	log.Println("listeners...?", len(cw.writers))

	// w.mu.Lock()
	// defer w.mu.Unlock()

	for _, ww := range cw.writers {
		log.Println("=====> writeHeader()", ww.ID)
		go func(ww *writer, status int, header http.Header) {
			h := map[string][]string(ww.Header())
			for k, v := range header {
				h[k] = v
			}
			h["X-Coalesce"] = []string{"hit"}
			h["X-ID"] = []string{fmt.Sprintf("%d", ww.ID)}

			ww.WriteHeader(status)
			// ww.wroteHeaderCh <- struct{}{}
			close(ww.wroteHeaderCh)
		}(ww, status, cw.header)
	}
}

// how does http streaming work...? can we broadcast streaming...?
// best is to make a test case really..
// but first, how does Flush() operate normally?
// what happens to connections normally after a request..? etc.?
func (cw *coalescer) Flush() {
	if atomic.LoadUint32(&cw.flushed) > 0 {
		// TODO: should we print an error or something...?
		return
	}
	atomic.AddUint32(&cw.flushed, 1)

	if atomic.LoadUint32(&cw.wroteHeader) == 0 {
		cw.WriteHeader(http.StatusOK)
	}

	log.Println("flushing..")

	// w.mu.Lock()
	// defer w.mu.Unlock()

	data := cw.bufw.Bytes()

	for _, ww := range cw.writers {
		go func(ww *writer, data []byte) {
			log.Println("=====> write()", ww.ID) //, "-", string(data))

			// Block until the header has been written
			<-ww.wroteHeaderCh
			// close(ww.wroteHeaderCh)

			// Write the data to the original response writer
			// and signal to the flush channel once complete.
			ww.Write(data)
			// ww.flushedCh <- struct{}{}
			close(ww.flushedCh)
		}(ww, data)
	}
}

func (cw *coalescer) Flushed() <-chan struct{} {
	return cw.writers[0].flushedCh
}

type writer struct {
	ID int64
	http.ResponseWriter
	wroteHeaderCh chan struct{}
	flushedCh     chan struct{}
}

func newWriter(w http.ResponseWriter, id int64) *writer {
	log.Println("newWriter, id:", id)
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

func (ww *writer) TmpCloseFlushCh() {
	close(ww.flushedCh)
}
