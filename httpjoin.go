package httpjoin

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"sync"
)

func Broadcast(methods ...string) func(next http.Handler) http.Handler {
	var requestsMu sync.Mutex
	requests := make(map[string]*BroadcastWriter)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.Println("broadcast handler..")

			var reqKey = fmt.Sprintf("%s %s", r.Method, r.URL.RequestURI())
			var bw *BroadcastWriter

			requestsMu.Lock()
			bw, ok := requests[reqKey]
			if ok {
				bw.Writers = append(bw.Writers, w)
			}
			requestsMu.Unlock()

			if ok {
				// existing request listening for the stuff..

				// hmm, we need a synchronizing channel here to
				// wait until we're done.
				// for/select ....?
				log.Println("waiting for existing request.. listening on bw.Done()..")

				// this wont work.. we need a doneCh for each writer
				// ..

				for {
					select {
					case <-bw.Done():
						return
					}
				}
				return
			}

			bw = NewBroadcastWriter(w)

			requestsMu.Lock()
			requests[reqKey] = bw
			requestsMu.Unlock()

			log.Println("sending request to next.ServeHTTP(bw,r)")
			next.ServeHTTP(bw, r)

			// Remove the request key from the map in case a request comes
			// in while we're writing to the listeners
			requestsMu.Lock()
			rbw := requests[reqKey] // necessary..? its a pointer..
			delete(requests, reqKey)
			requestsMu.Unlock()

			rbw.Flush()
		})
	}
}

type BroadcastWriter struct {
	Writers []http.ResponseWriter
	header  http.Header
	bufw    *bytes.Buffer

	headerSent bool

	// flushMu sync.Mutex
	flushed bool

	doneCh chan struct{}
}

func NewBroadcastWriter(w ...http.ResponseWriter) *BroadcastWriter {
	return &BroadcastWriter{
		Writers: []http.ResponseWriter(w),
		header:  http.Header{},
		bufw:    &bytes.Buffer{},
		doneCh:  make(chan struct{}),
	}
}

func (w *BroadcastWriter) AddListener(lw http.ResponseWriter, doneCh chan<- struct{}) {
	//??
}

func (w *BroadcastWriter) Header() http.Header {
	return w.header
}

func (w *BroadcastWriter) Write(p []byte) (int, error) {
	if !w.headerSent {
		w.WriteHeader(http.StatusOK)
	}
	return w.bufw.Write(p)
}

func (w *BroadcastWriter) WriteHeader(status int) {
	if w.headerSent {
		return
	}
	w.headerSent = true

	// TODO: we cant do this synchronization here............

	var wg sync.WaitGroup

	for _, ww := range w.Writers {
		wg.Add(1)
		go func(ww http.ResponseWriter, status int, header http.Header) {
			defer wg.Done()

			h := map[string][]string(ww.Header())
			for k, v := range header {
				// h.Set(k, v)
				h[k] = v
			}
			ww.WriteHeader(status)
		}(ww, status, w.header)
	}

	wg.Wait()
}

// how does http streaming work...? can we broadcast streaming...?
// best is to make a test case really..
// but first, how does Flush() operate normally?
// what happens to connections normally after a request..? etc.?
func (w *BroadcastWriter) Flush() {
	if w.flushed {
		return
	}

	log.Println("flushing..")

	// hmm.. should we lock this entire writer..?

	data := w.bufw.Bytes()

	var wg sync.WaitGroup

	for _, ww := range w.Writers {
		wg.Add(1)
		go func(ww http.ResponseWriter, data []byte) {
			defer wg.Done()
			// ww.WriteHeader(status)
			// hmm.. we can clone the map maybe..?
			ww.Write(data)
		}(ww, data)
	}

	wg.Wait()
	close(w.doneCh)

	// hmm.. set flushed...?
	// .. reset Body ...?

	// hmm.. do we collect all the response writers..?
	// not just one Response.. but all..
	// a slice of response writers..?

	// w.Response.WriteHeader(w.Status)
	// if w.Body.Len() > 0 {
	// 	_, err := w.Response.Write(w.Body.Bytes())
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	w.Body.Reset()
	// }
	w.flushed = true
}

func (w *BroadcastWriter) Done() <-chan struct{} {
	return w.doneCh
}
