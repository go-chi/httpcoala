package httpjoin // name..? broadcaster ..? uniquerequest ..? uniquehttp ?

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"sync"
)

// TODO: unexport the writers here...?

func Broadcast(methods ...string) func(next http.Handler) http.Handler {
	var requestsMu sync.Mutex
	requests := make(map[string]*BroadcastWriter)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.Println("broadcast handler..")

			var reqKey = fmt.Sprintf("%s %s", r.Method, r.URL.RequestURI())
			var bw *BroadcastWriter
			var lw *listener

			requestsMu.Lock()
			bw, ok := requests[reqKey]
			if ok {
				lw = newListener(w)
				_ = bw.AddListener(lw)
				// TODO: if false.. then, just go to next.ServeHTTP(w, r)
				// ...
			}
			requestsMu.Unlock()

			if ok {
				// existing request listening for the stuff..
				log.Println("waiting for existing request..")

				// TODO: hmm.. do we have a listener timeout..?
				// after that point, we close it up.. etc..?
				// ie. what if the first handler never responds..?
				// .. we should probably use context.Context ..
				// or, have a options with a timeout..

				select {
				case <-lw.Done():
					return
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
			delete(requests, reqKey)
			requestsMu.Unlock()

			bw.Flush()
		})
	}
}

type listener struct {
	// ID int64
	http.ResponseWriter
	headerSentCh chan struct{}
	flushedCh    chan struct{}
}

func newListener(w http.ResponseWriter) *listener {
	return &listener{
		ResponseWriter: w,
		headerSentCh:   make(chan struct{}, 1),
		flushedCh:      make(chan struct{}, 1),
	}
}

// func (lw *listener) WriteHeader(status int) {
// 	select {
// 	case <-lw.headerSentCh:
// 		log.Println("listener: WriteHeader(), case <-lw.headerSentCh")
// 	default:
// 		log.Println("listener: WriteHeader(), default")
//
// 		// lw.headerSentCh <- struct{}{}
// 		lw.ResponseWriter.WriteHeader(status)
// 		close(lw.headerSentCh)
// 	}
// }

// func (lw *listener) Write(p [][]byte) (int, error) {
// 	// select {
// 	// case ....
// 	// }
// }

func (lw *listener) Done() <-chan struct{} {
	return lw.flushedCh
}

// TODO: gotta implement Write() and WriteHeader() ...
// just to block on headerSentCh()
// .. and to close it..?

type BroadcastWriter struct { // Rename Broadcaster ...?
	listeners []*listener
	header    http.Header
	bufw      *bytes.Buffer

	headerSent bool
	// headerSentMu sync.Mutex

	flushed bool
	// flushCh    chan struct{} // channel of channels...?
}

func NewBroadcastWriter(w http.ResponseWriter) *BroadcastWriter {
	return &BroadcastWriter{
		listeners: []*listener{newListener(w)},
		header:    http.Header{},
		bufw:      &bytes.Buffer{},
		// flushCh:   make(chan struct{}),
	}
}

// TODO: we should add a bool to confirm
// the listener was added.. and a lock on headerSentMu perhaps,
// cuz, once the header is sent, we can't accept any more listeners..
func (w *BroadcastWriter) AddListener(lw *listener) bool {
	// w.headerSentMu.Lock()
	// defer w.headerSentMu.Unlock()

	if w.headerSent {
		return false
	}

	w.listeners = append(w.listeners, lw)
	return true
}

// TODO: RemoveListener ...?
// or Subscribe() and Unsubscribe() ?

func (w *BroadcastWriter) Header() http.Header {
	return w.header
}

func (w *BroadcastWriter) Write(p []byte) (int, error) {
	// w.headerSentMu.Lock()
	// defer w.headerSentMu.Unlock()

	if !w.headerSent {
		w.WriteHeader(http.StatusOK)
	}
	return w.bufw.Write(p)
}

func (w *BroadcastWriter) WriteHeader(status int) {
	// w.headerSentMu.Lock()
	// defer w.headerSentMu.Unlock()

	if w.headerSent {
		return
	}
	w.headerSent = true

	log.Println("listeners...?", len(w.listeners))

	for _, lw := range w.listeners {
		go func(lw *listener, status int, header http.Header) {
			h := map[string][]string(lw.Header())
			for k, v := range header {
				h[k] = v
			}
			// lw.headerSent = true
			lw.WriteHeader(status)
			lw.headerSentCh <- struct{}{}
		}(lw, status, w.header)
	}
}

// how does http streaming work...? can we broadcast streaming...?
// best is to make a test case really..
// but first, how does Flush() operate normally?
// what happens to connections normally after a request..? etc.?
func (w *BroadcastWriter) Flush() {
	if w.flushed {
		// TODO: should we print an error or something...?
		return
	}
	w.flushed = true

	log.Println("flushing..")

	// hmm.. should we lock this entire writer..?

	data := w.bufw.Bytes()

	for _, lw := range w.listeners {
		go func(lw *listener, data []byte) {
			// hmm.. block until headerSentCh ... ?

			lw.Write(data)
			// if lw.flushedCh != nil {
			close(lw.flushedCh)
			// lw.flushedCh <- struct{}{}
			// }
		}(lw, data)
	}
}
