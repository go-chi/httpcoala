package httpjoin

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHandler(t *testing.T) {
	var hits uint32

	var expectedStatus int = 201
	var expectedBody = []byte("hi")

	app := func(w http.ResponseWriter, r *http.Request) {
		log.Println("app handler")

		atomic.AddUint32(&hits, 1)

		// TODO: also test this with no sleep

		time.Sleep(100 * time.Millisecond) // slow handler
		w.Header().Set("X-Httpjoin", "test")
		w.WriteHeader(expectedStatus)
		w.Write(expectedBody)
	}

	// mw := func(next http.Handler) http.Handler {
	// 	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	// 		fmt.Println("mw..")
	// 		next.ServeHTTP(w, r)
	// 	})
	// }

	ts := httptest.NewServer(Broadcast("GET")(http.HandlerFunc(app)))
	defer ts.Close()

	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get(ts.URL)
			if err != nil {
				t.Fatal(err)
			}

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			log.Println("got resp:", resp, "len:", len(body), "body:", string(body))

			if string(body) != string(expectedBody) {
				t.Error("expecting response body:", string(expectedBody))
			}

			if resp.StatusCode != expectedStatus {
				t.Error("expecting response status:", expectedStatus)
			}

			if resp.Header.Get("X-Httpjoin") != "test" {
				t.Error("expecting x-httpjoin test header")
			}

		}()
	}

	wg.Wait()

	totalHits := atomic.LoadUint32(&hits)
	if totalHits > 1 {
		t.Fatal("handler was hit more than once. hits:", totalHits)
	}
}
