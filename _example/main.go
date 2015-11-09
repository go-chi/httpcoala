package main

import (
	"net/http"
	"time"

	"github.com/goware/httpcoala"
	"github.com/pressly/chi"
	"github.com/pressly/chi/middleware"
)

func main() {
	r := chi.NewRouter()

	r.Use(middleware.Logger)
	r.Use(httpcoala.Route("HEAD", "GET")) // or, Route("*")

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond) // expensive op
		w.WriteHeader(200)
		w.Write([]byte("hi"))
	})

	http.ListenAndServe(":1111", r)
}
