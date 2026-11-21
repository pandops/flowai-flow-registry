// Package health serves /v1/livez and /v1/readyz; every other path is 404.
package health

import "net/http"

// Handler serves the probes; ready reports PostgreSQL and NATS readiness.
func Handler(ready func() bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /v1/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	return mux
}
