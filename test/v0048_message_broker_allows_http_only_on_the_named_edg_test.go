//go:build !integration

package test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pandops/flowai-flow-registry/internal/health"
)

// Requirement: message-broker allows HTTP only on the named edges.
// Not applicable to flow-registry: "Source system reads its settings on the
// public edge" (source-controller), "Executor edge errors are
// distinguishable" (env-registry) and "Journal stream edge requires the
// Gateway token" (journal).

func TestV0048MessageBrokerAllowsHTTPOnlyOnTheNamedEdges(t *testing.T) {
	t.Run("Service offers no internal HTTP API", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3)
		commits := h.mem.Commits()
		srv := httptest.NewServer(health.Handler(func() bool { return h.svc.Ready(h.ctx) }))
		defer srv.Close()
		for _, req := range [][2]string{
			{"GET", "/ui/v1/flows"}, {"POST", "/ui/v1/flows"}, {"GET", "/v1/flows/1"}, {"PUT", "/v1/flows/1"},
			{"GET", "/v1/teams/3/flows"}, {"POST", "/v1/flows/preview"}, {"GET", "/"}, {"DELETE", "/v1/readyz"},
		} {
			r, _ := http.NewRequest(req[0], srv.URL+req[1], strings.NewReader(`{}`))
			resp, err := http.DefaultClient.Do(r)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			want := 404
			if req[1] == "/v1/readyz" {
				want = 405
			}
			if resp.StatusCode != want {
				t.Fatalf("%s %s = %d, want %d", req[0], req[1], resp.StatusCode, want)
			}
		}
		if h.mem.Commits() != commits || h.mem.FlowCount() != 1 {
			t.Fatalf("HTTP request changed data")
		}
	})
}
