//go:build !integration

package test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Requirement: service-platform service gets foreign data only from the broker.
//
// "Team archive reaches a projection" and "Projection ignores a replayed
// event" are not applicable to flow-registry: it keeps no projection and
// declares no durable consumer (schemas/consumers.json is []). This test
// pins that and that foreign data arrives only through broker requests.

func TestV0048ServicePlatformServiceGetsForeignDataOnlyFromTheBroker(t *testing.T) {
	t.Run("flow-registry declares no consumer and reads foreign data by request", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join(serviceDir(), "schemas", "consumers.json"))
		if err != nil || strings.TrimSpace(string(data)) != "[]" {
			t.Fatalf("consumers.json = %q, %v; want []", data, err)
		}
		h := newHarness(t)
		h.createFlow(3, bindTo(map[int64]int{12: 5}, 12))
		for _, subj := range []string{subjTeamTags, subjValidateModels, subjApplyFlowTeam, subjSourcesLookup, subjBindingGuard} {
			if len(h.requests(subj)) == 0 {
				t.Fatalf("create did not ask %s", subj)
			}
		}
		// Every SQL text in the service names only its own schema.
		prod, _ := goFiles(t)
		sqlDir := filepath.Join(serviceDir(), "internal", "migrations", "sql")
		files, _ := filepath.Glob(filepath.Join(sqlDir, "*.sql"))
		for _, f := range append(prod, files...) {
			body, _ := os.ReadFile(f)
			for _, foreign := range []string{"source_controller_schema", "model_registry_schema", "executor_registry_schema", "task_manager_schema", "env_registry_schema", "team_registry_schema", "registry_schema", "public."} {
				// Whole names only: flow_registry_schema itself ends in registry_schema.
				if regexp.MustCompile(`(^|[^A-Za-z0-9_])` + regexp.QuoteMeta(foreign)).Match(body) {
					t.Fatalf("%s names %s", f, foreign)
				}
			}
		}
	})
}
