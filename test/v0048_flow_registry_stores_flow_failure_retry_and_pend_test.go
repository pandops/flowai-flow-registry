//go:build !integration

package test

import "testing"

// Requirement: flow-registry stores Flow failure retry and pending settings.

var taskSettingNames = []string{"failure_retry_count", "failure_retry_wait_seconds", "pending_limit_seconds", "release_delay_seconds"}

func TestV0048FlowRegistryStoresFlowFailureRetryAndPendingSettings(t *testing.T) {
	t.Run("Flow is created with defaults", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3, bindTo(map[int64]int{12: 5}, 12))
		want := map[string]int64{"failure_retry_count": 3, "failure_retry_wait_seconds": 60, "pending_limit_seconds": 900, "release_delay_seconds": 300}
		for k, w := range want {
			if num(t, v[k]) != w {
				t.Fatalf("Flow %s = %v, want %d", k, v[k], w)
			}
		}
		rev := targetsOf(t, h, 3, 12).json(t)["flows"].([]any)[0].(map[string]any)["execution_revision"].(map[string]any)
		for k, w := range want {
			if num(t, rev[k]) != w {
				t.Fatalf("first execution revision %s = %v, want %d", k, rev[k], w)
			}
		}
	})

	t.Run("Operator disables failure retries", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3, bindTo(map[int64]int{12: 5}, 12))
		r := h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("failure_retry_count", 0), bindTo(map[int64]int{12: 6}, 12)), ifMatch(1))
		r.expect(t, 200, "")
		if num(t, r.json(t)["failure_retry_count"]) != 0 || num(t, r.json(t)["execution_revision"]) != 2 {
			t.Fatalf("replacement view = %s", r.Body)
		}
		rev := targetsOf(t, h, 3, 12).json(t)["flows"].([]any)[0].(map[string]any)["execution_revision"].(map[string]any)
		if num(t, rev["failure_retry_count"]) != 0 || num(t, rev["revision"]) != 2 {
			t.Fatalf("new execution revision = %v, want failure_retry_count 0 at revision 2", rev)
		}
		if ex := h.mem.Executions(3, flowID(t, v)); ex[0].FailureRetryCount != 3 {
			t.Fatalf("frozen revision 1 changed to %d", ex[0].FailureRetryCount)
		}
	})

	t.Run("Out-of-range setting is rejected", func(t *testing.T) {
		bad := []struct {
			name  string
			value any
		}{
			{"failure_retry_count", 11}, {"failure_retry_count", -1}, {"failure_retry_count", 1.5}, {"failure_retry_count", "3"}, {"failure_retry_count", nil},
			{"failure_retry_wait_seconds", 9}, {"failure_retry_wait_seconds", 86401},
			{"pending_limit_seconds", 30}, {"pending_limit_seconds", 86401}, {"pending_limit_seconds", nil},
			{"release_delay_seconds", -1}, {"release_delay_seconds", 59}, {"release_delay_seconds", 86401},
		}
		for _, c := range bad {
			h := newHarness(t)
			h.ui(3, "flow-create", nil, flowBody(set(c.name, c.value))).expect(t, 400, "invalid_request")
			v := h.createFlow(3)
			h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set(c.name, c.value)), ifMatch(1)).expect(t, 400, "invalid_request")
			if n := len(h.mem.Executions(3, flowID(t, v))); n != 1 {
				t.Fatalf("%s=%v appended a revision", c.name, c.value)
			}
		}
		h := newHarness(t)
		v := h.createFlow(3, set("failure_retry_count", 10), set("failure_retry_wait_seconds", 10), set("pending_limit_seconds", 86400), set("release_delay_seconds", 60))
		if num(t, v["failure_retry_count"]) != 10 || num(t, v["pending_limit_seconds"]) != 86400 {
			t.Fatalf("boundary values not stored: %v", v)
		}
	})

	t.Run("Omitted setting keeps its value on replacement", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3, set("release_delay_seconds", 600), set("pending_limit_seconds", 120))
		r := h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("prompt_template", "Changed {{external_id}}")), ifMatch(1))
		r.expect(t, 200, "")
		got := r.json(t)
		if num(t, got["release_delay_seconds"]) != 600 || num(t, got["pending_limit_seconds"]) != 120 || num(t, got["failure_retry_count"]) != 3 {
			t.Fatalf("omitted settings not kept: %v", got)
		}
		ex := h.mem.Executions(3, flowID(t, v))
		if len(ex) != 2 || ex[1].ReleaseDelaySeconds != 600 {
			t.Fatalf("new execution revision = %+v, want release_delay_seconds 600", ex)
		}
	})
}
