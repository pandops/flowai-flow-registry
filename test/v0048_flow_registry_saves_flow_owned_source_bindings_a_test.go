//go:build !integration

package test

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// Requirement: flow-registry saves flow-owned Source bindings atomically.

func TestV0048FlowRegistrySavesFlowOwnedSourceBindings(t *testing.T) {
	t.Run("Competing Flow binding edits", func(t *testing.T) {
		h := newHarness(t)
		a := h.createFlow(3, set("name", "a"))
		b := h.createFlow(3, set("name", "b"))
		results := make([]reply, 2)
		var wg sync.WaitGroup
		for i, v := range []map[string]any{a, b} {
			wg.Add(1)
			go func(i int, id int64) {
				defer wg.Done()
				results[i] = h.ui(3, "flow-replace", map[string]any{"flow_id": id}, flowBody(set("name", fmt.Sprint("ab"[i:i+1])), bindTo(map[int64]int{12: 5}, 12)), ifMatch(1))
			}(i, flowID(t, v))
		}
		wg.Wait()
		ok, stale := 0, 0
		for _, r := range results {
			switch r.Status {
			case 200:
				ok++
			case 412:
				stale++
			default:
				t.Fatalf("competing edit answered %d: %s", r.Status, r.Body)
			}
		}
		if ok != 1 || stale != 1 {
			t.Fatalf("competing edits: %d committed, %d stale; want 1 and 1", ok, stale)
		}
		bound := targetsOf(t, h, 3, 12).json(t)["flows"].([]any)
		if len(bound) != 1 {
			t.Fatalf("Source 12 bound to %d Flows, want 1", len(bound))
		}
		if h.other.sourceRevision(3, 12) != 6 {
			t.Fatalf("Source revision = %d, want 6 after one successful guard", h.other.sourceRevision(3, 12))
		}

		t.Run("33rd binding of one Source", func(t *testing.T) {
			h := newHarness(t)
			for i := 0; i < 32; i++ {
				h.createFlow(3, set("name", fmt.Sprintf("flow-%02d", i)), bindTo(map[int64]int{12: 5 + i}, 12))
			}
			rev := h.other.sourceRevision(3, 12)
			applies := len(h.requests(subjApplyFlowTeam))
			guards := len(h.requests(subjBindingGuard))
			h.ui(3, "flow-create", nil, flowBody(set("name", "thirty-third"), bindTo(map[int64]int{12: rev}, 12))).expect(t, 400, "invalid_request")
			if h.other.sourceRevision(3, 12) != rev || len(h.requests(subjApplyFlowTeam)) != applies || len(h.requests(subjBindingGuard)) != guards {
				t.Fatalf("limit-exceeding write changed a Source, guard or LiteLLM team")
			}
			if h.mem.FlowCount() != 32 {
				t.Fatalf("Flow count = %d, want 32", h.mem.FlowCount())
			}
		})

		t.Run("33 Sources on one Flow", func(t *testing.T) {
			h := newHarness(t)
			revs := map[int64]int{}
			var ids []int64
			for i := int64(100); i < 133; i++ {
				h.other.addSource(3, i, 1)
				revs[i] = 1
				ids = append(ids, i)
			}
			h.ui(3, "flow-create", nil, flowBody(bindTo(revs, ids...))).expect(t, 400, "invalid_request")
			if len(h.requests(subjBindingGuard)) != 0 {
				t.Fatalf("33 Sources reached the binding guard")
			}
		})
	})

	t.Run("Source schema changes during Flow edit", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3)
		// The open form loaded Source 12 at revision 5; schema registration moved it to 6.
		h.other.set(func(o *others) { o.sources[3][12] = 6 })
		validates := len(h.requests(subjValidateModels))
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(bindTo(map[int64]int{12: 5}, 12)), ifMatch(1)).expect(t, 412, "")
		if len(h.requests(subjValidateModels)) != validates || len(h.requests(subjApplyFlowTeam)) != 1 {
			t.Fatalf("stale Source revision reached model-registry")
		}
		got := h.ui(3, "flow-get", map[string]any{"flow_id": flowID(t, v)}, nil)
		if len(strs(got.json(t)["source_ids"])) != 0 || revision(t, got.json(t)) != 1 {
			t.Fatalf("Flow changed: %s", got.Body)
		}
	})

	t.Run("Successful binding advances the Source revision", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3)
		r := h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(bindTo(map[int64]int{12: 5}, 12)), ifMatch(1))
		r.expect(t, 200, "")
		if h.other.sourceRevision(3, 12) != 6 {
			t.Fatalf("Source 12 revision = %d, want 6", h.other.sourceRevision(3, 12))
		}
		if strings.Join(strs(r.json(t)["source_ids"]), ",") != "12" {
			t.Fatalf("binding not kept: %v", r.json(t)["source_ids"])
		}
		lookups := h.requests(subjSourcesLookup)
		guards := h.requests(subjBindingGuard)
		if len(lookups) != 1 || len(guards) != 1 {
			t.Fatalf("sources-lookup %d, binding-guard %d; want 1 each", len(lookups), len(guards))
		}
		var guard struct {
			Body struct {
				TeamID  int64 `json:"team_id"`
				Sources []struct {
					SourceID         int64 `json:"source_id"`
					ExpectedRevision int   `json:"expected_revision"`
				} `json:"sources"`
			} `json:"body"`
		}
		_ = json.Unmarshal(guards[0].Body, &guard)
		if guard.Body.TeamID != 3 || len(guard.Body.Sources) != 1 || guard.Body.Sources[0].SourceID != 12 || guard.Body.Sources[0].ExpectedRevision != 5 {
			t.Fatalf("binding-guard request = %s", guards[0].Body)
		}
		// Unbinding sends the union of old and new Sources.
		r = h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(bindTo(map[int64]int{12: 6, 13: 2}, 13)), ifMatch(2))
		r.expect(t, 200, "")
		if h.other.sourceRevision(3, 12) != 7 || h.other.sourceRevision(3, 13) != 3 {
			t.Fatalf("union not guarded: 12=%d 13=%d", h.other.sourceRevision(3, 12), h.other.sourceRevision(3, 13))
		}
		// source_revisions that are not the exact union are rejected.
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(bindTo(map[int64]int{12: 7}, 12)), ifMatch(3)).expect(t, 400, "invalid_request")
		// A foreign or unknown Source fails the lookup.
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(bindTo(map[int64]int{13: 3, 20: 1}, 13, 20)), ifMatch(3)).expect(t, 400, "invalid_request")
		if h.other.sourceRevision(3, 12) != 7 {
			t.Fatalf("failed writes advanced a Source")
		}
		// A Flow save never carries Source settings to source-controller.
		for _, req := range h.requests("") {
			if strings.Contains(string(req.Body), "settings\"") && strings.HasPrefix(req.Subject, "source-controller.") {
				t.Fatalf("Flow save sent Source settings: %s", req.Body)
			}
		}
	})

	t.Run("Old Flow client submits embedded settings", func(t *testing.T) {
		for _, settings := range []any{[]any{}, []any{map[string]any{"source_id": 12, "values": map[string]any{}}}, nil} {
			h := newHarness(t)
			h.ui(3, "flow-create", nil, flowBody(set("source_settings", settings))).expect(t, 400, "invalid_request")
			v := h.createFlow(3)
			before := h.callsTo("source-controller.", "model-registry.", "executor-registry.")
			h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("source_settings", settings)), ifMatch(1)).expect(t, 400, "invalid_request")
			if h.callsTo("source-controller.", "model-registry.", "executor-registry.") != before || h.mem.FlowCount() != 1 {
				t.Fatalf("source_settings=%v reached another service or persisted", settings)
			}
		}
	})

	t.Run("Archive alias removal keeps bindings", func(t *testing.T) {
		h := newHarness(t)
		h.other.addSource(3, 14, 1)
		v := h.createFlow(3, set("model_aliases", []any{"3/gpt-4o", "3/claude-sonnet"}),
			set("prompt_template", "Keep {{external_id}}"), bindTo(map[int64]int{12: 5, 13: 2, 14: 1}, 12, 13, 14))
		h.service("3", "retire-alias", map[string]any{"team_id": 3, "alias": "3/claude-sonnet", "operator_id": 7}).expect(t, 204, "")
		got := h.ui(3, "flow-get", map[string]any{"flow_id": flowID(t, v)}, nil).json(t)
		if strings.Join(strs(got["source_ids"]), ",") != "12,13,14" || got["prompt_template"] != "Keep {{external_id}}" {
			t.Fatalf("bindings or execution fields changed: %v", got)
		}
		if strings.Join(strs(got["model_aliases"]), ",") != "3/gpt-4o" || revision(t, got) != 2 || num(t, got["execution_revision"]) != 1 {
			t.Fatalf("only the model list and revision should change: %v", got)
		}
		// Settings-only writes keep bindings too.
		h.ui(3, "llm-settings-replace", map[string]any{"flow_id": flowID(t, v)}, map[string]any{"model_aliases": []any{"3/gpt-4o"}}, ifMatch(2)).expect(t, 200, "")
		got = h.ui(3, "flow-get", map[string]any{"flow_id": flowID(t, v)}, nil).json(t)
		if strings.Join(strs(got["source_ids"]), ",") != "12,13,14" {
			t.Fatalf("settings write changed bindings: %v", got["source_ids"])
		}
	})
}
