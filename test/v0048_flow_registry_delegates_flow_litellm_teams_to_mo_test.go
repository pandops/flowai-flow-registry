//go:build !integration

package test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// Requirement: flow-registry delegates Flow LiteLLM teams to model-registry.

func TestV0048FlowRegistryDelegatesFlowLiteLLMTeams(t *testing.T) {
	t.Run("model-registry is unavailable during a Flow save", func(t *testing.T) {
		for name, fail := range map[string]func(o *others){
			"no reply":                   func(o *others) { o.applyDown = true },
			"LiteLLM unavailable":        func(o *others) { o.applyUnavailable = true },
			"validate-models unanswered": func(o *others) { o.modelsDown = true },
		} {
			t.Run(name, func(t *testing.T) {
				h := newHarness(t)
				v := h.createFlow(3, set("flow_limit", map[string]any{"max_budget_usd": "20", "budget_period": "day"}))
				id := flowID(t, v)
				h.other.set(fail)
				h.ui(3, "flow-replace", map[string]any{"flow_id": id}, flowBody(set("flow_limit", map[string]any{"max_budget_usd": "50", "budget_period": "day"})), ifMatch(1)).
					expect(t, 503, "model_gateway_unavailable")
				got := h.ui(3, "flow-get", map[string]any{"flow_id": id}, nil)
				got.expect(t, 200, "")
				lim := got.json(t)["flow_limit"].(map[string]any)
				if revision(t, got.json(t)) != 1 || lim["max_budget_usd"] != "20" {
					t.Fatalf("record changed after the 503: %s", got.Body)
				}
				h.ui(3, "flow-create", nil, flowBody(set("name", "new one"))).expect(t, 503, "model_gateway_unavailable")
				if h.mem.FlowCount() != 1 {
					t.Fatalf("create during the outage persisted a Flow")
				}
			})
		}
	})

	t.Run("Commit fails after the LiteLLM team changed", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3, set("flow_limit", map[string]any{"max_budget_usd": "20", "budget_period": "week"}))
		id := flowID(t, v)
		h.flush()
		h.mem.FailNextCommit(errors.New("injected commit failure"))
		r := h.ui(3, "flow-replace", map[string]any{"flow_id": id}, flowBody(set("flow_limit", map[string]any{"max_budget_usd": "50", "budget_period": "week"})), ifMatch(1))
		if r.Status < 500 {
			t.Fatalf("failed commit answered %d, want a 5xx: %s", r.Status, r.Body)
		}
		// The previously committed 20 USD record stays and is re-applied
		// until model-registry confirms it.
		deadline := time.Now().Add(5 * time.Second)
		for {
			calls := h.other.appliedCalls()
			last := calls[len(calls)-1]
			var limit map[string]any
			_ = json.Unmarshal(last.FlowLimit, &limit)
			if len(calls) >= 3 && limit["max_budget_usd"] == "20" && last.FlowRevision == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("no restoring apply-flow-team with the committed record; calls %+v", calls)
			}
			time.Sleep(5 * time.Millisecond)
		}
		h.svc.Wait()
		got := h.ui(3, "flow-get", map[string]any{"flow_id": id}, nil)
		if revision(t, got.json(t)) != 1 || got.json(t)["flow_limit"].(map[string]any)["max_budget_usd"] != "20" {
			t.Fatalf("committed record changed: %s", got.Body)
		}
		var events []string
		for _, m := range h.flush() {
			if strings.HasPrefix(m.Subject, "flow-registry.event.") {
				events = append(events, m.Subject)
			}
		}
		if len(events) != 1 || events[0] != "flow-registry.event.flow_team_restored.pub" {
			t.Fatalf("events after restoration = %v, want one flow_team_restored", events)
		}
	})

	t.Run("Restoration retries until model-registry confirms", func(t *testing.T) {
		h := newHarness(t)
		v := h.createFlow(3, set("flow_limit", map[string]any{"max_budget_usd": "20", "budget_period": "week"}))
		// The operator's 50 USD apply succeeds; every later apply finds LiteLLM unavailable.
		h.other.set(func(o *others) {
			o.applyHook = func(applyCall) { h.other.set(func(o *others) { o.applyUnavailable = true; o.applyHook = nil }) }
		})
		h.mem.FailNextCommit(errors.New("injected commit failure"))
		h.ui(3, "flow-replace", map[string]any{"flow_id": flowID(t, v)}, flowBody(set("flow_limit", map[string]any{"max_budget_usd": "50", "budget_period": "week"})), ifMatch(1))
		deadline := time.Now().Add(5 * time.Second)
		for len(h.requests(subjApplyFlowTeam)) < 5 {
			if time.Now().After(deadline) {
				t.Fatalf("restoration did not keep retrying while model-registry was unavailable: %d requests", len(h.requests(subjApplyFlowTeam)))
			}
			time.Sleep(2 * time.Millisecond)
		}
		h.other.set(func(o *others) { o.applyUnavailable = false })
		h.svc.Wait()
		calls := h.other.appliedCalls()
		var limit map[string]any
		_ = json.Unmarshal(calls[len(calls)-1].FlowLimit, &limit)
		if limit["max_budget_usd"] != "20" || calls[len(calls)-1].FlowRevision != 1 {
			t.Fatalf("last confirmed apply = %+v %v, want the committed 20 USD revision 1", calls[len(calls)-1], limit)
		}
	})

	// Deviation D17, owner decision 2026-11-09: a failed create leaves no
	// committed record, so its LiteLLM team is removed instead of restored.
	t.Run("Create commit fails after the LiteLLM team was created", func(t *testing.T) {
		h := newHarness(t)
		h.mem.FailNextCommit(errors.New("injected commit failure"))
		r := h.ui(3, "flow-create", nil, flowBody(set("flow_limit", map[string]any{"max_budget_usd": "20", "budget_period": "week"})))
		if r.Status < 500 {
			t.Fatalf("failed commit answered %d, want a 5xx: %s", r.Status, r.Body)
		}
		applied := h.other.appliedCalls()
		if len(applied) != 1 {
			t.Fatalf("apply-flow-team calls = %+v, want the create's one", applied)
		}
		orphan := applied[0].FlowID
		h.svc.Wait()
		if got := h.other.removedTeams(); len(got) != 1 || got[0] != [2]int64{3, orphan} {
			t.Fatalf("remove-flow-team confirmations = %v, want one for (3, %d)", got, orphan)
		}
		if n := len(h.other.appliedCalls()); n != 1 {
			t.Fatalf("apply-flow-team calls = %d after the failed create, want no re-apply", n)
		}
		if h.mem.FlowCount() != 0 {
			t.Fatalf("the failed create persisted a Flow")
		}
		for _, m := range h.flush() {
			if strings.HasPrefix(m.Subject, "flow-registry.event.") {
				t.Fatalf("event %s after a failed create, want none", m.Subject)
			}
		}
		// The flow_id of the failed create is never reused.
		if id := flowID(t, h.createFlow(3)); id == orphan {
			t.Fatalf("next create reused flow_id %d", orphan)
		}
	})

	t.Run("Orphan team removal retries until model-registry confirms", func(t *testing.T) {
		h := newHarness(t)
		h.other.set(func(o *others) { o.removeUnavailable = true })
		h.mem.FailNextCommit(errors.New("injected commit failure"))
		h.ui(3, "flow-create", nil, flowBody())
		deadline := time.Now().Add(5 * time.Second)
		for len(h.requests(subjRemoveFlowTeam)) < 3 {
			if time.Now().After(deadline) {
				t.Fatalf("removal did not keep retrying while model-registry was unavailable: %d requests", len(h.requests(subjRemoveFlowTeam)))
			}
			time.Sleep(2 * time.Millisecond)
		}
		if got := h.other.removedTeams(); len(got) != 0 {
			t.Fatalf("removal confirmed while model-registry was unavailable: %v", got)
		}
		h.other.set(func(o *others) { o.removeUnavailable = false })
		h.svc.Wait()
		if got := h.other.removedTeams(); len(got) != 1 || got[0][0] != 3 {
			t.Fatalf("remove-flow-team confirmations = %v, want exactly one for team 3", got)
		}
	})

	t.Run("Archived model is rejected", func(t *testing.T) {
		h := newHarness(t)
		h.other.addModel(3, "3/archived", modelState{Availability: "paused", Selectable: false})
		h.other.addModel(3, "3/pending-archive", modelState{Availability: "active", Selectable: false})
		for _, alias := range []string{"3/archived", "3/pending-archive"} {
			h.ui(3, "flow-create", nil, flowBody(set("model_aliases", []any{"3/gpt-4o", alias}))).expect(t, 400, "invalid_request")
		}
		if n := len(h.requests(subjApplyFlowTeam)); n != 0 {
			t.Fatalf("apply-flow-team requests = %d, want 0", n)
		}
		if n := len(h.requests(subjValidateModels)); n != 2 {
			t.Fatalf("validate-models requests = %d, want 2", n)
		}
	})
}
