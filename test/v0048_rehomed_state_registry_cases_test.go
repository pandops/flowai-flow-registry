//go:build !integration

package test

// Survivors of deleted State Registry tests (decision ledger
// docs/testing/v0048-test-decisions.csv), re-homed to flow-registry with
// the old inputs and oracles translated to v0048 wire shapes.

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pandops/flowai-flow-registry/internal/flows"
)

// sameIDs reports whether got holds exactly the ids of want, in any order.
func sameIDs(got, want []int64) bool {
	a, b := slices.Clone(got), slices.Clone(want)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

func limitOf(c applyCall) string {
	var l map[string]any
	_ = json.Unmarshal(c.FlowLimit, &l)
	s, _ := l["max_budget_usd"].(string)
	return s
}

func dayLimit(usd string) map[string]any {
	return map[string]any{"max_budget_usd": usd, "budget_period": "day"}
}

// M175, flow-registry half (TestFlowWriteWaitsForModelChangeInsteadOfFailing):
// a Flow write whose validate-models reply waits for an in-flight model
// change succeeds once the change completes instead of failing.
func TestV0048FlowWriteWaitsForModelChangeInsteadOfFailing(t *testing.T) {
	h := newHarness(t)
	v := h.createFlow(3)
	id := flowID(t, v)
	changed := make(chan struct{})
	h.other.set(func(o *others) {
		o.validateHook = func() {
			// The pause of 3/claude-sonnet holds the model, then commits.
			time.Sleep(500 * time.Millisecond)
			h.other.addModel(3, "3/claude-sonnet", modelState{Availability: "paused", Selectable: true})
			close(changed)
		}
	})
	r := h.ui(3, "flow-replace", map[string]any{"flow_id": id}, flowBody(set("model_aliases", []any{"3/claude-sonnet", "3/gpt-4o"})), ifMatch(1))
	select {
	case <-changed:
	default:
		t.Fatal("the Flow write finished while the model change held the model")
	}
	r.expect(t, 200, "")
	if got := strs(r.json(t)["model_aliases"]); !reflect.DeepEqual(got, []string{"3/claude-sonnet", "3/gpt-4o"}) || revision(t, r.json(t)) != 2 {
		t.Fatalf("Flow after the write: %s", r.Body)
	}
}

// M178 (TestReviewFlowRepairPreservesNewCommittedBudget): after a failed
// Flow commit, a newer write that commits before the repair lands leaves
// model-registry with the newer committed budget, never the older one.
func TestV0048FlowRepairPreservesNewCommittedBudget(t *testing.T) {
	t.Run("newer commit between repair retries", func(t *testing.T) {
		h := newHarness(t, func(o *flows.Options) { o.RepairInterval = 400 * time.Millisecond })
		id := flowID(t, h.createFlow(3, set("flow_limit", dayLimit("10"))))
		// The failed write's apply succeeds; the first repair finds LiteLLM
		// unavailable.
		h.other.set(func(o *others) {
			o.applyHook = func(c applyCall) {
				if limitOf(c) == "20" {
					h.other.set(func(o *others) { o.applyUnavailable = true; o.applyHook = nil })
				}
			}
		})
		h.mem.FailNextCommit(errors.New("injected flow commit failure"))
		if r := h.ui(3, "flow-replace", map[string]any{"flow_id": id}, flowBody(set("name", "fail-review"), set("flow_limit", dayLimit("20"))), ifMatch(1)); r.Status < 500 {
			t.Fatalf("injected failed write = %d: %s", r.Status, r.Body)
		}
		deadline := time.Now().Add(5 * time.Second)
		for len(h.requests(subjApplyFlowTeam)) < 3 {
			if time.Now().After(deadline) {
				t.Fatal("no repair attempt")
			}
			time.Sleep(time.Millisecond)
		}
		h.other.set(func(o *others) { o.applyUnavailable = false })
		h.ui(3, "flow-replace", map[string]any{"flow_id": id}, flowBody(set("name", "accepted-review"), set("flow_limit", dayLimit("30"))), ifMatch(1)).expect(t, 200, "")
		h.svc.Wait()
		calls := h.other.appliedCalls()
		if last := calls[len(calls)-1]; limitOf(last) != "30" || last.FlowRevision != 2 {
			t.Fatalf("late repair set model-registry to %s revision %d, want the committed 30 revision 2; calls %+v", limitOf(last), last.FlowRevision, calls)
		}
		got := h.ui(3, "flow-get", map[string]any{"flow_id": id}, nil)
		if revision(t, got.json(t)) != 2 || got.json(t)["flow_limit"].(map[string]any)["max_budget_usd"] != "30" {
			t.Fatalf("committed Flow after the repair: %s", got.Body)
		}
	})

	t.Run("newer write while a repair is in flight", func(t *testing.T) {
		h := newHarness(t)
		id := flowID(t, h.createFlow(3, set("flow_limit", dayLimit("10"))))
		var mu sync.Mutex
		var completed []applyCall
		failed := false
		repairInFlight, releaseRepair, newerArrived := make(chan struct{}), make(chan struct{}), make(chan struct{})
		h.other.set(func(o *others) {
			o.applyHook = func(c applyCall) {
				mu.Lock()
				repair := failed && limitOf(c) == "10" && repairInFlight != nil
				if limitOf(c) == "20" {
					failed = true
				}
				first := repairInFlight
				if repair {
					repairInFlight = nil
				}
				mu.Unlock()
				if repair {
					close(first)
					select {
					case <-releaseRepair:
					case <-time.After(5 * time.Second):
					}
				}
				if limitOf(c) == "30" {
					close(newerArrived)
				}
				mu.Lock()
				completed = append(completed, c)
				mu.Unlock()
			}
		})
		inFlight := repairInFlight
		h.mem.FailNextCommit(errors.New("injected flow commit failure"))
		if r := h.ui(3, "flow-replace", map[string]any{"flow_id": id}, flowBody(set("flow_limit", dayLimit("20"))), ifMatch(1)); r.Status < 500 {
			t.Fatalf("injected failed write = %d: %s", r.Status, r.Body)
		}
		select {
		case <-inFlight:
		case <-time.After(5 * time.Second):
			t.Fatal("no repair apply-flow-team")
		}
		newer := make(chan reply, 1)
		go func() {
			newer <- h.deliver("flow-registry.ui.flow-replace.pub", map[string]string{"team": "3", "operator_id": "7", "request_id": "9301", "team_name": "Operations", "If-Match": `"1"`},
				envelope(map[string]any{"flow_id": id}, flowBody(set("flow_limit", dayLimit("30")))))
		}()
		select {
		case <-newerArrived:
		case <-time.After(300 * time.Millisecond):
		}
		close(releaseRepair)
		var r reply
		select {
		case r = <-newer:
		case <-time.After(10 * time.Second):
			t.Fatal("the newer write did not finish")
		}
		r.expect(t, 200, "")
		h.svc.Wait()
		mu.Lock()
		last := completed[len(completed)-1]
		mu.Unlock()
		if limitOf(last) != "30" || last.FlowRevision != 2 {
			t.Fatalf("model-registry was last set to %s revision %d, want the committed 30 revision 2", limitOf(last), last.FlowRevision)
		}
	})
}

// M271 (TestV0013_3_ModelsLinksIncludeEveryFlowIncludingSecondaryModelReferences):
// the Flow list lists a Flow that references a model only as its secondary
// alias, in written order, and the model_alias filter returns it.
func TestV0048FlowListIncludesSecondaryModelReferences(t *testing.T) {
	h := newHarness(t)
	one := flowID(t, h.createFlow(3, set("name", "Flow one")))
	aliases := []string{"3/claude-sonnet", "3/gpt-4o"}
	multi := flowID(t, h.createFlow(3, set("name", "Multi-model Flow"), set("model_aliases", []any{aliases[0], aliases[1]})))
	ids := func(r reply) []int64 {
		r.expect(t, 200, "")
		var out []int64
		for _, it := range r.json(t)["items"].([]any) {
			out = append(out, num(t, it.(map[string]any)["flow_id"]))
		}
		return out
	}
	list := h.ui(3, "flows-list", nil, nil)
	list.expect(t, 200, "")
	using := map[string][]int64{}
	names := map[int64]string{}
	for _, it := range list.json(t)["items"].([]any) {
		f := it.(map[string]any)
		id := num(t, f["flow_id"])
		names[id], _ = f["name"].(string)
		for _, a := range strs(f["model_aliases"]) {
			using[a] = append(using[a], id)
		}
		if id == multi && !reflect.DeepEqual(strs(f["model_aliases"]), aliases) {
			t.Fatalf("multi-model aliases = %v, want %v", f["model_aliases"], aliases)
		}
	}
	if len(using["3/gpt-4o"]) != 2 || names[one] != "Flow one" || names[multi] != "Multi-model Flow" {
		t.Fatalf("Flows using 3/gpt-4o = %v names = %v", using["3/gpt-4o"], names)
	}
	if got := ids(h.ui(3, "flows-list", map[string]any{"model_alias": "3/gpt-4o"}, nil)); !sameIDs(got, []int64{one, multi}) {
		t.Fatalf("model_alias filter = %v, want both Flows", got)
	}
	if got := ids(h.ui(3, "flows-list", map[string]any{"model_alias": "3/claude-sonnet"}, nil)); !reflect.DeepEqual(got, []int64{multi}) {
		t.Fatalf("secondary model_alias filter = %v, want [%d]", got, multi)
	}
}

// M163 (TestListFlowsContinuationBoundaries): the admin tag projection with
// a limit below, equal to and above the row count returns the right count
// and a continuation only below.
func TestV0048AdminTagListContinuationBoundaries(t *testing.T) {
	h := newHarness(t)
	h.other.tags[3] = []string{"alpha", "beta", "gamma"}
	for _, tag := range []string{"alpha", "beta", "gamma"} {
		h.createFlow(3, set("name", "flow "+tag), set("execution_tag", tag))
	}
	for _, tc := range []struct {
		name       string
		limit      int
		wantCount  int
		wantCursor bool
	}{
		{"limit below row count", 2, 2, true},
		{"limit equals row count", 3, 3, false},
		{"limit above row count", 4, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := h.admin("tags", map[string]any{"limit": tc.limit}, nil)
			r.expect(t, 200, "")
			var got struct {
				Items []map[string]any `json:"items"`
				Next  any              `json:"next"`
			}
			if err := json.Unmarshal(r.Body, &got); err != nil {
				t.Fatal(err)
			}
			if len(got.Items) != tc.wantCount {
				t.Fatalf("count = %d, want %d", len(got.Items), tc.wantCount)
			}
			if (got.Next != nil) != tc.wantCursor {
				t.Fatalf("next = %v, want a continuation: %v", got.Next, tc.wantCursor)
			}
		})
	}
}
