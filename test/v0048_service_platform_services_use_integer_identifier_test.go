//go:build !integration

package test

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
)

// Requirement: service-platform services use integer identifiers.
//
// "Redelivered message is deduplicated by publisher sequence" is not
// applicable: flow-registry consumes no stream (schemas/consumers.json is []).

func TestV0048ServicePlatformServicesUseIntegerIdentifiers(t *testing.T) {
	t.Run("Invalid identifier is rejected", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3)
		calls := h.callsTo("")
		for _, id := range []any{"abc", "0", "-1", "1.5", "9223372036854775808", "3f2c8a10-6b1e-4d2a-9c3e-5a7b8c9d0e1f", 0, -1, 1.5, 9223372036854775808.0, true, nil} {
			h.ui(3, "flow-get", map[string]any{"flow_id": id}, nil).expect(t, 400, "invalid_request")
			h.ui(3, "flows-list", map[string]any{"source_id": id}, nil).expect(t, 400, "invalid_request")
			h.service("3", "llm-settings", map[string]any{"team_id": id, "flow_id": 1}).expect(t, 400, "invalid_request")
		}
		for _, team := range []string{"abc", "0", "-1", "1.5", "9223372036854775808"} {
			h.ui(3, "flow-get", map[string]any{"flow_id": 1}, nil, withTeamHeader(team)).expect(t, 400, "invalid_request")
		}
		if h.callsTo("") != calls {
			t.Fatalf("invalid identifiers reached another service")
		}
	})

	t.Run("First record in each team is 1", func(t *testing.T) {
		h := newHarness(t)
		a := h.createFlow(3, set("name", "a"))
		b := h.createFlow(4, set("name", "b"))
		c := h.createFlow(3, set("name", "c"))
		if flowID(t, a) != 1 || flowID(t, b) != 1 || flowID(t, c) != 2 {
			t.Fatalf("ids = %d %d %d, want 1 1 2", flowID(t, a), flowID(t, b), flowID(t, c))
		}
	})

	t.Run("Equal numbers in two teams grant nothing", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3, set("name", "three"))
		h.createFlow(4, set("name", "four"))
		got := h.ui(3, "flow-get", map[string]any{"flow_id": 1}, nil).json(t)
		if got["name"] != "three" {
			t.Fatalf("team 3 Flow 1 = %v", got)
		}
		h.ui(3, "flow-replace", map[string]any{"flow_id": 1}, flowBody(set("name", "renamed")), ifMatch(1)).expect(t, 200, "")
		if f, _ := h.store.GetFlow(h.ctx, 4, 1); f.Name != "four" || f.Revision != 1 {
			t.Fatalf("team 4 Flow 1 changed: %+v", f)
		}
	})

	t.Run("Concurrent creates get unique increasing numbers", func(t *testing.T) {
		h := newHarness(t)
		h.mem.FailNextCommit(errors.New("injected rollback"))
		var wg sync.WaitGroup
		results := make([]reply, 20)
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results[i] = h.ui(3, "flow-create", nil, flowBody(set("name", fmt.Sprintf("c%02d", i))))
			}(i)
		}
		wg.Wait()
		h.svc.Wait()
		seen := map[int64]bool{}
		var ids []int64
		for _, r := range results {
			if r.Status == 201 {
				id := flowID(t, r.json(t))
				if seen[id] {
					t.Fatalf("number %d reused", id)
				}
				seen[id] = true
				ids = append(ids, id)
			}
		}
		if len(ids) != 19 {
			t.Fatalf("%d creates committed, want 19", len(ids))
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if ids[0] < 1 || ids[len(ids)-1] > 20 {
			t.Fatalf("ids = %v", ids)
		}
		next := h.createFlow(3, set("name", "after"))
		if flowID(t, next) <= ids[len(ids)-1] {
			t.Fatalf("next id %d does not increase past %d", flowID(t, next), ids[len(ids)-1])
		}
	})

	t.Run("Rolled-back transaction neither publishes nor reuses its sequence", func(t *testing.T) {
		h := newHarness(t)
		h.createFlow(3, set("name", "a"))
		first := h.mem.Outbox()
		h.mem.FailNextCommit(errors.New("injected"))
		h.ui(3, "flow-create", nil, flowBody(set("name", "b")))
		h.svc.Wait()
		h.createFlow(3, set("name", "c"))
		var seqs []int64
		for _, m := range h.flush() {
			var v map[string]any
			_ = jsonUnmarshal(m.Data, &v)
			seqs = append(seqs, num(t, v["publisher_seq"]))
		}
		seen := map[int64]bool{}
		for _, s := range seqs {
			if seen[s] {
				t.Fatalf("publisher_seq %d reused: %v", s, seqs)
			}
			seen[s] = true
		}
		rows := h.mem.Outbox()
		last := rows[len(rows)-1].Seq
		if last <= first[len(first)-1].Seq+1 {
			t.Fatalf("sequence after rollback = %d, want a gap after %d", last, first[len(first)-1].Seq)
		}
	})
}
