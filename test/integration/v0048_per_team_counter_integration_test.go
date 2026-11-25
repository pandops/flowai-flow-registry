//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pandops/flowai-flow-registry/internal/store"
)

// Integration: per-team Flow numbers under concurrent creates in one team are
// unique and increasing; gaps after a rollback are allowed, no number is
// reused, and the first Flow of a new team is 1.

func sampleFlow(team, id int64, name string) store.Flow {
	now := time.Date(2026, 11, 8, 9, 0, 0, 0, time.UTC)
	return store.Flow{
		TeamID: team, FlowID: id, Name: name, ModelAliases: []string{fmt.Sprintf("%d/gpt-4o", team)}, Revision: 1,
		Execution: store.Execution{Revision: 1, Tag: "openhands", PromptTemplate: "Solve {{external_id}}", Parameters: json.RawMessage(`{}`),
			FailureRetryCount: 3, FailureRetryWaitSeconds: 60, PendingLimitSeconds: 900, ReleaseDelaySeconds: 300, CreatedAt: now},
		CreatedAt: now, UpdatedAt: now,
	}
}

func TestV0048FlowRegistryPerTeamCounter(t *testing.T) {
	ctx := context.Background()
	_, st := migratedStore(t)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var committed, allocated []int64
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := st.NextFlowID(ctx, 3)
			if err != nil {
				errs <- err
				return
			}
			tx, err := st.Begin(ctx)
			if err != nil {
				errs <- err
				return
			}
			if err := tx.InsertFlow(sampleFlow(3, id, fmt.Sprintf("f%02d", i))); err != nil {
				_ = tx.Rollback()
				errs <- err
				return
			}
			mu.Lock()
			allocated = append(allocated, id)
			mu.Unlock()
			if i%4 == 0 {
				errs <- tx.Rollback()
				return
			}
			if err := tx.Commit(); err != nil {
				errs <- err
				return
			}
			mu.Lock()
			committed = append(committed, id)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent create: %v", err)
		}
	}
	seen := map[int64]bool{}
	for _, id := range allocated {
		if seen[id] {
			t.Fatalf("number %d allocated twice: %v", id, allocated)
		}
		seen[id] = true
	}
	if len(committed) != 15 {
		t.Fatalf("committed %d, want 15", len(committed))
	}
	sort.Slice(allocated, func(i, j int) bool { return allocated[i] < allocated[j] })
	if allocated[0] != 1 || allocated[len(allocated)-1] != 20 {
		t.Fatalf("allocated = %v, want 1..20", allocated)
	}
	next, err := st.NextFlowID(ctx, 3)
	if err != nil || next != 21 {
		t.Fatalf("next after rollbacks = %d, %v; want 21 (no reuse)", next, err)
	}
	first, err := st.NextFlowID(ctx, 4)
	if err != nil || first != 1 {
		t.Fatalf("first number of team 4 = %d, %v", first, err)
	}
	for _, id := range committed {
		if f, err := st.GetFlow(ctx, 3, id); err != nil || f.FlowID != id {
			t.Fatalf("GetFlow(3,%d) = %+v, %v", id, f, err)
		}
	}
	if _, err := st.GetFlow(ctx, 4, committed[0]); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("team 4 sees team 3's Flow: %v", err)
	}
}
