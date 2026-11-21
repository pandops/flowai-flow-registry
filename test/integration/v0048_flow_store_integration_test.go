//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pandops/flowai-flow-registry/internal/store"
)

// Integration: what the in-memory fake cannot prove about the PostgreSQL
// store: unique case-insensitive names per team, immutable execution
// revisions bound to the Flow's team, binding counts, row locks that
// serialize a Flow write against alias retirement, and keyset ordering.

func insert(t *testing.T, st *store.Postgres, f store.Flow) {
	t.Helper()
	tx, err := st.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.InsertFlow(f); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert %+v: %v", f, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestV0048FlowRegistryPostgresStore(t *testing.T) {
	ctx := context.Background()

	t.Run("Name is unique per team ignoring case", func(t *testing.T) {
		_, st := migratedStore(t)
		insert(t, st, sampleFlow(3, 1, "Intake"))
		insert(t, st, sampleFlow(4, 1, "intake"))
		tx, _ := st.Begin(ctx)
		defer tx.Rollback()
		taken, err := tx.NameTaken(3, "INTAKE", 0)
		if err != nil || !taken {
			t.Fatalf("NameTaken = %v, %v", taken, err)
		}
		if taken, _ := tx.NameTaken(3, "intake", 1); taken {
			t.Fatalf("own name counted as taken")
		}
		err = tx.InsertFlow(sampleFlow(3, 2, "iNtAkE"))
		if !errors.Is(err, store.ErrNameConflict) {
			t.Fatalf("duplicate insert err = %v, want ErrNameConflict", err)
		}
	})

	t.Run("Execution revisions are immutable and keep the Flow's team", func(t *testing.T) {
		e, st := migratedStore(t)
		f := sampleFlow(3, 1, "a")
		f.SourceIDs = []int64{12, 13}
		insert(t, st, f)
		tx, _ := st.Begin(ctx)
		cur, err := tx.LockFlow(3, 1)
		if err != nil {
			t.Fatal(err)
		}
		cur.Revision = 2
		cur.Execution.Revision = 2
		cur.Execution.PromptTemplate = "Fix {{external_id}}"
		cur.SourceIDs = []int64{13}
		if err := tx.UpdateFlow(cur, true); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		got, _ := st.GetFlow(ctx, 3, 1)
		if got.Execution.Revision != 2 || got.Execution.PromptTemplate != "Fix {{external_id}}" || len(got.SourceIDs) != 1 || got.SourceIDs[0] != 13 {
			t.Fatalf("Flow = %+v", got)
		}
		targets, _ := st.Targets(ctx, 3, 12)
		if len(targets) != 0 {
			t.Fatalf("unbound Source still targets the Flow")
		}
		// The database refuses to change or reparent a revision.
		for _, q := range []string{
			"UPDATE flow_execution_revisions SET prompt_template='x'",
			"UPDATE flow_execution_revisions SET team_id=4",
			"DELETE FROM flow_execution_revisions",
		} {
			if _, err := e.svc.ExecContext(ctx, q); err == nil {
				t.Fatalf("%s succeeded", q)
			}
		}
		var count int
		_ = e.svc.QueryRowContext(ctx, "SELECT count(*) FROM flow_execution_revisions WHERE team_id=3 AND flow_id=1").Scan(&count)
		if count != 2 {
			t.Fatalf("revisions = %d, want 2", count)
		}
		// A revision cannot reference a Flow of another team.
		if _, err := e.svc.ExecContext(ctx, `INSERT INTO flow_execution_revisions (team_id, flow_id, revision, execution_tag, prompt_template, execution_parameters,
			failure_retry_count, failure_retry_wait_seconds, pending_limit_seconds, release_delay_seconds, created_at)
			VALUES (4, 1, 3, 'gpu', 'x', '{}', 3, 60, 900, 300, now())`); err == nil {
			t.Fatalf("revision attached to a Flow team 4 does not own")
		}
	})

	t.Run("Binding count per Source", func(t *testing.T) {
		_, st := migratedStore(t)
		for i := int64(1); i <= 3; i++ {
			f := sampleFlow(3, i, "f"+string(rune('a'+i)))
			f.SourceIDs = []int64{12}
			insert(t, st, f)
		}
		tx, _ := st.Begin(ctx)
		defer tx.Rollback()
		if err := tx.LockSources(3, []int64{12, 13}); err != nil {
			t.Fatal(err)
		}
		if n, _ := tx.SourceBindingCount(3, 12, 0); n != 3 {
			t.Fatalf("count = %d", n)
		}
		if n, _ := tx.SourceBindingCount(3, 12, 2); n != 2 {
			t.Fatalf("count except Flow 2 = %d", n)
		}
		if n, _ := tx.SourceBindingCount(4, 12, 0); n != 0 {
			t.Fatalf("team 4 count = %d", n)
		}
	})

	t.Run("Alias guard lock orders a write against retirement", func(t *testing.T) {
		_, st := migratedStore(t)
		f := sampleFlow(3, 1, "a")
		f.ModelAliases = []string{"3/gpt-4o", "3/m1"}
		insert(t, st, f)
		writer, _ := st.Begin(ctx)
		retired, err := writer.LockAliases(3, []string{"3/m1"})
		if err != nil || retired["3/m1"] {
			t.Fatalf("LockAliases = %v, %v", retired, err)
		}
		done := make(chan time.Time, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, _ := st.Begin(ctx)
			if _, err := r.LockAliasExclusive(3, "3/m1"); err != nil {
				t.Errorf("LockAliasExclusive: %v", err)
			}
			done <- time.Now()
			_ = r.SetAliasRetired(3, "3/m1", true)
			_ = r.Commit()
		}()
		time.Sleep(300 * time.Millisecond)
		select {
		case <-done:
			t.Fatalf("retirement locked the alias while a write held it")
		default:
		}
		released := time.Now()
		_ = writer.Commit()
		wg.Wait()
		if at := <-done; at.Before(released) {
			t.Fatalf("retirement acquired the lock before the write committed")
		}
		tx, _ := st.Begin(ctx)
		defer tx.Rollback()
		if r, _ := tx.LockAliases(3, []string{"3/m1"}); !r["3/m1"] {
			t.Fatalf("alias not recorded as retired")
		}
	})

	t.Run("Lists and admin tags follow keyset order", func(t *testing.T) {
		_, st := migratedStore(t)
		for i := int64(1); i <= 5; i++ {
			f := sampleFlow(3, i, "f"+string(rune('a'+i)))
			f.CreatedAt = f.CreatedAt.Add(time.Duration(i) * time.Second)
			if i%2 == 0 {
				f.Execution.Tag = "gpu"
			}
			insert(t, st, f)
		}
		insert(t, st, sampleFlow(4, 1, "z"))
		page, err := st.ListFlows(ctx, 3, store.ListFilter{}, nil, 2)
		// ListFlows returns limit+1 rows so the caller can tell a next page exists.
		if err != nil || len(page) != 3 || page[0].FlowID != 1 || page[1].FlowID != 2 {
			t.Fatalf("first page = %+v, %v", page, err)
		}
		rest, _ := st.ListFlows(ctx, 3, store.ListFilter{}, &store.Position{CreatedAt: page[1].CreatedAt, FlowID: page[1].FlowID}, 10)
		if len(rest) != 3 || rest[0].FlowID != 3 {
			t.Fatalf("rest = %+v", rest)
		}
		tags, _ := st.AdminTags(ctx, store.TagFilter{}, nil, 10)
		var got []string
		for _, e := range tags {
			b, _ := json.Marshal([]any{e.TeamID, e.Tag, e.FlowID})
			got = append(got, string(b))
		}
		want := `[3,"gpu",2] [3,"gpu",4] [3,"openhands",1] [3,"openhands",3] [3,"openhands",5] [4,"openhands",1]`
		if flowsIn(got...) != flowsIn(strings.Fields(want)...) {
			t.Fatalf("admin tags = %v", got)
		}
		after, _ := st.AdminTags(ctx, store.TagFilter{Tag: "openhands"}, &store.TagPosition{TeamID: 3, Tag: "openhands", FlowID: 3}, 10)
		if len(after) != 2 || after[0].FlowID != 5 || after[1].TeamID != 4 {
			t.Fatalf("tags after position = %+v", after)
		}
	})
}
