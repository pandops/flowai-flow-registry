//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/pandops/flowai-flow-registry/internal/store"
)

// Integration: publisher_seq comes from flow-registry's own sequence in the
// committing transaction; a rolled-back transaction publishes nothing, and a
// restart continues the sequence, so a new message never reuses a Nats-Msg-Id.

func auditMsg(n int) store.OutboxMessage {
	return store.OutboxMessage{Subject: "flow-registry.audit.flow-get.pub", Team: "3", RequestID: int64(n),
		Build: func(seq int64) ([]byte, error) { return []byte(fmt.Sprintf(`{"publisher_seq":%d}`, seq)), nil }}
}

func TestV0048FlowRegistryPublisherSeq(t *testing.T) {
	ctx := context.Background()
	e, st := migratedStore(t)

	tx, _ := st.Begin(ctx)
	s1, err := tx.AppendOutbox(auditMsg(1))
	if err != nil || tx.Commit() != nil {
		t.Fatalf("append 1: %v", err)
	}
	tx, _ = st.Begin(ctx)
	s2, err := tx.AppendOutbox(auditMsg(2))
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	s3, err := st.AppendOutbox(ctx, auditMsg(3))
	if err != nil {
		t.Fatal(err)
	}
	if s1 >= s2 || s2 >= s3 {
		t.Fatalf("sequence %d %d %d is not increasing", s1, s2, s3)
	}
	rows, err := st.PendingOutbox(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Seq != s1 || rows[1].Seq != s3 {
		t.Fatalf("pending = %+v, want seqs %d and %d (rolled-back %d absent)", rows, s1, s3, s2)
	}
	if string(rows[1].Payload) != fmt.Sprintf(`{"publisher_seq":%d}`, s3) || rows[1].Team != "3" || rows[1].RequestID != 3 {
		t.Fatalf("row = %+v", rows[1])
	}
	if err := st.MarkSent(ctx, s1); err != nil {
		t.Fatal(err)
	}
	// Restart: a new connection pool and store continue the sequence.
	restarted := store.NewPostgres(e.open(t))
	s4, err := restarted.AppendOutbox(ctx, auditMsg(4))
	if err != nil || s4 <= s3 {
		t.Fatalf("after restart seq %d, %v; want > %d", s4, err, s3)
	}
	rows, _ = restarted.PendingOutbox(ctx, 100)
	if len(rows) != 2 || rows[0].Seq != s3 || rows[1].Seq != s4 {
		t.Fatalf("pending after restart = %+v", rows)
	}
	// The sequence lives in flow_registry_schema.
	var n int
	if err := e.admin.QueryRowContext(ctx, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		WHERE c.relkind='S' AND n.nspname='flow_registry_schema'`).Scan(&n); err != nil || n == 0 {
		t.Fatalf("no sequence in flow_registry_schema: %d %v", n, err)
	}
}
