// Package mocks holds in-process test fakes of flow-registry's boundaries:
// an in-memory store and a broker. Production wiring never uses them.
package mocks

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"

	"github.com/pandops/flowai-flow-registry/internal/store"
)

type aliasKey struct {
	team  int64
	alias string
}

type flowKey struct{ team, flow int64 }

// MemStore is a serializable in-memory store.Store: one write transaction
// runs at a time (a global lock taken by Begin), which is at least as strict
// as the row locks of the PostgreSQL store.
type MemStore struct {
	txLock sync.Mutex // held by an open write transaction
	mu     sync.Mutex // protects the data below

	counters   map[int64]int64
	flows      map[flowKey]store.Flow
	executions map[flowKey][]store.Execution
	retired    map[aliasKey]bool
	outbox     []store.OutboxRow
	sent       map[int64]bool
	seq        int64

	failCommit error
	commits    int
}

func NewMemStore() *MemStore {
	return &MemStore{
		counters:   map[int64]int64{},
		flows:      map[flowKey]store.Flow{},
		executions: map[flowKey][]store.Execution{},
		retired:    map[aliasKey]bool{},
		sent:       map[int64]bool{},
	}
}

// FailNextCommit makes the next Commit fail with err after rolling back.
func (m *MemStore) FailNextCommit(err error) {
	m.mu.Lock()
	m.failCommit = err
	m.mu.Unlock()
}

// Commits counts successful write transactions.
func (m *MemStore) Commits() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commits
}

// Executions returns every stored execution revision of one Flow.
func (m *MemStore) Executions(team, flow int64) []store.Execution {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]store.Execution(nil), m.executions[flowKey{team, flow}]...)
}

// FlowCount counts every stored Flow.
func (m *MemStore) FlowCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.flows)
}

// Outbox returns every stored outbox row, sent or not, in seq order.
func (m *MemStore) Outbox() []store.OutboxRow {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]store.OutboxRow(nil), m.outbox...)
}

// Seed stores a Flow directly (test setup), with its current execution.
func (m *MemStore) Seed(f store.Flow) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := flowKey{f.TeamID, f.FlowID}
	m.flows[k] = f.Clone()
	m.executions[k] = append(m.executions[k], f.Execution)
	if m.counters[f.TeamID] < f.FlowID {
		m.counters[f.TeamID] = f.FlowID
	}
}

func (m *MemStore) Ping(context.Context) error { return nil }

func (m *MemStore) NextFlowID(_ context.Context, team int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[team]++
	return m.counters[team], nil
}

func (m *MemStore) GetFlow(_ context.Context, team, flow int64) (store.Flow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.flows[flowKey{team, flow}]
	if !ok {
		return store.Flow{}, store.ErrNotFound
	}
	return f.Clone(), nil
}

func (m *MemStore) sortedTeamFlows(team int64, keep func(store.Flow) bool) []store.Flow {
	var out []store.Flow
	for k, f := range m.flows {
		if k.team == team && keep(f) {
			out = append(out, f.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].FlowID < out[j].FlowID
	})
	return out
}

func (m *MemStore) ListFlows(_ context.Context, team int64, filter store.ListFilter, after *store.Position, limit int) ([]store.Flow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rows := m.sortedTeamFlows(team, func(f store.Flow) bool {
		if filter.SourceID != 0 && !containsID(f.SourceIDs, filter.SourceID) {
			return false
		}
		if filter.ModelAlias != "" && !containsString(f.ModelAliases, filter.ModelAlias) {
			return false
		}
		if after != nil {
			if f.CreatedAt.Before(after.CreatedAt) || (f.CreatedAt.Equal(after.CreatedAt) && f.FlowID <= after.FlowID) {
				return false
			}
		}
		return true
	})
	if len(rows) > limit+1 {
		rows = rows[:limit+1]
	}
	return rows, nil
}

func (m *MemStore) Targets(_ context.Context, team, source int64) ([]store.Flow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rows := m.sortedTeamFlows(team, func(f store.Flow) bool { return containsID(f.SourceIDs, source) })
	sort.Slice(rows, func(i, j int) bool { return rows[i].FlowID < rows[j].FlowID })
	return rows, nil
}

func (m *MemStore) FlowsListingAlias(_ context.Context, team int64, alias string) ([]store.Flow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rows := m.sortedTeamFlows(team, func(f store.Flow) bool { return containsString(f.ModelAliases, alias) })
	sort.Slice(rows, func(i, j int) bool { return rows[i].FlowID < rows[j].FlowID })
	return rows, nil
}

func (m *MemStore) FlowExists(_ context.Context, team, flow int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.flows[flowKey{team, flow}]
	return ok, nil
}

func (m *MemStore) AdminTags(_ context.Context, filter store.TagFilter, after *store.TagPosition, limit int) ([]store.TagEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.TagEntry
	for _, f := range m.flows {
		e := store.TagEntry{TeamID: f.TeamID, FlowID: f.FlowID, Tag: f.Execution.Tag}
		if filter.TeamID != 0 && e.TeamID != filter.TeamID {
			continue
		}
		if filter.Tag != "" && e.Tag != filter.Tag {
			continue
		}
		if after != nil && !tagAfter(e, *after) {
			continue
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return tagLess(out[i], out[j]) })
	if len(out) > limit+1 {
		out = out[:limit+1]
	}
	return out, nil
}

func tagLess(a, b store.TagEntry) bool {
	if a.TeamID != b.TeamID {
		return a.TeamID < b.TeamID
	}
	if a.Tag != b.Tag {
		return a.Tag < b.Tag
	}
	return a.FlowID < b.FlowID
}

func tagAfter(e store.TagEntry, p store.TagPosition) bool {
	return tagLess(store.TagEntry{TeamID: p.TeamID, Tag: p.Tag, FlowID: p.FlowID}, e)
}

func (m *MemStore) AppendOutbox(_ context.Context, msg store.OutboxMessage) (int64, error) {
	m.txLock.Lock()
	defer m.txLock.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	seq := m.seq
	payload, err := msg.Build(seq)
	if err != nil {
		return 0, err
	}
	m.outbox = append(m.outbox, store.OutboxRow{Seq: seq, Subject: msg.Subject, Team: msg.Team, RequestID: msg.RequestID, Payload: payload})
	m.commits++
	return seq, nil
}

func (m *MemStore) PendingOutbox(_ context.Context, limit int) ([]store.OutboxRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.OutboxRow
	for _, r := range m.outbox {
		if !m.sent[r.Seq] {
			out = append(out, r)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (m *MemStore) MarkSent(_ context.Context, seq int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent[seq] = true
	return nil
}

// Begin opens the single write transaction.
func (m *MemStore) Begin(context.Context) (store.Tx, error) {
	m.txLock.Lock()
	return &memTx{m: m, flows: map[flowKey]*store.Flow{}, newExec: map[flowKey][]store.Execution{}, retired: map[aliasKey]*bool{}}, nil
}

type memTx struct {
	m       *MemStore
	done    bool
	flows   map[flowKey]*store.Flow
	newExec map[flowKey][]store.Execution
	retired map[aliasKey]*bool
	outbox  []store.OutboxRow
}

var errTxDone = errors.New("mocks: transaction already finished")

func (t *memTx) current(k flowKey) (store.Flow, bool) {
	if f, ok := t.flows[k]; ok {
		if f == nil {
			return store.Flow{}, false
		}
		return f.Clone(), true
	}
	t.m.mu.Lock()
	defer t.m.mu.Unlock()
	f, ok := t.m.flows[k]
	return f.Clone(), ok
}

func (t *memTx) allFlows(team int64) []store.Flow {
	t.m.mu.Lock()
	keys := map[flowKey]bool{}
	for k := range t.m.flows {
		if k.team == team {
			keys[k] = true
		}
	}
	t.m.mu.Unlock()
	for k := range t.flows {
		if k.team == team {
			keys[k] = true
		}
	}
	var out []store.Flow
	for k := range keys {
		if f, ok := t.current(k); ok {
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FlowID < out[j].FlowID })
	return out
}

func (t *memTx) isRetired(team int64, alias string) bool {
	k := aliasKey{team, alias}
	if v, ok := t.retired[k]; ok {
		return *v
	}
	t.m.mu.Lock()
	defer t.m.mu.Unlock()
	return t.m.retired[k]
}

func (t *memTx) LockAliases(team int64, aliases []string) (map[string]bool, error) {
	if t.done {
		return nil, errTxDone
	}
	out := map[string]bool{}
	for _, a := range aliases {
		out[a] = t.isRetired(team, a)
	}
	return out, nil
}

func (t *memTx) LockAliasExclusive(team int64, alias string) (bool, error) {
	if t.done {
		return false, errTxDone
	}
	return t.isRetired(team, alias), nil
}

func (t *memTx) SetAliasRetired(team int64, alias string, retired bool) error {
	v := retired
	t.retired[aliasKey{team, alias}] = &v
	return nil
}

func (t *memTx) LockFlow(team, flow int64) (store.Flow, error) {
	if t.done {
		return store.Flow{}, errTxDone
	}
	f, ok := t.current(flowKey{team, flow})
	if !ok {
		return store.Flow{}, store.ErrNotFound
	}
	return f, nil
}

func (t *memTx) LockFlowsListingAlias(team int64, alias string) ([]store.Flow, error) {
	var out []store.Flow
	for _, f := range t.allFlows(team) {
		if containsString(f.ModelAliases, alias) {
			out = append(out, f)
		}
	}
	return out, nil
}

func (t *memTx) NameTaken(team int64, name string, except int64) (bool, error) {
	for _, f := range t.allFlows(team) {
		if f.FlowID != except && strings.EqualFold(f.Name, name) {
			return true, nil
		}
	}
	return false, nil
}

func (t *memTx) LockSources(int64, []int64) error { return nil }

func (t *memTx) SourceBindingCount(team, source, except int64) (int, error) {
	n := 0
	for _, f := range t.allFlows(team) {
		if f.FlowID != except && containsID(f.SourceIDs, source) {
			n++
		}
	}
	return n, nil
}

func (t *memTx) InsertFlow(f store.Flow) error {
	k := flowKey{f.TeamID, f.FlowID}
	if _, ok := t.current(k); ok {
		return errors.New("mocks: duplicate flow key")
	}
	if taken, _ := t.NameTaken(f.TeamID, f.Name, f.FlowID); taken {
		return store.ErrNameConflict
	}
	c := f.Clone()
	t.flows[k] = &c
	t.newExec[k] = append(t.newExec[k], c.Execution)
	return nil
}

func (t *memTx) UpdateFlow(f store.Flow, appendExecution bool) error {
	k := flowKey{f.TeamID, f.FlowID}
	if _, ok := t.current(k); !ok {
		return store.ErrNotFound
	}
	if taken, _ := t.NameTaken(f.TeamID, f.Name, f.FlowID); taken {
		return store.ErrNameConflict
	}
	c := f.Clone()
	t.flows[k] = &c
	if appendExecution {
		t.newExec[k] = append(t.newExec[k], c.Execution)
	}
	return nil
}

func (t *memTx) AppendOutbox(msg store.OutboxMessage) (int64, error) {
	if t.done {
		return 0, errTxDone
	}
	t.m.mu.Lock()
	t.m.seq++ // a rolled-back transaction consumes its sequence value, like PostgreSQL
	seq := t.m.seq
	t.m.mu.Unlock()
	payload, err := msg.Build(seq)
	if err != nil {
		return 0, err
	}
	t.outbox = append(t.outbox, store.OutboxRow{Seq: seq, Subject: msg.Subject, Team: msg.Team, RequestID: msg.RequestID, Payload: payload})
	return seq, nil
}

func (t *memTx) Commit() error {
	if t.done {
		return errTxDone
	}
	t.done = true
	defer t.m.txLock.Unlock()
	t.m.mu.Lock()
	defer t.m.mu.Unlock()
	if err := t.m.failCommit; err != nil {
		t.m.failCommit = nil
		return err
	}
	for k, f := range t.flows {
		if f != nil {
			t.m.flows[k] = f.Clone()
		}
	}
	for k, ex := range t.newExec {
		t.m.executions[k] = append(t.m.executions[k], ex...)
	}
	for k, v := range t.retired {
		if *v {
			t.m.retired[k] = true
		} else {
			delete(t.m.retired, k)
		}
	}
	t.m.outbox = append(t.m.outbox, t.outbox...)
	sort.Slice(t.m.outbox, func(i, j int) bool { return t.m.outbox[i].Seq < t.m.outbox[j].Seq })
	if len(t.flows) > 0 || len(t.retired) > 0 || len(t.outbox) > 0 {
		t.m.commits++
	}
	return nil
}

func (t *memTx) Rollback() error {
	if t.done {
		return nil
	}
	t.done = true
	t.m.txLock.Unlock()
	return nil
}

func containsID(ids []int64, id int64) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
