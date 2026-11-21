// Package store holds flow-registry's durable data: Flows, their immutable
// execution revisions, flow-owned Source bindings, retired model aliases and
// the outbox. Everything lives in the service's own schema
// (default flow_registry_schema); nothing reads another service's schema.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound     = errors.New("store: not found")
	ErrNameConflict = errors.New("store: flow name already used in the team")
)

// Execution is one immutable Flow execution revision.
type Execution struct {
	Revision                int
	Tag                     string
	PromptTemplate          string
	Parameters              json.RawMessage // canonical JSON object, "{}" when empty
	DefaultImage            json.RawMessage // nil is the explicit absence marker
	FailureRetryCount       int
	FailureRetryWaitSeconds int
	PendingLimitSeconds     int
	ReleaseDelaySeconds     int
	CreatedAt               time.Time
}

// SameContent reports whether two revisions carry the same execution content.
func (e Execution) SameContent(o Execution) bool {
	return e.Tag == o.Tag && e.PromptTemplate == o.PromptTemplate &&
		string(e.Parameters) == string(o.Parameters) && string(e.DefaultImage) == string(o.DefaultImage) &&
		e.FailureRetryCount == o.FailureRetryCount && e.FailureRetryWaitSeconds == o.FailureRetryWaitSeconds &&
		e.PendingLimitSeconds == o.PendingLimitSeconds && e.ReleaseDelaySeconds == o.ReleaseDelaySeconds
}

// Flow is one team Flow with its current execution revision and bindings.
type Flow struct {
	TeamID       int64
	FlowID       int64
	Name         string
	ModelAliases []string
	PerTaskLimit json.RawMessage // nil is null
	FlowLimit    json.RawMessage // nil is null
	Revision     int             // shared Flow / LLM settings revision
	Execution    Execution       // current execution revision
	SourceIDs    []int64         // ascending
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Clone returns a deep copy so callers never alias stored slices.
func (f Flow) Clone() Flow {
	c := f
	c.ModelAliases = append([]string(nil), f.ModelAliases...)
	c.SourceIDs = append([]int64(nil), f.SourceIDs...)
	c.PerTaskLimit = cloneRaw(f.PerTaskLimit)
	c.FlowLimit = cloneRaw(f.FlowLimit)
	c.Execution.Parameters = cloneRaw(f.Execution.Parameters)
	c.Execution.DefaultImage = cloneRaw(f.Execution.DefaultImage)
	return c
}

func cloneRaw(r json.RawMessage) json.RawMessage {
	if r == nil {
		return nil
	}
	return append(json.RawMessage(nil), r...)
}

// Position is the (created_at, flow_id) continuation of a Flow list.
type Position struct {
	CreatedAt time.Time
	FlowID    int64
}

// ListFilter narrows a team's Flow list; zero values mean no filter.
type ListFilter struct {
	SourceID   int64
	ModelAlias string
}

// TagEntry is one entry of the administrator tag projection.
type TagEntry struct {
	TeamID int64
	FlowID int64
	Tag    string
}

// TagFilter narrows the tag projection; zero values mean no filter.
type TagFilter struct {
	TeamID int64
	Tag    string
}

// TagPosition is the (team_id, execution_tag, flow_id) continuation.
type TagPosition struct {
	TeamID int64
	Tag    string
	FlowID int64
}

// OutboxMessage is one stream message to store with the change that caused
// it. Build receives the publisher_seq allocated in the same transaction.
type OutboxMessage struct {
	Subject   string
	Team      string // team header value: decimal team_id or "admin"
	RequestID int64  // 0 when no human request caused it
	Build     func(publisherSeq int64) ([]byte, error)
}

// OutboxRow is one stored, not yet acknowledged stream message.
type OutboxRow struct {
	Seq       int64
	Subject   string
	Team      string
	RequestID int64
	Payload   []byte
}

// Store is flow-registry's persistence boundary.
type Store interface {
	Ping(ctx context.Context) error
	// Begin opens a write transaction. Lock methods on Tx hold their row
	// locks until Commit or Rollback.
	Begin(ctx context.Context) (Tx, error)
	// NextFlowID allocates the next per-team Flow number in its own short
	// transaction: the first Flow of a team is 1, gaps are allowed.
	NextFlowID(ctx context.Context, teamID int64) (int64, error)

	GetFlow(ctx context.Context, teamID, flowID int64) (Flow, error)
	// ListFlows returns up to limit+1 Flows ordered by (created_at, flow_id).
	ListFlows(ctx context.Context, teamID int64, filter ListFilter, after *Position, limit int) ([]Flow, error)
	// Targets returns the team's Flows bound to the Source, by flow_id.
	Targets(ctx context.Context, teamID, sourceID int64) ([]Flow, error)
	// FlowsListingAlias returns the team's Flows whose model list contains the alias.
	FlowsListingAlias(ctx context.Context, teamID int64, alias string) ([]Flow, error)
	FlowExists(ctx context.Context, teamID, flowID int64) (bool, error)
	// AdminTags returns up to limit+1 entries ordered by (team_id, tag, flow_id).
	AdminTags(ctx context.Context, filter TagFilter, after *TagPosition, limit int) ([]TagEntry, error)

	// AppendOutbox stores one message in its own transaction (audits of reads).
	AppendOutbox(ctx context.Context, msg OutboxMessage) (int64, error)
	PendingOutbox(ctx context.Context, limit int) ([]OutboxRow, error)
	MarkSent(ctx context.Context, seq int64) error
}

// Tx is one write transaction.
type Tx interface {
	// LockAliases takes shared locks on the aliases' guard rows in sorted
	// order and reports which are retired.
	LockAliases(teamID int64, aliases []string) (map[string]bool, error)
	// LockAliasExclusive takes the exclusive guard lock and reports whether
	// the alias is already retired.
	LockAliasExclusive(teamID int64, alias string) (bool, error)
	SetAliasRetired(teamID int64, alias string, retired bool) error
	// LockFlow locks one Flow row (ErrNotFound when absent in the team).
	LockFlow(teamID, flowID int64) (Flow, error)
	// LockFlowsListingAlias locks, in flow_id order, every Flow listing the alias.
	LockFlowsListingAlias(teamID int64, alias string) ([]Flow, error)
	NameTaken(teamID int64, name string, exceptFlowID int64) (bool, error)
	// LockSources locks the binding guard rows of the Sources in sorted order.
	LockSources(teamID int64, sourceIDs []int64) error
	// SourceBindingCount counts the Flows bound to the Source, except one.
	SourceBindingCount(teamID, sourceID, exceptFlowID int64) (int, error)
	// InsertFlow stores a new Flow with execution revision 1 and its bindings.
	InsertFlow(f Flow) error
	// UpdateFlow stores the Flow row and bindings; with appendExecution it
	// also appends f.Execution as a new immutable revision.
	UpdateFlow(f Flow, appendExecution bool) error
	AppendOutbox(msg OutboxMessage) (int64, error)
	Commit() error
	Rollback() error
}
