package flows

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/pandops/flowai-flow-registry/internal/store"
)

type serviceParams struct {
	TeamID     int64  `json:"team_id"`
	FlowID     int64  `json:"flow_id"`
	SourceID   int64  `json:"source_id"`
	Alias      string `json:"alias"`
	OperatorID int64  `json:"operator_id"`
}

// serveService answers the service operations. A decimal team header must
// equal params.team_id; team: admin trusts params.team_id.
func (s *Service) serveService(ctx context.Context, r *request) (*result, error) {
	var p serviceParams
	if err := json.Unmarshal(orEmpty(r.params), &p); err != nil || p.TeamID < 1 {
		return nil, invalid("params.team_id is required")
	}
	if r.teamID != 0 && r.teamID != p.TeamID {
		return nil, invalid("params.team_id differs from the team header")
	}
	r.teamID = p.TeamID
	switch r.op {
	case "targets":
		return s.targets(ctx, p)
	case "llm-settings":
		f, err := s.o.Store.GetFlow(ctx, p.TeamID, p.FlowID)
		if err != nil {
			return nil, storeErr(err)
		}
		return &result{status: 200, body: map[string]any{
			"model_aliases": nonNil(f.ModelAliases), "per_task_limit": rawOrNull(f.PerTaskLimit),
			"flow_limit": rawOrNull(f.FlowLimit), "revision": f.Revision,
		}}, nil
	case "alias-usage":
		rows, err := s.o.Store.FlowsListingAlias(ctx, p.TeamID, p.Alias)
		if err != nil {
			return nil, storeErr(err)
		}
		all, sole := refs(rows, p.Alias)
		return &result{status: 200, body: map[string]any{"flows": all, "sole_alias_flows": sole}}, nil
	case "retire-alias":
		return s.retireAlias(ctx, r, p)
	case "restore-alias":
		return s.restoreAlias(ctx, p)
	case "flow-owner":
		ok, err := s.o.Store.FlowExists(ctx, p.TeamID, p.FlowID)
		if err != nil {
			return nil, storeErr(err)
		}
		if !ok {
			return nil, fail(404, codeFlowNotFound, "flow not found")
		}
		return &result{status: 200, body: map[string]any{"team_id": p.TeamID}}, nil
	}
	return nil, errNotFound
}

func (s *Service) targets(ctx context.Context, p serviceParams) (*result, error) {
	rows, err := s.o.Store.Targets(ctx, p.TeamID, p.SourceID)
	if err != nil {
		return nil, storeErr(err)
	}
	if len(rows) == 0 {
		return nil, errNotFound
	}
	type target struct {
		FlowID            int64         `json:"flow_id"`
		ExecutionRevision executionView `json:"execution_revision"`
	}
	out := make([]target, 0, len(rows))
	for _, f := range rows {
		out = append(out, target{FlowID: f.FlowID, ExecutionRevision: executionOf(f.Execution)})
	}
	return &result{status: 200, body: map[string]any{"team_id": p.TeamID, "source_id": p.SourceID, "flows": out}}, nil
}

// refs lists the Flows and those whose only alias is the given one.
func refs(rows []store.Flow, alias string) (all, sole []flowRef) {
	all, sole = []flowRef{}, []flowRef{}
	for _, f := range rows {
		ref := flowRef{FlowID: f.FlowID, Name: f.Name}
		all = append(all, ref)
		if len(f.ModelAliases) == 1 && f.ModelAliases[0] == alias {
			sole = append(sole, ref)
		}
	}
	return all, sole
}

// retireAlias removes an archived alias from every team Flow, unless a Flow
// lists only that alias.
func (s *Service) retireAlias(ctx context.Context, r *request, p serviceParams) (*result, error) {
	tx, err := s.o.Store.Begin(ctx)
	if err != nil {
		return nil, storeErr(err)
	}
	defer func() { _ = tx.Rollback() }()
	already, err := tx.LockAliasExclusive(p.TeamID, p.Alias)
	if err != nil {
		return nil, storeErr(err)
	}
	if already {
		return &result{status: 204}, nil
	}
	rows, err := tx.LockFlowsListingAlias(p.TeamID, p.Alias)
	if err != nil {
		return nil, storeErr(err)
	}
	if _, sole := refs(rows, p.Alias); len(sole) > 0 {
		e := fail(409, codeModelLastInFlow, "the model is the only model of a Flow")
		e.flows = sole
		return nil, e
	}
	if err := tx.SetAliasRetired(p.TeamID, p.Alias, true); err != nil {
		return nil, storeErr(err)
	}
	now := s.o.Now()
	for _, f := range rows {
		f.ModelAliases = slices.DeleteFunc(f.ModelAliases, func(a string) bool { return a == p.Alias })
		f.Revision++
		f.UpdatedAt = now
		if err := tx.UpdateFlow(f, false); err != nil {
			return nil, storeErr(err)
		}
		id := f.FlowID
		msg := s.auditMessage(r, "retire-alias", p.OperatorID, &id, nil, map[string]any{"removed_model_alias": p.Alias, "revision": f.Revision})
		if _, err := tx.AppendOutbox(msg); err != nil {
			return nil, storeErr(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, storeErr(err)
	}
	return &result{status: 204}, nil
}

func (s *Service) restoreAlias(ctx context.Context, p serviceParams) (*result, error) {
	tx, err := s.o.Store.Begin(ctx)
	if err != nil {
		return nil, storeErr(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.LockAliasExclusive(p.TeamID, p.Alias); err != nil {
		return nil, storeErr(err)
	}
	if err := tx.SetAliasRetired(p.TeamID, p.Alias, false); err != nil {
		return nil, storeErr(err)
	}
	if err := tx.Commit(); err != nil {
		return nil, storeErr(err)
	}
	return &result{status: 204}, nil
}

type tagParams struct {
	TeamID *int64  `json:"team_id"`
	Tag    *string `json:"tag"`
	Limit  *int    `json:"limit"`
}

type tagPosition struct {
	TeamID       int64  `json:"team_id"`
	ExecutionTag string `json:"execution_tag"`
	FlowID       int64  `json:"flow_id"`
}

// adminTags serves the global (team_id, execution_tag, flow_id) projection.
func (s *Service) adminTags(ctx context.Context, r *request) (*result, error) {
	var p tagParams
	_ = json.Unmarshal(orEmpty(r.params), &p)
	var b struct {
		After *tagPosition `json:"after"`
	}
	_ = json.Unmarshal(orEmpty(r.body), &b)
	filter := store.TagFilter{}
	changes := map[string]any{}
	if p.TeamID != nil {
		filter.TeamID = *p.TeamID
		changes["team_id"] = *p.TeamID
	}
	if p.Tag != nil {
		filter.Tag = *p.Tag
		changes["execution_tag"] = *p.Tag
	}
	limit := defaultListLimit
	if p.Limit != nil {
		limit = *p.Limit
	}
	var after *store.TagPosition
	if b.After != nil {
		after = &store.TagPosition{TeamID: b.After.TeamID, Tag: b.After.ExecutionTag, FlowID: b.After.FlowID}
	}
	rows, err := s.o.Store.AdminTags(ctx, filter, after, limit)
	if err != nil {
		return nil, storeErr(err)
	}
	var next *tagPosition
	if len(rows) > limit {
		rows = rows[:limit]
		last := rows[len(rows)-1]
		next = &tagPosition{TeamID: last.TeamID, ExecutionTag: last.Tag, FlowID: last.FlowID}
	}
	items := make([]tagPosition, 0, len(rows))
	for _, e := range rows {
		items = append(items, tagPosition{TeamID: e.TeamID, ExecutionTag: e.Tag, FlowID: e.FlowID})
	}
	return &result{status: 200, body: map[string]any{"items": items, "next": next}, changes: changes}, nil
}
