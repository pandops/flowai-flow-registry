package flows

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/pandops/flowai-flow-registry/internal/broker"
	"github.com/pandops/flowai-flow-registry/internal/store"
)

const (
	subjTeamTags       = "executor-registry.service.team-tags.pub"
	subjValidateModels = "model-registry.service.validate-models.pub"
	subjApplyFlowTeam  = "model-registry.service.apply-flow-team.pub"
	subjRemoveFlowTeam = "model-registry.service.remove-flow-team.pub"
	subjSourcesLookup  = "source-controller.service.sources-lookup.pub"
	subjBindingGuard   = "source-controller.service.binding-guard.pub"
)

// writeFlow serves flow-create, flow-replace and llm-settings-replace.
//
// Order: shape checks (no external call), number allocation, then in one
// transaction: alias guards, Flow lock and If-Match, name, retired aliases,
// tag, Source bindings, model validation, the LiteLLM team, the row and its
// audit message.
func (s *Service) writeFlow(ctx context.Context, r *request, settingsOnly bool) (*result, error) {
	in, err := parseFlowInput(r.body)
	if err != nil {
		return nil, invalid("body is not valid")
	}
	create := r.op == "flow-create"
	var flowID int64
	var expected int
	if !create {
		flowID = flowIDParam(r)
		v, ok := r.header[broker.HeaderIfMatch]
		if !ok {
			return nil, invalid("If-Match header is required")
		}
		if expected, ok = parseIfMatch(v); !ok {
			return nil, invalid("If-Match header is malformed")
		}
	}
	if create {
		if flowID, err = s.o.Store.NextFlowID(ctx, r.teamID); err != nil {
			return nil, storeErr(err)
		}
	}
	tx, err := s.o.Store.Begin(ctx)
	if err != nil {
		return nil, storeErr(err)
	}
	done := false
	defer func() {
		if !done {
			_ = tx.Rollback()
		}
	}()
	retired, err := tx.LockAliases(r.teamID, sortedCopy(in.aliases))
	if err != nil {
		return nil, storeErr(err)
	}
	var old *store.Flow
	if !create {
		o, err := tx.LockFlow(r.teamID, flowID)
		if err != nil {
			return nil, storeErr(err)
		}
		if o.Revision != expected {
			return nil, fail(412, codeStale, "the Flow changed since it was read")
		}
		old = &o
	}
	rid := flowID
	f := merge(old, in, r.teamID, flowID)
	if old == nil || f.Name != old.Name {
		taken, err := tx.NameTaken(r.teamID, f.Name, flowID)
		if err != nil {
			return nil, storeErr(err)
		}
		if taken {
			return nil, fail(409, codeNameConflict, "a Flow with this name exists")
		}
	}
	for _, a := range in.aliases {
		if retired[a] {
			return nil, invalid("a selected model is archived")
		}
	}
	if !settingsOnly && (old == nil || f.Execution.Tag != old.Execution.Tag) {
		if err := s.checkTag(ctx, r, f.Execution.Tag); err != nil {
			return nil, err
		}
	}
	if !settingsOnly {
		if err := s.bindSources(ctx, r, tx, old, &f, in); err != nil {
			return nil, err
		}
	}
	if err := s.validateModels(ctx, r, f.ModelAliases); err != nil {
		return nil, err
	}
	now := s.o.Now()
	appendExec := old == nil || !f.Execution.SameContent(old.Execution)
	if old == nil {
		f.Revision, f.Execution.Revision, f.CreatedAt = 1, 1, now
	} else {
		f.Revision = old.Revision + 1
		if appendExec {
			f.Execution.Revision = old.Execution.Revision + 1
		}
	}
	if appendExec {
		f.Execution.CreatedAt = now
	}
	f.UpdatedAt = now
	if err := s.applyTeam(ctx, r.teamID, r.requestID, f.FlowID, f.Revision, f.FlowLimit); err != nil {
		return nil, err
	}
	// From here on model-registry holds the new record: any failure restores
	// the committed one in the background, or, for a create, removes the
	// new Flow's LiteLLM team (deviation D17).
	fail5xx := func(err error) (*result, error) {
		if !done {
			_ = tx.Rollback()
			done = true
		}
		if old != nil {
			s.restore(r.teamID, f.FlowID, r.requestID)
		} else {
			s.removeOrphan(r.teamID, f.FlowID, r.requestID)
		}
		var e *apiError
		if errors.As(err, &e) {
			return nil, e
		}
		s.o.Logger.Error("flow write failed after apply-flow-team", "team_id", r.teamID, "flow_id", f.FlowID, "error", err)
		return nil, fail(500, codeInternal, "the change could not be stored")
	}
	if old == nil {
		err = tx.InsertFlow(f)
	} else {
		err = tx.UpdateFlow(f, appendExec)
	}
	if errors.Is(err, store.ErrNameConflict) {
		return fail5xx(fail(409, codeNameConflict, "a Flow with this name exists"))
	}
	if err != nil {
		return fail5xx(err)
	}
	if _, err := tx.AppendOutbox(s.auditMessage(r, r.op, r.operatorID, &rid, nil, map[string]any{"revision": f.Revision})); err != nil {
		return fail5xx(err)
	}
	done = true
	if err := tx.Commit(); err != nil {
		return fail5xx(err)
	}
	status := 200
	if create {
		status = 201
	}
	var body any = viewOf(f)
	if settingsOnly {
		body = s.settingsOf(ctx, r, []store.Flow{f})[0]
	}
	return &result{status: status, body: body, etag: f.Revision, resourceID: &rid, audited: true}, nil
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	slices.Sort(out)
	return slices.Compact(out)
}

func (s *Service) checkTag(ctx context.Context, r *request, tag string) error {
	status, body, aerr := s.call(ctx, subjTeamTags, r.teamID, r.requestID,
		map[string]any{"params": map[string]any{"team_id": r.teamID}}, codeServiceDown)
	if aerr != nil {
		return aerr
	}
	var reply struct {
		Tags []string `json:"tags"`
	}
	if status != 200 || json.Unmarshal(body, &reply) != nil {
		return fail(503, codeServiceDown, "executor-registry did not answer")
	}
	if !slices.Contains(reply.Tags, tag) {
		return invalid("execution_tag is not served for this team")
	}
	return nil
}

// bindSources checks and guards the Flow's Source bindings.
func (s *Service) bindSources(ctx context.Context, r *request, tx store.Tx, old *store.Flow, f *store.Flow, in flowInput) error {
	if !in.hasSourceIDs {
		if old == nil && in.hasSourceRevisions && len(in.sourceRevisions) > 0 {
			return invalid("source_revisions must match the bound Sources")
		}
		return nil // replacement keeps its bindings
	}
	var oldIDs []int64
	if old != nil {
		oldIDs = old.SourceIDs
	}
	union := append(append([]int64(nil), oldIDs...), in.sourceIDs...)
	slices.Sort(union)
	union = slices.Compact(union)
	if len(in.sourceRevisions) != len(union) {
		return invalid("source_revisions must name exactly the old and new Sources")
	}
	for _, id := range union {
		if _, ok := in.sourceRevisions[id]; !ok {
			return invalid("source_revisions must name exactly the old and new Sources")
		}
	}
	if len(union) > maxBindingSourceCount {
		return invalid("too many Sources")
	}
	if err := tx.LockSources(r.teamID, union); err != nil {
		return storeErr(err)
	}
	var added []int64
	for _, id := range in.sourceIDs {
		if !slices.Contains(oldIDs, id) {
			added = append(added, id)
		}
	}
	for _, id := range added {
		n, err := tx.SourceBindingCount(r.teamID, id, f.FlowID)
		if err != nil {
			return storeErr(err)
		}
		if n >= maxFlowsPerSource {
			return invalid("a Source is bound to the maximum number of Flows")
		}
	}
	if len(added) > 0 {
		status, body, aerr := s.call(ctx, subjSourcesLookup, r.teamID, r.requestID,
			map[string]any{"body": map[string]any{"team_id": r.teamID, "source_ids": added}}, codeServiceDown)
		if aerr != nil {
			return aerr
		}
		var reply struct {
			Sources []struct {
				SourceID int64 `json:"source_id"`
			} `json:"sources"`
		}
		if status != 200 || json.Unmarshal(body, &reply) != nil {
			return fail(503, codeServiceDown, "source-controller did not answer")
		}
		found := map[int64]bool{}
		for _, src := range reply.Sources {
			found[src.SourceID] = true
		}
		for _, id := range added {
			if !found[id] {
				return invalid("a selected Source does not exist")
			}
		}
	}
	if len(union) > 0 {
		type guard struct {
			SourceID         int64 `json:"source_id"`
			ExpectedRevision int   `json:"expected_revision"`
		}
		list := make([]guard, 0, len(union))
		for _, id := range union {
			list = append(list, guard{SourceID: id, ExpectedRevision: in.sourceRevisions[id]})
		}
		status, _, aerr := s.call(ctx, subjBindingGuard, r.teamID, r.requestID,
			map[string]any{"body": map[string]any{"team_id": r.teamID, "sources": list}}, codeServiceDown)
		if aerr != nil {
			return aerr
		}
		switch status {
		case 200:
		case 409:
			return fail(412, codeStale, "a Source changed since it was read")
		default:
			return fail(503, codeServiceDown, "source-controller did not answer")
		}
	}
	f.SourceIDs = append([]int64{}, in.sourceIDs...)
	return nil
}

func (s *Service) validateModels(ctx context.Context, r *request, aliases []string) error {
	status, body, aerr := s.call(ctx, subjValidateModels, r.teamID, r.requestID,
		map[string]any{"body": map[string]any{"team_id": r.teamID, "aliases": aliases}}, codeGatewayDown)
	if aerr != nil {
		return aerr
	}
	var reply struct {
		Aliases []struct {
			Alias        string `json:"alias"`
			Owned        bool   `json:"owned"`
			Availability string `json:"availability"`
			Selectable   bool   `json:"selectable"`
		} `json:"aliases"`
	}
	if status != 200 || json.Unmarshal(body, &reply) != nil {
		return fail(503, codeGatewayDown, "model-registry did not answer")
	}
	ok := map[string]bool{}
	active := false
	for _, a := range reply.Aliases {
		if a.Owned && a.Selectable {
			ok[a.Alias] = true
			active = active || a.Availability == "active"
		}
	}
	for _, a := range aliases {
		if !ok[a] {
			return invalid("a selected model is not available to the team")
		}
	}
	if !active {
		return invalid("at least one selected model must be active")
	}
	return nil
}

func (s *Service) applyTeam(ctx context.Context, teamID, requestID, flowID int64, revision int, flowLimit json.RawMessage) error {
	status, _, aerr := s.call(ctx, subjApplyFlowTeam, teamID, requestID, map[string]any{"body": map[string]any{
		"team_id": teamID, "flow_id": flowID, "flow_revision": revision, "flow_limit": rawOrNull(flowLimit),
	}}, codeGatewayDown)
	if aerr != nil {
		if aerr.code == codeBrokerDown {
			return aerr
		}
		return fail(503, codeGatewayDown, "model-registry did not answer")
	}
	if status != 200 {
		return fail(503, codeGatewayDown, "the model gateway is unavailable")
	}
	return nil
}

// applyCommitted re-reads the committed Flow under its row lock and sends
// it to model-registry while still holding the lock, so a newer write cannot
// commit and apply between the read and this apply.
func (s *Service) applyCommitted(ctx context.Context, teamID, flowID, requestID int64) (store.Flow, error) {
	tx, err := s.o.Store.Begin(ctx)
	if err != nil {
		return store.Flow{}, err
	}
	defer func() { _ = tx.Rollback() }()
	f, err := tx.LockFlow(teamID, flowID)
	if err != nil {
		return store.Flow{}, err
	}
	if err := s.applyTeam(ctx, teamID, requestID, f.FlowID, f.Revision, f.FlowLimit); err != nil {
		return store.Flow{}, err
	}
	return f, nil
}

// restore re-applies the committed Flow record to model-registry until it
// confirms, then publishes flow_team_restored.
func (s *Service) restore(teamID, flowID, requestID int64) {
	s.bg.Add(1)
	go func() {
		defer s.bg.Done()
		ctx := context.Background()
		delay := s.o.RepairInterval
		for {
			f, err := s.applyCommitted(ctx, teamID, flowID, requestID)
			if err == nil {
				msg := s.outboxMessage("flow-registry.event.flow_team_restored.pub", strconv.FormatInt(teamID, 10), 0, map[string]any{
					"publisher": publisher, "team_id": teamID, "kind": "flow_team_restored", "flow_id": f.FlowID, "flow_revision": f.Revision,
				})
				if _, err = s.o.Store.AppendOutbox(ctx, msg); err == nil {
					return
				}
			} else if errors.Is(err, store.ErrNotFound) {
				return
			}
			s.o.Logger.Warn("flow team restoration retry", "team_id", teamID, "flow_id", flowID, "error", err)
			time.Sleep(delay)
			if delay *= 2; delay > maxRepairDelay {
				delay = maxRepairDelay
			}
		}
	}()
}

// removeOrphan removes the LiteLLM team of a create whose commit failed,
// retrying until model-registry confirms. If the commit did land after all,
// the committed record is restored instead. The flow_id is never reused.
func (s *Service) removeOrphan(teamID, flowID, requestID int64) {
	s.bg.Add(1)
	go func() {
		defer s.bg.Done()
		ctx := context.Background()
		delay := s.o.RepairInterval
		for {
			committed, err := s.committed(ctx, teamID, flowID)
			if err == nil && committed {
				s.restore(teamID, flowID, requestID)
				return
			}
			if err == nil {
				err = s.removeTeam(ctx, teamID, flowID, requestID)
				if err == nil {
					return
				}
			}
			s.o.Logger.Warn("orphan flow team removal retry", "team_id", teamID, "flow_id", flowID, "error", err)
			time.Sleep(delay)
			if delay *= 2; delay > maxRepairDelay {
				delay = maxRepairDelay
			}
		}
	}()
}

// committed reports whether flow-registry holds a committed Flow row.
func (s *Service) committed(ctx context.Context, teamID, flowID int64) (bool, error) {
	tx, err := s.o.Store.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.LockFlow(teamID, flowID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Service) removeTeam(ctx context.Context, teamID, flowID, requestID int64) error {
	status, _, aerr := s.call(ctx, subjRemoveFlowTeam, teamID, requestID, map[string]any{"body": map[string]any{
		"team_id": teamID, "flow_id": flowID,
	}}, codeGatewayDown)
	if aerr != nil {
		return aerr
	}
	if status != 200 {
		return fail(503, codeGatewayDown, "model-registry did not remove the Flow team")
	}
	return nil
}
