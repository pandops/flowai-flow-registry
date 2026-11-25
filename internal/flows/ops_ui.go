package flows

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"

	"github.com/pandops/flowai-flow-registry/internal/cursor"
	"github.com/pandops/flowai-flow-registry/internal/prompt"
	"github.com/pandops/flowai-flow-registry/internal/store"
)

const subjFlowsStatus = "model-registry.service.flows-status.pub"

var budgetPattern = regexp.MustCompile(`^(0|[1-9][0-9]{0,6})(\.[0-9]{1,2})?$`)

func (s *Service) serveUI(ctx context.Context, r *request) (*result, error) {
	switch r.op {
	case "flows-list":
		return s.listFlows(ctx, r)
	case "flow-get":
		return s.getFlow(ctx, r)
	case "flow-create":
		return s.writeFlow(ctx, r, false)
	case "flow-replace":
		return s.writeFlow(ctx, r, false)
	case "llm-settings-list":
		return s.listSettings(ctx, r)
	case "llm-settings-get":
		return s.getSettings(ctx, r)
	case "llm-settings-replace":
		return s.writeFlow(ctx, r, true)
	case "flow-preview":
		return s.preview(r)
	}
	return nil, errNotFound
}

func storeErr(err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return errNotFound
	}
	return fail(503, codeServiceDown, "storage unavailable")
}

type listParams struct {
	Limit      *int    `json:"limit"`
	Cursor     *string `json:"cursor"`
	SourceID   *int64  `json:"source_id"`
	ModelAlias *string `json:"model_alias"`
}

// page reads one cursor-bound page of the team's Flows.
func (s *Service) page(ctx context.Context, r *request, p listParams) ([]store.Flow, *string, error) {
	limit := defaultListLimit
	if p.Limit != nil {
		limit = *p.Limit
	}
	filter := store.ListFilter{}
	filters := map[string]string{}
	if p.SourceID != nil {
		filter.SourceID = *p.SourceID
		filters["source_id"] = strconv.FormatInt(*p.SourceID, 10)
	}
	if p.ModelAlias != nil {
		filter.ModelAlias = *p.ModelAlias
		filters["model_alias"] = *p.ModelAlias
	}
	if s.o.Cursors == nil {
		return nil, nil, fail(500, codeInternal, "cursor keys unavailable")
	}
	binding := cursor.Binding{Operation: r.op, TeamID: r.teamID, OperatorID: r.operatorID, Filters: filters}
	var after *store.Position
	if p.Cursor != nil {
		pos, err := s.o.Cursors.Decode(*p.Cursor, binding)
		if err != nil {
			return nil, nil, invalid("cursor is not valid for this read")
		}
		after = &pos
	}
	rows, err := s.o.Store.ListFlows(ctx, r.teamID, filter, after, limit)
	if err != nil {
		return nil, nil, storeErr(err)
	}
	var next *string
	if len(rows) > limit {
		rows = rows[:limit]
		last := rows[len(rows)-1]
		tok, err := s.o.Cursors.Encode(binding, store.Position{CreatedAt: last.CreatedAt, FlowID: last.FlowID})
		if err != nil {
			return nil, nil, fail(500, codeInternal, "cursor encoding failed")
		}
		next = &tok
	}
	return rows, next, nil
}

func (s *Service) listFlows(ctx context.Context, r *request) (*result, error) {
	var p listParams
	_ = json.Unmarshal(orEmpty(r.params), &p)
	rows, next, err := s.page(ctx, r, p)
	if err != nil {
		return nil, err
	}
	items := make([]flowView, 0, len(rows))
	for _, f := range rows {
		items = append(items, viewOf(f))
	}
	return &result{status: 200, body: map[string]any{"items": items, "next_cursor": next}}, nil
}

func orEmpty(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("{}")
	}
	return raw
}

func flowIDParam(r *request) int64 {
	var p struct {
		FlowID int64 `json:"flow_id"`
	}
	_ = json.Unmarshal(orEmpty(r.params), &p)
	return p.FlowID
}

func (s *Service) getFlow(ctx context.Context, r *request) (*result, error) {
	f, err := s.o.Store.GetFlow(ctx, r.teamID, flowIDParam(r))
	if err != nil {
		return nil, storeErr(err)
	}
	return &result{status: 200, body: viewOf(f), etag: f.Revision}, nil
}

func (s *Service) listSettings(ctx context.Context, r *request) (*result, error) {
	var p listParams
	_ = json.Unmarshal(orEmpty(r.params), &p)
	rows, next, err := s.page(ctx, r, p)
	if err != nil {
		return nil, err
	}
	return &result{status: 200, body: map[string]any{"items": s.settingsOf(ctx, r, rows), "next_cursor": next}}, nil
}

func (s *Service) getSettings(ctx context.Context, r *request) (*result, error) {
	f, err := s.o.Store.GetFlow(ctx, r.teamID, flowIDParam(r))
	if err != nil {
		return nil, storeErr(err)
	}
	return &result{status: 200, body: s.settingsOf(ctx, r, []store.Flow{f})[0], etag: f.Revision}, nil
}

// settingsOf adds alias availability and usage from model-registry. When it
// does not answer, every alias is "unavailable" and usage is null.
func (s *Service) settingsOf(ctx context.Context, r *request, rows []store.Flow) []settingsView {
	type statusFlow struct {
		FlowID int64 `json:"flow_id"`
		Models []struct {
			ModelAlias   string `json:"model_alias"`
			Availability string `json:"availability"`
		} `json:"models"`
		Usage *struct {
			SpendUSD      string  `json:"spend_usd"`
			BudgetResetAt *string `json:"budget_reset_at"`
			MaxBudgetUSD  string  `json:"max_budget_usd"`
		} `json:"usage"`
	}
	byFlow := map[int64]statusFlow{}
	if len(rows) > 0 {
		type reqFlow struct {
			FlowID    int64           `json:"flow_id"`
			Aliases   []string        `json:"aliases"`
			FlowLimit json.RawMessage `json:"flow_limit"`
		}
		var list []reqFlow
		for _, f := range rows {
			list = append(list, reqFlow{FlowID: f.FlowID, Aliases: nonNil(f.ModelAliases), FlowLimit: rawOrNull(f.FlowLimit)})
		}
		status, body, aerr := s.call(ctx, subjFlowsStatus, r.teamID, r.requestID,
			map[string]any{"body": map[string]any{"team_id": r.teamID, "flows": list}}, codeGatewayDown)
		if aerr == nil && status == 200 {
			var reply struct {
				Flows []statusFlow `json:"flows"`
			}
			if json.Unmarshal(body, &reply) == nil {
				for _, f := range reply.Flows {
					byFlow[f.FlowID] = f
				}
			}
		}
	}
	out := make([]settingsView, 0, len(rows))
	for _, f := range rows {
		st, known := byFlow[f.FlowID]
		avail := map[string]string{}
		for _, m := range st.Models {
			avail[m.ModelAlias] = m.Availability
		}
		v := settingsView{flowView: viewOf(f), Models: []modelView{}}
		for _, a := range f.ModelAliases {
			av := avail[a]
			switch av {
			case "active", "paused", "unavailable", "no_access":
			default:
				av = "unavailable"
			}
			v.Models = append(v.Models, modelView{ModelAlias: a, Availability: av})
		}
		if known && st.Usage != nil && st.Usage.BudgetResetAt != nil && budgetPattern.MatchString(st.Usage.MaxBudgetUSD) {
			v.Usage = &usageView{SpendUSD: st.Usage.SpendUSD, MaxBudgetUSD: st.Usage.MaxBudgetUSD, BudgetResetAt: *st.Usage.BudgetResetAt}
		}
		out = append(out, v)
	}
	return out
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func (s *Service) preview(r *request) (*result, error) {
	var b struct {
		PromptTemplate string `json:"prompt_template"`
		ExternalID     string `json:"external_id"`
	}
	if err := json.Unmarshal(r.body, &b); err != nil {
		return nil, invalid("body is not valid")
	}
	if err := prompt.Validate(b.PromptTemplate); err != nil {
		return nil, invalid("prompt_template is not valid")
	}
	out, err := prompt.Render(b.PromptTemplate, b.ExternalID)
	if err != nil {
		return nil, invalid("rendered prompt exceeds 32 KiB")
	}
	return &result{status: 200, body: map[string]any{"rendered_prompt": out}}, nil
}
