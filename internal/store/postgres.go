package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Postgres is the PostgreSQL Store; the connection's search_path is the schema.
type Postgres struct{ db *sql.DB }

func NewPostgres(db *sql.DB) *Postgres { return &Postgres{db: db} }

const flowColumns = `f.team_id, f.flow_id, f.name, f.model_aliases::text, f.per_task_limit, f.flow_limit,
	f.revision, f.created_at, f.updated_at,
	e.revision, e.execution_tag, e.prompt_template, e.execution_parameters, e.default_image,
	e.failure_retry_count, e.failure_retry_wait_seconds, e.pending_limit_seconds, e.release_delay_seconds, e.created_at,
	COALESCE((SELECT json_agg(s.source_id ORDER BY s.source_id)::text FROM flow_sources s
	          WHERE s.team_id = f.team_id AND s.flow_id = f.flow_id), '[]')`

const flowFrom = ` FROM flows f JOIN flow_execution_revisions e
	ON e.team_id = f.team_id AND e.flow_id = f.flow_id AND e.revision = f.execution_revision`

type scanner interface{ Scan(dest ...any) error }

func scanFlow(row scanner) (Flow, error) {
	var f Flow
	var aliases, sources string
	var perTask, flowLimit, image sql.NullString
	var params string
	err := row.Scan(&f.TeamID, &f.FlowID, &f.Name, &aliases, &perTask, &flowLimit,
		&f.Revision, &f.CreatedAt, &f.UpdatedAt,
		&f.Execution.Revision, &f.Execution.Tag, &f.Execution.PromptTemplate, &params, &image,
		&f.Execution.FailureRetryCount, &f.Execution.FailureRetryWaitSeconds, &f.Execution.PendingLimitSeconds,
		&f.Execution.ReleaseDelaySeconds, &f.Execution.CreatedAt, &sources)
	if errors.Is(err, sql.ErrNoRows) {
		return Flow{}, ErrNotFound
	}
	if err != nil {
		return Flow{}, err
	}
	if err := json.Unmarshal([]byte(aliases), &f.ModelAliases); err != nil {
		return Flow{}, err
	}
	if err := json.Unmarshal([]byte(sources), &f.SourceIDs); err != nil {
		return Flow{}, err
	}
	if f.SourceIDs == nil {
		f.SourceIDs = []int64{}
	}
	f.PerTaskLimit = raw(perTask)
	f.FlowLimit = raw(flowLimit)
	f.Execution.Parameters = json.RawMessage(params)
	f.Execution.DefaultImage = raw(image)
	f.CreatedAt, f.UpdatedAt, f.Execution.CreatedAt = f.CreatedAt.UTC(), f.UpdatedAt.UTC(), f.Execution.CreatedAt.UTC()
	return f, nil
}

func raw(s sql.NullString) json.RawMessage {
	if !s.Valid {
		return nil
	}
	return json.RawMessage(s.String)
}

func nullable(r json.RawMessage) any {
	if r == nil {
		return nil
	}
	return string(r)
}

func queryFlows(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query string, args ...any) ([]Flow, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Flow
	for rows.Next() {
		f, err := scanFlow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func (p *Postgres) Ping(ctx context.Context) error { return p.db.PingContext(ctx) }

func (p *Postgres) Begin(ctx context.Context) (Tx, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &pgTx{ctx: ctx, tx: tx}, nil
}

// NextFlowID allocates the next per-team number in its own short
// transaction; numbers of rolled-back creates are not reused.
func (p *Postgres) NextFlowID(ctx context.Context, team int64) (int64, error) {
	var id int64
	err := p.db.QueryRowContext(ctx, `INSERT INTO flow_counters (team_id, last_flow_id) VALUES ($1, 1)
		ON CONFLICT (team_id) DO UPDATE SET last_flow_id = flow_counters.last_flow_id + 1
		RETURNING last_flow_id`, team).Scan(&id)
	return id, err
}

func (p *Postgres) GetFlow(ctx context.Context, team, flow int64) (Flow, error) {
	return scanFlow(p.db.QueryRowContext(ctx, "SELECT "+flowColumns+flowFrom+" WHERE f.team_id = $1 AND f.flow_id = $2", team, flow))
}

func (p *Postgres) ListFlows(ctx context.Context, team int64, filter ListFilter, after *Position, limit int) ([]Flow, error) {
	q := "SELECT " + flowColumns + flowFrom + ` WHERE f.team_id = $1
		AND ($2::bigint = 0 OR EXISTS (SELECT 1 FROM flow_sources s WHERE s.team_id = f.team_id AND s.flow_id = f.flow_id AND s.source_id = $2))
		AND ($3::text = '' OR f.model_aliases ? $3)
		AND ($4::boolean = false OR (f.created_at, f.flow_id) > ($5::timestamptz, $6::bigint))
		ORDER BY f.created_at, f.flow_id LIMIT $7`
	var at time.Time
	var afterID int64
	if after != nil {
		at, afterID = after.CreatedAt, after.FlowID
	}
	return queryFlows(ctx, p.db, q, team, filter.SourceID, filter.ModelAlias, after != nil, at, afterID, limit+1)
}

func (p *Postgres) Targets(ctx context.Context, team, source int64) ([]Flow, error) {
	return queryFlows(ctx, p.db, "SELECT "+flowColumns+flowFrom+` WHERE f.team_id = $1
		AND EXISTS (SELECT 1 FROM flow_sources s WHERE s.team_id = f.team_id AND s.flow_id = f.flow_id AND s.source_id = $2)
		ORDER BY f.flow_id`, team, source)
}

func (p *Postgres) FlowsListingAlias(ctx context.Context, team int64, alias string) ([]Flow, error) {
	return queryFlows(ctx, p.db, "SELECT "+flowColumns+flowFrom+" WHERE f.team_id = $1 AND f.model_aliases ? $2 ORDER BY f.flow_id", team, alias)
}

func (p *Postgres) FlowExists(ctx context.Context, team, flow int64) (bool, error) {
	var ok bool
	err := p.db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM flows WHERE team_id = $1 AND flow_id = $2)", team, flow).Scan(&ok)
	return ok, err
}

func (p *Postgres) AdminTags(ctx context.Context, filter TagFilter, after *TagPosition, limit int) ([]TagEntry, error) {
	var aTeam, aFlow int64
	var aTag string
	if after != nil {
		aTeam, aTag, aFlow = after.TeamID, after.Tag, after.FlowID
	}
	rows, err := p.db.QueryContext(ctx, `SELECT f.team_id, f.flow_id, e.execution_tag`+flowFrom+`
		WHERE ($1::bigint = 0 OR f.team_id = $1) AND ($2::text = '' OR e.execution_tag = $2)
		AND ($3::boolean = false OR (f.team_id, e.execution_tag COLLATE "C", f.flow_id) > ($4::bigint, $5::text COLLATE "C", $6::bigint))
		ORDER BY f.team_id, e.execution_tag COLLATE "C", f.flow_id LIMIT $7`,
		filter.TeamID, filter.Tag, after != nil, aTeam, aTag, aFlow, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TagEntry
	for rows.Next() {
		var e TagEntry
		if err := rows.Scan(&e.TeamID, &e.FlowID, &e.Tag); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func appendOutbox(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, msg OutboxMessage) (int64, error) {
	var seq int64
	if err := q.QueryRowContext(ctx, "SELECT nextval('outbox_publisher_seq')").Scan(&seq); err != nil {
		return 0, err
	}
	payload, err := msg.Build(seq)
	if err != nil {
		return 0, err
	}
	_, err = q.ExecContext(ctx, "INSERT INTO outbox (publisher_seq, subject, team, request_id, payload) VALUES ($1, $2, $3, $4, $5)",
		seq, msg.Subject, msg.Team, msg.RequestID, string(payload))
	return seq, err
}

func (p *Postgres) AppendOutbox(ctx context.Context, msg OutboxMessage) (int64, error) {
	return appendOutbox(ctx, p.db, msg)
}

func (p *Postgres) PendingOutbox(ctx context.Context, limit int) ([]OutboxRow, error) {
	rows, err := p.db.QueryContext(ctx, "SELECT publisher_seq, subject, team, request_id, payload FROM outbox WHERE sent_at IS NULL ORDER BY publisher_seq LIMIT $1", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboxRow
	for rows.Next() {
		var r OutboxRow
		var payload string
		if err := rows.Scan(&r.Seq, &r.Subject, &r.Team, &r.RequestID, &payload); err != nil {
			return nil, err
		}
		r.Payload = []byte(payload)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (p *Postgres) MarkSent(ctx context.Context, seq int64) error {
	_, err := p.db.ExecContext(ctx, "UPDATE outbox SET sent_at = now() WHERE publisher_seq = $1", seq)
	return err
}

// pgTx is one write transaction; locks follow the order alias guards,
// Flow rows, Source lock rows.
type pgTx struct {
	ctx context.Context
	tx  *sql.Tx
}

func sortedUnique(aliases []string) []string {
	m := map[string]bool{}
	for _, a := range aliases {
		m[a] = true
	}
	out := make([]string, 0, len(m))
	for a := range m {
		out = append(out, a)
	}
	sort.Strings(out)
	return out
}

func (t *pgTx) ensureAlias(team int64, alias string) error {
	_, err := t.tx.ExecContext(t.ctx, "INSERT INTO alias_guards (team_id, alias) VALUES ($1, $2) ON CONFLICT DO NOTHING", team, alias)
	return err
}

func (t *pgTx) LockAliases(team int64, aliases []string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, a := range sortedUnique(aliases) {
		if err := t.ensureAlias(team, a); err != nil {
			return nil, err
		}
		var retired bool
		if err := t.tx.QueryRowContext(t.ctx, "SELECT retired FROM alias_guards WHERE team_id = $1 AND alias = $2 FOR SHARE", team, a).Scan(&retired); err != nil {
			return nil, err
		}
		out[a] = retired
	}
	return out, nil
}

func (t *pgTx) LockAliasExclusive(team int64, alias string) (bool, error) {
	if err := t.ensureAlias(team, alias); err != nil {
		return false, err
	}
	var retired bool
	err := t.tx.QueryRowContext(t.ctx, "SELECT retired FROM alias_guards WHERE team_id = $1 AND alias = $2 FOR UPDATE", team, alias).Scan(&retired)
	return retired, err
}

func (t *pgTx) SetAliasRetired(team int64, alias string, retired bool) error {
	_, err := t.tx.ExecContext(t.ctx, `INSERT INTO alias_guards (team_id, alias, retired) VALUES ($1, $2, $3)
		ON CONFLICT (team_id, alias) DO UPDATE SET retired = EXCLUDED.retired`, team, alias, retired)
	return err
}

func (t *pgTx) LockFlow(team, flow int64) (Flow, error) {
	return scanFlow(t.tx.QueryRowContext(t.ctx, "SELECT "+flowColumns+flowFrom+" WHERE f.team_id = $1 AND f.flow_id = $2 FOR UPDATE OF f", team, flow))
}

func (t *pgTx) LockFlowsListingAlias(team int64, alias string) ([]Flow, error) {
	return queryFlows(t.ctx, t.tx, "SELECT "+flowColumns+flowFrom+" WHERE f.team_id = $1 AND f.model_aliases ? $2 ORDER BY f.flow_id FOR UPDATE OF f", team, alias)
}

func (t *pgTx) NameTaken(team int64, name string, except int64) (bool, error) {
	var taken bool
	err := t.tx.QueryRowContext(t.ctx, "SELECT EXISTS (SELECT 1 FROM flows WHERE team_id = $1 AND lower(name) = lower($2) AND flow_id <> $3)", team, name, except).Scan(&taken)
	return taken, err
}

func (t *pgTx) LockSources(team int64, sources []int64) error {
	ids := append([]int64(nil), sources...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if _, err := t.tx.ExecContext(t.ctx, "INSERT INTO source_locks (team_id, source_id) VALUES ($1, $2) ON CONFLICT DO NOTHING", team, id); err != nil {
			return err
		}
		if _, err := t.tx.ExecContext(t.ctx, "SELECT 1 FROM source_locks WHERE team_id = $1 AND source_id = $2 FOR UPDATE", team, id); err != nil {
			return err
		}
	}
	return nil
}

func (t *pgTx) SourceBindingCount(team, source, except int64) (int, error) {
	var n int
	err := t.tx.QueryRowContext(t.ctx, "SELECT count(*) FROM flow_sources WHERE team_id = $1 AND source_id = $2 AND flow_id <> $3", team, source, except).Scan(&n)
	return n, err
}

func conflict(err error) error {
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" && strings.Contains(pe.ConstraintName, "name") {
		return ErrNameConflict
	}
	return err
}

func (t *pgTx) writeExecution(f Flow) error {
	e := f.Execution
	_, err := t.tx.ExecContext(t.ctx, `INSERT INTO flow_execution_revisions (team_id, flow_id, revision, execution_tag, prompt_template,
		execution_parameters, default_image, failure_retry_count, failure_retry_wait_seconds, pending_limit_seconds, release_delay_seconds, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		f.TeamID, f.FlowID, e.Revision, e.Tag, e.PromptTemplate, string(e.Parameters), nullable(e.DefaultImage),
		e.FailureRetryCount, e.FailureRetryWaitSeconds, e.PendingLimitSeconds, e.ReleaseDelaySeconds, e.CreatedAt)
	return err
}

func (t *pgTx) writeSources(f Flow) error {
	if _, err := t.tx.ExecContext(t.ctx, "DELETE FROM flow_sources WHERE team_id = $1 AND flow_id = $2", f.TeamID, f.FlowID); err != nil {
		return err
	}
	for _, s := range f.SourceIDs {
		if _, err := t.tx.ExecContext(t.ctx, "INSERT INTO flow_sources (team_id, flow_id, source_id) VALUES ($1, $2, $3)", f.TeamID, f.FlowID, s); err != nil {
			return err
		}
	}
	return nil
}

func (t *pgTx) InsertFlow(f Flow) error {
	aliases, _ := json.Marshal(f.ModelAliases)
	if _, err := t.tx.ExecContext(t.ctx, "SAVEPOINT insert_flow"); err != nil {
		return err
	}
	_, err := t.tx.ExecContext(t.ctx, `INSERT INTO flows (team_id, flow_id, name, model_aliases, per_task_limit, flow_limit, revision, execution_revision, created_at, updated_at)
		VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9, $10)`,
		f.TeamID, f.FlowID, f.Name, string(aliases), nullable(f.PerTaskLimit), nullable(f.FlowLimit), f.Revision, f.Execution.Revision, f.CreatedAt, f.UpdatedAt)
	if err != nil {
		_, _ = t.tx.ExecContext(t.ctx, "ROLLBACK TO SAVEPOINT insert_flow")
		return conflict(err)
	}
	if err := t.writeExecution(f); err != nil {
		return err
	}
	return t.writeSources(f)
}

func (t *pgTx) UpdateFlow(f Flow, appendExecution bool) error {
	aliases, _ := json.Marshal(f.ModelAliases)
	if _, err := t.tx.ExecContext(t.ctx, "SAVEPOINT update_flow"); err != nil {
		return err
	}
	res, err := t.tx.ExecContext(t.ctx, `UPDATE flows SET name = $3, model_aliases = $4::jsonb, per_task_limit = $5, flow_limit = $6,
		revision = $7, execution_revision = $8, updated_at = $9 WHERE team_id = $1 AND flow_id = $2`,
		f.TeamID, f.FlowID, f.Name, string(aliases), nullable(f.PerTaskLimit), nullable(f.FlowLimit), f.Revision, f.Execution.Revision, f.UpdatedAt)
	if err != nil {
		_, _ = t.tx.ExecContext(t.ctx, "ROLLBACK TO SAVEPOINT update_flow")
		return conflict(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	if appendExecution {
		// The Flow row points at the new revision, which is inserted now.
		if err := t.writeExecution(f); err != nil {
			return err
		}
	}
	return t.writeSources(f)
}

func (t *pgTx) AppendOutbox(msg OutboxMessage) (int64, error) { return appendOutbox(t.ctx, t.tx, msg) }
func (t *pgTx) Commit() error                                 { return t.tx.Commit() }
func (t *pgTx) Rollback() error {
	err := t.tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}
