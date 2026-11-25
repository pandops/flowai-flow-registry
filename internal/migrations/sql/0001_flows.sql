-- flow-registry's own schema. The connection's search_path is the schema,
-- so no object name carries a schema prefix.

CREATE TABLE flow_counters (
    team_id      bigint PRIMARY KEY CHECK (team_id > 0),
    last_flow_id bigint NOT NULL CHECK (last_flow_id > 0)
);

CREATE TABLE flows (
    team_id            bigint      NOT NULL CHECK (team_id > 0),
    flow_id            bigint      NOT NULL CHECK (flow_id > 0),
    name               text        NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    model_aliases      jsonb       NOT NULL CHECK (jsonb_typeof(model_aliases) = 'array'),
    per_task_limit     text,
    flow_limit         text,
    revision           integer     NOT NULL CHECK (revision > 0),
    execution_revision integer     NOT NULL CHECK (execution_revision > 0),
    created_at         timestamptz NOT NULL,
    updated_at         timestamptz NOT NULL,
    PRIMARY KEY (team_id, flow_id)
);

CREATE UNIQUE INDEX flows_team_name_key ON flows (team_id, lower(name));
CREATE INDEX flows_team_created_idx ON flows (team_id, created_at, flow_id);

-- Immutable execution revisions; the composite key binds each revision to
-- the Flow's team.
CREATE TABLE flow_execution_revisions (
    team_id                    bigint      NOT NULL,
    flow_id                    bigint      NOT NULL,
    revision                   integer     NOT NULL CHECK (revision > 0),
    execution_tag              text        NOT NULL CHECK (execution_tag ~ '^[A-Za-z0-9_-]{1,128}$'),
    prompt_template            text        NOT NULL,
    execution_parameters       text        NOT NULL,
    default_image              text,
    failure_retry_count        integer     NOT NULL CHECK (failure_retry_count BETWEEN 0 AND 10),
    failure_retry_wait_seconds integer     NOT NULL CHECK (failure_retry_wait_seconds BETWEEN 10 AND 86400),
    pending_limit_seconds      integer     NOT NULL CHECK (pending_limit_seconds BETWEEN 60 AND 86400),
    release_delay_seconds      integer     NOT NULL CHECK (release_delay_seconds BETWEEN 60 AND 86400),
    created_at                 timestamptz NOT NULL,
    PRIMARY KEY (team_id, flow_id, revision),
    FOREIGN KEY (team_id, flow_id) REFERENCES flows (team_id, flow_id)
);

CREATE FUNCTION flow_execution_revisions_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'flow execution revisions are immutable';
END;
$$;

CREATE TRIGGER flow_execution_revisions_no_change
    BEFORE UPDATE OR DELETE ON flow_execution_revisions
    FOR EACH ROW EXECUTE FUNCTION flow_execution_revisions_immutable();

CREATE TABLE flow_sources (
    team_id   bigint NOT NULL,
    flow_id   bigint NOT NULL,
    source_id bigint NOT NULL CHECK (source_id > 0),
    PRIMARY KEY (team_id, flow_id, source_id),
    FOREIGN KEY (team_id, flow_id) REFERENCES flows (team_id, flow_id)
);

CREATE INDEX flow_sources_source_idx ON flow_sources (team_id, source_id);

-- One lock row per bound Source: Flow writes lock them in sorted order.
CREATE TABLE source_locks (
    team_id   bigint NOT NULL CHECK (team_id > 0),
    source_id bigint NOT NULL CHECK (source_id > 0),
    PRIMARY KEY (team_id, source_id)
);

-- Alias guards: Flow writes take them FOR SHARE, retirement FOR UPDATE.
CREATE TABLE alias_guards (
    team_id bigint  NOT NULL CHECK (team_id > 0),
    alias   text    NOT NULL,
    retired boolean NOT NULL DEFAULT false,
    PRIMARY KEY (team_id, alias)
);

CREATE SEQUENCE outbox_publisher_seq AS bigint MINVALUE 1;

CREATE TABLE outbox (
    publisher_seq bigint      PRIMARY KEY,
    subject       text        NOT NULL,
    team          text        NOT NULL,
    request_id    bigint      NOT NULL DEFAULT 0,
    payload       text        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    sent_at       timestamptz
);

CREATE INDEX outbox_pending_idx ON outbox (publisher_seq) WHERE sent_at IS NULL;
