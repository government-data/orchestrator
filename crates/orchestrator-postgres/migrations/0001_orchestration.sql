-- The orchestrator's schema (decision 0011): one installation is one
-- organisation, so no table has a tenant. An engine is a free name checked
-- by the server against the engines registered now: no CHECK names one.

-- The installation's choices for every engine: one row.
CREATE TABLE settings (
    id BOOLEAN PRIMARY KEY DEFAULT true CHECK (id),
    missed_runs TEXT NOT NULL DEFAULT 'latest' CHECK (missed_runs IN ('latest', 'skip', 'all')),
    missed_max_hours INTEGER NOT NULL DEFAULT 24 CHECK (missed_max_hours BETWEEN 1 AND 720),
    missed_max_runs INTEGER NOT NULL DEFAULT 10 CHECK (missed_max_runs BETWEEN 1 AND 100),
    on_interrupt TEXT NOT NULL DEFAULT 'none' CHECK (on_interrupt IN ('none', 'repeat', 'continue')),
    queued_wait_minutes INTEGER CHECK (queued_wait_minutes BETWEEN 1 AND 10080),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO settings DEFAULT VALUES;

-- The engines and lines registered by their runners, with the descriptor
-- of the last registration.
CREATE TABLE engines (
    engine TEXT NOT NULL CHECK (engine ~ '^[a-z][a-z0-9-]{0,39}$'),
    line TEXT NOT NULL CHECK (line ~ '^(-|[a-z0-9.]{1,16})$'),
    engine_version TEXT NOT NULL,
    port INTEGER NOT NULL,
    kinds JSONB NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (engine, line)
);

-- Each runner seen: its machine identity, engine and line.
CREATE TABLE runners (
    runner TEXT PRIMARY KEY,
    engine TEXT NOT NULL,
    line TEXT NOT NULL,
    concurrency INTEGER NOT NULL CHECK (concurrency >= 1),
    paused_reason TEXT,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (engine, line) REFERENCES engines (engine, line)
);

-- An engine on pause: nothing new of it is handed out.
CREATE TABLE engine_pauses (
    engine TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    paused_by TEXT,
    paused_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- An engine's drain hold: no claim answered until `until` or its release.
CREATE TABLE engine_holds (
    engine TEXT PRIMARY KEY,
    hold TEXT NOT NULL,
    until TIMESTAMPTZ NOT NULL,
    held_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Folders group flows, for listing.
CREATE TABLE folders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE CHECK (length(name) BETWEEN 1 AND 120),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE flows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    folder_id UUID NOT NULL REFERENCES folders (id),
    name TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    description TEXT NOT NULL DEFAULT '',
    engine TEXT NOT NULL,
    line TEXT NOT NULL,
    kind TEXT NOT NULL,
    -- The cluster its artefact is on; NULL: any runner of its engine and line.
    cluster TEXT,
    parameters JSONB NOT NULL DEFAULT '{}',
    timeout_secs INTEGER NOT NULL CHECK (timeout_secs > 0),
    allow_overlap BOOLEAN NOT NULL DEFAULT false,
    -- NULL: the installation's rule.
    on_interrupt TEXT CHECK (on_interrupt IN ('none', 'repeat', 'continue')),
    paused_at TIMESTAMPTZ,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (folder_id, name)
);
CREATE INDEX flows_engine_line_idx ON flows (engine, line);

CREATE TABLE schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    flow_id UUID NOT NULL REFERENCES flows (id) ON DELETE CASCADE,
    -- Empty when `trigger` is set.
    cron TEXT NOT NULL DEFAULT '',
    timezone TEXT NOT NULL,
    trigger JSONB,
    -- Over the flow's parameters on each fire.
    parameters JSONB NOT NULL DEFAULT '{}',
    timeout_secs INTEGER CHECK (timeout_secs > 0),
    enabled BOOLEAN NOT NULL DEFAULT true,
    next_fire_at TIMESTAMPTZ,
    last_fire_at TIMESTAMPTZ,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX schedules_due_idx ON schedules (next_fire_at) WHERE enabled;

CREATE TABLE runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    flow_id UUID NOT NULL REFERENCES flows (id) ON DELETE CASCADE,
    -- Copied from the flow: a runner claims only its own.
    engine TEXT NOT NULL,
    line TEXT NOT NULL,
    kind TEXT NOT NULL,
    cluster TEXT,
    state TEXT NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'timed_out', 'cancelled', 'lost')),
    trigger TEXT NOT NULL CHECK (trigger IN ('manual', 'schedule')),
    triggered_by TEXT,
    schedule_id UUID REFERENCES schedules (id) ON DELETE SET NULL,
    -- The fire it runs, so that a fire queues one run however often the
    -- scheduler sees it.
    fire_at TIMESTAMPTZ,
    parameters JSONB NOT NULL DEFAULT '{}',
    timeout_secs INTEGER NOT NULL CHECK (timeout_secs > 0),
    attempt INTEGER NOT NULL DEFAULT 1 CHECK (attempt >= 1),
    retry_of UUID REFERENCES runs (id) ON DELETE SET NULL,
    resume BOOLEAN NOT NULL DEFAULT false,
    -- Not handed out before this (a repetition's delay).
    not_before TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Admission: at most one running run per lock, and a pool's size.
    lock_name TEXT,
    slot_pool TEXT,
    queued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    runner TEXT,
    lease TEXT,
    lease_until TIMESTAMPTZ,
    engine_ref TEXT,
    progress_fraction DOUBLE PRECISION,
    progress_detail TEXT,
    cancel_requested BOOLEAN NOT NULL DEFAULT false,
    interruption TEXT CHECK (interruption IN ('system', 'runner_stopped', 'runner_restarted',
                                              'runner_lost', 'runner_isolated', 'low_disk')),
    error_class TEXT,
    counts JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX runs_queue_idx ON runs (engine, line, queued_at) WHERE state = 'queued';
CREATE INDEX runs_running_idx ON runs (lease_until) WHERE state = 'running';
CREATE INDEX runs_flow_idx ON runs (flow_id, queued_at DESC);
CREATE UNIQUE INDEX runs_retry_idx ON runs (retry_of) WHERE retry_of IS NOT NULL;
CREATE UNIQUE INDEX runs_fire_idx ON runs (schedule_id, fire_at)
    WHERE schedule_id IS NOT NULL AND fire_at IS NOT NULL AND retry_of IS NULL;

-- The run log people read, numbered by the runner.
CREATE TABLE run_logs (
    run_id UUID NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    seq BIGINT NOT NULL CHECK (seq >= 1),
    stream TEXT NOT NULL CHECK (stream IN ('stdout', 'stderr', 'system')),
    line TEXT NOT NULL,
    logged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, seq)
);

-- Slot pools: how many runs naming the pool run at once.
CREATE TABLE slot_pools (
    name TEXT PRIMARY KEY,
    size INTEGER NOT NULL CHECK (size >= 1)
);

-- Secrets any flow may name; the value is sealed by the server, never
-- stored or logged in clear.
CREATE TABLE secrets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE CHECK (length(name) BETWEEN 1 AND 120),
    description TEXT NOT NULL DEFAULT '',
    sealed BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A flow's variables: a value (sealed when secret), or a secret by id.
CREATE TABLE flow_variables (
    flow_id UUID NOT NULL REFERENCES flows (id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (name ~ '^[A-Za-z_][A-Za-z0-9_]{0,99}$'),
    secret BOOLEAN NOT NULL DEFAULT false,
    value JSONB,
    sealed BYTEA,
    secret_id UUID REFERENCES secrets (id) ON DELETE SET NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (flow_id, name),
    CHECK (num_nonnulls(value, sealed, secret_id) = 1),
    CHECK (secret = (value IS NULL))
);

-- Maintenance windows per cluster.
CREATE TABLE maintenance_windows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 120),
    time JSONB NOT NULL,
    on_schedule TEXT NOT NULL CHECK (on_schedule IN ('queue', 'skip')),
    enabled BOOLEAN NOT NULL DEFAULT true,
    clusters TEXT[] NOT NULL CHECK (cardinality(clusters) >= 1)
);

-- Change notices that carry no data: the kind and the id.
CREATE FUNCTION orchestrator_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('orchestrator_events', json_build_object(
        'kind', TG_TABLE_NAME,
        'id', COALESCE(NEW.id, OLD.id)
    )::text);
    RETURN NULL;
END
$$;
CREATE TRIGGER runs_changed AFTER INSERT OR UPDATE OF state ON runs
    FOR EACH ROW EXECUTE FUNCTION orchestrator_notify();
CREATE TRIGGER flows_changed AFTER INSERT OR UPDATE OR DELETE ON flows
    FOR EACH ROW EXECUTE FUNCTION orchestrator_notify();
