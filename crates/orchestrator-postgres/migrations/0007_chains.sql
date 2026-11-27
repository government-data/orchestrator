-- Chains (core decision 0090, point 2: a cut fires as a chain): ordered
-- steps of one flow, each a run queued when the step before it succeeded.
-- `origin` is what fired it (a schedule's fire, a person's request), so one
-- fire makes one chain however often it is seen.
CREATE TABLE chains (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    flow_id UUID NOT NULL REFERENCES flows (id) ON DELETE CASCADE,
    origin TEXT NOT NULL UNIQUE CHECK (length(origin) BETWEEN 1 AND 200),
    trigger TEXT NOT NULL CHECK (trigger IN ('manual', 'schedule')),
    triggered_by TEXT,
    schedule_id UUID REFERENCES schedules (id) ON DELETE SET NULL,
    steps JSONB NOT NULL CHECK (jsonb_typeof(steps) = 'array'),
    -- The step it is at; `cardinality(steps)` once every step is done.
    step INTEGER NOT NULL DEFAULT 0 CHECK (step >= 0),
    state TEXT NOT NULL DEFAULT 'running' CHECK (state IN ('running', 'succeeded', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);
CREATE INDEX chains_open_idx ON chains (created_at) WHERE state <> 'succeeded';

-- A chain's run and its step; a repetition copies both.
ALTER TABLE runs ADD COLUMN chain_id UUID REFERENCES chains (id) ON DELETE CASCADE;
ALTER TABLE runs ADD COLUMN chain_step INTEGER;
CREATE UNIQUE INDEX runs_chain_step_idx ON runs (chain_id, chain_step)
    WHERE chain_id IS NOT NULL AND retry_of IS NULL;

-- The fingerprint a setup step last succeeded with, by its key (ruling 66:
-- a dataset's tables are prepared again only when its columns change).
CREATE TABLE setups (
    key TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    done_at TIMESTAMPTZ NOT NULL
);
