-- Deploys (decision 0011, import): a runner of the engine and line, on the
-- cluster the artefact is on, claims one and reports its end. An import
-- deploy carries no upload: the artefact is already there. Engines and
-- lines are free names, checked against those registered.
CREATE TABLE deploys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    flow_id UUID NOT NULL REFERENCES flows (id) ON DELETE CASCADE,
    engine TEXT NOT NULL,
    line TEXT NOT NULL,
    cluster TEXT NOT NULL,
    source_path TEXT NOT NULL CHECK (length(source_path) BETWEEN 1 AND 1000),
    source_sha256 TEXT NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    -- The external schedules that start it, for the runner to disable.
    schedules JSONB NOT NULL DEFAULT '[]',
    state TEXT NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'claimed', 'succeeded', 'failed')),
    -- The runner instance that claimed it.
    runner TEXT,
    queued_at TIMESTAMPTZ NOT NULL,
    claimed_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    -- Its end as the runner reported it.
    path TEXT,
    version TEXT,
    sha256 TEXT,
    -- false: the flow's schedules are created paused.
    external_disabled BOOLEAN
);
CREATE INDEX deploys_queue_idx ON deploys (engine, line, cluster, queued_at)
    WHERE state IN ('queued', 'claimed');
CREATE INDEX deploys_flow_idx ON deploys (flow_id, queued_at DESC);
