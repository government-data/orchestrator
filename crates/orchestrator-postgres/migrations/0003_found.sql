-- What each runner found on its cluster that no deploy placed (decision
-- 0011, import), replaced by its next report: an administrator imports
-- chosen ones. Paths, digests, sizes and flags; never content or values.
CREATE TABLE found_artefacts (
    runner TEXT NOT NULL REFERENCES runners (runner) ON DELETE CASCADE,
    path TEXT NOT NULL CHECK (length(path) BETWEEN 1 AND 1000),
    segments TEXT[] NOT NULL CHECK (cardinality(segments) BETWEEN 1 AND 16),
    sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    flags JSONB NOT NULL DEFAULT '[]',
    reported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (runner, path)
);

-- The external schedules that start them, as the runner read them.
CREATE TABLE found_schedules (
    runner TEXT NOT NULL REFERENCES runners (runner) ON DELETE CASCADE,
    artefact TEXT NOT NULL,
    schedule JSONB NOT NULL,
    reported_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX found_schedules_artefact_idx ON found_schedules (runner, artefact);
