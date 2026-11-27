-- A run's locks (core decision 0090, points 4 and 6.2.4): one writer per
-- named lock, taken by a running run before its writes and free again
-- when that run is no longer running. A run-level `runs.lock_name` and a
-- taken lock of the same name exclude each other.
CREATE TABLE run_locks (
    name TEXT PRIMARY KEY CHECK (name ~ '^[a-z0-9_.:-]{1,200}$'),
    run_id UUID NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    taken_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX run_locks_run_idx ON run_locks (run_id);
