-- The orchestrator's outbox (decision 0011, step 6; quality-sdk decision
-- 0017): an event is written in the same transaction as what it tells, and
-- the server sends it to the core's events, in order, until the core
-- stores it. A run's end is told as `orchestrator.run.finished` v1 by a
-- trigger, so every way a run ends (its runner's finish, the reaper, a
-- cancel) writes exactly one event. Ids and states only: no parameter,
-- value or person.
CREATE TABLE event_outbox (
    seq BIGSERIAL PRIMARY KEY,
    -- The event's id at the core, the same at every try.
    id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    topic TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    occurred_at TIMESTAMPTZ NOT NULL,
    data JSONB NOT NULL,
    sent_at TIMESTAMPTZ,
    -- The HTTP status of the core's refusal: never sent again.
    refused INTEGER
);

CREATE INDEX event_outbox_pending ON event_outbox (seq)
    WHERE sent_at IS NULL AND refused IS NULL;

CREATE FUNCTION run_finished_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO event_outbox (topic, version, occurred_at, data)
    VALUES ('orchestrator.run.finished', 1, COALESCE(NEW.finished_at, now()),
            jsonb_strip_nulls(jsonb_build_object(
                'run_id', NEW.id,
                'flow_id', NEW.flow_id,
                'engine', NEW.engine,
                'line', NEW.line,
                'kind', NEW.kind,
                'state', NEW.state,
                'attempt', NEW.attempt,
                'trigger', NEW.trigger,
                'retry_of', NEW.retry_of,
                'chain_id', NEW.chain_id,
                'error_class', NEW.error_class,
                'interruption', NEW.interruption,
                'started_at', NEW.started_at,
                'finished_at', COALESCE(NEW.finished_at, now()))));
    RETURN NEW;
END;
$$;

CREATE TRIGGER runs_finished AFTER UPDATE OF state ON runs
    FOR EACH ROW
    WHEN (OLD.state IN ('queued', 'running')
          AND NEW.state IN ('succeeded', 'failed', 'timed_out', 'cancelled', 'lost'))
    EXECUTE FUNCTION run_finished_event();
