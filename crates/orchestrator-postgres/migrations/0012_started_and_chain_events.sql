-- Two more events through the outbox (decision 0011, step 6; ruling 99),
-- written by triggers in the same transaction as the state they tell, as
-- `orchestrator.run.finished`: a run's claim as `orchestrator.run.started`
-- v1 and a chain's end as `orchestrator.chain.finished` v1. Ids, states and
-- times only: no parameter, value or person.
CREATE FUNCTION run_started_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO event_outbox (topic, version, occurred_at, data)
    VALUES ('orchestrator.run.started', 1, COALESCE(NEW.started_at, now()),
            jsonb_strip_nulls(jsonb_build_object(
                'run_id', NEW.id,
                'chain_id', NEW.chain_id,
                'attempt', NEW.attempt,
                'started_at', COALESCE(NEW.started_at, now()),
                'engine', NEW.engine,
                'line', NEW.line,
                'kind', NEW.kind)));
    RETURN NEW;
END;
$$;

CREATE TRIGGER runs_started AFTER UPDATE OF state ON runs
    FOR EACH ROW
    WHEN (OLD.state = 'queued' AND NEW.state = 'running')
    EXECUTE FUNCTION run_started_event();

-- A chain that failed and opens again when a repetition of its step
-- succeeds ends again: each end is told.
CREATE FUNCTION chain_finished_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO event_outbox (topic, version, occurred_at, data)
    VALUES ('orchestrator.chain.finished', 1, COALESCE(NEW.finished_at, now()),
            jsonb_build_object(
                'chain_id', NEW.id,
                'state', NEW.state,
                'origin', NEW.origin));
    RETURN NEW;
END;
$$;

CREATE TRIGGER chains_finished AFTER UPDATE OF state ON chains
    FOR EACH ROW
    WHEN (OLD.state = 'running' AND NEW.state IN ('succeeded', 'failed'))
    EXECUTE FUNCTION chain_finished_event();
