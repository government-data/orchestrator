-- A schedule kept by the core and fired by the orchestrator (core decision
-- 0090, point 2): the core's id of it. At each fire the core says the chain
-- to run; the fire itself follows the orchestrator's rules (missed fires,
-- windows, pauses) as any schedule's.
ALTER TABLE schedules ADD COLUMN core_id TEXT UNIQUE CHECK (length(core_id) BETWEEN 1 AND 200);
