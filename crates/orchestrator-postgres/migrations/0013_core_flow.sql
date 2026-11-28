-- The flow the core's schedules fire on and its chains start on (decision
-- 0011, ruling 97): created by the core through the orchestrator's internal
-- route and kept here, so no setting has to be written by hand. NULL: none
-- yet, the core's schedules and chains wait.
ALTER TABLE settings ADD COLUMN core_flow_id UUID REFERENCES flows (id) ON DELETE SET NULL;
