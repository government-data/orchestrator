-- A scan asked for a runner (decision 0011, import): its renewals answer
-- `scan` until its next found report, which clears it; so a lost answer or
-- a runner that crashed before reporting is told again. Only runners that
-- report what they found can be asked.
ALTER TABLE runners ADD COLUMN found_at TIMESTAMPTZ;
ALTER TABLE runners ADD COLUMN scan_requested_at TIMESTAMPTZ;
