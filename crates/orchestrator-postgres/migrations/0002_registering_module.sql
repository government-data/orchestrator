-- The engine module whose machine client registered each engine and line,
-- and each runner (decision 0015): what the check before a module's removal
-- counts. NULL: registered by a caller without a machine token.
ALTER TABLE engines ADD COLUMN module TEXT;
ALTER TABLE runners ADD COLUMN module TEXT;
