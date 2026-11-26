-- Re-attach after a reinstallation (decision 0011): a runner seen for the
-- first time adopts its cluster's inventory once. Runners seen before this
-- migration were never new: they keep `false`; a runner first seen from
-- now on gets `true` until its first inventory.
ALTER TABLE runners ADD COLUMN adopts_inventory BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE runners ALTER COLUMN adopts_inventory SET DEFAULT true;
ALTER TABLE runners ADD COLUMN adopted_at TIMESTAMPTZ;

-- The artefact's path on the flow's cluster, as an inventory placed it.
ALTER TABLE flows ADD COLUMN path TEXT CHECK (length(path) BETWEEN 1 AND 1000);
CREATE INDEX flows_cluster_path_idx ON flows (cluster, path) WHERE path IS NOT NULL;
