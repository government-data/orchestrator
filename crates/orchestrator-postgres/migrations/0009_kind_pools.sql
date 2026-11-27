-- Which kinds of an engine and line share a slot pool (core decision 0090,
-- point 6.3), and the run parameter whose values take turns in it. Sent by
-- the engine module; a claim stamps the pool on the run it hands out.
CREATE TABLE kind_pools (
    engine TEXT NOT NULL,
    line TEXT NOT NULL,
    kind TEXT NOT NULL,
    pool TEXT NOT NULL REFERENCES slot_pools (name),
    fair_by TEXT,
    PRIMARY KEY (engine, line, kind),
    FOREIGN KEY (engine, line) REFERENCES engines (engine, line)
);
