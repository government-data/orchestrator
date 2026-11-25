# orchestrator

The orchestrator of Quality Data, as the `orchestrator` module: it fires
schedules, keeps the state of every run and hands runs out to the engine
modules that run them (`engine-trino` first), which pull their work from it.
