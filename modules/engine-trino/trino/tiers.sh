# The sizing tiers of core decision 0090 (point 6.1), sourced by
# entrypoint.sh: estimates until T11 to T13 measure them. XS to M are one
# node, coordinator and worker; on L the coordinator only plans and the
# workers are query-trino's (their own memory is query-trino's tier). Any
# value can be set in the environment instead (TRINO_HEAP, …).
# shellcheck shell=bash
tier() {
  local heap headroom cap rules bi_n bi_mem bi_share include retry=NONE
  # Extract, source and sample jobs (ruling 93), and a console's or an
  # entity key lookup's run (rulings 84 and 102): concurrency and memory
  # per query of each group; no record sizes them, so small to begin with.
  local jobs_n=1 extract_mem=1GB read_mem=256MB
  case "${TRINO_TIER:-s}" in
    xs) heap=6G headroom=1800MB cap=3GB rules=1 bi_n=2 bi_mem=600MB bi_share=29% include=true extract_mem=512MB ;;
    s) heap=10G headroom=3GB cap=5GB rules=1 bi_n=4 bi_mem=512MB bi_share=29% include=true ;;
    m) heap=26G headroom=6GB cap=7GB rules=2 bi_n=6 bi_mem=1GB bi_share=30% include=true jobs_n=2 extract_mem=2GB read_mem=512MB ;;
    l) heap=10G headroom=3GB cap=7GB rules=2 bi_n=6 bi_mem=1GB bi_share=30% include=false retry=QUERY jobs_n=2 extract_mem=2GB read_mem=512MB ;;
    *) echo "TRINO_TIER: xs, s, m or l, not ${TRINO_TIER}" >&2; return 1 ;;
  esac
  TRINO_HEAP=${TRINO_HEAP:-$heap}
  TRINO_HEADROOM=${TRINO_HEADROOM:-$headroom}
  # The rule jobs' cap per node and per query (one node; on L, per worker
  # times TRINO_WORKERS).
  TRINO_NODE_CAP=${TRINO_NODE_CAP:-$cap}
  TRINO_QUERY_CAP=${TRINO_QUERY_CAP:-$cap}
  RULES_CONCURRENCY=${RULES_CONCURRENCY:-$rules}
  BI_CONCURRENCY=${BI_CONCURRENCY:-$bi_n}
  BI_MEMORY=${BI_MEMORY:-$bi_mem}
  BI_SHARE=${BI_SHARE:-$bi_share}
  BI_CPU_SOFT=${BI_CPU_SOFT:-1h}
  BI_CPU_HARD=${BI_CPU_HARD:-2h}
  BI_SCAN=${BI_SCAN:-500GB}
  # `extract` (job_extract_*), `source` (job_source_*), `sample`
  # (job_sample_*), `console` (job_console_r<run>) and `lookup`
  # (job_lookup_r<run>), apart from `rules`: their own queue and limits;
  # the last four read with READ_MEMORY.
  JOBS_CONCURRENCY=${JOBS_CONCURRENCY:-$jobs_n}
  EXTRACT_MEMORY=${EXTRACT_MEMORY:-$extract_mem}
  READ_MEMORY=${READ_MEMORY:-$read_mem}
  TRINO_INCLUDE_COORDINATOR=${TRINO_INCLUDE_COORDINATOR:-$include}
  # Tier L repeats a query a lost worker cut (BI does not: its session
  # sets NONE); one node has nothing to repeat on.
  TRINO_RETRY_POLICY=${TRINO_RETRY_POLICY:-$retry}
  export TRINO_INCLUDE_COORDINATOR
}

# `template out`: each @NAME@ replaced by that variable's value (bash only:
# the image has no sed).
render() {
  local text name
  text=$(<"$1")
  for name in TRINO_HEAP RULES_CONCURRENCY BI_CONCURRENCY BI_MEMORY BI_SHARE \
    BI_CPU_SOFT BI_CPU_HARD BI_SCAN JOBS_CONCURRENCY EXTRACT_MEMORY READ_MEMORY; do
    text=${text//@${name#TRINO_}@/${!name}}
    text=${text//@${name}@/${!name}}
  done
  printf '%s\n' "$text" >"$2"
}
