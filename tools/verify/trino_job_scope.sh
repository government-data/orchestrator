#!/usr/bin/env bash
# T5 of core decision 0090 (a job's scope), against engine-trino's
# coordinator files and the rules trino-rules makes from grants shaped as
# the core's:
#   - job_rules_loans: INSERT … SELECT runs; SELECT, EXPLAIN and DESCRIBE
#     refused by the `reject` selector; CTAS refused; SET SESSION refused;
#     it kills its own query;
#   - job_report_loans reads its gold rows and `$snapshots`, nothing else;
#   - job_sample_loans: a limited SELECT on silver; gold and writes refused;
#   - job_catalog creates and drops `src_*` catalogs only;
#   - with the rules URL answering 503, every query fails.
# Token checks (another key, audience, issuer, expired, look-alikes) are
# engine_trino_boot.sh's; `query()` needs a source catalog (T8).
#
# Run alone, never during a build, from the repository's root, with the
# rules renderer built (cargo build -p trino-rules --example render):
#   RENDER=<target>/debug/examples/render tools/verify/trino_job_scope.sh
# Containers and the network are `qo-verify-t5-*`, removed in a trap.
set -uo pipefail
P=qo-verify-t5
# shellcheck source=engine_trino_lib.sh
. "$(dirname "$0")/engine_trino_lib.sh"
: "${RENDER:?set RENDER to the render example of trino-rules}"

"$RENDER" >"$D/rules.json" 2>"$D/render.err" <<'JSON' || { cat "$D/render.err"; exit 1; }
{"core": [
  {"subject": "job_rules_loans", "namespace": "quality_gold", "tables": ["*"],
   "privileges": ["SELECT", "INSERT", "DELETE"], "rows": {"column": "dataset", "in": ["loans"]}},
  {"subject": "job_rules_loans", "namespace": "loans_silver", "tables": ["*"], "privileges": ["SELECT"]},
  {"subject": "job_report_loans", "namespace": "quality_gold", "tables": ["*"],
   "privileges": ["SELECT"], "rows": {"column": "dataset", "in": ["loans"]}},
  {"subject": "job_sample_loans", "namespace": "loans_silver", "tables": ["*"], "privileges": ["SELECT"]}
]}
JSON
et_pki
et_rustfs || exit 1
et_rules "$D/rules.json"
et_core
et_coordinator || exit 1

job() { et_token "job:00000000-0000-4000-8000-00000000000a:$1"; }
admin=$(et_token qd-admin)
rules=$(job rules:loans)
report=$(job report:loans)
sample=$(job sample:loans)
catalog=$(job catalog:crm)
G=lake.quality_gold.run_summary
S=lake.loans_silver.records
tq_ok "setup" "$admin" "CREATE SCHEMA lake.quality_gold"
tq_ok "setup" "$admin" "CREATE SCHEMA lake.loans_silver"
tq_ok "setup" "$admin" "CREATE TABLE $G (dataset varchar, n bigint)"
tq_ok "setup" "$admin" "CREATE TABLE $S AS SELECT x AS id FROM UNNEST(sequence(1, 50)) t(x)"
tq_ok "setup" "$admin" "INSERT INTO $G VALUES ('loans', 1), ('savings', 2)"

tq_ok "rules: INSERT … SELECT from silver" "$rules" "INSERT INTO $G SELECT 'loans', count(*) FROM $S"
for q in "SELECT * FROM $G" "EXPLAIN SELECT 1" "DESCRIBE $G"; do
  tq_refused "rules: '${q%% *}' refused by the selector" "$rules" "$q" "QUERY_QUEUE_FULL"
done
tq_refused "rules: CTAS" "$rules" "CREATE TABLE lake.quality_gold.x AS SELECT 1 AS a" "failed"
tq_refused "rules: SET SESSION" "$rules" "SET SESSION query_max_run_time = '1d'" "failed"
# Its own query, killed by itself while it runs.
docker exec -d -e T="$rules" "$P-coordinator" bash -c '/verify/tq.sh "INSERT INTO lake.quality_gold.run_summary
  SELECT '"'"'loans'"'"', count(*) FROM UNNEST(sequence(1, 2000)) a(x) CROSS JOIN UNNEST(sequence(1, 2000)) b(y)
  CROSS JOIN UNNEST(sequence(1, 2000)) c(z)" \
  >/data/trino/long.out 2>&1'
qid=
for _ in $(seq 1 60); do
  qid=$(tq "$admin" "SELECT query_id FROM system.runtime.queries WHERE user = 'job_rules_loans'
    AND state IN ('QUEUED', 'WAITING_FOR_RESOURCES', 'DISPATCHING', 'PLANNING', 'STARTING', 'RUNNING')
    LIMIT 1" | tr -d '[]"')
  [ -n "$qid" ] && break; sleep 0.5
done
code=$(docker exec -e T="$rules" "$P-coordinator" curl -s -o /dev/null -w '%{http_code}' -X DELETE \
  --cacert /run/qd-node/ca.crt -H "Authorization: Bearer $rules" "https://localhost/v1/query/$qid")
expect_match "rules: kills its own query ($qid)" '^20[04]$' "$code"

expect "report: its gold rows" '[["loans"],["loans"]]' "$(tq "$report" "SELECT dataset FROM $G ORDER BY n")"
tq_ok "report: \$snapshots" "$report" "SELECT count(*) FROM lake.quality_gold.\"run_summary\$snapshots\""
tq_refused "report: silver" "$report" "SELECT count(*) FROM $S" "PERMISSION_DENIED"
tq_refused "report: a write" "$report" "INSERT INTO $G VALUES ('loans', 9)" "PERMISSION_DENIED"

expect "sample: a limited SELECT on silver" '[[1],[2],[3]]' "$(tq "$sample" "SELECT id FROM $S ORDER BY id LIMIT 3" | tr -d '\n')"
tq_refused "sample: gold" "$sample" "SELECT * FROM $G" "PERMISSION_DENIED"
tq_refused "sample: a write" "$sample" "INSERT INTO $S VALUES 99" "PERMISSION_DENIED"

tq_ok "catalog: creates src_crm" "$catalog" "CREATE CATALOG src_crm USING memory"
tq_refused "catalog: another name" "$catalog" "CREATE CATALOG other USING memory" "failed"
tq_refused "catalog: no read" "$catalog" "SELECT count(*) FROM $G" "PERMISSION_DENIED"
tq_ok "catalog: drops src_crm" "$catalog" "DROP CATALOG src_crm"

docker stop "$P-core" >/dev/null
for _ in $(seq 1 15); do out=$(tq "$admin" "SELECT 1"); case "$out" in *failed*|*http*) break ;; esac; sleep 1; done
expect_match "the rules URL down: queries fail" "failed|http" "$out"

[ "$FAILED" = 0 ] && echo "ALL OK" || { echo "SOME CHECKS FAILED"; exit 1; }
