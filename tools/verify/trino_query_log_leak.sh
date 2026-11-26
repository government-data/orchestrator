#!/usr/bin/env bash
# T7 of core decision 0090 (what the query log keeps), against engine-trino's
# coordinator files and trino-rules' document:
#   - a rule job and a sample job over rows holding marker values, and
#     failing statements on them (a CAST of a marker, a division by zero):
#     no marker in the coordinator's log at INFO;
#   - `system.runtime.queries` refused to people and to job users;
#   - no spooled result (spooling is off): no object but the tables'.
# That the runner's report holds Trino's error name only is trino-client's
# and trino-runner's own tests (messages are not displayed or debugged).
# On one node: a worker's log is the coordinator's here.
#
# Run alone, never during a build, from the repository's root:
#   RENDER=<target>/debug/examples/render tools/verify/trino_query_log_leak.sh
# Containers and the network are `qo-verify-t7-*`, removed in a trap.
set -uo pipefail
P=qo-verify-t7
# shellcheck source=engine_trino_lib.sh
. "$(dirname "$0")/engine_trino_lib.sh"
: "${RENDER:?set RENDER to the render example of trino-rules}"

MARK="qomarker$(openssl rand -hex 6)"
ANA=person-00000000-0000-4000-8000-00000000000a
"$RENDER" >"$D/rules.json" 2>"$D/render.err" <<JSON || { cat "$D/render.err"; exit 1; }
{"core": [
  {"subject": "job_rules_loans", "namespace": "quality_gold", "tables": ["*"],
   "privileges": ["SELECT", "INSERT", "DELETE"], "rows": {"column": "dataset", "in": ["loans"]}},
  {"subject": "job_rules_loans", "namespace": "loans_silver", "tables": ["*"], "privileges": ["SELECT"]},
  {"subject": "job_sample_loans", "namespace": "loans_silver", "tables": ["*"], "privileges": ["SELECT"]},
  {"subject": "$ANA", "namespace": "quality_gold", "tables": ["*"], "privileges": ["SELECT"],
   "rows": {"column": "dataset", "in": ["loans"]}}
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
sample=$(job sample:loans)
ana=$(et_token "$ANA")
S=lake.loans_silver.records
G=lake.quality_gold.run_summary
tq_ok "setup" "$admin" "CREATE SCHEMA lake.quality_gold"
tq_ok "setup" "$admin" "CREATE SCHEMA lake.loans_silver"
tq_ok "setup" "$admin" "CREATE TABLE $G (dataset varchar, n bigint)"
tq_ok "setup" "$admin" "CREATE TABLE $S AS SELECT * FROM (VALUES ('$MARK-1', 0), ('$MARK-2', 3)) t(v, d)"
start=$(date -u +%Y-%m-%dT%H:%M:%S)

tq_ok "rules: a count over the markers" "$rules" \
  "INSERT INTO $G SELECT 'loans', count(*) FROM $S WHERE v LIKE '%-1'"
tq_refused "rules: a CAST of a marker fails" "$rules" \
  "INSERT INTO $G SELECT 'loans', CAST(v AS bigint) FROM $S" "INVALID_CAST_ARGUMENT|failed"
tq_refused "rules: a division by zero fails" "$rules" \
  "INSERT INTO $G SELECT 'loans', CAST(10 / d AS bigint) FROM $S" "DIVISION_BY_ZERO|failed"
tq_ok "sample: the markers read" "$sample" "SELECT v FROM $S LIMIT 10"
tq_refused "sample: a CAST of a marker fails" "$sample" "SELECT CAST(v AS bigint) FROM $S" "failed"

leaks=$(docker logs --since "$start" "$P-coordinator" 2>&1 | grep -c "$MARK")
expect "no marker in the coordinator's log" "0" "$leaks"
tq_refused "a person: system.runtime.queries" "$ana" "SELECT count(*) FROM system.runtime.queries" "PERMISSION_DENIED"
tq_refused "a job: system.runtime.queries" "$sample" "SELECT count(*) FROM system.runtime.queries" "PERMISSION_DENIED"
tq_ok "qd-admin: system.runtime.queries" "$admin" "SELECT count(*) FROM system.runtime.queries"

objects=$(docker exec -e AK="$(cat "$D/secrets/lake-access-key")" -e SK="$(cat "$D/secrets/lake-secret-key")" \
  "$P-lake" sh -c "curl -sS --user \"\$AK:\$SK\" --aws-sigv4 aws:amz:us-east-1:s3 \
  'http://127.0.0.1:9000/$WAREHOUSE?list-type=2'" | grep -o '<Key>[^<]*</Key>' | grep -vc '/data/\|/metadata/')
expect "no object outside the tables' data and metadata" "0" "$objects"

[ "$FAILED" = 0 ] && echo "ALL OK" || { echo "SOME CHECKS FAILED"; exit 1; }
