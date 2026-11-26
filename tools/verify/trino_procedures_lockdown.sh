#!/usr/bin/env bash
# T6 of core decision 0090 (procedures locked down), against engine-trino's
# coordinator files and trino-rules' document; procedures are blocked by
# query type and the rules document, not by text (ruled by the user
# 2026-11-25):
#   - a person on `gold`: `SELECT` runs, `ALTER TABLE EXECUTE` and `CALL`
#     refused;
#   - `ALTER TABLE EXECUTE` from any user but `job_maint_*` is in `reject`,
#     whatever its spelling (quoted, mixed case, commented);
#   - `lake` at 36500 d: snapshot expiry and orphan removal refused even
#     to qd-admin;
#   - `job_maint_loans` on `lake_maint`: `optimize`, `expire_snapshots`
#     (default minimum), then an `INSERT` accepted; `remove_orphan_files`
#     refused; what `optimize_manifests` does is said.
# Not here: the snapshot growth per month (needs the reference workload).
#
# Run alone, never during a build, from the repository's root:
#   RENDER=<target>/debug/examples/render tools/verify/trino_procedures_lockdown.sh
# Containers and the network are `qo-verify-t6-*`, removed in a trap.
set -uo pipefail
P=qo-verify-t6
# shellcheck source=engine_trino_lib.sh
. "$(dirname "$0")/engine_trino_lib.sh"
: "${RENDER:?set RENDER to the render example of trino-rules}"

ANA=person-00000000-0000-4000-8000-00000000000a
"$RENDER" >"$D/rules.json" 2>"$D/render.err" <<JSON || { cat "$D/render.err"; exit 1; }
{"core": [
  {"subject": "job_maint_loans", "namespace": "loans_bronze", "tables": ["*"],
   "privileges": ["SELECT", "INSERT", "DELETE"]},
  {"subject": "job_rules_loans", "namespace": "quality_gold", "tables": ["*"],
   "privileges": ["SELECT", "INSERT", "DELETE"], "rows": {"column": "dataset", "in": ["loans"]}},
  {"subject": "$ANA", "namespace": "quality_gold", "tables": ["*"], "privileges": ["SELECT"],
   "rows": {"column": "dataset", "in": ["loans"]}}
]}
JSON
et_pki
et_rustfs || exit 1
et_rules "$D/rules.json"
et_core
et_coordinator || exit 1

admin=$(et_token qd-admin)
ana=$(et_token "$ANA")
rules=$(et_token "job:00000000-0000-4000-8000-00000000000a:rules:loans")
maint=$(et_token "job:00000000-0000-4000-8000-00000000000a:maint:loans")
G=quality_gold.run_summary
B=loans_bronze.records
for q in "CREATE SCHEMA lake.quality_gold" "CREATE SCHEMA lake.loans_bronze" \
  "CREATE TABLE lake.$G (dataset varchar, n bigint)" "INSERT INTO lake.$G VALUES ('loans', 1)" \
  "CREATE TABLE lake.$B (id bigint)" "INSERT INTO lake.$B VALUES 1" "INSERT INTO lake.$B VALUES 2"; do
  tq_ok "setup" "$admin" "$q"
done

tq_ok "a person: SELECT on gold" "$ana" "SELECT count(*) FROM gold.$G"
tq_refused "a person: ALTER TABLE EXECUTE on gold" "$ana" "ALTER TABLE gold.$G EXECUTE optimize" "failed"
tq_refused "a person: CALL" "$ana" "CALL gold.system.rollback_to_snapshot('quality_gold', 'run_summary', 1)" "failed"
for q in "ALTER TABLE lake.$G EXECUTE optimize" "alter TABLE \"lake\".\"quality_gold\".\"run_summary\" ExEcUtE \"optimize\"" \
  "/* x */ ALTER TABLE lake.$G -- y
  EXECUTE optimize"; do
  tq_refused "a rule job: '$(printf '%s' "$q" | head -c 40)…' in reject" "$rules" "$q" "QUERY_QUEUE_FULL"
done
tq_refused "qd-admin: ALTER TABLE EXECUTE is in reject too" "$admin" "ALTER TABLE lake.$G EXECUTE optimize" "QUERY_QUEUE_FULL"

tq_ok "maint: optimize on lake_maint" "$maint" "ALTER TABLE lake_maint.$B EXECUTE optimize"
tq_ok "maint: expire_snapshots (7 d)" "$maint" "ALTER TABLE lake_maint.$B EXECUTE expire_snapshots(retention_threshold => '7d')"
tq_ok "maint: an INSERT afterwards" "$maint" "INSERT INTO lake_maint.$B VALUES 3"
tq_refused "maint: remove_orphan_files (36500 d)" "$maint" \
  "ALTER TABLE lake_maint.$B EXECUTE remove_orphan_files(retention_threshold => '7d')" "failed"
tq_refused "maint: expiry on lake (36500 d)" "$maint" \
  "ALTER TABLE lake.$B EXECUTE expire_snapshots(retention_threshold => '7d')" "failed"
info "maint: optimize_manifests answered: $(tq "$maint" "ALTER TABLE lake_maint.$B EXECUTE optimize_manifests")"
expect "maint: the table still reads" '[[3]]' "$(tq "$maint" "SELECT count(*) FROM lake_maint.$B")"
tq_refused "maint: SET SESSION of a minimum retention" "$maint" \
  "SET SESSION lake_maint.expire_snapshots_min_retention = '0s'" "failed"

[ "$FAILED" = 0 ] && echo "ALL OK" || { echo "SOME CHECKS FAILED"; exit 1; }
