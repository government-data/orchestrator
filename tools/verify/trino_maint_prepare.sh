#!/usr/bin/env bash
# The `prepare` job's scope (core decision 0090, ruling 66), against
# engine-trino's coordinator files and the rules trino-rules makes from
# grants shaped as the core's: `job_maint_loans` owns the tables of its own
# bronze and silver schemas on `lake_maint`, so it
#   - creates a table there (`CREATE TABLE IF NOT EXISTS`, twice), adds a
#     column (`ADD COLUMN IF NOT EXISTS`, twice) and writes a row;
#   - is refused another dataset's schema, gold, the `lake` catalog and a
#     new schema;
# and `job_rules_loans` and `job_extract_loans` create no table. What a
# table owner may do besides (DROP TABLE, RENAME) is said, not judged.
#
# Run alone, never during a build, from the repository's root, with the
# rules renderer built (cargo build -p trino-rules --example render):
#   RENDER=<target>/debug/examples/render tools/verify/trino_maint_prepare.sh
# Containers and the network are `qo-verify-prep-*`, removed in a trap.
set -uo pipefail
P=qo-verify-prep
# shellcheck source=engine_trino_lib.sh
. "$(dirname "$0")/engine_trino_lib.sh"
: "${RENDER:?set RENDER to the render example of trino-rules}"

"$RENDER" >"$D/rules.json" 2>"$D/render.err" <<'JSON' || { cat "$D/render.err"; exit 1; }
{"core": [
  {"subject": "job_maint_loans", "namespace": "loans_bronze", "tables": ["*"],
   "privileges": ["SELECT", "INSERT", "DELETE"]},
  {"subject": "job_maint_loans", "namespace": "loans_silver", "tables": ["*"],
   "privileges": ["SELECT", "INSERT", "DELETE"]},
  {"subject": "job_maint_gold", "namespace": "quality_gold", "tables": ["*"],
   "privileges": ["SELECT", "DELETE"]},
  {"subject": "job_rules_loans", "namespace": "loans_silver", "tables": ["*"], "privileges": ["SELECT"]},
  {"subject": "job_extract_loans", "namespace": "loans_bronze", "tables": ["*"],
   "privileges": ["SELECT", "INSERT", "DELETE"]}
]}
JSON
et_pki
et_rustfs || exit 1
et_rules "$D/rules.json"
et_core
et_coordinator || exit 1

admin=$(et_token qd-admin)
maint=$(et_token "job:00000000-0000-4000-8000-00000000000a:maint:loans")
rules=$(et_token "job:00000000-0000-4000-8000-00000000000a:rules:loans")
extract=$(et_token "job:00000000-0000-4000-8000-00000000000a:extract:loans")
for s in loans_bronze loans_silver savings_bronze quality_gold; do
  tq_ok "setup: schema $s" "$admin" "CREATE SCHEMA lake.$s"
done

B=lake_maint.loans_bronze.records
S=lake_maint.loans_silver.records
for t in $B $S; do
  tq_ok "maint: create $t" "$maint" "CREATE TABLE IF NOT EXISTS $t (code varchar)"
  tq_ok "maint: create $t again" "$maint" "CREATE TABLE IF NOT EXISTS $t (code varchar)"
  tq_ok "maint: add a column to $t" "$maint" "ALTER TABLE $t ADD COLUMN IF NOT EXISTS amount bigint"
  tq_ok "maint: add it again" "$maint" "ALTER TABLE $t ADD COLUMN IF NOT EXISTS amount bigint"
  tq_ok "maint: a row in $t" "$maint" "INSERT INTO $t VALUES ('a', 1)"
done
expect "admin: the bronze table reads through lake" '[["a",1]]' \
  "$(tq "$admin" "SELECT code, amount FROM lake.loans_bronze.records")"

tq_refused "maint: another dataset's bronze" "$maint" \
  "CREATE TABLE lake_maint.savings_bronze.records (code varchar)" "failed"
tq_refused "maint: a table in gold" "$maint" \
  "CREATE TABLE lake_maint.quality_gold.records (code varchar)" "failed"
tq_refused "maint: its bronze through lake" "$maint" \
  "CREATE TABLE IF NOT EXISTS lake.loans_bronze.other (code varchar)" "failed"
tq_refused "maint: a new schema" "$maint" "CREATE SCHEMA lake_maint.loans_extra" "failed"
tq_refused "rules: a table in silver" "$rules" \
  "CREATE TABLE lake.loans_silver.other (code varchar)" "failed"
tq_refused "extract: a table in bronze" "$extract" \
  "CREATE TABLE lake.loans_bronze.other (code varchar)" "failed"
tq_refused "extract: a column in bronze" "$extract" \
  "ALTER TABLE lake.loans_bronze.records ADD COLUMN other varchar" "failed"

tq_ok "maint: a scratch table to drop" "$maint" "CREATE TABLE lake_maint.loans_silver.gone (code varchar)"
info "maint: DROP TABLE of its own answered: $(tq "$maint" "DROP TABLE lake_maint.loans_silver.gone")"
info "maint: RENAME of its own answered: $(tq "$maint" "ALTER TABLE $S RENAME TO lake_maint.loans_silver.moved")"

[ "$FAILED" = 0 ] && echo "ALL OK" || { echo "SOME CHECKS FAILED"; exit 1; }
