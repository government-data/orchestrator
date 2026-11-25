#!/usr/bin/env bash
# T4 of core decision 0090 (row filters), against engine-trino's
# coordinator files and the rules document trino-rules makes from grants
# shaped as the core's (decision 0011 step 11; core 0089 step 6's check):
#   - job_rules_loans (SELECT, INSERT and DELETE on gold, filter dataset =
#     'loans'; with INSERT and DELETE alone, RULES_PRIVILEGES='"INSERT",
#     "DELETE"', Trino refuses every write and delete):
#     a `savings` row refused, a `loans` row written; DELETE sees only
#     `loans` rows; UPDATE, MERGE and ALTER TABLE EXECUTE refused;
#   - a person's filter on SELECT, count(*), GROUP BY and SHOW STATS;
#     `$partitions` and `$files` refused; information_schema lists tables;
#   - a withdrawn grant stops rows within the refresh period;
#   - bi-superset lists gold's tables and reads no row.
# Not here: "two file systems" (one rules file, one access control).
#
# Run alone, never during a build, from the repository's root, with the
# rules renderer built (cargo build -p trino-rules --example render):
#   RENDER=<target>/debug/examples/render tools/verify/trino_row_filters.sh
# Containers and the network are `qo-verify-t4-*`, removed in a trap.
set -uo pipefail
P=qo-verify-t4
# shellcheck source=engine_trino_lib.sh
. "$(dirname "$0")/engine_trino_lib.sh"
: "${RENDER:?set RENDER to the render example of trino-rules}"

ANA=person-00000000-0000-4000-8000-00000000000a
BRUNO=person-00000000-0000-4000-8000-00000000000b
PRIVILEGES=${RULES_PRIVILEGES:-'"SELECT", "INSERT", "DELETE"'}
grants() { # people's grants as JSON, then the document
  "$RENDER" >"$D/rules.json" 2>"$D/render.err" <<JSON || { cat "$D/render.err"; return 1; }
{"core": [
  {"subject": "job_rules_loans", "namespace": "quality_gold", "tables": ["*"],
   "privileges": [$PRIVILEGES],
   "rows": {"column": "dataset", "in": ["loans"]}},
  $1
]}
JSON
  et_rules "$D/rules.json"
}
both="{\"subject\": \"$ANA\", \"namespace\": \"quality_gold\", \"tables\": [\"*\"],
  \"privileges\": [\"SELECT\"], \"rows\": {\"column\": \"dataset\", \"in\": [\"loans\"]}},
  {\"subject\": \"$BRUNO\", \"namespace\": \"quality_gold\", \"tables\": [\"*\"],
  \"privileges\": [\"SELECT\"], \"rows\": {\"column\": \"dataset\", \"in\": [\"savings\"]}}"

et_pki
et_rustfs || exit 1
grants "$both" || exit 1
et_core
et_coordinator || exit 1

admin=$(et_token qd-admin)
rules=$(et_token "job:00000000-0000-4000-8000-00000000000a:rules:loans")
ana=$(et_token "$ANA")
bruno=$(et_token "$BRUNO")
bi=$(et_token bi-superset)
G=lake.quality_gold.run_summary
tq_ok "setup: schema" "$admin" "CREATE SCHEMA lake.quality_gold"
tq_ok "setup: table" "$admin" "CREATE TABLE $G (dataset varchar, cut varchar, n bigint)
  WITH (partitioning = ARRAY['dataset'])"
tq_ok "setup: rows" "$admin" "INSERT INTO $G VALUES ('loans','c1',1),('loans','c2',2),('savings','c1',10)"

tq_refused "rules: a savings row" "$rules" "INSERT INTO $G VALUES ('savings','c9',9)" "PERMISSION_DENIED"
tq_ok "rules: a loans row" "$rules" "INSERT INTO $G VALUES ('loans','c3',3)"
tq_refused "rules: UPDATE" "$rules" "UPDATE $G SET n = 0" "PERMISSION_DENIED|NOT_SUPPORTED|failed"
tq_refused "rules: MERGE" "$rules" "MERGE INTO $G t USING (VALUES 'loans') s(d) ON t.dataset = s.d
  WHEN MATCHED THEN DELETE" "failed"
tq_refused "rules: ALTER TABLE EXECUTE" "$rules" "ALTER TABLE $G EXECUTE optimize" "failed"
out=$(tq "$rules" "DELETE FROM $G WHERE cut = 'c1'")
info "rules: DELETE WHERE cut = 'c1' answered: $out"
expect "rules: DELETE left savings' c1 and loans' others" '[["loans",2],["savings",1]]' \
  "$(tq "$admin" "SELECT dataset, count(*) FROM $G GROUP BY dataset ORDER BY 1" | tr -d '\n')"
expect "rules: DELETE of the whole table only drops loans" '[["savings",1]]' \
  "$(tq "$rules" "DELETE FROM $G" >/dev/null; tq "$admin" "SELECT dataset, count(*) FROM $G GROUP BY 1")"
tq_ok "setup: loans again" "$admin" "INSERT INTO $G VALUES ('loans','c1',1),('loans','c2',2)"

g=gold.quality_gold.run_summary
expect "ana: SELECT" '[["loans"],["loans"]]' "$(tq "$ana" "SELECT dataset FROM $g ORDER BY cut")"
expect "ana: count(*)" '[[2]]' "$(tq "$ana" "SELECT count(*) FROM $g")"
expect "bruno: GROUP BY" '[["savings",1]]' "$(tq "$bruno" "SELECT dataset, count(*) FROM $g GROUP BY 1")"
info "ana: SHOW STATS answered: $(tq "$ana" "SHOW STATS FOR $g" | head -c 300)"
tq_refused "ana: \$partitions" "$ana" "SELECT * FROM gold.quality_gold.\"run_summary\$partitions\"" "PERMISSION_DENIED"
tq_refused "ana: \$files" "$ana" "SELECT * FROM gold.quality_gold.\"run_summary\$files\"" "PERMISSION_DENIED"
expect "ana: information_schema lists the table" '[["run_summary"]]' \
  "$(tq "$ana" "SELECT table_name FROM gold.information_schema.tables WHERE table_schema = 'quality_gold'")"
tq_refused "ana: the lake catalog" "$ana" "SELECT count(*) FROM $G" "PERMISSION_DENIED"
tq_refused "ana: a write" "$ana" "INSERT INTO $g VALUES ('loans','c9',9)" "failed"
expect "bi-superset: lists" '[["run_summary"]]' \
  "$(tq "$bi" "SELECT table_name FROM gold.information_schema.tables WHERE table_schema = 'quality_gold'")"
expect "bi-superset: no row" '' "$(tq "$bi" "SELECT * FROM $g")"

grants "{\"subject\": \"$BRUNO\", \"namespace\": \"quality_gold\", \"tables\": [\"*\"],
  \"privileges\": [\"SELECT\"], \"rows\": {\"column\": \"dataset\", \"in\": [\"savings\"]}}"
start=$(date +%s)
while :; do
  out=$(tq "$ana" "SELECT count(*) FROM $g")
  case "$out" in '[[2]]') ;; *) break ;; esac
  [ $(($(date +%s) - start)) -gt 40 ] && break
  sleep 1
done
expect_match "ana's grant withdrawn: no row within the refresh ($(($(date +%s) - start)) s)" \
  'PERMISSION_DENIED|^\[\[0\]\]$' "$out"

[ "$FAILED" = 0 ] && echo "ALL OK" || { echo "SOME CHECKS FAILED"; exit 1; }
