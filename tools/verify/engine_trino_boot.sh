#!/usr/bin/env bash
# engine-trino's coordinator starts from modules/engine-trino/trino as
# written (decision 0011 step 11; core decision 0090 step 17's coordinator
# part): Trino 484 by digest, tier xs sized down, HTTPS with a machine
# certificate of a test CA, JWT of a stand-in core, rules from its URL, the
# resource groups and the session property manager, spill, RE2J, and the
# three catalogs over RustFS 1.0.5. Checks:
#   - it starts, and every rendered setting is accepted;
#   - a token's subject becomes the user of its kind (`job:…:rules:loans`
#     is job_rules_loans, `person-<uuid>` itself); a look-alike, another
#     audience and another issuer are refused;
#   - each user lands in its resource group; a rule user's SELECT is in
#     `reject` and does not run;
#   - the session property manager sets the BI group's limits;
#   - RE2J is the regular expression library; spill is on;
#   - `gold` refuses a write; `lake` takes one.
#
# Run alone, never during a build, from the repository's root:
#   tools/verify/engine_trino_boot.sh
# Containers, volumes and the network are `qo-verify-et-boot-*`, removed
# in a trap.
set -uo pipefail
P=qo-verify-et-boot
# shellcheck source=engine_trino_lib.sh
. "$(dirname "$0")/engine_trino_lib.sh"

et_pki
et_rustfs || exit 1
cat >"$D/rules.json" <<'JSON'
{
  "catalogs": [{ "allow": "all" }],
  "schemas": [{ "owner": true }],
  "tables": [{ "privileges": ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP"] }],
  "queries": [{ "allow": ["execute", "view", "kill"] }],
  "system_information": [{ "allow": ["read"] }]
}
JSON
et_rules "$D/rules.json"
et_core
et_coordinator || exit 1
ok "the coordinator started with tier xs's rendered files"

admin=$(et_token qd-admin)
person=$(et_token "person-00000000-0000-4000-8000-000000000001")
rules=$(et_token "job:00000000-0000-4000-8000-00000000000a:rules:loans")
tq_ok "qd-admin runs a statement" "$admin" "SELECT 1"
expect "a person's user" '[["person-00000000-0000-4000-8000-000000000001"]]' "$(tq "$person" 'SELECT current_user')"
tq_refused "a rule job's SELECT is not run (reject group)" "$rules" "SELECT 1" "failed"
tq_refused "a look-alike subject" "$(et_token job_rules_loans)" "SELECT 1" "http 401"
tq_refused "another audience" "$(et_token qd-admin other)" "SELECT 1" "http 401"
tq_refused "another issuer" "$(et_token qd-admin trino https://other.test)" "SELECT 1" "http 401"
tq_refused "an expired token" "$(et_token qd-admin trino https://core.test/api -60)" "SELECT 1" "http 401"

groups=$(tq "$admin" "SELECT user, resource_group_id FROM system.runtime.queries
  WHERE user IN ('qd-admin', 'person-00000000-0000-4000-8000-000000000001') ORDER BY created")
expect_match "qd-admin's group" '"qd-admin",\["global","admin"\]' "$groups"
expect_match "a person's group" '"person-[0-9a-f-]+",\["global","bi","person-' "$groups"
# The session the manager gave a person's query, read from its query info.
pid=$(tq "$admin" "SELECT query_id FROM system.runtime.queries WHERE user LIKE 'person-%' LIMIT 1" | tr -d '[]"')
expect_match "a person's session limits (BI, tier xs)" \
  "'query_max_memory': '600MB'.*|.*'spill_enabled': 'false'" \
  "$(docker exec -e T="$admin" "$P-coordinator" curl -sS --cacert /run/qd-node/ca.crt \
    -H "Authorization: Bearer $admin" "https://localhost:8490/v1/query/$pid" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["session"]["systemProperties"])')"
tq_refused "RE2J: a backreference is refused" "$admin" "SELECT regexp_like('aa', '(a)\\1')" "failed"
expect "spill on" "spill-enabled=true" \
  "$(docker exec "$P-coordinator" cat /data/trino/etc/config.properties | grep -o 'spill-enabled=true')"

tq_ok "lake: a schema" "$admin" "CREATE SCHEMA lake.qo_verify"
tq_ok "lake: a table" "$admin" "CREATE TABLE lake.qo_verify.t AS SELECT 1 AS x"
tq_refused "gold: no write" "$admin" "INSERT INTO gold.qo_verify.t VALUES 2" "failed"
tq_ok "gold: reads" "$admin" "SELECT count(*) FROM gold.qo_verify.t"

[ "$FAILED" = 0 ] && echo "ALL OK" || { echo "SOME CHECKS FAILED"; exit 1; }
