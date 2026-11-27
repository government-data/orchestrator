#!/usr/bin/env bash
# T15 of core decision 0090 (an abandoned query), against engine-trino's
# coordinator files on one node:
#   - a client that stops following an INSERT: the query runs on (its
#     state 10 s later is said); a fast one commits without its client;
#   - killed by its id, it stops before it commits;
#   - the INSERT run again leaves one copy;
#   - a coordinator restart mid-statement: no partial commit, and the
#     client's next page is gone.
#
# Run alone, never during a build, from the repository's root:
#   tools/verify/trino_abandoned_query.sh
# Containers and the network are `qo-verify-t15-*`, removed in a trap.
set -uo pipefail
P=qo-verify-t15
# shellcheck source=engine_trino_lib.sh
. "$(dirname "$0")/engine_trino_lib.sh"

et_pki
et_rustfs || exit 1
et_core
et_coordinator || exit 1
admin=$(et_token qd-admin)
T=lake.qo_verify.t
# A slow INSERT of one row: 60 M hashes.
INSERT="INSERT INTO $T SELECT sum(length(to_hex(sha256(to_utf8(CAST(a * 1000000 + b * 1000 + c AS varchar))))))
  FROM UNNEST(sequence(1, 60)) x(a) CROSS JOIN UNNEST(sequence(1, 1000)) y(b) CROSS JOIN UNNEST(sequence(1, 1000)) z(c)"
FAST="INSERT INTO $T SELECT a FROM UNNEST(sequence(1, 1000)) x(a)"
tq_ok "setup" "$admin" "CREATE SCHEMA lake.qo_verify"
tq_ok "setup" "$admin" "CREATE TABLE $T (x bigint)"
running() {
  tq "$admin" "SELECT query_id, state FROM system.runtime.queries WHERE query LIKE 'INSERT INTO $T%'
    AND state NOT IN ('FINISHED', 'FAILED') LIMIT 1"
}

# The client follows 3 s, then stops.
docker exec -e T="$admin" "$P-coordinator" timeout 3 /verify/tq.sh "$INSERT" >/dev/null 2>&1
sleep 10
now=$(running)
info "10 s after the client left: $now"
qid=$(printf '%s' "$now" | sed -n 's/^\[\["\([^"]*\)".*/\1/p')
if [ -n "$qid" ]; then
  docker exec "$P-coordinator" curl -s -o /dev/null -X DELETE --cacert /run/qd-node/ca.crt \
    -H "Authorization: Bearer $admin" "https://localhost/v1/query/$qid"
  for _ in $(seq 1 30); do [ -z "$(running)" ] && break; sleep 1; done
  expect "killed by id: nothing committed" '[[0]]' "$(tq "$admin" "SELECT count(*) FROM $T")"
else
  info "the abandoned query had already ended"
  info "rows after it: $(tq "$admin" "SELECT count(*) FROM $T")"
fi
# An empty table's DELETE fails its commit (core 0090 T2): only when rows are there.
[ "$(tq "$admin" "SELECT count(*) FROM $T")" = '[[0]]' ] || tq_ok "setup: empty again" "$admin" "DELETE FROM $T"
tq_ok "run again to its end" "$admin" "$INSERT"
expect "one copy" '[[1]]' "$(tq "$admin" "SELECT count(*) FROM $T")"
# A fast INSERT whose client leaves before the last page: committed, and
# the query waits in FINISHING for a client that never comes.
docker exec -e T="$admin" "$P-coordinator" timeout 1 /verify/tq.sh "$FAST" >/dev/null 2>&1
sleep 5
info "a fast INSERT whose client left: $(running), rows $(tq "$admin" "SELECT count(*) FROM $T")"
tq_ok "setup: one row again" "$admin" "DELETE FROM $T WHERE x <= 1000"

# A restart of the coordinator while an INSERT runs.
docker exec -d -e T="$admin" "$P-coordinator" bash -c "/verify/tq.sh \"$INSERT\" >/data/trino/restart.out 2>&1"
sleep 5
docker restart -t 10 "$P-coordinator" >/dev/null
for _ in $(seq 1 90); do
  docker logs --since 1m "$P-coordinator" 2>&1 | grep -q "SERVER STARTED" && break; sleep 2
done
info "the client's answer across the restart: $(docker exec "$P-coordinator" cat /data/trino/restart.out 2>/dev/null || echo '(the client died with the container)')"
for _ in $(seq 1 30); do out=$(tq "$admin" "SELECT count(*) FROM $T"); case "$out" in '[['*) break ;; esac; sleep 2; done
expect "no partial commit across the restart" '[[1]]' "$out"
# A page of a query from before the restart.
code=$(docker exec "$P-coordinator" curl -s -o /dev/null -w '%{http_code}' --cacert /run/qd-node/ca.crt \
  -H "Authorization: Bearer $admin" "https://localhost/v1/statement/executing/20261125_000000_00001_aaaaa/y0/1")
expect "a page of a query the coordinator no longer knows" "404" "$code"

[ "$FAILED" = 0 ] && echo "ALL OK" || { echo "SOME CHECKS FAILED"; exit 1; }
