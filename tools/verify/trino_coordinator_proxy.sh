#!/usr/bin/env bash
# T14 of core decision 0090 (the loopback proxy, point 6.2.1), against
# engine-trino's coordinator files and its `lake-proxy` (proxy/dynamic.yml):
# a lake gate that requires a client certificate of the installation's CA
# (Traefik v3.6 before RustFS 1.0.5), the coordinator with no JVM keystore
# reaching the lake through the proxy in its network namespace, its rules
# over plain HTTP on the module's network.
#   - the lake reads and writes through the proxy; straight to the gate
#     without a certificate it is refused;
#   - the machine certificate renewed during a long INSERT and a rules
#     refresh: both go on, and neither Trino nor the proxy restarts.
# Not here: throughput direct against through the proxy.
#
# Run alone, never during a build, from the repository's root:
#   tools/verify/trino_coordinator_proxy.sh
# Containers and the network are `qo-verify-t14-*`, removed in a trap.
set -uo pipefail
P=qo-verify-t14
# shellcheck source=engine_trino_lib.sh
. "$(dirname "$0")/engine_trino_lib.sh"
TRAEFIK_IMAGE=${TRAEFIK_IMAGE:-traefik:v3.6}

renew() { # a new machine.pem for the coordinator, same name, same CA
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/O=qd/OU=app/CN=$P-coordinator" \
    -keyout "$D/n2.key" -out "$D/n2.csr" 2>/dev/null
  openssl x509 -req -in "$D/n2.csr" -CA "$D/node/ca.crt" -CAkey "$D/ca.key" -CAcreateserial \
    -days 1 -extfile "$D/node.ext" -out "$D/n2.crt" 2>/dev/null
  openssl pkcs8 -topk8 -nocrypt -in "$D/n2.key" -out "$D/n2.p8"
  cat "$D/n2.crt" "$D/n2.p8" >"$D/node/machine.pem.new"
  chmod 600 "$D/node/machine.pem.new"
  mv "$D/node/machine.pem.new" "$D/node/machine.pem"
  touch "$D/proxy/dynamic.yml"
}

et_pki
et_rustfs || exit 1
et_core
# The gate: TLS with its own certificate, client certificates of the CA only.
mkdir -p "$D/gate" "$D/proxy"
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/O=qd/OU=lake/CN=$P-gate" \
  -keyout "$D/gate/tls.key" -out "$D/gate.csr" 2>/dev/null
printf 'subjectAltName=DNS:%s\n' "$P-gate" >"$D/gate.ext"
openssl x509 -req -in "$D/gate.csr" -CA "$D/node/ca.crt" -CAkey "$D/ca.key" -CAcreateserial \
  -days 1 -extfile "$D/gate.ext" -out "$D/gate/tls.crt" 2>/dev/null
cp "$D/node/ca.crt" "$D/gate/ca.crt"
cat >"$D/gate/dynamic.yml" <<YML
tls:
  certificates: [{certFile: /gate/tls.crt, keyFile: /gate/tls.key}]
  options:
    default:
      clientAuth: {caFiles: [/gate/ca.crt], clientAuthType: RequireAndVerifyClientCert}
http:
  routers:
    lake: {entryPoints: [gate], rule: "PathPrefix(\`/\`)", service: lake, tls: {}}
  services:
    lake: {loadBalancer: {servers: [{url: "http://$P-lake:9000"}]}}
YML
chmod -R a+rX "$D/gate"
docker run -d --name "$P-gate" --network "$P" -v "$D/gate:/gate:ro" "$TRAEFIK_IMAGE" \
  --entryPoints.gate.address=:9443 --providers.file.filename=/gate/dynamic.yml >/dev/null
cp "$MODULE/proxy/dynamic.yml" "$D/proxy/dynamic.yml"
chmod 755 "$D/proxy"; chmod 644 "$D/proxy/dynamic.yml"

et_coordinator TRINO_LAKE_S3_URL=http://127.0.0.1:9000 || exit 1
docker run -d --name "$P-proxy" --network "container:$P-coordinator" --user 1000:1000 \
  -e QD_LAKE_GATE="$P-gate:9443" -e QD_LAKE_NAME="$P-gate" \
  -v "$D/proxy:/etc/qd/proxy:ro" -v "$D/node:/run/qd-node:ro" "$TRAEFIK_IMAGE" \
  --entryPoints.lake.address=127.0.0.1:9000 --providers.file.filename=/etc/qd/proxy/dynamic.yml \
  --providers.file.watch=true >/dev/null
sleep 3
admin=$(et_token qd-admin)

tq_ok "the lake through the proxy: a schema" "$admin" "CREATE SCHEMA lake.qo_verify"
tq_ok "the lake through the proxy: a table" "$admin" "CREATE TABLE lake.qo_verify.t (x bigint)"
expect "the gate without a certificate" "000" "$(docker exec "$P-coordinator" curl -s -o /dev/null \
  -w '%{http_code}' --cacert /run/qd-node/ca.crt "https://$P-gate:9443/" 2>/dev/null || true)"
expect "no JVM keystore in Trino's environment" "" "$(docker exec "$P-coordinator" bash -c 'echo ${JAVA_TOOL_OPTIONS:-}')"

trino_started=$(docker inspect -f '{{.State.StartedAt}}' "$P-coordinator")
proxy_started=$(docker inspect -f '{{.State.StartedAt}}' "$P-proxy")
docker exec -d -e T="$admin" "$P-coordinator" bash -c '/verify/tq.sh "INSERT INTO lake.qo_verify.t
  SELECT a * 4000000 + b * 2000 + c FROM UNNEST(sequence(1, 200)) x(a)
  CROSS JOIN UNNEST(sequence(1, 2000)) y(b) CROSS JOIN UNNEST(sequence(1, 50)) z(c)" >/data/trino/long.out 2>&1'
sleep 3
renew
info "machine.pem renewed during the INSERT"
for _ in $(seq 1 300); do
  out=$(docker exec "$P-coordinator" cat /data/trino/long.out 2>/dev/null); [ -n "$out" ] && break; sleep 1
done
expect "the INSERT went on" '[[20000000]]' "$out"
sleep 12
tq_ok "a statement after a rules refresh" "$admin" "SELECT count(*) FROM lake.qo_verify.t"
tq_ok "a new lake connection with the renewed certificate" "$admin" "INSERT INTO lake.qo_verify.t VALUES 1"
expect "Trino did not restart" "$trino_started" "$(docker inspect -f '{{.State.StartedAt}}' "$P-coordinator")"
expect "the proxy did not restart" "$proxy_started" "$(docker inspect -f '{{.State.StartedAt}}' "$P-proxy")"

[ "$FAILED" = 0 ] && echo "ALL OK" || { echo "SOME CHECKS FAILED"; exit 1; }
