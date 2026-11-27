# shellcheck shell=bash
# Shared helpers of engine-trino's verification scripts: a throwaway
# network, RustFS 1.0.5 (one warehouse, its root keys as Trino's lake user),
# a test CA with the coordinator's machine certificate, the core's stand-in
# (an ES256 key, its JWKS and a rules document served by the JDK's
# `jwebserver`), and the coordinator started from
# modules/engine-trino/trino as compose.yml starts it. Tokens are made
# here with the stand-in's key. No port is published; every call runs
# inside a container. Synthetic data only; keys are random, kept under "$D"
# and never printed. Not run by itself: a script sets P (qo-verify-…) and
# sources this file from the repository's root.

: "${P:?set P to qo-verify-<name> before sourcing}"
TRINO_IMAGE=${TRINO_IMAGE:-trinodb/trino:484@sha256:2704eb53935af5e582d63d6033e9bf99d20c325e9b0e30b1016ef59a4773979f}
RUSTFS_IMAGE=${RUSTFS_IMAGE:-rustfs/rustfs:1.0.5}
WAREHOUSE=${WAREHOUSE:-qo-verify-warehouse}
MODULE=$PWD/modules/engine-trino
FAILED=0
D=$(mktemp -d)
chmod 700 "$D"

ok() { printf '[OK  ] %s\n' "$*"; }
ko() { printf '[FAIL] %s\n' "$*"; FAILED=1; }
info() { printf '[INFO] %s\n' "$*"; }
expect() { # NAME EXPECTED ACTUAL
  if [ "$2" = "$3" ]; then ok "$1: $3"; else ko "$1: expected '$2', got '$3'"; fi
}
expect_match() { # NAME REGEX ACTUAL
  if printf '%s' "$3" | grep -Eq "$2"; then ok "$1: $(printf '%s' "$3" | tr '\n' ' ' | head -c 200)"; else
    ko "$1: expected /$2/, got '$(printf '%s' "$3" | tr '\n' ' ' | head -c 300)'"
  fi
}

et_cleanup() {
  [ "${KEEP:-0}" = 1 ] && { echo "kept: $P-* and $D"; return; }
  docker ps -aq --filter "name=^$P-" | xargs -r docker rm -f -v >/dev/null 2>&1
  docker volume ls -q --filter "name=^$P-" | xargs -r docker volume rm >/dev/null 2>&1
  docker network rm "$P" >/dev/null 2>&1
  rm -rf "$D"
}
trap et_cleanup EXIT
trap 'exit 130' INT TERM
docker network create "$P" >/dev/null || exit 1

# --- the CA, the coordinator's certificate, the stand-in's signing key -------
et_pki() {
  mkdir -p "$D/node" "$D/core" "$D/secrets"
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 1 \
    -subj "/O=qd/CN=qo-verify-ca" -keyout "$D/ca.key" -out "$D/node/ca.crt" 2>/dev/null
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/O=qd/OU=app/CN=$P-coordinator" \
    -keyout "$D/node.key" -out "$D/node.csr" 2>/dev/null
  printf 'subjectAltName=DNS:%s,DNS:localhost\n' "$P-coordinator" >"$D/node.ext"
  openssl x509 -req -in "$D/node.csr" -CA "$D/node/ca.crt" -CAkey "$D/ca.key" -CAcreateserial \
    -days 1 -extfile "$D/node.ext" -out "$D/node.crt" 2>/dev/null
  openssl pkcs8 -topk8 -nocrypt -in "$D/node.key" -out "$D/node.p8"
  cat "$D/node.crt" "$D/node.p8" >"$D/node/machine.pem"
  openssl ecparam -name prime256v1 -genkey -noout -out "$D/jwt.key" 2>/dev/null
  openssl rand -base64 32 | tr -d '\n' >"$D/secrets/trino-shared-secret"
  python3 - "$D/jwt.key" >"$D/core/jwks.json" <<'PY'
import base64, subprocess, sys, json
der = subprocess.run(["openssl", "ec", "-in", sys.argv[1], "-pubout", "-outform", "DER"],
                     capture_output=True, check=True).stdout
point = der[-64:]
b = lambda x: base64.urlsafe_b64encode(x).rstrip(b"=").decode()
print(json.dumps({"keys": [{"kty": "EC", "crv": "P-256", "kid": "qo-verify", "alg": "ES256",
                            "use": "sig", "x": b(point[:32]), "y": b(point[32:])}]}))
PY
}

# et_token SUB [AUD] [ISS] [EXP_SECS]: a JWT of the stand-in's key.
et_token() {
  python3 - "$D/jwt.key" "$1" "${2:-trino}" "${3:-https://core.test/api}" "${4:-900}" <<'PY'
import base64, json, subprocess, sys, time
key, sub, aud, iss, ttl = sys.argv[1:]
b = lambda x: base64.urlsafe_b64encode(x).rstrip(b"=").decode()
now = int(time.time())
head = b(json.dumps({"alg": "ES256", "kid": "qo-verify", "typ": "JWT"}).encode())
body = b(json.dumps({"sub": sub, "aud": aud, "iss": iss, "iat": now, "exp": now + int(ttl)}).encode())
der = subprocess.run(["openssl", "dgst", "-sha256", "-sign", key], input=f"{head}.{body}".encode(),
                     capture_output=True, check=True).stdout
# DER ECDSA signature to the raw r || s that JWS takes.
def ints(d):
    i, out = 2, []
    for _ in range(2):
        n = d[i + 1]; v = d[i + 2:i + 2 + n]; out.append(v.lstrip(b"\0").rjust(32, b"\0")); i += 2 + n
    return out
r, s = ints(der)
print(f"{head}.{body}.{b(r + s)}")
PY
}

# --- RustFS, one warehouse ----------------------------------------------------
et_rustfs() {
  printf '%s' "qo$(openssl rand -hex 8)" >"$D/secrets/lake-query-engine-access-key"
  openssl rand -hex 20 | tr -d '\n' >"$D/secrets/lake-query-engine-secret-key"
  docker run -d --name "$P-lake" --network "$P" --memory 1g \
    -e RUSTFS_ACCESS_KEY="$(cat "$D/secrets/lake-query-engine-access-key")" \
    -e RUSTFS_SECRET_KEY="$(cat "$D/secrets/lake-query-engine-secret-key")" \
    -e RUSTFS_VOLUMES=/data "$RUSTFS_IMAGE" server --address :9000 >/dev/null || return 1
  for _ in $(seq 1 30); do
    docker exec "$P-lake" curl -s -o /dev/null http://127.0.0.1:9000/health 2>/dev/null && break
    sleep 1
  done
  local p
  for p in "/$WAREHOUSE" "/iceberg/v1/buckets/$WAREHOUSE"; do
    docker exec -e AK="$(cat "$D/secrets/lake-query-engine-access-key")" -e SK="$(cat "$D/secrets/lake-query-engine-secret-key")" \
      "$P-lake" sh -c "curl -sS -o /dev/null -w '%{http_code}' -X PUT --user \"\$AK:\$SK\" \
      --aws-sigv4 aws:amz:us-east-1:s3 http://127.0.0.1:9000$p" | grep -q 200 || {
      echo "warehouse: $p refused"; return 1
    }
  done
}

# --- the core's stand-in: JWKS and the rules document --------------------------
# et_rules FILE: the rules document the coordinator reads from now on.
et_rules() { cp "$1" "$D/core/rules.json"; chmod 644 "$D/core/rules.json"; }
et_core() {
  [ -f "$D/core/rules.json" ] || printf '{"catalogs":[{"allow":"all"}]}\n' >"$D/core/rules.json"
  chmod 755 "$D/core"; chmod 644 "$D/core"/*
  docker run -d --name "$P-core" --network "$P" --memory 256m --entrypoint jwebserver \
    -v "$D/core:/srv:ro" "$TRINO_IMAGE" -b 0.0.0.0 -p 8000 -d /srv >/dev/null
}

# et_coordinator [ENV=VALUE…]: the module's coordinator, tier xs sized down
# for this machine unless the caller says otherwise.
et_coordinator() {
  local envs=() e
  for e in TRINO_TIER=xs TRINO_HEAP=2G TRINO_HEADROOM=600MB TRINO_NODE_CAP=1GB TRINO_QUERY_CAP=1GB "$@"; do
    envs+=(-e "$e")
  done
  chmod 755 "$D" "$D/node" "$D/secrets"
  chmod 644 "$D/node/ca.crt" "$D/secrets"/*
  # As the agent writes it: 0600, Trino's user's (uid 1000, this user's).
  chmod 600 "$D/node/machine.pem"
  et_tq_script
  docker run -d --name "$P-coordinator" --hostname "$P-coordinator" --network "$P" --memory 3g \
    --sysctl net.ipv4.ip_unprivileged_port_start=443 \
    --entrypoint /etc/trino-module/entrypoint.sh \
    -e TRINO_NODE_ID=coordinator -e TRINO_INTERNAL_ADDRESS="$P-coordinator" \
    -e TRINO_DISCOVERY_URI="https://$P-coordinator" \
    -e TRINO_JWT_ISSUER=https://core.test/api -e TRINO_JWKS_URL="http://$P-core:8000/jwks.json" \
    -e TRINO_RULES_URL="http://$P-core:8000/rules.json" \
    -e TRINO_LAKE_S3_URL="http://$P-lake:9000" -e TRINO_LAKE_WAREHOUSE="$WAREHOUSE" \
    "${envs[@]}" \
    -v "$MODULE/trino:/etc/trino-module:ro" -v "$D/node:/run/qd-node:ro" -v "$D/bin:/verify:ro" \
    -v "$D/secrets/trino-shared-secret:/run/secrets/trino-shared-secret:ro" \
    -v "$D/secrets/lake-query-engine-access-key:/run/secrets/lake-query-engine-access-key:ro" \
    -v "$D/secrets/lake-query-engine-secret-key:/run/secrets/lake-query-engine-secret-key:ro" \
    --tmpfs /spill:uid=1000,gid=1000 "$TRINO_IMAGE" >/dev/null || return 1
  for _ in $(seq 1 90); do
    docker logs "$P-coordinator" 2>&1 | grep -q "SERVER STARTED" && return 0
    docker ps -q --filter "name=^$P-coordinator$" | grep -q . || break
    sleep 2
  done
  echo "the coordinator did not start:"; docker logs "$P-coordinator" 2>&1 | tail -4 | cut -c1-3000
  return 1
}

# The client run inside the coordinator: curl over HTTPS with a token.
et_tq_script() {
  mkdir -p "$D/bin"
  cat >"$D/bin/tq.sh" <<'SH'
#!/bin/bash
# tq.sh SQL (token in T): the statement polled to its end; the rows as JSON,
# or "failed: <error name>" or "http <code>", exit 1.
call() { curl -sS -w '\n%{http_code}' --cacert /run/qd-node/ca.crt -H "Authorization: Bearer $T" "$@"; }
r=$(call --data-binary "$1" https://localhost/v1/statement)
while :; do
  code=${r##*$'\n'} body=${r%$'\n'*}
  [ "$code" = 200 ] || { echo "http $code"; exit 1; }
  [[ $body =~ \"errorName\":\"([A-Z_]+)\" ]] && { echo "failed: ${BASH_REMATCH[1]}"; exit 1; }
  [[ $body =~ \"data\":(\[.*\]),\"stats\" ]] && echo "${BASH_REMATCH[1]}"
  [[ $body =~ \"nextUri\":\"([^\"]+)\" ]] || exit 0
  sleep 0.2
  r=$(call "${BASH_REMATCH[1]}")
done
SH
  chmod 755 "$D/bin" "$D/bin/tq.sh"
}
# tq TOKEN SQL: as tq.sh, inside the coordinator.
tq() { docker exec -e T="$1" "$P-coordinator" /verify/tq.sh "$2"; }
tq_ok() { # NAME TOKEN SQL
  local out
  if out=$(tq "$2" "$3"); then ok "$1: $(printf '%s' "$out" | tr '\n' ' ' | head -c 200)"; else ko "$1: $out"; fi
}
tq_refused() { # NAME TOKEN SQL [REGEX]
  local out
  if out=$(tq "$2" "$3"); then ko "$1: expected a refusal, it ran: $(printf '%s' "$out" | head -c 200)"; else
    expect_match "$1 (refused)" "${4:-.}" "$out"
  fi
}
