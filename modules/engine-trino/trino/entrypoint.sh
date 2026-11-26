#!/bin/bash
# Starts engine-trino's coordinator (decision 0011): bash, coreutils, curl
# and the JDK only, as the Trino image ships no sed or grep.
#
#   - the tier's settings (tiers.sh) render Trino's etc directory under
#     /data/trino/etc from this module's files (/etc/trino-module);
#   - the internal communication's shared secret and the lake user's keys
#     are read from their secret files into the environment Trino reads
#     (`${ENV:…}`), never printed;
#   - the static catalogs are copied where `CREATE CATALOG` writes;
#   - the core's public keys for audience `trino` are kept as one PEM file
#     per key id, read again every few minutes.
set -euo pipefail
MODULE=/etc/trino-module
# shellcheck source=tiers.sh
. "$MODULE/tiers.sh"
tier

ETC=/data/trino/etc
mkdir -p "$ETC" /data/trino/catalog
chmod 700 /data/trino/catalog
cp "$MODULE"/etc/*.properties "$MODULE/etc/user-mapping.json" "$ETC/"
render "$MODULE/etc/jvm.config.in" "$ETC/jvm.config"
render "$MODULE/etc/resource-groups.json.in" "$ETC/resource-groups.json"
render "$MODULE/etc/session-property-config.json.in" "$ETC/session-property-config.json"
cat >>"$ETC/config.properties" <<PROPS

# Tier ${TRINO_TIER:-s} (tiers.sh): the pool is the heap less the headroom;
# the rule jobs' cap per node and per query; spill on its own volume.
memory.heap-headroom-per-node=${TRINO_HEADROOM}
query.max-memory-per-node=${TRINO_NODE_CAP}
query.max-memory=${TRINO_QUERY_CAP}
spill-enabled=true
spiller-spill-path=/spill
spill-compression-codec=LZ4
retry-policy=${TRINO_RETRY_POLICY}
PROPS
cp "$MODULE"/etc/catalog/*.properties /data/trino/catalog/

secret() { # the first line of a secret file
  local line
  IFS= read -r line <"$1" || [ -n "$line" ]
  printf '%s' "$line"
}
TRINO_SHARED_SECRET=$(secret /run/secrets/trino-shared-secret)
TRINO_LAKE_ACCESS_KEY=$(secret /run/secrets/lake-access-key)
TRINO_LAKE_SECRET_KEY=$(secret /run/secrets/lake-secret-key)
export TRINO_SHARED_SECRET TRINO_LAKE_ACCESS_KEY TRINO_LAKE_SECRET_KEY

# The core's keys: Trino 484 verifies RSA and EC keys from files; a P-256
# public key in X.509 form is a fixed 27-byte header then x and y.
P256_HEADER='\x30\x59\x30\x13\x06\x07\x2a\x86\x48\xce\x3d\x02\x01\x06\x08\x2a\x86\x48\xce\x3d\x03\x01\x07\x03\x42\x00\x04'
b64std() { # base64url without padding -> base64
  local s=${1//-/+}
  s=${s//_//}
  while ((${#s} % 4)); do s+='='; done
  printf '%s' "$s"
}
sync_jwks() {
  local jwks key kid x y dir=/data/trino/jwks ca=()
  [ -f /run/qd-node/ca.crt ] && ca=(--cacert /run/qd-node/ca.crt)
  jwks=$(curl --fail --silent --show-error --max-time 10 "${ca[@]}" "$TRINO_JWKS_URL") || return 0
  mkdir -p "$dir"
  while [[ $jwks =~ \{([^{}]*)\} ]]; do
    key=${BASH_REMATCH[1]}
    jwks=${jwks#*"${BASH_REMATCH[0]}"}
    key=${key//[[:space:]]/}
    [[ $key =~ \"kid\":\"([A-Za-z0-9_-]+)\" ]] || continue
    kid=${BASH_REMATCH[1]}
    if [[ $key =~ \"kty\":\"EC\" && $key =~ \"crv\":\"P-256\" &&
      $key =~ \"x\":\"([A-Za-z0-9_-]{43})\".*\"y\":\"([A-Za-z0-9_-]{43})\" ]]; then
      x=${BASH_REMATCH[1]} y=${BASH_REMATCH[2]}
      {
        echo '-----BEGIN PUBLIC KEY-----'
        { printf "$P256_HEADER"; b64std "$x" | base64 -d; b64std "$y" | base64 -d; } | base64 -w 64
        echo '-----END PUBLIC KEY-----'
      } >"$dir/$kid.pem.tmp"
      mv "$dir/$kid.pem.tmp" "$dir/$kid.pem"
    fi
  done
}
sync_jwks
(while sleep "${TRINO_JWKS_REFRESH_SECONDS:-300}"; do sync_jwks; done) &

exec /usr/lib/trino/bin/launcher run --etc-dir "$ETC"
