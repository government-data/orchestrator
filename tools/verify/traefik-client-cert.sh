#!/usr/bin/env bash
# What Traefik passes to a module behind it from a machine's client
# certificate (decision 0011: the engine port is served on the `internal`
# entry point with mTLS). Runs Traefik with a test CA, `passTLSClientCert`
# with the subject and serial, and a backend that echoes the headers; calls
# it with a client certificate shaped as the installation's machines
# (O=qd, OU=<role>, CN=<name>), without one, and with one of another CA.
#
# Run alone, never during a build: tools/verify/traefik-client-cert.sh
# Containers and the network are `qo-verify-*`, removed in a trap.
set -euo pipefail

TRAEFIK_IMAGE=${TRAEFIK_IMAGE:-traefik:v3.6}
WHOAMI_IMAGE=${WHOAMI_IMAGE:-traefik/whoami:v1.11}
PORT=${PORT:-39443}
work=$(mktemp -d)
net=qo-verify-cert-net

cleanup() {
  docker rm -f qo-verify-traefik qo-verify-whoami >/dev/null 2>&1 || true
  docker network rm "$net" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

cd "$work"
ca() { # name
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 1 \
    -subj "/O=qd/CN=$1" -keyout "$1.key" -out "$1.crt" 2>/dev/null
}
leaf() { # ca subject name [san]
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj "$2" -keyout "$3.key" -out "$3.csr" 2>/dev/null
  printf '%s\n' "${4:-}" > "$3.ext"
  openssl x509 -req -in "$3.csr" -CA "$1.crt" -CAkey "$1.key" -CAcreateserial \
    -days 1 -extfile "$3.ext" -out "$3.crt" 2>/dev/null
}
ca ca
ca other
leaf ca "/O=qd/CN=internal.test" server "subjectAltName=DNS:internal.test"
leaf ca "/O=qd/OU=worker/CN=cluster-1" machine "extendedKeyUsage=clientAuth"
leaf other "/O=qd/OU=worker/CN=intruder" intruder "extendedKeyUsage=clientAuth"

cat > dynamic.yml <<'YAML'
tls:
  certificates:
    - certFile: /etc/qo/server.crt
      keyFile: /etc/qo/server.key
  options:
    mtls:
      clientAuth:
        caFiles: [/etc/qo/ca.crt]
        clientAuthType: RequireAndVerifyClientCert
http:
  middlewares:
    machine:
      passTLSClientCert:
        info:
          serialNumber: true
          subject:
            organization: true
            organizationalUnit: true
            commonName: true
  routers:
    port:
      rule: Host(`internal.test`) && PathPrefix(`/internal`)
      entryPoints: [internal]
      middlewares: [machine]
      service: echo
      tls:
        options: mtls
  services:
    echo:
      loadBalancer:
        servers:
          - url: http://qo-verify-whoami:80
YAML
chmod -R a+r "$work"

docker network create "$net" >/dev/null
docker run -d --name qo-verify-whoami --network "$net" "$WHOAMI_IMAGE" >/dev/null
docker run -d --name qo-verify-traefik --network "$net" -p "127.0.0.1:$PORT:9443" \
  -v "$work:/etc/qo:ro" "$TRAEFIK_IMAGE" \
  --entrypoints.internal.address=:9443 \
  --providers.file.filename=/etc/qo/dynamic.yml --log.level=ERROR >/dev/null
for _ in $(seq 1 30); do
  curl -sk --resolve "internal.test:$PORT:127.0.0.1" "https://internal.test:$PORT/" -o /dev/null && break
  sleep 1
done
docker exec qo-verify-traefik traefik version | head -2

call() { # label curl-args...
  label=$1; shift
  echo "== $label"
  curl -s --cacert ca.crt --resolve "internal.test:$PORT:127.0.0.1" "$@" \
    -H 'X-Forwarded-Tls-Client-Cert-Info: Subject="O=qd,OU=core,CN=forged"' \
    "https://internal.test:$PORT/internal/claim" -w '\nstatus %{http_code}\n' 2>&1 \
    | grep -iv "^accept\|^user-agent\|^x-real\|^x-forwarded-\(host\|port\|proto\|server\)\|^ip:\|^remoteaddr\|^hostname\|^get \|^host:\|^accept-encoding" || true
}
call "a machine of the installation" --cert machine.crt --key machine.key
call "no client certificate"
call "a certificate of another CA" --cert intruder.crt --key intruder.key
