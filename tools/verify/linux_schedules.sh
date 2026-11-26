#!/usr/bin/env bash
# V7 (decision 0011): Linux schedules for import. On a Debian and an Ubuntu
# container running systemd and cron, a fake engine command started by a
# line in /etc/cron.d, a user's crontab and a systemd timer:
#   1. what systemctl prints where engine-runner's scanner reads it
#      (list-timers, TimersCalendar, TimersMonotonic, Persistent, ExecStart,
#      User), and that the cron lines start the command;
#   2. that commenting a cron line (the file replaced by a rename, as the
#      scanner does) and `systemctl disable --now` stop the next start;
#   3. what a timer with Persistent=true does after being disabled: a
#      daemon-reload, and enabling it again.
#
# Run alone, never during a build: tools/verify/linux_schedules.sh
# Containers and images are `qo-verify-v7-*`, removed in a trap.
set -euo pipefail

DISTROS=${DISTROS:-"debian:12 ubuntu:24.04"}
work=$(mktemp -d)
names=()
fails=0
oks=0

cleanup() {
  for n in "${names[@]}"; do
    docker rm -f "$n" >/dev/null 2>&1 || true
    docker rmi -f "$n" >/dev/null 2>&1 || true
  done
  rm -rf "$work"
}
trap cleanup EXIT

ok() { echo "OK   $*"; oks=$((oks + 1)); }
fail() { echo "FAIL $*"; fails=$((fails + 1)); }
on() { docker exec "$1" bash -c "$2"; }

cat > "$work/Dockerfile" <<'EOF'
ARG BASE
FROM ${BASE}
RUN apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq systemd cron >/dev/null \
 && useradd -m svc && mkdir -p /opt/engine /srv/flows/a /srv/flows/b /srv/flows/c /srv/flows/d
# The fake engine: one line per start, with the artefact it was given.
RUN printf '#!/bin/sh\necho "$(date +%%s) $*" >> /tmp/starts\n' > /opt/engine/run && chmod 755 /opt/engine/run \
 && touch /tmp/starts && chmod 666 /tmp/starts
STOPSIGNAL SIGRTMIN+3
CMD ["/lib/systemd/systemd"]
EOF

starts() { on "$1" "grep -c -- '-artefact=$2' /tmp/starts || true"; }

# Waits until the minute turns and 20 s more, so each cron minute has run.
next_minute() { sleep $((80 - $(date +%S | sed 's/^0//'))); }

setup() { # name
  local n=$1
  on "$n" 'until systemctl is-system-running 2>/dev/null | grep -qE "running|degraded"; do sleep 1; done'
  on "$n" 'systemctl enable --now cron >/dev/null 2>&1; ln -sf /usr/share/zoneinfo/UTC /etc/localtime'
  on "$n" 'echo "* * * * * svc /opt/engine/run -artefact=/srv/flows/a -var=region=north" > /etc/cron.d/flows; chmod 644 /etc/cron.d/flows'
  on "$n" 'echo "* * * * * /opt/engine/run \"-artefact=/srv/flows/b\"" | crontab -u svc -'
  on "$n" 'cat > /etc/systemd/system/flow-c.service <<U
[Service]
Type=oneshot
User=svc
ExecStart=/opt/engine/run "-artefact=/srv/flows/c" -var=day=1
U
cat > /etc/systemd/system/flow-c.timer <<U
[Timer]
OnCalendar=minutely
[Install]
WantedBy=timers.target
U
cat > /etc/systemd/system/flow-d.service <<U
[Service]
Type=oneshot
ExecStart=/opt/engine/run -artefact=/srv/flows/d
U
cat > /etc/systemd/system/flow-d.timer <<U
[Timer]
OnCalendar=Mon..Fri *-*-* 06:00
OnBootSec=1h
Persistent=true
[Install]
WantedBy=timers.target
U
systemctl daemon-reload && systemctl enable --now flow-c.timer flow-d.timer >/dev/null 2>&1'
}

report() { # name
  local n=$1
  echo "--- $n: what the scanner reads"
  on "$n" 'systemctl list-timers --all --plain --no-legend --no-pager | grep flow-'
  on "$n" 'systemctl show --no-pager -p TimersCalendar -p TimersMonotonic -p Persistent flow-c.timer flow-d.timer'
  on "$n" 'systemctl show --no-pager -p ExecStart -p User flow-c.service flow-d.service'
  on "$n" 'ls -l /var/spool/cron/crontabs/; cat /var/spool/cron/crontabs/svc | grep -v "^#"'
  local lt
  lt=$(on "$n" 'systemctl list-timers --all --plain --no-legend --no-pager | grep flow-c' | awk '{print $(NF-1), $NF}')
  [[ $lt == "flow-c.timer flow-c.service" ]] && ok "$n: list-timers ends in unit, activates" || fail "$n: list-timers columns: $lt"
  on "$n" 'systemctl show -p TimersCalendar flow-c.timer' | grep -q 'OnCalendar=\*-\*-\* \*:\*:00 ;' \
    && ok "$n: minutely normalised to *-*-* *:*:00" || fail "$n: TimersCalendar of minutely"
  on "$n" 'systemctl show -p TimersCalendar flow-d.timer' | grep -q 'OnCalendar=Mon..Fri \*-\*-\* 06:00:00 ;' \
    && ok "$n: Mon..Fri 06:00 normalised" || fail "$n: TimersCalendar of Mon..Fri"
  on "$n" 'systemctl show -p TimersMonotonic flow-d.timer' | grep -q 'OnBootUSec=' \
    && ok "$n: OnBootSec in TimersMonotonic" || fail "$n: TimersMonotonic"
  on "$n" 'systemctl show -p ExecStart flow-c.service' | grep -q 'argv\[\]=/opt/engine/run -artefact=/srv/flows/c -var=day=1 ;' \
    && ok "$n: ExecStart argv[] without the quotes" || fail "$n: ExecStart format"
}

for distro in $DISTROS; do
  n="qo-verify-v7-${distro%%:*}"
  names+=("$n")
  docker build -q --build-arg BASE="$distro" -t "$n" "$work" >/dev/null
  docker run -d --name "$n" --privileged --cgroupns=host -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
    --tmpfs /run --tmpfs /run/lock "$n" >/dev/null
done
for n in "${names[@]}"; do setup "$n"; done
next_minute
next_minute
for n in "${names[@]}"; do
  on "$n" 'cat /etc/os-release | grep PRETTY; systemctl --version | head -1; dpkg -l cron | tail -1 | awk "{print \$2, \$3}"'
  report "$n"
  for a in a b c; do
    [[ $(starts "$n" "/srv/flows/$a") -ge 1 ]] && ok "$n: /srv/flows/$a started" || fail "$n: /srv/flows/$a never started"
  done
  on "$n" 'grep -- "-artefact=/srv/flows/a" /tmp/starts | tail -1'
done

# 2. Disable: the cron lines commented by a rename, the timer disabled.
marker='# qd-orchestrator disabled: '
for n in "${names[@]}"; do
  on "$n" "sed 's|^\\*|$marker*|' /etc/cron.d/flows > /etc/cron.d/.flows.qd-tmp && chmod 644 /etc/cron.d/.flows.qd-tmp && mv /etc/cron.d/.flows.qd-tmp /etc/cron.d/flows"
  on "$n" "f=/var/spool/cron/crontabs/svc; sed 's|^\\*|$marker*|' \$f > /var/spool/cron/crontabs/.svc.qd-tmp && chown --reference=\$f /var/spool/cron/crontabs/.svc.qd-tmp && chmod --reference=\$f /var/spool/cron/crontabs/.svc.qd-tmp && mv /var/spool/cron/crontabs/.svc.qd-tmp \$f"
  on "$n" 'systemctl disable --now flow-c.timer flow-d.timer >/dev/null 2>&1'
done
# cron reads a changed file at its next minute: one more start may come.
next_minute
declare -A before
for n in "${names[@]}"; do
  for a in a b c; do before[$n$a]=$(starts "$n" "/srv/flows/$a"); done
done
next_minute
next_minute
for n in "${names[@]}"; do
  for a in a b c; do
    now=$(starts "$n" "/srv/flows/$a")
    [[ $now == "${before[$n$a]}" ]] && ok "$n: /srv/flows/$a not started after disabling" \
      || fail "$n: /srv/flows/$a started after disabling (${before[$n$a]} -> $now)"
  done
  on "$n" 'systemctl is-enabled flow-c.timer flow-d.timer; systemctl is-active flow-d.timer || true' | tr '\n' ' '; echo
done

# 3. Persistent=true after being disabled: a daemon-reload, then enabled again.
for n in "${names[@]}"; do
  on "$n" 'cat > /etc/systemd/system/flow-e.service <<U
[Service]
Type=oneshot
ExecStart=/opt/engine/run -artefact=/srv/flows/e
U
cat > /etc/systemd/system/flow-e.timer <<U
[Timer]
OnCalendar=minutely
Persistent=true
U
systemctl daemon-reload && systemctl start flow-e.timer'
done
next_minute
for n in "${names[@]}"; do
  on "$n" 'systemctl disable --now flow-e.timer >/dev/null 2>&1; ls /var/lib/systemd/timers/ | grep flow-e || echo "no stamp"'
done
next_minute
next_minute
for n in "${names[@]}"; do
  b=$(starts "$n" /srv/flows/e)
  on "$n" 'systemctl daemon-reload'
  sleep 5
  a=$(starts "$n" /srv/flows/e)
  [[ $a == "$b" ]] && ok "$n: a disabled Persistent timer does not catch up on daemon-reload" \
    || fail "$n: it caught up on daemon-reload ($b -> $a)"
  on "$n" 'systemctl start flow-e.timer'
  sleep 5
  c=$(starts "$n" /srv/flows/e)
  echo "INFO $n: started again after being off 2 minutes: $b -> $c starts within 5 s"
done

echo "ALL: $oks OK, $fails FAIL"
[[ $fails -eq 0 ]]
