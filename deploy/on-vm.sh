#!/bin/bash
# Runs on the Bifrost VM, from /srv/bifrost, at the end of the deploy workflow.
#
# It exists because the gateway drops a session that carries nothing for five minutes
# (warpgate.yaml: inactivity_timeout), and a container build says nothing at all while npm and tsc
# work. A build that ran long therefore had its session killed and the deploy reported as failed
# even though the VM went on to finish it and swap the container. A heartbeat keeps the channel
# honest, and the build's own output is kept and printed so a real failure is still readable.
set -uo pipefail
cd /srv/bifrost
log=$(mktemp)
while :; do sleep 60; echo "  ... still building"; done &
heartbeat=$!
trap 'kill $heartbeat 2>/dev/null' EXIT
docker compose up -d --build > "$log" 2>&1
rc=$?
kill $heartbeat 2>/dev/null
if [ $rc -ne 0 ]; then
  echo "build failed (rc=$rc):"
  tail -40 "$log"
  rm -f "$log"
  exit $rc
fi
tail -3 "$log"
rm -f "$log"
docker image prune -f > /dev/null
sleep 3
curl -sf --max-time 20 http://127.0.0.1:8080/api/health
