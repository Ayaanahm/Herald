#!/usr/bin/env bash
# Stop processes started by dev-up.sh (SIGTERM: they drain and exit cleanly).
pkill -TERM -f '^bin/herald (api|worker|provider)' || true
for i in $(seq 1 150); do
  pgrep -f '^bin/herald (api|worker|provider)' >/dev/null || exit 0
  sleep 0.2
done
echo "some processes did not exit within 30s:" >&2
pgrep -af '^bin/herald' >&2
exit 1
