#!/bin/bash
# restart-signal-relay.sh
#
# Cron entry per signaling node, offset so no two nodes restart in the
# same window — at least 2 of 3 stay live at all times.
#
#   # signal-1 crontab (checks every 5 min, only acts in minute-0 window)
#   */5 * * * * /usr/local/bin/restart-signal-relay.sh 0
#   # signal-2 (offset ~20 min into each hour-ish cycle)
#   */5 * * * * /usr/local/bin/restart-signal-relay.sh 20
#   # signal-3
#   */5 * * * * /usr/local/bin/restart-signal-relay.sh 40
#
# Usage: restart-signal-relay.sh <offset_minutes_within_hour>

set -euo pipefail

OFFSET_MIN="${1:?Usage: restart-signal-relay.sh <offset_minutes>}"
HEALTH_URL="http://localhost:${SIGNAL_PORT:-8766}/should-restart"
PM2_APP_NAME="gun-signal"
LOG_TAG="[restart-signal-relay]"

CURRENT_MIN=$(date +%-M)
# Only proceed within a 5-minute window starting at OFFSET_MIN, so this
# cron entry (running every 5 min) only ever acts once per hour per node.
WINDOW_END=$(( (OFFSET_MIN + 5) % 60 ))
if [ "$OFFSET_MIN" -le "$CURRENT_MIN" ] && [ "$CURRENT_MIN" -lt "$WINDOW_END" ]; then
  : # in window, continue
else
  exit 0
fi

RESPONSE=$(curl -fsS --max-time 5 "$HEALTH_URL" || echo '{"shouldRestart":false}')
SHOULD_RESTART=$(echo "$RESPONSE" | grep -o '"shouldRestart":[a-z]*' | cut -d: -f2)

if [ "$SHOULD_RESTART" = "true" ]; then
  echo "$LOG_TAG $(date -Iseconds) memory threshold exceeded, restarting $PM2_APP_NAME"
  # SIGTERM via pm2 restart triggers the graceful shutdown() handler in
  # signaling-server.js — sweep stops, in-flight writes finish, then exit.
  pm2 restart "$PM2_APP_NAME" --update-env
else
  echo "$LOG_TAG $(date -Iseconds) below threshold, no restart needed"
fi
