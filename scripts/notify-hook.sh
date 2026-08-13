#!/bin/bash
# fish-agent-notify.sh — claude-code hook that reports task events to fish-agent.
#
# Usage (from ~/.claude/settings.json):
#   Stop hook:            fish-agent-notify.sh completed
#   Notification hook:    fish-agent-notify.sh awaiting   (matcher: permission_prompt)
#
# stdin: claude-code hook JSON (Stop: last_assistant_message; Notification: message)
#
# The hook reports only when running inside a fish-term session — the shell
# spawned by fish-agent exports FISH_SESSION_ID / FISH_TOKEN. Claude instances
# started anywhere else inherit neither variable and this script exits silently.
#
# Deploy to the fish-agent host: install as ~/bin/fish-agent-notify.sh and
# chmod +x. Requires curl; python3 is optional (richer JSON handling).

set -u

event="${1:-}"
[ -n "$event" ] || exit 0
[ -n "${FISH_SESSION_ID:-}" ] || exit 0   # not a fish-term session — stay silent
[ -n "${FISH_TOKEN:-}" ] || exit 0

# Default matches fish-agent's default listen port. Override for testing.
endpoint="${FISH_NOTIFY_URL:-http://127.0.0.1:8765/notify}"

if command -v python3 >/dev/null 2>&1; then
  # Parse the hook JSON, truncate the message, POST a properly-escaped payload.
  # NOTE: use `python3 -c` (not `python3 -`) so stdin stays available for the
  # hook JSON — with `-` the stdin is consumed as the script itself.
  python3 -c '
import json, os, sys, urllib.request

event, session_id, token = sys.argv[1], sys.argv[2], sys.argv[3]
endpoint = os.environ.get("FISH_NOTIFY_URL", "http://127.0.0.1:8765/notify")
try:
    data = json.load(sys.stdin)
except Exception:
    data = {}
message = (data.get("message") or data.get("last_assistant_message") or "").strip()
message = message[:400]  # notification body — keep it short
payload = json.dumps({"session": session_id, "event": event, "message": message})
req = urllib.request.Request(
    endpoint,
    data=payload.encode("utf-8"),
    headers={"X-Fish-Token": token, "Content-Type": "application/json"},
    method="POST",
)
# localhost target — never route through an environment proxy
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
try:
    opener.open(req, timeout=3)
except Exception:
    pass  # best-effort — the device notification pipeline is lossy by design
' "$event" "$FISH_SESSION_ID" "$FISH_TOKEN"
else
  # curl fallback: crude field extraction, JSON escaping is best-effort.
  raw=$(cat)
  message=$(printf '%s' "$raw" | grep -o '"last_assistant_message"[^,]*' | head -1)
  [ -n "$message" ] || message=$(printf '%s' "$raw" | grep -o '"message"[^,]*' | head -1)
  message=$(printf '%s' "$message" | cut -c1-400)
  curl -s --noproxy '*' -m 3 -X POST "$endpoint" \
    -H "X-Fish-Token: $FISH_TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"session\":\"$FISH_SESSION_ID\",\"event\":\"$event\",\"message\":\"$message\"}" \
    >/dev/null 2>&1
fi

# Emit nothing on stdout — claude interprets hook stdout as a JSON response;
# an empty response means "allowed".
exit 0
