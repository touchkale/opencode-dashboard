#!/bin/zsh
PORT=8765
URL="http://127.0.0.1:$PORT"
DIR="$(cd "$(dirname "$0")" && pwd)"
CHROME="/Applications/Google Chrome.app"
PROFILE="$HOME/.cache/ocd-dashboard-chrome"

open_dashboard() {
  # -n + dedicated profile => true standalone app window, even if Chrome is already open
  open -na "$CHROME" --args --app="$URL" --user-data-dir="$PROFILE" --window-size=1280,1600
}

# already running? just reopen the window
if curl -s -o /dev/null --max-time 1 "$URL"; then
  open_dashboard
  exit 0
fi

# start the server in this terminal, then open the dashboard window
python3 "$DIR/opencode_dashboard.py" --port "$PORT" 2>&1 &
SERVER_PID=$!

for _ in {1..30}; do
  if curl -s -o /dev/null --max-time 1 "$URL"; then break; fi
  sleep 0.2
done

open_dashboard

# keep the terminal window open and active while the server runs
wait $SERVER_PID