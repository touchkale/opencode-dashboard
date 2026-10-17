#!/bin/zsh
PORT=8765
URL="http://127.0.0.1:$PORT"
DIR="$(cd "$(dirname "$0")" && pwd)"

# Run the beautiful TUI in this terminal. The HTTP server still starts in the
# background so pressing 'o' inside the TUI opens the traditional Chrome UI.
# Closing this terminal stops both the TUI and the server.
python3 "$DIR/opencode_dashboard.py" --tui --port "$PORT"
