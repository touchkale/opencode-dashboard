# OpenCode Dashboard

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)]()
[![Platform](https://img.shields.io/badge/macOS-Linux-gray.svg)]()
[![GitHub stars](https://img.shields.io/github/stars/touchkale/opencode-dashboard?style=social)](https://github.com/touchkale/opencode-dashboard)

> A live usage dashboard for [OpenCode](https://github.com/anomalyco/opencode), served by a single Python file — no framework, no build step, no dependencies.

![dashboard](.github/social.png)

- **Single file**: `opencode_dashboard.py` is the whole server and page.
- **Auto-refreshing** every 15s; shows session history, per-day token/message charts, cost by model, tool usage, and a work-hour heatmap.
- **Click to filter**: days, tools, agents, models. Hover for tooltips.
- **Standalone app**: the included `OpenCode Dashboard.command` launcher opens a dedicated Chrome window and shuts the server down when the window closes.

## Requirements

- Python 3.9+
- An OpenCode installation (it reads the SQLite db that OpenCode maintains)
- Chrome (optional, for the `.command` launcher's dedicated window)

## Usage

```sh
python3 opencode_dashboard.py --port 8765
# open http://localhost:8765
```

Or double-click `OpenCode Dashboard.command` for a dedicated app window.

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `8765` | HTTP port to serve on |
| `--path` | auto | Path to the directory containing OpenCode's SQLite db |
| `--open` | — | Launch the app window immediately |
| `--time` | 0 | Also re-render a plain-text stats file every N seconds |

### API

- `GET /api/stats` — everything the page renders, as JSON
- `GET /` — the dashboard page
- `POST /api/close` — stop the server (used by the launcher on quit)

## Why a single file?

Copy `opencode_dashboard.py` (and, optionally, the `.command` launcher) to any machine with a Python 3 and an OpenCode db. Point it at that db and you're done. Everything — HTML, CSS, JS, SVG charts, KPI math, HTTP server, graceful shutdown — lives in one importable file you can read end to end.

## CSV export

From the dashboard you can export the full stats JSON with the **Export** button.

## Notes

- The `sessions.cost` column is not always populated by every session; the dashboard estimates the per-session cost as the model's share of that session's tokens instead.
- Reasoning tokens are counted as output ("Output · reasoning"); cached tokens are shown separately from the input/output bars.