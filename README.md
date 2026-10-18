# OpenCode Dashboard

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)]()
[![Platform](https://img.shields.io/badge/macOS-Linux-gray.svg)]()
[![GitHub stars](https://img.shields.io/github/stars/touchkale/opencode-dashboard?style=social)](https://github.com/touchkale/opencode-dashboard)

> A live usage dashboard for [OpenCode](https://github.com/anomalyco/opencode), served by a single Python file — no framework, no build step, no dependencies.

![dashboard](.github/social.png)

- **Single file**: `opencode_dashboard.py` is the whole server and page.
- **Token-first**: every chart, bar and heatmap shows tokens used — Input vs Output (incl. reasoning) vs Cache — with day-vs-day and model-vs-model comparisons.
- **OpenCode 1.0 + 2.0 schemas**: reads `session`/`message` (1.0) or `session_v2`/`session_message` (2.0) automatically, so a database carrying both generations still reports consistent session, per-day and per-file token totals.
- **Leak-proof token history**: a persistent per-database ledger (`opencode.db.usage-ledger.sqlite`) records every session's tokens, so totals, per-day counts, the heatmap, the model/agent/project breakdowns and records survive chat deletion inside OpenCode (only the recent-sessions table stays live-only).
- **Terminal UI**: `--tui` runs a responsive curses dashboard with the same OpenCode / Codex / Cline / Recap data — a prominent **Today** KPI card per harness, rounded cards, a sparkline-bar activity chart, top models with `free` / `$0` badges, a scrollable list, search (`/`), a detail modal (`Enter`), a harness on/off modal (`h`, persisted in the ledger), range switching (`t`), help (`?`), refresh (`r`), tab switching (`1-4`, Tab/arrows) and `o` to pop open the traditional Chrome app window.
- **Four tabs**: **OpenCode** (local SQLite), **Codex** (router `usage-events.jsonl`), **Cline** (Desktop app `~/.cline/data` sessions, same KPIs as Codex), and **Recap** — a recording-friendly daily total across all harnesses with per-harness split, providers, and top conversations for your YT Shorts.
- **Harness toggles**: the ⚙ Harnesses menu lets you disable any harness you are not using — its tab hides and Recap excludes it (persisted in `localStorage`).
- **Auto-refreshing** (30s, paused in background tabs); shows Today/7d/30d summary, per-day stacked token bars, tokens by file/subsystem, tokens per git commit, model universal comparison, records, and a GitHub-style **52-week token heatmap**.
- **Time-range presets** (7/30/90 days, 1 year, all time) with period-over-period deltas, plus Split-reasoning and 100%-share toggles.
- **Click to filter**: days, models, agents, files, subsystems, commits. Hover *or focus* for tooltips.
- **Resource-light**: local stats are cached and re-aggregated only when the database file changes; idle polling costs ~1 ms per request. In web mode the server shuts itself down once every dashboard window is closed.
- **Standalone app**: the included `OpenCode Dashboard.command` launcher opens the terminal UI and its background server; press `o` there for the dedicated Chrome window.

## Requirements

- Python 3.9+
- An OpenCode installation (it reads the SQLite db that OpenCode maintains)
- Chrome (optional, for the `.command` launcher's dedicated window)
- Optional: Codex Router events at `~/.codex/codex-router/usage-events.jsonl` (Codex tab)
- Optional: Cline Desktop data at `~/.cline/data` (Cline tab — `sessions/*/*.messages.json` + `db/sessions.db`)

## Usage

```sh
python3 opencode_dashboard.py --port 8765
# open http://localhost:8765

python3 opencode_dashboard.py --tui      # terminal UI + background web server
```

Or double-click `OpenCode Dashboard.command` for the terminal UI. The launcher starts
`--tui` in the Terminal window; the HTTP server runs in the background so pressing `o`
opens the same dedicated Chrome app window the launcher used before
(`--app=... --user-data-dir=~/.cache/ocd-dashboard-chrome --window-size=1280,1600`).
Closing that Chrome window does **not** stop the TUI or its server — only quitting the
TUI (`q` / closing the terminal) does.

### Terminal UI (`--tui`)

A responsive curses dashboard: header with a live clock, tab bar with the active time
range, a row of rounded KPI cards, a token-activity chart drawn with sparkline-style bars,
top models with `free` / `$0` badges, a scrollable session/request table, and — on the
OpenCode tab — a 52-week token heatmap with records. Codex and Cline replace the heatmap
with rate-limit / provider insights. The footer shows the key hints and the live
`Chrome UI: <url>` address.

Every harness tab leads with a **Today** card: that harness's tokens consumed today (local
date), with `7d` / `30d` comparisons and today's session/message count in the details. The
card always reads *today*; the `t` range toggle never changes it. The Recap page shows the
combined today total for the visible harnesses next to its range total.

| Key | Action |
|-----|--------|
| `q` | quit the TUI (stops the background server) |
| `r` | refresh all data (clears the stats caches) |
| `o` / `w` | open the traditional Chrome app window |
| `1`-`4` | jump to OpenCode / Codex / Cline / Recap (hidden harness tabs are skipped) |
| `Tab` / `→` / `←` | cycle the visible tabs |
| `↑` / `↓` (or `j` / `k`) | move the list selection; `PgUp`/`PgDn`/`Home`/`End` jump |
| `Enter` | open the detail modal for the selected session / request / Cline session (scroll with `j` / `k`, close with `Esc` or `q`) |
| `/` | filter the list by title/model (type, `Enter` applies, `Esc` cancels; `Esc` again clears the filter) |
| `t` | cycle the time range 14d / 30d / 90d / all — cards, chart and recap follow it |
| `h` | open the **Harnesses** modal: `↑`/`↓` move, `Space`/`Enter` toggle OpenCode / Codex / Cline, `h`/`Esc` close |
| `?` | help modal with every binding |
| `Esc` | close a modal, cancel a search, or clear an active filter |

Harness visibility is saved in the ledger's `prefs` table (`harness_visibility`), so a
hidden harness stays hidden across restarts. Hidden harnesses are dimmed in the tab bar,
skipped by `1-4`, `Tab` and `←`/`→`, and excluded from the Recap total, split bar and
today total; at least one harness always stays visible. When something is hidden the tab
bar (or footer on narrow terminals) shows a `harness 2/3` indicator.

The layout adapts to the terminal size instead of overlapping: at ≥110×30 the KPI cards,
chart + top models, and sessions + heatmap/records are side by side; at ≥84×26 the chart
row is side by side and the table spans the width; narrower terminals stack cards, chart,
top models and the session table; below 60 columns or 20 rows the TUI falls back to one
compact card with a sparkline. Panels are dropped as space runs out — never clipped or
drawn over each other.

The TUI footer shows the live `Chrome UI: <url>` address. If the requested port is busy,
the server walks forward through the next 10 ports (`--port`+1 … `--port`+10); if none
bind, the TUI still runs and `o` reports that the Chrome UI is unavailable. Top-model rows
show tokens, cost, and a `free` / `$0` marker. The cost column is the session cost for
OpenCode and the what-if paid cost for Codex; Cline records no per-model cost, so its rows
read `$0`. `free` is shown when the model name contains `free` or the payload flags it
free, and `$0` only when that tab's cost is actually zero. The synthetic recovered row
named `Other` is never marked `free`/`$0`: it carries an accent `recovered` badge instead
and is excluded from the "free models" KPI. The cost column is dropped on very narrow
layouts, keeping the badge.

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `db` | `~/.local/share/opencode/opencode.db` | Path to the OpenCode SQLite database (positional argument) |
| `--port` | `8765` | HTTP port to serve on |
| `--tui` | — | Run the curses terminal UI (with the HTTP server in the background) |
| `--router-events` | `~/.codex/codex-router/usage-events.jsonl` | Path to Codex Router usage events (Codex tab) |
| `--router-limits` | `~/.codex/codex-router/rate-limits.json` | Path to Codex Router rate limits |
| `--cline-dir` | `~/.cline/data` | Path to Cline Desktop data dir (Cline tab) |
| `--recovered-tokens` | auto: `4,500,000,000` for the default db | Recovered OpenCode tokens attributed to an `Other` model row (positive value overrides for any db; `0` disables) |
| `--idle-timeout` | `0` | Shut the server down after N seconds with no requests (0 = off) |
| `--quiet` | — | Suppress per-request logging |

### Data sources and the persistent ledger

- **OpenCode 1.0 and 2.0**: the local stats auto-detect `session`/`message` (1.0) or
  `session_v2`/`session_message` (2.0). Only parts belonging to the active session table
  are counted, so per-day totals, per-file attribution and the heatmap stay consistent
  even when a 2.0 database still carries legacy rows.
- **Deletion-proof history**: next to the database the dashboard maintains
  `<db>.usage-ledger.sqlite`, a persistent per-session token ledger. Every refresh upserts
  the live sessions; sessions you delete inside OpenCode stay in the ledger (marked
  deleted), so token totals, per-day session/message counts, the heatmap, the
  model/agent/project breakdowns, records (biggest day/session, fastest burn, streaks) and
  the date range never shrink when a chat is deleted — only when the ledger file itself is
  removed. The recent-sessions table is intentionally live-only; everything else is
  rebuilt from the ledger.
- **Recovered OpenCode tokens**: history deleted before the dashboard ever saw it can be
  accounted for with a fixed recovered amount. The default is
  `RECOVERED_OPENCODE_TOKENS = 4_500_000_000`, applied automatically **only for the default
  database path** (`~/.local/share/opencode/opencode.db`); `--recovered-tokens N` overrides
  it for any database and `--recovered-tokens 0` disables it. The amount is distributed
  across input / output / reasoning / cache-read / cache-write in the same ratio as the live
  totals (rounded, with the last nonzero bucket corrected so the parts sum exactly to the
  requested amount) and appears as a model row named **Other** (`n=0`, `$0` cost). In the
  TUI it shows an accent `recovered` badge and is deliberately **not** counted as a free
  model. It is intentionally historical: `days`, `activity`, `records` and the date range
  are left untouched, so daily charts, the heatmap and records stay truthful while the
  all-time totals and the model comparison include the recovery. The TUI and Recap inherit
  it through the same payload, and the ledger `prefs` table stores TUI-only preferences
  such as `harness_visibility`.

### API

- `GET /api/stats` — local SQLite token stats as JSON (always a well-formed payload, even on errors)
- `GET /api/router` — Codex Router stats as JSON
- `GET /api/cline` — Cline Desktop stats as JSON
- `GET /api/session/<id>` — prompts for one session (loaded on demand when you open a session row)
- `GET /` — the dashboard page
- `POST /api/close?wid=<window-id>` — web mode: stop the server once the last dashboard window closes; TUI mode: closes only that window (the TUI keeps serving)

## Why a single file?

Copy `opencode_dashboard.py` (and, optionally, the `.command` launcher) to any machine with Python 3 and an OpenCode db. Point it at that db and you're done. Everything — HTML, CSS, JS, SVG charts, KPI math, HTTP server, graceful shutdown — lives in one importable file you can read end to end.

## Export

The **Export** button offers JSON (full token payload) or CSV (per-day `date,messages,sessions,tokens_in,tokens_out,tokens_reasoning,tokens_cache,tokens_total`).
The **Recap CSV** option exports per-day totals across all harnesses (`date,opencode_tokens,codex_tokens,cline_tokens,total_tokens`).

## Recap tab (YT Shorts)

Open the **Recap** tab while recording: pick Today / Yesterday / any date, read the giant total, the per-harness split (OpenCode · Codex · Cline), providers, and top conversations — no tab-switching. **⧉ Copy for Shorts** copies a paste-ready summary for your video description.

## Security model

- The server binds to `127.0.0.1` only and serves no network-exposed endpoints.
- `POST /api/close` validates the `Origin` header, blocking cross-site (CSRF / DNS-rebinding) requests.
- All database-derived text is HTML-escaped and JSON-safe-embedded, including session titles and prompts (the payload is escaped so a malicious title cannot break out of the page's script tag).

## Development

```sh
python3 -m pytest tests/ -q   # tests need pytest
ruff check .                  # lint
```

CI (`.github/workflows/ci.yml`) runs ruff, a compile check, and the test suite on Python 3.9/3.11/3.13.

## Notes

- Reasoning tokens are merged into Output by default ("Output incl. reasoning") — use the **Split reasoning** toggle to see Output vs Reasoning separately; cached tokens always show separately.
- The session table shows `Input | Output | Cache | Total` explicitly, plus burn rate (`tokens/min`) in the detail modal.
- The session table shows the most recent 500 sessions to keep the payload bounded; full history remains in the charts and activity heatmap.