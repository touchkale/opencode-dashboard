# OpenCode Dashboard

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)]()
[![Platform](https://img.shields.io/badge/macOS-Linux-gray.svg)]()
[![GitHub stars](https://img.shields.io/github/stars/touchkale/opencode-dashboard?style=social)](https://github.com/touchkale/opencode-dashboard)

> A live, OpenCode-first usage dashboard for [OpenCode](https://github.com/anomalyco/opencode), served by a single Python file — no framework, no build step, no dependencies.

![dashboard](.github/social.png)

- **Single file**: `opencode_dashboard.py` is the whole server and page.
- **OpenCode first, by default**: on first run the rail and command palette show only OpenCode; Codex, Cline and Recap are hidden. A Settings checkbox (`OpenCode focus mode — hide Codex, Cline and Recap`) reveals them and is persisted in `localStorage['ocd-focus']`. Explicit deep links to a hidden harness still render it, with a dismissible focus note when focus mode hid it.
- **Token-first and explicit**: every chart, bar and heatmap shows recorded tokens — Input, Output (incl. reasoning) and Cached tokens (read+write). The cache hit rate is read-only (`cache_read / (input + cache_read)`). Recovered history is never mixed into those recorded figures.
- **OpenCode 1.0 + 2.0 schemas**: reads `session`/`message` (1.0) or `session_v2`/`session_message` (2.0) automatically, so a database carrying both generations still reports consistent session, per-day and per-file token totals.
- **Deletion-proof, reconciled history**: a persistent per-database ledger (`opencode.db.usage-ledger.sqlite`) records every session's tokens, so totals, per-day counts, the heatmap, the model/agent/project breakdowns and records survive chat deletion inside OpenCode. Live per-day values are preserved; deleted-session tokens are added as deltas on their recorded dates. Only the recent-sessions table stays live-only.
- **Recovered history is metadata, not a model**: an optional fixed estimate for history deleted before the dashboard saw it is exposed separately as `recovered` (`included_in_totals: false`) and `lifetime = recorded + recovered`. It creates no synthetic model row and is excluded from recorded totals, model ranking, daily charts, the heatmap and records.
- **Terminal UI**: `--tui` runs a responsive curses dashboard with the same OpenCode / Codex / Cline / Recap data. It starts OpenCode-first (Codex and Cline hidden until enabled with `h`, persisted), uses positional `1`-`9` tab jumps, an Insights modal on `i`, a chart header with trailing 7-day average and next-7-day projection, and a separate recorded/recovered split. It also has per-harness **Today** cards, rounded cards, a sparkline-bar activity chart, top models with `free` / `$0` badges, a scrollable list, search (`/`), a detail modal (`Enter`), range switching (`t`), help (`?`), refresh (`r`) and `o` to open the traditional Chrome app window.
- **Four tabs**: **OpenCode** (local SQLite), **Codex** (router `usage-events.jsonl`), **Cline** (Desktop app `~/.cline/data` sessions, same KPIs as Codex), and **Recap** — a recording-friendly daily total across visible harnesses with per-harness split, providers, and top conversations for your YT Shorts. OpenCode focus mode hides the other three initially.
- **Harness toggles**: with focus mode off, the ⚙ Harnesses menu can disable any harness you are not using — its tab disappears from the rail and palette and Recap excludes it (persisted in `localStorage['ocd-harness']`). At least one harness always stays enabled.
- **Auto-refreshing** (30s, paused in background tabs); shows a recorded Today/7d/30d summary, per-day stacked token bars, tokens by file/subsystem, tokens per git commit, model universal comparison, records, and a GitHub-style **52-week recorded-token heatmap**. Recovered history is surfaced separately, not folded into these views.
- **Time-range presets** (7/30/90 days, 1 year, all time) with period-over-period deltas, plus Split-reasoning and 100%-share toggles.
- **Click to filter**: days, models, agents, files, subsystems, commits. Hover *or focus* for tooltips.
- **Resource-light**: local stats are cached and re-aggregated only when the database file changes; idle polling costs ~1 ms per request. In web mode the server shuts itself down once every dashboard window is closed.
- **Standalone app**: the included `OpenCode Dashboard.command` launcher opens the terminal UI and its background server; press `o` there for the dedicated Chrome window.

## Requirements

- Python 3.9+
- An OpenCode installation (it reads the SQLite db that OpenCode maintains)
- Chrome or another desktop browser (optional; the TUI runs without one, and Chrome is recommended for the `.command` launcher's dedicated window). There is no mobile app.
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
OpenCode tab — a 52-week recorded-token heatmap with records. Codex and Cline replace the
heatmap with rate-limit / provider insights. The footer shows the key hints and the web UI
shortcut; the help modal shows the live `Chrome UI: <url>` address. On first run the TUI
starts OpenCode-first: the Codex and Cline harnesses are hidden until you enable them with
`h` (the choice is persisted), and Recap remains available as its own tab. The chart header
shows a trailing 7-day average and a next-7-day projection of recorded tokens; `i` opens
the Insights modal.

Every harness tab leads with a **Today** card (OpenCode labels it **Today (recorded)**):
that harness's recorded tokens consumed today (local date), with `7d` / `30d` comparisons
and today's session/request/message count in the details. The card always reads *today*;
the `t` range toggle never changes it. OpenCode totals are split into **recorded** and
**recovered**: the Today/range cards, model ranking and daily charts use recorded tokens,
while recovered history is shown separately and is never ranked as a model. The Recap view
shows the combined today total for the visible harnesses next to its range total.

| Key | Action |
|-----|--------|
| `q` | quit the TUI (stops the background server) |
| `r` | refresh all data (clears the stats caches) |
| `o` / `w` | open the traditional Chrome app window |
| `1`-`9` | jump positionally to the 1st–9th visible tabs (hidden harnesses are skipped) |
| `Tab` / `→` / `←` | cycle the visible tabs |
| `↑` / `↓` (or `j` / `k`) | move the list selection; `PgUp`/`PgDn`/`Home`/`End` jump |
| `Enter` | open the detail modal for the selected session / request / Cline session (scroll with `j` / `k`, close with `Esc` or `q`) |
| `i` | open the Insights modal for the active tab (Recap merges the visible harnesses) |
| `/` | filter the list by title/model (type, `Enter` applies, `Esc` cancels; `Esc` again clears the filter) |
| `t` | cycle the time range 14d / 30d / 90d / all — cards, chart and recap follow it |
| `h` | open the **Harnesses** modal: `↑`/`↓` move, `Space`/`Enter` toggle OpenCode / Codex / Cline, `h`/`Esc` close |
| `?` | help modal with every binding |
| `Esc` | close a modal, cancel a search, or clear an active filter |

TUI harness visibility is saved in the ledger's `prefs` table (`harness_visibility`), so a
hidden harness stays hidden across restarts. First run starts with only OpenCode enabled
among the usage harnesses; enable Codex/Cline with `h` when you need them. Hidden harnesses
are skipped by `1`-`9`, `Tab` and `←`/`→`, and excluded from the Recap total, split bar and
today total. At least one harness always stays visible. When something is hidden the tab
bar, or the footer when it does not fit, shows a `+N hidden · h` tag.

The layout adapts to the terminal size instead of overlapping: at ≥110×30 the KPI cards,
chart + top models, and sessions + heatmap/records are side by side; at ≥84×26 the chart
row is side by side and the table spans the width; narrower terminals stack cards, chart,
top models and the session table; below 60 columns or 20 rows the TUI falls back to one
compact card with a sparkline. Panels are dropped as space runs out — never clipped or
drawn over each other.

The help modal and the wide-terminal footer show the live `Chrome UI: <url>` address. If
the requested port is busy, the server walks forward through the next 10 ports
(`--port`+1 … `--port`+10); if none bind, the TUI still runs and `o` reports that the
Chrome UI is unavailable. Top-model rows
show tokens, cost, and a `free` / `$0` marker. The cost column is the session cost for
OpenCode and the what-if paid cost for Codex; Cline records no per-model cost, so its rows
read `$0`. `free` is shown when the model name contains `free` or the payload flags it
free, and `$0` only when that tab's cost is actually zero. OpenCode's model ranking is
recorded-only; recovered history never creates a model row. The cost column is dropped on
very narrow layouts, keeping the badge.

### Web focus mode and harness visibility

The web UI is OpenCode-first: focus mode is on by default, so on first run only OpenCode
appears in the rail and command palette; Codex, Cline and Recap are hidden. The Settings
menu (⚙ **Harnesses**) contains the
`#set-focus` checkbox **OpenCode focus mode — hide Codex, Cline and Recap**, persisted in
`localStorage['ocd-focus']`. Turning focus off reveals every harness immediately; with
focus off, the individual OpenCode / Codex / Cline checkboxes hide harnesses you do not
use, persisted in `localStorage['ocd-harness']`. Hidden harnesses are excluded from the
rail and command palette; individually disabled harnesses are also left out of the Recap
totals. A deep link to a hidden harness still renders that harness (with a dismissible
focus note when focus mode was the reason it was hidden). At least one harness must stay
enabled.

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `db` | `~/.local/share/opencode/opencode.db` | Path to the OpenCode SQLite database (positional argument) |
| `--port` | `8765` | HTTP port to serve on |
| `--tui` | — | Run the curses terminal UI (with the HTTP server in the background) |
| `--router-events` | `~/.codex/codex-router/usage-events.jsonl` | Path to Codex Router usage events (Codex tab) |
| `--router-limits` | `~/.codex/codex-router/rate-limits.json` | Path to Codex Router rate limits |
| `--cline-dir` | `~/.cline/data` | Path to Cline Desktop data dir (Cline tab) |
| `--recovered-tokens` | auto: `4,500,000,000` for the default db | Recovered history exposed as **lifetime metadata only** (recorded + recovered), never added to recorded totals, model rows or daily charts. A positive value overrides the amount for any db; `0` disables it. |
| `--idle-timeout` | `0` | Shut the server down after N seconds with no requests (0 = off) |
| `--quiet` | — | Suppress per-request logging |

### Data sources and the persistent ledger

- **OpenCode 1.0 and 2.0, reconciled per session**: the local stats auto-detect
  `session`/`message` (1.0) or `session_v2`/`session_message` (2.0). Session rows are the
  source of truth for token totals, and parts are filtered to the active session table so
  legacy rows never leak into per-day totals, per-file attribution or the heatmap. Session
  totals are attributed to local days from `step-finish` parts; when a 2.0 session's parts
  are missing or incomplete, assistant `session_message.data.tokens` are read by message
  timestamp to fill only the uncovered gap. Any remaining difference is added as an
  explicit residual on the session's last attributed date (or its start date), and tokens
  that still cannot be dated are reported as `totals.unattributed` instead of being forced
  into a day.
- **Recorded vs recovered**: *recorded* tokens are what the dashboard has observed in the
  database or ledger, including sessions deleted after they were recorded. *Recovered*
  tokens are the optional fixed estimate for history deleted before the dashboard ever saw
  it. The two are never added together implicitly: `recorded` is the default, and
  `lifetime` is the explicit sum.
- **Deletion-proof history**: next to the database the dashboard maintains
  `<db>.usage-ledger.sqlite`, a persistent per-session token ledger. Every refresh upserts
  the live sessions; sessions you delete inside OpenCode stay in the ledger (marked
  deleted), so token totals, per-day session/message counts, the heatmap, the
  model/agent/project breakdowns, records (biggest day/session, fastest burn, streaks) and
  the date range never shrink when a chat is deleted — only when the ledger file itself is
  removed. The recent-sessions table is intentionally live-only.
- **Daily attribution is preserved, not rewritten**: the ledger merge keeps each live day's
  existing tokens and adds only deleted-session tokens as deltas on the dates recorded in
  the ledger (session start). A live session that ran past midnight therefore stays on its
  activity dates instead of being moved back onto its start date. Totals, the
  model/agent/project rollups and the date range come from the full ledger; records
  (biggest day, biggest session, fastest burn, streaks) are rebuilt from the merged day
  history and the ledger rows. If ledger tokens cannot be dated, the payload reports them
  as `totals.unattributed` instead of forcing them into a day; daily charts and the heatmap
  show dated recorded tokens only.
- **Cache definitions**: `tokens_cache_total = tokens_cache_read + tokens_cache_write` is
  the cached volume (the UI label is **Cached tokens (read+write)**). `cache_rate` — shown as
  **Cache hit (read)** — is read-only: `tokens_cache_read / (tokens_input +
  tokens_cache_read)`. Codex is different by design: cached input is a subset of input, so
  its cache rate is `cached_input / input`.
- **Recovered OpenCode tokens (metadata only)**: history deleted before the dashboard ever
  saw it can be acknowledged with a fixed recovered amount. The default is
  `RECOVERED_OPENCODE_TOKENS = 4_500_000_000`, applied automatically **only for the default
  database path** (`~/.local/share/opencode/opencode.db`); `--recovered-tokens N` overrides
  it for any database and `--recovered-tokens 0` disables it. The amount is split across
  input / output / reasoning / cache (read+write) in the same ratio as the recorded totals,
  rounded with the last nonzero bucket corrected so the parts sum exactly. It does **not**
  create a model row, and it does not change `totals`, `models`, `days`, `activity`,
  `records` or `range`. The payload exposes `recovered` with `included_in_totals: false`
  plus `lifetime` (`recorded + recovered`). When recovery is configured, the web hero adds
  **Lifetime** alongside **Recorded tokens**, the All-time card reads `recorded` or
  `+recovered = lifetime`, and a note under Models states that recovered history is
  excluded from recorded totals, the model ranking and daily charts. When recovery is
  configured, the TUI shows the same recorded/recovered split; Recap totals remain
  recorded-only. The ledger `prefs` table also stores TUI-only preferences such as
  `harness_visibility`.

### API

- `GET /api/stats` — local SQLite token stats as JSON (always a well-formed payload, even on errors). Includes `recovered` and `lifetime` when recovery is configured; both are display-only metadata.
- `GET /api/router` — Codex Router stats as JSON
- `GET /api/cline` — Cline Desktop stats as JSON
- `GET /api/session/<id>` — prompts for one session (loaded on demand when you open a session row)
- `GET /` — the dashboard page
- `POST /api/close?wid=<window-id>` — web mode: stop the server once the last dashboard window closes; TUI mode: closes only that window (the TUI keeps serving)

## Why a single file?

Copy `opencode_dashboard.py` (and, optionally, the `.command` launcher) to any machine with Python 3 and an OpenCode db. Point it at that db and you're done. Everything — HTML, CSS, JS, SVG charts, KPI math, HTTP server, graceful shutdown — lives in one importable file you can read end to end.

## Export

The **Export** button offers JSON (full token payload, including recovered/lifetime metadata when configured) or CSV (per-day `date,messages,sessions,tokens_in,tokens_out,tokens_reasoning,tokens_cache,tokens_total`). The CSV is a recorded per-day table; recovered history is not injected into its rows. The **Recap CSV** option exports per-day recorded totals across the enabled harnesses (`date,opencode_tokens,codex_tokens,cline_tokens,total_tokens`).

## Recap tab (YT Shorts)

Turn off OpenCode focus mode in Settings to reveal the **Recap** tab (a direct deep link also renders it, with a focus note). While recording: pick Today / Yesterday / any date, read the giant total, the per-harness split (OpenCode · Codex · Cline), providers, and top conversations — no tab-switching. **⧉ Copy for Shorts** copies a paste-ready summary for your video description. Recap totals are recorded tokens; recovered history is not folded in.

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

- Reasoning tokens are merged into Output by default ("Output incl. reasoning") — use the **Split reasoning** toggle to see Output vs Reasoning separately; the cached volume (`read+write`) always shows separately.
- The cache hit rate is read-only (`cache_read / (input + cache_read)`); cache writes count toward the cached volume but not toward the hit rate.
- The session table shows `Input | Output | Cache | Total` explicitly, plus burn rate (`tokens/min`) in the detail modal.
- The session table shows the most recent 500 sessions to keep the payload bounded. Charts and the activity heatmap use every dated recorded session; recovered history and any `totals.unattributed` tokens are deliberately not forced into daily buckets.
- The web UI is built for desktop browsers — there is no mobile app or mobile support promise.
