import argparse
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from urllib.request import pathname2url

DB_PATH = Path(os.environ.get("HOME", "")) / ".local/share/opencode/opencode.db"
ROUTER_EVENTS_DEFAULT = Path(os.environ.get("HOME", "")) / ".codex/codex-router/usage-events.jsonl"
ROUTER_LIMITS_DEFAULT = Path(os.environ.get("HOME", "")) / ".codex/codex-router/rate-limits.json"

# All cross-thread shared state is guarded by LOCK.
LOCK = threading.Lock()
LOCAL_CACHE = {"key": None, "stats": None}
ROUTER_CACHE = {"key": None, "stats": None}
MAX_ROUTER_EVENTS = 30000  # most-recent lines scanned from usage-events.jsonl
MAX_ROUTER_ROWS = 400  # recent requests kept in the /api/router payload

# What-if paid pricing per 1M tokens (input, output) for known *-free models.
# Actual free cost is always $0; this estimates what the same tokens would cost.
WHAT_IF_PRICING = {
    "muse-spark-1.3-contributor-free": (0.10, 0.20),
    "muse-spark-1.2-contributor-free": (0.10, 0.20),
    "muse-spark-1.2-contributor": (0.10, 0.20),
}
WINDOWS = {}  # window id -> last-seen (unix seconds); heartbeat registry
LAST_REQUEST = 0.0
HAD_WINDOW = False
CLOSE_TIMER = None
MAX_SESSION_ROWS = 500
ACTIVITY_DAYS = 364  # 52 weeks of daily cells, one column per day
COMMIT_WINDOW_HOURS = 24  # sessions in this window before a commit count toward it
MAX_COMMITS_PER_REPO = 200  # recent commits scanned per worktree
MAX_FILE_ROWS = 15  # top files/subsystems kept in the payload (+ implicit Other)
MAX_COMMIT_ROWS = 60  # commit rows kept in the payload, by tokens desc


def connect(db_path):
    if not Path(db_path).exists():
        raise RuntimeError(f"Database not found: {db_path}")
    uri = f"file:{pathname2url(str(Path(db_path).resolve()))}?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    con.row_factory = sqlite3.Row
    return con


def json_html(obj):
    """Serialize for safe embedding inside <script>const S0 = ...;</script>.

    json.dumps leaves '<' unescaped, so a DB-sourced title containing
    '</script><script>...' could break out of the script tag. Escaping '<'
    (and the JS string terminators U+2028/U+2029) makes the payload inert.
    """
    out = json.dumps(obj, ensure_ascii=True).replace("<", "\\u003c")
    return out.replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")


def zero_days_window():
    """Canonical 52-week daily window ending today (local time), zero-filled.

    Keys are 'YYYY-MM-DD' strings in local time, matching how per-day usage
    is bucketed elsewhere. Returns a list of day dicts.
    """
    today = datetime.now().date()
    start = today - timedelta(days=ACTIVITY_DAYS - 1)
    out = []
    d = start
    while d <= today:
        out.append(
            {
                "date": d.isoformat(),
                "msgs": 0,
                "sessions": 0,
                "ti": 0,
                "to": 0,
                "tr": 0,
                "cache": 0,
                "cost": 0.0,
            }
        )
        d += timedelta(days=1)
    return out


def pad_activity(days):
    """Overlay recorded per-day usage onto the canonical 52-week window.

    All numeric keys are carried over (not just the OpenCode set), so the
    Codex day breakdown — reqs/ok/err/err429/what_if — survives padding and
    stays consistent between the per-day table, the heatmap and the
    period rollups. Totals are recomputed by each caller with the correct
    token semantics (local includes the separate cache stream, Codex input
    already contains cached input).
    """
    window = zero_days_window()
    for slot in window:
        slot.setdefault("reqs", 0)
        slot.setdefault("ok", 0)
        slot.setdefault("err", 0)
    by_date = {d["date"]: d for d in days}
    for slot in window:
        entry = by_date.get(slot["date"])
        if not entry:
            continue
        for key, value in entry.items():
            if key == "date" or key == "total":
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            slot[key] = slot.get(key, 0) + value
    return window


def day_total(d):
    """Total tokens for a day/slot dict (input+output+reasoning+cache)."""
    return (d.get("ti", 0) or 0) + (d.get("to", 0) or 0) + (d.get("tr", 0) or 0) + (d.get("cache", 0) or 0)


def cache_rate(ti, cache):
    """OpenCode cache share, where input excludes the separate cache stream."""
    denom = (ti or 0) + (cache or 0)
    return round((cache or 0) / denom * 100, 1) if denom else 0.0


def router_cache_rate(input_total, cached_input):
    """Codex cache share, where cached input is a subset of total input."""
    return round(min(cached_input or 0, input_total or 0) / (input_total or 1) * 100, 1) if input_total else 0.0


def _router_day_tokens(d):
    """Router day total without double-counting cached input (subset of input)."""
    return (d.get("ti", 0) or 0) + (d.get("to", 0) or 0) + (d.get("tr", 0) or 0)


def compute_streaks(activity, tokens_only=False):
    """Longest + current active-day streaks from padded activity window.

    An empty today must not zero the current streak: when the last slot
    (today) is inactive, counting starts back from yesterday. With
    tokens_only=True (Codex tab) error-only / unmetered-zero days with
    requests but no tokens do not extend a streak.
    """
    def active(slot):
        if tokens_only:
            return _router_day_tokens(slot) > 0
        return (slot.get("msgs", 0) or 0) > 0 or day_total(slot) > 0

    slots = list(activity or [])
    if slots and not active(slots[-1]):
        slots = slots[:-1]  # today quiet so far — streak stands through yesterday
    cur = 0
    for slot in reversed(slots):
        if active(slot):
            cur += 1
        else:
            break
    best = run = 0
    for slot in activity or []:
        if active(slot):
            run += 1
            best = max(best, run)
        else:
            run = 0
    return {"current": cur, "longest": best}


def parse_model(raw):
    if not raw:
        return "—"
    try:
        return json.loads(raw).get("id", raw)
    except (ValueError, AttributeError):
        return str(raw)


def short_dir(path):
    if not path:
        return "—"
    parts = [p for p in path.split("/") if p]
    if len(parts) > 2:
        return "/".join(parts[-2:])
    return path


def fmt_dur_min(minutes):
    if minutes < 1:
        return "under a minute"
    if minutes < 60:
        return f"{round(minutes)} min"
    return f"{round(minutes // 60)}h {round(minutes % 60)}m"


def _day(ms):
    return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d") if ms else None


def subsystem_for_file(filepath, worktrees):
    """Map a touched file to a subsystem: first path segment under its worktree.

    The longest matching worktree wins, so sessions touching files across
    repos still bucket correctly. Files outside every known worktree (scratch
    dirs, /tmp) fall back to their parent folder name instead of vanishing.
    """
    fp = filepath or ""
    best = ""
    for wt in worktrees or []:
        w = (wt or "").rstrip("/")
        if w and (fp == w or fp.startswith(w + "/")) and len(w) > len(best):
            best = w
    if best:
        rel = fp[len(best):].lstrip("/")
        if not rel:
            return "(root)"
        return rel.split("/", 1)[0] or "(root)"
    parent = fp.rsplit("/", 1)[0] if "/" in fp else ""
    return (parent.rsplit("/", 1)[-1] if parent else "") or "(root)"


def db_worktrees(db_path):
    """[(display name, worktree)] from the project table; [] when unreadable."""
    try:
        con = connect(db_path)
        try:
            return [
                (r["name"] or short_dir(r["worktree"]), r["worktree"])
                for r in con.execute("SELECT name, worktree FROM project")
            ]
        finally:
            con.close()
    except Exception:
        return []


def repo_heads(worktrees):
    """Current HEAD per worktree — cheap git fingerprint for cache invalidation."""
    heads = []
    seen = set()
    for _, wt in worktrees or []:
        if not wt or wt in seen:
            continue
        seen.add(wt)
        try:
            if not Path(wt).is_dir():
                heads.append((wt, None))
                continue
            out = subprocess.run(
                ["git", "-C", wt, "rev-parse", "HEAD"],
                capture_output=True, text=True, timeout=5,
            )
            heads.append((wt, out.stdout.strip() if out.returncode == 0 else None))
        except (OSError, ValueError, subprocess.SubprocessError):
            heads.append((wt, None))
    return tuple(heads)


def collect_repo_commits(worktrees, per_repo=MAX_COMMITS_PER_REPO):
    """Recent commits per worktree: sha/subject/date/files/+/-.

    Skips missing dirs and non-git worktrees silently — the panels simply
    show fewer repos. Never raises for git failures.
    """
    commits = []
    seen = set()
    for name, wt in worktrees or []:
        if not wt or wt in seen:
            continue
        seen.add(wt)
        try:
            if not Path(wt).is_dir():
                continue
            out = subprocess.run(
                ["git", "-C", wt, "log", f"-n{per_repo}",
                 "--format=%H\x1f%s\x1f%ct", "--numstat"],
                capture_output=True, text=True, timeout=15,
            )
            if out.returncode != 0:
                continue
        except (OSError, ValueError, subprocess.SubprocessError):
            continue
        cur = None
        for line in out.stdout.splitlines():
            if "\x1f" in line:
                if cur:
                    commits.append(cur)
                parts = line.split("\x1f")
                try:
                    ts = int(parts[2])
                except (ValueError, IndexError):
                    cur = None
                    continue
                cur = {
                    "sha": parts[0],
                    "subject": parts[1] if len(parts) > 1 else "",
                    "time": ts * 1000,
                    "date": datetime.fromtimestamp(ts).strftime("%Y-%m-%d"),
                    "project": name,
                    "files": 0,
                    "add": 0,
                    "del": 0,
                }
            elif cur and line.strip():
                bits = line.split()
                if len(bits) >= 3:
                    cur["files"] += 1
                    try:
                        cur["add"] += int(bits[0])
                    except ValueError:
                        pass
                    try:
                        cur["del"] += int(bits[1])
                    except ValueError:
                        pass
        if cur:
            commits.append(cur)
    return commits


def query_stats(con):
    totals = dict(
        con.execute(
            """
            SELECT (SELECT COUNT(*) FROM session) AS sessions,
                   (SELECT COUNT(*) FROM message) AS messages,
                   (SELECT COALESCE(SUM(tokens_input),0) FROM session) AS tokens_input,
                   (SELECT COALESCE(SUM(tokens_output),0) FROM session) AS tokens_output,
                   (SELECT COALESCE(SUM(tokens_reasoning),0) FROM session) AS tokens_reasoning,
                   (SELECT COALESCE(SUM(tokens_cache_read),0) FROM session) AS tokens_cache_read,
                   (SELECT COALESCE(SUM(tokens_cache_write),0) FROM session) AS tokens_cache_write,
                   (SELECT COALESCE(SUM(cost),0) FROM session) AS cost,
                   (SELECT COALESCE(AVG((time_updated-time_created)/60000.0),0)
                      FROM session WHERE time_created>0 AND time_updated>time_created) AS avg_duration_min
            """
        ).fetchone()
    )

    # Single pass over step-finish parts: per-day token totals.
    # Part 'total' = input+output+reasoning+cache.read+cache.write, so the
    # derived cache (total-input-output-reasoning) is read+write combined,
    # matching the session rollups exactly.
    tokens_by_day = {}
    for r in con.execute(
        """
        SELECT time_created, data FROM part
        WHERE json_extract(data,'$.type')='step-finish'
        """
    ):
        try:
            data = json.loads(r["data"])
        except (ValueError, TypeError):
            data = {}
        t = data.get("tokens") or {}
        ti = num(t.get("input"))
        to = num(t.get("output"))
        tr = num(t.get("reasoning"))
        tt = num(t.get("total"))
        cr = num((t.get("cache") or {}).get("read"))
        cost = num(data.get("cost"))
        ms = r["time_created"]
        day = _day(ms)
        if not day:
            continue
        cache = max(0.0, tt - ti - to - tr) or cr
        entry = tokens_by_day.setdefault(
            day, {"ti": 0.0, "to": 0.0, "tr": 0.0, "cache": 0.0, "cost": 0.0}
        )
        entry["ti"] += ti
        entry["to"] += to
        entry["tr"] += tr
        entry["cache"] += cache
        entry["cost"] += cost

    # One scan of the message table gives per-day counts plus the legacy
    # per-hour message intensity series.
    day_msgs = defaultdict(int)
    hours = defaultdict(int)
    for r in con.execute(
        """
        SELECT date(time_created/1000,'unixepoch','localtime') AS d,
               CAST(strftime('%H',time_created/1000,'unixepoch','localtime') AS INTEGER) AS h,
               COUNT(*) AS c
        FROM message GROUP BY d,h
        """
    ):
        day_msgs[r["d"]] += r["c"]
        hours[r["h"]] += r["c"]

    day_sessions = defaultdict(int)
    for r in con.execute(
        "SELECT date(time_created/1000,'unixepoch','localtime') AS d, COUNT(*) AS c FROM session GROUP BY d"
    ):
        day_sessions[r["d"]] = r["c"]

    agents = defaultdict(lambda: {"n": 0, "toks": 0})
    for r in con.execute(
        "SELECT agent, COUNT(*) AS n, COALESCE(SUM(tokens_input)+SUM(tokens_output)+SUM(tokens_reasoning)+SUM(tokens_cache_read)+SUM(tokens_cache_write),0) AS t FROM session GROUP BY agent"
    ):
        a = r["agent"] or "default"
        agents[a]["n"] += r["n"]
        agents[a]["toks"] += r["t"]

    models = defaultdict(lambda: {"n": 0, "ti": 0, "to": 0, "tr": 0, "cache": 0, "cost": 0})
    for r in con.execute(
        """
        SELECT model, COUNT(*) AS n,
                COALESCE(SUM(tokens_input),0) AS ti,
                COALESCE(SUM(tokens_output),0) AS to_,
                COALESCE(SUM(tokens_reasoning),0) AS tr,
                COALESCE(SUM(tokens_cache_read),0)+COALESCE(SUM(tokens_cache_write),0) AS ca,
                COALESCE(SUM(cost),0) AS cost
        FROM session GROUP BY model
        """
    ):
        m = parse_model(r["model"])
        models[m]["n"] += r["n"]
        models[m]["ti"] += r["ti"]
        models[m]["to"] += r["to_"]
        models[m]["tr"] += r["tr"]
        models[m]["cache"] += r["ca"]
        models[m]["cost"] += r["cost"]

    # Most recent sessions only — the payload stays bounded and the detail
    # endpoint fetches prompts on demand.
    sessions = []
    for r in con.execute(
        """
        SELECT s.id, s.title, s.agent, s.model, s.time_created, s.time_updated,
                s.tokens_input, s.tokens_output, s.tokens_reasoning,
                COALESCE(s.tokens_cache_read,0)+COALESCE(s.tokens_cache_write,0) AS cache_read, s.cost,
               p.worktree,
               (SELECT COUNT(*) FROM message m WHERE m.session_id = s.id) AS msgs
        FROM session s LEFT JOIN project p ON p.id = s.project_id
        ORDER BY s.time_created DESC LIMIT ?
        """,
        (MAX_SESSION_ROWS,),
    ):
        dur = max(0, (r["time_updated"] - r["time_created"]) / 60000.0)
        ti = r["tokens_input"] or 0
        to = r["tokens_output"] or 0
        tr = r["tokens_reasoning"] or 0
        ca = r["cache_read"] or 0
        total = ti + to + tr + ca
        sessions.append(
            {
                "id": r["id"],
                "title": r["title"],
                "agent": r["agent"] or "default",
                "model": parse_model(r["model"]),
                "time_created": r["time_created"],
                "time_updated": r["time_updated"],
                "duration_min": round(dur, 1),
                "tokens_input": ti,
                "tokens_output": to,
                "tokens_reasoning": tr,
                "tokens_cache": ca,
                "tokens_total": total,
                "burn": round(total / dur, 0) if dur >= 0.5 else total,
                "cost": r["cost"],
                "worktree": short_dir(r["worktree"]),
                "msgs": r["msgs"],
            }
        )

    projects = defaultdict(lambda: {"n": 0, "toks": 0})
    pid_info = {}
    for r in con.execute(
        """SELECT p.id, p.name, p.worktree, COUNT(s.id) AS n,
                   COALESCE(SUM(s.tokens_input)+SUM(s.tokens_output)+SUM(s.tokens_reasoning)+SUM(s.tokens_cache_read)+SUM(s.tokens_cache_write),0) AS t
            FROM project p LEFT JOIN session s ON s.project_id=p.id GROUP BY p.id"""
    ):
        key = r["name"] or short_dir(r["worktree"])
        pid_info[r["id"]] = (key, r["worktree"])
        if r["n"]:
            projects[key]["n"] += r["n"]
            projects[key]["toks"] += r["t"] or 0
    worktree_paths = [wt for _, wt in pid_info.values() if wt]

    # Full-table session map (not capped) for token attribution.
    sess_all = {}
    for r in con.execute(
        """SELECT id, project_id, time_updated,
                  COALESCE(tokens_input,0)+COALESCE(tokens_output,0)
                   +COALESCE(tokens_reasoning,0)+COALESCE(tokens_cache_read,0)
                   +COALESCE(tokens_cache_write,0) AS tot
           FROM session"""
    ):
        nm, wt = pid_info.get(r["project_id"], ("—", ""))
        sess_all[r["id"]] = (r["tot"] or 0, r["time_updated"] or 0, nm, wt)

    # Changed files from edit/write tool parts: distinct filePaths per session.
    sess_files = defaultdict(set)
    try:
        for r in con.execute(
            """SELECT session_id, json_extract(data,'$.state.input.filePath') AS fp
               FROM part
               WHERE json_extract(data,'$.type')='tool'
                 AND json_extract(data,'$.tool') IN ('edit','write')"""
        ):
            if r["fp"] and r["session_id"]:
                sess_files[r["session_id"]].add(r["fp"])
    except sqlite3.Error:
        pass

    # Equal-split each session's tokens across its touched files so table
    # sums reconcile with totals instead of double-counting shared sessions.
    file_agg = defaultdict(lambda: {"toks": 0.0, "sess": set()})
    subsys_agg = defaultdict(lambda: {"toks": 0.0, "sess": set(), "files": set()})
    file_subsys = {}
    for sid, files in sess_files.items():
        info = sess_all.get(sid)
        if not info or not files:
            continue
        tot = info[0]
        share = tot / len(files)
        for fp in files:
            fa = file_agg[fp]
            fa["toks"] += share
            fa["sess"].add(sid)
            sub = subsystem_for_file(fp, worktree_paths)
            file_subsys[fp] = sub
            sa = subsys_agg[sub]
            sa["toks"] += share
            sa["sess"].add(sid)
            sa["files"].add(fp)
    file_rows = sorted(
        ([fp, round(v["toks"], 1), len(v["sess"]),
          round(v["toks"] / len(v["sess"]), 0) if v["sess"] else 0]
         for fp, v in file_agg.items()),
        key=lambda r: -r[1],
    )[:MAX_FILE_ROWS]
    subsys_rows = sorted(
        ([k, round(v["toks"], 1), len(v["sess"]), len(v["files"])]
         for k, v in subsys_agg.items()),
        key=lambda r: -r[1],
    )[:MAX_FILE_ROWS]

    # Tokens per commit: sessions in the same project whose update time falls
    # in the 24h window before each commit.
    sess_by_proj = defaultdict(list)
    for sid, (tot, t_upd, nm, _wt) in sess_all.items():
        sess_by_proj[nm].append((t_upd, tot, sid))
    window_ms = COMMIT_WINDOW_HOURS * 3600 * 1000
    commit_rows = []
    commit_sessions = {}
    for c in collect_repo_commits([(nm, wt) for nm, wt in pid_info.values() if wt]):
        matched = [
            (t, tot, sid) for (t, tot, sid) in sess_by_proj.get(c["project"], [])
            if c["time"] - window_ms <= t <= c["time"]
        ]
        toks = sum(tot for _, tot, _ in matched)
        sids = [sid for _, _, sid in sorted(matched, reverse=True)[:25]]
        commit_rows.append(
            [c["sha"], c["subject"][:72], c["date"], c["project"],
             c["files"], c["add"], c["del"], round(toks, 1), len(matched)]
        )
        if sids:
            commit_sessions[c["sha"]] = sids
    commit_rows.sort(key=lambda r: -r[7])
    commit_rows = commit_rows[:MAX_COMMIT_ROWS]
    keep_shas = {r[0] for r in commit_rows}
    commit_sessions = {k: v for k, v in commit_sessions.items() if k in keep_shas}

    days = sorted(set(day_msgs) | set(day_sessions) | set(tokens_by_day))
    day_entries = [
        {
            "date": d,
            "msgs": day_msgs.get(d, 0),
            "sessions": day_sessions.get(d, 0),
            **{k: round(v, 4) for k, v in tokens_by_day.get(d, {"ti": 0, "to": 0, "tr": 0, "cache": 0, "cost": 0}).items()},
        }
        for d in days
    ]
    for e in day_entries:
        e["total"] = round(e["ti"] + e["to"] + e["tr"] + e["cache"], 1)
    activity = pad_activity(day_entries)
    for slot in activity:
        slot["total"] = round(slot["ti"] + slot["to"] + slot["tr"] + slot["cache"], 1)

    # Cache = read + write everywhere locally: part 'total' fields already
    # contain both (total = input+output+reasoning+read+write), so the
    # derived per-day cache matches the session rollups exactly.
    total_tokens = (totals["tokens_input"] + totals["tokens_output"]
                    + totals["tokens_reasoning"] + totals["tokens_cache_read"]
                    + totals["tokens_cache_write"])
    totals["tokens_total"] = total_tokens
    totals["cache_rate"] = cache_rate(totals["tokens_input"], totals["tokens_cache_read"])
    totals["avg_tokens_per_session"] = round(total_tokens / totals["sessions"], 0) if totals["sessions"] else 0

    # Records: biggest day / session / burn / streaks — pure tokens, fun to know.
    # Biggest session + fastest burn come from full-table queries (not the
    # capped recent-sessions list) so large histories still report all-time
    # bests accurately.
    biggest_day = max(day_entries, key=lambda d: d["total"]) if day_entries else None
    biggest_row = con.execute(
        """
        SELECT title, model,
               COALESCE(tokens_input,0)+COALESCE(tokens_output,0)
                 +COALESCE(tokens_reasoning,0)+COALESCE(tokens_cache_read,0)
                 +COALESCE(tokens_cache_write,0) AS tot
        FROM session ORDER BY tot DESC LIMIT 1
        """
    ).fetchone()
    biggest_session = (
        {"title": biggest_row["title"], "tokens_total": biggest_row["tot"],
         "model": parse_model(biggest_row["model"])}
        if biggest_row else None
    )
    fastest_row = con.execute(
        """
        SELECT title,
               (COALESCE(tokens_input,0)+COALESCE(tokens_output,0)
                 +COALESCE(tokens_reasoning,0)+COALESCE(tokens_cache_read,0)
                 +COALESCE(tokens_cache_write,0)) AS tot,
               (time_updated-time_created)/60000.0 AS mins
        FROM session
        WHERE time_created>0 AND time_updated>time_created
          AND (time_updated-time_created)/60000.0 >= 0.5
        ORDER BY tot/((time_updated-time_created)/60000.0) DESC LIMIT 1
        """
    ).fetchone()
    fastest = (
        {"title": fastest_row["title"],
         "burn": round(fastest_row["tot"] / fastest_row["mins"], 0)}
        if fastest_row else None
    )
    streaks = compute_streaks(activity)
    busiest = biggest_day["date"] if biggest_day else None
    top_model = sorted(models.items(), key=lambda kv: -(kv[1]["ti"] + kv[1]["to"] + kv[1]["tr"] + kv[1]["cache"]))[0] if models else None
    top_agent = sorted(agents.items(), key=lambda kv: -kv[1]["n"])[0] if agents else ("—", {"n": 0})
    longest = con.execute(
        """
        SELECT title, (time_updated-time_created)/60000.0 AS mins FROM session
        WHERE time_created>0 AND time_updated>time_created
        ORDER BY (time_updated-time_created) DESC LIMIT 1
        """
    ).fetchone()

    insights = [
        f"Across {totals['sessions']} sessions you used {total_tokens:,.0f} tokens in {totals['messages']:,} messages.",
        f"Your biggest token day was {busiest} — {biggest_day['total']:,.0f} tokens across {biggest_day['sessions']} sessions."
        if biggest_day else "No activity recorded yet.",
        f"Most-used model: {top_model[0]} — {top_model[1]['n']:,} sessions, {top_model[1]['ti'] + top_model[1]['to'] + top_model[1]['tr'] + top_model[1]['cache']:,.0f} tokens."
        if top_model else "",
        f"The {top_agent[0]} agent did the heavy lifting with {top_agent[1]['n']} sessions and {top_agent[1]['toks']:,} tokens."
        if agents else "",
        f"Top subsystem: {subsys_rows[0][0]} — {subsys_rows[0][1]:,.0f} tokens across {subsys_rows[0][2]} sessions."
        if subsys_rows else "",
        f"Priciest commit: “{(commit_rows[0][1] or '')[:60]}” — {commit_rows[0][7]:,.0f} tokens in the prior 24h."
        if commit_rows and commit_rows[0][7] else "",
        f"Longest session: “{longest['title']}” — {fmt_dur_min(longest['mins'])}."
        if longest else "",
        (
            f"Cache hit rate is {totals['cache_rate']}% — {totals['tokens_cache_read'] + totals['tokens_cache_write']:,.0f} cache tokens reused."
            if totals["tokens_cache_read"] else ""
        ),
    ]
    insights = [i for i in insights if i]

    model_rows = []
    for k, v in models.items():
        tot = v["ti"] + v["to"] + v["tr"] + v["cache"]
        model_rows.append(
            [k, v["n"], tot, v["ti"], v["to"], v["tr"], v["cache"], v["cost"],
             round(tot / v["n"], 0) if v["n"] else 0,
             cache_rate(v["ti"], v["cache"])]
        )
    model_rows.sort(key=lambda kv: -kv[2])

    recent_ids = {s["id"] for s in sessions}
    session_files = {
        sid: sorted(files)[:20] for sid, files in sess_files.items() if sid in recent_ids
    }

    stats = {
        "totals": totals,
        "sessions": sessions,
        "tools": [],
        "agents": sorted([[k, v["n"], v["toks"]] for k, v in agents.items()], key=lambda kv: -kv[1]),
        "models": model_rows,
        "projects": sorted([[k, v["n"], v["toks"]] for k, v in projects.items()], key=lambda kv: -kv[2]),
        "files": file_rows,
        "subsystems": subsys_rows,
        "file_subsys": file_subsys,
        "commits": commit_rows,
        "session_files": session_files,
        "commit_sessions": commit_sessions,
        "days": day_entries,
        "hours": [hours.get(h, 0) for h in range(24)],
        "activity": activity,
        "insights": insights,
        "records": {
            "biggest_day": {"date": biggest_day["date"], "total": biggest_day["total"], "sessions": biggest_day["sessions"]} if biggest_day else None,
            "biggest_session": {"title": biggest_session["title"], "total": biggest_session["tokens_total"], "model": biggest_session["model"]} if biggest_session else None,
            "fastest_burn": {"title": fastest["title"], "burn": fastest["burn"]} if fastest else None,
            "streaks": streaks,
        },
        "range": f"{days[0]} → {days[-1]}" if days else "no activity",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    stats["source"] = "local"
    stats["source_label"] = "Local OpenCode data"
    return stats


def num(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


ZERO_TOTALS = {
    "sessions": 0,
    "messages": 0,
    "tokens_input": 0,
    "tokens_output": 0,
    "tokens_reasoning": 0,
    "tokens_cache_read": 0,
    "tokens_cache_write": 0,
    "tokens_total": 0,
    "cache_rate": 0.0,
    "avg_tokens_per_session": 0,
    "cost": 0,
    "avg_duration_min": 0,
}


def blank_stats(source="local", source_label="Local OpenCode data", error=None, insight=None):
    """Well-formed zero/empty stats skeleton (local-only, token-first)."""
    now = datetime.now().isoformat(timespec="seconds")
    stats = {
        "source": source,
        "source_label": source_label,
        "totals": dict(ZERO_TOTALS),
        "sessions": [],
        "tools": [],
        "agents": [],
        "models": [],
        "projects": [],
        "files": [],
        "subsystems": [],
        "file_subsys": {},
        "commits": [],
        "session_files": {},
        "commit_sessions": {},
        "days": [],
        "hours": [0] * 24,
        "activity": zero_days_window(),
        "insights": [],
        "records": {
            "biggest_day": None,
            "biggest_session": None,
            "fastest_burn": None,
            "streaks": {"current": 0, "longest": 0},
        },
        "range": "no activity",
        "generated_at": now,
    }
    if insight:
        stats["insights"] = [insight]
    if error:
        stats["error"] = error
    return stats


def _fingerprint_paths(paths):
    """mtime+size fingerprint covering SQLite sidecars.

    In WAL mode all writes land in `<db>-wal` while the main file may keep
    the same mtime/size for long stretches. Fingerprinting only the main
    file would then serve stale stats, so -wal/-shm are included.
    """
    fps = []
    for p in paths:
        candidates = [Path(p)]
        if Path(p).suffix in ("", ".db"):
            candidates += [Path(str(p) + "-wal"), Path(str(p) + "-shm")]
        for q in candidates:
            try:
                st = q.stat()
                fps.append((str(q), st.st_mtime_ns, st.st_size))
            except OSError:
                fps.append((str(q), None, None))
    return tuple(fps)


def cached_local_stats(db_path):
    path = Path(db_path)
    try:
        path.stat()
    except OSError:
        raise RuntimeError(
            f"Database not found: {db_path}\nRun OpenCode at least once to create it."
        ) from None
    # Git HEADs join the fingerprint so fresh commits refresh the per-commit
    # panel even when the database itself is untouched.
    fingerprint = (_fingerprint_paths([path]), repo_heads(db_worktrees(db_path)))
    with LOCK:
        if LOCAL_CACHE.get("key") == fingerprint and LOCAL_CACHE.get("stats") is not None:
            return LOCAL_CACHE["stats"]
    con = connect(db_path)
    try:
        stats = query_stats(con)
    finally:
        con.close()
    with LOCK:
        LOCAL_CACHE["key"] = fingerprint
        LOCAL_CACHE["stats"] = stats
    return stats


def router_short(model):
    """Short display name: last path segment, e.g. 'a/b' -> 'b'."""
    if not model:
        return "—"
    return str(model).split("/")[-1] or str(model)


def router_is_free(model):
    return str(model or "").endswith("-free")


def what_if_cost(short, ti, to):
    """What-if paid cost for free-model tokens; 0.0 when pricing unknown."""
    pricing = WHAT_IF_PRICING.get(short or "")
    if not pricing:
        return 0.0
    pi, po = pricing
    return round((ti or 0) / 1e6 * pi + (to or 0) / 1e6 * po, 6)


def _router_event_day_hour(at):
    """Parse usage-events 'at' ISO timestamp -> (local date, local hour)."""
    if not at:
        return None, None
    try:
        s = str(at).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.astimezone()
        return dt.date().isoformat(), dt.hour
    except (ValueError, TypeError, OverflowError):
        return None, None


def parse_router_events(path, limit=MAX_ROUTER_EVENTS):
    """Read usage-events.jsonl tail; return list of normalized event dicts.

    Each event: {at, date, hour, model, short, provider, status, ok,
    ti, cache, to, total, ms, free, what_if}. Missing token fields on
    error rows (401/429/500) become 0 and are counted as errors, never
    estimated — actuals only.
    """
    p = Path(path)
    try:
        with open(p, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except OSError:
        raise RuntimeError(f"Router usage file not found: {path}")
    if limit and len(lines) > limit:
        lines = lines[-limit:]
    events = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            e = json.loads(line)
        except ValueError:
            continue
        if not isinstance(e, dict):
            continue
        model = e.get("model") or "—"
        short = router_short(model)
        try:
            status = int(e.get("status", 0))
        except (TypeError, ValueError):
            status = 0
        ok = status in (0, 200) or 200 <= status < 300
        ti = num(e.get("inputTokens"))
        cache = num(e.get("cachedInputTokens"))
        to = num(e.get("outputTokens"))
        total = num(e.get("totalTokens"))
        if not total:
            total = ti + to
        try:
            ms = float(e.get("durationMs") or 0)
        except (TypeError, ValueError):
            ms = 0.0
        date, hour = _router_event_day_hour(e.get("at"))
        events.append(
            {
                "at": e.get("at"),
                "date": date,
                "hour": hour,
                "model": str(model),
                "short": short,
                "provider": str(e.get("provider") or "—"),
                "status": status,
                "ok": ok,
                "ti": ti,
                "cache": cache,
                "to": to,
                "total": total,
                "ms": ms,
                "free": router_is_free(model),
                "what_if": what_if_cost(short, ti, to),
            }
        )
    return events


def _router_time_key(at):
    """Parse a usage-events 'at' timestamp to epoch seconds for ordering.

    Falls back to 0.0 for missing/unparseable values (sorted last).
    Naive datetimes are interpreted in local time, aware ones converted.
    """
    if not at:
        return 0.0
    try:
        s = str(at).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt.timestamp()
    except (ValueError, TypeError, OverflowError):
        return 0.0


def read_rate_limits(path):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def query_router_stats(events_path, limits_path=None, worktrees=None):
    """Aggregate Codex Router usage-events into a dashboard payload (all models).

    Only the most recent MAX_ROUTER_EVENTS lines are scanned; when the file
    is longer the payload reports truncated=True with scanned/total_lines
    counts so the window is never silently understated.

    `worktrees` ([(name, path)]) feeds the tokens-per-commit panel: router
    events carry no project info, so attribution is timestamp-only — request
    tokens in the 24h window before each commit.
    """
    try:
        with open(events_path, "r", encoding="utf-8", errors="replace") as f:
            total_lines = sum(1 for _ in f)
    except OSError:
        raise RuntimeError(f"Router usage file not found: {events_path}")
    truncated = total_lines > MAX_ROUTER_EVENTS
    scanned_events = parse_router_events(events_path, MAX_ROUTER_EVENTS)
    # Successful responses that carried no token fields (local/unmetered
    # models, image calls) are excluded from every count and average —
    # never estimated — and reported separately as `unmetered`.
    unmetered = sum(1 for e in scanned_events if e["ok"] and not e["total"])
    events = [e for e in scanned_events if not e["ok"] or e["total"]]
    ok_events = [e for e in events if e["ok"]]
    err_events = [e for e in events if not e["ok"]]
    err429 = sum(1 for e in events if e["status"] == 429)
    err500 = sum(1 for e in events if e["status"] >= 500)

    ti = sum(e["ti"] for e in ok_events)
    cache = sum(e["cache"] for e in ok_events)
    to = sum(e["to"] for e in ok_events)
    what_if = round(sum(e["what_if"] for e in ok_events), 4)
    free_unpriced = sum(1 for e in ok_events if e["free"] and not e["what_if"])
    # Latency over successful requests only: fast 429/401 rejects would
    # otherwise drag the average down and misrepresent model speed.
    ms_vals = [e["ms"] for e in ok_events if e["ms"] > 0]
    avg_ms = round(sum(ms_vals) / len(ms_vals), 1) if ms_vals else 0

    by_model = {}
    for e in events:
        m = by_model.setdefault(
            e["model"],
            {"provider": e["provider"], "reqs": 0, "ok": 0, "err": 0,
             "ti": 0.0, "to": 0.0, "cache": 0.0, "total": 0.0,
             "what_if": 0.0, "ms": 0.0, "free": e["free"]},
        )
        m["reqs"] += 1
        if e["ok"]:
            m["ok"] += 1
            m["ti"] += e["ti"]
            m["to"] += e["to"]
            m["cache"] += e["cache"]
            m["total"] += e["total"]
            m["what_if"] += e["what_if"]
        else:
            m["err"] += 1
        m["ms"] += e["ms"]

    model_rows = []
    for name, m in by_model.items():
        tot = m["total"]
        model_rows.append(
            [name, m["reqs"], tot, m["ti"], m["to"], m["cache"], m["ok"], m["err"],
             round(tot / m["ok"], 0) if m["ok"] else 0,
             router_cache_rate(m["ti"], m["cache"]), m["provider"],
             m["free"], round(m["what_if"], 4)]
        )
    model_rows.sort(key=lambda r: -r[2])

    by_provider = defaultdict(lambda: {"reqs": 0, "toks": 0.0})
    for e in ok_events:
        by_provider[e["provider"]]["reqs"] += 1
        by_provider[e["provider"]]["toks"] += e["total"]

    day_buckets = {}
    hour_reqs = defaultdict(int)
    hour_tokens = defaultdict(float)
    for e in events:
        if e["hour"] is not None:
            hour_reqs[e["hour"]] += 1
            if e["ok"]:
                hour_tokens[e["hour"]] += e["total"]
        if not e["date"]:
            continue
        b = day_buckets.setdefault(
            e["date"], {"reqs": 0, "ok": 0, "err": 0, "err429": 0, "err500": 0, "ti": 0.0, "to": 0.0,
                        "cache": 0.0, "what_if": 0.0}
        )
        b["reqs"] += 1
        if e["status"] == 429:
            b["err429"] += 1
        if e["status"] >= 500:
            b["err500"] += 1
        if e["ok"]:
            b["ok"] += 1
            b["ti"] += e["ti"]
            b["to"] += e["to"]
            b["cache"] += e["cache"]
            b["what_if"] += e["what_if"]
        else:
            b["err"] += 1

    day_entries = []
    for d in sorted(day_buckets):
        b = day_buckets[d]
        # Router inputTokens already includes cachedInputTokens.
        tot = b["ti"] + b["to"]
        day_entries.append(
            {"date": d, "msgs": b["reqs"], "sessions": b["ok"], "reqs": b["reqs"],
             "ok": b["ok"], "err": b["err"], "err429": b["err429"], "err500": b["err500"],
             "ti": round(b["ti"], 1),
             "to": round(b["to"], 1), "tr": 0, "cache": round(b["cache"], 1),
             "cost": round(b["what_if"], 4), "what_if": round(b["what_if"], 4),
             "total": round(tot, 1)}
        )
    activity = pad_activity(day_entries)
    for slot in activity:
        slot["total"] = round(slot["ti"] + slot["to"] + slot["tr"], 1)
        slot.setdefault("reqs", slot.get("msgs", 0))

    recent = sorted(events, key=lambda e: (_router_time_key(e.get("at")), str(e.get("at") or "")),
                      reverse=True)[:MAX_ROUTER_ROWS]

    # Tokens per commit from router request tokens (timestamp-only join: the
    # usage-events stream has no project/file info). Same commit list and
    # 24h window as the local tab so both sides stay comparable.
    ok_times = sorted(
        (_router_time_key(e.get("at")) * 1000, e["total"])
        for e in ok_events if _router_time_key(e.get("at")) > 0
    )
    window_ms = COMMIT_WINDOW_HOURS * 3600 * 1000
    r_commit_rows = []
    for c in collect_repo_commits(worktrees or []):
        lo = c["time"] - window_ms
        toks = reqs = 0
        for t_ms, tot in ok_times:
            if t_ms > c["time"]:
                break
            if t_ms >= lo:
                toks += tot
                reqs += 1
        r_commit_rows.append(
            [c["sha"], c["subject"][:72], c["date"], c["project"],
             c["files"], c["add"], c["del"], round(toks, 1), reqs]
        )
    r_commit_rows.sort(key=lambda r: -r[7])
    r_commit_rows = r_commit_rows[:MAX_COMMIT_ROWS]

    avg_per_session = round((ti + to) / len(ok_events), 0) if ok_events else 0
    totals = {
        "requests": len(events),
        "ok": len(ok_events),
        "errors": len(err_events),
        "err429": err429,
        "err500": err500,
        "unmetered": unmetered,
        "tokens_input": ti,
        "tokens_output": to,
        "tokens_reasoning": 0,
        "tokens_cache_read": cache,
        "tokens_cache_write": 0,
        "tokens_total": ti + to,
        "cache_rate": router_cache_rate(ti, cache),
        "models": len(by_model),
        "providers": len(by_provider),
        "what_if_cost": what_if,
        "avg_ms": avg_ms,
        "sessions": len(ok_events),
        "messages": len(events),
        "cost": 0,
        "avg_duration_min": 0,
        "avg_tokens_per_session": avg_per_session,
        "avg_tokens_per_metered": avg_per_session,
    }

    biggest_day = max(day_entries, key=lambda d: d["total"]) if day_entries else None
    top_model = model_rows[0] if model_rows else None
    biggest_req = max(ok_events, key=lambda e: e["total"]) if ok_events else None
    peak_hour = max(range(24), key=lambda h: hour_tokens.get(h, 0)) if any(hour_tokens.values()) else None
    insights = [
        f"Codex made {len(events):,} requests across {len(by_model)} models — {len(ok_events):,} ok, {len(err_events):,} errors.",
        (f"Top model: {router_short(top_model[0])} — {top_model[2]:,.0f} tokens over {top_model[1]:,} requests."
         if top_model else ""),
        (f"Busiest day: {biggest_day['date']} — {biggest_day['total']:,.0f} tokens, {biggest_day['reqs']} requests."
         if biggest_day else "No Codex activity recorded yet."),
    ]
    if err429:
        insights.append(f"{err429} requests hit 429 rate limits — usually free-model capacity, not token size.")
    if unmetered:
        insights.append(
            f"{unmetered:,} successful requests reported no token counts "
            f"(local / unmetered models) — excluded from all counts and averages."
        )
    if free_unpriced:
        insights.append(
            f"What-if paid ${what_if:.2f} covers only models with known pricing "
            f"(muse-spark) — {free_unpriced:,} other free-model requests have no rate on file."
        )
    if truncated:
        insights.append(
            f"Showing the most recent {len(scanned_events):,} of {total_lines:,} events — "
            f"totals cover the scanned window only."
        )
    insights = [i for i in insights if i]

    limits = read_rate_limits(limits_path) if limits_path else {}
    stats = {
        "source": "router",
        "source_label": "Codex data",
        "totals": totals,
        "models": model_rows,
        "providers": sorted([[k, v["reqs"], round(v["toks"], 1)] for k, v in by_provider.items()],
                            key=lambda kv: -kv[2]),
        "days": day_entries,
        "hours": [hour_reqs.get(h, 0) for h in range(24)],
        "commits": r_commit_rows,
        "activity": activity,
        "requests": recent,
        "rate_limits": limits,
        "insights": insights,
        "records": {
            "biggest_day": {"date": biggest_day["date"], "total": biggest_day["total"],
                            "sessions": biggest_day["reqs"]} if biggest_day else None,
            "biggest_session": None,
            "fastest_burn": None,
            "biggest_model": {"name": top_model[0], "total": top_model[2],
                              "requests": top_model[1]} if top_model else None,
            "biggest_request": {"at": biggest_req["at"], "model": biggest_req["model"],
                                "total": biggest_req["total"]} if biggest_req else None,
            "peak_hour": peak_hour,
            "streaks": compute_streaks(activity, tokens_only=True),
        },
        "range": f"{day_entries[0]['date']} → {day_entries[-1]['date']}" if day_entries else "no activity",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scanned": len(scanned_events),
        "total_lines": total_lines,
        "truncated": truncated,
    }
    return stats


def blank_router_stats(error=None):
    """Well-formed empty router payload (same shape as query_router_stats)."""
    now = datetime.now().isoformat(timespec="seconds")
    stats = {
        "source": "router",
        "source_label": "Codex data",
        "totals": {"requests": 0, "ok": 0, "errors": 0, "err429": 0, "err500": 0,
                   "unmetered": 0,
                   "tokens_input": 0, "tokens_output": 0, "tokens_reasoning": 0,
                   "tokens_cache_read": 0, "tokens_cache_write": 0, "tokens_total": 0,
                   "cache_rate": 0.0, "models": 0, "providers": 0, "what_if_cost": 0,
                   "avg_ms": 0, "sessions": 0, "messages": 0, "cost": 0,
                   "avg_duration_min": 0, "avg_tokens_per_session": 0,
                   "avg_tokens_per_metered": 0},
        "models": [],
        "providers": [],
        "days": [],
        "hours": [0] * 24,
        "commits": [],
        "activity": zero_days_window(),
        "requests": [],
        "rate_limits": {},
        "insights": ["No Codex activity found."],
        "records": {"biggest_day": None, "biggest_session": None,
                    "fastest_burn": None, "biggest_model": None,
                    "biggest_request": None, "peak_hour": None,
                    "streaks": {"current": 0, "longest": 0}},
        "range": "no activity",
        "generated_at": now,
        "scanned": 0,
        "total_lines": 0,
        "truncated": False,
    }
    if error:
        stats["error"] = error
    return stats


def cached_router_stats(events_path, limits_path=None, worktrees=None):
    paths = [Path(events_path)]
    if limits_path:
        paths.append(Path(limits_path))
    try:
        fps = []
        for p in paths:
            st = p.stat()
            fps.append((str(p), st.st_mtime_ns, st.st_size))
        fingerprint = (tuple(fps), repo_heads(worktrees))
    except OSError:
        raise RuntimeError(f"Router usage file not found: {events_path}")
    with LOCK:
        if ROUTER_CACHE.get("key") == fingerprint and ROUTER_CACHE.get("stats") is not None:
            return ROUTER_CACHE["stats"]
    stats = query_router_stats(events_path, limits_path, worktrees)
    with LOCK:
        ROUTER_CACHE["key"] = fingerprint
        ROUTER_CACHE["stats"] = stats
    return stats


def session_prompts(con, session_id):
    """Prompts for one session — from session_input when available, otherwise
    reconstructed from user text parts (older opencode versions)."""
    rows = [
        {"t": r["time_created"], "p": r["prompt"]}
        for r in con.execute(
            "SELECT time_created, prompt FROM session_input "
            "WHERE session_id=? ORDER BY time_created",
            (session_id,),
        )
    ]
    if rows:
        return rows[-50:]
    rows = []
    for r in con.execute(
        """
        SELECT p.time_created, json_extract(p.data,'$.text') AS txt
        FROM part p JOIN message m ON m.id = p.message_id
        WHERE p.session_id = ?
          AND json_extract(p.data,'$.type')='text'
          AND json_extract(m.data,'$.role')='user'
        ORDER BY p.time_created
        """,
        (session_id,),
    ):
        txt = (r["txt"] or "").strip()
        if txt:
            rows.append({"t": r["time_created"], "p": txt})
    return rows[-50:]


PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>OpenCode Dashboard</title>
  <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect x='8' y='30' width='18' height='40' rx='4' fill='%234f46e5'/><rect x='40' y='14' width='18' height='56' rx='4' fill='%230d9488'/><rect x='72' y='44' width='18' height='26' rx='4' fill='%23d97706'/></svg>">
<style>
:root{
  --bg:#f7f7f5; --panel:#ffffff; --panel2:#fbfbfa; --line:#e7e5e0; --line2:#efede8;
  --text:#1c1c1a; --muted:#6d6a63; --subtle:#9a968c;
  --accent:#4f46e5; --accent2:#0d9488; --accent3:#9333ea; --cache:#d97706;
  --good:#16a34a; --bad:#dc2626; --chip:#f1efe9;
  --shadow:0 1px 2px rgba(28,28,26,.05),0 8px 24px rgba(28,28,26,.06);
  --mono:ui-monospace,"SF Mono",SFMono-Regular,Menlo,Consolas,"Liberation Mono",monospace;
  --sans:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;
}
[data-theme="dark"]{
  --bg:#0c0d10; --panel:#131418; --panel2:#17181d; --line:#24252c; --line2:#1e1f25;
  --text:#e9e8e4; --muted:#9b998f; --subtle:#6a685e;
  --accent:#7c86ff; --accent2:#2dd4bf; --accent3:#c084fc; --cache:#fbbf24;
  --good:#4ade80; --bad:#f87171; --chip:#1d1e24;
  --shadow:0 1px 2px rgba(0,0,0,.4),0 10px 30px rgba(0,0,0,.35);
}
*{box-sizing:border-box;margin:0;padding:0}
html{scroll-behavior:smooth}
body{background:var(--bg);color:var(--text);font-family:var(--sans);font-size:14px;line-height:1.5;
  transition:background .25s ease,color .25s ease}
.wrap{max-width:1080px;margin:0 auto;padding:0 28px 70px}
.mono{font-family:var(--mono)}

/* top bar */
.topbar{position:sticky;top:0;z-index:40;background:var(--bg);border-bottom:1px solid var(--line)}
.topbar-in{max-width:1080px;margin:0 auto;padding:12px 28px;display:flex;align-items:center;gap:14px}
.brand{display:flex;align-items:center;gap:9px;font-weight:800;font-size:15px;letter-spacing:-.02em}
.brand .dot{width:9px;height:9px;border-radius:3px;background:linear-gradient(135deg,var(--accent),var(--accent2))}
.brand small{color:var(--subtle);font-weight:600}
.spacer{flex:1}
.tbtn{appearance:none;background:var(--chip);border:1px solid var(--line);color:var(--muted);border-radius:9px;
  height:34px;padding:0 12px;font-family:var(--sans);font-size:12.5px;font-weight:600;cursor:pointer;transition:.15s}
.tbtn:hover{color:var(--text);border-color:var(--subtle)}
.tbtn:active{transform:scale(.97)}
.tbtn:disabled{opacity:.5;cursor:wait}
#status{font-family:var(--mono);font-size:11px;color:var(--subtle);white-space:nowrap}

/* hero */
.hero{padding:52px 0 14px}
.hero .eyebrow{font-family:var(--mono);font-size:11.5px;color:var(--subtle);letter-spacing:.12em;text-transform:uppercase}
.hero h1{font-size:clamp(26px,4vw,38px);font-weight:800;letter-spacing:-.035em;line-height:1.12;margin-top:10px;max-width:720px}
.hero h1 b{font-weight:800;background:linear-gradient(100deg,var(--accent),var(--accent2));-webkit-background-clip:text;background-clip:text;color:transparent}
.hero .pills{display:flex;flex-wrap:wrap;gap:8px;margin-top:18px}
.pill{font-family:var(--mono);font-size:11px;background:var(--panel);border:1px solid var(--line);color:var(--muted);
  padding:5px 12px;border-radius:999px}

/* KPI strip */
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(290px,1fr));gap:12px;margin-top:26px}
.kpi{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:16px 18px;box-shadow:var(--shadow)}
.kpi h3{font-size:10.5px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--subtle);margin-bottom:10px}
.kpi .big{font-family:var(--mono);font-size:26px;font-weight:700;letter-spacing:-.03em;font-variant-numeric:tabular-nums}
.kpi .big small{font-size:11.5px;color:var(--subtle);font-weight:500}
.tokbar{display:flex;height:10px;border-radius:6px;overflow:hidden;background:var(--chip);margin:12px 0 10px}
.tokbar i{height:100%}
.krow{display:flex;justify-content:space-between;align-items:center;gap:10px;font-size:12.5px;color:var(--muted);padding:3.5px 0;font-variant-numeric:tabular-nums}
.krow b{color:var(--text);font-family:var(--mono);font-weight:600}
.krow .lbl{display:flex;align-items:center;min-width:0}
.krow .dot{width:8px;height:8px;border-radius:3px;margin-right:8px;flex-shrink:0}
.krow .dim{color:var(--subtle);font-size:11px;margin-left:5px}

/* sections */
h2.sec{font-size:17px;font-weight:700;letter-spacing:-.02em;margin:34px 0 14px;display:flex;align-items:baseline;gap:10px;scroll-margin-top:70px}
h2.sec .hint{font-family:var(--mono);font-size:11px;color:var(--subtle);font-weight:500}
.card{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:20px 22px;box-shadow:var(--shadow)}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:14px}
@media(max-width:900px){.grid2{grid-template-columns:1fr}}
.chart-card .ctitle{font-size:11px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--subtle);margin-bottom:12px}
.chart-card .ctitle small{font-weight:500;letter-spacing:0;text-transform:none;color:var(--subtle);margin-left:8px}

/* insight cards */
.insights{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:12px;margin-top:20px}
.in{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:14px 16px;box-shadow:var(--shadow)}
.in p{font-size:13px;color:var(--muted);line-height:1.55}
.in p b{color:var(--text)}

/* legend */
.legend{display:flex;gap:16px;flex-wrap:wrap;margin-top:12px;font-size:11.5px;color:var(--muted)}
.legend i{display:inline-block;width:9px;height:9px;border-radius:3px;margin-right:6px;vertical-align:-1px}

/* tools */
.tool{display:flex;align-items:center;gap:12px;margin-bottom:12px}
.tool:last-child{margin-bottom:0}
.tool .nm{width:92px;font-size:12.5px;text-align:right;color:var(--muted);font-weight:600;flex-shrink:0;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tool .track{flex:1;height:8px;background:var(--chip);border-radius:6px;overflow:hidden}
.tool .fill{height:100%;border-radius:6px;background:linear-gradient(90deg,var(--accent),var(--accent2));width:0;transition:width .7s cubic-bezier(.22,.9,.35,1)}
.tool .ct{width:88px;text-align:right;font-family:var(--mono);font-size:12px;color:var(--muted)}
.tool.dim{opacity:.3}
.tool.clickable{cursor:pointer}
.tool.clickable:hover .nm{color:var(--text)}

/* chips */
.chips{display:flex;flex-wrap:wrap;gap:8px}
.chip{display:flex;align-items:center;gap:8px;background:var(--chip);border:1px solid var(--line);border-radius:10px;
  padding:8px 13px;font-size:12.5px;cursor:pointer;transition:border-color .12s,transform .12s}
.chip:hover{border-color:var(--subtle);transform:translateY(-1px)}
.chip.active{border-color:var(--accent);box-shadow:0 0 0 2px color-mix(in srgb,var(--accent) 25%,transparent)}
.chip b{font-family:var(--mono);font-weight:700}
.chip .n{color:var(--muted)}

/* interactive svg */
.inter{cursor:pointer}
.inter:hover{filter:brightness(1.15)}
.dim{opacity:.22}

/* table */
.tbl-tools{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px;align-items:center}
.search{flex:1;min-width:200px;background:var(--bg);border:1px solid var(--line);border-radius:9px;padding:9px 13px;
  font-family:var(--sans);font-size:13px;color:var(--text);outline:none}
.search:focus{border-color:var(--accent)}
.search::placeholder{color:var(--subtle)}
.filter{appearance:none;background:var(--bg);border:1px solid var(--line);border-radius:9px;padding:8px 12px;
  font-family:var(--sans);font-size:12.5px;color:var(--muted);cursor:pointer;outline:none}
.fbar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;padding:9px 12px;margin-bottom:10px;
  background:color-mix(in srgb,var(--accent) 6%,transparent);border:1px solid color-mix(in srgb,var(--accent) 30%,transparent);
  border-radius:10px;font-size:12.5px}
.fbar .fl{color:var(--muted);font-weight:600}
.fchip{display:inline-flex;align-items:center;gap:7px;background:var(--panel);border:1px solid var(--line);border-radius:999px;
  padding:3px 12px;font-family:var(--mono);font-size:11.5px;cursor:pointer}
.fchip:hover{border-color:var(--bad)}
.fchip .fx{color:var(--subtle);font-weight:800}
.fchip:hover .fx{color:var(--bad)}
.fclear{background:none;border:none;color:var(--bad);font-family:var(--sans);font-size:12px;font-weight:600;cursor:pointer;padding:3px 6px}
.fclear:hover{text-decoration:underline}
.tbl-scroll{overflow:auto;max-height:min(62vh,640px)}
.tbl{width:100%;border-collapse:separate;border-spacing:0;font-size:13px}
.tbl thead th{position:sticky;top:0;z-index:5;background:var(--panel);text-align:left;font-size:10.5px;font-weight:700;
  text-transform:uppercase;letter-spacing:.08em;color:var(--subtle);padding:10px 12px;
  box-shadow:0 1px 0 var(--line);white-space:nowrap;cursor:pointer;user-select:none}
.tbl thead th:hover{color:var(--text)}
.tbl thead th.asc::after{content:" ↑"}
.tbl thead th.desc::after{content:" ↓"}
.tbl td{padding:11px 12px;border-bottom:1px solid var(--line2);vertical-align:top}
.tbl tr:last-child td{border-bottom:none}
.tbl tbody tr{cursor:pointer;transition:background .12s}
.tbl tbody tr:hover{background:color-mix(in srgb,var(--accent) 5%,transparent)}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums}
.badge{display:inline-block;padding:2px 9px;border-radius:999px;font-size:10.5px;font-weight:700;background:var(--chip);color:var(--muted)}
.badge.agent{background:color-mix(in srgb,var(--accent2) 14%,transparent);color:var(--accent2)}
.badge.model{background:color-mix(in srgb,var(--accent3) 12%,transparent);color:var(--accent3)}
.badge.latest{background:color-mix(in srgb,var(--accent) 16%,transparent);color:var(--accent)}
.tbl tbody tr.latest td{background:color-mix(in srgb,var(--accent) 4%,transparent)}
.tbl tbody tr.latest td:first-child{box-shadow:inset 3px 0 0 var(--accent)}
.tbl .t{font-weight:700;line-height:1.35;overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;word-break:break-word}
.tbl .badges{display:flex;gap:4px;flex-wrap:wrap;margin-top:5px}
.tbl .badges.mix{flex-direction:column;align-items:flex-start;gap:4px;margin-top:0}
.tbl .badges .badge{display:inline-block;margin:0;max-width:190px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.pill.accent{border-color:color-mix(in srgb,var(--accent) 45%,transparent)}

/* activity heatmap (GitHub/Codex-style: one cell per day, 52 weeks) */
.act-head{display:flex;flex-wrap:wrap;gap:8px;align-items:baseline;margin-bottom:14px}
.act-head .sum{font-size:12.5px;color:var(--muted)}
.act-head .sum b{color:var(--text);font-family:var(--mono);font-weight:700}
.act-scroll{overflow-x:auto;padding-bottom:6px}
.act-grid{display:grid;grid-template-columns:auto 1fr;gap:8px 10px;min-width:640px}
.act-months{display:flex;grid-column:2;height:16px;font-size:9.5px;color:var(--subtle);white-space:nowrap}
.act-months span{padding-top:2px;overflow:hidden}
.act-body{display:flex;gap:8px}
.act-labels{display:flex;flex-direction:column;justify-content:space-around;width:26px;font-size:9px;color:var(--subtle);text-align:right}
.act-cols{display:flex;gap:3px}
.act-col{display:flex;flex-direction:column;gap:3px}
.act-cell{appearance:none;border:none;width:12px;height:12px;border-radius:3px;background:var(--chip);cursor:pointer;padding:0;transition:transform .08s,outline-color .08s}
.act-cell:hover{transform:scale(1.35)}
.act-cell:focus-visible{outline:2px solid var(--accent);outline-offset:1px;transform:scale(1.35)}
.act-cell.today{outline:1px solid var(--text);outline-offset:1px}
.act-cell.dim{opacity:.22}
.act-legend{display:flex;align-items:center;gap:5px;margin-top:12px;font-size:10.5px;color:var(--subtle)}
.act-legend .sw{width:11px;height:11px;border-radius:3px;display:inline-block}
.act-legend .sw.e{background:var(--chip)}

/* period presets */
.presets{display:flex;gap:6px;flex-wrap:wrap;align-items:center;margin-top:16px}
.presets .pl{font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--subtle);margin-right:4px}
.pbtn{appearance:none;background:var(--panel);border:1px solid var(--line);border-radius:999px;color:var(--muted);
  font-family:var(--mono);font-size:11px;font-weight:600;padding:5px 12px;cursor:pointer;transition:.15s}
.pbtn:hover{color:var(--text);border-color:var(--subtle)}
.pbtn.active{background:color-mix(in srgb,var(--accent) 14%,transparent);border-color:var(--accent);color:var(--accent)}
.pbtn:focus-visible{outline:2px solid var(--accent);outline-offset:2px}

/* KPI deltas */
.delta{font-family:var(--mono);font-size:11px;font-weight:700;margin-left:8px;padding:1px 7px;border-radius:999px}
.delta.up{color:var(--good);background:color-mix(in srgb,var(--good) 12%,transparent)}
.delta.down{color:var(--bad);background:color-mix(in srgb,var(--bad) 12%,transparent)}
.delta.flat{color:var(--subtle);background:var(--chip)}

/* export menu */
.export-wrap{position:relative}
.expmenu{position:absolute;right:0;top:calc(100% + 6px);z-index:50;background:var(--panel);border:1px solid var(--line);
  border-radius:10px;box-shadow:var(--shadow);padding:5px;display:none;min-width:150px}
.expmenu.open{display:block}
.expmenu button{display:block;width:100%;text-align:left;background:none;border:none;color:var(--text);
  font-family:var(--sans);font-size:13px;padding:8px 11px;border-radius:7px;cursor:pointer}
.expmenu button:hover{background:var(--chip)}
.expmenu button small{display:block;color:var(--subtle);font-size:11px}

/* toasts */
.toasts{position:fixed;right:18px;bottom:18px;z-index:120;display:flex;flex-direction:column;gap:8px;max-width:340px}
.toast{background:var(--panel);border:1px solid var(--line);border-left:3px solid var(--accent);border-radius:10px;
  box-shadow:var(--shadow);padding:11px 14px;font-size:12.5px;color:var(--text);animation:up .2s ease;line-height:1.45}
.toast.ok{border-left-color:var(--good)}
.toast.err{border-left-color:var(--bad)}

/* a11y */
:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
.tbl tbody tr:focus-visible{outline:2px solid var(--accent);outline-offset:-2px}
#search{outline:none}
#search:focus-visible{border-color:var(--accent)}
@media (prefers-reduced-motion: reduce){
  *{animation:none!important;transition:none!important}
  html{scroll-behavior:auto}
}
/* tooltip */
.tip{position:fixed;z-index:100;pointer-events:none;display:none;max-width:280px;
  background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:10px 13px;
  font-family:var(--sans);font-size:12px;line-height:1.55;color:var(--muted);
  box-shadow:var(--shadow);backdrop-filter:blur(8px)}
.tip b{color:var(--text);font-family:var(--mono);font-weight:700}
.tip .thint{display:block;margin-top:6px;font-size:10.5px;color:var(--accent);font-weight:600}
.tip .trow{display:flex;justify-content:space-between;gap:16px}
.tip .trow b{color:var(--text)}

/* modal */
.overlay{position:fixed;inset:0;background:rgba(0,0,0,.45);backdrop-filter:blur(4px);z-index:60;display:none;
  align-items:flex-end;justify-content:center}
.overlay.open{display:flex}
.modal{background:var(--panel);border:1px solid var(--line);border-top-left-radius:18px;border-top-right-radius:18px;
  width:100%;max-width:820px;max-height:88vh;overflow:auto;padding:24px 28px 34px;box-shadow:var(--shadow);
  animation:up .22s ease}
@keyframes up{from{transform:translateY(26px);opacity:0}to{transform:none;opacity:1}}
.modal-x{float:right;background:var(--chip);border:1px solid var(--line);color:var(--muted);border-radius:8px;
  width:30px;height:30px;cursor:pointer;font-size:14px}
.modal-x:hover{color:var(--text)}
.tabs{display:flex;gap:8px;margin:18px 0 -34px;position:relative;z-index:2}
.wrap.has-tabs .hero{padding-top:18px}
.tab{appearance:none;background:var(--panel);border:1px solid var(--line);border-bottom:none;border-radius:10px 10px 0 0;
  padding:9px 18px;font-family:var(--sans);font-size:12.5px;font-weight:700;color:var(--muted);cursor:pointer;transition:.15s}
.tab:hover{color:var(--text)}
.tab.active{color:var(--text);border-color:var(--accent);box-shadow:0 -2px 0 var(--accent) inset;background:var(--panel2)}
.banner{background:color-mix(in srgb,var(--bad) 10%,transparent);border:1px solid color-mix(in srgb,var(--bad) 35%,transparent);
  color:var(--bad);border-radius:10px;padding:10px 14px;margin-top:18px;font-size:12.5px;display:flex;gap:10px;align-items:center}
.banner b{font-weight:700}
.banner button{margin-left:auto;flex-shrink:0}
.settings-form{display:grid;gap:16px;margin-top:18px}
.srow{display:grid;gap:7px}
.srow label{font-size:10.5px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--subtle)}
.srow input[type=text],.srow input[type=password],.srow input[type=number],.srow select{
  background:var(--bg);border:1px solid var(--line);border-radius:9px;padding:9px 12px;font-family:var(--sans);
  font-size:13px;color:var(--text);outline:none;width:100%}
.srow input:focus,.srow select:focus{border-color:var(--accent)}
.srow .hint{font-size:11.5px;color:var(--subtle);text-transform:none;letter-spacing:0;font-weight:500}
.seg{display:inline-flex;border:1px solid var(--line);border-radius:10px;overflow:hidden;background:var(--bg);width:fit-content}
.segbtn{appearance:none;border:none;background:transparent;padding:8px 14px;font-family:var(--sans);font-size:12.5px;font-weight:600;color:var(--muted);cursor:pointer}
.segbtn.active{background:var(--accent);color:#fff}
.sbtns{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
#set-status{font-family:var(--mono);font-size:11px;color:var(--muted)}
.prompt{background:var(--bg);border:1px solid var(--line);border-left:3px solid var(--accent);border-radius:10px;
  padding:11px 14px;margin-bottom:10px;font-size:13px;color:var(--muted);line-height:1.55;white-space:pre-wrap;word-break:break-word}
.mmeta{display:flex;flex-wrap:wrap;gap:7px;margin:14px 0 4px}
.empty{color:var(--subtle);text-align:center;padding:26px;font-size:13px}
::-webkit-scrollbar{width:10px;height:10px}
::-webkit-scrollbar-thumb{background:var(--line);border-radius:8px;border:2px solid var(--bg)}
::-webkit-scrollbar-thumb:hover{background:var(--subtle)}
/* source tabs: OpenCode | Codex */
.source-tabs{display:flex;gap:8px;margin:22px 0 0;position:relative;z-index:2}
.stab{appearance:none;background:var(--panel);border:1px solid var(--line);border-radius:10px 10px 0 0;
  padding:9px 20px;font-family:var(--sans);font-size:13px;font-weight:700;color:var(--muted);cursor:pointer;transition:.15s}
.stab:hover{color:var(--text)}
.stab.active{color:var(--text);border-color:var(--accent);box-shadow:0 -2px 0 var(--accent) inset;background:var(--panel2)}
.stab .cnt{font-family:var(--mono);font-size:10.5px;font-weight:600;color:var(--subtle);margin-left:7px}
.stab.active .cnt{color:var(--accent)}
[role="tabpanel"][hidden]{display:none!important}
.rbanner{background:color-mix(in srgb,var(--cache) 10%,transparent);border:1px solid color-mix(in srgb,var(--cache) 35%,transparent);
  color:var(--text);border-radius:10px;padding:10px 14px;margin-top:18px;font-size:12.5px;display:flex;gap:10px;align-items:center;flex-wrap:wrap}
.rbanner b{font-weight:700}
.status-ok{color:var(--good);font-weight:700}
.status-err{color:var(--bad);font-weight:700}
.free-tag{font-size:9.5px;font-weight:800;letter-spacing:.06em;text-transform:uppercase;color:var(--accent2);
  border:1px solid color-mix(in srgb,var(--accent2) 40%,transparent);border-radius:5px;padding:1px 5px;margin-left:7px;vertical-align:1px}
/* token-first additions */
.summary{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-top:18px}
.sum-card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:12px 14px;box-shadow:var(--shadow)}
.sum-card .sl{font-size:10px;font-weight:700;letter-spacing:.09em;text-transform:uppercase;color:var(--subtle)}
.sum-card .sv{font-family:var(--mono);font-size:20px;font-weight:700;letter-spacing:-.02em;margin-top:4px;font-variant-numeric:tabular-nums}
.sum-card .ss{font-size:11px;color:var(--muted);margin-top:2px;font-variant-numeric:tabular-nums}
.records{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}
.rec{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:14px 16px;box-shadow:var(--shadow)}
.rec .rl{font-size:10px;font-weight:700;letter-spacing:.09em;text-transform:uppercase;color:var(--subtle)}
.rec .rv{font-family:var(--mono);font-size:18px;font-weight:700;margin-top:6px}
.rec .rs{font-size:12px;color:var(--muted);margin-top:4px;line-height:1.5}
.model-row{display:grid;grid-template-columns:minmax(140px,220px) 1fr auto;gap:12px;align-items:center;padding:10px 0;border-bottom:1px solid var(--line2)}
.model-row:last-child{border-bottom:none}
.model-row .mn{font-size:12.5px;font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.model-row .ms{font-size:11px;color:var(--subtle);font-family:var(--mono)}
.model-row .mbar{height:12px;background:var(--chip);border-radius:7px;overflow:hidden;display:flex}
.model-row .mbar i{height:100%;display:block}
.model-row .mv{font-family:var(--mono);font-size:12px;font-weight:700;text-align:right;white-space:nowrap}
.model-row.clickable{cursor:pointer;border-radius:8px}
.model-row.clickable:hover{background:color-mix(in srgb,var(--accent) 4%,transparent)}
.model-row.active{outline:2px solid var(--accent);outline-offset:-2px}
/* compact leaderboard rows (file/subsystem/commit panels) */
.crows .model-row{grid-template-columns:minmax(120px,180px) 1fr auto;gap:10px;padding:6px 0}
.crows .model-row .mn{font-size:12px}
.crows .model-row .ms{font-size:10.5px}
.crows .model-row .mbar{height:9px;border-radius:5px}
.crows .model-row .mv{font-size:11.5px}
.more-btn{display:block;width:100%;margin-top:8px;background:none;border:1px solid var(--line);border-radius:9px;
  padding:7px 10px;font-family:var(--sans);font-size:12px;font-weight:600;color:var(--muted);cursor:pointer}
.more-btn:hover{color:var(--text);border-color:var(--subtle)}
.hbar-row{display:flex;align-items:flex-end;gap:3px;height:90px;margin-top:6px}
.hbar{flex:1;background:linear-gradient(180deg,var(--accent),var(--accent2));border-radius:3px 3px 0 0;min-height:2px;cursor:pointer}
.hbar:hover{filter:brightness(1.15)}
.pbtn[aria-pressed="true"]{background:color-mix(in srgb,var(--accent) 14%,transparent);border-color:var(--accent);color:var(--accent)}
@media(max-width:700px){.model-row{grid-template-columns:1fr;gap:6px}.model-row .mv{text-align:left}}
</style>
</head>
<body>
<div class="topbar">
  <div class="topbar-in">
    <div class="brand"><span class="dot"></span>OpenCode<small>· usage</small></div>
    <div class="spacer"></div>
    <span id="status"></span>
    <button class="tbtn" id="theme">◐&nbsp; Light</button>
    <span class="export-wrap"><button class="tbtn" id="export">⤓ Export</button>
      <div class="expmenu" id="expmenu">
        <button data-x="json">JSON <small>full stats payload</small></button>
        <button data-x="csv">CSV <small>per-day usage table</small></button>
      </div></span>
    <button class="tbtn" id="refresh">⟳ Refresh</button>
  </div>
</div>

<div class="wrap">
  <div class="source-tabs" role="tablist" aria-label="Data source">
    <button class="stab active" id="tab-opencode" role="tab" aria-selected="true" aria-controls="view-opencode">OpenCode<span class="cnt" id="tab-opencode-cnt"></span></button>
    <button class="stab" id="tab-router" role="tab" aria-selected="false" aria-controls="view-router">Codex<span class="cnt" id="tab-router-cnt"></span></button>
  </div>
  <div id="view-opencode" role="tabpanel" aria-labelledby="tab-opencode">
  <section class="hero">
    <div class="eyebrow" id="range"></div>
    <h1 id="headline"></h1>
    <div class="pills" id="pills"></div>
    <section class="summary" id="summary" aria-label="Token summary"></section>
    <div class="presets" id="presets" role="group" aria-label="Time range">
      <span class="pl">Period</span>
      <button class="pbtn active" data-r="all">All time</button>
      <button class="pbtn" data-r="7">7 days</button>
      <button class="pbtn" data-r="30">30 days</button>
      <button class="pbtn" data-r="90">90 days</button>
      <button class="pbtn" data-r="365">1 year</button>
      <span class="pl" style="margin-left:14px">View</span>
      <button class="pbtn" id="split-btn" aria-pressed="false" title="Split Output into Output + Reasoning">Split reasoning: off</button>
      <button class="pbtn" id="share-btn" aria-pressed="false" title="Show 100% share instead of absolute tokens">Share: off</button>
    </div>
  </section>

  <section class="kpis" id="kpis"></section>

  <h2 class="sec">Records <span class="hint">personal bests · token-only</span></h2>
  <section class="records" id="records"></section>

  <h2 class="sec">Notes on your usage <span class="hint" id="notes-hint">generated from your local data</span></h2>
  <section class="insights" id="insights"></section>

  <h2 class="sec" id="sec-tokens">Tokens per day <span class="hint">input · output incl. reasoning · cache · click a day to filter</span></h2>
  <div class="card chart-card">
    <div class="ctitle">Tokens per day <small id="tok-sub">stacked absolute</small></div>
    <div id="tok-chart"></div>
    <div class="legend" id="tok-legend"></div>
  </div>
  <div class="grid2" style="margin-top:14px">
    <div class="card chart-card">
      <div class="ctitle">Tokens by file / subsystem <small id="file-sub">equal-split · click to filter</small></div>
      <div class="presets" style="margin:0 0 10px" role="group" aria-label="Files or subsystems">
        <button class="pbtn active" data-fs="file">Files</button>
        <button class="pbtn" data-fs="sub">Subsystems</button>
      </div>
      <div id="file-chart" class="crows"></div>
    </div>
    <div class="card chart-card">
      <div class="ctitle">Tokens per commit <small id="commit-sub">prior 24h sessions · click to filter</small></div>
      <div class="presets" style="margin:0 0 10px" role="group" aria-label="Commit order">
        <button class="pbtn active" data-cs="toks">By tokens</button>
        <button class="pbtn" data-cs="recent">Recent</button>
      </div>
      <div id="commit-chart" class="crows"></div>
    </div>
  </div>

  <h2 class="sec" id="sec-env">Tokens by model <span class="hint">universal comparison · click a model to filter</span></h2>
  <div class="card" id="model-card"><div id="models"></div><div class="legend" id="model-legend"></div></div>
  <div class="grid2" style="margin-top:14px">
    <div class="card"><div class="ctitle">Projects <small>token leaderboard</small></div><div class="chips" id="projects"></div></div>
    <div class="card"><div class="ctitle">Agents <small>sessions · tokens</small></div><div class="chips" id="agents"></div></div>
  </div>

  <h2 class="sec" id="sec-sessions">Sessions <span class="hint" id="sess-hint"></span></h2>
  <div class="card">
    <div class="tbl-tools">
      <input class="search" id="search" placeholder="Search sessions…" aria-label="Search sessions">
      <select class="filter" id="agent-f"><option value="">All agents</option></select>
      <select class="filter" id="model-f"><option value="">All models</option></select>
      <span class="badge" id="sess-count"></span>
    </div>
    <div id="fbar" class="fbar" style="display:none"></div>
    <div class="tbl-scroll">
      <table class="tbl" id="tbl">
        <thead><tr>
          <th data-k="title">Session</th><th>Model</th><th data-k="time_created">Started</th>
          <th class="num" data-k="tokens_input">Input</th><th class="num" data-k="tokens_output">Output</th>
          <th class="num" data-k="tokens_cache">Cache</th><th class="num" data-k="tokens">Total</th>
          <th class="num" data-k="msgs">Msgs</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <h2 class="sec" id="sec-heat">Activity <span class="hint">last 52 weeks · token intensity · click to filter</span></h2>
  <div class="card">
    <div class="act-head"><span class="sum" id="act-summary"></span></div>
    <div class="act-scroll"><div id="act"></div></div>
    <div class="act-legend" id="act-legend"></div>
  </div>
  </div><!-- /view-opencode -->
  <div id="view-router" role="tabpanel" aria-labelledby="tab-router" hidden>
  <section class="hero" style="padding-top:26px">
    <div class="eyebrow" id="r-range"></div>
    <h1 id="r-headline"></h1>
    <div class="pills" id="r-pills"></div>
    <section class="summary" id="r-summary" aria-label="Codex token summary"></section>
    <div class="presets" id="r-presets" role="group" aria-label="Codex time range">
      <span class="pl">Period</span>
      <button class="pbtn active" data-rr="all">All time</button>
      <button class="pbtn" data-rr="7">7 days</button>
      <button class="pbtn" data-rr="30">30 days</button>
      <button class="pbtn" data-rr="90">90 days</button>
      <button class="pbtn" data-rr="365">1 year</button>
      <span class="pl" style="margin-left:14px">View</span>
      <button class="pbtn" id="r-split-btn" aria-pressed="false" title="Split Output into Output + Reasoning">Split reasoning: off</button>
      <button class="pbtn" id="r-share-btn" aria-pressed="false" title="Show 100% share instead of absolute tokens">Share: off</button>
    </div>
  </section>
  <div class="rbanner" id="r-rate-banner" style="display:none"></div>
  <section class="kpis" id="r-kpis"></section>

  <h2 class="sec">Records <span class="hint">personal bests · token-only</span></h2>
  <section class="records" id="r-records"></section>

  <h2 class="sec">Notes on your usage <span class="hint" id="r-notes-hint">generated from your codex data</span></h2>
  <section class="insights" id="r-insights"></section>

  <h2 class="sec">Tokens per day <span class="hint">input · output · cache · all models</span></h2>
  <div class="card chart-card">
    <div class="ctitle">Tokens per day <small id="r-tok-sub">stacked absolute</small></div>
    <div id="r-tok-chart"></div>
    <div class="legend" id="r-tok-legend"></div>
  </div>
  <div class="grid2" style="margin-top:14px">
    <div class="card chart-card">
      <div class="ctitle">Tokens by file / subsystem <small id="r-file-sub">from local edit + write touches</small></div>
      <div class="presets" style="margin:0 0 10px" role="group" aria-label="Files or subsystems">
        <button class="pbtn active" data-rfs="file">Files</button>
        <button class="pbtn" data-rfs="sub">Subsystems</button>
      </div>
      <div id="r-file-chart" class="crows"></div>
    </div>
    <div class="card chart-card">
      <div class="ctitle">Tokens per commit <small id="r-commit-sub">prior 24h requests · time-matched</small></div>
      <div class="presets" style="margin:0 0 10px" role="group" aria-label="Commit order">
        <button class="pbtn active" data-rcs="toks">By tokens</button>
        <button class="pbtn" data-rcs="recent">Recent</button>
      </div>
      <div id="r-commit-chart" class="crows"></div>
    </div>
  </div>

  <h2 class="sec">Tokens by model <span class="hint">universal comparison · click a model to filter</span></h2>
  <div class="card" id="r-model-card"><div id="r-models"></div><div class="legend" id="r-model-legend"></div></div>
  <div class="grid2" style="margin-top:14px">
    <div class="card"><div class="ctitle">Providers <small>request leaderboard</small></div><div class="chips" id="r-providers"></div></div>
    <div class="card"><div class="ctitle">Status <small>ok · errors · click to filter</small></div><div class="chips" id="r-status"></div></div>
  </div>

  <h2 class="sec">Requests <span class="hint" id="r-req-hint"></span></h2>
  <div class="card">
    <div class="tbl-tools">
      <input class="search" id="r-search" placeholder="Filter by model or provider…" aria-label="Filter codex requests">
      <select class="filter" id="r-provider-f"><option value="">All providers</option></select>
      <select class="filter" id="r-model-f"><option value="">All models</option></select>
      <select class="filter" id="r-status-f"><option value="">All statuses</option><option value="ok">OK only</option><option value="err">Errors only</option><option value="429">429 only</option></select>
      <span class="badge" id="r-req-count"></span>
    </div>
    <div id="r-fbar" class="fbar" style="display:none"></div>
    <div class="tbl-scroll">
      <table class="tbl" id="r-tbl">
        <thead><tr>
          <th>Time</th><th>Model</th><th>Status</th>
          <th class="num">Input</th><th class="num">Output</th>
          <th class="num">Cache</th><th class="num">Total</th><th class="num">ms</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <h2 class="sec">Activity <span class="hint">last 52 weeks · token intensity · click to filter</span></h2>
  <div class="card">
    <div class="act-head"><span class="sum" id="r-act-summary"></span></div>
    <div class="act-scroll"><div id="r-act"></div></div>
    <div class="act-legend" id="r-act-legend"></div>
  </div>
  </div><!-- /view-router -->
</div>

<div class="overlay" id="overlay"><div class="modal" id="modal" role="dialog" aria-modal="true" aria-label="Session details"></div></div>
<div class="tip" id="tip"></div>
<div class="toasts" id="toasts"></div>

<script>const S0 = __DATA__; const R0 = __ROUTER__;</script>
<script>
const $=id=>document.getElementById(id);
const ESC=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const BOLD=s=>{const ents=[];const t=s.replace(/&(?:#\d+|[a-z]+);/g,m=>{ents.push(m);return'@@'+String.fromCharCode(65+ents.length-1)+'@@';});
  return t.replace(/(\d[\d,]*(?:\.\d+)?)/g,'<b>$1</b>').replace(/@@[A-Z]+@@/g,m=>ents[m.charCodeAt(2)-65]);};
const F=n=>{const s=Math.round(Number(n)||0).toString();return s.replace(/\B(?=(\d{3})+(?!\d))/g,',');};
const FN=n=>{n=Number(n)||0;return n>=1e6?(n/1e6).toFixed(2)+'M':n>=1e3?(n/1e3).toFixed(1)+'K':String(Math.round(n));};
const DT=t=>{if(!t)return'—';const d=new Date(t>1e12?t:t*1000);return d.toLocaleDateString('en-CA')+' '+String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0');};
const DS=t=>{if(!t)return'';const d=new Date(t>1e12?t:t*1000);return d.toLocaleDateString('en-CA');};
const DUR=m=>{if(m==null)return'—';if(m<1)return'<1m';if(m<60)return Math.round(m)+'m';const h=Math.floor(m/60);return h+'h '+Math.round(m%60)+'m';};
const BURN=b=>{b=Number(b)||0;return b>=1000?(b/1000).toFixed(1)+'K/m':Math.round(b)+'/m';};
let S=null; if(typeof S0!=='undefined'&&S0){S=S0;}
let R=null; try{if(typeof R0!=='undefined'&&R0){R=R0;}}catch(_){R=null;}
let TAB='opencode';
let sortK='time_created', sortAsc=false;
let RANGE='all', SPLIT=false, SHARE=false;
let RRANGE='all', RSPLIT=false, RSHARE=false;
let FS='file', CS='toks', RFS='file', RCS='toks';
let FS_EXP=false, CS_EXP=false, RFS_EXP=false, RCS_EXP=false;
let currentRows=[];
let currentRRows=[];
const RFSTATE={day:null,model:null,provider:null,status:''};
const WID=sessionStorage.getItem('ocd-wid')||('w'+Math.random().toString(36).slice(2));
sessionStorage.setItem('ocd-wid',WID);
const FSTATE={day:null,model:null,agent:null,file:null,sub:null,commit:null};
const tip=$('tip');
function tipShow(html,e){tip.innerHTML=html;tip.style.display='block';tipPos(e);}
function tipHide(){tip.style.display='none';}
function tipPos(e){
  const r=tip.getBoundingClientRect();
  let x=e.clientX+14,y=e.clientY+14;
  if(x+r.width>innerWidth-8)x=e.clientX-r.width-12;
  if(y+r.height>innerHeight-8)y=e.clientY-r.height-12;
  tip.style.left=x+'px';tip.style.top=y+'px';
}
function tipAt(el){
  const r=el.getBoundingClientRect();
  tip.style.display='block';
  const tw=tip.offsetWidth;
  let x=r.left+r.width/2-tw/2,y=r.bottom+8;
  if(y+tip.offsetHeight>innerHeight)y=r.top-tip.offsetHeight-8;
  tip.style.left=Math.max(8,Math.min(x,innerWidth-tw-8))+'px';tip.style.top=Math.max(8,y)+'px';
}
function toast(msg,ok){
  const t=document.createElement('div');
  t.className='toast'+(ok?' ok':ok===false?' err':'');
  t.textContent=msg;
  $('toasts').appendChild(t);
  setTimeout(()=>{t.style.opacity='0';t.style.transition='opacity .3s';setTimeout(()=>t.remove(),320);},3600);
}
/* theme */
const THEMES={light:{accent:'#4f46e5',accent2:'#0d9488',accent3:'#9333ea',reason:'#a855f7',cache:'#d97706'},
              dark:{accent:'#7c86ff',accent2:'#2dd4bf',accent3:'#c084fc',reason:'#d8b4fe',cache:'#fbbf24'}};
let theme=localStorage.getItem('ocd-theme')||(matchMedia('(prefers-color-scheme: dark)').matches?'dark':'light');
document.documentElement.dataset.theme=theme;
const T=()=>THEMES[theme];
function syncThemeBtn(){$('theme').textContent=theme==='dark'?'☀ Light':'◐ Dark';}
syncThemeBtn();
$('theme').onclick=()=>{theme=theme==='dark'?'light':'dark';document.documentElement.dataset.theme=theme;
  localStorage.setItem('ocd-theme',theme);syncThemeBtn();renderAll();renderRouter();};
/* svg */
const svg=(tag,at,html)=>'<'+tag+Object.entries(at).map(([k,v])=>' '+k+'="'+ESC(v)+'"').join('')+'>'+(html||'')+'</'+tag+'>';
const dayTotal=d=>(d.ti||0)+(d.to||0)+(d.tr||0)+(d.cache||0);
const outMerged=d=>(d.to||0)+(d.tr||0);
function dayTip(d){
  const tot=dayTotal(d);
  return '<b>'+d.date+'</b>'+
    '<span class="trow"><span>Input</span><b>'+FN(d.ti)+'</b></span>'+
    '<span class="trow"><span>Output incl. reasoning</span><b>'+FN(outMerged(d))+'</b></span>'+
    '<span class="trow"><span>Cache</span><b>'+FN(d.cache)+'</b></span>'+
    '<span class="trow"><span>Total</span><b>'+FN(tot)+'</b></span>'+
    '<span class="trow"><span>Sessions</span><b>'+d.sessions+'</b></span>'+
    '<span class="trow"><span>Messages</span><b>'+F(d.msgs)+'</b></span>';
}
function dayDim(d){return FSTATE.day&&FSTATE.day!==d.date?' dim':'';}
function stackChart(days){
  const t=T();
  const keys=SPLIT?[{p:'ti',c:t.accent,l:'Input'},{p:'to',c:t.accent2,l:'Output'},{p:'tr',c:t.reason,l:'Reasoning'},{p:'cache',c:t.cache,l:'Cache'}]
                   :[{p:'ti',c:t.accent,l:'Input'},{p:'out',c:t.accent2,l:'Output incl. reasoning'},{p:'cache',c:t.cache,l:'Cache'}];
  const val=(d,k)=>k.p==='out'?outMerged(d):(d[k.p]||0);
  const w=980,h=250,p=34;
  let max=1;
  if(SHARE){max=100;}
  else{max=Math.max(...days.map(d=>keys.reduce((a,k)=>a+val(d,k),0)),1);}
  const Y=v=>p+(1-v/max)*(h-2*p),X=i=>days.length>1?p+i*(w-2*p)/(days.length-1):p+(w-2*p)/2;
  const slot=(w-2*p)/Math.max(days.length,1);
  const sw=Math.min(slot*0.32,p*0.85);
  const step=Math.ceil(days.length/12)||1;
  let cells='',xl='';
  days.forEach((day,i)=>{
    const tot=keys.reduce((a,k)=>a+val(day,k),0);
    let acc=0;
    const x1=X(i)-sw,x2=X(i)+sw;
    keys.forEach(k=>{
      let v=val(day,k), y0,y1;
      if(SHARE){
        const pct=tot?v/tot*100:0;
        y1=Y(acc+((k===keys[0])?0:0)); // placeholder
        const share0=acc/tot*100||0;
        acc+=v;
        const share1=acc/tot*100||0;
        y0=Y(share1); y1=Y(share0);
        if(!v)return;
        const segTip=('<b>'+k.l+' · '+day.date+'</b><span class="trow"><span>Tokens</span><b>'+FN(v)+'</b></span>'+
          '<span class="trow"><span>Share</span><b>'+pct.toFixed(1)+'%</b></span>');
        cells+=svg('path',{d:'M'+x1.toFixed(1)+','+y0.toFixed(1)+' L'+x2.toFixed(1)+','+y0.toFixed(1)+' L'+x2.toFixed(1)+','+y1.toFixed(1)+' L'+x1.toFixed(1)+','+y1.toFixed(1)+' Z',
          fill:k.c,class:'inter'+dayDim(day),'data-tip':segTip,'data-f':'day|'+day.date});
      } else {
        if(!v)return;
        const segTip=('<b>'+k.l+' · '+day.date+'</b><span class="trow"><span>Tokens</span><b>'+FN(v)+'</b></span>'+
          '<span class="trow"><span>Share</span><b>'+(tot?Math.round(v/tot*100):0)+'%</b></span>');
        cells+=svg('path',{d:'M'+x1.toFixed(1)+','+Y(acc+v).toFixed(1)+' L'+x2.toFixed(1)+','+Y(acc+v).toFixed(1)+' L'+x2.toFixed(1)+','+Y(acc).toFixed(1)+' L'+x1.toFixed(1)+','+Y(acc).toFixed(1)+' Z',
          fill:k.c,class:'inter'+dayDim(day),'data-tip':segTip,'data-f':'day|'+day.date});
        acc+=v;
      }
    });
    cells+=svg('rect',{x:(X(i)-slot/2).toFixed(1),y:p,width:(slot*0.96).toFixed(1),height:(h-2*p).toFixed(1),fill:'transparent',
      class:'inter'+dayDim(day),'data-tip':dayTip(day),'data-f':'day|'+day.date});
    if(i%step===0)xl+=svg('text',{x:X(i).toFixed(1),y:h-p+17,'text-anchor':'middle',fill:'#9a968c','font-size':10},day.date.slice(5));
  });
  const sub=$('tok-sub');
  if(sub)sub.textContent=(SHARE?'100% share':'stacked absolute')+(SPLIT?' · reasoning split':' · output merged');
  return '<svg viewBox="0 0 '+w+' '+(h+10)+'" width="100%">'+cells+xl+'</svg>';
}
function shortPath(p){const seg=(p||'').split('/').filter(Boolean);return seg.length>2?'…/'+seg.slice(-2).join('/'):p;}
function fileRows(){
  // All-time backend aggregates, or a period recompute from recent sessions.
  if(RANGE==='all'&&!FSTATE.day)return {rows:(FS==='file'?(S.files||[]):(S.subsystems||[])),approx:false};
  const cut=RANGE==='all'?0:Date.now()-RANGE*86400000;
  const agg={};
  (S.sessions||[]).forEach(s=>{
    if(s.time_created<cut)return;
    if(FSTATE.day&&DS(s.time_created)!==FSTATE.day)return;
    const files=((S.session_files||{})[s.id])||[];
    if(!files.length)return;
    const share=(s.tokens_total||0)/files.length;
    files.forEach(fp=>{
      const key=FS==='file'?fp:(((S.file_subsys||{})[fp])||fp.split('/').filter(Boolean).slice(-2,-1)[0]||fp);
      const a=agg[key]||(agg[key]={toks:0,sess:0});
      a.toks+=share;a.sess+=1;
    });
  });
  const rows=Object.entries(agg).map(([k,a])=>[k,Math.round(a.toks*10)/10,a.sess,Math.round(a.toks/(a.sess||1))]);
  rows.sort((a,b)=>b[1]-a[1]);
  const approx=(S.totals&&S.totals.sessions||0)>(S.sessions||[]).length;
  return {rows:rows.slice(0,15),approx};
}
function renderFileChart(){
  const el=$('file-chart');if(!el)return;
  const {rows,approx}=fileRows();
  const sub=$('file-sub');
  if(sub)sub.textContent=(FS==='file'?'top files · equal-split':'top subsystems · top-level dir')+' · click to filter'+(approx?' · period from recent sessions':'');
  if(!rows.length){el.innerHTML='<div class="empty">No file touches yet.</div>';return;}
  const t=T(),totalAll=rows.reduce((a,r)=>a+(r[1]||0),0)||1;
  const max=Math.max(...rows.map(r=>r[1]||0),1);
  const shown=FS_EXP?rows:rows.slice(0,6);
  el.innerHTML=shown.map(r=>{
    const name=r[0],toks=r[1]||0,n=r[2]||0,avg=r[3]||0;
    const short=FS==='file'?shortPath(name):name;
    const key=FS==='file'?'file':'sub';
    const active=key==='file'?(FSTATE.file===name):(FSTATE.sub===name);
    const tip='<b>'+ESC(name)+'</b><span class=\'trow\'><span>Tokens</span><b>'+FN(toks)+' ('+(toks/totalAll*100).toFixed(1)+'% of top)</b></span>'+
      '<span class=\'trow\'><span>Sessions</span><b>'+n+'</b></span><span class=\'trow\'><span>Avg / session</span><b>'+FN(avg)+'</b></span>';
    return '<div class="model-row clickable'+(active?' active':'')+'" data-f="'+key+'|'+ESC(name)+'" data-tip="'+tip+'">'+
      '<div><div class="mn">'+ESC(short)+'</div><div class="ms">'+n+' sess · avg '+FN(avg)+'</div></div>'+
      '<div class="mbar"><i style="width:'+(toks/max*100).toFixed(1)+'%;background:'+(FS==='file'?t.accent:t.accent2)+'"></i></div>'+
      '<div class="mv">'+FN(toks)+'</div></div>';
  }).join('')+(rows.length>6?'<button class="more-btn" data-exp="fs">'+(FS_EXP?'Show less':'Show all '+rows.length)+' '+(FS_EXP?'▴':'▾')+'</button>':'');
}
function commitRows(){
  const cut=RANGE==='all'?0:Date.now()-RANGE*86400000;
  let rows=(S.commits||[]).filter(c=>{
    if(FSTATE.day&&c[2]!==FSTATE.day)return false;
    if(cut){
      const ms=Date.parse(c[2]+'T00:00:00');
      if(ms&&ms<cut-86400000)return false;
    }
    return true;
  });
  rows=rows.slice();
  if(CS==='recent')rows.sort((a,b)=>a[2]<b[2]?1:-1);
  else rows.sort((a,b)=>(b[7]||0)-(a[7]||0));
  return rows.slice(0,20);
}
function renderCommitChart(){
  const el=$('commit-chart');if(!el)return;
  const rows=commitRows();
  const sub=$('commit-sub');
  if(sub)sub.textContent='prior 24h sessions · click to filter';
  if(!rows.length){el.innerHTML='<div class="empty">No git commits found in tracked worktrees.</div>';return;}
  const t=T();
  const max=Math.max(...rows.map(r=>r[7]||0),1);
  const shown=CS_EXP?rows:rows.slice(0,6);
  el.innerHTML=shown.map(r=>{
    const sha=r[0],msg=r[1]||'(no message)',date=r[2],proj=r[3],nf=r[4]||0,add=r[5]||0,del=r[6]||0,toks=r[7]||0,ns=r[8]||0;
    const tip='<b>'+ESC(msg)+'</b><span class=\'trow\'><span>SHA</span><b>'+ESC(sha.slice(0,12))+'</b></span>'+
      '<span class=\'trow\'><span>Date · project</span><b>'+date+' · '+ESC(proj)+'</b></span>'+
      '<span class=\'trow\'><span>Changed</span><b>'+nf+' files · +'+F(add)+' / -'+F(del)+'</b></span>'+
      '<span class=\'trow\'><span>Tokens (prior 24h)</span><b>'+FN(toks)+' · '+ns+' sessions</b></span>';
    return '<div class="model-row clickable'+(FSTATE.commit===sha?' active':'')+'" data-f="commit|'+sha+'" data-tip="'+tip+'">'+
      '<div><div class="mn">'+ESC(msg.length>44?msg.slice(0,44)+'…':msg)+'</div>'+
      '<div class="ms">'+sha.slice(0,7)+' · '+date+' · '+ESC(proj)+' · +'+F(add)+'/-'+F(del)+'</div></div>'+
      '<div class="mbar"><i style="width:'+Math.max(toks?2:0,toks/max*100).toFixed(1)+'%;background:'+t.accent2+'"></i></div>'+
      '<div class="mv">'+(toks?FN(toks):'—')+'</div></div>';
  }).join('')+(rows.length>6?'<button class="more-btn" data-exp="cs">'+(CS_EXP?'Show less':'Show all '+rows.length)+' '+(CS_EXP?'▴':'▾')+'</button>':'');
}
/* period helpers */
function cutDate(){
  if(RANGE==='all')return'';
  const d=new Date(Date.now()-RANGE*86400000);
  return d.toLocaleDateString('en-CA');
}
function inPeriod(day){const c=cutDate();return !c||day.date>=c;}
function periodDays(){return S.days.filter(inPeriod);}
function periodPrev(){
  const c=cutDate();if(!c)return[];
  const end=new Date();end.setDate(end.getDate()-RANGE);
  const start=new Date(end);start.setDate(start.getDate()-RANGE);
  const s=start.toLocaleDateString('en-CA'),e=end.toLocaleDateString('en-CA');
  return S.days.filter(d=>d.date>=s&&d.date<e);
}
function sumDays(list){
  const t={sessions:0,msgs:0,ti:0,to:0,tr:0,cache:0,total:0};
  list.forEach(d=>{t.sessions+=d.sessions;t.msgs+=d.msgs;t.ti+=d.ti;t.to+=d.to;t.tr+=d.tr;t.cache+=d.cache;t.total+=dayTotal(d);});
  return t;
}
function deltaPct(cur,prev){if(prev==null)return null;if(prev===0)return cur>0?100:0;return Math.round((cur-prev)/prev*100);}
function deltaTag(cur,prev){const p=deltaPct(cur,prev);if(p==null)return'';
  return p===0?'<span class="delta flat">±0%</span>':p>0?'<span class="delta up">▲ '+p+'%</span>':'<span class="delta down">▼ '+Math.abs(p)+'%</span>';}
/* renderers */
function renderAll(){
  if(!S)return;
  renderHero();renderSummary();renderKpis();renderRecords();renderInsights();renderCharts();renderModels();renderProjAgents();renderAct();renderFilterBar();renderSessions();
}
function renderHero(){
  const periodLbl=RANGE==='all'?'ALL TIME':(RANGE/30>=1?RANGE/30+' MO':RANGE+' DAYS');
  $('range').textContent='LOCAL · RECORDED '+S.range.toUpperCase()+' · LAST '+periodLbl+' · REFRESHES AUTOMATICALLY';
  $('headline').innerHTML=BOLD(ESC(S.insights[0]||'No activity yet.'));
  const last=[...S.sessions].sort((a,b)=>b.time_created-a.time_created)[0];
  const ltitle=last?ESC(last.title):'—';
  const nModels=S.models.length;
  $('pills').innerHTML=[
    ['Total tokens',FN(S.totals.tokens_total||0)],
    ['Models',nModels>1?nModels+' models':ESC(((S.models[0]||['—'])[0]+'').split('/').pop())],
    ['Latest',ltitle.length>36?ltitle.slice(0,36)+'…':ltitle]]
    .map(([k,v])=>'<span class="pill">'+ESC(k)+' <b style="color:var(--text)">'+v+'</b></span>').join('');
}
function periodTotal(n){
  const now=new Date();now.setHours(0,0,0,0);
  const start=new Date(now);start.setDate(start.getDate()-(n-1));
  const s=start.toLocaleDateString('en-CA');
  const list=S.days.filter(d=>d.date>=s);
  return sumDays(list);
}
function renderSummary(){
  const el=$('summary');if(!el)return;
  const t7=periodTotal(7),t30=periodTotal(30),t1=periodTotal(1);
  const prev7=(()=>{const end=new Date();end.setDate(end.getDate()-7);const st=new Date(end);st.setDate(st.getDate()-7);
    return sumDays(S.days.filter(d=>d.date>=st.toLocaleDateString('en-CA')&&d.date<end.toLocaleDateString('en-CA')));})();
  const cards=[
    ['Today',t1.total,'','tokens'],
    ['Last 7 days',t7.total,deltaTag(t7.total,prev7.total),'tokens'],
    ['Last 30 days',t30.total,'','tokens'],
    ['All time',S.totals.tokens_total||0,'','tokens'],
  ];
  el.innerHTML=cards.map(([l,v,tag])=>'<div class="sum-card"><div class="sl">'+l+'</div><div class="sv">'+FN(v)+' '+tag+'</div><div class="ss">tokens</div></div>').join('');
}
function renderKpis(){
  const t=S.totals,c=T();
  const cur=RANGE==='all'?null:sumDays(periodDays());
  const prev=RANGE==='all'?null:sumDays(periodPrev());
  const pv=prev||{sessions:0,msgs:0,ti:0,to:0,tr:0,cache:0,total:0};
  const tok=cur?{ti:cur.ti,out:cur.to+cur.tr,tr:cur.tr,ca:cur.cache}:{ti:t.tokens_input,out:t.tokens_output+t.tokens_reasoning,tr:t.tokens_reasoning,ca:t.tokens_cache_read};
  const total=tok.ti+tok.out+tok.ca;
  const pct=v=>total?Math.round(v/total*100):0;
  const row=(lbl,v,color,sub)=>'<div class="krow"><span class="lbl"><i class="dot" style="background:'+color+'"></i>'+lbl+
    (sub?' <span class="dim">'+sub+'</span>':'')+'</span><b>'+v+'</b></div>';
  const sessions=cur?cur.sessions:t.sessions;
  const msgs=cur?cur.msgs:t.messages;
  const tokTag=cur?deltaTag(total,pv.total):'';
  const actTag=cur?deltaTag(sessions+msgs,pv.sessions+pv.msgs):'';
  const hitRate=(()=>{const d=(tok.ti||0)+(tok.ca||0);return d?Math.round(tok.ca/d*100):0;})();
  const tokCard='<div class="kpi"><h3>Tokens '+(SPLIT?'(reasoning split)':'(output merged)')+'</h3><div class="big">'+FN(total)+' <small>total</small>'+tokTag+'</div>'+
    '<div class="tokbar">'+[[tok.ti,c.accent],[tok.out-(SPLIT?tok.tr:0),c.accent2]].concat(SPLIT?[[tok.tr,c.reason]]:[]).concat([[tok.ca,c.cache]]).map(([v,k])=>'<i style="width:'+pct(v)+'%;background:'+k+'"></i>').join('')+'</div>'+
    row('Input',FN(tok.ti),c.accent,pct(tok.ti)+'%')+row('Output incl. reasoning',FN(tok.out),c.accent2,pct(tok.out)+'%')+
    (SPLIT?row('&nbsp;&nbsp;↳ reasoning',FN(tok.tr),c.reason,pct(tok.tr)+'%'):'')+
    row('Cache',FN(tok.ca),c.cache,pct(tok.ca)+'%')+'</div>';
  const avgSess=sessions?Math.round(total/sessions):0;
  const tpm=msgs?Math.round(total/msgs):0;
  const effCard='<div class="kpi"><h3>Cache &amp; efficiency</h3><div class="big">'+hitRate+'% <small>cache hit</small></div>'+
    row('Cached tokens',FN(tok.ca),c.cache)+row('Avg / session',FN(avgSess),c.accent)+row('Tokens / message',FN(tpm),c.accent2)+
    row('Avg tokens/session (all)',FN(t.avg_tokens_per_session||0),'var(--subtle)')+'</div>';
  const topSub=((S.subsystems||[])[0])||null;
  const burnCard='<div class="kpi"><h3>Burn &amp; activity</h3><div class="big">'+F(sessions)+' <small>sessions</small>'+actTag+'</div>'+
    row('Messages',F(msgs),c.accent)+row('Top subsystem',topSub?ESC(topSub[0]):'—',c.accent2,'all-time')+
    row('Avg session',DUR(t.avg_duration_min),'var(--subtle)','all-time')+'</div>';
  $('kpis').innerHTML=tokCard+effCard+burnCard;
}
function humanize(n){n=Number(n)||0;const words=Math.round(n/1.3);if(words>1e6)return (words/1e6).toFixed(1)+'M words';if(words>1e3)return (words/1e3).toFixed(0)+'K words';return words+' words';}
function renderRecords(){
  const el=$('records');if(!el||!S.records)return;
  const r=S.records;
  const cards=[];
  if(r.biggest_day)cards.push(['Biggest day',FN(r.biggest_day.total),r.biggest_day.date+' · '+r.biggest_day.sessions+' sessions · '+humanize(r.biggest_day.total)]);
  if(r.biggest_session)cards.push(['Biggest session',FN(r.biggest_session.total),(r.biggest_session.title||'Untitled').slice(0,48)+' · '+((r.biggest_session.model||'')+'').split('/').pop()]);
  if(r.fastest_burn)cards.push(['Fastest burn',BURN(r.fastest_burn.burn),(r.fastest_burn.title||'').slice(0,48)]);
  if(r.streaks)cards.push(['Streaks',r.streaks.current+'d now', 'longest '+r.streaks.longest+'d · '+humanize(S.totals.tokens_total)+' total ≈ '+(Math.round((S.totals.tokens_total||0)/750000*10)/10)+' novels']);
  el.innerHTML=cards.map(([l,v,s])=>'<div class="rec"><div class="rl">'+l+'</div><div class="rv">'+ESC(v)+'</div><div class="rs">'+ESC(s)+'</div></div>').join('')||'<div class="empty">No records yet.</div>';
}
function renderInsights(){
  $('notes-hint').textContent=RANGE==='all'?'generated from your local data':'all-time notes · charts/tables follow the period filter';
  $('insights').innerHTML=S.insights.slice(1,5).map(ins=>'<div class="in"><p>'+BOLD(ESC(ins))+'</p></div>').join('');
}
function renderCharts(){
  const d=periodDays();
  if(!d.length){$('tok-chart').innerHTML='<div class="empty">Nothing in this period.</div>';$('file-chart').innerHTML='';$('commit-chart').innerHTML='';return;}
  $('tok-chart').innerHTML=stackChart(d);
  const t=T();
  const keys=SPLIT?[{l:'Input',c:t.accent},{l:'Output',c:t.accent2},{l:'Reasoning',c:t.reason},{l:'Cache',c:t.cache}]:[{l:'Input',c:t.accent},{l:'Output incl. reasoning',c:t.accent2},{l:'Cache',c:t.cache}];
  $('tok-legend').innerHTML=keys.map(k=>'<span><i style="background:'+k.c+'"></i>'+k.l+'</span>').join('')+(SHARE?'<span>100% share mode</span>':'');
  renderFileChart();
  renderCommitChart();
}
function modelShort(m){return (m||'—').split('/').pop();}
function renderModels(){
  const el=$('models');if(!el)return;
  const t=T();
  const max=Math.max(...S.models.map(m=>m[2]||0),1);
  const totalAll=S.models.reduce((a,m)=>a+(m[2]||0),0)||1;
  if(!S.models.length){el.innerHTML='<div class="empty">No models yet.</div>';return;}
  el.innerHTML=S.models.map(m=>{
    const name=m[0],n=m[1],tot=m[2]||0,ti=m[3]||0,to=m[4]||0,tr=m[5]||0,ca=m[6]||0,avg=m[8]||0,hr=m[9]||0;
    const out=to+tr;
    const segs=SPLIT?[[ti,t.accent],[to,t.accent2],[tr,t.reason],[ca,t.cache]]:[[ti,t.accent],[out,t.accent2],[ca,t.cache]];
    const bar=segs.map(([v,c])=>'<i style="width:'+(tot?v/tot*100:0)+'%;background:'+c+'"></i>').join('');
    const tipHtml=('<b>'+ESC(name)+'</b><span class=\'trow\'><span>Sessions</span><b>'+n+'</b></span>'+
      '<span class=\'trow\'><span>Total tokens</span><b>'+FN(tot)+' ('+(tot/totalAll*100).toFixed(1)+'%)</b></span>'+
      '<span class=\'trow\'><span>Input / Output / Cache</span><b>'+FN(ti)+' / '+FN(out)+' / '+FN(ca)+'</b></span>'+
      (SPLIT?'<span class=\'trow\'><span>Reasoning</span><b>'+FN(tr)+'</b></span>':'')+
      '<span class=\'trow\'><span>Avg / session</span><b>'+FN(avg)+'</b></span>'+
      '<span class=\'trow\'><span>Cache hit</span><b>'+hr+'%</b></span>');
    return '<div class="model-row clickable'+(FSTATE.model===name?' active':'')+'" data-f="model|'+ESC(name)+'" data-tip="'+tipHtml+'">'+
      '<div><div class="mn">'+ESC(modelShort(name))+'</div><div class="ms">'+n+' sess · '+(tot/totalAll*100).toFixed(1)+'% · avg '+FN(avg)+'</div></div>'+
      '<div class="mbar">'+bar+'</div>'+
      '<div class="mv">'+FN(tot)+'</div></div>';
  }).join('');
  $('model-legend').innerHTML='<span><i style="background:'+t.accent+'"></i>Input</span><span><i style="background:'+t.accent2+'"></i>Output incl. reasoning</span>'+(SPLIT?'<span><i style="background:'+t.reason+'"></i>Reasoning</span>':'')+'<span><i style="background:'+t.cache+'"></i>Cache</span>';
}
function renderProjAgents(){
  $('projects').innerHTML='<span style="font-size:11px;text-transform:uppercase;letter-spacing:.1em;color:var(--subtle);font-weight:700;align-self:center">Projects</span>'+
    S.projects.slice(0,8).map(p=>'<span class="chip" data-tip="<b>'+ESC(p[0])+'</b><span class=\'trow\'><span>Sessions</span><b>'+p[1]+'</b></span><span class=\'trow\'><span>Tokens</span><b>'+FN(p[2]||0)+'</b></span>">'+
      '<b>'+FN(p[2]||p[1])+'</b><span class="n">'+ESC(p[0])+' · '+p[1]+'</span></span>').join('')||'<span class="empty">No projects</span>';
  $('agents').innerHTML=S.agents.map(a=>'<span class="chip'+(FSTATE.agent===a[0]?' active':'')+'" data-f="agent|'+ESC(a[0])+'" data-tip="'+
    ('<b>'+ESC(a[0])+' agent</b><span class=\'trow\'><span>Sessions</span><b>'+a[1]+'</b></span>'+
     '<span class=\'trow\'><span>Tokens</span><b>'+FN(a[2])+'</b></span>')+'">'+
    '<i style="width:9px;height:9px;border-radius:3px;background:'+[T().accent,T().accent2,T().accent3][S.agents.indexOf(a)%3]+';display:inline-block"></i>'+
    ESC(a[0])+' · '+a[1]+' · '+FN(a[2])+'</span>').join('');
}
function renderAct(){
  const act=S.activity||[];
  const $w=$('act');
  const MONTHS=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  if(!act.length){$w.innerHTML='<div class="empty">Nothing yet.</div>';$('act-summary').textContent='';$('act-legend').innerHTML='';return;}
  const today=new Date().toLocaleDateString('en-CA');
  const cols=[];let cur=null;
  act.forEach(d=>{
    const dt=new Date(d.date+'T00:00:00');
    const dow=(dt.getDay()+6)%7;
    if(dow===0||!cur){cur={days:new Array(7).fill(null),month:dt.getMonth()};cols.push(cur);}
    cur.days[dow]=d;
  });
  const mx=Math.max(...act.map(d=>dayTotal(d)),1);
  const lvl=v=>{if(!v)return 0;const t=v/mx;return t>=0.75?4:t>=0.5?3:t>=0.3?2:1;};
  const pct=[0,18,34,52,70,86];
  const cell=(d)=>{
    const tot=d?dayTotal(d):0;
    const lv=lvl(tot);
    const bg=lv?'color-mix(in srgb,'+T().accent+' '+pct[lv]+'%, var(--bg))':'var(--chip)';
    const tipH=d?('<b>'+d.date+'</b><span class="trow"><span>Total tokens</span><b>'+FN(tot)+'</b></span>'+
      '<span class="trow"><span>Input</span><b>'+FN(d.ti)+'</b></span>'+
      '<span class="trow"><span>Output incl. reasoning</span><b>'+FN(outMerged(d))+'</b></span>'+
      '<span class="trow"><span>Cache</span><b>'+FN(d.cache)+'</b></span>'+
      '<span class="trow"><span>Sessions</span><b>'+d.sessions+'</b></span>'):'';
    const f=d?' data-date="'+d.date+'" data-tip=\''+tipH+'\' data-f="day|'+d.date+'"':'';
    return '<button class="act-cell'+(d&&d.date===today?' today':'')+(d&&FSTATE.day&&FSTATE.day!==d.date?' dim':'')+'" '+
      'style="background:'+bg+'" aria-label="'+(d?d.date+', '+FN(tot)+' tokens':'No activity')+'"'+f+'></button>';
  };
  let monthHtml='',i=0;
  while(i<cols.length){const m=cols[i].month;let j=i;while(j<cols.length&&cols[j].month===m)j++;
    monthHtml+='<span style="width:'+((j-i)*(12+3)-3)+'px">'+MONTHS[m]+'</span>';i=j;}
  const labels=['Mon','Wed','Fri'].map(d=>'<span>'+d+'</span>').join('');
  $w.innerHTML='<div class="act-grid"><div class="act-months">'+monthHtml+'</div>'+
    '<div class="act-body"><div class="act-labels">'+labels+'</div>'+
    '<div class="act-cols">'+cols.map(c=>'<div class="act-col">'+c.days.map(cell).join('')+'</div>').join('')+'</div></div></div>';
  const active=act.filter(d=>dayTotal(d)>0).length;
  const tot=act.reduce((a,d)=>({tokens:a.tokens+dayTotal(d),sessions:a.sessions+d.sessions,msgs:a.msgs+d.msgs}),{tokens:0,sessions:0,msgs:0});
  $('act-summary').innerHTML='<b>'+active+'</b> active days · <b>'+FN(tot.tokens)+'</b> tokens · '+
    '<b>'+F(tot.sessions)+'</b> sessions in the last 52 weeks';
  $('act-legend').innerHTML='Less <span class="sw e"></span>'+
    [1,2,3,4].map(l=>'<span class="sw" style="background:color-mix(in srgb,'+T().accent+' '+pct[l]+'%, var(--bg))"></span>').join('')+' More <span style="margin-left:8px">token intensity</span>';
}
/* sessions */
function sessionRows(){
  const q=($('search').value||'').toLowerCase();
  const cut=RANGE==='all'?0:Date.now()-RANGE*86400000;
  return S.sessions.filter(s=>{
    if(s.time_created<cut)return false;
    if(FSTATE.agent&&s.agent!==FSTATE.agent)return false;
    if(FSTATE.model&&s.model!==FSTATE.model)return false;
    if(FSTATE.day&&DS(s.time_created)!==FSTATE.day)return false;
    if(FSTATE.file&&!(((S.session_files||{})[s.id])||[]).includes(FSTATE.file))return false;
    if(FSTATE.sub){
      const files=((S.session_files||{})[s.id])||[];
      const subs=files.map(fp=>((S.file_subsys||{})[fp])||'');
      if(!subs.includes(FSTATE.sub))return false;
    }
    if(FSTATE.commit&&!(((S.commit_sessions||{})[FSTATE.commit])||[]).includes(s.id))return false;
    if(q&&!(s.title||'').toLowerCase().includes(q)&&!(s.worktree||'').toLowerCase().includes(q))return false;
    return true;});
}
function renderSessions(){
  const rows=sessionRows();
  const mf=$('model-f');
  const curSel=mf.value;
  mf.innerHTML='<option value="">All models</option>'+S.models.map(m=>'<option value="'+ESC(m[0])+'">'+ESC(modelShort(m[0]))+'</option>').join('');
  mf.value=FSTATE.model||curSel||'';
  const af=$('agent-f');
  const curA=af.value;
  af.innerHTML='<option value="">All agents</option>'+S.agents.map(a=>'<option value="'+ESC(a[0])+'">'+ESC(a[0])+'</option>').join('');
  af.value=FSTATE.agent||curA||'';
  rows.sort((a,b)=>{let x=a[sortK],y=b[sortK];
    if(sortK==='tokens'){x=a.tokens_total||0;y=b.tokens_total||0;}
    return(x<y?-1:x>y?1:0)*(sortAsc?1:-1);});
  currentRows=rows;
  const fcount=Object.values(FSTATE).filter(Boolean).length;
  $('sess-count').textContent=F(rows.length)+' of '+F(S.sessions.length)+' shown';
  $('sess-hint').textContent=(fcount?'filtered by '+fcount+' criteria · ':'')+'click a row for token details';
  const latestTime=[...S.sessions].sort((a,b)=>b.time_created-a.time_created)[0]?.time_created;
  const dayLbl=t=>{const d=new Date(t>1e12?t:t*1000);
    const s=new Date(d),n=new Date();s.setHours(0,0,0,0);n.setHours(0,0,0,0);
    const diff=Math.round((n-s)/86400000);
    const hh=String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0');
    return diff===0?'Today '+hh:diff===1?'Yesterday '+hh:(d.getMonth()+1)+'/'+d.getDate()+' '+hh;};
  const tb=$('tbl').querySelector('tbody');
  tb.innerHTML=rows.map((s,i)=>{
    const tot=s.tokens_total||((s.tokens_input||0)+(s.tokens_output||0)+(s.tokens_reasoning||0)+(s.tokens_cache||0));
    const out=(s.tokens_output||0)+(s.tokens_reasoning||0);
    const isLatest=s.time_created===latestTime;
    return'<tr data-i="'+i+'" tabindex="0" role="link" aria-label="Open session '+ESC(s.title)+'"'+(isLatest?' class="latest"':'')+'><td><div class="t">'+ESC(s.title)+'</div>'+
    '<div class="badges"><span class="badge">'+ESC(s.worktree)+'</span>'+(isLatest?'<span class="badge latest">Latest</span>':'')+'</div></td>'
    +'<td><div class="badges mix"><span class="badge agent">'+ESC(s.agent)+'</span><span class="badge model">'+ESC(modelShort(s.model))+'</span></div></td>'
    +'<td class="mono" style="font-size:11.5px;color:var(--muted)">'+dayLbl(s.time_created)+'</td>'
    +'<td class="num">'+FN(s.tokens_input||0)+'</td><td class="num">'+FN(out)+'</td><td class="num">'+FN(s.tokens_cache||0)+'</td>'
    +'<td class="num" style="font-weight:700">'+FN(tot)+'</td><td class="num" style="color:var(--subtle)">'+F(s.msgs)+'</td></tr>';}).join('')
    ||'<tr><td colspan="8"><div class="empty">No sessions match.</div></td></tr>';
}
function renderFilterBar(){
  const bar=$('fbar');
  const active=Object.entries(FSTATE).filter(([k,v])=>v);
  if(!active.length){bar.style.display='none';return;}
  bar.style.display='flex';
  const labels={day:'Day',model:'Model',agent:'Agent',file:'File',sub:'Subsystem',commit:'Commit'};
  const disp=(k,v)=>{
    if(k==='model')return modelShort(v);
    if(k==='file')return shortPath(v);
    if(k==='commit')return String(v).slice(0,7);
    return v;
  };
  bar.innerHTML='<span class="fl">Filtering:</span>'+active.map(([k,v])=>
    '<span class="fchip" data-k="'+k+'">'+labels[k]+': '+ESC(disp(k,v))+' <span class="fx">✕</span></span>').join('')
    +'<button class="fclear">clear all</button>';
  bar.querySelectorAll('.fchip').forEach(ch=>ch.onclick=()=>{FSTATE[ch.dataset.k]=null;renderAll();});
  bar.querySelector('.fclear').onclick=()=>{for(const k in FSTATE)FSTATE[k]=null;$('agent-f').value='';$('model-f').value='';renderAll();};
}
/* ---- Codex tab (all models, from usage-events.jsonl) ---- */
function rModelShort(m){return (m||'—').split('/').pop();}
function rDayTokenTotal(d){return (d.ti||0)+(d.to||0)+(d.tr||0);}
function rCutDate(){
  if(RRANGE==='all')return'';
  const d=new Date(Date.now()-RRANGE*86400000);
  return d.toLocaleDateString('en-CA');
}
function rInPeriod(day){const c=rCutDate();return !c||day.date>=c;}
function rPeriodDays(){return (R&&R.days?R.days:[]).filter(rInPeriod);}
function rSumDays(list){
  const t={reqs:0,ok:0,err:0,err429:0,err500:0,ti:0,to:0,tr:0,cache:0,total:0,what_if:0};
  list.forEach(d=>{t.reqs+=d.reqs||0;t.ok+=d.ok||0;t.err+=d.err||0;t.err429+=d.err429||0;t.err500+=d.err500||0;t.ti+=d.ti||0;t.to+=d.to||0;t.tr+=d.tr||0;t.cache+=d.cache||0;t.total+=rDayTokenTotal(d);t.what_if+=d.what_if||0;});
  return t;
}
function rDayTip(d){
  return '<b>'+d.date+'</b>'+
    '<span class="trow"><span>Requests</span><b>'+F(d.reqs||0)+' ('+F(d.ok||0)+' ok)</b></span>'+
    '<span class="trow"><span>Input total</span><b>'+FN(d.ti)+'</b></span>'+
    '<span class="trow"><span>↳ cached input</span><b>'+FN(d.cache||0)+'</b></span>'+
    '<span class="trow"><span>↳ uncached input</span><b>'+FN(Math.max(0,(d.ti||0)-(d.cache||0)))+'</b></span>'+
    '<span class="trow"><span>Output incl. reasoning</span><b>'+FN((d.to||0)+(d.tr||0))+'</b></span>'+
    (RSPLIT?'<span class="trow"><span>Reasoning</span><b>'+FN(d.tr||0)+'</b></span>':'')+
    '<span class="trow"><span>Total</span><b>'+FN(rDayTokenTotal(d))+'</b></span>';
}
function rStackChart(days){
  const t=T();
  const keys=RSPLIT?[{p:'uncached',c:t.accent,l:'Uncached input'},{p:'cache',c:t.cache,l:'Cached input'},{p:'to',c:t.accent2,l:'Output'},{p:'tr',c:t.reason,l:'Reasoning'}]
                    :[{p:'uncached',c:t.accent,l:'Uncached input'},{p:'cache',c:t.cache,l:'Cached input'},{p:'out',c:t.accent2,l:'Output incl. reasoning'}];
  const val=(d,k)=>k.p==='uncached'?Math.max(0,(d.ti||0)-(d.cache||0)):k.p==='out'?(d.to||0)+(d.tr||0):(d[k.p]||0);
  const w=980,h=250,p=34;
  let max=1;
  if(RSHARE){max=100;}
  else{max=Math.max(...days.map(d=>keys.reduce((a,k)=>a+val(d,k),0)),1);}
  const Y=v=>p+(1-v/max)*(h-2*p),X=i=>days.length>1?p+i*(w-2*p)/(days.length-1):p+(w-2*p)/2;
  const slot=(w-2*p)/Math.max(days.length,1);
  const sw=Math.min(slot*0.32,p*0.85);
  const step=Math.ceil(days.length/12)||1;
  let cells='',xl='';
  days.forEach((day,i)=>{
    const tot=keys.reduce((a,k)=>a+val(day,k),0);
    let acc=0;
    const x1=X(i)-sw,x2=X(i)+sw;
    keys.forEach(k=>{
      const v=val(day,k);
      if(RSHARE){
        const share0=acc/(tot||1)*100||0;
        acc+=v;
        const share1=acc/(tot||1)*100||0;
        const y0=Y(share1),y1=Y(share0);
        if(!v)return;
        cells+=svg('path',{d:'M'+x1.toFixed(1)+','+y0.toFixed(1)+' L'+x2.toFixed(1)+','+y0.toFixed(1)+' L'+x2.toFixed(1)+','+y1.toFixed(1)+' L'+x1.toFixed(1)+','+y1.toFixed(1)+' Z',
          fill:k.c,class:'inter','data-tip':'<b>'+k.l+' · '+day.date+'</b><span class="trow"><span>Tokens</span><b>'+FN(v)+'</b></span>'});
      }else{
        if(!v)return;
        cells+=svg('path',{d:'M'+x1.toFixed(1)+','+Y(acc+v).toFixed(1)+' L'+x2.toFixed(1)+','+Y(acc+v).toFixed(1)+' L'+x2.toFixed(1)+','+Y(acc).toFixed(1)+' L'+x1.toFixed(1)+','+Y(acc).toFixed(1)+' Z',
          fill:k.c,class:'inter','data-tip':'<b>'+k.l+' · '+day.date+'</b><span class="trow"><span>Tokens</span><b>'+FN(v)+'</b></span>'});
        acc+=v;
      }
    });
    cells+=svg('rect',{x:(X(i)-slot/2).toFixed(1),y:p,width:(slot*0.96).toFixed(1),height:(h-2*p).toFixed(1),fill:'transparent',
      class:'inter','data-tip':rDayTip(day)});
    if(i%step===0)xl+=svg('text',{x:X(i).toFixed(1),y:h-p+17,'text-anchor':'middle',fill:'#9a968c','font-size':10},day.date.slice(5));
  });
  const sub=$('r-tok-sub');
  if(sub)sub.textContent=(RSHARE?'100% share':'stacked absolute')+(RSPLIT?' · reasoning split':' · output merged')+' · all Codex models';
  return '<svg viewBox="0 0 '+w+' '+(h+10)+'" width="100%">'+cells+xl+'</svg>';
}
function rFileRows(){
  // Mirrored from the local edit/write aggregates: the router event stream
  // carries no file info, so this panel is display-only context.
  return (RFS==='file'?((S&&S.files)||[]):((S&&S.subsystems)||[]))||[];
}
function renderRouterFileChart(){
  const el=$('r-file-chart');if(!el)return;
  const rows=rFileRows();
  const sub=$('r-file-sub');
  if(sub)sub.textContent=(RFS==='file'?'top files · equal-split':'top subsystems')+' · from local edit + write touches';
  if(!S||!rows.length){el.innerHTML='<div class="empty">No local file data to mirror.</div>';return;}
  const t=T();
  const totalAll=rows.reduce((a,r)=>a+(r[1]||0),0)||1;
  const max=Math.max(...rows.map(r=>r[1]||0),1);
  const shown=RFS_EXP?rows:rows.slice(0,6);
  el.innerHTML=shown.map(r=>{
    const name=r[0],toks=r[1]||0,n=r[2]||0,avg=r[3]||0;
    const short=RFS==='file'?shortPath(name):name;
    const tip='<b>'+ESC(name)+'</b><span class=\'trow\'><span>Tokens</span><b>'+FN(toks)+'</b></span>'+
      '<span class=\'trow\'><span>Sessions</span><b>'+n+'</b></span><span class=\'trow\'><span>Avg / session</span><b>'+FN(avg)+'</b></span>'+
      '<span class=\'trow\'><span>Source</span><b>local touches</b></span>';
    return '<div class="model-row" data-tip="'+tip+'">'+
      '<div><div class="mn">'+ESC(short)+'</div><div class="ms">'+n+' sess · avg '+FN(avg)+'</div></div>'+
      '<div class="mbar"><i style="width:'+(toks/max*100).toFixed(1)+'%;background:'+(RFS==='file'?t.accent:t.accent2)+'"></i></div>'+
      '<div class="mv">'+FN(toks)+'</div></div>';
  }).join('')+(rows.length>6?'<button class="more-btn" data-exp="rfs">'+(RFS_EXP?'Show less':'Show all '+rows.length)+' '+(RFS_EXP?'▴':'▾')+'</button>':'');
}
function rCommitRows(){
  const cut=RRANGE==='all'?0:Date.now()-RRANGE*86400000;
  let rows=((R&&R.commits)||[]).filter(c=>{
    if(RFSTATE.day&&c[2]!==RFSTATE.day)return false;
    if(cut){
      const ms=Date.parse(c[2]+'T00:00:00');
      if(ms&&ms<cut-86400000)return false;
    }
    return true;
  });
  rows=rows.slice();
  if(RCS==='recent')rows.sort((a,b)=>a[2]<b[2]?1:-1);
  else rows.sort((a,b)=>(b[7]||0)-(a[7]||0));
  return rows.slice(0,20);
}
function renderRouterCommitChart(){
  const el=$('r-commit-chart');if(!el)return;
  const rows=rCommitRows();
  const sub=$('r-commit-sub');
  if(sub)sub.textContent='prior 24h requests · time-matched';
  if(!rows.length){el.innerHTML='<div class="empty">No git commits found in tracked worktrees.</div>';return;}
  const t=T();
  const max=Math.max(...rows.map(r=>r[7]||0),1);
  const shown=RCS_EXP?rows:rows.slice(0,6);
  el.innerHTML=shown.map(r=>{
    const sha=r[0],msg=r[1]||'(no message)',date=r[2],proj=r[3],nf=r[4]||0,add=r[5]||0,del=r[6]||0,toks=r[7]||0,nq=r[8]||0;
    const tip='<b>'+ESC(msg)+'</b><span class=\'trow\'><span>SHA</span><b>'+ESC(sha.slice(0,12))+'</b></span>'+
      '<span class=\'trow\'><span>Date · project</span><b>'+date+' · '+ESC(proj)+'</b></span>'+
      '<span class=\'trow\'><span>Changed</span><b>'+nf+' files · +'+F(add)+' / -'+F(del)+'</b></span>'+
      '<span class=\'trow\'><span>Tokens (prior 24h)</span><b>'+FN(toks)+' · '+nq+' requests</b></span>';
    return '<div class="model-row" data-tip="'+tip+'">'+
      '<div><div class="mn">'+ESC(msg.length>44?msg.slice(0,44)+'…':msg)+'</div>'+
      '<div class="ms">'+sha.slice(0,7)+' · '+date+' · '+ESC(proj)+' · +'+F(add)+'/-'+F(del)+'</div></div>'+
      '<div class="mbar"><i style="width:'+Math.max(toks?2:0,toks/max*100).toFixed(1)+'%;background:'+t.accent2+'"></i></div>'+
      '<div class="mv">'+(toks?FN(toks):'—')+'</div></div>';
  }).join('')+(rows.length>6?'<button class="more-btn" data-exp="rcs">'+(RCS_EXP?'Show less':'Show all '+rows.length)+' '+(RCS_EXP?'▴':'▾')+'</button>':'');
}
function rHumanize(n){n=Number(n)||0;const words=Math.round(n/1.3);if(words>1e6)return (words/1e6).toFixed(1)+'M words';if(words>1e3)return (words/1e3).toFixed(0)+'K words';return words+' words';}
function renderRouterRecords(R,totTokens){
  const el=$('r-records');if(!el||!R.records)return;
  const r=R.records;
  const cards=[];
  if(r.biggest_day)cards.push(['Biggest day',FN(r.biggest_day.total),r.biggest_day.date+' · '+r.biggest_day.sessions+' requests · '+rHumanize(r.biggest_day.total)]);
  if(r.biggest_model)cards.push(['Biggest model',FN(r.biggest_model.total),rModelShort(r.biggest_model.name)+' · '+F(r.biggest_model.requests)+' requests']);
  if(r.biggest_request)cards.push(['Biggest request',FN(r.biggest_request.total),rModelShort(r.biggest_request.model)+' · '+String(r.biggest_request.at||'').replace('T',' ').slice(0,19)]);
  if(r.streaks)cards.push(['Streaks',r.streaks.current+'d now','longest '+r.streaks.longest+'d · '+rHumanize(totTokens)+' total']);
  el.innerHTML=cards.map(([l,v,s])=>'<div class="rec"><div class="rl">'+l+'</div><div class="rv">'+ESC(v)+'</div><div class="rs">'+ESC(s)+'</div></div>').join('')||'<div class="empty">No records yet.</div>';
}
function renderRouterStatus(pc){
  const t=(R&&R.totals)||{};
  const v=k=>pc?pc[k]:(t[k==='ok'?'ok':k==='err'?'errors':k]||0);
  const items=[
    ['OK',v('ok'),T().accent2,'ok'],
    ['Errors',v('err'),(v('err')?'var(--bad)':'var(--subtle)'),'err'],
    ['429 rate-limited',v('err429'),(v('err429')?'var(--bad)':'var(--subtle)'),'429'],
    ['5xx upstream',v('err500'),'var(--bad)','err'],
  ];
  $('r-status').innerHTML=items.map(([l,v,cc,k])=>'<span class="chip'+(RFSTATE.status===k?' active':'')+'" data-rs="'+k+'" data-tip="<b>'+l+'</b><span class=\'trow\'><span>Requests</span><b>'+F(v)+'</b></span>">'+
    '<i style="width:9px;height:9px;border-radius:3px;background:'+cc+';display:inline-block"></i>'+l+' · '+F(v)+'</span>').join('');
}
function rActDayTotal(dd){return rDayTokenTotal(dd);}
function renderRouterAct(){
  const act=(R&&R.activity)||[];
  const $w=$('r-act');
  const MONTHS=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  if(!act.length){$w.innerHTML='<div class="empty">Nothing yet.</div>';$('r-act-summary').textContent='';$('r-act-legend').innerHTML='';return;}
  const today=new Date().toLocaleDateString('en-CA');
  const cols=[];let cur=null;
  act.forEach(dd=>{
    const dt=new Date(dd.date+'T00:00:00');
    const dow=(dt.getDay()+6)%7;
    if(dow===0||!cur){cur={days:new Array(7).fill(null),month:dt.getMonth()};cols.push(cur);}
    cur.days[dow]=dd;
  });
  const mx=Math.max(...act.map(dd=>rActDayTotal(dd)),1);
  const lvl=v=>{if(!v)return 0;const tt=v/mx;return tt>=0.75?4:tt>=0.5?3:tt>=0.3?2:1;};
  const pc=[0,18,34,52,70,86];
  const cell=(dd)=>{
    const tot=dd?rActDayTotal(dd):0;
    const lv=lvl(tot);
    const bg=lv?'color-mix(in srgb,'+T().accent+' '+pc[lv]+'%, var(--bg))':'var(--chip)';
    const tipH=dd?('<b>'+dd.date+'</b><span class="trow"><span>Total tokens</span><b>'+FN(tot)+'</b></span>'+
      '<span class="trow"><span>Requests</span><b>'+F(dd.reqs||dd.msgs||0)+'</b></span>'):'';
    const f=dd?' data-tip=\''+tipH+'\' data-rday="'+dd.date+'"':'';
    return '<button class="act-cell'+(dd&&dd.date===today?' today':'')+(dd&&RFSTATE.day&&RFSTATE.day!==dd.date?' dim':'')+'" '+
      'style="background:'+bg+'" aria-label="'+(dd?dd.date+', '+FN(tot)+' tokens':'No activity')+'"'+f+'></button>';
  };
  let monthHtml='',i=0;
  while(i<cols.length){const m=cols[i].month;let j=i;while(j<cols.length&&cols[j].month===m)j++;
    monthHtml+='<span style="width:'+((j-i)*(12+3)-3)+'px">'+MONTHS[m]+'</span>';i=j;}
  const labels=['Mon','Wed','Fri'].map(dd=>'<span>'+dd+'</span>').join('');
  $w.innerHTML='<div class="act-grid"><div class="act-months">'+monthHtml+'</div>'+
    '<div class="act-body"><div class="act-labels">'+labels+'</div>'+
    '<div class="act-cols">'+cols.map(cc=>'<div class="act-col">'+cc.days.map(cell).join('')+'</div>').join('')+'</div></div></div>';
  const active=act.filter(dd=>rActDayTotal(dd)>0).length;
  const tot=act.reduce((a,dd)=>({tokens:a.tokens+rActDayTotal(dd),reqs:a.reqs+(dd.reqs||dd.msgs||0)}),{tokens:0,reqs:0});
  $('r-act-summary').innerHTML='<b>'+active+'</b> active days · <b>'+FN(tot.tokens)+'</b> tokens · '+
    '<b>'+F(tot.reqs)+'</b> requests in the last 52 weeks';
  $('r-act-legend').innerHTML='Less <span class="sw e"></span>'+
    [1,2,3,4].map(l=>'<span class="sw" style="background:color-mix(in srgb,'+T().accent+' '+pc[l]+'%, var(--bg))"></span>').join('')+' More <span style="margin-left:8px">token intensity</span>';
}
function renderRouterFilterBar(){
  const bar=$('r-fbar');
  const active=Object.entries(RFSTATE).filter(([k,v])=>v);
  if(!active.length){bar.style.display='none';return;}
  bar.style.display='flex';
  const labels={day:'Day',model:'Model',provider:'Provider',status:'Status'};
  const disp=(k,v)=>k==='model'?rModelShort(v):v;
  bar.innerHTML='<span class="fl">Filtering:</span>'+active.map(([k,v])=>
    '<span class="fchip" data-rk="'+k+'">'+labels[k]+': '+ESC(disp(k,v))+' <span class="fx">✕</span></span>').join('')
    +'<button class="fclear">clear all</button>';
  bar.querySelectorAll('.fchip').forEach(ch=>ch.onclick=()=>{
    const k=ch.dataset.rk;RFSTATE[k]=null;
    if(k==='status')$('r-status-f').value='';
    if(k==='provider')$('r-provider-f').value='';
    if(k==='model')$('r-model-f').value='';
    renderRouter();
  });
  bar.querySelector('.fclear').onclick=()=>{for(const k in RFSTATE)RFSTATE[k]=null;$('r-status-f').value='';$('r-provider-f').value='';$('r-model-f').value='';renderRouter();};
}
function renderRouter(){
  if(!R)return;
  const t=R.totals||{};
  const periodLbl=RRANGE==='all'?'ALL TIME':(RRANGE/30>=1?RRANGE/30+' MO':RRANGE+' DAYS');
  $('r-range').textContent='CODEX · '+(R.range||'').toUpperCase()+' · LAST '+periodLbl+' · REFRESHES AUTOMATICALLY';
  const cur=RRANGE==='all'?null:rSumDays(rPeriodDays());
  const totTokens=cur?cur.total:(t.tokens_total||0);
  const totReqs=cur?cur.reqs:(t.requests||0);
  $('r-headline').innerHTML=BOLD(ESC(totReqs+' requests · '+Math.round(totTokens).toLocaleString('en-US')+' tokens with Codex.'));
  const nModels=(R.models||[]).length;
  $('r-pills').innerHTML=[
    ['Requests',F(totReqs)],
    ['Tokens',FN(totTokens)],
    ['Models',F(nModels)],
    ['What-if paid','$'+Number(cur?cur.what_if:(t.what_if_cost||0)).toFixed(2)]]
    .map(([k,v])=>'<span class="pill">'+ESC(k)+' <b style="color:var(--text)">'+v+'</b></span>').join('');
  const t7=(()=>{const now=new Date();now.setHours(0,0,0,0);const s=new Date(now);s.setDate(s.getDate()-6);
    return rSumDays((R.days||[]).filter(d=>d.date>=s.toLocaleDateString('en-CA')));})();
  const t30=(()=>{const now=new Date();now.setHours(0,0,0,0);const s=new Date(now);s.setDate(s.getDate()-29);
    return rSumDays((R.days||[]).filter(d=>d.date>=s.toLocaleDateString('en-CA')));})();
  const t1=(()=>{const s=new Date().toLocaleDateString('en-CA');
    return rSumDays((R.days||[]).filter(d=>d.date>=s));})();
  $('r-summary').innerHTML=[['Today',t1.total],['Last 7 days',t7.total],['Last 30 days',t30.total],['All time',t.tokens_total||0]]
    .map(([l,v])=>'<div class="sum-card"><div class="sl">'+l+'</div><div class="sv">'+FN(v)+'</div><div class="ss">tokens</div></div>').join('');
  const rb=$('r-rate-banner');
  const lims=R.rate_limits||{};
  const limKeys=Object.keys(lims);
  const be=cur?cur.err429:(t.err429||0);
  const bw=cur?cur.what_if:(t.what_if_cost||0);
  if((t.err429||0)>0||limKeys.length){
    rb.style.display='flex';
    rb.innerHTML='<span>⚠ <b>'+F(be)+'</b> Codex requests hit <b>429</b> (usually free-model capacity)'+(cur?' in this period':'')+
      (limKeys.length?' · rate-limits: <span class="mono" style="font-size:11px">'+ESC(limKeys.slice(0,4).join(', '))+(limKeys.length>4?'…':'')+'</span>':'')+
      ' · actual free cost <b>$0</b> · what-if <b>$'+Number(bw||0).toFixed(2)+'</b>'+(cur?' (period)':'')+'</span>';
  }else{rb.style.display='none';rb.innerHTML='';}
  const c=T();
  const tok=cur?{input:cur.ti,out:cur.to+cur.tr,tr:cur.tr,ca:cur.cache}:{input:t.tokens_input,out:t.tokens_output+t.tokens_reasoning,tr:t.tokens_reasoning,ca:t.tokens_cache_read};
  const uncached=Math.max(0,tok.input-tok.ca);
  const total=tok.input+tok.out;
  const pct=v=>total?Math.round(v/total*100):0;
  const row=(lbl,v,color,sub)=>'<div class="krow"><span class="lbl"><i class="dot" style="background:'+color+'"></i>'+lbl+
    (sub?' <span class="dim">'+sub+'</span>':'')+'</span><b>'+v+'</b></div>';
  const okRate=totReqs?Math.round(((cur?cur.ok:t.ok)||0)/totReqs*100):0;
  const hitRate=tok.input?Math.round(tok.ca/tok.input*100):0;
  const avgReq=totReqs?Math.round(total/totReqs):0;
  const topCommit=((R.commits||[])[0])||null;
  $('r-kpis').innerHTML=
    '<div class="kpi"><h3>Tokens '+(RSPLIT?'(reasoning split)':'(output merged)')+'</h3><div class="big">'+FN(total)+' <small>total</small></div>'+
    '<div class="tokbar">'+[[uncached,c.accent],[tok.ca,c.cache],[tok.out-(RSPLIT?tok.tr:0),c.accent2]].concat(RSPLIT?[[tok.tr,c.reason]]:[]).map(([v,k])=>'<i style="width:'+pct(v)+'%;background:'+k+'"></i>').join('')+'</div>'+
    row('Input total',FN(tok.input),c.accent,pct(tok.input)+'%')+row('&nbsp;&nbsp;↳ cached input',FN(tok.ca),c.cache,hitRate+'% of input')+
    row('&nbsp;&nbsp;↳ uncached input',FN(uncached),c.accent,(100-hitRate)+'% of input')+row('Output incl. reasoning',FN(tok.out),c.accent2,pct(tok.out)+'%')+
    (RSPLIT?row('&nbsp;&nbsp;↳ reasoning',FN(tok.tr),c.reason,pct(tok.tr)+'%'):'')+'</div>'+
    '<div class="kpi"><h3>Cache &amp; efficiency</h3><div class="big">'+hitRate+'% <small>cache hit</small></div>'+
    row('Cached tokens',FN(tok.ca),c.cache)+row('Avg / request',FN(avgReq),c.accent)+
    row('What-if paid','$'+Number(cur?cur.what_if:(t.what_if_cost||0)).toFixed(2)+(cur?' (period)':''),c.accent2)+
    row('Avg tokens/request (all)',FN(t.avg_tokens_per_session||0),'var(--subtle)')+'</div>'+
    '<div class="kpi"><h3>Burn &amp; activity</h3><div class="big">'+F(totReqs)+' <small>requests · '+okRate+'% ok</small></div>'+
    row('Errors',F(cur?cur.err:(t.errors||0)),(cur?cur.err:t.errors)?'var(--bad)':'var(--subtle)')+
    row('429s',F(cur?cur.err429:(t.err429||0)),(cur?cur.err429:(t.err429||0))?'var(--bad)':'var(--subtle)')+
    row('Top commit',topCommit?ESC((topCommit[1]||'').slice(0,30)):'—',c.accent2,'all-time')+
    row('Avg latency',F(t.avg_ms||0)+' ms','var(--subtle)','ok-only · all-time')+'</div>';
  renderRouterRecords(R,totTokens);
  const d=rPeriodDays();
  $('r-tok-chart').innerHTML=d.length?rStackChart(d):'<div class="empty">Nothing in this period.</div>';
  $('r-tok-legend').innerHTML='<span><i style="background:'+c.accent+'"></i>Uncached input</span><span><i style="background:'+c.cache+'"></i>Cached input</span><span><i style="background:'+c.accent2+'"></i>'+(RSPLIT?'Output':'Output incl. reasoning')+'</span>'+(RSPLIT?'<span><i style="background:'+c.reason+'"></i>Reasoning</span>':'')+(RSHARE?'<span>100% share mode</span>':'');
  renderRouterFileChart();
  renderRouterCommitChart();
  $('r-providers').innerHTML=(R.providers||[]).map(p=>'<span class="chip'+(RFSTATE.provider===p[0]?' active':'')+'" data-rp="'+ESC(p[0])+'" data-tip="<b>'+ESC(p[0])+'</b><span class=\'trow\'><span>Requests</span><b>'+p[1]+'</b></span><span class=\'trow\'><span>Tokens</span><b>'+FN(p[2]||0)+'</b></span>">'+
    '<b>'+FN(p[2]||p[1])+'</b><span class="n">'+ESC(p[0])+' · '+p[1]+'</span></span>').join('')||'<span class="empty">No providers</span>';
  renderRouterStatus(cur);
  const totalAll=(R.models||[]).reduce((a,m)=>a+(m[2]||0),0)||1;
  $('r-models').innerHTML=(R.models||[]).length?(R.models||[]).map(m=>{
    const name=m[0],reqs=m[1],tot=m[2]||0,ti=m[3]||0,to=m[4]||0,ca=m[5]||0,ok=m[6]||0,err=m[7]||0;
    const prov=m[10]||'',free=m[11],wi=m[12]||0;
    const tr=0,out=to+tr,uncached=Math.max(0,ti-ca);
    const segs=RSPLIT?[[uncached,c.accent],[ca,c.cache],[to,c.accent2],[tr,c.reason]]:[[uncached,c.accent],[ca,c.cache],[out,c.accent2]];
    const bar=segs.map(([v,k])=>'<i style="width:'+(tot?v/tot*100:0)+'%;background:'+k+'"></i>').join('');
    const tipH=('<b>'+ESC(name)+'</b><span class=\'trow\'><span>Requests</span><b>'+reqs+' ('+ok+' ok'+(err?', '+err+' err':'')+')</b></span>'+
      '<span class=\'trow\'><span>Total tokens</span><b>'+FN(tot)+' ('+(tot/totalAll*100).toFixed(1)+'%)</b></span>'+
      '<span class=\'trow\'><span>Input / Cached / Output</span><b>'+FN(ti)+' / '+FN(ca)+' / '+FN(out)+'</b></span>'+
      (RSPLIT?'<span class=\'trow\'><span>Reasoning</span><b>'+FN(tr)+'</b></span>':'')+
      '<span class=\'trow\'><span>Provider</span><b>'+ESC(prov)+'</b></span>'+
      ((!tot&&ok)?'<span class=\'trow\'><span>Note</span><b>unmetered — 0 tokens reported</b></span>':'')+
      ((free&&!wi)?'<span class=\'trow\'><span>Pricing</span><b>no rate on file — excluded from what-if</b></span>':'')+
      (free?'<span class=\'trow\'><span>What-if paid</span><b>$'+Number(wi).toFixed(4)+'</b></span>':''));
    return '<div class="model-row clickable'+(RFSTATE.model===name?' active':'')+'" data-rm="'+ESC(name)+'" data-tip="'+tipH+'">'+
      '<div><div class="mn">'+ESC(rModelShort(name))+(free?'<span class="free-tag">free</span>':'')+'</div><div class="ms">'+reqs+' req · '+(tot/totalAll*100).toFixed(1)+'% · '+ESC(prov)+'</div></div>'+
      '<div class="mbar">'+bar+'</div>'+
      '<div class="mv">'+FN(tot)+'</div></div>';
  }).join(''):'<div class="empty">No router models yet.</div>';
  $('r-model-legend').innerHTML='<span><i style="background:'+c.accent+'"></i>Uncached input</span><span><i style="background:'+c.cache+'"></i>Cached input</span><span><i style="background:'+c.accent2+'"></i>Output incl. reasoning</span>'+(RSPLIT?'<span><i style="background:'+c.reason+'"></i>Reasoning</span>':'')+'<span>click a model to filter requests</span>';
  $('r-insights').innerHTML=(R.insights||[]).map(ins=>'<div class="in"><p>'+BOLD(ESC(ins))+'</p></div>').join('');
  $('r-notes-hint').textContent=RRANGE==='all'?'generated from your codex data':'all-time notes · charts/tables follow the period filter';
  const pf=$('r-provider-f'),mf=$('r-model-f');
  pf.innerHTML='<option value="">All providers</option>'+(R.providers||[]).map(p=>'<option value="'+ESC(p[0])+'">'+ESC(p[0])+'</option>').join('');
  mf.innerHTML='<option value="">All models</option>'+(R.models||[]).map(m=>'<option value="'+ESC(m[0])+'">'+ESC(rModelShort(m[0]))+'</option>').join('');
  pf.value=RFSTATE.provider||'';mf.value=RFSTATE.model||'';
  renderRouterFilterBar();
  renderRouterRequests();
  renderRouterAct();
  const oc=(S&&S.totals?S.totals.tokens_total:0)||0;
  $('tab-opencode-cnt').textContent=FN(oc);
  $('tab-router-cnt').textContent=F(t.requests||0)+' req';
}
function routerRequestRows(){
  const q=(($('r-search')||{}).value||'').toLowerCase();
  const st=($('r-status-f')||{}).value||'';
  const cut=RRANGE==='all'?0:Date.now()-RRANGE*86400000;
  return (R.requests||[]).filter(r=>{
    if(RFSTATE.model&&r.model!==RFSTATE.model)return false;
    if(RFSTATE.provider&&r.provider!==RFSTATE.provider)return false;
    if(RFSTATE.day){
      const ms0=Date.parse(r.at||'');
      if(!ms0||new Date(ms0).toLocaleDateString('en-CA')!==RFSTATE.day)return false;
    }
    if(st==='ok'&&!r.ok)return false;
    if(st==='err'&&r.ok)return false;
    if(st==='429'&&r.status!==429)return false;
    if(cut){
      const ms=Date.parse(r.at||'');
      if(ms&&ms<cut)return false;
    }
    if(q&&!(r.model||'').toLowerCase().includes(q)&&!(r.provider||'').toLowerCase().includes(q))return false;
    return true;});
}
function renderRouterRequests(){
  const rows=routerRequestRows();
  currentRRows=rows;
  $('r-req-count').textContent=F(rows.length)+' of '+F((R.requests||[]).length)+' shown';
  const rf=Object.values(RFSTATE).filter(Boolean).length;
  const rt=(R&&R.totals)||{};
  $('r-req-hint').textContent=(rf?'filtered by '+rf+' criteria · ':'')+'newest '+F(rows.length)+' of '+F(rt.requests||0)+' counted'+
    (R&&R.truncated?' (file capped at recent '+F(R.scanned||0)+')':'')+
    ' · '+F(rt.unmetered||0)+' unmetered excluded · errors show 0 tokens (actuals only)';
  const tb=$('r-tbl').querySelector('tbody');
  tb.innerHTML=rows.map(r=>{
    const ok=r.ok;
    return '<tr><td class="mono" style="font-size:11.5px;color:var(--muted)">'+ESC((r.at||'').replace('T',' ').slice(0,19))+'</td>'+
    '<td><div class="t" style="font-size:12.5px">'+ESC(rModelShort(r.model))+'</div><div class="badges"><span class="badge">'+ESC(r.provider)+'</span></div></td>'+
    '<td><span class="'+(ok?'status-ok':'status-err')+' mono" style="font-size:12px">'+r.status+'</span></td>'+
    '<td class="num">'+FN(r.ti||0)+'</td><td class="num">'+FN(r.to||0)+'</td><td class="num">'+FN(r.cache||0)+'</td>'+
    '<td class="num" style="font-weight:700">'+FN(r.total||0)+'</td><td class="num" style="color:var(--subtle)">'+F(r.ms||0)+'</td></tr>';}).join('')
    ||'<tr><td colspan="8"><div class="empty">No requests match.</div></td></tr>';
}
function setTab(name){
  TAB=name;
  const oc=name==='opencode';
  $('tab-opencode').classList.toggle('active',oc);
  $('tab-router').classList.toggle('active',!oc);
  $('tab-opencode').setAttribute('aria-selected',oc);
  $('tab-router').setAttribute('aria-selected',!oc);
  $('view-opencode').hidden=!oc;
  $('view-router').hidden=oc;
  try{sessionStorage.setItem('ocd-tab',name);}catch(_){}
  if(!oc)renderRouter();else renderAll();
}
function sortH(e){const th=e.target.closest('th');if(!th)return;const k=th.dataset.k;if(!k)return;
  if(k===sortK)sortAsc=!sortAsc;else{sortK=k;sortAsc=false;}
  document.querySelectorAll('#tbl th').forEach(t=>{
    t.classList.remove('asc','desc');
    t.setAttribute('aria-sort',t===th?(sortAsc?'ascending':'descending'):'none');});
  th.classList.add(sortAsc?'asc':'desc');renderSessions();}
function openSession(s){
  const tot=s.tokens_total||((s.tokens_input||0)+(s.tokens_output||0)+(s.tokens_reasoning||0)+(s.tokens_cache||0));
  const out=(s.tokens_output||0)+(s.tokens_reasoning||0);
  const t=T();
  const rows=[['Input',s.tokens_input||0,t.accent],['Output',s.tokens_output||0,t.accent2],['Reasoning',s.tokens_reasoning||0,t.reason],['Cache',s.tokens_cache||0,t.cache]];
  const bar=rows.filter(([l,v])=>v>0).map(([l,v,c])=>'<i style="width:'+(tot?v/tot*100:0)+'%;background:'+c+'" title="'+l+'"></i>').join('');
  const brow=rows.map(([l,v,c])=>'<div class="krow"><span class="lbl"><i class="dot" style="background:'+c+'"></i>'+l+'</span><b>'+FN(v)+' · '+(tot?Math.round(v/tot*100):0)+'%</b></div>').join('');
  lastFocus=document.activeElement;
  $('modal').innerHTML='<button class="modal-x" aria-label="Close">✕</button>'
  +'<h2 style="font-size:18px;font-weight:700;letter-spacing:-.02em">'+ESC(s.title)+'</h2>'
  +'<div class="mmeta">'+['<span class="badge agent">'+ESC(s.agent)+'</span>','<span class="badge model">'+ESC(s.model)+'</span>',
    '<span class="badge">'+DT(s.time_created)+'</span>','<span class="badge">'+DUR(s.duration_min)+'</span>',
    '<span class="badge">'+F(s.msgs)+' msgs</span>','<span class="badge">'+FN(tot)+' tokens</span>','<span class="badge">'+BURN(s.burn)+' burn</span>'].join('')+'</div>'
  +'<div class="card" style="margin-top:16px"><div style="font-size:11px;text-transform:uppercase;letter-spacing:.1em;color:var(--subtle);font-weight:700;margin-bottom:12px">Token breakdown</div>'
  +'<div class="tokbar" style="height:12px">'+bar+'</div>'+brow+'</div>'
  +'<div class="card" style="margin-top:12px"><div style="font-size:11px;text-transform:uppercase;letter-spacing:.1em;color:var(--subtle);font-weight:700;margin-bottom:12px">Prompts</div>'
  +'<div id="modal-prompts"><div class="empty">Loading prompts…</div></div></div>';
  $('overlay').classList.add('open');
  const x=$('modal').querySelector('.modal-x');if(x)x.focus();
  fetch('/api/session/'+encodeURIComponent(s.id)).then(r=>r.json()).then(pj=>{
    const el=$('modal-prompts');if(!el)return;
    if(pj&&pj.error){el.innerHTML='<div class="empty">'+ESC(pj.error)+'</div>';return;}
    const list=(pj&&pj.prompts)||[];
    el.innerHTML=list.length?list.slice().reverse().map(p=>'<div class="prompt"><span class="mono" style="font-size:10.5px;color:var(--subtle)">'+DT(p.t)+'</span><br>'+ESC(p.p)+'</div>').join(''):'<div class="empty">No prompts recorded.</div>';
  }).catch(()=>{const el=$('modal-prompts');if(el)el.innerHTML='<div class="empty">Could not load prompts.</div>';});
}
/* interactions */
let lastFocus=null;
document.addEventListener('mousemove',e=>{
  const el=e.target.closest?e.target.closest('[data-tip]'):null;
  if(el&&el.dataset.tip){tipShow(el.dataset.tip,e);}
  else if(tip.style.display==='block'){tipHide();}
});
document.addEventListener('focusin',e=>{
  const el=e.target.closest?e.target.closest('[data-tip]'):null;
  if(el&&el.dataset.tip){tip.innerHTML=el.dataset.tip;tipAt(el);}
});
document.addEventListener('focusout',()=>tipHide());
document.addEventListener('click',e=>{
  if(!e.target.closest('.export-wrap'))$('expmenu').classList.remove('open');
  const el=e.target.closest?e.target.closest('[data-f]'):null;
  if(!el)return;
  const idx=el.dataset.f.indexOf('|');
  const k=el.dataset.f.slice(0,idx),v=el.dataset.f.slice(idx+1);
  if(k==='day'&&!v)return;
  FSTATE[k]=FSTATE[k]===v?null:v;
  if(k==='agent'&&FSTATE.agent!==null)$('agent-f').value=FSTATE.agent;
  if(k==='model'&&FSTATE.model!==null)$('model-f').value=FSTATE.model;
  renderAll();
});
$('agent-f').addEventListener('change',e=>{FSTATE.agent=e.target.value||null;renderAll();});
$('model-f').addEventListener('change',e=>{FSTATE.model=e.target.value||null;renderAll();});
document.querySelectorAll('.pbtn[data-fs]').forEach(b=>b.onclick=()=>{
  FS=b.dataset.fs;FS_EXP=false;
  document.querySelectorAll('.pbtn[data-fs]').forEach(x=>x.classList.toggle('active',x===b));
  renderFileChart();
});
document.querySelectorAll('.pbtn[data-cs]').forEach(b=>b.onclick=()=>{
  CS=b.dataset.cs;CS_EXP=false;
  document.querySelectorAll('.pbtn[data-cs]').forEach(x=>x.classList.toggle('active',x===b));
  renderCommitChart();
});
document.querySelectorAll('.pbtn[data-rfs]').forEach(b=>b.onclick=()=>{
  RFS=b.dataset.rfs;RFS_EXP=false;
  document.querySelectorAll('.pbtn[data-rfs]').forEach(x=>x.classList.toggle('active',x===b));
  renderRouterFileChart();
});
document.querySelectorAll('.pbtn[data-rcs]').forEach(b=>b.onclick=()=>{
  RCS=b.dataset.rcs;RCS_EXP=false;
  document.querySelectorAll('.pbtn[data-rcs]').forEach(x=>x.classList.toggle('active',x===b));
  renderRouterCommitChart();
});
document.addEventListener('click',e=>{
  const b=e.target.closest?e.target.closest('[data-exp]'):null;
  if(!b)return;
  const k=b.dataset.exp;
  if(k==='fs'){FS_EXP=!FS_EXP;renderFileChart();}
  else if(k==='cs'){CS_EXP=!CS_EXP;renderCommitChart();}
  else if(k==='rfs'){RFS_EXP=!RFS_EXP;renderRouterFileChart();}
  else if(k==='rcs'){RCS_EXP=!RCS_EXP;renderRouterCommitChart();}
});
document.querySelectorAll('.pbtn[data-r]').forEach(b=>b.onclick=()=>{
  RANGE=b.dataset.r;
  document.querySelectorAll('.pbtn[data-r]').forEach(x=>x.classList.toggle('active',x===b));
  renderAll();
});
$('split-btn').onclick=()=>{SPLIT=!SPLIT;$('split-btn').textContent='Split reasoning: '+(SPLIT?'on':'off');$('split-btn').setAttribute('aria-pressed',SPLIT);renderAll();};
$('share-btn').onclick=()=>{SHARE=!SHARE;$('share-btn').textContent='Share: '+(SHARE?'on':'off');$('share-btn').setAttribute('aria-pressed',SHARE);renderAll();};
document.querySelectorAll('.pbtn[data-rr]').forEach(b=>b.onclick=()=>{
  RRANGE=b.dataset.rr;
  document.querySelectorAll('.pbtn[data-rr]').forEach(x=>x.classList.toggle('active',x===b));
  renderRouter();
});
$('r-split-btn').onclick=()=>{RSPLIT=!RSPLIT;$('r-split-btn').textContent='Split reasoning: '+(RSPLIT?'on':'off');$('r-split-btn').setAttribute('aria-pressed',RSPLIT);renderRouter();};
$('r-share-btn').onclick=()=>{RSHARE=!RSHARE;$('r-share-btn').textContent='Share: '+(RSHARE?'on':'off');$('r-share-btn').setAttribute('aria-pressed',RSHARE);renderRouter();};
$('tab-opencode').onclick=()=>setTab('opencode');
$('tab-router').onclick=()=>setTab('router');
$('r-search').addEventListener('input',renderRouterRequests);
$('r-status-f').addEventListener('change',e=>{RFSTATE.status=e.target.value||'';renderRouter();});
$('r-provider-f').addEventListener('change',e=>{RFSTATE.provider=e.target.value||null;renderRouter();});
$('r-model-f').addEventListener('change',e=>{RFSTATE.model=e.target.value||null;renderRouter();});
document.addEventListener('click',e=>{
  const rm=e.target.closest?e.target.closest('[data-rm]'):null;
  if(rm){RFSTATE.model=RFSTATE.model===rm.dataset.rm?null:rm.dataset.rm;$('r-model-f').value=RFSTATE.model||'';renderRouter();return;}
  const rp=e.target.closest?e.target.closest('[data-rp]'):null;
  if(rp){RFSTATE.provider=RFSTATE.provider===rp.dataset.rp?null:rp.dataset.rp;$('r-provider-f').value=RFSTATE.provider||'';renderRouter();return;}
  const rs=e.target.closest?e.target.closest('[data-rs]'):null;
  if(rs){const k=rs.dataset.rs;RFSTATE.status=RFSTATE.status===k?null:k;$('r-status-f').value=RFSTATE.status||'';renderRouter();return;}
  const rd=e.target.closest?e.target.closest('[data-rday]'):null;
  if(rd){RFSTATE.day=RFSTATE.day===rd.dataset.rday?null:rd.dataset.rday;renderRouter();return;}
});
/* session rows: event delegation + keyboard */
$('tbl').addEventListener('click',e=>{const tr=e.target.closest('tbody tr[data-i]');if(!tr)return;
  const s=currentRows[+tr.dataset.i];if(s)openSession(s);});
$('tbl').addEventListener('keydown',e=>{if(e.key!=='Enter'&&e.key!==' '&&e.key!=='Spacebar')return;
  const tr=e.target.closest('tbody tr[data-i]');if(!tr)return;e.preventDefault();
  const s=currentRows[+tr.dataset.i];if(s)openSession(s);});
document.querySelectorAll('#tbl th').forEach(th=>{th.onclick=sortH;th.tabIndex=0;th.setAttribute('role','button');
  th.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();sortH(e);}});});
/* export */
function exportData(kind){
  const isR=TAB==='router'&&R;
  if(kind==='json'){
    const b=new Blob([JSON.stringify(isR?R:S,null,2)],{type:'application/json'});
    const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download=isR?'codex-tokens.json':'opencode-tokens.json';a.click();
  }else if(isR){
    const rows=rPeriodDays();
    const head=['date','requests','ok','errors','tokens_in','tokens_out','tokens_cache','tokens_total','what_if_cost'];
    const lines=[head.join(',')].concat(rows.map(d=>[d.date,d.reqs,d.ok,d.err,Math.round(d.ti),Math.round(d.to),Math.round(d.cache),Math.round(rDayTokenTotal(d)),(d.what_if||0)].join(',')));
    const b=new Blob([lines.join('\n')],{type:'text/csv'});
    const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='codex-tokens-days.csv';a.click();
  }else{
    const rows=periodDays();
    const head=['date','messages','sessions','tokens_in','tokens_out','tokens_reasoning','tokens_cache','tokens_total'];
    const lines=[head.join(',')].concat(rows.map(d=>[d.date,d.msgs,d.sessions,Math.round(d.ti),Math.round(d.to),Math.round(d.tr),Math.round(d.cache),Math.round(dayTotal(d))].join(',')));
    const b=new Blob([lines.join('\n')],{type:'text/csv'});
    const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='opencode-tokens-days.csv';a.click();
  }
}
$('export').onclick=e=>{e.stopPropagation();$('expmenu').classList.toggle('open');};
document.querySelectorAll('#expmenu button').forEach(b=>b.onclick=()=>{exportData(b.dataset.x);$('expmenu').classList.remove('open');});
/* stop the hidden server when this window closes or quits (Cmd+Q) */
function stopServer(){try{navigator.sendBeacon&&navigator.sendBeacon('/api/close?wid='+WID);}catch(e){try{fetch('/api/close?wid='+WID,{method:'POST',keepalive:true});}catch(_){}}}
window.addEventListener('beforeunload',stopServer);
window.addEventListener('pagehide',stopServer);
/* modals */
function closeOverlay(id){const ov=$(id);if(ov&&ov.classList.contains('open')){ov.classList.remove('open');if(lastFocus&&lastFocus.focus)lastFocus.focus();}}
$('overlay').onclick=e=>{if(e.target.id==='overlay')closeOverlay('overlay');};
$('modal').addEventListener('click',e=>{if(e.target.classList.contains('modal-x'))closeOverlay('overlay');});
document.addEventListener('keydown',e=>{
  if(e.key==='Escape'){closeOverlay('overlay');return;}
  if(e.key==='Tab'){
    const open=$('overlay').classList.contains('open')?$('overlay'):null;
    if(!open)return;
    const els=[...open.querySelectorAll('button,input,select,a,[tabindex]')].filter(el=>!el.disabled&&el.tabIndex>=0);
    if(!els.length)return;
    if(e.shiftKey&&document.activeElement===els[0]){e.preventDefault();els[els.length-1].focus();}
    else if(!e.shiftKey&&document.activeElement===els[els.length-1]){e.preventDefault();els[0].focus();}
  }
});
/* boot */
let loading=false;
async function load(){
  if(loading)return;loading=true;$('refresh').disabled=true;
  try{
    const [r1,r2]=await Promise.all([
      fetch('/api/stats?_='+Date.now(),{headers:{'X-Window-Id':WID}}),
      fetch('/api/router?_='+Date.now(),{headers:{'X-Window-Id':WID}}),
    ]);
    if(!r1.ok)throw new Error('HTTP '+r1.status);
    S=await r1.json();renderAll();
    try{if(r2.ok){R=await r2.json();if(TAB==='router')renderRouter();else renderRouter();}}catch(_){}
    $('status').textContent='Updated '+new Date().toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
  }catch(e){$('status').textContent='Failed: '+e.message;}
  finally{loading=false;$('refresh').disabled=false;}
}
$('refresh').onclick=load;
$('search').addEventListener('input',renderSessions);
document.addEventListener('visibilitychange',()=>{if(!document.hidden)load();});
setInterval(()=>{if(!document.hidden)load();},30000);
try{const saved=sessionStorage.getItem('ocd-tab');if(saved==='router'){setTab('router');}}catch(_){}
if(S){renderAll();$('status').textContent='Updated just now';}
if(R){renderRouter();}
load();
</script>
</body>
</html>"""


def _cancel_close_timer():
    with LOCK:
        if CLOSE_TIMER:
            CLOSE_TIMER.cancel()


class Handler(BaseHTTPRequestHandler):
    db_path = None
    router_events = None
    router_limits = None
    server = None
    quiet = False

    def _cancel_close(self):
        _cancel_close_timer()

    @staticmethod
    def _close_now():
        if Handler.server:
            Handler.server.shutdown()

    def _arm_close(self):
        global CLOSE_TIMER
        with LOCK:
            if CLOSE_TIMER:
                CLOSE_TIMER.cancel()
            CLOSE_TIMER = threading.Timer(3.0, Handler._close_now)
            CLOSE_TIMER.daemon = True
            CLOSE_TIMER.start()

    def _check_origin(self):
        """Reject cross-origin / DNS-rebinding POSTs (CSRF + key-exfil guard)."""
        origin = self.headers.get("Origin")
        if not origin:
            return True  # local tooling without an Origin header
        try:
            origin_host = urlparse(origin).netloc
        except ValueError:
            return False
        return bool(origin_host) and origin_host == (self.headers.get("Host") or "")

    def _touch(self, wid=None):
        with LOCK:
            now = time.time()
            globals()["LAST_REQUEST"] = now
            if wid:
                WINDOWS[wid] = now
                globals()["HAD_WINDOW"] = True

    def _read_json(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            return {}
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return {}

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        wid = (query.get("wid") or [""])[0] or self.headers.get("X-Window-Id", "")
        if path.startswith(("/api/", "/")):
            self._cancel_close()
            self._touch(wid or None)
        if path in ("/", "/index.html"):
            try:
                payload = json_html(cached_local_stats(self.db_path))
            except Exception as e:
                payload = json_html(blank_stats(
                    "local", "Local OpenCode data", error=str(e),
                    insight="Could not read the local database.",
                ))
            try:
                router_payload = json_html(cached_router_stats(
                    self.router_events, self.router_limits,
                    db_worktrees(self.db_path)))
            except Exception as e:
                router_payload = json_html(blank_router_stats(error=str(e)))
            body = PAGE.replace("__DATA__", payload).replace("__ROUTER__", router_payload).encode("utf-8")
            self._send(200, "text/html; charset=utf-8", body)
        elif path.startswith("/api/session/"):
            session_id = path[len("/api/session/"):]
            if not session_id:
                self._send(404, "application/json", b'{"error":"missing session id"}')
                return
            try:
                con = connect(self.db_path)
                try:
                    prompts = session_prompts(con, session_id)
                finally:
                    con.close()
                self._send(200, "application/json", json.dumps({"prompts": prompts}).encode("utf-8"))
            except Exception as e:
                self._send(404, "application/json", json.dumps({"error": str(e)}).encode("utf-8"))
        elif path.startswith("/api/stats"):
            try:
                stats = cached_local_stats(self.db_path)
            except Exception as e:
                stats = blank_stats("local", "Local OpenCode data", error=str(e))
            body = json.dumps(stats).encode("utf-8")
            self._send(200, "application/json", body)
        elif path.startswith("/api/router"):
            try:
                stats = cached_router_stats(self.router_events, self.router_limits,
                                              db_worktrees(self.db_path))
            except Exception as e:
                stats = blank_router_stats(error=str(e))
            body = json.dumps(stats).encode("utf-8")
            self._send(200, "application/json", body)
        else:
            self._send(404, "text/plain", b"not found")

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        if not self._check_origin():
            self._send(403, "text/plain", b"forbidden")
            return
        if path == "/api/close":
            wid = (query.get("wid") or [""])[0]
            self._send(200, "text/plain", b"bye")
            with LOCK:
                if wid:
                    WINDOWS.pop(wid, None)
                now = time.time()
                others = any(now - last < 90 for w, last in WINDOWS.items())
            if not others:
                self._arm_close()
        else:
            self._send(404, "text/plain", b"not found")

    def _send(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, fmt, *args):
        if not Handler.quiet:
            print("[%s] %s" % (self.log_date_time_string(), fmt % args))


def _idle_monitor(server, idle_timeout):
    """Exit when the app is abandoned: all dashboard windows closed, or the
    --idle-timeout grace period elapsed with no requests at all."""
    while True:
        time.sleep(5)
        with LOCK:
            now = time.time()
            last = LAST_REQUEST
            windowless = HAD_WINDOW and not WINDOWS
            if idle_timeout > 0 and now - last > idle_timeout:
                stop = True
            elif windowless and now - last > 120:
                stop = True
            else:
                stop = False
        if stop:
            try:
                server.shutdown()
            except Exception:
                pass
            return


def main():
    parser = argparse.ArgumentParser(description="OpenCode usage dashboard")
    parser.add_argument("db", nargs="?", default=str(DB_PATH), help="path to opencode.db")
    parser.add_argument("--port", type=int, default=8765, help="HTTP port to serve on")
    parser.add_argument(
        "--idle-timeout", type=int, default=0, metavar="SECONDS",
        help="shut the server down after this many seconds with no requests (0 = off)",
    )
    parser.add_argument("--quiet", action="store_true", help="suppress per-request logging")
    parser.add_argument("--router-events", default=str(ROUTER_EVENTS_DEFAULT),
                        help="path to Codex Router usage-events.jsonl")
    parser.add_argument("--router-limits", default=str(ROUTER_LIMITS_DEFAULT),
                        help="path to Codex Router rate-limits.json")
    args = parser.parse_args()

    Handler.db_path = args.db
    Handler.router_events = args.router_events
    Handler.router_limits = args.router_limits
    Handler.quiet = args.quiet

    try:
        connect(args.db)
    except RuntimeError as e:
        print(f"warning: {e}", file=sys.stderr)
        print("The dashboard will still start and show a setup page once OpenCode has written data.", file=sys.stderr)

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    Handler.server = server
    url = f"http://127.0.0.1:{args.port}"
    print(f"Dashboard live at {url}  (Ctrl+C to stop)")

    monitor = threading.Thread(
        target=_idle_monitor, args=(server, args.idle_timeout), daemon=True
    )
    monitor.start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
