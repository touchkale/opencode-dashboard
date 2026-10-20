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
CLINE_DIR_DEFAULT = Path(os.environ.get("HOME", "")) / ".cline/data"

# All cross-thread shared state is guarded by LOCK.
LOCK = threading.Lock()
LOCAL_CACHE = {"key": None, "stats": None}
ROUTER_CACHE = {"key": None, "stats": None}
CLINE_CACHE = {"key": None, "stats": None}
MAX_ROUTER_EVENTS = 30000  # most-recent lines scanned from usage-events.jsonl
MAX_ROUTER_ROWS = 400  # recent requests kept in the /api/router payload
MAX_CLINE_MESSAGES = 30000  # most-recent assistant messages scanned from Cline sessions
MAX_CLINE_ROWS = 400  # recent Cline messages kept in the /api/cline payload

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
MAX_FILE_ROWS = 15  # top files/subsystems kept in the payload
MAX_COMMIT_ROWS = 60  # commit rows kept in the payload, by tokens desc
RECOVERED_OPENCODE_TOKENS = 4_500_000_000  # deleted OpenCode history, lifetime metadata only
RECOVERED_TOKENS = None  # CLI setting: None = auto (default DB only), 0 = off, >0 explicit


def connect(db_path):
    if not Path(db_path).exists():
        raise RuntimeError(f"Database not found: {db_path}")
    uri = f"file:{pathname2url(str(Path(db_path).resolve()))}?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    con.row_factory = sqlite3.Row
    return con


def _tables(con):
    try:
        return {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
    except sqlite3.Error:
        return set()


def session_table(con):
    tabs = _tables(con)
    if "session_v2" in tabs:
        return "session_v2"
    return "session"


def message_table(con):
    tabs = _tables(con)
    if "session_message" in tabs:
        return "session_message"
    return "message"


def has_table(con, name):
    return name in _tables(con)


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
    ST = session_table(con)
    MT = message_table(con)
    totals = dict(
        con.execute(
            f"""
            SELECT (SELECT COUNT(*) FROM {ST}) AS sessions,
                   (SELECT COUNT(*) FROM {MT}) AS messages,
                   (SELECT COALESCE(SUM(tokens_input),0) FROM {ST}) AS tokens_input,
                   (SELECT COALESCE(SUM(tokens_output),0) FROM {ST}) AS tokens_output,
                   (SELECT COALESCE(SUM(tokens_reasoning),0) FROM {ST}) AS tokens_reasoning,
                   (SELECT COALESCE(SUM(tokens_cache_read),0) FROM {ST}) AS tokens_cache_read,
                   (SELECT COALESCE(SUM(tokens_cache_write),0) FROM {ST}) AS tokens_cache_write,
                   (SELECT COALESCE(SUM(cost),0) FROM {ST}) AS cost,
                   (SELECT COALESCE(AVG((time_updated-time_created)/60000.0),0)
                      FROM {ST} WHERE time_created>0 AND time_updated>time_created) AS avg_duration_min
            """
        ).fetchone()
    )

    totals["tokens_cache_total"] = (
        (totals.get("tokens_cache_read") or 0)
        + (totals.get("tokens_cache_write") or 0)
    )

    # Session rows are the source of truth for tokens.  Build a complete
    # rollup map first so every session can be attributed to dates below.
    session_roll = {}
    for r in con.execute(
        f"""
        SELECT id, time_created,
               COALESCE(tokens_input,0) AS ti,
               COALESCE(tokens_output,0) AS to_,
               COALESCE(tokens_reasoning,0) AS tr,
               COALESCE(tokens_cache_read,0) AS cr,
               COALESCE(tokens_cache_write,0) AS cw,
               COALESCE(cost,0) AS cost
        FROM {ST}
        """
    ):
        session_roll[r["id"]] = {
            "start": r["time_created"] or 0,
            "start_day": _day(r["time_created"] or 0),
            "ti": num(r["ti"]),
            "to": num(r["to_"]),
            "tr": num(r["tr"]),
            "cr": num(r["cr"]),
            "cw": num(r["cw"]),
            "cost": num(r["cost"]),
        }
        session_roll[r["id"]]["total"] = (
            session_roll[r["id"]]["ti"]
            + session_roll[r["id"]]["to"]
            + session_roll[r["id"]]["tr"]
            + session_roll[r["id"]]["cr"]
            + session_roll[r["id"]]["cw"]
        )

    def token_total(entry):
        return (entry["ti"] + entry["to"] + entry["tr"]
                + entry["cr"] + entry["cw"])

    # Parts first: one pass over step-finish parts, grouped per session.  A
    # nested cache object is authoritative (read+write); only when neither
    # nested field exists do we fall back to total-input-output-reasoning.
    session_parts = defaultdict(list)
    for r in con.execute(
        f"""
        SELECT session_id, time_created, data FROM part
        WHERE json_extract(data,'$.type')='step-finish'
          AND session_id IN (SELECT id FROM {ST})
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
        cache_obj = t.get("cache")
        if isinstance(cache_obj, dict) and (
            "read" in cache_obj or "write" in cache_obj
        ):
            cr = num(cache_obj.get("read"))
            cw = num(cache_obj.get("write"))
        else:
            # Legacy parts only carry 'total'; treat the derived cache as
            # read tokens so the read-hit-rate basis stays meaningful.
            cr = max(0.0, tt - ti - to - tr)
            cw = 0.0
        sid = r["session_id"]
        start_day = session_roll.get(sid, {}).get("start_day")
        session_parts[sid].append(
            {
                "day": _day(r["time_created"]) or start_day,
                "ti": ti,
                "to": to,
                "tr": tr,
                "cr": cr,
                "cw": cw,
                "cost": num(data.get("cost")),
            }
        )

    # Incomplete sessions need message-level tokens.  Compute part coverage
    # first; only those sessions are queried, in chunks, so the 5.9GB
    # message table is never fully scanned.
    incomplete = []
    for sid, roll in session_roll.items():
        covered = sum(token_total(e) for e in session_parts.get(sid, []))
        if covered < roll["total"] - 0.5:
            incomplete.append(sid)

    session_messages = defaultdict(list)
    if incomplete and MT == "session_message":
        CHUNK = 400
        for i in range(0, len(incomplete), CHUNK):
            chunk = incomplete[i:i + CHUNK]
            placeholders = ",".join("?" for _ in chunk)
            for r in con.execute(
                f"""
                SELECT session_id, time_created, data
                FROM session_message
                WHERE session_id IN ({placeholders}) AND type='assistant'
                """,
                chunk,
            ):
                try:
                    data = json.loads(r["data"])
                except (ValueError, TypeError):
                    continue
                t = data.get("tokens") or {}
                if not isinstance(t, dict) or not t:
                    continue
                ti = num(t.get("input"))
                to = num(t.get("output"))
                tr = num(t.get("reasoning"))
                tt = num(t.get("total"))
                cache_obj = t.get("cache")
                if isinstance(cache_obj, dict) and (
                    "read" in cache_obj or "write" in cache_obj
                ):
                    cr = num(cache_obj.get("read"))
                    cw = num(cache_obj.get("write"))
                else:
                    cr = max(0.0, tt - ti - to - tr)
                    cw = 0.0
                if not (ti or to or tr or cr or cw):
                    continue
                sid = r["session_id"]
                start_day = session_roll.get(sid, {}).get("start_day")
                session_messages[sid].append(
                    {
                        "day": _day(r["time_created"]) or start_day,
                        "ti": ti,
                        "to": to,
                        "tr": tr,
                        "cr": cr,
                        "cw": cw,
                        "cost": 0.0,
                    }
                )

    tokens_by_day = {}
    unattributed = 0.0

    def add_day_tokens(day, entry):
        bucket = tokens_by_day.setdefault(
            day,
            {"ti": 0.0, "to": 0.0, "tr": 0.0, "cr": 0.0, "cw": 0.0,
             "cost": 0.0},
        )
        for key in ("ti", "to", "tr", "cr", "cw", "cost"):
            bucket[key] += entry.get(key, 0.0)

    for sid, roll in session_roll.items():
        entries = [dict(e) for e in session_parts.get(sid, [])]
        target = roll["total"]
        part_covered = sum(token_total(e) for e in entries)

        # Fill part gaps with assistant-message tokens by message date.  The
        # message stream can describe the whole session, so cap it at the
        # part residual rather than double-counting already attributed tokens.
        if part_covered < target - 0.5 and session_messages.get(sid):
            room = target - part_covered
            msgs = session_messages[sid]
            msg_total = sum(token_total(e) for e in msgs)
            factor = min(1.0, room / msg_total) if msg_total > 0 else 0.0
            for e in msgs:
                scaled = {k: e.get(k, 0.0) * factor for k in
                          ("ti", "to", "tr", "cr", "cw", "cost")}
                scaled["day"] = e["day"]
                entries.append(scaled)

        attributed = sum(token_total(e) for e in entries)
        if attributed > target and attributed > 0:
            factor = target / attributed
            for e in entries:
                for key in ("ti", "to", "tr", "cr", "cw", "cost"):
                    e[key] = e.get(key, 0.0) * factor
            attributed = sum(token_total(e) for e in entries)

        missing = target - attributed
        if missing > 0.5 or (not entries and target > 0):
            used = {
                key: sum(e.get(key, 0.0) for e in entries)
                for key in ("ti", "to", "tr", "cr", "cw")
            }
            residual = {
                key: max(0.0, roll[key] - used[key])
                for key in ("ti", "to", "tr", "cr", "cw")
            }
            residual_sum = sum(residual.values())
            if missing > 0.5:
                if residual_sum > 0:
                    scale = missing / residual_sum
                    for key in residual:
                        residual[key] *= scale
                else:
                    residual["ti"] = missing
            else:
                residual = {key: 0.0 for key in residual}
            dated = [e["day"] for e in entries if e.get("day")]
            day = max(dated) if dated else roll["start_day"]
            cost_missing = roll["cost"] - sum(
                e.get("cost", 0.0) for e in entries
            )
            residual["day"] = day
            residual["cost"] = cost_missing
            entries.append(residual)

        for entry in entries:
            if not token_total(entry) and not entry.get("cost"):
                continue
            day = entry.get("day")
            if not day:
                unattributed += token_total(entry)
                continue
            add_day_tokens(day, entry)

    # One scan of the message table gives per-day counts plus the legacy
    # per-hour message intensity series.
    day_msgs = defaultdict(int)
    hours = defaultdict(int)
    for r in con.execute(
        f"""
        SELECT date(time_created/1000,'unixepoch','localtime') AS d,
               CAST(strftime('%H',time_created/1000,'unixepoch','localtime') AS INTEGER) AS h,
               COUNT(*) AS c
        FROM {MT} WHERE time_created > 0 GROUP BY d,h
        """
    ):
        day_msgs[r["d"]] += r["c"]
        hours[r["h"]] += r["c"]

    day_sessions = defaultdict(int)
    for r in con.execute(
        f"SELECT date(time_created/1000,'unixepoch','localtime') AS d, COUNT(*) AS c FROM {ST} WHERE time_created > 0 GROUP BY d"
    ):
        day_sessions[r["d"]] = r["c"]

    agents = defaultdict(lambda: {"n": 0, "toks": 0})
    for r in con.execute(
        f"SELECT agent, COUNT(*) AS n, COALESCE(SUM(tokens_input)+SUM(tokens_output)+SUM(tokens_reasoning)+SUM(tokens_cache_read)+SUM(tokens_cache_write),0) AS t FROM {ST} GROUP BY agent"
    ):
        a = r["agent"] or "default"
        agents[a]["n"] += r["n"]
        agents[a]["toks"] += r["t"]

    models = defaultdict(lambda: {"n": 0, "ti": 0, "to": 0, "tr": 0, "cache": 0, "cr": 0, "cost": 0})
    for r in con.execute(
        f"""
        SELECT model, COUNT(*) AS n,
                COALESCE(SUM(tokens_input),0) AS ti,
                COALESCE(SUM(tokens_output),0) AS to_,
                COALESCE(SUM(tokens_reasoning),0) AS tr,
                COALESCE(SUM(tokens_cache_read),0) AS cr,
                COALESCE(SUM(tokens_cache_write),0) AS cw,
                COALESCE(SUM(cost),0) AS cost
        FROM {ST} GROUP BY model
        """
    ):
        m = parse_model(r["model"])
        models[m]["n"] += r["n"]
        models[m]["ti"] += r["ti"]
        models[m]["to"] += r["to_"]
        models[m]["tr"] += r["tr"]
        models[m]["cache"] += (r["cr"] or 0) + (r["cw"] or 0)
        models[m]["cr"] += r["cr"] or 0
        models[m]["cost"] += r["cost"]

    # Most recent sessions only — the payload stays bounded and the detail
    # endpoint fetches prompts on demand.
    sessions = []
    for r in con.execute(
        f"""
        SELECT s.id, s.title, s.agent, s.model, s.time_created, s.time_updated,
                s.tokens_input, s.tokens_output, s.tokens_reasoning,
                COALESCE(s.tokens_cache_read,0)+COALESCE(s.tokens_cache_write,0) AS cache_read, s.cost,
               p.worktree,
               (SELECT COUNT(*) FROM {MT} m WHERE m.session_id = s.id) AS msgs
        FROM {ST} s LEFT JOIN project p ON p.id = s.project_id
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
        f"""SELECT p.id, p.name, p.worktree, COUNT(s.id) AS n,
                   COALESCE(SUM(s.tokens_input)+SUM(s.tokens_output)+SUM(s.tokens_reasoning)+SUM(s.tokens_cache_read)+SUM(s.tokens_cache_write),0) AS t
            FROM project p LEFT JOIN {ST} s ON s.project_id=p.id GROUP BY p.id"""
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
        f"""SELECT id, project_id, time_updated,
                  COALESCE(tokens_input,0)+COALESCE(tokens_output,0)
                   +COALESCE(tokens_reasoning,0)+COALESCE(tokens_cache_read,0)
                   +COALESCE(tokens_cache_write,0) AS tot
           FROM {ST}"""
    ):
        nm, wt = pid_info.get(r["project_id"], ("—", ""))
        sess_all[r["id"]] = (r["tot"] or 0, r["time_updated"] or 0, nm, wt)

    # Changed files from edit/write tool parts: distinct filePaths per session.
    sess_files = defaultdict(set)
    try:
        for r in con.execute(
            f"""SELECT session_id, json_extract(data,'$.state.input.filePath') AS fp
               FROM part
               WHERE json_extract(data,'$.type')='tool'
                 AND json_extract(data,'$.tool') IN ('edit','write')
                 AND session_id IN (SELECT id FROM {ST})"""
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
    day_entries = []
    for d in days:
        tok = tokens_by_day.get(d, {})
        cache_read = tok.get("cr", 0.0)
        cache_write = tok.get("cw", 0.0)
        day_entries.append(
            {
                "date": d,
                "msgs": day_msgs.get(d, 0),
                "sessions": day_sessions.get(d, 0),
                "ti": round(tok.get("ti", 0.0), 4),
                "to": round(tok.get("to", 0.0), 4),
                "tr": round(tok.get("tr", 0.0), 4),
                "cache": round(cache_read + cache_write, 4),
                "cache_read": round(cache_read, 4),
                "cache_write": round(cache_write, 4),
                "cost": round(tok.get("cost", 0.0), 4),
            }
        )
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
    totals["tokens_cache_total"] = (
        totals["tokens_cache_read"] + totals["tokens_cache_write"]
    )
    totals["unattributed"] = round(unattributed, 1)
    totals["cache_rate"] = cache_rate(totals["tokens_input"], totals["tokens_cache_read"])
    totals["avg_tokens_per_session"] = round(total_tokens / totals["sessions"], 0) if totals["sessions"] else 0

    # Records: biggest day / session / burn / streaks — pure tokens, fun to know.
    # Biggest session + fastest burn come from full-table queries (not the
    # capped recent-sessions list) so large histories still report all-time
    # bests accurately.
    biggest_day = max(day_entries, key=lambda d: d["total"]) if day_entries else None
    biggest_row = con.execute(
        f"""
        SELECT title, model,
               COALESCE(tokens_input,0)+COALESCE(tokens_output,0)
                 +COALESCE(tokens_reasoning,0)+COALESCE(tokens_cache_read,0)
                 +COALESCE(tokens_cache_write,0) AS tot
        FROM {ST} ORDER BY tot DESC LIMIT 1
        """
    ).fetchone()
    biggest_session = (
        {"title": biggest_row["title"], "tokens_total": biggest_row["tot"],
         "model": parse_model(biggest_row["model"])}
        if biggest_row else None
    )
    fastest_row = con.execute(
        f"""
        SELECT title,
               (COALESCE(tokens_input,0)+COALESCE(tokens_output,0)
                 +COALESCE(tokens_reasoning,0)+COALESCE(tokens_cache_read,0)
                 +COALESCE(tokens_cache_write,0)) AS tot,
               (time_updated-time_created)/60000.0 AS mins
        FROM {ST}
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
        f"""
        SELECT title, (time_updated-time_created)/60000.0 AS mins FROM {ST}
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
             cache_rate(v["ti"], v["cr"])]
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
    "tokens_cache_total": 0,
    "tokens_total": 0,
    "unattributed": 0,
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


def ledger_path_for(db_path):
    p = Path(db_path)
    return p.with_name(p.name + ".usage-ledger.sqlite")


def ledger_connect(db_path):
    lp = ledger_path_for(db_path)
    con = sqlite3.connect(str(lp))
    con.row_factory = sqlite3.Row
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS session_usage (
            session_id TEXT PRIMARY KEY,
            title TEXT,
            agent TEXT,
            model TEXT,
            project_id TEXT,
            project_name TEXT,
            worktree TEXT,
            time_created INTEGER,
            time_updated INTEGER,
            tokens_input INTEGER DEFAULT 0,
            tokens_output INTEGER DEFAULT 0,
            tokens_reasoning INTEGER DEFAULT 0,
            tokens_cache_read INTEGER DEFAULT 0,
            tokens_cache_write INTEGER DEFAULT 0,
            cost REAL DEFAULT 0,
            msgs INTEGER DEFAULT 0,
            deleted INTEGER DEFAULT 0,
            last_seen INTEGER
        )
        """
    )
    con.commit()
    return con


def _prefs_table(con):
    con.execute("CREATE TABLE IF NOT EXISTS prefs (key TEXT PRIMARY KEY, value TEXT)")


def ledger_get_pref(db_path, key, default=None):
    """Read a JSON-encoded preference from the ledger's `prefs` table.

    Any missing/corrupt value (or unreadable ledger) returns `default`, so a
    bad file can never take the dashboard down.
    """
    try:
        con = ledger_connect(db_path)
    except Exception:
        return default
    try:
        _prefs_table(con)
        con.commit()
        row = con.execute("SELECT value FROM prefs WHERE key = ?", (str(key),)).fetchone()
        if row is None:
            return default
        return json.loads(row["value"])
    except Exception:
        return default
    finally:
        try:
            con.close()
        except Exception:
            pass


def ledger_set_pref(db_path, key, value):
    """Persist a JSON-encoded preference in the ledger; True on success."""
    try:
        con = ledger_connect(db_path)
    except Exception:
        return False
    try:
        _prefs_table(con)
        con.execute(
            "INSERT INTO prefs(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (str(key), json.dumps(value)),
        )
        con.commit()
        return True
    except Exception:
        return False
    finally:
        try:
            con.close()
        except Exception:
            pass


def sync_session_ledger(db_path, con):
    """Upsert every session currently in the OpenCode DB into the persistent
    ledger. Sessions that disappear from the source DB are retained (marked
    deleted) so their tokens are never lost.

    Returns True only when the session source was read successfully.  A failed
    or interrupted query returns False and leaves existing ledger rows alone,
    so the caller can skip the merge instead of resurrecting stale data.
    """
    try:
        lcon = ledger_connect(db_path)
    except Exception:
        return False
    ok = True
    try:
        ST = session_table(con)
        MT = message_table(con)
        pid_info = {}
        try:
            for r in con.execute("SELECT id, name, worktree FROM project"):
                pid_info[r["id"]] = (r["name"] or short_dir(r["worktree"]), r["worktree"])
        except sqlite3.Error:
            pass
        msgs = {}
        try:
            for r in con.execute(
                f"SELECT session_id, COUNT(*) AS c FROM {MT} GROUP BY session_id"
            ):
                msgs[r["session_id"]] = r["c"]
        except sqlite3.Error:
            pass
        rows = []
        query_failed = False
        try:
            rows = con.execute(
                f"""SELECT id, title, agent, model, project_id, time_created, time_updated,
                           tokens_input, tokens_output, tokens_reasoning, tokens_cache_read,
                           tokens_cache_write, cost FROM {ST}"""
            ).fetchall()
        except sqlite3.Error:
            query_failed = True
            ok = False
        now = int(time.time() * 1000)
        cur_ids = []
        for r in rows:
            sid = r["id"]
            cur_ids.append(sid)
            nm, wt = pid_info.get(r["project_id"], ("—", ""))
            lcon.execute(
                """
                INSERT INTO session_usage (
                    session_id, title, agent, model, project_id, project_name, worktree,
                    time_created, time_updated, tokens_input, tokens_output, tokens_reasoning,
                    tokens_cache_read, tokens_cache_write, cost, msgs, deleted, last_seen)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?)
                ON CONFLICT(session_id) DO UPDATE SET
                    title=excluded.title, agent=excluded.agent, model=excluded.model,
                    project_id=excluded.project_id, project_name=excluded.project_name,
                    worktree=excluded.worktree, time_created=excluded.time_created,
                    time_updated=excluded.time_updated, tokens_input=excluded.tokens_input,
                    tokens_output=excluded.tokens_output, tokens_reasoning=excluded.tokens_reasoning,
                    tokens_cache_read=excluded.tokens_cache_read,
                    tokens_cache_write=excluded.tokens_cache_write, cost=excluded.cost,
                    msgs=excluded.msgs, deleted=0, last_seen=excluded.last_seen
                """,
                (
                    sid, r["title"], r["agent"], r["model"], r["project_id"], nm, wt,
                    r["time_created"], r["time_updated"], r["tokens_input"] or 0,
                    r["tokens_output"] or 0, r["tokens_reasoning"] or 0,
                    r["tokens_cache_read"] or 0, r["tokens_cache_write"] or 0,
                    r["cost"] or 0, msgs.get(sid, 0), now,
                ),
            )
        if not query_failed:
            if cur_ids:
                placeholders = ",".join("?" for _ in cur_ids)
                lcon.execute(
                    f"UPDATE session_usage SET deleted=1 WHERE session_id NOT IN ({placeholders})",
                    cur_ids,
                )
            else:
                # Every chat is gone from the source: keep the history and mark
                # all retained ledger rows deleted instead of leaving them live.
                lcon.execute("UPDATE session_usage SET deleted=1")
        lcon.commit()
    except Exception:
        ok = False
    finally:
        try:
            lcon.close()
        except Exception:
            pass
    return ok


def ledger_totals(db_path):
    try:
        lcon = ledger_connect(db_path)
    except Exception:
        return None
    try:
        row = lcon.execute(
            """
            SELECT COUNT(*) AS sessions,
                   COALESCE(SUM(msgs),0) AS messages,
                   COALESCE(SUM(tokens_input),0) AS ti,
                   COALESCE(SUM(tokens_output),0) AS to_,
                   COALESCE(SUM(tokens_reasoning),0) AS tr,
                   COALESCE(SUM(tokens_cache_read),0) AS cr,
                   COALESCE(SUM(tokens_cache_write),0) AS cw,
                   COALESCE(SUM(cost),0) AS cost
            FROM session_usage
            """
        ).fetchone()
        return {
            "sessions": row["sessions"] or 0,
            "messages": row["messages"] or 0,
            "ti": row["ti"] or 0,
            "to": row["to_"] or 0,
            "tr": row["tr"] or 0,
            "cr": row["cr"] or 0,
            "cw": row["cw"] or 0,
            "cost": row["cost"] or 0,
        }
    except Exception:
        return None
    finally:
        try:
            lcon.close()
        except Exception:
            pass


def ledger_day_tokens(db_path, deleted_only=False):
    """Per-day ledger rollup: tokens/cost plus session and message counts.

    Days are keyed like everywhere else ('YYYY-MM-DD' local time, from each
    ledger session's creation time).  With ``deleted_only=True`` only rows that
    are marked deleted *or* no longer exist in the source DB are included, so
    the merge adds historical deltas without re-attributing live activity.
    """
    out = {}
    live_ids = None
    if deleted_only:
        try:
            src = connect(db_path)
            try:
                ST = session_table(src)
                live_ids = {r[0] for r in src.execute(f"SELECT id FROM {ST}")}
            finally:
                src.close()
        except Exception:
            live_ids = None
    try:
        lcon = ledger_connect(db_path)
    except Exception:
        return out
    try:
        for r in lcon.execute(
            """SELECT session_id, deleted, time_created, tokens_input, tokens_output,
                      tokens_reasoning, tokens_cache_read, tokens_cache_write, cost, msgs
               FROM session_usage"""
        ):
            if deleted_only:
                missing_from_source = (
                    live_ids is not None and r["session_id"] not in live_ids
                )
                if not (r["deleted"] or missing_from_source):
                    continue
            ms = r["time_created"] or 0
            day = _day(ms)
            if not day:
                continue
            ti = r["tokens_input"] or 0
            to = r["tokens_output"] or 0
            tr = r["tokens_reasoning"] or 0
            cr = r["tokens_cache_read"] or 0
            cw = r["tokens_cache_write"] or 0
            cache = cr + cw
            cost = r["cost"] or 0
            e = out.setdefault(
                day,
                {"ti": 0.0, "to": 0.0, "tr": 0.0, "cache": 0.0,
                 "cache_read": 0.0, "cache_write": 0.0, "cost": 0.0,
                 "sessions": 0, "msgs": 0},
            )
            e["ti"] += ti
            e["to"] += to
            e["tr"] += tr
            e["cache"] += cache
            e["cache_read"] += cr
            e["cache_write"] += cw
            e["cost"] += cost
            e["sessions"] += 1
            e["msgs"] += r["msgs"] or 0
    except Exception:
        pass
    finally:
        try:
            lcon.close()
        except Exception:
            pass
    return out


def ledger_rollups(db_path, ldays):
    """Rebuild model/agent/project/record rollups from the persistent ledger.

    query_stats() derives these from live sessions, which go empty once every
    chat is deleted. The ledger keeps one row per session forever, so these
    rollups let the panels, records and date range survive deletion. Row
    shapes match the live payloads exactly. Returns None when the ledger is
    unreadable so callers can keep the live values.
    """
    try:
        lcon = ledger_connect(db_path)
    except Exception:
        return None
    try:
        models = defaultdict(lambda: {"n": 0, "ti": 0, "to": 0, "tr": 0, "cache": 0, "cr": 0, "cost": 0})
        for r in lcon.execute(
            """SELECT model, COUNT(*) AS n,
                      COALESCE(SUM(tokens_input),0) AS ti,
                      COALESCE(SUM(tokens_output),0) AS to_,
                      COALESCE(SUM(tokens_reasoning),0) AS tr,
                      COALESCE(SUM(tokens_cache_read),0) AS cr,
                      COALESCE(SUM(tokens_cache_write),0) AS cw,
                      COALESCE(SUM(cost),0) AS cost
               FROM session_usage GROUP BY model"""
        ):
            m = models[parse_model(r["model"])]
            m["n"] += r["n"]
            m["ti"] += r["ti"]
            m["to"] += r["to_"]
            m["tr"] += r["tr"]
            m["cache"] += (r["cr"] or 0) + (r["cw"] or 0)
            m["cr"] += r["cr"] or 0
            m["cost"] += r["cost"]
        model_rows = []
        for k, v in models.items():
            tot = v["ti"] + v["to"] + v["tr"] + v["cache"]
            model_rows.append(
                [k, v["n"], tot, v["ti"], v["to"], v["tr"], v["cache"], v["cost"],
                 round(tot / v["n"], 0) if v["n"] else 0,
                 cache_rate(v["ti"], v["cr"])]
            )
        model_rows.sort(key=lambda kv: -kv[2])

        agents = defaultdict(lambda: {"n": 0, "toks": 0})
        for r in lcon.execute(
            """SELECT agent, COUNT(*) AS n,
                      COALESCE(SUM(tokens_input)+SUM(tokens_output)+SUM(tokens_reasoning)
                               +SUM(tokens_cache_read)+SUM(tokens_cache_write),0) AS t
               FROM session_usage GROUP BY agent"""
        ):
            a = r["agent"] or "default"
            agents[a]["n"] += r["n"]
            agents[a]["toks"] += r["t"]
        agent_rows = sorted(
            [[k, v["n"], v["toks"]] for k, v in agents.items()], key=lambda kv: -kv[1]
        )

        projects = defaultdict(lambda: {"n": 0, "toks": 0})
        for r in lcon.execute(
            """SELECT project_name, COUNT(*) AS n,
                      COALESCE(SUM(tokens_input)+SUM(tokens_output)+SUM(tokens_reasoning)
                               +SUM(tokens_cache_read)+SUM(tokens_cache_write),0) AS t
               FROM session_usage GROUP BY project_name"""
        ):
            key = r["project_name"] or "—"
            projects[key]["n"] += r["n"]
            projects[key]["toks"] += r["t"]
        project_rows = sorted(
            [[k, v["n"], v["toks"]] for k, v in projects.items()], key=lambda kv: -kv[2]
        )

        records = {
            "biggest_day": None,
            "biggest_session": None,
            "fastest_burn": None,
            "streaks": {"current": 0, "longest": 0},
        }
        for date, ld in ldays.items():
            tot = ld["ti"] + ld["to"] + ld["tr"] + ld["cache"]
            if records["biggest_day"] is None or tot > records["biggest_day"]["total"]:
                records["biggest_day"] = {
                    "date": date,
                    "total": round(tot, 1),
                    "sessions": ld.get("sessions", 0),
                }
        row = lcon.execute(
            """SELECT title, model,
                      COALESCE(tokens_input,0)+COALESCE(tokens_output,0)
                        +COALESCE(tokens_reasoning,0)+COALESCE(tokens_cache_read,0)
                        +COALESCE(tokens_cache_write,0) AS tot
               FROM session_usage ORDER BY tot DESC LIMIT 1"""
        ).fetchone()
        if row and row["tot"]:
            records["biggest_session"] = {
                "title": row["title"],
                "total": row["tot"],
                "model": parse_model(row["model"]),
            }
        row = lcon.execute(
            """SELECT title,
                      (COALESCE(tokens_input,0)+COALESCE(tokens_output,0)
                        +COALESCE(tokens_reasoning,0)+COALESCE(tokens_cache_read,0)
                        +COALESCE(tokens_cache_write,0)) AS tot,
                      (time_updated-time_created)/60000.0 AS mins
               FROM session_usage
               WHERE time_created>0 AND time_updated>time_created
                 AND (time_updated-time_created) >= 30000
               ORDER BY tot/((time_updated-time_created)/60000.0) DESC LIMIT 1"""
        ).fetchone()
        if row and row["mins"]:
            records["fastest_burn"] = {
                "title": row["title"],
                "burn": round(row["tot"] / row["mins"], 0),
            }
        activity = zero_days_window()
        for slot in activity:
            ld = ldays.get(slot["date"])
            if not ld:
                continue
            slot["ti"] = ld["ti"]
            slot["to"] = ld["to"]
            slot["tr"] = ld["tr"]
            slot["cache"] = ld["cache"]
            slot["cache_read"] = ld.get("cache_read", 0)
            slot["cache_write"] = ld.get("cache_write", 0)
            slot["msgs"] = ld.get("msgs", 0)
            slot["sessions"] = ld.get("sessions", 0)
        records["streaks"] = compute_streaks(activity)

        if ldays:
            dates = sorted(ldays)
            rng = f"{dates[0]} → {dates[-1]}"
        else:
            rng = "no activity"
        return {
            "models": model_rows,
            "agents": agent_rows,
            "projects": project_rows,
            "records": records,
            "range": rng,
        }
    except Exception:
        return None
    finally:
        try:
            lcon.close()
        except Exception:
            pass


def merge_ledger_into_stats(stats, db_path):
    agg = ledger_totals(db_path)
    if not agg or agg["sessions"] <= 0:
        return stats
    stats["totals"]["sessions"] = agg["sessions"]
    if agg["messages"] > 0:
        stats["totals"]["messages"] = agg["messages"]
    stats["totals"]["tokens_input"] = agg["ti"]
    stats["totals"]["tokens_output"] = agg["to"]
    stats["totals"]["tokens_reasoning"] = agg["tr"]
    stats["totals"]["tokens_cache_read"] = agg["cr"]
    stats["totals"]["tokens_cache_write"] = agg["cw"]
    stats["totals"]["cost"] = agg["cost"]
    total_tokens = agg["ti"] + agg["to"] + agg["tr"] + agg["cr"] + agg["cw"]
    stats["totals"]["tokens_total"] = total_tokens
    stats["totals"]["tokens_cache_total"] = agg["cr"] + agg["cw"]
    stats["totals"]["cache_rate"] = cache_rate(agg["ti"], agg["cr"])
    stats["totals"]["avg_tokens_per_session"] = (
        round(total_tokens / agg["sessions"], 0) if agg["sessions"] else 0
    )

    # Live days are already reconciled against live session totals.  Only add
    # the delta for sessions that have disappeared; never replace live day
    # values with ledger session-start attribution (that would move a session
    # that ran past midnight onto its start date).
    deleted_days = ledger_day_tokens(db_path, deleted_only=True)
    live_by_date = {e["date"]: e for e in stats["days"]}
    for date, ld in deleted_days.items():
        e = live_by_date.get(date)
        if e is None:
            e = {
                "date": date,
                "msgs": 0,
                "sessions": 0,
                "ti": 0.0,
                "to": 0.0,
                "tr": 0.0,
                "cache": 0.0,
                "cache_read": 0.0,
                "cache_write": 0.0,
                "cost": 0.0,
            }
            stats["days"].append(e)
            live_by_date[date] = e
        e["ti"] = round((e.get("ti") or 0) + ld["ti"], 4)
        e["to"] = round((e.get("to") or 0) + ld["to"], 4)
        e["tr"] = round((e.get("tr") or 0) + ld["tr"], 4)
        e["cache"] = round((e.get("cache") or 0) + ld["cache"], 4)
        e["cache_read"] = round((e.get("cache_read") or 0) + ld.get("cache_read", 0), 4)
        e["cache_write"] = round((e.get("cache_write") or 0) + ld.get("cache_write", 0), 4)
        e["cost"] = round((e.get("cost") or 0) + ld["cost"], 4)
        e["sessions"] = (e.get("sessions") or 0) + ld["sessions"]
        e["msgs"] = (e.get("msgs") or 0) + ld["msgs"]
    for e in stats["days"]:
        e["total"] = round(
            (e.get("ti") or 0) + (e.get("to") or 0) + (e.get("tr") or 0)
            + (e.get("cache") or 0),
            1,
        )
    stats["days"] = sorted(stats["days"], key=lambda x: x["date"])

    # Activity keeps its existing live values; only deleted deltas are added.
    for slot in stats.get("activity", []):
        ld = deleted_days.get(slot["date"])
        if not ld:
            continue
        slot["ti"] = round((slot.get("ti") or 0) + ld["ti"], 4)
        slot["to"] = round((slot.get("to") or 0) + ld["to"], 4)
        slot["tr"] = round((slot.get("tr") or 0) + ld["tr"], 4)
        slot["cache"] = round((slot.get("cache") or 0) + ld["cache"], 4)
        slot["cache_read"] = round((slot.get("cache_read") or 0) + ld.get("cache_read", 0), 4)
        slot["cache_write"] = round((slot.get("cache_write") or 0) + ld.get("cache_write", 0), 4)
        slot["cost"] = round((slot.get("cost") or 0) + ld["cost"], 4)
        slot["sessions"] = (slot.get("sessions") or 0) + ld["sessions"]
        slot["msgs"] = (slot.get("msgs") or 0) + ld["msgs"]
        slot["total"] = round(
            (slot.get("ti") or 0) + (slot.get("to") or 0) + (slot.get("tr") or 0)
            + (slot.get("cache") or 0),
            1,
        )

    # Historical rollups: models/agents/projects/records/range are rebuilt
    # from the full ledger, while biggest_day/streaks use the merged days.
    merged_days = {e["date"]: e for e in stats["days"]}
    rollups = ledger_rollups(db_path, merged_days)
    if rollups:
        stats["models"] = rollups["models"]
        stats["agents"] = rollups["agents"]
        stats["projects"] = rollups["projects"]
        stats["records"] = rollups["records"]
        stats["range"] = rollups["range"]
    else:
        if stats["days"]:
            biggest = max(stats["days"], key=lambda d: d["total"])
            stats["records"]["biggest_day"] = {
                "date": biggest["date"],
                "total": biggest["total"],
                "sessions": biggest.get("sessions", 0),
            }
            stats["range"] = f"{stats['days'][0]['date']} → {stats['days'][-1]['date']}"

    # Keep the daily identity even if the ledger retains undated sessions:
    # any token not attributable to a date is reported as unattributed.
    day_sum = sum(e.get("total", 0) for e in stats["days"])
    residual = total_tokens - day_sum
    stats["totals"]["unattributed"] = round(residual, 1) if residual > 0.5 else 0

    if stats.get("insights"):
        stats["insights"][0] = (
            f"Across {agg['sessions']} sessions you used {total_tokens:,.0f} tokens "
            f"in {stats['totals']['messages']:,} messages."
        )
        # The live "no activity" line would contradict the rehydrated records.
        stale = "No activity recorded yet."
        if rollups and rollups["records"]["biggest_day"]:
            bd = rollups["records"]["biggest_day"]
            fresh = (
                f"Your biggest token day was {bd['date']} — {bd['total']:,.0f} tokens "
                f"across {bd['sessions']} sessions."
            )
            stats["insights"] = [fresh if i == stale else i for i in stats["insights"]]
    return stats


def recovered_tokens_for(db_path):
    """Resolve the recovered-token amount for a database path.

    `RECOVERED_TOKENS` None means auto: apply `RECOVERED_OPENCODE_TOKENS`
    only for the module's default DB_PATH, so tests and other databases stay
    exact. 0 disables, a positive value overrides for any database.
    """
    setting = RECOVERED_TOKENS
    if setting is None:
        try:
            if Path(db_path).resolve() == DB_PATH.resolve():
                return RECOVERED_OPENCODE_TOKENS
        except Exception:
            return 0
        return 0
    amount = int(round(num(setting)))
    return amount if amount > 0 else 0


def apply_recovered_adjustment(stats, amount):
    """Attach recovered OpenCode history as lifetime-only metadata.

    The amount is distributed across input / output / reasoning / cache in the
    same ratio as the recorded totals, so the recovered history keeps the
    user's real token mix.  It is deliberately *not* folded into `totals` or
    `models`: recorded totals, daily charts and rankings stay exact.  A
    separate `lifetime` rollup (recorded + recovered) is exposed for display.
    """
    if not isinstance(stats, dict):
        return stats
    amount = int(round(num(amount)))
    if amount <= 0:
        return stats
    totals = stats.get("totals")
    if not isinstance(totals, dict):
        return stats
    buckets = (
        ("input", num(totals.get("tokens_input"))),
        ("output", num(totals.get("tokens_output"))),
        ("reasoning", num(totals.get("tokens_reasoning"))),
        ("cache_read", num(totals.get("tokens_cache_read"))),
        ("cache_write", num(totals.get("tokens_cache_write"))),
    )
    base_sum = sum(value for _key, value in buckets)
    if base_sum <= 0:
        return stats
    parts = {}
    allocated = 0
    for key, value in buckets:
        share = int(round(amount * value / base_sum))
        parts[key] = share
        allocated += share
    diff = amount - allocated
    if diff:
        # rounding leftovers go to the last nonzero bucket so the sum is exact
        for key, _value in reversed(buckets):
            if parts[key] > 0:
                parts[key] += diff
                break
        else:
            parts["input"] += diff
    cache_total = parts["cache_read"] + parts["cache_write"]

    recorded_total = num(totals.get("tokens_total"))
    if not recorded_total:
        recorded_total = sum(value for _key, value in buckets)
    recorded_cache = num(totals.get("tokens_cache_total"))
    if not recorded_cache:
        recorded_cache = num(totals.get("tokens_cache_read")) + num(
            totals.get("tokens_cache_write")
        )

    stats["recovered"] = {
        "tokens_total": amount,
        "tokens_input": parts["input"],
        "tokens_output": parts["output"],
        "tokens_reasoning": parts["reasoning"],
        "tokens_cache": cache_total,
        "included_in_totals": False,
    }
    stats["lifetime"] = {
        "tokens_total": recorded_total + amount,
        "tokens_input": num(totals.get("tokens_input")) + parts["input"],
        "tokens_output": num(totals.get("tokens_output")) + parts["output"],
        "tokens_reasoning": num(totals.get("tokens_reasoning")) + parts["reasoning"],
        "tokens_cache": recorded_cache + cache_total,
        "cost": num(totals.get("cost")),
        "recorded_total": recorded_total,
        "recovered_total": amount,
    }

    insights = stats.get("insights")
    if isinstance(insights, list):
        note = (
            f"Recovered history: {amount:,.0f} tokens from deleted sessions — "
            f"lifetime metadata only, excluded from recorded totals and daily charts."
        )
        if note not in insights:
            insights.insert(1 if insights else 0, note)
    return stats


def cached_local_stats(db_path):
    path = Path(db_path)
    try:
        path.stat()
    except OSError:
        raise RuntimeError(
            f"Database not found: {db_path}\nRun OpenCode at least once to create it."
        ) from None
    # Git HEADs join the fingerprint so fresh commits refresh the per-commit
    # panel even when the database itself is untouched; the recovered-token
    # setting joins too so changing it invalidates cached stats.
    fingerprint = (_fingerprint_paths([path]), repo_heads(db_worktrees(db_path)),
                   recovered_tokens_for(db_path))
    with LOCK:
        if LOCAL_CACHE.get("key") == fingerprint and LOCAL_CACHE.get("stats") is not None:
            return LOCAL_CACHE["stats"]
    con = connect(db_path)
    try:
        synced = sync_session_ledger(db_path, con)
        stats = query_stats(con)
    finally:
        con.close()
    if synced:
        try:
            stats = merge_ledger_into_stats(stats, db_path)
        except Exception:
            pass
    try:
        amount = recovered_tokens_for(db_path)
        if amount:
            stats = apply_recovered_adjustment(stats, amount)
    except Exception:
        pass
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


def _cline_day_hour(ts_ms):
    """Epoch-ms -> (local date, local hour); (None, None) when unusable."""
    try:
        if not ts_ms:
            return None, None
        dt = datetime.fromtimestamp(float(ts_ms) / 1000)
        return dt.date().isoformat(), dt.hour
    except (TypeError, ValueError, OverflowError, OSError):
        return None, None


def _cline_time_key(ts_ms):
    try:
        if not ts_ms:
            return 0.0
        return float(ts_ms) / 1000
    except (TypeError, ValueError):
        return 0.0


def _cline_is_free(model):
    m = str(model or "")
    return m.endswith("-free") or m.startswith("cline-free/")


def parse_cline_messages(cline_dir, limit=MAX_CLINE_MESSAGES):
    """Scan Cline Desktop sessions into normalized per-message event dicts.

    Source: <cline_dir>/sessions/*/<id>.messages.json (assistant messages with
    `metrics: {inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens}`)
    joined with <cline_dir>/db/sessions.db for provider/model/cwd fallback.
    Each event: {at, ts, date, hour, model, short, provider, ti, cache, to,
    total, session_id, cwd}. Missing-metric assistant rows are unmetered and
    skipped by callers (never estimated).
    """
    base = Path(cline_dir)
    meta = {}
    db_path = base / "db" / "sessions.db"
    try:
        con = connect(db_path)
        try:
            for r in con.execute(
                "SELECT session_id, provider, model, cwd, workspace_root,"
                " started_at, prompt FROM sessions"
            ):
                meta[r["session_id"]] = {
                    "provider": r["provider"] or "cline",
                    "model": r["model"] or "—",
                    "cwd": r["workspace_root"] or r["cwd"] or "",
                    "started_at": r["started_at"],
                    "prompt": r["prompt"] or "",
                }
        finally:
            con.close()
    except Exception:
        meta = {}
    sess_dir = base / "sessions"
    try:
        session_dirs = sorted(
            [p for p in sess_dir.iterdir() if p.is_dir()],
            key=lambda p: p.stat().st_mtime_ns,
        )
    except OSError:
        raise RuntimeError(f"Cline sessions dir not found: {sess_dir}")
    events = []
    unmetered = 0
    for sdir in session_dirs:
        sid = sdir.name
        info = meta.get(sid, {})
        msg_files = sorted(sdir.glob("*.messages.json"))
        if not msg_files:
            continue
        try:
            with open(msg_files[0], "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
        except (OSError, ValueError):
            continue
        msgs = data.get("messages") if isinstance(data, dict) else None
        if not isinstance(msgs, list):
            continue
        for m in msgs:
            if not isinstance(m, dict) or m.get("role") != "assistant":
                continue
            metrics = m.get("metrics") or {}
            ti = num(metrics.get("inputTokens"))
            to = num(metrics.get("outputTokens"))
            cr = num(metrics.get("cacheReadTokens"))
            cw = num(metrics.get("cacheWriteTokens"))
            total = ti + to + cr + cw
            model_info = m.get("modelInfo") or {}
            model = model_info.get("id") or info.get("model") or "—"
            provider = model_info.get("provider") or info.get("provider") or "cline"
            ts = m.get("ts")
            if not ts and info.get("started_at"):
                try:
                    s = str(info["started_at"]).strip()
                    if s.endswith("Z"):
                        s = s[:-1] + "+00:00"
                    ts = datetime.fromisoformat(s).timestamp() * 1000
                except (ValueError, TypeError, OverflowError):
                    ts = 0
            date, hour = _cline_day_hour(ts)
            at = None
            try:
                if ts:
                    at = datetime.fromtimestamp(float(ts) / 1000).astimezone().isoformat()
            except (TypeError, ValueError, OverflowError, OSError):
                at = None
            if not total:
                unmetered += 1
                continue
            events.append(
                {
                    "at": at,
                    "ts": ts or 0,
                    "date": date,
                    "hour": hour,
                    "model": str(model),
                    "short": router_short(model),
                    "provider": str(provider or "cline"),
                    "status": 200,
                    "ok": True,
                    "ti": ti,
                    "cache": cr + cw,
                    "to": to,
                    "total": total,
                    "ms": 0.0,
                    "free": _cline_is_free(model),
                    "session_id": sid,
                    "cwd": info.get("cwd") or "",
                    "prompt": (info.get("prompt") or "")[:120],
                }
            )
    events.sort(key=lambda e: (_cline_time_key(e.get("ts")), str(e.get("at") or "")))
    if limit and len(events) > limit:
        events = events[-limit:]
        truncated_scan = True
    else:
        truncated_scan = False
    return events, unmetered, truncated_scan


def query_cline_stats(cline_dir, worktrees=None):
    """Aggregate Cline Desktop usage into a Codex-shaped dashboard payload.

    Cache semantics are OpenCode-style (cache read+write is a separate stream,
    NOT a subset of input), so day totals are ti+to+cache everywhere here.
    """
    base = Path(cline_dir)
    if not (base / "sessions").is_dir():
        raise RuntimeError(f"Cline sessions dir not found: {base / 'sessions'}")
    events, unmetered, truncated_scan = parse_cline_messages(cline_dir)
    ti = sum(e["ti"] for e in events)
    cache = sum(e["cache"] for e in events)
    to = sum(e["to"] for e in events)

    by_model = {}
    for e in events:
        m = by_model.setdefault(
            e["model"],
            {"provider": e["provider"], "reqs": 0, "ok": 0, "err": 0,
             "ti": 0.0, "to": 0.0, "cache": 0.0, "total": 0.0},
        )
        m["reqs"] += 1
        m["ok"] += 1
        m["ti"] += e["ti"]
        m["to"] += e["to"]
        m["cache"] += e["cache"]
        m["total"] += e["total"]
    model_rows = []
    for name, m in by_model.items():
        tot = m["total"]
        model_rows.append(
            [name, m["reqs"], tot, m["ti"], m["to"], m["cache"], m["ok"], m["err"],
             round(tot / m["ok"], 0) if m["ok"] else 0,
             cache_rate(m["ti"], m["cache"]), m["provider"], _cline_is_free(name), 0.0]
        )
    model_rows.sort(key=lambda r: -r[2])

    by_provider = defaultdict(lambda: {"reqs": 0, "toks": 0.0})
    for e in events:
        by_provider[e["provider"]]["reqs"] += 1
        by_provider[e["provider"]]["toks"] += e["total"]

    by_session = {}
    for e in events:
        s = by_session.setdefault(
            e["session_id"],
            {"model": e["model"], "provider": e["provider"], "cwd": e["cwd"],
             "prompt": e["prompt"], "ts": 0, "at": None, "msgs": 0,
             "ti": 0.0, "to": 0.0, "cache": 0.0, "total": 0.0},
        )
        s["msgs"] += 1
        s["ti"] += e["ti"]
        s["to"] += e["to"]
        s["cache"] += e["cache"]
        s["total"] += e["total"]
        if _cline_time_key(e.get("ts")) >= _cline_time_key(s["ts"]):
            s["ts"] = e.get("ts") or s["ts"]
            s["at"] = e.get("at") or s["at"]
            s["model"] = e["model"]
            s["provider"] = e["provider"]
    sessions = [
        {"id": sid, "title": (v["prompt"] or sid)[:80] or sid,
         "agent": "cline", "model": v["model"], "provider": v["provider"],
         "time_created": v["ts"] or 0, "at": v["at"],
         "worktree": short_dir(v["cwd"]), "msgs": v["msgs"],
         "tokens_input": v["ti"], "tokens_output": v["to"],
         "tokens_reasoning": 0, "tokens_cache": v["cache"],
         "tokens_total": v["total"]}
        for sid, v in by_session.items()
    ]
    sessions.sort(key=lambda s: s["time_created"], reverse=True)

    day_buckets = {}
    hour_reqs = defaultdict(int)
    hour_tokens = defaultdict(float)
    for e in events:
        if e["hour"] is not None:
            hour_reqs[e["hour"]] += 1
            hour_tokens[e["hour"]] += e["total"]
        if not e["date"]:
            continue
        b = day_buckets.setdefault(
            e["date"], {"reqs": 0, "ok": 0, "err": 0, "ti": 0.0, "to": 0.0, "cache": 0.0}
        )
        b["reqs"] += 1
        b["ok"] += 1
        b["ti"] += e["ti"]
        b["to"] += e["to"]
        b["cache"] += e["cache"]
    day_entries = []
    for d in sorted(day_buckets):
        b = day_buckets[d]
        day_entries.append(
            {"date": d, "msgs": b["reqs"], "sessions": b["ok"], "reqs": b["reqs"],
             "ok": b["ok"], "err": 0, "ti": round(b["ti"], 1),
             "to": round(b["to"], 1), "tr": 0, "cache": round(b["cache"], 1),
             "cost": 0, "total": round(b["ti"] + b["to"] + b["cache"], 1)}
        )
    activity = pad_activity(day_entries)
    for slot in activity:
        slot["total"] = round(slot["ti"] + slot["to"] + slot["tr"] + slot["cache"], 1)
        slot.setdefault("reqs", slot.get("msgs", 0))

    recent = sorted(events, key=lambda e: (_cline_time_key(e.get("ts")), str(e.get("at") or "")),
                    reverse=True)[:MAX_CLINE_ROWS]

    ok_times = sorted(
        (_cline_time_key(e.get("ts")) * 1000, e["total"])
        for e in events if _cline_time_key(e.get("ts")) > 0
    )
    window_ms = COMMIT_WINDOW_HOURS * 3600 * 1000
    c_commit_rows = []
    for c in collect_repo_commits(worktrees or []):
        lo = c["time"] - window_ms
        toks = reqs = 0
        for t_ms, tot in ok_times:
            if t_ms > c["time"]:
                break
            if t_ms >= lo:
                toks += tot
                reqs += 1
        c_commit_rows.append(
            [c["sha"], c["subject"][:72], c["date"], c["project"],
             c["files"], c["add"], c["del"], round(toks, 1), reqs]
        )
    c_commit_rows.sort(key=lambda r: -r[7])
    c_commit_rows = c_commit_rows[:MAX_COMMIT_ROWS]

    totals = {
        "requests": len(events),
        "ok": len(events),
        "errors": 0,
        "err429": 0,
        "err500": 0,
        "unmetered": unmetered,
        "tokens_input": ti,
        "tokens_output": to,
        "tokens_reasoning": 0,
        "tokens_cache_read": cache,
        "tokens_cache_write": 0,
        "tokens_total": ti + to + cache,
        "cache_rate": cache_rate(ti, cache),
        "models": len(by_model),
        "providers": len(by_provider),
        "what_if_cost": 0,
        "avg_ms": 0,
        "sessions": len(by_session),
        "messages": len(events),
        "cost": 0,
        "avg_duration_min": 0,
        "avg_tokens_per_session": round((ti + to + cache) / len(by_session), 0) if by_session else 0,
        "avg_tokens_per_metered": round((ti + to + cache) / len(events), 0) if events else 0,
    }
    biggest_day = max(day_entries, key=lambda d: d["total"]) if day_entries else None
    top_model = model_rows[0] if model_rows else None
    biggest_req = max(events, key=lambda e: e["total"]) if events else None
    biggest_sess = max(sessions, key=lambda s: s["tokens_total"]) if sessions else None
    peak_hour = max(range(24), key=lambda h: hour_tokens.get(h, 0)) if any(hour_tokens.values()) else None
    insights = [
        f"Cline ran {len(events):,} assistant messages across {len(by_session)} sessions"
        f" and {len(by_model)} models.",
        (f"Top model: {router_short(top_model[0])} — {top_model[2]:,.0f} tokens over {top_model[1]:,} messages."
         if top_model else ""),
        (f"Busiest day: {biggest_day['date']} — {biggest_day['total']:,.0f} tokens, {biggest_day['reqs']} messages."
         if biggest_day else "No Cline activity recorded yet."),
    ]
    if unmetered:
        insights.append(
            f"{unmetered:,} assistant messages reported no token counts — excluded from all counts."
        )
    if truncated_scan:
        insights.append(
            f"Showing the most recent {len(events):,} of the scanned Cline messages — totals cover that window only."
        )
    insights = [i for i in insights if i]
    return {
        "source": "cline",
        "source_label": "Cline data",
        "totals": totals,
        "models": model_rows,
        "providers": sorted([[k, v["reqs"], round(v["toks"], 1)] for k, v in by_provider.items()],
                            key=lambda kv: -kv[2]),
        "sessions": sessions,
        "days": day_entries,
        "hours": [hour_reqs.get(h, 0) for h in range(24)],
        "commits": c_commit_rows,
        "activity": activity,
        "requests": recent,
        "insights": insights,
        "records": {
            "biggest_day": {"date": biggest_day["date"], "total": biggest_day["total"],
                            "sessions": biggest_day["reqs"]} if biggest_day else None,
            "biggest_session": {"title": biggest_sess["title"], "total": biggest_sess["tokens_total"],
                                "model": biggest_sess["model"]} if biggest_sess else None,
            "fastest_burn": None,
            "biggest_model": {"name": top_model[0], "total": top_model[2],
                              "requests": top_model[1]} if top_model else None,
            "biggest_request": {"at": biggest_req["at"], "model": biggest_req["model"],
                                "total": biggest_req["total"]} if biggest_req else None,
            "peak_hour": peak_hour,
            "streaks": compute_streaks(activity),
        },
        "range": f"{day_entries[0]['date']} → {day_entries[-1]['date']}" if day_entries else "no activity",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scanned": len(events),
        "truncated": truncated_scan,
    }


def blank_cline_stats(error=None):
    """Well-formed empty Cline payload (same shape as query_cline_stats)."""
    now = datetime.now().isoformat(timespec="seconds")
    stats = {
        "source": "cline",
        "source_label": "Cline data",
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
        "sessions": [],
        "days": [],
        "hours": [0] * 24,
        "commits": [],
        "activity": zero_days_window(),
        "requests": [],
        "insights": ["No Cline activity found."],
        "records": {"biggest_day": None, "biggest_session": None,
                    "fastest_burn": None, "biggest_model": None,
                    "biggest_request": None, "peak_hour": None,
                    "streaks": {"current": 0, "longest": 0}},
        "range": "no activity",
        "generated_at": now,
        "scanned": 0,
        "truncated": False,
    }
    if error:
        stats["error"] = error
    return stats


def _cline_fingerprint(cline_dir):
    base = Path(cline_dir)
    fps = []
    db = base / "db" / "sessions.db"
    for q in [db, Path(str(db) + "-wal"), Path(str(db) + "-shm")]:
        try:
            st = q.stat()
            fps.append((str(q), st.st_mtime_ns, st.st_size))
        except OSError:
            fps.append((str(q), None, None))
    newest = (None, None)
    try:
        for p in (base / "sessions").glob("*/*.messages.json"):
            try:
                st = p.stat()
                if newest[0] is None or st.st_mtime_ns > newest[0]:
                    newest = (st.st_mtime_ns, st.st_size)
            except OSError:
                continue
    except OSError:
        pass
    fps.append(("cline-newest-messages", newest[0], newest[1]))
    return tuple(fps)


def cached_cline_stats(cline_dir, worktrees=None):
    try:
        fingerprint = (_cline_fingerprint(cline_dir), repo_heads(worktrees))
    except OSError:
        raise RuntimeError(f"Cline sessions dir not found: {Path(cline_dir) / 'sessions'}")
    with LOCK:
        if CLINE_CACHE.get("key") == fingerprint and CLINE_CACHE.get("stats") is not None:
            return CLINE_CACHE["stats"]
    stats = query_cline_stats(cline_dir, worktrees)
    with LOCK:
        CLINE_CACHE["key"] = fingerprint
        CLINE_CACHE["stats"] = stats
    return stats


def session_prompts(con, session_id):
    """Prompts for one session — from session_input / session_message when
    available, otherwise reconstructed from user text parts."""
    # Legacy session_input table (OpenCode 1.0).
    try:
        if has_table(con, "session_input"):
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
    except sqlite3.Error:
        pass
    # OpenCode 2.0: user messages live in session_message with data.text.
    try:
        if has_table(con, "session_message"):
            rows = []
            for r in con.execute(
                "SELECT time_created, json_extract(data,'$.text') AS txt "
                "FROM session_message WHERE session_id=? AND type='user' "
                "ORDER BY time_created",
                (session_id,),
            ):
                txt = (r["txt"] or "").strip()
                if txt:
                    rows.append({"t": r["time_created"], "p": txt})
            if rows:
                return rows[-50:]
    except sqlite3.Error:
        pass
    # Fallback: user text parts.
    MT = message_table(con)
    # 2.0 messages carry a `type` column ('user'/'assistant'); 1.0 messages
    # carry data.role instead. Pick the right user check for the active schema.
    user_check = (
        "m.type='user'"
        if MT == "session_message"
        else "json_extract(m.data,'$.role')='user'"
    )
    rows = []
    try:
        for r in con.execute(
            f"""
            SELECT p.time_created, json_extract(p.data,'$.text') AS txt
            FROM part p LEFT JOIN {MT} m ON m.id = p.message_id
            WHERE p.session_id = ?
              AND json_extract(p.data,'$.type')='text'
              AND (m.id IS NULL OR {user_check})
            ORDER BY p.time_created
            """,
            (session_id,),
        ):
            txt = (r["txt"] or "").strip()
            if txt:
                rows.append({"t": r["time_created"], "p": txt})
    except sqlite3.Error:
        pass
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
  --topbar-h:59px;
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
/* app shell */
.app{display:flex;align-items:flex-start;min-height:100vh}
.main{flex:1;min-width:0;display:flex;flex-direction:column;min-height:100vh}
.wrap{width:100%;max-width:1240px;margin:0 auto;padding:0 28px 70px}
.mono{font-family:var(--mono)}
.skip{position:fixed;left:12px;top:-64px;z-index:300;background:var(--panel);border:1px solid var(--accent);
  color:var(--text);border-radius:9px;padding:9px 14px;font-weight:700;text-decoration:none;box-shadow:var(--shadow);
  transition:top .15s ease}
.skip:focus{top:12px}

/* rail */
.rail{position:sticky;top:0;z-index:45;width:232px;flex:0 0 232px;height:100vh;display:flex;flex-direction:column;
  gap:10px;padding:18px 14px 14px;background:var(--panel2);border-right:1px solid var(--line);overflow:hidden}
.rail-brand{display:flex;align-items:center;gap:10px;padding:6px 8px 14px;color:var(--text);text-decoration:none;
  border-radius:10px;min-width:0}
.rail-brand:hover{background:var(--chip)}
.rail-brand:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
.rail-logo{width:30px;height:30px;flex:0 0 30px;border-radius:9px;background:linear-gradient(135deg,var(--accent),var(--accent2));
  display:flex;align-items:center;justify-content:center;gap:2px;box-shadow:var(--shadow)}
.rail-logo i{display:block;width:3px;border-radius:2px;background:#fff;opacity:.92}
.rail-logo i:nth-child(1){height:9px}.rail-logo i:nth-child(2){height:15px}.rail-logo i:nth-child(3){height:12px}
.rail-brand-txt{display:flex;flex-direction:column;line-height:1.15;min-width:0}
.rail-brand-txt b{font-size:14.5px;font-weight:800;letter-spacing:-.02em;white-space:nowrap}
.rail-brand-txt small{font-size:10.5px;color:var(--subtle);font-weight:600;letter-spacing:.03em;white-space:nowrap}
.rail-scroll{flex:1;min-height:0;overflow-y:auto;overflow-x:hidden;scrollbar-width:thin}
.rail-nav{display:flex;flex-direction:column;gap:2px}
.rail-kicker{font-family:var(--mono);font-size:9.5px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;
  color:var(--subtle);padding:12px 10px 6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.rail-item{appearance:none;width:100%;display:flex;align-items:center;gap:10px;background:transparent;border:1px solid transparent;
  color:var(--muted);border-radius:9px;padding:8px 10px;font-family:var(--sans);font-size:13px;font-weight:600;
  cursor:pointer;text-align:left;transition:background .12s,color .12s,border-color .12s;min-width:0}
.rail-item:hover{background:var(--chip);color:var(--text)}
.rail-item.active{background:color-mix(in srgb,var(--accent) 10%,transparent);border-color:color-mix(in srgb,var(--accent) 26%,transparent);
  color:var(--accent)}
.rail-item:focus-visible{outline:2px solid var(--accent);outline-offset:1px}
.ri-ico{width:22px;flex:0 0 22px;display:inline-flex;align-items:center;justify-content:center;font-family:var(--mono);
  font-size:10.5px;font-weight:800;letter-spacing:-.04em}
.view-link .ri-ico{font-size:13px;font-weight:700}
.ri-label{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ri-cnt{font-family:var(--mono);font-size:9.5px;font-weight:600;color:var(--subtle);white-space:nowrap;max-width:78px;
  overflow:hidden;text-overflow:ellipsis}
.rail-item.active .ri-cnt{color:var(--accent)}
.rail-div{height:1px;background:var(--line);margin:10px 8px}
.rail-foot{display:flex;flex-direction:column;gap:2px;border-top:1px solid var(--line);padding-top:10px;flex:0 0 auto}
.rail-item kbd{font-family:var(--mono);font-size:9.5px;color:var(--subtle);border:1px solid var(--line);border-radius:5px;
  padding:1px 4px;background:var(--bg);white-space:nowrap}

/* top bar */
.topbar{position:sticky;top:0;z-index:40;background:var(--bg);border-bottom:1px solid var(--line)}
.topbar-in{max-width:1240px;margin:0 auto;padding:12px 28px;display:flex;align-items:center;gap:12px;flex-wrap:nowrap;min-width:0}
.crumbs{display:flex;align-items:center;gap:8px;min-width:0;font-size:13px;font-weight:700;letter-spacing:-.01em;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.crumbs .crumb-h{color:var(--text)}
.crumbs .crumb-sep{color:var(--subtle);font-family:var(--mono);font-weight:500}
.crumbs .crumb-v{color:var(--muted);overflow:hidden;text-overflow:ellipsis}
.spacer{flex:1}
.tbtn{appearance:none;background:var(--chip);border:1px solid var(--line);color:var(--muted);border-radius:9px;
  height:34px;padding:0 12px;font-family:var(--sans);font-size:12.5px;font-weight:600;cursor:pointer;transition:.15s;
  display:inline-flex;align-items:center;gap:7px;white-space:nowrap;flex:0 0 auto}
.tbtn:hover{color:var(--text);border-color:var(--subtle)}
.tbtn:active{transform:scale(.97)}
.tbtn:disabled{opacity:.5;cursor:wait}
.tbtn[aria-expanded="true"]{border-color:var(--accent);color:var(--text)}
.tico{font-size:13px;line-height:1}
#status{font-family:var(--mono);font-size:11px;color:var(--subtle);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}

/* mobile navigation strips */
.mobile-nav{display:none}
.subnav,.source-tabs{display:flex;gap:8px;flex-wrap:nowrap;overflow-x:auto;overflow-y:hidden;
  scrollbar-width:none;-ms-overflow-style:none;max-width:100%;min-width:0}
.subnav::-webkit-scrollbar,.source-tabs::-webkit-scrollbar{display:none}
.subnav{padding:9px 16px 0}
.subnav-btn{appearance:none;flex:0 0 auto;background:var(--panel);border:1px solid var(--line);color:var(--muted);
  border-radius:999px;padding:6px 13px;font-family:var(--sans);font-size:12px;font-weight:600;cursor:pointer;
  white-space:nowrap;transition:.15s}
.subnav-btn:hover{color:var(--text);border-color:var(--subtle)}
.subnav-btn.active{background:color-mix(in srgb,var(--accent) 12%,transparent);border-color:var(--accent);color:var(--accent)}
.harness-bar{display:none;border-bottom:1px solid var(--line);background:var(--bg)}
.source-tabs{max-width:1240px;margin:0 auto;padding:9px 28px 0}
.stab{white-space:nowrap;flex:0 0 auto}
.stab .cnt{white-space:nowrap;display:inline-block}

/* sub-views */
.subview{display:none}
.subview.active{display:block}
[hidden]{display:none!important}
.subview:not([data-sub="overview"])>.sec:first-child{margin-top:28px}
.subview>.hero:first-child+.kpis{margin-top:22px}

/* hero */
.hero{padding:52px 0 14px}
.hero .eyebrow{font-family:var(--mono);font-size:11.5px;color:var(--subtle);letter-spacing:.12em;text-transform:uppercase}
.hero h1{font-size:clamp(26px,4vw,38px);font-weight:800;letter-spacing:-.035em;line-height:1.12;margin-top:10px;max-width:720px}
.hero h1 b{font-weight:800;background:linear-gradient(100deg,var(--accent),var(--accent2));-webkit-background-clip:text;background-clip:text;color:transparent}
.hero .pills{display:flex;flex-wrap:wrap;gap:8px;margin-top:18px}
.pill{font-family:var(--mono);font-size:11px;background:var(--panel);border:1px solid var(--line);color:var(--muted);
  padding:5px 12px;border-radius:999px}
.hero-note{font-family:var(--mono);font-size:11.5px;color:var(--subtle);margin-top:10px;line-height:1.6}
.hero-note b{color:var(--text);font-weight:700}
.recovered-note{font-size:12px;color:var(--muted);line-height:1.6;background:var(--panel);border:1px dashed var(--line);
  border-radius:12px;padding:12px 15px;margin-top:12px}
.recovered-note b{color:var(--text)}
.focus-note{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:9px 18px;
  border-bottom:1px solid var(--line);background:color-mix(in srgb,var(--accent) 7%,var(--bg));
  font-size:12.5px;color:var(--muted)}
.focus-note[hidden]{display:none}
.focus-note button{appearance:none;background:transparent;border:1px solid var(--line);border-radius:7px;
  color:var(--muted);font-size:12px;line-height:1;padding:5px 8px;cursor:pointer}
.focus-note button:hover{background:var(--chip);color:var(--text)}

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
h2.sec{font-size:17px;font-weight:700;letter-spacing:-.02em;margin:34px 0 14px;display:flex;align-items:baseline;gap:10px;
  scroll-margin-top:calc(var(--topbar-h) + 14px)}
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
.search:focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-color:var(--accent)}
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
#search:focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-color:var(--accent)}
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

/* command palette (top-centred dialog, outside every view) */
.palette-overlay{position:fixed;inset:0;z-index:200;background:rgba(0,0,0,.42);backdrop-filter:blur(4px);
  display:flex;align-items:flex-start;justify-content:center;padding:11vh 16px 18px}
.palette{width:min(640px,100%);max-height:min(72vh,600px);display:flex;flex-direction:column;background:var(--panel);
  border:1px solid var(--line);border-radius:16px;box-shadow:var(--shadow);overflow:hidden;animation:up .16s ease}
.palette-search{display:flex;align-items:center;gap:10px;padding:15px 18px;border-bottom:1px solid var(--line)}
.palette-search span{font-size:15px;color:var(--subtle)}
.palette-search input{flex:1;min-width:0;border:none;outline:none;background:transparent;color:var(--text);
  font-family:var(--sans);font-size:15px}
.palette-search input::placeholder{color:var(--subtle)}
.palette-search input:focus-visible{outline:2px solid var(--accent);outline-offset:3px;border-radius:4px}
.palette-list{overflow-y:auto;padding:8px;flex:1;min-height:0}
.pal-group{margin-bottom:4px}
.pal-group-label{font-family:var(--mono);font-size:9.5px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;
  color:var(--subtle);padding:9px 12px 5px}
.pal-item{appearance:none;display:flex;align-items:center;gap:12px;width:100%;text-align:left;background:transparent;
  border:1px solid transparent;border-radius:9px;padding:9px 12px;color:var(--text);font-family:var(--sans);
  font-size:13px;cursor:pointer;min-width:0}
.pal-item:hover,.pal-item.active{background:color-mix(in srgb,var(--accent) 10%,transparent);
  border-color:color-mix(in srgb,var(--accent) 26%,transparent)}
.pal-main{flex:1;min-width:0;display:flex;flex-direction:column;gap:1px}
.pal-label{font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.pal-hint{font-size:11px;color:var(--subtle);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.pal-key{font-family:var(--mono);font-size:9.5px;color:var(--subtle);border:1px solid var(--line);border-radius:5px;
  padding:2px 5px;white-space:nowrap;flex:0 0 auto}
.pal-empty{padding:26px;text-align:center;color:var(--subtle);font-size:13px}
.palette-foot{border-top:1px solid var(--line);padding:9px 16px;display:flex;gap:16px;align-items:center;
  font-family:var(--mono);font-size:10px;color:var(--subtle);white-space:nowrap;overflow:hidden}
.palette-foot b{color:var(--muted);font-weight:700}
.palette-close{appearance:none;background:var(--chip);border:1px solid var(--line);color:var(--muted);border-radius:7px;
  width:26px;height:26px;cursor:pointer;font-size:12px;flex:0 0 auto}
.palette-close:hover{color:var(--text)}
.help-modal{max-width:560px}
.help-modal h2{font-size:18px;letter-spacing:-.02em;margin-bottom:6px}
.help-modal p{color:var(--muted);font-size:12.5px;margin-bottom:14px}
.shortcuts{display:grid;gap:8px;margin-top:6px}
.shortcut{display:flex;align-items:center;justify-content:space-between;gap:18px;border-bottom:1px solid var(--line2);
  padding:8px 0;font-size:12.5px;color:var(--muted)}
.shortcut kbd{font-family:var(--mono);font-size:10.5px;color:var(--text);background:var(--chip);border:1px solid var(--line);
  border-radius:6px;padding:2px 7px;white-space:nowrap}
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
.prompt{background:var(--bg);border:1px solid var(--line);border-left:3px solid var(--accent);border-radius:10px;
  padding:11px 14px;margin-bottom:10px;font-size:13px;color:var(--muted);line-height:1.55;white-space:pre-wrap;word-break:break-word}
.mmeta{display:flex;flex-wrap:wrap;gap:7px;margin:14px 0 4px}
.empty{color:var(--subtle);text-align:center;padding:26px;font-size:13px}
::-webkit-scrollbar{width:10px;height:10px}
::-webkit-scrollbar-thumb{background:var(--line);border-radius:8px;border:2px solid var(--bg)}
::-webkit-scrollbar-thumb:hover{background:var(--subtle)}
/* source tab pills: only used by the narrow (<=860px) harness bar */
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
.pbtn[aria-pressed="true"]{background:color-mix(in srgb,var(--accent) 14%,transparent);border-color:var(--accent);color:var(--accent)}
@media(max-width:700px){.model-row{grid-template-columns:1fr;gap:6px}.model-row .mv{text-align:left}}
/* settings menu */
.setmenu{position:absolute;right:0;top:calc(100% + 6px);z-index:50;background:var(--panel);border:1px solid var(--line);
  border-radius:10px;box-shadow:var(--shadow);padding:8px;display:none;min-width:210px}
.setmenu.open{display:block}
.setmenu label{display:flex;align-items:center;gap:9px;font-size:13px;font-weight:600;color:var(--text);
  padding:8px 10px;border-radius:7px;cursor:pointer}
.setmenu label:hover{background:var(--chip)}
.setmenu input{accent-color:var(--accent);width:15px;height:15px}
.setmenu .shint{font-size:11px;color:var(--subtle);padding:6px 10px 4px;line-height:1.5}
/* recap tab (YT Shorts recording view) */
.recap-hero{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:26px 28px;
  box-shadow:var(--shadow);margin-top:22px;text-align:center}
.recap-date{font-family:var(--mono);font-size:12px;letter-spacing:.12em;color:var(--subtle);text-transform:uppercase}
.recap-big{font-family:var(--mono);font-size:clamp(44px,9vw,76px);font-weight:800;letter-spacing:-.04em;
  line-height:1.05;margin-top:8px;font-variant-numeric:tabular-nums}
.recap-big small{font-size:clamp(14px,2.5vw,20px);color:var(--subtle);font-weight:600;letter-spacing:0}
.recap-sub{font-size:13px;color:var(--muted);margin-top:8px}
.recap-split{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-top:16px}
.recap-card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:14px 16px;box-shadow:var(--shadow)}
.recap-card .rl{font-size:10px;font-weight:700;letter-spacing:.09em;text-transform:uppercase;color:var(--subtle)}
.recap-card .rv{font-family:var(--mono);font-size:24px;font-weight:700;margin-top:6px;font-variant-numeric:tabular-nums}
.recap-card .rs{font-size:11.5px;color:var(--muted);margin-top:4px}
.recap-controls{display:flex;gap:8px;flex-wrap:wrap;align-items:center;justify-content:center;margin-top:16px}
.recap-controls input[type=date]{background:var(--bg);border:1px solid var(--line);border-radius:9px;padding:7px 11px;
  font-family:var(--sans);font-size:12.5px;color:var(--text);outline:none}
/* responsive app shell */
@media(max-width:1280px){#status{display:none}}
@media(max-width:1080px){
  .rail{width:72px;flex-basis:72px;padding:16px 9px 12px}
  .rail-brand{justify-content:center;padding:4px 0 12px}
  .rail-brand-txt{display:none}
  .rail-kicker{display:none}
  .rail-item{justify-content:center;padding:9px 5px;gap:0}
  .ri-label,.ri-cnt,.rail-item kbd{display:none}
  .rail-div{margin:8px 4px}
  .topbar-in{padding:12px 20px}
  .source-tabs{padding:9px 20px 0}
  .wrap{padding:0 20px 60px}
}
@media(max-width:860px){
  html{scrollbar-width:none}
  html::-webkit-scrollbar{width:0;height:0}
  .app{display:block}
  .rail{display:none}
  .main{min-height:100vh}
  .harness-bar{display:block}
  .mobile-nav{display:block;border-bottom:1px solid var(--line);background:var(--bg)}
  .source-tabs{padding:8px 16px 0;max-width:none}
  .wrap{padding:0 16px 56px}
  .topbar-in{padding:10px 16px;gap:10px}
}
@media(max-width:640px){
  .topbar-in{flex-wrap:wrap;padding:8px 12px;gap:8px}
  .crumbs{flex:1 1 100%;min-width:0;font-size:12.5px}
  #status,#export-wrap{display:none}
  .tbtn{padding:0;width:36px;height:36px;justify-content:center}
  .btn-lbl{display:none}
  .tico{font-size:15px}
  .subnav{padding:8px 12px 0}
  .source-tabs{padding:8px 12px 0}
  .wrap{padding:0 14px 52px}
  .hero{padding-top:34px}
  .palette-overlay{padding:7vh 10px 12px}
  .palette-foot{gap:10px}
}
</style>
</head>
<body>
<a class="skip" href="#main">Skip to content</a>
<div class="app">
  <aside class="rail" aria-label="Primary navigation">
    <a class="rail-brand" id="rail-brand" href="#/opencode/overview" aria-label="OpenCode Dashboard home">
      <span class="rail-logo" aria-hidden="true"><i></i><i></i><i></i></span>
      <span class="rail-brand-txt"><b>OpenCode</b><small>usage</small></span>
    </a>
    <div class="rail-scroll">
      <nav class="rail-nav" aria-label="Harnesses">
        <div class="rail-kicker">Harnesses</div>
        <button class="rail-item active" id="rail-tab-opencode" data-harness="opencode" aria-controls="view-opencode" aria-current="page" title="OpenCode" aria-label="OpenCode">
          <span class="ri-ico" aria-hidden="true">OC</span><span class="ri-label">OpenCode</span><span class="ri-cnt" id="rail-cnt-opencode"></span>
        </button>
        <button class="rail-item" id="rail-tab-router" data-harness="router" aria-controls="view-router" title="Codex" aria-label="Codex">
          <span class="ri-ico" aria-hidden="true">CX</span><span class="ri-label">Codex</span><span class="ri-cnt" id="rail-cnt-router"></span>
        </button>
        <button class="rail-item" id="rail-tab-cline" data-harness="cline" aria-controls="view-cline" title="Cline" aria-label="Cline">
          <span class="ri-ico" aria-hidden="true">CL</span><span class="ri-label">Cline</span><span class="ri-cnt" id="rail-cnt-cline"></span>
        </button>
        <button class="rail-item" id="rail-tab-recap" data-harness="recap" aria-controls="view-recap" title="Recap" aria-label="Recap">
          <span class="ri-ico" aria-hidden="true">RC</span><span class="ri-label">Recap</span><span class="ri-cnt" id="rail-cnt-recap"></span>
        </button>
      </nav>
      <div class="rail-div"></div>
      <nav class="rail-nav" aria-label="Views">
        <div class="rail-kicker" id="rail-view-kicker">Views</div>
        <div id="rail-views"></div>
      </nav>
    </div>
    <div class="rail-foot">
      <button class="rail-item" id="rail-help" title="Help" aria-label="Help and keyboard shortcuts">
        <span class="ri-ico" aria-hidden="true">?</span><span class="ri-label">Help</span>
      </button>
      <button class="rail-item" id="rail-palette" title="Command palette (⌘K)" aria-label="Open command palette" aria-haspopup="dialog" aria-expanded="false">
        <span class="ri-ico" aria-hidden="true">⌘</span><span class="ri-label">Command</span><kbd>⌘K</kbd>
      </button>
    </div>
  </aside>
  <div class="main">
    <div class="topbar">
      <div class="topbar-in">
        <div class="crumbs" aria-label="Location"><span class="crumb-h" id="crumb-harness">OpenCode</span><span class="crumb-sep" aria-hidden="true">/</span><span class="crumb-v" id="crumb-view">Overview</span></div>
        <div class="spacer"></div>
        <span id="status" role="status" aria-live="polite"></span>
        <button class="tbtn" id="palette-btn" aria-label="Open command palette" aria-haspopup="dialog" aria-expanded="false" title="Command palette (⌘K)">
          <span class="tico" aria-hidden="true">⌘</span><span class="btn-lbl">Command</span>
        </button>
        <button class="tbtn" id="theme" aria-pressed="false" aria-label="Switch to dark theme" title="Toggle theme">
          <span class="tico" aria-hidden="true">◐</span><span class="btn-lbl">Light</span>
        </button>
        <span class="export-wrap" id="export-wrap"><button class="tbtn" id="export" aria-haspopup="menu" aria-expanded="false" aria-label="Export data" title="Export data">
          <span class="tico" aria-hidden="true">⤓</span><span class="btn-lbl">Export</span></button>
          <div class="expmenu" id="expmenu" role="menu" aria-label="Export formats">
            <button data-x="json" role="menuitem">JSON <small>full stats payload</small></button>
            <button data-x="csv" role="menuitem">CSV <small>per-day usage table</small></button>
            <button data-x="recap" role="menuitem">Recap CSV <small>all harnesses per day</small></button>
          </div></span>
        <span class="export-wrap"><button class="tbtn" id="settings" aria-haspopup="true" aria-expanded="false" aria-label="Harness settings" title="Harness settings">
          <span class="tico" aria-hidden="true">⚙</span><span class="btn-lbl">Harnesses</span></button>
          <div class="setmenu" id="setmenu" role="menu" aria-label="Harness toggles">
            <label><input type="checkbox" id="set-focus" checked> OpenCode focus mode — hide Codex, Cline and Recap</label>
            <div class="shint">Focus keeps the rail and palette OpenCode-first. Turn it off to show every harness.</div>
            <div class="rail-div" style="margin:8px 0"></div>
            <label><input type="checkbox" id="set-opencode" checked> OpenCode</label>
            <label><input type="checkbox" id="set-router" checked> Codex</label>
            <label><input type="checkbox" id="set-cline" checked> Cline</label>
            <div class="shint">Disabled harnesses hide their tab and are excluded from the Recap tab.</div>
          </div></span>
        <button class="tbtn" id="refresh" aria-label="Refresh data" title="Refresh data">
          <span class="tico" aria-hidden="true">⟳</span><span class="btn-lbl">Refresh</span></button>
      </div>
    </div>
    <div class="focus-note" id="focus-note" hidden>
      <span>Focus mode is on — this harness is hidden from the rail</span>
      <button type="button" id="focus-note-x" aria-label="Dismiss focus mode note">✕</button>
    </div>
    <div class="mobile-nav">
      <div class="subnav" id="subnav" role="tablist" aria-label="Views"></div>
    </div>
    <div class="harness-bar">
      <div class="source-tabs" role="tablist" aria-label="Data source">
        <button class="stab active" id="tab-opencode" role="tab" aria-selected="true" aria-controls="view-opencode" data-harness="opencode">OpenCode<span class="cnt" id="tab-opencode-cnt"></span></button>
        <button class="stab" id="tab-router" role="tab" aria-selected="false" aria-controls="view-router" data-harness="router">Codex<span class="cnt" id="tab-router-cnt"></span></button>
        <button class="stab" id="tab-cline" role="tab" aria-selected="false" aria-controls="view-cline" data-harness="cline">Cline<span class="cnt" id="tab-cline-cnt"></span></button>
        <button class="stab" id="tab-recap" role="tab" aria-selected="false" aria-controls="view-recap" data-harness="recap">Recap<span class="cnt" id="tab-recap-cnt"></span></button>
      </div>
    </div>
    <main class="wrap" id="main" tabindex="-1">
  <div id="view-opencode" role="tabpanel" aria-labelledby="tab-opencode">
  <div class="subview active" data-sub="overview">
  <section class="hero">
    <div class="eyebrow" id="range"></div>
    <h1 id="headline"></h1>
    <div class="pills" id="pills"></div>
    <div class="hero-note" id="hero-note"></div>
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

  <h2 class="sec">Notes on your usage <span class="hint" id="notes-hint">generated from your local data</span></h2>
  <section class="insights" id="insights"></section>

  <h2 class="sec" id="sec-tokens">Tokens per day <span class="hint">input · output incl. reasoning · cache · click a day to filter</span></h2>
  <div class="card chart-card">
    <div class="ctitle">Tokens per day <small id="tok-sub">stacked absolute</small></div>
    <div id="tok-chart"></div>
    <div class="legend" id="tok-legend"></div>
  </div>
  </div><!-- /subview overview -->

  <div class="subview" data-sub="code">
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
  </div><!-- /subview code -->

  <div class="subview" data-sub="models">
  <h2 class="sec" id="sec-env">Tokens by model <span class="hint">universal comparison · click a model to filter</span></h2>
  <div class="card" id="model-card"><div id="models"></div><div class="legend" id="model-legend"></div></div>
  <div class="recovered-note" id="recovered-note" hidden></div>
  <div class="grid2" style="margin-top:14px">
    <div class="card"><div class="ctitle">Projects <small>token leaderboard</small></div><div class="chips" id="projects"></div></div>
    <div class="card"><div class="ctitle">Agents <small>sessions · tokens</small></div><div class="chips" id="agents"></div></div>
  </div>
  </div><!-- /subview models -->

  <div class="subview" data-sub="sessions">
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
  </div><!-- /subview sessions -->

  <div class="subview" data-sub="activity">
  <h2 class="sec">Records <span class="hint">personal bests · token-only</span></h2>
  <section class="records" id="records"></section>

  <h2 class="sec" id="sec-heat">Activity <span class="hint">last 52 weeks · token intensity · click to filter</span></h2>
  <div class="card">
    <div class="act-head"><span class="sum" id="act-summary"></span></div>
    <div class="act-scroll"><div id="act"></div></div>
    <div class="act-legend" id="act-legend"></div>
  </div>
  </div><!-- /subview activity -->
  </div><!-- /view-opencode -->
  <div id="view-router" role="tabpanel" aria-labelledby="tab-router" hidden>
  <div class="subview active" data-sub="overview">
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

  <h2 class="sec">Notes on your usage <span class="hint" id="r-notes-hint">generated from your codex data</span></h2>
  <section class="insights" id="r-insights"></section>

  <h2 class="sec">Tokens per day <span class="hint">input · output · cache · all models</span></h2>
  <div class="card chart-card">
    <div class="ctitle">Tokens per day <small id="r-tok-sub">stacked absolute</small></div>
    <div id="r-tok-chart"></div>
    <div class="legend" id="r-tok-legend"></div>
  </div>
  </div><!-- /subview overview -->

  <div class="subview" data-sub="models">
  <h2 class="sec">Tokens by model <span class="hint">universal comparison · click a model to filter</span></h2>
  <div class="card" id="r-model-card"><div id="r-models"></div><div class="legend" id="r-model-legend"></div></div>
  <div class="grid2" style="margin-top:14px">
    <div class="card"><div class="ctitle">Providers <small>request leaderboard</small></div><div class="chips" id="r-providers"></div></div>
    <div class="card"><div class="ctitle">Status <small>ok · errors · click to filter</small></div><div class="chips" id="r-status"></div></div>
  </div>
  </div><!-- /subview models -->

  <div class="subview" data-sub="requests">
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
  </div><!-- /subview requests -->

  <div class="subview" data-sub="activity">
  <h2 class="sec">Records <span class="hint">personal bests · token-only</span></h2>
  <section class="records" id="r-records"></section>

  <h2 class="sec">Activity <span class="hint">last 52 weeks · token intensity · click to filter</span></h2>
  <div class="card">
    <div class="act-head"><span class="sum" id="r-act-summary"></span></div>
    <div class="act-scroll"><div id="r-act"></div></div>
    <div class="act-legend" id="r-act-legend"></div>
  </div>
  </div><!-- /subview activity -->
  </div><!-- /view-router -->
  <div id="view-cline" role="tabpanel" aria-labelledby="tab-cline" hidden>
  <div class="subview active" data-sub="overview">
  <section class="hero" style="padding-top:26px">
    <div class="eyebrow" id="c-range"></div>
    <h1 id="c-headline"></h1>
    <div class="pills" id="c-pills"></div>
    <section class="summary" id="c-summary" aria-label="Cline token summary"></section>
    <div class="presets" id="c-presets" role="group" aria-label="Cline time range">
      <span class="pl">Period</span>
      <button class="pbtn active" data-cr="all">All time</button>
      <button class="pbtn" data-cr="7">7 days</button>
      <button class="pbtn" data-cr="30">30 days</button>
      <button class="pbtn" data-cr="90">90 days</button>
      <button class="pbtn" data-cr="365">1 year</button>
      <span class="pl" style="margin-left:14px">View</span>
      <button class="pbtn" id="c-share-btn" aria-pressed="false" title="Show 100% share instead of absolute tokens">Share: off</button>
    </div>
  </section>
  <section class="kpis" id="c-kpis"></section>

  <h2 class="sec">Notes on your usage <span class="hint" id="c-notes-hint">generated from your cline data</span></h2>
  <section class="insights" id="c-insights"></section>

  <h2 class="sec">Tokens per day <span class="hint">input · output · cache · click a day to filter</span></h2>
  <div class="card chart-card">
    <div class="ctitle">Tokens per day <small id="c-tok-sub">stacked absolute</small></div>
    <div id="c-tok-chart"></div>
    <div class="legend" id="c-tok-legend"></div>
  </div>
  </div><!-- /subview overview -->

  <div class="subview" data-sub="models">
  <h2 class="sec">Tokens by model <span class="hint">universal comparison · click a model to filter</span></h2>
  <div class="card" id="c-model-card"><div id="c-models"></div><div class="legend" id="c-model-legend"></div></div>
  <div class="card" style="margin-top:14px"><div class="ctitle">Providers <small>message leaderboard</small></div><div class="chips" id="c-providers"></div></div>
  </div><!-- /subview models -->

  <div class="subview" data-sub="sessions">
  <h2 class="sec">Sessions <span class="hint" id="c-req-hint"></span></h2>
  <div class="card" style="margin-bottom:14px"><div class="ctitle">Sessions <small>click to filter</small></div><div class="chips" id="c-sessions"></div></div>
  <div class="card">
    <div class="tbl-tools">
      <input class="search" id="c-search" placeholder="Filter by title, model or directory…" aria-label="Filter cline sessions">
      <select class="filter" id="c-provider-f"><option value="">All providers</option></select>
      <select class="filter" id="c-model-f"><option value="">All models</option></select>
      <span class="badge" id="c-req-count"></span>
    </div>
    <div id="c-fbar" class="fbar" style="display:none"></div>
    <div class="tbl-scroll">
      <table class="tbl" id="c-tbl">
        <thead><tr>
          <th>Session</th><th>Model</th><th>Started</th>
          <th class="num">Input</th><th class="num">Output</th>
          <th class="num">Cache</th><th class="num">Total</th><th class="num">Msgs</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>
  </div><!-- /subview sessions -->

  <div class="subview" data-sub="activity">
  <h2 class="sec">Records <span class="hint">personal bests · token-only</span></h2>
  <section class="records" id="c-records"></section>

  <h2 class="sec">Activity <span class="hint">last 52 weeks · token intensity · click to filter</span></h2>
  <div class="card">
    <div class="act-head"><span class="sum" id="c-act-summary"></span></div>
    <div class="act-scroll"><div id="c-act"></div></div>
    <div class="act-legend" id="c-act-legend"></div>
  </div>
  </div><!-- /subview activity -->
  </div><!-- /view-cline -->
  <div id="view-recap" role="tabpanel" aria-labelledby="tab-recap" hidden>
  <div class="subview active" data-sub="recap">
  <section class="hero" style="padding-top:26px">
    <div class="eyebrow">RECAP · DAILY TOKENS FOR RECORDING</div>
    <h1>How many tokens did I use?</h1>
  </section>
  <div class="recap-hero">
    <div class="recap-date" id="recap-date"></div>
    <div class="recap-big" id="recap-big"></div>
    <div class="recap-sub" id="recap-sub"></div>
    <div class="recap-controls" role="group" aria-label="Recap date">
      <button class="pbtn active" data-recap="today">Today</button>
      <button class="pbtn" data-recap="yesterday">Yesterday</button>
      <input type="date" id="recap-picker" aria-label="Pick a recap date">
      <button class="pbtn" id="recap-copy">⧉ Copy for Shorts</button>
    </div>
  </div>
  <div class="recap-split" id="recap-split"></div>
  <h2 class="sec">Providers <span class="hint">enabled harnesses · selected date</span></h2>
  <div class="card"><div class="chips" id="recap-providers"></div></div>
  <h2 class="sec">Conversations <span class="hint" id="recap-conv-hint">top by tokens · selected date</span></h2>
  <div class="card"><div class="tbl-scroll"><table class="tbl" id="recap-tbl">
    <thead><tr><th>Conversation</th><th>Harness</th><th>Model</th><th class="num">Tokens</th></tr></thead>
    <tbody></tbody>
  </table></div></div>
  </div><!-- /subview recap -->
  </div><!-- /view-recap -->
    </main>
  </div><!-- /main -->
</div><!-- /app -->

<div class="overlay" id="overlay"><div class="modal" id="modal" role="dialog" aria-modal="true" aria-label="Session details"></div></div>
<div class="tip" id="tip"></div>
<div class="toasts" id="toasts"></div>
<div class="palette-overlay" id="palette" hidden>
  <div class="palette" role="dialog" aria-modal="true" aria-labelledby="palette-title">
    <div class="palette-search">
      <span aria-hidden="true">⌘</span>
      <input id="palette-input" type="text" autocomplete="off" spellcheck="false" placeholder="Search views, actions, sessions…" aria-label="Command palette search" aria-controls="palette-list" aria-autocomplete="list" role="combobox" aria-expanded="true">
      <button class="palette-close" id="palette-close" aria-label="Close command palette">✕</button>
    </div>
    <div class="palette-list" id="palette-list" role="listbox" aria-label="Commands">
      <div class="pal-empty">Type to search commands.</div>
    </div>
    <div class="palette-foot"><span><b>↑↓</b> navigate</span><span><b>↵</b> open</span><span><b>esc</b> close</span><span id="palette-title" style="margin-left:auto">Command palette</span></div>
  </div>
</div>
<div class="overlay" id="help-overlay">
  <div class="modal help-modal" id="help-modal" role="dialog" aria-modal="true" aria-labelledby="help-title">
    <button class="modal-x" id="help-close" aria-label="Close help">✕</button>
    <h2 id="help-title">Keyboard &amp; navigation</h2>
    <p>Move between harnesses and views without leaving the keyboard.</p>
    <div class="shortcuts">
      <div class="shortcut"><span>Open command palette</span><kbd>⌘K / Ctrl K</kbd></div>
      <div class="shortcut"><span>Move through palette results</span><kbd>↑ ↓</kbd></div>
      <div class="shortcut"><span>Open the highlighted result</span><kbd>↵</kbd></div>
      <div class="shortcut"><span>Close palette, help or session details</span><kbd>Esc</kbd></div>
      <div class="shortcut"><span>Jump to content</span><kbd>Tab</kbd></div>
    </div>
    <p style="margin-top:16px;margin-bottom:0">Tip: every view has a deep link — copy the address bar to reopen it later.</p>
  </div>
</div>

<script>const S0 = __DATA__; const R0 = __ROUTER__; const C0 = __CLINE__;</script>
<script>
const $=id=>document.getElementById(id);
const ESC=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const BOLD=s=>{const ents=[];const t=s.replace(/&(?:#\d+|[a-z]+);/g,m=>{ents.push(m);return'@@'+String.fromCharCode(65+ents.length-1)+'@@';});
  return t.replace(/(\d[\d,]*(?:\.\d+)?)/g,'<b>$1</b>').replace(/@@[A-Z]+@@/g,m=>ents[m.charCodeAt(2)-65]);};
const F=n=>{const s=Math.round(Number(n)||0).toString();return s.replace(/\B(?=(\d{3})+(?!\d))/g,',');};
const FN=n=>{n=Number(n)||0;return n>=1e6?(n/1e6).toFixed(2)+'M':n>=1e3?(n/1e3).toFixed(1)+'K':String(Math.round(n));};
const FB=n=>{n=Number(n)||0;return n>=1e9?(n/1e9).toFixed(2)+'B':n>=1e6?(n/1e6).toFixed(2)+'M':n>=1e3?(n/1e3).toFixed(1)+'K':String(Math.round(n));};
const DT=t=>{if(!t)return'—';const d=new Date(t>1e12?t:t*1000);return d.toLocaleDateString('en-CA')+' '+String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0');};
const DS=t=>{if(!t)return'';const d=new Date(t>1e12?t:t*1000);return d.toLocaleDateString('en-CA');};
const DUR=m=>{if(m==null)return'—';if(m<1)return'<1m';if(m<60)return Math.round(m)+'m';const h=Math.floor(m/60);return h+'h '+Math.round(m%60)+'m';};
const BURN=b=>{b=Number(b)||0;return b>=1000?(b/1000).toFixed(1)+'K/m':Math.round(b)+'/m';};
let S=null; if(typeof S0!=='undefined'&&S0){S=S0;}
let R=null; try{if(typeof R0!=='undefined'&&R0){R=R0;}}catch(_){R=null;}
let C=null; try{if(typeof C0!=='undefined'&&C0){C=C0;}}catch(_){C=null;}
let TAB='opencode';
let sortK='time_created', sortAsc=false;
let RANGE='all', SPLIT=false, SHARE=false;
let RRANGE='all', RSPLIT=false, RSHARE=false;
let CRANGE='all', CSHARE=false;
let FS='file', CS='toks', RFS='file', RCS='toks', CFS='file', CCS='toks';
let FS_EXP=false, CS_EXP=false, RFS_EXP=false, RCS_EXP=false, CFS_EXP=false, CCS_EXP=false;
let currentRows=[];
let currentRRows=[];
let currentCRows=[];
const RFSTATE={day:null,model:null,provider:null,status:''};
const CFSTATE={day:null,model:null,provider:null,session:null};
/* harness toggles (Settings gear) + OpenCode focus mode */
let FOCUS = true;
try{const f=localStorage.getItem('ocd-focus');if(f==='0')FOCUS=false;else if(f==='1')FOCUS=true;}catch(_){}
let HARNESS={opencode:true,router:false,cline:false};
let _savedHarness=null;
try{_savedHarness=JSON.parse(localStorage.getItem('ocd-harness')||'null');}catch(_){}
if(_savedHarness&&typeof _savedHarness==='object'){
  HARNESS={opencode:_savedHarness.opencode!==false,router:_savedHarness.router!==false,cline:_savedHarness.cline!==false};
}else if(!FOCUS){
  HARNESS={opencode:true,router:true,cline:true};
}
function harnessVisible(t){
  if(t==='opencode')return true;
  if(FOCUS)return false;
  if(t==='recap')return true;
  return !!HARNESS[t];
}
let RECAP_DATE=new Date().toLocaleDateString('en-CA');
/* navigation model: one VIEWS entry per harness, hash-routable with #/slug/view */
const VIEWS={
  opencode:[{id:'overview',label:'Overview',icon:'◫'},{id:'models',label:'Models',icon:'◈'},{id:'code',label:'Code',icon:'</>'},{id:'sessions',label:'Sessions',icon:'▤'},{id:'activity',label:'Activity',icon:'▦'}],
  router:[{id:'overview',label:'Overview',icon:'◫'},{id:'models',label:'Models',icon:'◈'},{id:'requests',label:'Requests',icon:'⇄'},{id:'activity',label:'Activity',icon:'▦'}],
  cline:[{id:'overview',label:'Overview',icon:'◫'},{id:'models',label:'Models',icon:'◈'},{id:'sessions',label:'Sessions',icon:'▤'},{id:'activity',label:'Activity',icon:'▦'}],
  recap:[{id:'recap',label:'Daily recap',icon:'◉'}]
};
const HARNESS_NAME={opencode:'OpenCode',router:'Codex',cline:'Cline',recap:'Recap'};
const TAB_ALIAS={opencode:'opencode',codex:'router',router:'router',cline:'cline',recap:'recap'};
const RANGE_VALUES=['7','30','90','365','all'];
const SUBVIEW={opencode:'overview',router:'overview',cline:'overview',recap:'recap'};
try{
  const sv=JSON.parse(sessionStorage.getItem('ocd-subview')||'null');
  if(sv&&typeof sv==='object'){
    Object.keys(SUBVIEW).forEach(k=>{
      if(sv[k]&&(VIEWS[k]||[]).some(v=>v.id===sv[k]))SUBVIEW[k]=sv[k];
    });
  }
}catch(_){}
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
function syncThemeBtn(){
  const dark=theme==='dark';
  const btn=$('theme');
  if(btn){
    btn.innerHTML='<span class="tico" aria-hidden="true">'+(dark?'☀':'◐')+'</span><span class="btn-lbl">'+(dark?'Light':'Dark')+'</span>';
    btn.setAttribute('aria-pressed',dark?'true':'false');
    btn.setAttribute('aria-label',dark?'Switch to light theme':'Switch to dark theme');
  }
}
syncThemeBtn();
function toggleTheme(){
  theme=theme==='dark'?'light':'dark';document.documentElement.dataset.theme=theme;
  localStorage.setItem('ocd-theme',theme);syncThemeBtn();renderAll();renderRouter();renderCline();if(TAB==='recap')renderRecap();
}
$('theme').onclick=toggleTheme;
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
      class:'inter'+dayDim(day),'data-tip':dayTip(day),'data-f':'day|'+day.date,tabindex:'0',role:'button',
      'aria-label':day.date+', '+FN(dayTotal(day))+' tokens, filter by day'});
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
  const t={sessions:0,msgs:0,ti:0,to:0,tr:0,cache:0,cache_read:0,total:0};
  list.forEach(d=>{t.sessions+=d.sessions;t.msgs+=d.msgs;t.ti+=d.ti;t.to+=d.to;t.tr+=d.tr;t.cache+=d.cache;t.cache_read+=d.cache_read||0;t.total+=dayTotal(d);});
  return t;
}
function deltaPct(cur,prev){if(prev==null)return null;if(prev===0)return cur>0?100:0;return Math.round((cur-prev)/prev*100);}
function deltaTag(cur,prev){const p=deltaPct(cur,prev);if(p==null)return'';
  return p===0?'<span class="delta flat">±0%</span>':p>0?'<span class="delta up">▲ '+p+'%</span>':'<span class="delta down">▼ '+Math.abs(p)+'%</span>';}
/* renderers */
function renderAll(){
  if(!S)return;
  renderHero();renderSummary();renderKpis();renderRecords();renderInsights();renderCharts();renderModels();renderProjAgents();renderAct();renderFilterBar();renderSessions();
  updateTabCounts();
}
function renderHero(){
  const periodLbl=RANGE==='all'?'':(RANGE==='365'?'12 MO':(RANGE/30>=1?Math.round(RANGE/30)+' MO':RANGE+' DAYS'));
  $('range').textContent='LOCAL · RECORDED '+S.range.toUpperCase()+(periodLbl?' · LAST '+periodLbl:'')+' · REFRESHES AUTOMATICALLY';
  $('headline').innerHTML=BOLD(ESC(S.insights[0]||'No activity yet.'));
  const last=[...S.sessions].sort((a,b)=>b.time_created-a.time_created)[0];
  const ltitle=last?ESC(last.title):'—';
  const nModels=S.models.length;
  const pills=[
    ['Recorded tokens',FN(S.totals.tokens_total||0)],
    ['Models',nModels>1?nModels+' models':ESC(((S.models[0]||['—'])[0]+'').split('/').pop())],
    ['Latest',ltitle.length>36?ltitle.slice(0,36)+'…':ltitle]
  ];
  if(S.recovered&&S.lifetime)pills.push(['Lifetime',FB(S.lifetime.tokens_total)]);
  $('pills').innerHTML=pills
    .map(([k,v])=>'<span class="pill">'+ESC(k)+' <b style="color:var(--text)">'+v+'</b></span>').join('');
  const note=$('hero-note');
  if(note){
    if(S.recovered&&S.lifetime){
      note.innerHTML='Lifetime '+FB(S.lifetime.tokens_total)+' = recorded '+FB(S.totals.tokens_total||0)+
        ' + recovered '+FB(S.recovered.tokens_total)+' · recovered history is metadata from deleted sessions and is excluded from recorded totals.';
    }else{
      note.textContent='';
    }
  }
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
    ['Today',t1.total,'','tokens','tokens'],
    ['Last 7 days',t7.total,deltaTag(t7.total,prev7.total),'tokens','tokens'],
    ['Last 30 days',t30.total,'','tokens','tokens'],
    ['All time',S.totals.tokens_total||0,'','tokens',
      (S.recovered&&S.lifetime)
        ? '+'+FB(S.recovered.tokens_total)+' recovered = '+FB(S.lifetime.tokens_total)+' lifetime'
        : 'recorded'],
  ];
  el.innerHTML=cards.map(([l,v,tag,unit,sub])=>'<div class="sum-card"><div class="sl">'+l+'</div><div class="sv">'+FN(v)+' '+unit+tag+'</div><div class="ss">'+ESC(sub||unit)+'</div></div>').join('');
}
function renderKpis(){
  const t=S.totals,c=T();
  const cur=RANGE==='all'?null:sumDays(periodDays());
  const prev=RANGE==='all'?null:sumDays(periodPrev());
  const pv=prev||{sessions:0,msgs:0,ti:0,to:0,tr:0,cache:0,total:0};
  const tok=cur
    ?{ti:cur.ti,out:cur.to+cur.tr,tr:cur.tr,ca:cur.cache,cr:cur.cache_read||0}
    :{ti:t.tokens_input,out:t.tokens_output+t.tokens_reasoning,tr:t.tokens_reasoning,
      ca:(t.tokens_cache_total||((t.tokens_cache_read||0)+(t.tokens_cache_write||0))),
      cr:t.tokens_cache_read||0};
  const total=tok.ti+tok.out+tok.ca;
  const pct=v=>total?Math.round(v/total*100):0;
  const row=(lbl,v,color,sub)=>'<div class="krow"><span class="lbl"><i class="dot" style="background:'+color+'"></i>'+lbl+
    (sub?' <span class="dim">'+sub+'</span>':'')+'</span><b>'+v+'</b></div>';
  const sessions=cur?cur.sessions:t.sessions;
  const msgs=cur?cur.msgs:t.messages;
  const tokTag=cur?deltaTag(total,pv.total):'';
  const actTag=cur?deltaTag(sessions+msgs,pv.sessions+pv.msgs):'';
  // Hit rate is read-only; the cached-token volume is read+write.
  const hitRate=(()=>{const d=(tok.ti||0)+(tok.cr||0);return d?Math.round(tok.cr/d*100):0;})();
  const tokCard='<div class="kpi"><h3>Tokens '+(SPLIT?'(reasoning split)':'(output merged)')+'</h3><div class="big">'+FN(total)+' <small>total</small>'+tokTag+'</div>'+
    '<div class="tokbar">'+[[tok.ti,c.accent],[tok.out-(SPLIT?tok.tr:0),c.accent2]].concat(SPLIT?[[tok.tr,c.reason]]:[]).concat([[tok.ca,c.cache]]).map(([v,k])=>'<i style="width:'+pct(v)+'%;background:'+k+'"></i>').join('')+'</div>'+
    row('Input',FN(tok.ti),c.accent,pct(tok.ti)+'%')+row('Output incl. reasoning',FN(tok.out),c.accent2,pct(tok.out)+'%')+
    (SPLIT?row('&nbsp;&nbsp;↳ reasoning',FN(tok.tr),c.reason,pct(tok.tr)+'%'):'')+
    row('Cache',FN(tok.ca),c.cache,pct(tok.ca)+'%')+'</div>';
  const avgSess=sessions?Math.round(total/sessions):0;
  const tpm=msgs?Math.round(total/msgs):0;
  const effCard='<div class="kpi"><h3>Cache &amp; efficiency</h3><div class="big">'+hitRate+'% <small>cache hit (read)</small></div>'+
    row('Cached tokens (read+write)',FN(tok.ca),c.cache)+row('Avg tokens / session (period)',FN(avgSess),c.accent)+row('Tokens / message',FN(tpm),c.accent2)+
    row('Avg tokens / session (all)',FN(t.avg_tokens_per_session||0),'var(--subtle)')+'</div>';
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
  const rec=$('recovered-note');
  if(rec){
    if(S.recovered&&S.lifetime){
      rec.hidden=false;
      rec.innerHTML='<b>Recovered history</b> · '+FB(S.recovered.tokens_total)+
        ' tokens restored from deleted sessions. This is lifetime metadata only — it is excluded from recorded totals, the model ranking and daily charts. '+
        'Lifetime total: '+FB(S.lifetime.tokens_total)+'.';
    }else{
      rec.hidden=true;rec.innerHTML='';
    }
  }
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
    return '<button type="button" class="act-cell'+(d&&d.date===today?' today':'')+(d&&FSTATE.day&&FSTATE.day!==d.date?' dim':'')+'" '+
      'style="background:'+bg+'" tabindex="0" role="button" aria-label="'+(d?d.date+', '+FN(tot)+' tokens, filter by day':'No activity')+'"'+f+'></button>';
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
    ['5xx upstream',v('err500'),(v('err500')?'var(--bad)':'var(--subtle)'),'err500'],
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
    return '<button type="button" class="act-cell'+(dd&&dd.date===today?' today':'')+(dd&&RFSTATE.day&&RFSTATE.day!==dd.date?' dim':'')+'" '+
      'style="background:'+bg+'" tabindex="0" role="button" aria-label="'+(dd?dd.date+', '+FN(tot)+' tokens, filter by day':'No activity')+'"'+f+'></button>';
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
  const periodLbl=RRANGE==='all'?'':(RRANGE==='365'?'12 MO':(RRANGE/30>=1?Math.round(RRANGE/30)+' MO':RRANGE+' DAYS'));
  $('r-range').textContent='CODEX · '+(R.range||'').toUpperCase()+(periodLbl?' · LAST '+periodLbl:'')+' · REFRESHES AUTOMATICALLY';
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
  updateTabCounts();
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
    if(st==='err500'&&!(r.status>=500))return false;
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
/* ---- Cline tab (Desktop app, from ~/.cline/data) ---- */
function cModelShort(m){return (m||'—').split('/').pop();}
function cDayTotal(d){return (d.ti||0)+(d.to||0)+(d.tr||0)+(d.cache||0);}
function cCutDate(){
  if(CRANGE==='all')return'';
  const d=new Date(Date.now()-CRANGE*86400000);
  return d.toLocaleDateString('en-CA');
}
function cInPeriod(day){const c=cCutDate();return !c||day.date>=c;}
function cPeriodDays(){return (C&&C.days?C.days:[]).filter(cInPeriod);}
function cSumDays(list){
  const t={reqs:0,ok:0,ti:0,to:0,tr:0,cache:0,total:0};
  list.forEach(d=>{t.reqs+=d.reqs||0;t.ok+=d.ok||0;t.ti+=d.ti||0;t.to+=d.to||0;t.tr+=d.tr||0;t.cache+=d.cache||0;t.total+=cDayTotal(d);});
  return t;
}
function cDayTip(d){
  return '<b>'+d.date+'</b>'+
    '<span class="trow"><span>Messages</span><b>'+F(d.reqs||0)+'</b></span>'+
    '<span class="trow"><span>Input</span><b>'+FN(d.ti)+'</b></span>'+
    '<span class="trow"><span>Output</span><b>'+FN(d.to||0)+'</b></span>'+
    '<span class="trow"><span>Cache</span><b>'+FN(d.cache||0)+'</b></span>'+
    '<span class="trow"><span>Total</span><b>'+FN(cDayTotal(d))+'</b></span>';
}
function cStackChart(days){
  const t=T();
  const keys=[{p:'ti',c:t.accent,l:'Input'},{p:'to',c:t.accent2,l:'Output'},{p:'cache',c:t.cache,l:'Cache'}];
  const val=(d,k)=>(d[k.p]||0);
  const w=980,h=250,p=34;
  let max=1;
  if(CSHARE){max=100;}
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
      if(CSHARE){
        const share0=acc/(tot||1)*100||0;
        acc+=v;
        const share1=acc/(tot||1)*100||0;
        const y0=Y(share1),y1=Y(share0);
        if(!v)return;
        cells+=svg('path',{d:'M'+x1.toFixed(1)+','+y0.toFixed(1)+' L'+x2.toFixed(1)+','+y0.toFixed(1)+' L'+x2.toFixed(1)+','+y1.toFixed(1)+' L'+x1.toFixed(1)+','+y1.toFixed(1)+' Z',
          fill:k.c,class:'inter'+(CFSTATE.day&&CFSTATE.day!==day.date?' dim':''),'data-tip':'<b>'+k.l+' · '+day.date+'</b><span class="trow"><span>Tokens</span><b>'+FN(v)+'</b></span>'});
      }else{
        if(!v)return;
        cells+=svg('path',{d:'M'+x1.toFixed(1)+','+Y(acc+v).toFixed(1)+' L'+x2.toFixed(1)+','+Y(acc+v).toFixed(1)+' L'+x2.toFixed(1)+','+Y(acc).toFixed(1)+' L'+x1.toFixed(1)+','+Y(acc).toFixed(1)+' Z',
          fill:k.c,class:'inter'+(CFSTATE.day&&CFSTATE.day!==day.date?' dim':''),'data-tip':'<b>'+k.l+' · '+day.date+'</b><span class="trow"><span>Tokens</span><b>'+FN(v)+'</b></span>'});
        acc+=v;
      }
    });
    cells+=svg('rect',{x:(X(i)-slot/2).toFixed(1),y:p,width:(slot*0.96).toFixed(1),height:(h-2*p).toFixed(1),fill:'transparent',
      class:'inter'+(CFSTATE.day&&CFSTATE.day!==day.date?' dim':''),'data-tip':cDayTip(day),'data-cday':day.date,
      tabindex:'0',role:'button','aria-label':day.date+', '+FN(cDayTotal(day))+' tokens, filter by day'});
    if(i%step===0)xl+=svg('text',{x:X(i).toFixed(1),y:h-p+17,'text-anchor':'middle',fill:'#9a968c','font-size':10},day.date.slice(5));
  });
  const sub=$('c-tok-sub');
  if(sub)sub.textContent=(CSHARE?'100% share':'stacked absolute')+' · output merged';
  return '<svg viewBox="0 0 '+w+' '+(h+10)+'" width="100%">'+cells+xl+'</svg>';
}
function cFileRows(){
  return (CFS==='file'?((S&&S.files)||[]):((S&&S.subsystems)||[]))||[];
}
function renderClineFileChart(){
  const el=$('c-file-chart');if(!el)return;
  const rows=cFileRows();
  const sub=$('c-file-sub');
  if(sub)sub.textContent=(CFS==='file'?'top files · equal-split':'top subsystems')+' · from local edit + write touches';
  if(!S||!rows.length){el.innerHTML='<div class="empty">No local file data to mirror.</div>';return;}
  const t=T();
  const totalAll=rows.reduce((a,r)=>a+(r[1]||0),0)||1;
  const max=Math.max(...rows.map(r=>r[1]||0),1);
  const shown=CFS_EXP?rows:rows.slice(0,6);
  el.innerHTML=shown.map(r=>{
    const name=r[0],toks=r[1]||0,n=r[2]||0,avg=r[3]||0;
    const short=CFS==='file'?shortPath(name):name;
    const tip='<b>'+ESC(name)+'</b><span class=\'trow\'><span>Tokens</span><b>'+FN(toks)+'</b></span>'+
      '<span class=\'trow\'><span>Sessions</span><b>'+n+'</b></span><span class=\'trow\'><span>Avg / session</span><b>'+FN(avg)+'</b></span>'+
      '<span class=\'trow\'><span>Source</span><b>local touches</b></span>';
    return '<div class="model-row" data-tip="'+tip+'">'+
      '<div><div class="mn">'+ESC(short)+'</div><div class="ms">'+n+' sess · avg '+FN(avg)+'</div></div>'+
      '<div class="mbar"><i style="width:'+(toks/max*100).toFixed(1)+'%;background:'+(CFS==='file'?t.accent:t.accent2)+'"></i></div>'+
      '<div class="mv">'+FN(toks)+'</div></div>';
  }).join('')+(rows.length>6?'<button class="more-btn" data-exp="cfs">'+(CFS_EXP?'Show less':'Show all '+rows.length)+' '+(CFS_EXP?'▴':'▾')+'</button>':'');
}
function cCommitRows(){
  const cut=CRANGE==='all'?0:Date.now()-CRANGE*86400000;
  let rows=((C&&C.commits)||[]).filter(c=>{
    if(CFSTATE.day&&c[2]!==CFSTATE.day)return false;
    if(cut){
      const ms=Date.parse(c[2]+'T00:00:00');
      if(ms&&ms<cut-86400000)return false;
    }
    return true;
  });
  rows=rows.slice();
  if(CCS==='recent')rows.sort((a,b)=>a[2]<b[2]?1:-1);
  else rows.sort((a,b)=>(b[7]||0)-(a[7]||0));
  return rows.slice(0,20);
}
function renderClineCommitChart(){
  const el=$('c-commit-chart');if(!el)return;
  const rows=cCommitRows();
  const sub=$('c-commit-sub');
  if(sub)sub.textContent='prior 24h messages · time-matched';
  if(!rows.length){el.innerHTML='<div class="empty">No git commits found in tracked worktrees.</div>';return;}
  const t=T();
  const max=Math.max(...rows.map(r=>r[7]||0),1);
  const shown=CCS_EXP?rows:rows.slice(0,6);
  el.innerHTML=shown.map(r=>{
    const sha=r[0],msg=r[1]||'(no message)',date=r[2],proj=r[3],nf=r[4]||0,add=r[5]||0,del=r[6]||0,toks=r[7]||0,nq=r[8]||0;
    const tip='<b>'+ESC(msg)+'</b><span class=\'trow\'><span>SHA</span><b>'+ESC(sha.slice(0,12))+'</b></span>'+
      '<span class=\'trow\'><span>Date · project</span><b>'+date+' · '+ESC(proj)+'</b></span>'+
      '<span class=\'trow\'><span>Changed</span><b>'+nf+' files · +'+F(add)+' / -'+F(del)+'</b></span>'+
      '<span class=\'trow\'><span>Tokens (prior 24h)</span><b>'+FN(toks)+' · '+nq+' messages</b></span>';
    return '<div class="model-row" data-tip="'+tip+'">'+
      '<div><div class="mn">'+ESC(msg.length>44?msg.slice(0,44)+'…':msg)+'</div>'+
      '<div class="ms">'+sha.slice(0,7)+' · '+date+' · '+ESC(proj)+' · +'+F(add)+'/-'+F(del)+'</div></div>'+
      '<div class="mbar"><i style="width:'+Math.max(toks?2:0,toks/max*100).toFixed(1)+'%;background:'+t.accent2+'"></i></div>'+
      '<div class="mv">'+(toks?FN(toks):'—')+'</div></div>';
  }).join('')+(rows.length>6?'<button class="more-btn" data-exp="ccs">'+(CCS_EXP?'Show less':'Show all '+rows.length)+' '+(CCS_EXP?'▴':'▾')+'</button>':'');
}
function cHumanize(n){n=Number(n)||0;const words=Math.round(n/1.3);if(words>1e6)return (words/1e6).toFixed(1)+'M words';if(words>1e3)return (words/1e3).toFixed(0)+'K words';return words+' words';}
function renderClineRecords(totTokens){
  const el=$('c-records');if(!el||!C.records)return;
  const r=C.records;
  const cards=[];
  if(r.biggest_day)cards.push(['Biggest day',FN(r.biggest_day.total),r.biggest_day.date+' · '+r.biggest_day.sessions+' messages · '+cHumanize(r.biggest_day.total)]);
  if(r.biggest_model)cards.push(['Biggest model',FN(r.biggest_model.total),cModelShort(r.biggest_model.name)+' · '+F(r.biggest_model.requests)+' messages']);
  if(r.biggest_session)cards.push(['Biggest session',FN(r.biggest_session.total),(r.biggest_session.title||'').slice(0,48)]);
  if(r.streaks)cards.push(['Streaks',r.streaks.current+'d now','longest '+r.streaks.longest+'d · '+cHumanize(totTokens)+' total']);
  el.innerHTML=cards.map(([l,v,s])=>'<div class="rec"><div class="rl">'+l+'</div><div class="rv">'+ESC(v)+'</div><div class="rs">'+ESC(s)+'</div></div>').join('')||'<div class="empty">No records yet.</div>';
}
function renderClineAct(){
  const act=(C&&C.activity)||[];
  const $w=$('c-act');
  const MONTHS=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  if(!act.length){$w.innerHTML='<div class="empty">Nothing yet.</div>';$('c-act-summary').textContent='';$('c-act-legend').innerHTML='';return;}
  const today=new Date().toLocaleDateString('en-CA');
  const cols=[];let cur=null;
  act.forEach(dd=>{
    const dt=new Date(dd.date+'T00:00:00');
    const dow=(dt.getDay()+6)%7;
    if(dow===0||!cur){cur={days:new Array(7).fill(null),month:dt.getMonth()};cols.push(cur);}
    cur.days[dow]=dd;
  });
  const mx=Math.max(...act.map(dd=>cDayTotal(dd)),1);
  const lvl=v=>{if(!v)return 0;const tt=v/mx;return tt>=0.75?4:tt>=0.5?3:tt>=0.3?2:1;};
  const pc=[0,18,34,52,70,86];
  const cell=(dd)=>{
    const tot=dd?cDayTotal(dd):0;
    const lv=lvl(tot);
    const bg=lv?'color-mix(in srgb,'+T().accent+' '+pc[lv]+'%, var(--bg))':'var(--chip)';
    const tipH=dd?('<b>'+dd.date+'</b><span class="trow"><span>Total tokens</span><b>'+FN(tot)+'</b></span>'+
      '<span class="trow"><span>Messages</span><b>'+F(dd.reqs||dd.msgs||0)+'</b></span>'):'';
    const f=dd?' data-tip=\''+tipH+'\' data-cday="'+dd.date+'"':'';
    return '<button type="button" class="act-cell'+(dd&&dd.date===today?' today':'')+(dd&&CFSTATE.day&&CFSTATE.day!==dd.date?' dim':'')+'" '+
      'style="background:'+bg+'" tabindex="0" role="button" aria-label="'+(dd?dd.date+', '+FN(tot)+' tokens, filter by day':'No activity')+'"'+f+'></button>';
  };
  let monthHtml='',i=0;
  while(i<cols.length){const m=cols[i].month;let j=i;while(j<cols.length&&cols[j].month===m)j++;
    monthHtml+='<span style="width:'+((j-i)*(12+3)-3)+'px">'+MONTHS[m]+'</span>';i=j;}
  const labels=['Mon','Wed','Fri'].map(dd=>'<span>'+dd+'</span>').join('');
  $w.innerHTML='<div class="act-grid"><div class="act-months">'+monthHtml+'</div>'+
    '<div class="act-body"><div class="act-labels">'+labels+'</div>'+
    '<div class="act-cols">'+cols.map(cc=>'<div class="act-col">'+cc.days.map(cell).join('')+'</div>').join('')+'</div></div></div>';
  const active=act.filter(dd=>cDayTotal(dd)>0).length;
  const tot=act.reduce((a,dd)=>({tokens:a.tokens+cDayTotal(dd),reqs:a.reqs+(dd.reqs||dd.msgs||0)}),{tokens:0,reqs:0});
  $('c-act-summary').innerHTML='<b>'+active+'</b> active days · <b>'+FN(tot.tokens)+'</b> tokens · '+
    '<b>'+F(tot.reqs)+'</b> messages in the last 52 weeks';
  $('c-act-legend').innerHTML='Less <span class="sw e"></span>'+
    [1,2,3,4].map(l=>'<span class="sw" style="background:color-mix(in srgb,'+T().accent+' '+pc[l]+'%, var(--bg))"></span>').join('')+' More <span style="margin-left:8px">token intensity</span>';
}
function renderClineFilterBar(){
  const bar=$('c-fbar');
  const active=Object.entries(CFSTATE).filter(([k,v])=>v);
  if(!active.length){bar.style.display='none';return;}
  bar.style.display='flex';
  const labels={day:'Day',model:'Model',provider:'Provider',session:'Session'};
  const disp=(k,v)=>k==='model'?cModelShort(v):(k==='session'?String(v).slice(-8):v);
  bar.innerHTML='<span class="fl">Filtering:</span>'+active.map(([k,v])=>
    '<span class="fchip" data-ck="'+k+'">'+labels[k]+': '+ESC(disp(k,v))+' <span class="fx">✕</span></span>').join('')
    +'<button class="fclear">clear all</button>';
  bar.querySelectorAll('.fchip').forEach(ch=>ch.onclick=()=>{
    const k=ch.dataset.ck;CFSTATE[k]=null;
    if(k==='provider')$('c-provider-f').value='';
    if(k==='model')$('c-model-f').value='';
    renderCline();
  });
  bar.querySelector('.fclear').onclick=()=>{for(const k in CFSTATE)CFSTATE[k]=null;$('c-provider-f').value='';$('c-model-f').value='';renderCline();};
}
function clineSessionRows(){
  const q=(($('c-search')||{}).value||'').toLowerCase();
  const cut=CRANGE==='all'?0:Date.now()-CRANGE*86400000;
  return (C.sessions||[]).filter(s=>{
    if(CFSTATE.model&&s.model!==CFSTATE.model)return false;
    if(CFSTATE.provider&&s.provider!==CFSTATE.provider)return false;
    if(CFSTATE.session&&s.id!==CFSTATE.session)return false;
    if(CFSTATE.day&&DS(s.time_created)!==CFSTATE.day)return false;
    if(cut&&s.time_created&&s.time_created<cut)return false;
    if(q&&!(s.title||'').toLowerCase().includes(q)&&!(s.model||'').toLowerCase().includes(q)&&!(s.worktree||'').toLowerCase().includes(q))return false;
    return true;});
}
function renderClineSessions(){
  const rows=clineSessionRows();
  currentCRows=rows;
  $('c-req-count').textContent=F(rows.length)+' of '+F((C.sessions||[]).length)+' shown';
  const rf=Object.values(CFSTATE).filter(Boolean).length;
  const ct=(C&&C.totals)||{};
  $('c-req-hint').textContent=(rf?'filtered by '+rf+' criteria · ':'')+F(rows.length)+' sessions · '+F(ct.messages||0)+' messages counted'+
    (C&&C.truncated?' (capped at recent '+F(C.scanned||0)+')':'')+' · '+F(ct.unmetered||0)+' unmetered excluded';
  const tb=$('c-tbl').querySelector('tbody');
  tb.innerHTML=rows.map(s=>{
    const tot=s.tokens_total||((s.tokens_input||0)+(s.tokens_output||0)+(s.tokens_cache||0));
    return '<tr><td><div class="t" style="font-size:12.5px">'+ESC((s.title||s.id||'Untitled').slice(0,80))+'</div>'+
    '<div class="badges"><span class="badge">'+ESC(s.worktree||'—')+'</span></div></td>'+
    '<td><div class="badges mix"><span class="badge model">'+ESC(cModelShort(s.model))+'</span><span class="badge agent">'+ESC(s.provider||'cline')+'</span></div></td>'+
    '<td class="mono" style="font-size:11.5px;color:var(--muted)">'+DT(s.time_created)+'</td>'+
    '<td class="num">'+FN(s.tokens_input||0)+'</td><td class="num">'+FN(s.tokens_output||0)+'</td><td class="num">'+FN(s.tokens_cache||0)+'</td>'+
    '<td class="num" style="font-weight:700">'+FN(tot)+'</td><td class="num" style="color:var(--subtle)">'+F(s.msgs)+'</td></tr>';}).join('')
    ||'<tr><td colspan="8"><div class="empty">No sessions match.</div></td></tr>';
}
function renderCline(){
  if(!C)return;
  const t=C.totals||{};
  const periodLbl=CRANGE==='all'?'':(CRANGE==='365'?'12 MO':(CRANGE/30>=1?Math.round(CRANGE/30)+' MO':CRANGE+' DAYS'));
  $('c-range').textContent='CLINE · '+(C.range||'').toUpperCase()+(periodLbl?' · LAST '+periodLbl:'')+' · REFRESHES AUTOMATICALLY';
  const cur=CRANGE==='all'?null:cSumDays(cPeriodDays());
  const totTokens=cur?cur.total:(t.tokens_total||0);
  const totMsgs=cur?cur.reqs:(t.requests||0);
  $('c-headline').innerHTML=BOLD(ESC(F(totMsgs)+' messages · '+Math.round(totTokens).toLocaleString('en-US')+' tokens with Cline.'));
  const nModels=(C.models||[]).length;
  $('c-pills').innerHTML=[
    ['Messages',F(totMsgs)],
    ['Tokens',FN(totTokens)],
    ['Sessions',F(cur?new Set((C.sessions||[]).filter(s=>!cCutDate()||DS(s.time_created)>=cCutDate()).map(s=>s.id)).size:(t.sessions||0))],
    ['Models',F(nModels)]]
    .map(([k,v])=>'<span class="pill">'+ESC(k)+' <b style="color:var(--text)">'+v+'</b></span>').join('');
  const t7=(()=>{const now=new Date();now.setHours(0,0,0,0);const s=new Date(now);s.setDate(s.getDate()-6);
    return cSumDays((C.days||[]).filter(d=>d.date>=s.toLocaleDateString('en-CA')));})();
  const t30=(()=>{const now=new Date();now.setHours(0,0,0,0);const s=new Date(now);s.setDate(s.getDate()-29);
    return cSumDays((C.days||[]).filter(d=>d.date>=s.toLocaleDateString('en-CA')));})();
  const t1=(()=>{const s=new Date().toLocaleDateString('en-CA');
    return cSumDays((C.days||[]).filter(d=>d.date>=s));})();
  $('c-summary').innerHTML=[['Today',t1.total],['Last 7 days',t7.total],['Last 30 days',t30.total],['All time',t.tokens_total||0]]
    .map(([l,v])=>'<div class="sum-card"><div class="sl">'+l+'</div><div class="sv">'+FN(v)+'</div><div class="ss">tokens</div></div>').join('');
  const c=T();
  const tok=cur?{input:cur.ti,out:cur.to,ca:cur.cache}:{input:t.tokens_input,out:t.tokens_output,ca:t.tokens_cache_read};
  const total=tok.input+tok.out+tok.ca;
  const pct=v=>total?Math.round(v/total*100):0;
  const row=(lbl,v,color,sub)=>'<div class="krow"><span class="lbl"><i class="dot" style="background:'+color+'"></i>'+lbl+
    (sub?' <span class="dim">'+sub+'</span>':'')+'</span><b>'+v+'</b></div>';
  const hitRate=(tok.input+tok.ca)?Math.round(tok.ca/(tok.input+tok.ca)*100):0;
  const avgMsg=totMsgs?Math.round(total/totMsgs):0;
  $('c-kpis').innerHTML=
    '<div class="kpi"><h3>Tokens (output merged)</h3><div class="big">'+FN(total)+' <small>total</small></div>'+
    '<div class="tokbar">'+[[tok.input,c.accent],[tok.out,c.accent2],[tok.ca,c.cache]].map(([v,k])=>'<i style="width:'+pct(v)+'%;background:'+k+'"></i>').join('')+'</div>'+
    row('Input',FN(tok.input),c.accent,pct(tok.input)+'%')+row('Output',FN(tok.out),c.accent2,pct(tok.out)+'%')+
    row('Cache',FN(tok.ca),c.cache,pct(tok.ca)+'%')+'</div>'+
    '<div class="kpi"><h3>Cache &amp; efficiency</h3><div class="big">'+hitRate+'% <small>cache hit</small></div>'+
    row('Cached tokens',FN(tok.ca),c.cache)+row('Avg / message',FN(avgMsg),c.accent)+
    row('Avg tokens/session (all)',FN(t.avg_tokens_per_session||0),'var(--subtle)')+'</div>'+
    '<div class="kpi"><h3>Burn &amp; activity</h3><div class="big">'+F(totMsgs)+' <small>messages</small></div>'+
    row('Sessions',F(cur?new Set((C.sessions||[]).filter(s=>!cCutDate()||DS(s.time_created)>=cCutDate()).map(s=>s.id)).size:(t.sessions||0)),c.accent)+
    row('Top model',topClineModelName(),c.accent2,'all-time')+
    row('Unmetered',F(t.unmetered||0),'var(--subtle)','excluded')+'</div>';
  renderClineRecords(totTokens);
  const d=cPeriodDays();
  $('c-tok-chart').innerHTML=d.length?cStackChart(d):'<div class="empty">Nothing in this period.</div>';
  $('c-tok-legend').innerHTML='<span><i style="background:'+c.accent+'"></i>Input</span><span><i style="background:'+c.accent2+'"></i>Output</span><span><i style="background:'+c.cache+'"></i>Cache</span>'+(CSHARE?'<span>100% share mode</span>':'');
  renderClineFileChart();
  renderClineCommitChart();
  $('c-providers').innerHTML=(C.providers||[]).map(p=>'<span class="chip'+(CFSTATE.provider===p[0]?' active':'')+'" data-cp="'+ESC(p[0])+'" data-tip="<b>'+ESC(p[0])+'</b><span class=\'trow\'><span>Messages</span><b>'+p[1]+'</b></span><span class=\'trow\'><span>Tokens</span><b>'+FN(p[2]||0)+'</b></span>">'+
    '<b>'+FN(p[2]||p[1])+'</b><span class="n">'+ESC(p[0])+' · '+p[1]+'</span></span>').join('')||'<span class="empty">No providers</span>';
  const sessChips=(C.sessions||[]).slice(0,8);
  $('c-sessions').innerHTML=sessChips.map(s=>'<span class="chip'+(CFSTATE.session===s.id?' active':'')+'" data-cs="'+ESC(s.id)+'" data-tip="<b>'+ESC((s.title||s.id).slice(0,60))+'</b><span class=\'trow\'><span>Tokens</span><b>'+FN(s.tokens_total||0)+'</b></span><span class=\'trow\'><span>Messages</span><b>'+s.msgs+'</b></span>">'+
    '<b>'+FN(s.tokens_total||0)+'</b><span class="n">'+ESC((s.title||s.id).slice(0,24))+'</span></span>').join('')||'<span class="empty">No sessions</span>';
  const totalAll=(C.models||[]).reduce((a,m)=>a+(m[2]||0),0)||1;
  $('c-models').innerHTML=(C.models||[]).length?(C.models||[]).map(m=>{
    const name=m[0],reqs=m[1],tot=m[2]||0,ti=m[3]||0,to=m[4]||0,ca=m[5]||0,ok=m[6]||0;
    const prov=m[10]||'',free=m[11];
    const segs=[[ti,c.accent],[to,c.accent2],[ca,c.cache]];
    const bar=segs.map(([v,k])=>'<i style="width:'+(tot?v/tot*100:0)+'%;background:'+k+'"></i>').join('');
    const tipH=('<b>'+ESC(name)+'</b><span class=\'trow\'><span>Messages</span><b>'+reqs+'</b></span>'+
      '<span class=\'trow\'><span>Total tokens</span><b>'+FN(tot)+' ('+(tot/totalAll*100).toFixed(1)+'%)</b></span>'+
      '<span class=\'trow\'><span>Input / Output / Cache</span><b>'+FN(ti)+' / '+FN(to)+' / '+FN(ca)+'</b></span>'+
      '<span class=\'trow\'><span>Provider</span><b>'+ESC(prov)+'</b></span>');
    return '<div class="model-row clickable'+(CFSTATE.model===name?' active':'')+'" data-cm="'+ESC(name)+'" data-tip="'+tipH+'">'+
      '<div><div class="mn">'+ESC(cModelShort(name))+(free?'<span class="free-tag">free</span>':'')+'</div><div class="ms">'+reqs+' msg · '+(tot/totalAll*100).toFixed(1)+'% · '+ESC(prov)+'</div></div>'+
      '<div class="mbar">'+bar+'</div>'+
      '<div class="mv">'+FN(tot)+'</div></div>';
  }).join(''):'<div class="empty">No cline models yet.</div>';
  $('c-model-legend').innerHTML='<span><i style="background:'+c.accent+'"></i>Input</span><span><i style="background:'+c.accent2+'"></i>Output</span><span><i style="background:'+c.cache+'"></i>Cache</span><span>click a model to filter sessions</span>';
  $('c-insights').innerHTML=(C.insights||[]).map(ins=>'<div class="in"><p>'+BOLD(ESC(ins))+'</p></div>').join('');
  $('c-notes-hint').textContent=CRANGE==='all'?'generated from your cline data':'all-time notes · charts/tables follow the period filter';
  const pf=$('c-provider-f'),mf=$('c-model-f');
  pf.innerHTML='<option value="">All providers</option>'+(C.providers||[]).map(p=>'<option value="'+ESC(p[0])+'">'+ESC(p[0])+'</option>').join('');
  mf.innerHTML='<option value="">All models</option>'+(C.models||[]).map(m=>'<option value="'+ESC(m[0])+'">'+ESC(cModelShort(m[0]))+'</option>').join('');
  pf.value=CFSTATE.provider||'';mf.value=CFSTATE.model||'';
  renderClineFilterBar();
  renderClineSessions();
  renderClineAct();
  updateTabCounts();
}
function topClineModelName(){
  if(!C||!C.models||!C.models.length)return'—';
  return ESC(cModelShort(C.models[0][0]).slice(0,30));
}
/* ---- Recap tab (daily totals for YT Shorts) ---- */
function recapDayEntry(date){
  const oc=(S&&S.days?S.days.find(d=>d.date===date):null);
  const rx=(R&&R.days?R.days.find(d=>d.date===date):null);
  const cx=(C&&C.days?C.days.find(d=>d.date===date):null);
  const ocT=oc?dayTotal(oc):0, rxT=rx?rDayTokenTotal(rx):0, cxT=cx?cDayTotal(cx):0;
  return {oc,rx,cx,ocT,rxT,cxT,
    total:(HARNESS.opencode?ocT:0)+(HARNESS.router?rxT:0)+(HARNESS.cline?cxT:0)};
}
function recapConversations(date){
  const out=[];
  if(HARNESS.opencode&&S&&S.sessions){
    S.sessions.filter(s=>DS(s.time_created)===date).forEach(s=>{
      out.push({title:s.title||'Untitled',harness:'OpenCode',model:modelShort(s.model),
        toks:s.tokens_total||((s.tokens_input||0)+(s.tokens_output||0)+(s.tokens_reasoning||0)+(s.tokens_cache||0))});});
  }
  if(HARNESS.router&&R&&R.requests){
    const byModel={};
    R.requests.forEach(r=>{
      let ms=Date.parse(r.at||'');if(!ms||new Date(ms).toLocaleDateString('en-CA')!==date)return;
      const k=r.model+'|'+r.provider;
      const a=byModel[k]||(byModel[k]={title:rModelShort(r.model),harness:'Codex',model:rModelShort(r.model),toks:0,n:0,prov:r.provider});
      a.toks+=r.total||0;a.n+=1;});
    Object.values(byModel).forEach(a=>{a.title=a.title+' · '+a.n+' req ('+a.prov+')';out.push(a);});
  }
  if(HARNESS.cline&&C&&C.sessions){
    C.sessions.filter(s=>DS(s.time_created)===date).forEach(s=>{
      out.push({title:(s.title||s.id||'Untitled').slice(0,80),harness:'Cline',model:cModelShort(s.model),toks:s.tokens_total||0});});
  }
  out.sort((a,b)=>(b.toks||0)-(a.toks||0));
  return out.slice(0,10);
}
function recapProviders(date){
  const agg={};
  const add=(k,v)=>{if(!k)return;agg[k]=(agg[k]||0)+v;};
  if(HARNESS.opencode&&S&&S.sessions){
    S.sessions.filter(s=>DS(s.time_created)===date).forEach(s=>{
      const m=(s.model||'').split('/')[0]||'opencode';
      add(m,s.tokens_total||0);});
  }
  if(HARNESS.router&&R&&R.requests){
    R.requests.forEach(r=>{
      let ms=Date.parse(r.at||'');if(!ms||new Date(ms).toLocaleDateString('en-CA')!==date)return;
      add(r.provider||'codex',r.total||0);});
  }
  if(HARNESS.cline&&C&&C.requests){
    C.requests.forEach(r=>{
      let ms=r.ts||Date.parse(r.at||'');if(!ms)return;
      if(new Date(ms).toLocaleDateString('en-CA')!==date)return;
      add(r.provider||'cline',r.total||0);});
  }
  return Object.entries(agg).sort((a,b)=>b[1]-a[1]).slice(0,12);
}
function renderRecap(){
  const date=RECAP_DATE;
  const e=recapDayEntry(date);
  const today=new Date().toLocaleDateString('en-CA');
  $('recap-date').textContent=(date===today?'TODAY · ':'')+date;
  $('recap-big').innerHTML=FN(e.total)+' <small>tokens</small>';
  const parts=[];
  if(HARNESS.opencode)parts.push('OpenCode '+FN(e.ocT));
  if(HARNESS.router)parts.push('Codex '+FN(e.rxT));
  if(HARNESS.cline)parts.push('Cline '+FN(e.cxT));
  $('recap-sub').textContent=parts.length?parts.join('  ·  '):'All harnesses disabled in Settings.';
  const cards=[];
  if(HARNESS.opencode)cards.push(['OpenCode',e.ocT,(e.oc?e.oc.sessions+' sess · '+e.oc.msgs+' msgs':'no activity')]);
  if(HARNESS.router)cards.push(['Codex',e.rxT,(e.rx?e.rx.reqs+' req · '+e.rx.ok+' ok':'no activity')]);
  if(HARNESS.cline)cards.push(['Cline',e.cxT,(e.cx?e.cx.reqs+' msg · '+e.cx.ok+' sess-eq':'no activity')]);
  $('recap-split').innerHTML=cards.map(([l,v,s])=>'<div class="recap-card"><div class="rl">'+l+'</div><div class="rv">'+FN(v)+'</div><div class="rs">'+ESC(s)+'</div></div>').join('')||'<div class="empty">Enable a harness in Settings.</div>';
  const provs=recapProviders(date);
  $('recap-providers').innerHTML=provs.map(([k,v])=>'<span class="chip" data-tip="<b>'+ESC(k)+'</b><span class=\'trow\'><span>Tokens</span><b>'+FN(v)+'</b></span>"><b>'+FN(v)+'</b><span class="n">'+ESC(k)+'</span></span>').join('')||'<span class="empty">No provider data for this date.</span>';
  const convs=recapConversations(date);
  $('recap-conv-hint').textContent=convs.length?'top by tokens · '+date:'nothing recorded · '+date;
  const tb=$('recap-tbl').querySelector('tbody');
  tb.innerHTML=convs.map(c=>'<tr><td><div class="t" style="font-size:12.5px">'+ESC((c.title||'Untitled').slice(0,90))+'</div></td>'+
    '<td><span class="badge agent">'+ESC(c.harness)+'</span></td><td><span class="badge model">'+ESC((c.model||'—').slice(0,32))+'</span></td>'+
    '<td class="num" style="font-weight:700">'+FN(c.toks||0)+'</td></tr>').join('')
    ||'<tr><td colspan="4"><div class="empty">No conversations on this date.</div></td></tr>';
  updateTabCounts();
}
function recapCopyText(){
  const date=RECAP_DATE;
  const e=recapDayEntry(date);
  const convs=recapConversations(date).slice(0,3);
  const lines=[date+': '+Math.round(e.total).toLocaleString('en-US')+' tokens'];
  const bits=[];
  if(HARNESS.opencode)bits.push('OpenCode '+Math.round(e.ocT).toLocaleString('en-US'));
  if(HARNESS.router)bits.push('Codex '+Math.round(e.rxT).toLocaleString('en-US'));
  if(HARNESS.cline)bits.push('Cline '+Math.round(e.cxT).toLocaleString('en-US'));
  if(bits.length)lines.push('('+bits.join(' · ')+')');
  convs.forEach(c=>lines.push('— '+(c.title||'Untitled').slice(0,60)+' ('+Math.round(c.toks||0).toLocaleString('en-US')+')'));
  return lines.join('\n');
}
/* ---- harness settings ---- */
/* ---- navigation, hash routing and chrome ---- */
function firstVisibleTab(){
  if(FOCUS)return'opencode';
  if(HARNESS.opencode)return'opencode';
  if(HARNESS.router)return'router';
  if(HARNESS.cline)return'cline';
  return'recap';
}
function tabRange(tab){
  return tab==='router'?RRANGE:tab==='cline'?CRANGE:tab==='opencode'?RANGE:'all';
}
function setTabRange(tab,r){
  if(RANGE_VALUES.indexOf(r)<0)return;
  if(tab==='router')RRANGE=r;else if(tab==='cline')CRANGE=r;else if(tab==='opencode')RANGE=r;
}
function hashForState(){
  const slug=TAB==='router'?'codex':TAB;
  let h='#/'+slug;
  if(TAB!=='recap')h+='/'+(SUBVIEW[TAB]||VIEWS[TAB][0].id);
  const r=tabRange(TAB);
  if(TAB!=='recap'&&r&&r!=='all')h+='?range='+encodeURIComponent(r);
  return h;
}
function parseHash(raw){
  if(!raw||raw.charAt(0)!=='#'||raw.charAt(1)!=='/')return null;
  let rest=raw.slice(2),qs='';
  const qi=rest.indexOf('?');
  if(qi>=0){qs=rest.slice(qi+1);rest=rest.slice(0,qi);}
  const parts=rest.split('/').filter(Boolean);
  const tab=TAB_ALIAS[(parts[0]||'').toLowerCase()];
  let range=null;
  if(qs){
    try{const p=new URLSearchParams(qs),r=p.get('range');if(RANGE_VALUES.indexOf(r)>=0)range=r;}catch(_){}
  }
  if(!tab)return {tab:firstVisibleTab(),view:VIEWS[firstVisibleTab()][0].id,range:null,invalid:true};
  let view=parts[1]||null;
  if(!view||!VIEWS[tab].some(v=>v.id===view))view=VIEWS[tab][0].id;
  return {tab:tab,view:view,range:range};
}
function syncHash(mode){
  const h=hashForState();
  if(location.hash===h)return;
  try{history[mode==='replace'?'replaceState':'pushState'](null,'',h);}
  catch(_){location.hash=h.slice(1);}
}
function syncPresetButtons(){
  [['opencode','data-r',RANGE],['router','data-rr',RRANGE],['cline','data-cr',CRANGE]].forEach(([tab,attr,val])=>{
    document.querySelectorAll('.pbtn['+attr+']').forEach(b=>{
      const on=b.getAttribute(attr)===val;
      b.classList.toggle('active',on);
      b.setAttribute('aria-pressed',on?'true':'false');
    });
  });
}
function updateChrome(){
  const list=VIEWS[TAB]||VIEWS.opencode;
  const view=list.find(v=>v.id===SUBVIEW[TAB])||list[0];
  if($('crumb-harness'))$('crumb-harness').textContent=HARNESS_NAME[TAB];
  if($('crumb-view'))$('crumb-view').textContent=view.label;
  document.title=HARNESS_NAME[TAB]+' · '+view.label+' — OpenCode Dashboard';
  syncPresetButtons();
}
function renderViewNav(){
  const list=VIEWS[TAB]||[],active=SUBVIEW[TAB],name=HARNESS_NAME[TAB];
  if($('rail-view-kicker'))$('rail-view-kicker').textContent='Views · '+name;
  if($('rail-views')){
    $('rail-views').innerHTML=list.map(v=>'<button class="rail-item view-link'+(v.id===active?' active':'')+'" data-view="'+ESC(v.id)+'" title="'+ESC(v.label)+'" aria-label="'+ESC(name+' '+v.label)+'"'+(v.id===active?' aria-current="page"':'')+'>'+
      '<span class="ri-ico" aria-hidden="true">'+ESC(v.icon)+'</span><span class="ri-label">'+ESC(v.label)+'</span></button>').join('');
  }
  if($('subnav')){
    $('subnav').innerHTML=list.map(v=>'<button type="button" class="subnav-btn'+(v.id===active?' active':'')+'" data-view="'+ESC(v.id)+'" role="tab" aria-selected="'+(v.id===active?'true':'false')+'">'+ESC(v.label)+'</button>').join('');
  }
}
function activateTabDom(){
  ['opencode','router','cline','recap'].forEach(t=>{
    const btn=$('tab-'+t),view=$('view-'+t),rail=$('rail-tab-'+t),on=t===TAB;
    if(btn){btn.classList.toggle('active',on);btn.setAttribute('aria-selected',on?'true':'false');}
    if(rail){rail.classList.toggle('active',on);if(on)rail.setAttribute('aria-current','page');else rail.removeAttribute('aria-current');}
    if(view)view.hidden=!on;
  });
}
function showSubviewDom(){
  const active=SUBVIEW[TAB];
  document.querySelectorAll('#view-opencode .subview,#view-router .subview,#view-cline .subview,#view-recap .subview').forEach(sv=>{
    const view=sv.parentElement;
    const on=view&&view.id==='view-'+TAB&&sv.dataset.sub===active;
    sv.classList.toggle('active',on);
    sv.hidden=!on;
  });
}
function renderTab(){
  if(TAB==='router')renderRouter();
  else if(TAB==='cline')renderCline();
  else if(TAB==='recap')renderRecap();
  else renderAll();
}
function applyState(tab,view,range,historyMode){
  // Explicit deep links may target a harness hidden by focus mode: render it
  // with a dismissible note instead of silently redirecting.
  if(!VIEWS[tab]){tab=firstVisibleTab();view=VIEWS[tab][0].id;}
  const list=VIEWS[tab];
  if(!view||!list.some(v=>v.id===view))view=SUBVIEW[tab];
  if(!view||!list.some(v=>v.id===view))view=list[0].id;
  TAB=tab;SUBVIEW[tab]=view;
  if(range)setTabRange(tab,range);
  try{sessionStorage.setItem('ocd-tab',tab);}catch(_){}
  try{sessionStorage.setItem('ocd-subview',JSON.stringify(SUBVIEW));}catch(_){}
  if(TAB==='opencode')FOCUS_NOTE_DISMISSED=false;
  activateTabDom();showSubviewDom();renderViewNav();updateChrome();
  updateFocusNote();
  renderTab();
  if(historyMode)syncHash(historyMode);
}
function setTab(name){
  applyState(name,SUBVIEW[name]||VIEWS[name][0].id,null,'push');
}
function setSubview(name){
  if(!VIEWS[TAB].some(v=>v.id===name)||SUBVIEW[TAB]===name)return;
  applyState(TAB,name,null,'push');
}
function onHistoryRoute(){
  const route=parseHash(location.hash);
  if(!route)return;
  if(route.tab===TAB&&route.view===SUBVIEW[TAB]&&(route.range||'all')===tabRange(TAB))return;
  applyState(route.tab,route.view,route.range||'all',null);
}
let ROUTED=false;
let FOCUS_NOTE_DISMISSED=false;
function updateFocusNote(){
  const el=$('focus-note');
  if(!el)return;
  el.hidden=!(FOCUS&&TAB!=='opencode'&&!FOCUS_NOTE_DISMISSED);
}
function initRouting(){
  let tab=null,view=null,range=null;
  const route=parseHash(location.hash);
  if(route){tab=route.tab;view=route.view;range=route.range;}
  else if(FOCUS){
    tab='opencode';
    view=SUBVIEW.opencode||VIEWS.opencode[0].id;
  }
  else{
    let saved=null;try{saved=sessionStorage.getItem('ocd-tab');}catch(_){}
    if(saved&&VIEWS[saved]&&harnessVisible(saved))tab=saved;
    else tab=firstVisibleTab();
    view=SUBVIEW[tab]||VIEWS[tab][0].id;
  }
  applyState(tab,view,range,null);
  ROUTED=true;
  try{history.replaceState(null,'',hashForState());}catch(_){}
  window.addEventListener('popstate',onHistoryRoute);
  window.addEventListener('hashchange',onHistoryRoute);
}
function applyHarness(){
  const map={opencode:['tab-opencode','rail-tab-opencode'],router:['tab-router','rail-tab-router'],cline:['tab-cline','rail-tab-cline'],recap:['tab-recap','rail-tab-recap']};
  for(const k in map){
    map[k].forEach(id=>{const el=$(id);if(el)el.style.display=harnessVisible(k)?'':'none';});
  }
  try{
    $('set-focus').checked=!!FOCUS;
    $('set-opencode').checked=!!HARNESS.opencode;
    $('set-router').checked=!!HARNESS.router;
    $('set-cline').checked=!!HARNESS.cline;
  }catch(_){}
  try{localStorage.setItem('ocd-harness',JSON.stringify(HARNESS));}catch(_){}
  try{localStorage.setItem('ocd-focus',FOCUS?'1':'0');}catch(_){}
  updateFocusNote();
  updateTabCounts();
  if(TAB==='recap')renderRecap();
}
function updateTabCounts(){
  const setCnt=(id,v)=>{const el=$(id);if(el)el.textContent=v;};
  try{
    if(S&&S.totals){const v=FN(S.totals.tokens_total||0);setCnt('tab-opencode-cnt',v);setCnt('rail-cnt-opencode',v);}
    if(R&&R.totals){const v=F(R.totals.requests||0)+' req';setCnt('tab-router-cnt',v);setCnt('rail-cnt-router',v);}
    if(C&&C.totals){const v=F(C.totals.requests||0)+' msg';setCnt('tab-cline-cnt',v);setCnt('rail-cnt-cline',v);}
    const t=new Date().toLocaleDateString('en-CA');
    const rv=FN(recapDayEntry(RECAP_DATE||t).total);
    setCnt('tab-recap-cnt',rv);setCnt('rail-cnt-recap',rv);
  }catch(_){}
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
  if(!e.target.closest('.export-wrap')){setExportOpen(false);setMenuOpen(false);}
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
document.querySelectorAll('.pbtn[data-cfs]').forEach(b=>b.onclick=()=>{
  CFS=b.dataset.cfs;CFS_EXP=false;
  document.querySelectorAll('.pbtn[data-cfs]').forEach(x=>x.classList.toggle('active',x===b));
  renderClineFileChart();
});
document.querySelectorAll('.pbtn[data-ccs]').forEach(b=>b.onclick=()=>{
  CCS=b.dataset.ccs;CCS_EXP=false;
  document.querySelectorAll('.pbtn[data-ccs]').forEach(x=>x.classList.toggle('active',x===b));
  renderClineCommitChart();
});
document.addEventListener('click',e=>{
  const b=e.target.closest?e.target.closest('[data-exp]'):null;
  if(!b)return;
  const k=b.dataset.exp;
  if(k==='fs'){FS_EXP=!FS_EXP;renderFileChart();}
  else if(k==='cs'){CS_EXP=!CS_EXP;renderCommitChart();}
  else if(k==='rfs'){RFS_EXP=!RFS_EXP;renderRouterFileChart();}
  else if(k==='rcs'){RCS_EXP=!RCS_EXP;renderRouterCommitChart();}
  else if(k==='cfs'){CFS_EXP=!CFS_EXP;renderClineFileChart();}
  else if(k==='ccs'){CCS_EXP=!CCS_EXP;renderClineCommitChart();}
});
document.querySelectorAll('.pbtn[data-r]').forEach(b=>b.onclick=()=>{
  RANGE=b.dataset.r;syncPresetButtons();renderAll();syncHash('push');
});
$('split-btn').onclick=()=>{SPLIT=!SPLIT;$('split-btn').textContent='Split reasoning: '+(SPLIT?'on':'off');$('split-btn').setAttribute('aria-pressed',SPLIT);renderAll();};
$('share-btn').onclick=()=>{SHARE=!SHARE;$('share-btn').textContent='Share: '+(SHARE?'on':'off');$('share-btn').setAttribute('aria-pressed',SHARE);renderAll();};
document.querySelectorAll('.pbtn[data-rr]').forEach(b=>b.onclick=()=>{
  RRANGE=b.dataset.rr;syncPresetButtons();renderRouter();syncHash('push');
});
$('r-split-btn').onclick=()=>{RSPLIT=!RSPLIT;$('r-split-btn').textContent='Split reasoning: '+(RSPLIT?'on':'off');$('r-split-btn').setAttribute('aria-pressed',RSPLIT);renderRouter();};
$('r-share-btn').onclick=()=>{RSHARE=!RSHARE;$('r-share-btn').textContent='Share: '+(RSHARE?'on':'off');$('r-share-btn').setAttribute('aria-pressed',RSHARE);renderRouter();};
document.querySelectorAll('.pbtn[data-cr]').forEach(b=>b.onclick=()=>{
  CRANGE=b.dataset.cr;syncPresetButtons();renderCline();syncHash('push');
});
$('c-share-btn').onclick=()=>{CSHARE=!CSHARE;$('c-share-btn').textContent='Share: '+(CSHARE?'on':'off');$('c-share-btn').setAttribute('aria-pressed',CSHARE);renderCline();};
document.addEventListener('click',e=>{
  const h=e.target.closest?e.target.closest('[data-harness]'):null;
  if(h){e.preventDefault();setTab(h.dataset.harness);return;}
  const v=e.target.closest?e.target.closest('[data-view]'):null;
  if(v){e.preventDefault();setSubview(v.dataset.view);}
});
$('c-search').addEventListener('input',renderClineSessions);
$('c-provider-f').addEventListener('change',e=>{CFSTATE.provider=e.target.value||null;renderCline();});
$('c-model-f').addEventListener('change',e=>{CFSTATE.model=e.target.value||null;renderCline();});
document.addEventListener('click',e=>{
  const cm=e.target.closest?e.target.closest('[data-cm]'):null;
  if(cm){CFSTATE.model=CFSTATE.model===cm.dataset.cm?null:cm.dataset.cm;$('c-model-f').value=CFSTATE.model||'';renderCline();return;}
  const cp=e.target.closest?e.target.closest('[data-cp]'):null;
  if(cp){CFSTATE.provider=CFSTATE.provider===cp.dataset.cp?null:cp.dataset.cp;$('c-provider-f').value=CFSTATE.provider||'';renderCline();return;}
  const cs=e.target.closest?e.target.closest('[data-cs]'):null;
  if(cs){CFSTATE.session=CFSTATE.session===cs.dataset.cs?null:cs.dataset.cs;renderCline();return;}
  const cd=e.target.closest?e.target.closest('[data-cday]'):null;
  if(cd){CFSTATE.day=CFSTATE.day===cd.dataset.cday?null:cd.dataset.cday;renderCline();return;}
});
/* recap controls */
function setRecapDate(d){
  RECAP_DATE=d;
  document.querySelectorAll('.pbtn[data-recap]').forEach(x=>{
    const today=new Date().toLocaleDateString('en-CA');
    const yest=new Date(Date.now()-86400000).toLocaleDateString('en-CA');
    const mine=x.dataset.recap==='today'?today:x.dataset.recap==='yesterday'?yest:null;
    x.classList.toggle('active',mine===d);
  });
  try{$('recap-picker').value=d;}catch(_){}
  renderRecap();
}
document.querySelectorAll('.pbtn[data-recap]').forEach(b=>b.onclick=()=>{
  const today=new Date().toLocaleDateString('en-CA');
  setRecapDate(b.dataset.recap==='yesterday'?new Date(Date.now()-86400000).toLocaleDateString('en-CA'):today);
});
$('recap-picker').addEventListener('change',e=>{if(e.target.value)setRecapDate(e.target.value);});
$('recap-copy').onclick=async()=>{
  const txt=recapCopyText();
  try{await navigator.clipboard.writeText(txt);toast('Recap copied — paste it into your Short.');}
  catch(_){
    const ta=document.createElement('textarea');ta.value=txt;document.body.appendChild(ta);ta.select();
    try{document.execCommand('copy');toast('Recap copied — paste it into your Short.');}
    catch(__){toast('Copy failed — select the numbers manually.',false);}
    ta.remove();
  }
};
/* harness settings */
function setMenuOpen(open){
  $('setmenu').classList.toggle('open',open);
  $('settings').setAttribute('aria-expanded',open?'true':'false');
}
function openSettingsMenu(){
  setMenuOpen(true);
  const first=$('set-opencode');if(first)first.focus();
}
$('settings').onclick=e=>{e.stopPropagation();const open=!$('setmenu').classList.contains('open');setExportOpen(false);setMenuOpen(open);};
$('set-focus').addEventListener('change',e=>{
  FOCUS=!!e.target.checked;
  FOCUS_NOTE_DISMISSED=false;
  if(FOCUS){
    HARNESS.opencode=true;
    applyHarness();
    applyState('opencode','overview',null,'push');
  }else{
    // Turning focus off reveals every harness immediately; individual
    // checkboxes can then be used to hide them again.
    HARNESS={opencode:true,router:true,cline:true};
    applyHarness();
  }
  $('set-focus').checked=FOCUS;
});
$('focus-note-x').onclick=()=>{FOCUS_NOTE_DISMISSED=true;updateFocusNote();};
[['set-opencode','opencode'],['set-router','router'],['set-cline','cline']].forEach(([id,key])=>{
  $(id).addEventListener('change',e=>{
    if(!e.target.checked){
      const on=Object.keys(HARNESS).filter(k=>k===key?false:HARNESS[k]);
      if(!on.length){e.target.checked=true;toast('Keep at least one harness enabled.',false);return;}
    }
    HARNESS[key]=e.target.checked;
    applyHarness();
    if(ROUTED&&!harnessVisible(TAB))setTab(firstVisibleTab());
  });
});
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
/* keyboard activation for SVG day cells (heatmap cells are real buttons) */
document.addEventListener('keydown',e=>{
  if(e.key!=='Enter'&&e.key!==' '&&e.key!=='Spacebar')return;
  const cell=e.target.closest?e.target.closest('svg [data-f],svg [data-cday]'):null;
  if(!cell)return;
  e.preventDefault();
  cell.dispatchEvent(new MouseEvent('click',{bubbles:true}));
});
document.querySelector('.skip').addEventListener('click',e=>{
  e.preventDefault();
  const main=$('main');if(!main)return;
  main.focus();
  main.scrollIntoView();
});
/* export */
function exportData(kind){
  const isR=TAB==='router'&&R;
  const isC=TAB==='cline'&&C;
  if(kind==='json'){
    const b=new Blob([JSON.stringify(isR?R:(isC?C:S),null,2)],{type:'application/json'});
    const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download=isR?'codex-tokens.json':(isC?'cline-tokens.json':'opencode-tokens.json');a.click();
  }else if(kind==='recap'){
    const dates=[...new Set([...(S.days||[]).map(d=>d.date),...(R.days||[]).map(d=>d.date),...(C.days||[]).map(d=>d.date)])].sort();
    const head=['date','opencode_tokens','codex_tokens','cline_tokens','total_tokens'];
    const lines=[head.join(',')].concat(dates.map(d=>{const e=recapDayEntry(d);return [d,Math.round(e.ocT),Math.round(e.rxT),Math.round(e.cxT),Math.round(e.total)].join(',');}));
    const b=new Blob([lines.join('\n')],{type:'text/csv'});
    const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='recap-tokens-days.csv';a.click();
  }else if(isR){
    const rows=rPeriodDays();
    const head=['date','requests','ok','errors','tokens_in','tokens_out','tokens_cache','tokens_total','what_if_cost'];
    const lines=[head.join(',')].concat(rows.map(d=>[d.date,d.reqs,d.ok,d.err,Math.round(d.ti),Math.round(d.to),Math.round(d.cache),Math.round(rDayTokenTotal(d)),(d.what_if||0)].join(',')));
    const b=new Blob([lines.join('\n')],{type:'text/csv'});
    const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='codex-tokens-days.csv';a.click();
  }else if(isC){
    const rows=cPeriodDays();
    const head=['date','messages','sessions','tokens_in','tokens_out','tokens_cache','tokens_total'];
    const lines=[head.join(',')].concat(rows.map(d=>[d.date,d.reqs,d.ok,Math.round(d.ti),Math.round(d.to),Math.round(d.cache),Math.round(cDayTotal(d))].join(',')));
    const b=new Blob([lines.join('\n')],{type:'text/csv'});
    const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='cline-tokens-days.csv';a.click();
  }else{
    const rows=periodDays();
    const head=['date','messages','sessions','tokens_in','tokens_out','tokens_reasoning','tokens_cache','tokens_total'];
    const lines=[head.join(',')].concat(rows.map(d=>[d.date,d.msgs,d.sessions,Math.round(d.ti),Math.round(d.to),Math.round(d.tr),Math.round(d.cache),Math.round(dayTotal(d))].join(',')));
    const b=new Blob([lines.join('\n')],{type:'text/csv'});
    const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='opencode-tokens-days.csv';a.click();
  }
}
function setExportOpen(open){
  $('expmenu').classList.toggle('open',open);
  $('export').setAttribute('aria-expanded',open?'true':'false');
}
$('export').onclick=e=>{e.stopPropagation();const open=!$('expmenu').classList.contains('open');setMenuOpen(false);setExportOpen(open);};
document.querySelectorAll('#expmenu button').forEach(b=>b.onclick=()=>{exportData(b.dataset.x);setExportOpen(false);});
/* command palette */
let PALETTE_OPEN=false,PAL_ITEMS=[],PAL_ACTIVE=0,paletteReturnFocus=null;
function palScore(item,q){
  if(!q)return 0;
  const label=(item.label||'').toLowerCase();
  const idx=label.indexOf(q);
  if(idx>=0)return 200-idx+(idx===0?60:0);
  const hay=((item.label||'')+' '+(item.hint||'')+' '+(item.terms||'')).toLowerCase();
  const at=hay.indexOf(q);
  return at>=0?80-Math.min(at,60):-1;
}
function buildPalette(query){
  const q=(query||'').trim().toLowerCase();
  const nav=[],ranges=[],actions=[],sessions=[];
  const tabs=['opencode','router','cline'].filter(t=>harnessVisible(t));
  if(harnessVisible('recap'))tabs.push('recap');
  tabs.forEach(t=>{
    nav.push({label:'Go to '+HARNESS_NAME[t],hint:'Harness',terms:HARNESS_NAME[t]+' harness '+t+' tab',
      run:()=>{closePalette();applyState(t,SUBVIEW[t]||VIEWS[t][0].id,null,'push');}});
    VIEWS[t].forEach(v=>{
      nav.push({label:HARNESS_NAME[t]+' · '+v.label,hint:'View',terms:HARNESS_NAME[t]+' '+v.label+' '+v.id+' view',
        run:()=>{closePalette();applyState(t,v.id,null,'push');}});
    });
  });
  if(TAB!=='recap'){
    [['7','7 days'],['30','30 days'],['90','90 days'],['all','All time']].forEach(([r,lbl])=>{
      ranges.push({label:'Range · '+lbl,hint:HARNESS_NAME[TAB],terms:'range period '+lbl+' '+r,
        key:tabRange(TAB)===r?'●':'',
        run:()=>{closePalette();setTabRange(TAB,r);syncPresetButtons();renderTab();syncHash('push');}});
    });
  }
  actions.push({label:'Toggle theme',hint:'Light / dark appearance',terms:'theme dark light appearance',key:'',
    run:()=>{closePalette();toggleTheme();}});
  actions.push({label:'Refresh data',hint:'Reload every harness',terms:'refresh reload update',key:'',
    run:()=>{closePalette();load();}});
  actions.push({label:'Export JSON',hint:'Full stats payload',terms:'export download json data',key:'',
    run:()=>{closePalette();exportData('json');}});
  actions.push({label:'Export CSV',hint:'Per-day usage table',terms:'export download csv days',key:'',
    run:()=>{closePalette();exportData('csv');}});
  actions.push({label:'Export Recap CSV',hint:'All harnesses per day',terms:'export download recap csv harnesses',key:'',
    run:()=>{closePalette();exportData('recap');}});
  actions.push({label:'Harness settings',hint:'Show or hide harnesses',terms:'settings harness toggle gear configure',key:'',
    run:()=>{closePalette();openSettingsMenu();}});
  actions.push({label:'Help',hint:'Keyboard shortcuts',terms:'help shortcuts keyboard guide',key:'?',
    run:()=>{closePalette();openHelp();}});
  if(q&&S&&S.sessions){
    S.sessions.forEach(s=>{
      const title=s.title||'Untitled',model=s.model||'';
      const hit=palScore({label:title,terms:model},q);
      if(hit<0)return;
      const tot=s.tokens_total||((s.tokens_input||0)+(s.tokens_output||0)+(s.tokens_reasoning||0)+(s.tokens_cache||0));
      sessions.push({label:title,hint:modelShort(model)+' · '+FN(tot)+' tokens',terms:model,_score:hit,
        run:()=>{closePalette();openSession(s);}});
    });
    sessions.sort((a,b)=>b._score-a._score);
    sessions.splice(6);
  }
  const groups=[{label:'Navigate',items:nav},{label:'Range',items:ranges},{label:'Actions',items:actions},{label:'Sessions',items:sessions}];
  groups.forEach(g=>{
    if(q){
      g.items.forEach(it=>{it._score=palScore(it,q);});
      g.items=g.items.filter(it=>it._score>=0).sort((a,b)=>b._score-a._score);
    }
  });
  return groups.filter(g=>g.items.length);
}
function updatePaletteActive(){
  const list=$('palette-list');if(!list)return;
  list.querySelectorAll('[data-pal-idx]').forEach(el=>{
    const on=Number(el.dataset.palIdx)===PAL_ACTIVE;
    el.classList.toggle('active',on);
    el.setAttribute('aria-selected',on?'true':'false');
  });
  const active=list.querySelector('[data-pal-idx="'+PAL_ACTIVE+'"]');
  if(active&&active.scrollIntoView)active.scrollIntoView({block:'nearest'});
}
function renderPalette(){
  const groups=buildPalette($('palette-input').value);
  let flat=[];
  groups.forEach(g=>g.items.forEach(it=>{it._i=flat.length;flat.push(it);}));
  PAL_ITEMS=flat;
  if(PAL_ACTIVE>=flat.length)PAL_ACTIVE=Math.max(0,flat.length-1);
  if(!flat.length){$('palette-list').innerHTML='<div class="pal-empty">No matches.</div>';return;}
  let html='';
  groups.forEach(g=>{
    if(!g.items.length)return;
    html+='<div class="pal-group"><div class="pal-group-label">'+ESC(g.label)+'</div>';
    g.items.forEach(it=>{
      const on=it._i===PAL_ACTIVE;
      html+='<button type="button" class="pal-item'+(on?' active':'')+'" data-pal-idx="'+it._i+'" role="option" aria-selected="'+(on?'true':'false')+'">'+
        '<span class="pal-main"><span class="pal-label">'+ESC(it.label)+'</span>'+(it.hint?'<span class="pal-hint">'+ESC(it.hint)+'</span>':'')+'</span>'+
        (it.key?'<span class="pal-key">'+ESC(it.key)+'</span>':'')+'</button>';
    });
    html+='</div>';
  });
  $('palette-list').innerHTML=html;
}
function paletteMove(delta){
  if(!PAL_ITEMS.length)return;
  PAL_ACTIVE=(PAL_ACTIVE+delta+PAL_ITEMS.length)%PAL_ITEMS.length;
  updatePaletteActive();
}
function paletteRun(i){
  const it=PAL_ITEMS[i];if(!it)return;
  it.run();
}
function openPalette(){
  if(PALETTE_OPEN)return;
  paletteReturnFocus=document.activeElement;
  PALETTE_OPEN=true;PAL_ACTIVE=0;
  $('palette').hidden=false;
  $('palette-btn').setAttribute('aria-expanded','true');
  $('rail-palette').setAttribute('aria-expanded','true');
  $('palette-input').setAttribute('aria-expanded','true');
  $('palette-input').value='';
  renderPalette();
  $('palette-input').focus();
}
function closePalette(){
  if(!PALETTE_OPEN)return;
  PALETTE_OPEN=false;
  $('palette').hidden=true;
  $('palette-btn').setAttribute('aria-expanded','false');
  $('rail-palette').setAttribute('aria-expanded','false');
  $('palette-input').setAttribute('aria-expanded','false');
  if(paletteReturnFocus&&paletteReturnFocus.focus)paletteReturnFocus.focus();
}
function togglePalette(){PALETTE_OPEN?closePalette():openPalette();}
$('palette-btn').onclick=openPalette;
$('rail-palette').onclick=openPalette;
$('palette-close').onclick=closePalette;
$('palette-input').addEventListener('input',()=>{PAL_ACTIVE=0;renderPalette();});
$('palette').addEventListener('click',e=>{if(e.target.id==='palette')closePalette();});
$('palette-list').addEventListener('mousemove',e=>{
  const b=e.target.closest?e.target.closest('[data-pal-idx]'):null;
  if(!b)return;
  const i=Number(b.dataset.palIdx);
  if(i!==PAL_ACTIVE){PAL_ACTIVE=i;updatePaletteActive();}
});
$('palette-list').addEventListener('click',e=>{
  const b=e.target.closest?e.target.closest('[data-pal-idx]'):null;
  if(b)paletteRun(Number(b.dataset.palIdx));
});
$('palette').addEventListener('keydown',e=>{
  if(e.key==='Escape'){e.preventDefault();e.stopPropagation();closePalette();return;}
  if(e.key==='ArrowDown'){e.preventDefault();paletteMove(1);return;}
  if(e.key==='ArrowUp'){e.preventDefault();paletteMove(-1);return;}
  if(e.key==='Enter'){
    if(e.target.id==='palette-close')return;
    e.preventDefault();paletteRun(PAL_ACTIVE);
  }
});
/* help */
function openHelp(){
  lastFocus=document.activeElement;
  $('help-overlay').classList.add('open');
  const x=$('help-close');if(x)x.focus();
}
function closeHelp(){closeOverlay('help-overlay');}
$('rail-help').onclick=openHelp;
$('help-close').onclick=closeHelp;
$('help-overlay').onclick=e=>{if(e.target.id==='help-overlay')closeHelp();};
/* stop the hidden server when this window closes or quits (Cmd+Q) */
function stopServer(){try{navigator.sendBeacon&&navigator.sendBeacon('/api/close?wid='+WID);}catch(e){try{fetch('/api/close?wid='+WID,{method:'POST',keepalive:true});}catch(_){}}}
window.addEventListener('beforeunload',stopServer);
window.addEventListener('pagehide',stopServer);
/* modals */
function closeOverlay(id){const ov=$(id);if(ov&&ov.classList.contains('open')){ov.classList.remove('open');if(lastFocus&&lastFocus.focus)lastFocus.focus();}}
$('overlay').onclick=e=>{if(e.target.id==='overlay')closeOverlay('overlay');};
$('modal').addEventListener('click',e=>{if(e.target.classList.contains('modal-x'))closeOverlay('overlay');});
document.addEventListener('keydown',e=>{
  if((e.metaKey||e.ctrlKey)&&!e.altKey&&(e.key==='k'||e.key==='K')){e.preventDefault();togglePalette();return;}
  if(e.key==='Escape'){
    if(PALETTE_OPEN){closePalette();return;}
    if($('help-overlay').classList.contains('open')){closeHelp();return;}
    closeOverlay('overlay');return;
  }
  if(e.key==='Tab'){
    const open=PALETTE_OPEN?$('palette'):($('help-overlay').classList.contains('open')?$('help-overlay'):($('overlay').classList.contains('open')?$('overlay'):null));
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
    const [r1,r2,r3]=await Promise.all([
      fetch('/api/stats?_='+Date.now(),{headers:{'X-Window-Id':WID}}),
      fetch('/api/router?_='+Date.now(),{headers:{'X-Window-Id':WID}}),
      fetch('/api/cline?_='+Date.now(),{headers:{'X-Window-Id':WID}}),
    ]);
    if(!r1.ok)throw new Error('HTTP '+r1.status);
    S=await r1.json();renderAll();
    try{if(r2.ok){R=await r2.json();if(TAB==='router')renderRouter();else renderRouter();}}catch(_){}
    try{if(r3.ok){C=await r3.json();if(TAB==='cline')renderCline();else if(C)renderCline();}}catch(_){}
    if(TAB==='recap')renderRecap();else updateTabCounts();
    $('status').textContent='Updated '+new Date().toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
  }catch(e){$('status').textContent='Failed: '+e.message;}
  finally{loading=false;$('refresh').disabled=false;}
}
$('refresh').onclick=load;
$('search').addEventListener('input',renderSessions);
document.addEventListener('visibilitychange',()=>{if(!document.hidden)load();});
setInterval(()=>{if(!document.hidden)load();},30000);
applyHarness();
try{$('recap-picker').value=RECAP_DATE;}catch(_){}
initRouting();
if(S){$('status').textContent='Updated just now';}
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
    cline_dir = None
    server = None
    quiet = False
    # When True (TUI mode) the background HTTP server outlives Chrome windows:
    # closing the web UI only closes that window, never the server.
    keep_alive = False

    def _cancel_close(self):
        _cancel_close_timer()

    @staticmethod
    def _close_now():
        if Handler.keep_alive:
            return
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
            try:
                cline_payload = json_html(cached_cline_stats(
                    self.cline_dir, db_worktrees(self.db_path)))
            except Exception as e:
                cline_payload = json_html(blank_cline_stats(error=str(e)))
            body = PAGE.replace("__DATA__", payload).replace("__ROUTER__", router_payload).replace("__CLINE__", cline_payload).encode("utf-8")
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
        elif path.startswith("/api/cline"):
            try:
                stats = cached_cline_stats(self.cline_dir,
                                           db_worktrees(self.db_path))
            except Exception as e:
                stats = blank_cline_stats(error=str(e))
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
            if Handler.keep_alive:
                return  # TUI owns the server; a closed Chrome window is harmless
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


def _fmt(n):
    try:
        n = float(n)
    except (TypeError, ValueError):
        return str(n)
    if abs(n) >= 1_000_000_000:
        return f"{n/1_000_000_000:.2f}B"
    if abs(n) >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if abs(n) >= 1_000:
        return f"{n/1_000:.1f}K"
    return f"{n:,.0f}"


def _safe_addstr(win, y, x, text, attr=0):
    h, w = win.getmaxyx()
    if y < 0 or y >= h or x < 0 or x >= w:
        return
    # Writing the bottom-right cell can raise curses.error (scroll); clip only
    # that row. Every other row may use the full width so header/box fills and
    # the rightmost column are not left blank.
    limit = max(0, w - x - 1) if y == h - 1 else max(0, w - x)
    try:
        win.addstr(y, x, str(text)[:limit], attr)
    except Exception:
        # curses.error on odd cells and encoding errors from odd DB text should
        # never take the whole TUI down.
        pass


def open_chrome_window(url):
    """Open `url` in the same dedicated Chrome app window the old launcher used.

    Returns True when a browser was launched. On non-macOS (or when Chrome is
    not installed) fall back to the system default browser.
    """
    import webbrowser

    if url and sys.platform == "darwin":
        chrome = Path("/Applications/Google Chrome.app")
        if chrome.is_dir():
            profile = str(Path.home() / ".cache" / "ocd-dashboard-chrome")
            try:
                subprocess.Popen(
                    [
                        "open", "-na", str(chrome), "--args",
                        f"--app={url}",
                        f"--user-data-dir={profile}",
                        "--window-size=1280,1600",
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                return True
            except Exception:
                pass
    if not url:
        return False
    try:
        return bool(webbrowser.open(url))
    except Exception:
        return False


TUI_RANGES = (("14d", 14), ("30d", 30), ("90d", 90), ("All", 0))


def tui_layout(w, h):
    """Responsive TUI layout for a w x h terminal (pure, no curses).

    Returns a dict with `mode`, `cards` (rect list) and rects for the chart,
    models, sessions and records panels. Panels are omitted (None) when the
    terminal is too small, so the renderer degrades instead of crashing.
    A rect is (y, x, height, width).
    """
    layout = {
        "mode": "compact",
        "cards": [],
        "chart": None,
        "models": None,
        "sessions": None,
        "records": None,
        "card_h": 5,
    }
    if w < 60 or h < 20:
        return layout
    content_x = 1
    content_w = w - 2
    content_bottom = h - 3  # footer separator at h-2, footer at h-1
    y = 3
    if content_bottom < y + 6:
        return layout

    if w >= 110 and h >= 30:
        layout["mode"] = "wide"
        layout["card_h"] = 6
        n_cards = 5  # Today + 4 range/context cards
    elif w >= 84 and h >= 26:
        layout["mode"] = "medium"
        layout["card_h"] = 6
        n_cards = 4  # Today + 3 range/context cards
    else:
        layout["mode"] = "narrow"
        layout["card_h"] = 4
        n_cards = 4 if h >= 28 else 2

    # ---- KPI card row(s) ----
    rows = 2 if (layout["mode"] == "narrow" and n_cards == 4) else 1
    per_row = n_cards // rows if rows else n_cards
    gap = 1
    card_h = layout["card_h"]
    card_w = (content_w - gap * (per_row - 1)) // per_row
    rest = content_w - (card_w * per_row + gap * (per_row - 1))
    for r in range(rows):
        yy = y + r * (card_h + 1)
        if yy + card_h - 1 > content_bottom:
            break
        for c in range(per_row):
            cw = card_w + (rest if c == per_row - 1 else 0)
            layout["cards"].append((yy, content_x + c * (card_w + gap), card_h, cw))
    if layout["cards"]:
        last = layout["cards"][-1]
        # cards occupy y .. y+card_h-1, so the next free row is y+card_h
        y = last[0] + last[2]

    avail = content_bottom - y + 1
    if avail < 4:
        return layout

    def side_by_side(yy, hh, total_w, frac):
        left_w = int(total_w * frac)
        left_w = max(10, left_w)
        if total_w - left_w - 1 < 12:
            left_w = max(10, total_w - 13)
        right_w = total_w - left_w - 1
        return (yy, content_x, hh, left_w), (yy, content_x + left_w + 1, hh, right_w)

    mode = layout["mode"]
    if mode in ("wide", "medium") and avail >= 16:
        # breathing room between the KPI cards and the chart row
        y += 1
        avail -= 1
    if mode == "wide":
        mid_h = min(14, max(7, (avail * 45) // 100))
        if avail - mid_h - 1 < 3:
            mid_h = max(5, avail - 4)
        chart, models = side_by_side(y, mid_h, content_w, 0.58)
        layout["chart"] = chart
        layout["models"] = models
        y += mid_h + 1
        bottom_h = content_bottom - y + 1
        if bottom_h >= 4:
            sess, rec = side_by_side(y, bottom_h, content_w, 0.56)
            layout["sessions"] = sess
            layout["records"] = rec
        elif bottom_h >= 2:
            layout["sessions"] = (y, content_x, bottom_h, content_w)
    elif mode == "medium":
        mid_h = min(12, max(6, (avail * 45) // 100))
        chart, models = side_by_side(y, mid_h, content_w, 0.55)
        layout["chart"] = chart
        layout["models"] = models
        y += mid_h + 1
        bottom_h = content_bottom - y + 1
        if bottom_h >= 4:
            layout["sessions"] = (y, content_x, bottom_h, content_w)
    else:  # narrow: stacked panels
        # Reserve a usable sessions table (header + separator + >=1 data row)
        # before sizing the chart/models panels above it.
        if avail >= 15:
            budget = avail - 7  # two 1-row gaps + 5 session rows
            chart_h = min(8, max(4, (budget * 55) // 100))
            models_h = min(7, max(4, budget - chart_h))
            sessions_h = avail - chart_h - models_h - 2
            layout["chart"] = (y, content_x, chart_h, content_w)
            layout["models"] = (y + chart_h + 1, content_x, models_h, content_w)
            layout["sessions"] = (
                y + chart_h + models_h + 2, content_x, sessions_h, content_w)
        else:
            chart_h = min(6, max(4, avail // 3))
            sessions_h = avail - chart_h - 1
            layout["chart"] = (y, content_x, chart_h, content_w)
            if sessions_h >= 4:
                layout["sessions"] = (y + chart_h + 1, content_x, sessions_h, content_w)
    return layout


def tui_period_totals(stats, days):
    """Sum day rows from a stats payload over the last `days` (0 = all days).

    Works for OpenCode, Codex and Cline day rows: ti/to/tr/cache and the
    optional reqs/ok/err/err429/cost/what_if fields are summed, while
    tokens_total is always ti+to+tr+cache.
    """
    out = {
        "ti": 0.0, "to": 0.0, "tr": 0.0, "cache": 0.0, "tokens_total": 0.0,
        "sessions": 0, "messages": 0, "cost": 0.0, "what_if": 0.0,
        "ok": 0, "err": 0, "err429": 0, "days_used": 0, "vmax": 0.0,
    }
    rows = (stats or {}).get("days") or []
    if days and len(rows) > days:
        rows = rows[-days:]
    for d in rows:
        if not isinstance(d, dict):
            continue
        ti = num(d.get("ti"))
        to = num(d.get("to"))
        tr = num(d.get("tr"))
        ca = num(d.get("cache"))
        out["ti"] += ti
        out["to"] += to
        out["tr"] += tr
        out["cache"] += ca
        tot = ti + to + tr + ca
        out["tokens_total"] += tot
        out["vmax"] = max(out["vmax"], tot)
        out["sessions"] += int(num(d.get("sessions")))
        out["messages"] += int(num(d.get("msgs")))
        out["cost"] += num(d.get("cost"))
        out["what_if"] += num(d.get("what_if"))
        out["ok"] += int(num(d.get("ok")))
        out["err"] += int(num(d.get("err")))
        out["err429"] += int(num(d.get("err429")))
        out["days_used"] += 1
    return out


def tui_scale_blocks(values, width):
    """Aggregate `values` into exactly `width` columns (sum per column).

    Widening returns the values unchanged; narrowing buckets consecutive
    values so long ranges fit a small chart without dropping data.
    """
    if width <= 0 or not values:
        return []
    vals = [num(v) for v in values]
    if width >= len(vals):
        return vals
    out = []
    for i in range(width):
        lo = (i * len(vals)) // width
        hi = ((i + 1) * len(vals)) // width
        out.append(sum(vals[lo:hi]) if hi > lo else 0.0)
    return out


def tui_heat(value, vmax):
    """Bucket a value into 0..4 heat levels (0 = empty/invalid)."""
    v = num(value)
    m = num(vmax)
    if v <= 0 or m <= 0:
        return 0
    frac = v / m
    if frac >= 0.75:
        return 4
    if frac >= 0.5:
        return 3
    if frac >= 0.25:
        return 2
    return 1


def tui_fit(text, width, suffix="…"):
    """Truncate text to `width` display chars, appending `suffix` if needed."""
    s = "" if text is None else str(text)
    if width <= 0:
        return ""
    if len(s) <= width:
        return s
    if width <= len(suffix):
        return suffix[:width]
    return s[: width - len(suffix)] + suffix


def tui_wrap(text, width):
    """Word-wrap text into lines of at most `width` chars (never drops chars)."""
    if width <= 0:
        return []
    lines = []
    for para in str(text if text is not None else "").splitlines() or [""]:
        words = para.split()
        if not words:
            lines.append("")
            continue
        cur = ""
        for word in words:
            while len(word) > width:
                if cur:
                    lines.append(cur)
                    cur = ""
                lines.append(word[:width])
                word = word[width:]
            if not cur:
                cur = word
            elif len(cur) + 1 + len(word) <= width:
                cur += " " + word
            else:
                lines.append(cur)
                cur = word
        if cur:
            lines.append(cur)
    return lines


def tui_filter_sessions(rows, text, fields=("title", "model")):
    """Case-insensitive substring filter over selected dict fields."""
    q = (text or "").strip().lower()
    if not q:
        return list(rows or [])
    out = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        hay = " ".join(str(row.get(f, "")) for f in fields).lower()
        if q in hay:
            out.append(row)
    return out


def tui_model_badge(model_row, cost_idx=7):
    """'free' / '$0' / '' badge for a model row (same shape as stats).

    `cost_idx` is 7 for OpenCode rows (actual session cost) and 12 for
    Codex/Cline rows (Codex: what-if paid cost; Cline: 0, it records no
    per-model cost). Index 11 is the free flag when the payload has one.

    Legacy payloads may still contain a synthetic `Other` row; new payloads
    do not. Such a row never gets a free/$0 badge: it is recovered usage, not
    a free model.
    """
    if not isinstance(model_row, (list, tuple)) or not model_row:
        return ""
    name = str(model_row[0] or "")
    if name.strip().lower() == "other":
        return ""
    cost = num(model_row[cost_idx]) if len(model_row) > cost_idx else 0.0
    free_flag = bool(model_row[11]) if len(model_row) > 11 else False
    if "free" in name.lower() or free_flag:
        return "free"
    if cost <= 0:
        return "$0"
    return ""


def tui_recovered_badge(model_row):
    """'recovered' for a legacy synthetic `Other` row, '' otherwise.

    New payloads report recovered history as a top-level `recovered` block,
    not as a model row; this remains for old saved payloads.
    """
    if not isinstance(model_row, (list, tuple)) or not model_row:
        return ""
    return "recovered" if str(model_row[0] or "").strip().lower() == "other" else ""


def tui_fmt_cost(value):
    """Compact cost string: $0, sub-cent precision, or 2 decimals."""
    c = num(value)
    if c <= 0:
        return "$0"
    if c < 0.01:
        return f"${c:.4f}"
    return f"${c:,.2f}"


def tui_free_model_count(models, cost_idx=7):
    """Number of models that are free or carry zero recorded cost.

    Pass `cost_idx=12` for Codex/Cline payloads so the count is not driven by
    the error count at index 7.
    """
    n = 0
    for row in models or []:
        if tui_model_badge(row, cost_idx):
            n += 1
    return n


TUI_HARNESS_KEYS = ("opencode", "router", "cline")
TUI_HARNESS_NAMES = ("OpenCode", "Codex", "Cline")
TUI_HARNESS_SHORT = ("OC", "CX", "CL")

# First run is OpenCode-first: the other harnesses stay one `h` away instead
# of diluting the default view.
TUI_DEFAULT_VISIBILITY = {"opencode": True, "router": False, "cline": False}


def tui_initial_visibility(pref):
    """Visibility for a fresh ledger (None) or an explicit saved preference.

    Only a missing preference uses the OpenCode-first default. Any stored
    value (including a corrupt one) goes through the normalizer, so explicit
    saved maps always win and legacy all-visible values keep their meaning.
    """
    if pref is None:
        return dict(TUI_DEFAULT_VISIBILITY)
    return tui_harness_visibility(pref)


def tui_tab_items(visible):
    """Visible tab bar items as (tab_index, full_name, short_name)."""
    vis = tui_harness_visibility(visible)
    items = [
        (i, TUI_HARNESS_NAMES[i], TUI_HARNESS_SHORT[i])
        for i, key in enumerate(TUI_HARNESS_KEYS)
        if vis[key]
    ]
    items.append((3, "Recap", "RC"))
    return items


def tui_tab_for_key(key, visible):
    """Tab index for a digit key over the visible tab bar, or None."""
    if not isinstance(key, int) or not (ord("1") <= key <= ord("9")):
        return None
    items = tui_tab_items(visible)
    idx = key - ord("1")
    if 0 <= idx < len(items):
        return items[idx][0]
    return None


def _tui_token_buckets():
    return {"input": 0.0, "output": 0.0, "reasoning": 0.0,
            "cache": 0.0, "cost": 0.0, "total": 0.0}


def tui_token_split(stats):
    """Separate recorded usage from recovered-history metadata.

    `recorded` is what the live ledger actually recorded; `recovered` is the
    historical backfill exposed as metadata only, so totals never double count
    it. Older payloads folded the recovery into `totals`; those are unfolded
    here unless `included_in_totals` is false. Never raises.
    """
    out = {"recorded": _tui_token_buckets(),
           "recovered": _tui_token_buckets(),
           "has_recovered": False}
    if not isinstance(stats, dict):
        return out
    totals = stats.get("totals")
    if not isinstance(totals, dict):
        totals = {}
    rec = stats.get("recovered")
    if not isinstance(rec, dict):
        rec = {}

    recovered = _tui_token_buckets()
    recovered["input"] = num(rec.get("tokens_input"))
    recovered["output"] = num(rec.get("tokens_output"))
    recovered["reasoning"] = num(rec.get("tokens_reasoning"))
    recovered["cache"] = num(rec.get("tokens_cache"))
    if not recovered["cache"]:
        recovered["cache"] = (num(rec.get("tokens_cache_read"))
                              + num(rec.get("tokens_cache_write")))
    recovered["total"] = num(rec.get("tokens_total"))
    if recovered["total"] <= 0:
        recovered["total"] = (recovered["input"] + recovered["output"]
                              + recovered["reasoning"] + recovered["cache"])

    recorded = _tui_token_buckets()
    recorded["input"] = num(totals.get("tokens_input"))
    recorded["output"] = num(totals.get("tokens_output"))
    recorded["reasoning"] = num(totals.get("tokens_reasoning"))
    recorded["cache"] = (num(totals.get("tokens_cache_read"))
                         + num(totals.get("tokens_cache_write")))
    recorded["cost"] = num(totals.get("cost"))
    has_recorded_total = "tokens_total_recorded" in totals
    recorded["total"] = num(totals.get("tokens_total_recorded") if has_recorded_total
                            else totals.get("tokens_total"))

    # Legacy payload: `totals` already include the recovered backfill.
    if (not has_recorded_total and recovered["total"] > 0
            and bool(rec.get("included_in_totals", True))):
        for key in ("input", "output", "reasoning", "cache"):
            recorded[key] = max(0.0, recorded[key] - recovered[key])
        recorded["total"] = max(0.0, recorded["total"] - recovered["total"])

    out["recorded"] = recorded
    out["recovered"] = recovered
    out["has_recovered"] = recovered["total"] > 0
    return out


def tui_visible_models(models):
    """Model rows that are real models (recovered metadata is not a model)."""
    if not models:
        return []
    out = []
    for row in models:
        if not isinstance(row, (list, tuple)):
            continue
        if tui_recovered_badge(row):
            continue
        out.append(row)
    return out


def tui_forecast(rows, window=7, horizon=7, today=None):
    """Zero-filled calendar forecast from day rows (never raises).

    Averages over exactly `window` calendar days ending at the latest valid
    date <= `today`, then projects `horizon` days at that average. `basis` is
    the number of days in the window that actually had activity.
    """
    win = int(num(window))
    hor = int(num(horizon))
    out = {"avg": 0.0, "projected": 0.0, "recorded": 0.0,
           "window": max(0, win), "horizon": max(0, hor),
           "end": "", "basis": 0, "ok": False}
    if win <= 0 or hor < 0:
        return out

    if today is None:
        end_cap = datetime.now().date()
    elif isinstance(today, datetime):
        end_cap = today.date()
    else:
        try:
            end_cap = datetime.strptime(str(today)[:10], "%Y-%m-%d").date()
        except (TypeError, ValueError):
            end_cap = datetime.now().date()

    by_day = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        try:
            day = datetime.strptime(str(row.get("date"))[:10], "%Y-%m-%d").date()
        except (TypeError, ValueError):
            continue
        if day > end_cap:
            continue
        tokens = (num(row.get("ti")) + num(row.get("to"))
                  + num(row.get("tr")) + num(row.get("cache")))
        by_day[day] = by_day.get(day, 0.0) + tokens
    if not by_day:
        return out

    end = max(by_day)
    start = end - timedelta(days=win - 1)
    recorded = 0.0
    basis = 0
    for i in range(win):
        day = start + timedelta(days=i)
        if day in by_day:
            recorded += by_day[day]
            basis += 1
    avg = recorded / win
    out.update({
        "avg": avg,
        "projected": avg * hor,
        "recorded": recorded,
        "end": end.isoformat(),
        "basis": basis,
        "ok": True,
    })
    return out


def tui_chart_header(total, vmax, n, fc, width):
    """One-line chart header that always fits `width`."""
    width = max(0, int(num(width)))
    fc = fc if isinstance(fc, dict) else {}
    win = int(num(fc.get("window", 7))) or 7
    hor = int(num(fc.get("horizon", 7))) or 7
    legacy = f"total {_fmt(total)}  peak {_fmt(vmax)}  days {int(num(n))}"
    if not fc.get("ok", True):
        text = legacy
    elif width >= 50:
        text = (f"total {_fmt(total)}  {win}d avg {_fmt(num(fc.get('avg')))}"
                f"  next {hor}d ~{_fmt(num(fc.get('projected')))}")
    elif width >= 30:
        text = (f"total {_fmt(total)}  next {hor}d ~"
                f"{_fmt(num(fc.get('projected')))}")
    else:
        text = legacy
    return tui_fit(text, width)


def tui_insight_rows(stats):
    """Insight modal rows from structured cards or legacy strings."""
    rows = []
    if not isinstance(stats, dict):
        return rows
    split = tui_token_split(stats)
    if split["has_recovered"]:
        rows.append({
            "title": "Recovered history",
            "value": _fmt(split["recovered"]["total"]),
            "detail": "metadata only — not in recorded totals",
        })
    cards = stats.get("insight_cards")
    if isinstance(cards, (list, tuple)):
        for card in cards:
            if not isinstance(card, dict):
                continue
            title = card.get("title")
            value = card.get("value")
            detail = card.get("detail")
            if not any(str(v).strip() for v in (title, value, detail) if v is not None):
                continue
            rows.append({
                "title": "" if title is None else str(title),
                "value": "" if value is None else str(value),
                "detail": "" if detail is None else str(detail),
            })
    else:
        legacy = stats.get("insights")
        if isinstance(legacy, (list, tuple)):
            for text in legacy:
                if text is None:
                    continue
                text = str(text).strip()
                if not text:
                    continue
                rows.append({"title": "", "value": "", "detail": text})
    return rows[:12]


def tui_today_totals(stats):
    """Tokens consumed today (local date) from a stats payload's day rows.

    Always reads the row whose date is today's local date, so it is independent
    of the `t` range toggle. Missing payloads/days return zeros.
    """
    today = datetime.now().date().isoformat()
    out = {"ti": 0.0, "to": 0.0, "tr": 0.0, "cache": 0.0, "tokens_total": 0.0,
           "sessions": 0, "messages": 0, "found": False}
    for d in (stats or {}).get("days") or []:
        if not isinstance(d, dict) or str(d.get("date") or "") != today:
            continue
        out["ti"] = num(d.get("ti"))
        out["to"] = num(d.get("to"))
        out["tr"] = num(d.get("tr"))
        out["cache"] = num(d.get("cache"))
        out["tokens_total"] = out["ti"] + out["to"] + out["tr"] + out["cache"]
        out["sessions"] = int(num(d.get("sessions")))
        out["messages"] = int(num(d.get("msgs")))
        out["found"] = True
        break
    return out


def tui_harness_visibility(value):
    """Normalize a stored harness_visibility value to {key: bool}.

    Missing keys default to visible; corrupt values and an all-hidden map fall
    back to everything visible, so a bad ledger can never blank the TUI.
    """
    out = {key: True for key in TUI_HARNESS_KEYS}
    data = value
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except (TypeError, ValueError):
            data = None
    if isinstance(data, dict):
        for key in TUI_HARNESS_KEYS:
            if key in data:
                out[key] = bool(data[key])
    if not any(out.values()):
        return {key: True for key in TUI_HARNESS_KEYS}
    return out


def tui_tab_order(visible):
    """Tab indices to cycle through: visible harness tabs, then Recap (3)."""
    vis = tui_harness_visibility(visible)
    return [i for i, key in enumerate(TUI_HARNESS_KEYS) if vis[key]] + [3]


def tui_next_tab(tab, delta, visible):
    """Next tab index in the visible order (wraps); first visible when hidden."""
    order = tui_tab_order(visible)
    if tab not in order:
        return order[0]
    return order[(order.index(tab) + delta) % len(order)]


def run_tui(args):
    import curses
    import locale

    global RECOVERED_TOKENS
    if hasattr(args, "recovered_tokens"):
        RECOVERED_TOKENS = args.recovered_tokens

    Handler.db_path = args.db
    Handler.router_events = args.router_events
    Handler.router_limits = args.router_limits
    Handler.cline_dir = args.cline_dir
    Handler.quiet = True
    # The TUI owns the server: closing the Chrome window must not stop it.
    Handler.keep_alive = True

    # Background web server so the traditional Chrome UI can be opened on demand.
    # If the requested port is busy, walk forward through the next 10 ports.
    server = None
    url = None
    last_error = None
    for offset in range(11):
        port = args.port + offset
        if port > 65535:
            break
        try:
            server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
            url = f"http://127.0.0.1:{port}"
            break
        except OSError as e:
            last_error = e
    Handler.server = server
    srv_thread = None
    if server is not None:
        srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
        srv_thread.start()
    else:
        print(
            f"warning: could not bind any port {args.port}-{args.port + 10} "
            f"({last_error}); Chrome UI disabled",
            file=sys.stderr,
        )

    # ---- glyph set: UTF-8 where the terminal can take it, ASCII otherwise ----
    def _init_glyphs():
        enc = ""
        try:
            enc = (getattr(sys.stdout, "encoding", "") or "").lower()
        except Exception:
            enc = ""
        pref = ""
        try:
            pref = (locale.getpreferredencoding(False) or "").lower()
        except Exception:
            pref = ""
        if "utf" in enc or (not enc and "utf" in pref):
            return {
                "tl": "╭", "tr": "╮", "bl": "╰", "br": "╯", "h": "─", "v": "│",
                "bar": "█", "spark": [" ", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"],
                "heat": ["░", "▒", "▓", "█"],
                "dot": "●", "arrow": "▶", "ell": "…", "mid": "·", "sep": "  ·  ",
                "spin": ["◐", "◓", "◑", "◒"],
            }
        return {
            "tl": "+", "tr": "+", "bl": "+", "br": "+", "h": "-", "v": "|",
            "bar": "#", "spark": [" ", ".", ":", "-", "=", "+", "*", "#", "#"],
            "heat": [".", ":", "+", "#"],
            "dot": "*", "arrow": ">", "ell": "..", "mid": "|", "sep": "  |  ",
            "spin": ["|", "/", "-", "\\"],
        }

    G = _init_glyphs()

    def load_harness_visibility():
        """Ledger-stored visibility; a fresh ledger shows OpenCode only.

        The default is never written back: an untouched ledger keeps reading
        as a first run, while any saved map (including a legacy all-visible
        one) wins over the default.
        """
        try:
            if not Path(args.db).exists():
                return tui_initial_visibility(None)
            return tui_initial_visibility(
                ledger_get_pref(args.db, "harness_visibility"))
        except Exception:
            return tui_initial_visibility(None)

    state = {
        "tab": 0, "S": None, "R": None, "C": None,
        "msg": "", "status": "", "last": 0.0, "url": url,
        "sel": [0, 0, 0, 0],
        "modal": None, "filter": "", "search": None,
        "range_idx": 1, "loading": False, "spinner": 0,
        "visible": load_harness_visibility(),
    }
    render = {"pal": None}

    def visible_order():
        return tui_tab_order(state["visible"])

    def ensure_visible_tab():
        order = visible_order()
        if state["tab"] not in order:
            state["tab"] = order[0]

    ensure_visible_tab()

    def save_harness_visibility():
        try:
            ledger_set_pref(
                args.db, "harness_visibility",
                {key: bool(state["visible"].get(key, True)) for key in TUI_HARNESS_KEYS},
            )
        except Exception:
            pass

    def toggle_harness(idx):
        if idx < 0 or idx >= len(TUI_HARNESS_KEYS):
            return
        key = TUI_HARNESS_KEYS[idx]
        visible = dict(state["visible"])
        shown = sum(1 for k in TUI_HARNESS_KEYS if visible.get(k, True))
        if visible.get(key, True) and shown <= 1:
            state["status"] = "keep one harness visible"
            return
        visible[key] = not visible.get(key, True)
        state["visible"] = visible
        save_harness_visibility()
        ensure_visible_tab()
        state["status"] = (f"{TUI_HARNESS_NAMES[idx]} "
                           f"{'shown' if visible[key] else 'hidden'} (saved)")

    def tab_step(delta):
        state["tab"] = tui_next_tab(state["tab"], delta, state["visible"])

    def clear_caches():
        with LOCK:
            LOCAL_CACHE.clear()
            ROUTER_CACHE.clear()
            CLINE_CACHE.clear()

    def load_data():
        wt = db_worktrees(args.db)
        try:
            state["S"] = cached_local_stats(args.db)
        except Exception as e:
            state["S"] = None
            state["msg"] = f"OpenCode error: {e}"
        try:
            state["R"] = cached_router_stats(args.router_events, args.router_limits, wt)
        except Exception:
            state["R"] = None
        try:
            state["C"] = cached_cline_stats(args.cline_dir, wt)
        except Exception:
            state["C"] = None
        state["last"] = time.time()
        state["status"] = ""
        if state["url"]:
            state["msg"] = "updated " + datetime.now().strftime("%H:%M:%S")
        else:
            state["msg"] = f"no web UI (ports {args.port}-{args.port + 10} busy)"

    # ---------------------------------------------------------------- palette
    def _init_palette():
        keys = ("hdr", "hdr_dim", "acc", "acc2", "good", "warn", "bad", "mag",
                "dim", "muted", "border", "value", "sel", "tab")
        pal = {k: 0 for k in keys}
        pal["heat"] = []
        try:
            curses.start_color()
            try:
                curses.use_default_colors()
            except Exception:
                pass
        except Exception:
            return pal
        if getattr(curses, "COLORS", 0) >= 256:
            spec = {
                "hdr": (231, 17), "hdr_dim": (146, 17),
                "acc": (51, -1), "acc2": (44, -1), "good": (48, -1),
                "warn": (214, -1), "bad": (203, -1), "mag": (213, -1),
                "dim": (245, -1), "muted": (240, -1), "border": (60, -1),
                "value": (231, -1), "sel": (231, 24), "tab": (16, 51),
            }
            heat = [(236, -1), (24, -1), (31, -1), (45, -1), (51, -1)]
        else:
            C = curses
            spec = {
                "hdr": (C.COLOR_WHITE, C.COLOR_BLUE), "hdr_dim": (C.COLOR_CYAN, C.COLOR_BLUE),
                "acc": (C.COLOR_CYAN, -1), "acc2": (C.COLOR_GREEN, -1),
                "good": (C.COLOR_GREEN, -1), "warn": (C.COLOR_YELLOW, -1),
                "bad": (C.COLOR_RED, -1), "mag": (C.COLOR_MAGENTA, -1),
                "dim": (C.COLOR_WHITE, -1), "muted": (C.COLOR_WHITE, -1),
                "border": (C.COLOR_BLUE, -1), "value": (C.COLOR_WHITE, -1),
                "sel": (C.COLOR_BLACK, C.COLOR_CYAN), "tab": (C.COLOR_BLACK, C.COLOR_CYAN),
            }
            heat = [(C.COLOR_BLUE, -1), (C.COLOR_CYAN, -1), (C.COLOR_GREEN, -1),
                    (C.COLOR_YELLOW, -1), (C.COLOR_MAGENTA, -1)]
        idx = 1
        for key, (fg, bg) in spec.items():
            try:
                curses.init_pair(idx, fg, bg)
                pal[key] = curses.color_pair(idx)
            except Exception:
                pal[key] = 0
            idx += 1
        for fg, bg in heat:
            try:
                curses.init_pair(idx, fg, bg)
                pal["heat"].append(curses.color_pair(idx))
            except Exception:
                pal["heat"].append(0)
            idx += 1
        return pal

    # -------------------------------------------------------- draw primitives
    def fill(win, y, x, hh, ww, attr=0):
        if hh <= 0 or ww <= 0:
            return
        row = " " * ww
        for i in range(hh):
            _safe_addstr(win, y + i, x, row, attr)

    def box(win, rect, title="", border_attr=0, title_attr=0):
        y, x, hh, ww = rect
        if hh < 2 or ww < 2:
            return
        _safe_addstr(win, y, x, G["tl"] + G["h"] * (ww - 2) + G["tr"], border_attr)
        _safe_addstr(win, y + hh - 1, x, G["bl"] + G["h"] * (ww - 2) + G["br"], border_attr)
        for i in range(1, hh - 1):
            _safe_addstr(win, y + i, x, G["v"], border_attr)
            _safe_addstr(win, y + i, x + ww - 1, G["v"], border_attr)
        if title and ww > 6:
            t = " " + tui_fit(title, ww - 6, G["ell"]) + " "
            _safe_addstr(win, y, x + max(2, (ww - len(t)) // 2), t, title_attr or border_attr)

    def put(win, y, x, text, attr=0):
        _safe_addstr(win, y, x, text, attr)

    def pct(part, whole):
        return (part / whole * 100) if whole else 0.0

    # ---------------------------------------------------------------- cards
    def _no_data_card(title, pal):
        return {
            "title": title,
            "value": "no data",
            "value_attr": pal["warn"] | curses.A_BOLD,
            "details": [("hint", state.get("msg") or "waiting…")],
        }

    def build_cards(tab, pal):
        BOLD = curses.A_BOLD
        days = TUI_RANGES[state["range_idx"]][1]
        if tab == 0:
            S = state["S"]
            if not S:
                return [_no_data_card("OpenCode", pal)]
            p = tui_period_totals(S, days)
            split = tui_token_split(S)
            rec_tokens = split["recorded"]
            rec_meta = split["recovered"]
            if days:
                # Day rows never include recovered history, so the range view
                # is recorded by construction.
                tok_total = p["tokens_total"]
                tok_bar = [("acc", p["ti"]), ("acc2", p["to"]),
                           ("mag", p["tr"]), ("warn", p["cache"])]
                tok_details = [
                    ("input", _fmt(p["ti"])),
                    ("output", _fmt(p["to"])),
                    ("reasoning", _fmt(p["tr"])),
                    ("cache", _fmt(p["cache"])),
                ]
            else:
                # All: use the split so ledger-only recorded sessions count
                # even when they have no day rows.
                tok_total = rec_tokens["total"]
                tok_bar = [("acc", rec_tokens["input"]),
                           ("acc2", rec_tokens["output"]),
                           ("mag", rec_tokens["reasoning"]),
                           ("warn", rec_tokens["cache"])]
                tok_details = [
                    ("input", _fmt(rec_tokens["input"])),
                    ("output", _fmt(rec_tokens["output"])),
                    ("reasoning", _fmt(rec_tokens["reasoning"])),
                    ("cache", _fmt(rec_tokens["cache"])),
                ]
            tokens_card = {
                "title": "Tokens · recorded",
                "value": _fmt(tok_total),
                "value_attr": pal["acc"] | BOLD,
                "bar": tok_bar,
                "details": tok_details,
            }
            if split["has_recovered"]:
                tokens_card["subvalue"] = f"rec {_fmt(rec_meta['total'])}"
                tokens_card["subvalue_attr"] = pal["warn"] | BOLD
            rec = (S.get("records") or {})
            streaks = rec.get("streaks") or {}
            big = rec.get("biggest_day") or {}
            free_n = tui_free_model_count(S.get("models"))
            today = tui_today_totals(S)
            return [
                {
                    "title": "Today (recorded)",
                    "value": _fmt(today["tokens_total"]),
                    "value_attr": pal["mag"] | BOLD,
                    "details": [
                        ("7d", _fmt(tui_period_totals(S, 7)["tokens_total"])),
                        ("30d", _fmt(tui_period_totals(S, 30)["tokens_total"])),
                        ("sessions", str(today["sessions"])),
                    ],
                },
                tokens_card,
                {
                    "title": "Sessions",
                    "value": str(p["sessions"]),
                    "value_attr": pal["value"] | BOLD,
                    "details": [
                        ("messages", str(p["messages"])),
                        ("avg / session", _fmt(p["tokens_total"] / p["sessions"] if p["sessions"] else 0)),
                        ("streak", f"{streaks.get('current', 0)}d"),
                    ],
                },
                {
                    "title": "Cost",
                    "value": tui_fmt_cost(p["cost"]),
                    "value_attr": pal["good"] | BOLD,
                    "details": [
                        ("cache rate", f"{cache_rate(p['ti'], p['cache']):.1f}%"),
                        ("free models", str(free_n)),
                        ("cost / 1M tok", tui_fmt_cost(p["cost"] / (p["tokens_total"] / 1e6) if p["tokens_total"] else 0)),
                    ],
                },
                {
                    "title": "Activity",
                    "value": f"{streaks.get('current', 0)}d",
                    "value_attr": pal["mag"] | BOLD,
                    "details": [
                        ("longest", f"{streaks.get('longest', 0)}d"),
                        ("best day", str(big.get("date") or "—")),
                        ("best day tokens", _fmt(big.get("total", 0))),
                    ],
                },
            ]
        if tab == 1:
            R = state["R"]
            if not R:
                return [_no_data_card("Codex", pal)]
            p = tui_period_totals(R, days)
            top = (R.get("models") or [None])[0]
            today = tui_today_totals(R)
            return [
                {
                    "title": "Today",
                    "value": _fmt(today["tokens_total"]),
                    "value_attr": pal["mag"] | BOLD,
                    "details": [
                        ("7d", _fmt(tui_period_totals(R, 7)["tokens_total"])),
                        ("30d", _fmt(tui_period_totals(R, 30)["tokens_total"])),
                        ("requests", str(today["messages"])),
                    ],
                },
                {
                    "title": "Requests",
                    "value": str(p["messages"]),
                    "value_attr": pal["acc"] | BOLD,
                    "details": [
                        ("ok", str(p["ok"])),
                        ("errors", str(p["err"])),
                        ("429s", str(p["err429"])),
                    ],
                },
                {
                    "title": "Tokens",
                    "value": _fmt(p["tokens_total"]),
                    "value_attr": pal["value"] | BOLD,
                    "bar": [("acc", p["ti"]), ("acc2", p["to"])],
                    "details": [
                        ("input incl cache", _fmt(p["ti"])),
                        ("output", _fmt(p["to"])),
                        ("cache", _fmt(p["cache"])),
                    ],
                },
                {
                    "title": "Errors",
                    "value": str(p["err"]),
                    "value_attr": (pal["bad"] if p["err"] else pal["good"]) | BOLD,
                    "details": [
                        ("429 rate-limit", str(p["err429"])),
                        ("unmetered", str(((R.get("totals") or {}).get("unmetered", 0)))),
                        ("models", str(len(R.get("models") or []))),
                    ],
                },
                {
                    "title": "What-if cost",
                    "value": tui_fmt_cost(p["what_if"]),
                    "value_attr": pal["good"] | BOLD,
                    "details": [
                        ("requests", str(p["messages"])),
                        ("top model", tui_fit(router_short(str(top[0]) if top else "—"), 18, G["ell"])),
                        ("avg / req", _fmt(p["tokens_total"] / p["messages"] if p["messages"] else 0)),
                    ],
                },
            ]
        C = state["C"]
        if not C:
            return [_no_data_card("Cline", pal)]
        p = tui_period_totals(C, days)
        today = tui_today_totals(C)
        return [
            {
                "title": "Today",
                "value": _fmt(today["tokens_total"]),
                "value_attr": pal["mag"] | BOLD,
                "details": [
                    ("7d", _fmt(tui_period_totals(C, 7)["tokens_total"])),
                    ("30d", _fmt(tui_period_totals(C, 30)["tokens_total"])),
                    ("messages", str(today["messages"])),
                ],
            },
            {
                "title": "Requests",
                "value": str(p["messages"]),
                "value_attr": pal["acc"] | BOLD,
                "details": [
                    ("ok", str(p["ok"])),
                    ("errors", str(p["err"])),
                    ("models", str(len(C.get("models") or []))),
                ],
            },
            {
                "title": "Sessions",
                "value": str(p["sessions"]),
                "value_attr": pal["value"] | BOLD,
                "details": [
                    ("messages", str(p["messages"])),
                    ("avg / session", _fmt(p["tokens_total"] / p["sessions"] if p["sessions"] else 0)),
                    ("providers", str(len(C.get("providers") or []))),
                ],
            },
            {
                "title": "Tokens",
                "value": _fmt(p["tokens_total"]),
                "value_attr": pal["acc2"] | BOLD,
                "bar": [("acc", p["ti"]), ("acc2", p["to"]), ("warn", p["cache"])],
                "details": [
                    ("input", _fmt(p["ti"])),
                    ("output", _fmt(p["to"])),
                    ("cache", _fmt(p["cache"])),
                ],
            },
            {
                "title": "Cache",
                "value": _fmt(p["cache"]),
                "value_attr": pal["warn"] | BOLD,
                "details": [
                    ("cache rate", f"{cache_rate(p['ti'], p['cache']):.1f}%"),
                    ("cache / req", _fmt(p["cache"] / p["messages"] if p["messages"] else 0)),
                ],
            },
        ]

    def draw_cards(win, rects, cards, pal):
        for rect, card in zip(rects, cards):
            y, x, hh, ww = rect
            if hh < 3 or ww < 8:
                continue
            box(win, rect, card.get("title", ""), pal["border"],
                pal["acc"] | curses.A_BOLD)
            ix = x + 2
            iw = ww - 4
            if iw < 4:
                continue
            yy = y + 1
            value_attr = card.get("value_attr") or (pal["value"] | curses.A_BOLD)
            value_txt = tui_fit(card.get("value", ""), iw)
            sub_txt = card.get("subvalue")
            if sub_txt:
                sub_txt = tui_fit(str(sub_txt), iw, G["ell"])
                # Only pair value + subvalue when they fit side by side.
                if len(value_txt) + 1 + len(sub_txt) <= iw:
                    put(win, yy, ix, value_txt, value_attr)
                    put(win, yy, ix + iw - len(sub_txt), sub_txt,
                        card.get("subvalue_attr") or (pal["warn"] | curses.A_BOLD))
                else:
                    put(win, yy, ix, value_txt, value_attr)
            else:
                put(win, yy, ix, value_txt, value_attr)
            yy += 1
            bar_parts = card.get("bar") or []
            if bar_parts and yy <= y + hh - 2:
                total = sum(max(0.0, num(f)) for _k, f in bar_parts) or 1.0
                bx = ix
                for key, frac in bar_parts:
                    seg = int(round(max(0.0, num(frac)) / total * iw))
                    if seg <= 0 or bx >= ix + iw:
                        continue
                    seg = min(seg, ix + iw - bx)
                    put(win, yy, bx, G["bar"] * seg, pal.get(key, 0))
                    bx += seg
                yy += 1
            for label, value in card.get("details", []):
                if yy > y + hh - 2:
                    break
                # size the value first, then fit the label into what is left so
                # a long value can never collide with its label
                val = tui_fit(str(value), max(4, iw - 6), G["ell"])
                lab = tui_fit(str(label), max(1, iw - len(val) - 1), G["ell"])
                put(win, yy, ix, lab, pal["muted"])
                put(win, yy, ix + max(0, iw - len(val)), val, pal["dim"] | curses.A_BOLD)
                yy += 1

    # ---------------------------------------------------------------- chart
    def draw_chart(win, rect, rows, pal, range_label):
        y, x, hh, ww = rect
        box(win, rect, f"Recorded token activity {G['mid']} {range_label}", pal["border"],
            pal["acc"] | curses.A_BOLD)
        iw = ww - 4
        if iw < 8 or hh < 4:
            return
        ix = x + 2
        vals = []
        dates = []
        for d in rows or []:
            if not isinstance(d, dict):
                continue
            vals.append(num(d.get("ti")) + num(d.get("to")) + num(d.get("tr")) + num(d.get("cache")))
            dates.append(str(d.get("date") or ""))
        if not vals:
            put(win, y + 1, ix, "no activity in this range", pal["muted"])
            return
        total = sum(vals)
        vmax = max(vals) or 1.0
        cols = tui_scale_blocks(vals, iw)
        col_max = max(cols) or 1.0
        if hh >= 5:
            fc = tui_forecast(rows)
            put(win, y + 1, ix,
                tui_chart_header(total, vmax, len(vals), fc, iw),
                pal["muted"])
            labels_y = y + hh - 2
            bar_bottom = labels_y - 1
            bar_top = y + 2
        else:
            labels_y = None
            bar_top = y + 1
            bar_bottom = y + hh - 2
        bh = max(1, bar_bottom - bar_top + 1)
        for i, v in enumerate(cols):
            if i >= iw:
                break
            eighths = int(round(v / col_max * bh * 8)) if col_max else 0
            eighths = max(0, min(bh * 8, eighths))
            level = tui_heat(v, col_max)
            attr = pal["heat"][level] if pal["heat"] else pal["acc"]
            cx = ix + i
            full, rem = divmod(eighths, 8)
            for j in range(min(full, bh)):
                put(win, bar_bottom - j, cx, G["bar"], attr)
            if rem and full < bh:
                put(win, bar_bottom - full, cx, G["spark"][rem], attr)
        if labels_y is not None and dates:
            mid = dates[len(dates) // 2]
            put(win, labels_y, ix, tui_fit(dates[0], max(5, iw // 3), G["ell"]), pal["muted"])
            put(win, labels_y, ix + max(0, (iw - len(mid)) // 2), mid, pal["muted"])
            put(win, labels_y, ix + max(0, iw - len(dates[-1])), dates[-1], pal["muted"])

    # ---------------------------------------------------------------- models
    def model_badge_attr(badge, pal):
        """Badge colour: `recovered` is an accent/warn marker, free/$0 are good."""
        if badge == "recovered":
            return pal["warn"] | curses.A_BOLD
        if badge == "free":
            return pal["good"] | curses.A_BOLD
        return pal["good"]

    def draw_models(win, rect, models, pal, cost_idx=7, recovered=0.0):
        """Top models table; recovered history is metadata, never a model row."""
        y, x, hh, ww = rect
        box(win, rect, f"Top models {G['mid']} all time", pal["border"],
            pal["acc"] | curses.A_BOLD)
        iw = ww - 4
        if iw < 10 or hh < 3:
            return
        ix = x + 2
        # Legacy payloads may still contain a synthetic `Other` row; new
        # payloads do not. Filter before ranking so it never sets vmax and
        # never competes for a model slot.
        rows = [m for m in tui_visible_models(models) if len(m) >= 3]
        footer = ""
        if num(recovered) > 0:
            footer = f"+ rec {_fmt(recovered)} (metadata)"
        visible = max(1, hh - 3)
        if footer and hh >= 4:
            visible = max(1, visible - 1)
        if not rows:
            put(win, y + 1, ix, "no model data", pal["muted"])
            if footer and y + 2 <= y + hh - 2:
                put(win, y + 2, ix, tui_fit(footer, iw, G["ell"]),
                    pal["warn"] | curses.A_BOLD)
            return
        vmax = max(num(m[2]) for m in rows) or 1.0
        show_cost = iw >= 40
        badge_w = 5
        if show_cost:
            tail_w = 7 + 2 + 7 + 2 + badge_w
        else:
            # narrow models panel: keep tokens + the badge, drop cost
            tail_w = 7 + 2 + badge_w
        tail_x = ix + max(0, iw - tail_w)
        name_space = max(6, tail_x - ix - 1)
        bar_w = min(16, max(0, name_space - 16))
        name_w = max(6, name_space - (bar_w + 1 if bar_w else 0))
        put(win, y + 1, ix, tui_fit("model", name_w, G["ell"]), pal["muted"])
        if show_cost:
            put(win, y + 1, tail_x, f"{'tokens':>7}  {'cost':>7}", pal["muted"])
        else:
            put(win, y + 1, tail_x, f"{'tokens':>7}", pal["muted"])
        for i, m in enumerate(rows[:visible]):
            yy = y + 2 + i
            if yy > y + hh - 2:
                break
            name = str(m[0])
            tot = num(m[2])
            cost = num(m[cost_idx]) if len(m) > cost_idx else 0.0
            badge = tui_model_badge(m, cost_idx)
            put(win, yy, ix, tui_fit(name, name_w, G["ell"]),
                pal["value"] if i == 0 else pal["dim"])
            if bar_w > 0:
                frac = tot / vmax if vmax else 0.0
                filled = max(0, min(bar_w, int(round(frac * bar_w))))
                level = tui_heat(tot, vmax)
                attr = pal["heat"][level] if pal["heat"] else pal["acc"]
                put(win, yy, ix + name_w + 1, G["bar"] * filled, attr)
            if show_cost:
                num_txt = f"{_fmt(tot):>7}  {tui_fit(tui_fmt_cost(cost), 7):>7}  "
                put(win, yy, tail_x, num_txt, pal["muted"])
                if badge:
                    put(win, yy, tail_x + len(num_txt), f"{badge:<{badge_w}}",
                        model_badge_attr(badge, pal))
            else:
                put(win, yy, tail_x, f"{_fmt(tot):>7}  ", pal["dim"] | curses.A_BOLD)
                if badge:
                    put(win, yy, tail_x + 9, f"{badge:<{badge_w}}",
                        model_badge_attr(badge, pal))
        if footer:
            fy = y + 2 + min(len(rows), visible)
            if fy <= y + hh - 2:
                put(win, fy, ix, tui_fit(footer, iw, G["ell"]),
                    pal["warn"] | curses.A_BOLD)

    # --------------------------------------------------------------- heatmap
    def draw_heatmap(win, y, x, iw, hh, activity, pal):
        """7 rows x N columns of heat cells; returns the rows consumed."""
        slots = [s for s in (activity or []) if isinstance(s, dict)]
        if not slots or iw < 24 or hh < 8:
            return 0
        weeks = min(iw - 2, 53)
        need = weeks * 7
        wd = datetime.now().weekday()
        last_idx = need - 7 + wd
        if last_idx < 0:
            return 0
        n = min(len(slots), last_idx + 1)
        chunk = slots[-n:] if n else []
        grid = [[None] * weeks for _ in range(7)]
        start = last_idx - n + 1
        for i, s in enumerate(chunk):
            gi = start + i
            if 0 <= gi < need:
                grid[gi % 7][gi // 7] = s
        vmax = 0.0
        for s in chunk:
            vmax = max(vmax, num(s.get("ti")) + num(s.get("to")) + num(s.get("tr")) + num(s.get("cache")))
        heat = G["heat"]
        for col in range(weeks):
            for row in range(7):
                s = grid[row][col]
                cx = x + col
                cy = y + row
                if s is None:
                    put(win, cy, cx, heat[0], pal["muted"])
                    continue
                v = num(s.get("ti")) + num(s.get("to")) + num(s.get("tr")) + num(s.get("cache"))
                level = tui_heat(v, vmax)
                glyph = heat[0] if level == 0 else heat[min(level, len(heat) - 1)]
                attr = pal["heat"][level] if pal["heat"] else 0
                put(win, cy, cx, glyph, attr)
        used = 7
        if hh >= 9:
            prev = None
            for col in range(weeks):
                s = None
                for row in range(7):
                    if grid[row][col] is not None:
                        s = grid[row][col]
                        break
                if not s:
                    continue
                month = str(s.get("date") or "")[:7]
                if month and month != prev:
                    try:
                        label = datetime.strptime(month, "%Y-%m").strftime("%b")
                    except ValueError:
                        label = month
                    put(win, y + 7, x + col, label, pal["muted"])
                    prev = month
            used = 8
        return used

    # ---------------------------------------------------------------- panels
    def draw_records(win, rect, records, activity, pal):
        y, x, hh, ww = rect
        box(win, rect, f"Records {G['mid']} recorded", pal["border"],
            pal["acc"] | curses.A_BOLD)
        iw = ww - 4
        if iw < 8 or hh < 4:
            return
        ix = x + 2
        yy = y + 1
        if iw >= 24 and hh >= 12:
            yy += draw_heatmap(win, yy, ix, iw, hh - 2, activity, pal)
        rec = records or {}
        lines = []
        bd = rec.get("biggest_day")
        if bd:
            lines.append(("best day", f"{bd.get('date', '—')} {G['mid']} {_fmt(bd.get('total', 0))}"))
        bs = rec.get("biggest_session")
        if bs:
            lines.append(("best session", str(bs.get("title") or "—")))
        fb = rec.get("fastest_burn")
        if fb:
            lines.append(("fastest burn", f"{_fmt(fb.get('burn', 0))}/min"))
        st = rec.get("streaks") or {}
        lines.append(("streak now", f"{st.get('current', 0)}d"))
        lines.append(("streak best", f"{st.get('longest', 0)}d"))
        for label, value in lines:
            if yy > y + hh - 2:
                break
            val = tui_fit(str(value), max(4, iw // 2 + 4), G["ell"])
            put(win, yy, ix, tui_fit(label, max(4, iw - len(val) - 1), G["ell"]), pal["muted"])
            put(win, yy, ix + max(0, iw - len(val)), val, pal["dim"] | curses.A_BOLD)
            yy += 1
        if not lines and yy <= y + hh - 2:
            put(win, yy, ix, "no records yet", pal["muted"])

    def draw_side_panel(win, rect, tab, pal):
        y, x, hh, ww = rect
        payload = state["R"] if tab == 1 else state["C"]
        title = "Rate limits" if tab == 1 else "Providers"
        box(win, rect, f"{title} {G['mid']} insights", pal["border"], pal["acc"] | curses.A_BOLD)
        iw = ww - 4
        if iw < 8 or hh < 4:
            return
        ix = x + 2
        yy = y + 1
        lines = []
        if tab == 1:
            limits = (payload or {}).get("rate_limits") or {}
            for key, value in list(limits.items())[:4]:
                if isinstance(value, dict):
                    value = "  ".join(f"{k} {v}" for k, v in value.items())
                lines.append((str(key), str(value)))
        else:
            for prov in ((payload or {}).get("providers") or [])[:6]:
                if isinstance(prov, (list, tuple)) and len(prov) >= 3:
                    lines.append((str(prov[0]), f"{prov[1]} req {G['mid']} {_fmt(prov[2])}"))
        if not lines:
            for ins in ((payload or {}).get("insights") or [])[:8]:
                lines.append(("", str(ins)))
        for label, value in lines:
            if yy > y + hh - 2:
                break
            if label:
                put(win, yy, ix, tui_fit(label, max(6, iw // 3), G["ell"]), pal["acc2"])
                put(win, yy, ix + max(0, iw // 3 + 1),
                    tui_fit(value, max(6, iw - iw // 3 - 1), G["ell"]), pal["dim"])
            else:
                put(win, yy, ix, tui_fit(value, iw, G["ell"]), pal["muted"])
            yy += 1
        if not lines and yy <= y + hh - 2:
            put(win, yy, ix, "no data", pal["muted"])

    # ---------------------------------------------------------------- table
    TABLE_SPECS = {
        0: ("Recent sessions", [
            ("title", -1, "l"), ("model", 16, "l"), ("tokens", 8, "r"),
            ("msgs", 5, "r"), ("cost", 8, "r"),
        ]),
        1: ("Recent requests", [
            ("time", 8, "l"), ("model", -1, "l"), ("st", 4, "r"),
            ("tokens", 8, "r"), ("ms", 7, "r"),
        ]),
        2: ("Cline sessions", [
            ("title", -1, "l"), ("model", 16, "l"), ("tokens", 8, "r"),
            ("msgs", 5, "r"),
        ]),
    }

    def table_cells(tab, row):
        if not isinstance(row, dict):
            return []
        if tab == 0:
            return [
                str(row.get("title") or "(untitled)"),
                router_short(row.get("model")) or "—",
                _fmt(row.get("tokens_total", 0)),
                str(row.get("msgs", 0)),
                tui_fmt_cost(row.get("cost")),
            ]
        if tab == 1:
            at = str(row.get("at") or "")
            hhmm = at[11:19] if len(at) >= 19 else at[-8:]
            return [
                hhmm or "—",
                router_short(row.get("model")),
                "ok" if row.get("ok") else str(row.get("status", "err")),
                _fmt(row.get("total", 0)),
                f"{num(row.get('ms', 0)):.0f}ms",
            ]
        return [
            str(row.get("title") or row.get("id") or "(session)"),
            router_short(row.get("model")),
            _fmt(row.get("tokens_total", 0)),
            str(row.get("msgs", 0)),
        ]

    def draw_table(win, rect, tab, rows, pal):
        title, cols = TABLE_SPECS[tab]
        y, x, hh, ww = rect
        box(win, rect, title, pal["border"], pal["acc"] | curses.A_BOLD)
        iw = ww - 2
        if iw < 16 or hh < 4:
            return
        ix = x + 1
        marker_w = 2
        fixed = sum(wd for _n, wd, _a in cols if wd > 0)
        flex_n = sum(1 for _n, wd, _a in cols if wd < 0)
        gaps = len(cols) - 1
        flex_w = max(6, (iw - marker_w - fixed - gaps) // flex_n) if flex_n else 0
        widths = [flex_w if wd < 0 else wd for _n, wd, _a in cols]
        while len(widths) > 1 and sum(widths) + marker_w + len(widths) - 1 > iw:
            widths.pop()
            cols = cols[:len(widths)]
        sel = state["sel"][tab]
        if rows:
            sel = max(0, min(sel, len(rows) - 1))
            state["sel"][tab] = sel
        else:
            sel = 0
            state["sel"][tab] = 0
        hint = hh >= 6
        # 3 chrome rows (border, header, separator) + the hint row when shown;
        # never let the last data row land on the hint or bottom border.
        rows_h = max(0, hh - 3 - (2 if hint else 1))
        first = 0
        if rows and rows_h:
            first = max(0, sel - rows_h // 2)
            first = min(first, max(0, len(rows) - rows_h))
        hdr_y = y + 1
        cx = ix + marker_w
        for (name, _wd, align), wd in zip(cols, widths):
            txt = tui_fit(name.upper(), wd, G["ell"])
            txt = txt.rjust(wd) if align == "r" else txt.ljust(wd)
            put(win, hdr_y, cx, txt, pal["muted"])
            cx += wd + 1
        put(win, y + 2, ix, G["h"] * iw, pal["muted"])
        for i in range(rows_h):
            idx = first + i
            if idx >= len(rows):
                break
            ry = y + 3 + i
            if ry > y + hh - 2:
                break
            selected = idx == sel
            if selected:
                put(win, ry, ix, " " * iw, pal["sel"])
            marker = G["arrow"] + " " if selected else "  "
            put(win, ry, ix, marker, pal["sel"] | curses.A_BOLD if selected else pal["muted"])
            cells = table_cells(tab, rows[idx])
            cx = ix + marker_w
            for j, wd in enumerate(widths):
                txt = cells[j] if j < len(cells) else ""
                txt = tui_fit(txt, wd, G["ell"])
                txt = txt.rjust(wd) if cols[j][2] == "r" else txt.ljust(wd)
                if selected:
                    attr = pal["sel"]
                elif j == 0:
                    attr = pal["value"]
                else:
                    attr = pal["dim"]
                put(win, ry, cx, txt, attr)
                cx += wd + 1
        if hint:
            note = f"{sel + 1 if rows else 0}/{len(rows)}"
            if state.get("filter"):
                note = f"filter: {tui_fit(state['filter'], 16, G['ell'])} {G['mid']} {note}"
            put(win, y + hh - 2, ix, tui_fit(note, max(4, iw - 18), G["ell"]), pal["muted"])
            put(win, y + hh - 2, ix + max(0, iw - 17), "j/k enter / esc", pal["muted"])

    # ---------------------------------------------------------------- recap
    def draw_recap(win, y, x, hh, ww, pal):
        vis = state["visible"]
        shown_count = sum(1 for key in TUI_HARNESS_KEYS if vis.get(key, True))
        if shown_count == 3:
            title = f"Recap {G['mid']} all harnesses"
        elif shown_count == 1 and vis.get("opencode", True):
            title = f"Recap {G['mid']} OpenCode only"
        else:
            title = f"Recap {G['mid']} {shown_count} of 3 harnesses"
        box(win, (y, x, hh, ww), title, pal["border"], pal["acc"] | curses.A_BOLD)
        iw = ww - 4
        if iw < 10 or hh < 4:
            return
        ix = x + 2
        days = TUI_RANGES[state["range_idx"]][1]
        rng = TUI_RANGES[state["range_idx"]][0]
        entries = [
            ("OpenCode", "opencode",
             tui_period_totals(state["S"], days) if state["S"] else None, "acc"),
            ("Codex", "router",
             tui_period_totals(state["R"], days) if state["R"] else None, "acc2"),
            ("Cline", "cline",
             tui_period_totals(state["C"], days) if state["C"] else None, "mag"),
        ]
        shown = [e for e in entries if vis.get(e[1], True)]
        total = sum((e[2]["tokens_total"] if e[2] else 0.0) for e in shown)
        today_total = sum(
            tui_today_totals(state["S"] if e[1] == "opencode" else
                             state["R"] if e[1] == "router" else state["C"])["tokens_total"]
            for e in shown
        )
        put(win, y + 1, ix, f"TOTAL recorded {_fmt(total)}", pal["mag"] | curses.A_BOLD)
        put(win, y + 1, ix + 24, f"range {rng}   today {_fmt(today_total)}", pal["muted"])
        put(win, y + 2, ix,
            "   ".join(f"{e[0]} {_fmt(e[2]['tokens_total'] if e[2] else 0)}" for e in shown),
            pal["dim"])
        split = tui_token_split(state["S"])
        if (hh >= 5 and vis.get("opencode", True)
                and split["has_recovered"]):
            put(win, y + 3, ix,
                tui_fit(f"Recovered history  {_fmt(split['recovered']['total'])} "
                        f"(not in totals)", iw, G["ell"]),
                pal["warn"] | curses.A_BOLD)
        by = y + 4
        denom = total or 1.0
        bx = ix
        for e in shown:
            value = e[2]["tokens_total"] if e[2] else 0.0
            seg = int(round(value / denom * iw))
            if seg > 0 and bx < ix + iw:
                seg = min(seg, ix + iw - bx)
                put(win, by, bx, G["bar"] * seg, pal[e[3]])
                bx += seg
        put(win, by + 1, ix,
            f" {G['mid']} ".join(
                f"{pct(e[2]['tokens_total'] if e[2] else 0, total):.0f}% {e[0]}" for e in shown),
            pal["muted"])
        yy = by + 3
        for name, _key, p, attr_key in shown:
            if yy > y + hh - 2:
                break
            put(win, yy, ix, f"{name:<9}", pal[attr_key] | curses.A_BOLD)
            if p:
                put(win, yy, ix + 10,
                    f"{_fmt(p['tokens_total']):>9}   {p['messages']:>6} req   {p['sessions']:>5} sessions",
                    pal["dim"])
            else:
                put(win, yy, ix + 10, "no data", pal["muted"])
            yy += 1
        if yy <= y + hh - 2:
            put(win, yy, ix, f"press t to change range {G['mid']} bars and cards follow it", pal["muted"])

    # ------------------------------------------------------------- compact
    def draw_compact(win, w, h, pal):
        """Rounded summary card + sparkline for terminals below the panel grid."""
        tab = state["tab"]
        days = TUI_RANGES[state["range_idx"]][1]
        rng = TUI_RANGES[state["range_idx"]][0]
        names = {0: "OpenCode", 1: "Codex", 2: "Cline", 3: "Recap"}
        payload = {0: state["S"], 1: state["R"], 2: state["C"]}.get(tab)

        def period(source):
            return tui_period_totals(source, days) if source else tui_period_totals(None, days)

        box_h = h - 5  # rows 3 .. h-3; footer separator stays on h-2
        if box_h < 4 or w < 26:
            # tiny terminal: plain summary lines, no borders
            p = period(payload)
            if tab == 3:
                vis = state["visible"]
                oc = period(state["S"])["tokens_total"] if vis.get("opencode", True) else 0.0
                rx = period(state["R"])["tokens_total"] if vis.get("router", True) else 0.0
                cn = period(state["C"])["tokens_total"] if vis.get("cline", True) else 0.0
                today = sum(
                    tui_today_totals(src)["tokens_total"]
                    for key, src in (("opencode", state["S"]), ("router", state["R"]),
                                     ("cline", state["C"])) if vis.get(key, True))
                lines = [("TOTAL", _fmt(oc + rx + cn), "mag")]
                if vis.get("opencode", True):
                    lines.append(("OpenCode", _fmt(oc), "acc"))
                if vis.get("router", True):
                    lines.append(("Codex", _fmt(rx), "acc2"))
                if vis.get("cline", True):
                    lines.append(("Cline", _fmt(cn), "mag"))
                lines.append(("Today", _fmt(today), "mag"))
                split = tui_token_split(state["S"])
                if vis.get("opencode", True) and split["has_recovered"]:
                    lines.append(("Recovered", _fmt(split["recovered"]["total"]), "warn"))
            else:
                lines = [("Today", _fmt(tui_today_totals(payload)["tokens_total"]), "mag"),
                         ("Tokens", _fmt(p["tokens_total"]), "acc"),
                         ("Sessions", str(p["sessions"]), "value"),
                         ("Messages", str(p["messages"]), "value"),
                         ("Cost", tui_fmt_cost(p["cost"]), "good")]
                if tab == 0:
                    split = tui_token_split(payload)
                    if split["has_recovered"]:
                        lines.append(("Recovered",
                                      _fmt(split["recovered"]["total"]), "warn"))
            yy = 3
            for label, value, key in lines:
                if yy > h - 3:
                    break
                put(win, yy, 2, f"{label:<10}", pal["muted"])
                put(win, yy, 13, tui_fit(value, max(4, w - 15), G["ell"]),
                    pal[key] | curses.A_BOLD)
                yy += 1
            return

        box(win, (3, 1, box_h, w - 2), f"{names.get(tab, '')} {G['mid']} {rng}",
            pal["border"], pal["acc"] | curses.A_BOLD)
        ix = 3
        iw = w - 6
        if iw < 12:
            return
        y = 3
        bottom = y + box_h - 2

        def kv(yy, label, value, attr=None):
            if yy > bottom:
                return
            lab = tui_fit(str(label), max(4, iw // 2), G["ell"])
            val = tui_fit(str(value), max(4, iw - len(lab) - 1), G["ell"])
            put(win, yy, ix, lab, pal["muted"])
            put(win, yy, ix + iw - len(val), val, attr or (pal["dim"] | curses.A_BOLD))

        def spark(yy, source):
            if yy > bottom:
                return
            rows = (source or {}).get("days") or []
            vals = [num(d.get("ti")) + num(d.get("to")) + num(d.get("tr")) + num(d.get("cache"))
                    for d in rows if isinstance(d, dict)]
            if days:
                vals = vals[-days:]
            cols = tui_scale_blocks(vals, iw)
            vmax = max(cols) if cols else 0.0
            if not cols or vmax <= 0:
                put(win, yy, ix, "no activity in this range", pal["muted"])
                return
            for i, v in enumerate(cols[:iw]):
                level = int(round(v / vmax * 8)) if vmax else 0
                put(win, yy, ix + i, G["spark"][max(0, min(8, level))], pal["acc"])

        def top_model(yy, models):
            if yy > bottom:
                return
            rows = tui_visible_models(models)
            if not rows:
                return
            m = rows[0]
            badge = tui_model_badge(m, 12 if tab in (1, 2) else 7)
            put(win, yy, ix, "top model", pal["muted"])
            room = iw - 11 - (len(badge) + 1 if badge else 0)
            put(win, yy, ix + 10, tui_fit(str(m[0]), max(6, room), G["ell"]), pal["dim"])
            if badge:
                put(win, yy, ix + iw - len(badge), badge,
                    model_badge_attr(badge, pal))

        if tab == 3:
            vis = state["visible"]
            sources = [("OpenCode", "opencode", state["S"], "acc"),
                       ("Codex", "router", state["R"], "acc2"),
                       ("Cline", "cline", state["C"], "mag")]
            shown = [e for e in sources if vis.get(e[1], True)]
            total = sum(period(e[2])["tokens_total"] for e in shown)
            today = sum(tui_today_totals(e[2])["tokens_total"] for e in shown)
            put(win, y + 1, ix, _fmt(total), pal["mag"] | curses.A_BOLD)
            denom = total or 1.0
            bx = ix
            for _name, _key, src, attr_key in shown:
                seg = int(round(period(src)["tokens_total"] / denom * iw))
                if seg > 0 and bx < ix + iw:
                    seg = min(seg, ix + iw - bx)
                    put(win, y + 2, bx, G["bar"] * seg, pal[attr_key])
                    bx += seg
            yy = y + 3
            for name, _key, src, attr_key in shown:
                kv(yy, name, _fmt(period(src)["tokens_total"]), pal[attr_key] | curses.A_BOLD)
                yy += 1
            kv(yy, "today", _fmt(today), pal["mag"] | curses.A_BOLD)
            yy += 1
            split = tui_token_split(state["S"])
            if vis.get("opencode", True) and split["has_recovered"]:
                kv(yy, "recovered",
                   f"{_fmt(split['recovered']['total'])} meta",
                   pal["warn"] | curses.A_BOLD)
            return

        p = period(payload)
        today = tui_today_totals(payload)
        put(win, y + 1, ix, _fmt(p["tokens_total"]), pal["acc"] | curses.A_BOLD)
        sec = f"{p['messages']} requests" if tab == 1 else f"{p['sessions']} sessions"
        put(win, y + 1, ix + max(0, iw - len(sec)), tui_fit(sec, iw // 2), pal["muted"])
        spark(y + 2, payload)
        yy = y + 3
        kv(yy, "today", _fmt(today["tokens_total"]), pal["mag"] | curses.A_BOLD)
        if tab == 0:
            kv(yy + 1, "input", _fmt(p["ti"]))
            kv(yy + 2, "output", _fmt(p["to"]))
            kv(yy + 3, "cache", _fmt(p["cache"]))
            kv(yy + 4, "cost", tui_fmt_cost(p["cost"]))
            top_model(yy + 5, (payload or {}).get("models"))
            split = tui_token_split(payload)
            if split["has_recovered"]:
                kv(yy + 6, "recovered",
                   f"{_fmt(split['recovered']['total'])} meta",
                   pal["warn"] | curses.A_BOLD)
        elif tab == 1:
            kv(yy + 1, "ok", str(p["ok"]))
            kv(yy + 2, "errors", str(p["err"]))
            kv(yy + 3, "429s", str(p["err429"]))
            kv(yy + 4, "what-if", tui_fmt_cost(p["what_if"]))
            top_model(yy + 5, (payload or {}).get("models"))
        else:
            kv(yy + 1, "requests", str(p["messages"]))
            kv(yy + 2, "sessions", str(p["sessions"]))
            kv(yy + 3, "cache", _fmt(p["cache"]))
            top_model(yy + 4, (payload or {}).get("models"))

    # ---------------------------------------------------------------- lists
    def filtered_rows(tab):
        q = state.get("filter") or ""
        if tab == 0:
            rows = (state["S"] or {}).get("sessions") or []
            return tui_filter_sessions(rows, q, ("title", "model", "agent", "worktree"))
        if tab == 1:
            rows = (state["R"] or {}).get("requests") or []
            return tui_filter_sessions(rows, q, ("model", "provider", "at", "status"))
        if tab == 2:
            rows = (state["C"] or {}).get("sessions") or []
            return tui_filter_sessions(rows, q, ("title", "model", "provider", "id"))
        return []

    # --------------------------------------------------------------- modals
    def _ts_label(ms):
        try:
            return datetime.fromtimestamp(num(ms) / 1000).strftime("%b %d %H:%M")
        except Exception:
            return "—"

    def session_modal(row):
        sid = str(row.get("id") or "")
        lines = [
            ("h", f"session {sid[:20]}"),
            ("t", str(row.get("title") or "(untitled)")),
            ("d", f"{router_short(row.get('model'))} {G['mid']} agent {row.get('agent') or 'default'} "
                  f"{G['mid']} {row.get('worktree') or '—'}"),
            ("d", f"{_ts_label(row.get('time_created'))} {G['mid']} "
                  f"{num(row.get('duration_min', 0)):.1f} min {G['mid']} {row.get('msgs', 0)} messages"),
            ("d", f"tokens {_fmt(row.get('tokens_total', 0))}  (in {_fmt(row.get('tokens_input', 0))}, "
                  f"out {_fmt(row.get('tokens_output', 0))}, reason {_fmt(row.get('tokens_reasoning', 0))}, "
                  f"cache {_fmt(row.get('tokens_cache', 0))})"),
            ("d", f"cost {tui_fmt_cost(row.get('cost'))}"),
        ]
        files = ((state["S"] or {}).get("session_files") or {}).get(sid) or []
        if files:
            lines.append(("blank", ""))
            lines.append(("h", "files touched"))
            for f in files[:8]:
                lines.append(("d", str(f)))
        lines.append(("blank", ""))
        lines.append(("h", "prompts"))
        prompts = None
        try:
            con = connect(args.db)
            try:
                prompts = session_prompts(con, sid)
            finally:
                con.close()
        except Exception as e:
            lines.append(("d", f"(prompts unavailable: {e})"))
        if prompts is not None:
            if not prompts:
                lines.append(("d", "(no prompts recorded for this session)"))
            else:
                for p in prompts[-12:]:
                    lines.append(("k", _ts_label(p.get("t"))))
                    lines.append(("", str(p.get("p") or "")))
        return {"kind": "session", "title": "Session detail", "lines": lines, "scroll": 0}

    def request_modal(row):
        ok = bool(row.get("ok"))
        lines = [
            ("h", "request"),
            ("t", str(row.get("model") or "—")),
            ("d", str(row.get("at") or "—")),
            ("d", f"provider {row.get('provider') or '—'} {G['mid']} status {row.get('status')} "
                  f"({'ok' if ok else 'error'})"),
            ("d", f"tokens {_fmt(row.get('total', 0))}  (in {_fmt(row.get('ti', 0))}, "
                  f"out {_fmt(row.get('to', 0))}, cache {_fmt(row.get('cache', 0))})"),
            ("d", f"duration {num(row.get('ms', 0)):.0f} ms"),
        ]
        return {"kind": "request", "title": "Request detail", "lines": lines, "scroll": 0}

    def cline_modal(row):
        lines = [
            ("h", "cline session"),
            ("t", str(row.get("title") or row.get("id") or "(session)")),
            ("d", f"{router_short(row.get('model'))} {G['mid']} provider {row.get('provider') or 'cline'}"),
            ("d", f"{row.get('msgs', 0)} messages {G['mid']} tokens {_fmt(row.get('tokens_total', 0))}"),
            ("d", f"cache {_fmt(row.get('tokens_cache', 0))} {G['mid']} worktree {row.get('worktree') or '—'}"),
            ("blank", ""),
            ("d", "Cline keeps transcripts in its own app; prompts are not available here."),
        ]
        return {"kind": "cline", "title": "Cline session", "lines": lines, "scroll": 0}

    def help_modal():
        web = state["url"] if state["url"] else "unavailable (ports busy)"
        bindings = [
            ("q", "quit the TUI"),
            ("r", "refresh all data (clears caches)"),
            ("o / w", f"open Chrome UI: {web}"),
            ("1-9 visible tabs", "switch tabs (tab/arrows also cycle)"),
            ("up/down, j/k", "move selection in lists"),
            ("enter", "open the detail modal"),
            ("/ then enter", "filter sessions (esc cancels)"),
            ("t", "cycle range 14d / 30d / 90d / all"),
            ("i", "insights for this tab"),
            ("h", "show/hide harnesses (saved to the ledger)"),
            ("?", "this help"),
            ("esc", "close modal / clear filter"),
        ]
        lines = [("h", "key bindings"), ("blank", "")]
        for key, action in bindings:
            lines.append(("k", key))
            lines.append(("d", f"    {action}"))
        return {"kind": "help", "title": "Help", "lines": lines, "scroll": 0}

    def insights_modal():
        """Insights for the active tab; Recap merges the visible harnesses."""
        tab = state["tab"]
        if tab == 3:
            name = "Recap"
            rows = []
            for i, key in enumerate(TUI_HARNESS_KEYS):
                if not state["visible"].get(key, True):
                    continue
                src = (state["S"], state["R"], state["C"])[i]
                for row in tui_insight_rows(src or {}):
                    row = dict(row)
                    if row.get("title"):
                        row["title"] = f"{TUI_HARNESS_NAMES[i]} · {row['title']}"
                    elif row.get("detail"):
                        row["detail"] = f"{TUI_HARNESS_NAMES[i]}: {row['detail']}"
                    rows.append(row)
            rows = rows[:12]
        else:
            payload = {0: state["S"], 1: state["R"], 2: state["C"]}.get(tab)
            name = TUI_HARNESS_NAMES[tab]
            rows = tui_insight_rows(payload or {})
        lines = [("h", f"{name} insights"), ("blank", "")]
        if not rows:
            lines.append(("d", "no insights yet — refresh with r"))
        for row in rows:
            title = row.get("title") or ""
            value = row.get("value") or ""
            detail = row.get("detail") or ""
            if title:
                lines.append(("k", f"{title}: {value}" if value else str(title)))
                if detail:
                    lines.append(("d", f"    {detail}"))
            elif detail:
                lines.append(("d", f"{G['dot']} {detail}"))
            elif value:
                lines.append(("d", f"{G['dot']} {value}"))
        return {"kind": "insights", "title": f"Insights {G['mid']} {name}",
                "lines": lines, "scroll": 0}

    def draw_modal(stdscr, pal):
        m = state["modal"]
        h, w = stdscr.getmaxyx()
        if w < 30 or h < 10:
            return
        mw = min(max(30, w - 8), 92)
        mh = min(max(8, h - 4), 26)
        my = max(0, (h - mh) // 2)
        mx = max(0, (w - mw) // 2)
        iw = mw - 4
        wrapped = []
        for style, text in m.get("lines") or []:
            if style == "blank":
                wrapped.append(("blank", ""))
                continue
            for part in tui_wrap(text, iw) or [""]:
                wrapped.append((style, part))
        visible = max(1, mh - 4)
        scroll = int(m.get("scroll") or 0)
        scroll = max(0, min(scroll, max(0, len(wrapped) - visible)))
        m["scroll"] = scroll
        # clear the interior first so the page behind never bleeds through
        fill(stdscr, my + 1, mx + 1, mh - 2, mw - 2, 0)
        box(stdscr, (my, mx, mh, mw), m.get("title", ""),
            pal["acc"] | curses.A_BOLD, pal["acc"] | curses.A_BOLD)
        attrs = {"h": pal["acc"] | curses.A_BOLD, "t": pal["value"] | curses.A_BOLD,
                 "k": pal["good"] | curses.A_BOLD}
        for i in range(visible):
            idx = scroll + i
            if idx >= len(wrapped):
                break
            style, text = wrapped[idx]
            if style == "blank":
                continue
            put(stdscr, my + 1 + i, mx + 2, tui_fit(text, iw),
                attrs.get(style, pal["dim"]))
        hint = "esc close"
        if len(wrapped) > visible:
            hint = (f"esc close {G['mid']} j/k scroll {G['mid']} "
                    f"{scroll + 1}-{min(scroll + visible, len(wrapped))}/{len(wrapped)}")
        put(stdscr, my + mh - 2, mx + 2, tui_fit(hint, iw), pal["muted"])

    def draw_harness_modal(stdscr, pal):
        """Centered harness visibility modal with [x]/[ ] checkboxes."""
        m = state["modal"]
        h, w = stdscr.getmaxyx()
        if w < 30 or h < 8:
            return
        mw = min(46, max(30, w - 8))
        mh = 8
        my = max(0, (h - mh) // 2)
        mx = max(0, (w - mw) // 2)
        iw = mw - 4
        fill(stdscr, my + 1, mx + 1, mh - 2, mw - 2, 0)
        box(stdscr, (my, mx, mh, mw), m.get("title", "Harnesses"),
            pal["acc"] | curses.A_BOLD, pal["acc"] | curses.A_BOLD)
        sel = int(m.get("sel") or 0) % len(TUI_HARNESS_KEYS)
        for i, name in enumerate(TUI_HARNESS_NAMES):
            row = my + 1 + i
            if row > my + mh - 2:
                break
            shown = state["visible"].get(TUI_HARNESS_KEYS[i], True)
            mark = f"[{'x' if shown else ' '}]"
            cursor = G["arrow"] + " " if i == sel else "  "
            attr = (pal["value"] | curses.A_BOLD) if i == sel else pal["dim"]
            if not shown:
                attr = pal["muted"]
            put(stdscr, row, mx + 2, tui_fit(cursor + f"{mark} {name}", iw), attr)
        if my + 5 <= my + mh - 2:
            put(stdscr, my + 5, mx + 2,
                tui_fit("space/enter toggle   h/esc close", iw), pal["muted"])
        if my + 6 <= my + mh - 2:
            put(stdscr, my + 6, mx + 2,
                tui_fit("hidden leave tabs + Recap · x = shown", iw), pal["muted"])

    # ------------------------------------------------------------ page bodies
    def draw_body(win, layout, tab, pal):
        if tab == 0:
            payload = state["S"]
        elif tab == 1:
            payload = state["R"]
        else:
            payload = state["C"]
        days = TUI_RANGES[state["range_idx"]][1]
        rng_label = TUI_RANGES[state["range_idx"]][0]
        draw_cards(win, layout.get("cards") or [], build_cards(tab, pal), pal)
        chart_rows = (payload or {}).get("days") or []
        if days:
            chart_rows = chart_rows[-days:]
        if layout.get("chart"):
            draw_chart(win, layout["chart"], chart_rows, pal, rng_label)
        if layout.get("models"):
            cost_idx = 12 if tab in (1, 2) else 7
            split = tui_token_split(state["S"]) if tab == 0 else None
            recovered = (split["recovered"]["total"]
                         if split and split["has_recovered"] else 0.0)
            draw_models(win, layout["models"], (payload or {}).get("models") or [],
                        pal, cost_idx, recovered)
        if layout.get("sessions"):
            draw_table(win, layout["sessions"], tab, filtered_rows(tab), pal)
        if layout.get("records"):
            if tab == 0:
                S = state["S"] or {}
                draw_records(win, layout["records"], S.get("records") or {},
                             S.get("activity") or [], pal)
            else:
                draw_side_panel(win, layout["records"], tab, pal)

    # ----------------------------------------------------------------- draw
    def draw(stdscr):
        stdscr.erase()
        h, w = stdscr.getmaxyx()
        if render["pal"] is None:
            render["pal"] = _init_palette()
        pal = render["pal"]
        BOLD = curses.A_BOLD
        tab = state["tab"]

        # header
        fill(stdscr, 0, 0, 1, w, pal["hdr"])
        brand = G["dot"] + " OpenCode Dashboard"
        clock = datetime.now().strftime("%a %d %b  %H:%M:%S")
        clock_x = max(1, w - len(clock) - 2)
        put(stdscr, 0, 1, brand, pal["hdr"] | BOLD)
        subtitle = f"{G['mid']} usage observatory"
        sub_x = 3 + len(brand)
        if sub_x + len(subtitle) < clock_x - 1:
            put(stdscr, 0, sub_x, subtitle, pal["hdr_dim"])
        if clock_x > 1 + len(brand):
            put(stdscr, 0, clock_x, clock, pal["hdr"] | BOLD)

        layout = tui_layout(w, h)
        state["mode"] = layout["mode"]
        # tab bar: only visible harnesses, numbered positionally
        short = layout["mode"] == "compact" or w < 64
        items = tui_tab_items(state["visible"])
        x = 1
        for pos, (idx, name, short_name) in enumerate(items):
            label = short_name if short else name
            text = f" {pos + 1}{label} " if short else f" {pos + 1} {label} "
            if x + len(text) > w - 1:
                break
            attr = pal["tab"] | BOLD if idx == tab else pal["muted"]
            put(stdscr, 1, x, text, attr)
            x += len(text) + 1
        hidden_count = sum(1 for key in TUI_HARNESS_KEYS
                           if not state["visible"].get(key, True))
        hidden_shown = False
        if hidden_count:
            tag = f"+{hidden_count} hidden {G['mid']} h"
            if x + len(tag) <= w - 1:
                put(stdscr, 1, x, tag, pal["warn"])
                x += len(tag) + 1
                hidden_shown = True
        tabs_end = x
        rng = TUI_RANGES[state["range_idx"]][0]
        right = f"range {rng}"
        live = f"{G['dot']} live"
        live_x = max(1, w - len(live) - 1)
        right_x = live_x - len(right) - 3
        if w >= 48 and right_x >= tabs_end + 2:
            put(stdscr, 1, right_x, right, pal["dim"])
            if live_x >= right_x + len(right) + 2:
                put(stdscr, 1, live_x, live, pal["good"] | BOLD)
        elif w >= 20 and live_x >= tabs_end + 2:
            put(stdscr, 1, live_x, live, pal["good"] | BOLD)
        if state.get("filter") and w >= 48:
            tag = f"filter:{tui_fit(state['filter'], 12, G['ell'])}"
            tag_x = right_x - len(tag) - 2
            if tag_x >= tabs_end + 2:
                put(stdscr, 1, tag_x, tag, pal["warn"])

        # body
        if layout["mode"] == "compact":
            draw_compact(stdscr, w, h, pal)
        elif tab == 3:
            # cap the recap card so a tall terminal is not a mostly-empty box
            draw_recap(stdscr, 3, 1, min(max(3, h - 5), 17), w - 2, pal)
        else:
            draw_body(stdscr, layout, tab, pal)

        # footer
        put(stdscr, h - 2, 0, G["h"] * max(0, w - 1), pal["muted"])
        parts = ["q quit", "r refresh", "o web UI", "t range", "/ search",
                 "i insights", "? help", "h harness"]
        if w >= 100:
            parts[2] = f"o {state['url']}" if state["url"] else "o web UI off"
        elif w < 80:
            # 70-79 cols: the full hint list does not fit on one line
            parts = ["q quit", "r refresh", "t range", "/ search",
                     "i insights", "? help", "h harness"]
        if w < 36:
            parts = ["q quit", "? help", "h harness"]
        elif w < 64:
            parts = ["q quit", "r refresh", "i insights", "? help", "h harness"]
        for optional in ("h harness", "i insights"):
            while (optional in parts
                   and len(" " + G["sep"].join(parts) + " ") > w - 1):
                parts.remove(optional)
        if hidden_count and not hidden_shown:
            for tag in (f"+{hidden_count} hidden {G['mid']} h",
                        f"+{hidden_count} hidden"):
                if len(" " + G["sep"].join(parts + [tag]) + " ") <= w - 1:
                    parts.append(tag)
                    break
            else:
                parts = [p for p in parts if p != "h harness"]
                for tag in (f"+{hidden_count} hidden {G['mid']} h",
                            f"+{hidden_count} hidden"):
                    if len(" " + G["sep"].join(parts + [tag]) + " ") <= w - 1:
                        parts.append(tag)
                        break
        hints = " " + G["sep"].join(parts) + " "
        msg = state.get("msg") or ""
        searching = state.get("search") is not None
        if searching:
            msg = f"/ {state['search']}_   enter apply {G['mid']} esc cancel"
            attr = pal["warn"]
        elif state["loading"]:
            spin = G["spin"][state["spinner"] % len(G["spin"])]
            msg = f"{spin} refreshing…"
            attr = pal["acc"]
        elif state.get("status"):
            msg = tui_fit(state["status"], max(4, w - 2), G["ell"])
            attr = pal["warn"]
        elif state.get("filter"):
            msg = f"filter: {tui_fit(state['filter'], 18, G['ell'])} {G['mid']} esc clears"
            attr = pal["warn"]
        else:
            attr = pal["good"]
        if msg:
            right_x = w - len(msg) - 2
            if not searching and right_x >= len(hints) + 1:
                put(stdscr, h - 1, 0, hints, pal["muted"])
                put(stdscr, h - 1, right_x, msg, attr)
            elif searching or attr != pal["good"]:
                # status/search replaces the hints so nothing overlaps
                put(stdscr, h - 1, 1, tui_fit(msg, w - 2), attr)
            else:
                put(stdscr, h - 1, 0, hints, pal["muted"])
        else:
            put(stdscr, h - 1, 0, hints, pal["muted"])

        if state["modal"] is not None:
            if state["modal"].get("kind") == "harness":
                draw_harness_modal(stdscr, pal)
            else:
                draw_modal(stdscr, pal)
        stdscr.refresh()

    def draw_loading(stdscr):
        if render["pal"] is None:
            render["pal"] = _init_palette()
        pal = render["pal"]
        stdscr.erase()
        h, w = stdscr.getmaxyx()
        fill(stdscr, 0, 0, 1, w, pal["hdr"])
        brand = G["dot"] + " OpenCode Dashboard"
        put(stdscr, 0, 1, brand, pal["hdr"] | curses.A_BOLD)
        clock = datetime.now().strftime("%H:%M:%S")
        put(stdscr, 0, max(1, w - len(clock) - 2), clock, pal["hdr"] | curses.A_BOLD)
        spin = G["spin"][state["spinner"] % len(G["spin"])]
        msg = f"{spin} Loading dashboard…"
        put(stdscr, max(0, h // 2), max(1, (w - len(msg)) // 2), msg,
            pal["acc"] | curses.A_BOLD)
        shown_names = [TUI_HARNESS_NAMES[i] for i, key in enumerate(TUI_HARNESS_KEYS)
                       if state["visible"].get(key, True)]
        if len(shown_names) == 1:
            reading = f"reading {shown_names[0]} data"
        elif len(shown_names) == 2:
            reading = f"reading {shown_names[0]} and {shown_names[1]} data"
        elif shown_names:
            reading = (f"reading {shown_names[0]}, {shown_names[1]} "
                       f"and {shown_names[2]} data")
        else:
            reading = "reading OpenCode data"
        reading = tui_fit(reading, max(8, w - 4), G["ell"])
        put(stdscr, max(0, h // 2 + 2), max(1, (w - len(reading)) // 2),
            reading, pal["muted"])
        stdscr.refresh()

    # --------------------------------------------------------------- actions
    def cycle_range():
        state["range_idx"] = (state["range_idx"] + 1) % len(TUI_RANGES)

    def move_sel(delta):
        tab = state["tab"]
        rows = filtered_rows(tab)
        if not rows:
            state["sel"][tab] = 0
            return
        sel = state["sel"][tab] + delta
        state["sel"][tab] = max(0, min(sel, len(rows) - 1))

    def open_detail():
        tab = state["tab"]
        if tab == 3:
            state["msg"] = "recap has no detail view"
            return
        if state.get("mode") == "compact":
            state["msg"] = "session list hidden at this size — resize for details"
            return
        rows = filtered_rows(tab)
        if not rows:
            state["msg"] = "nothing to open"
            return
        idx = max(0, min(state["sel"][tab], len(rows) - 1))
        row = rows[idx]
        try:
            if tab == 0:
                state["modal"] = session_modal(row)
            elif tab == 1:
                state["modal"] = request_modal(row)
            else:
                state["modal"] = cline_modal(row)
        except Exception as e:
            state["modal"] = {
                "kind": "error", "title": "Detail", "scroll": 0,
                "lines": [("t", "Could not open detail"), ("d", str(e))],
            }

    def refresh_now(stdscr):
        state["loading"] = True
        state["spinner"] = (state["spinner"] + 1) % 4
        clear_caches()
        draw(stdscr)
        load_data()
        state["loading"] = False

    def main_loop(stdscr):
        curses.curs_set(0)
        stdscr.timeout(200)
        try:
            stdscr.keypad(True)
        except Exception:
            pass
        try:
            # keep a lone Esc snappy instead of waiting out ncurses' ESCDELAY
            curses.set_escdelay(25)
        except Exception:
            pass
        pending = []  # keys read while disambiguating Esc, replayed on the next loop

        def take_key():
            if pending:
                return pending.pop(0)
            try:
                return stdscr.getch()
            except Exception:
                return -1

        def arrow_from_esc():
            """Decode CSI/SS3 arrow sequences curses left undecoded.

            Terminals that do not honour application-keypad mode send
            ESC [ A..D even with keypad(True); map those so Up/Down and
            Left/Right keep working there. A real lone Esc still returns -1.
            """
            stdscr.timeout(40)
            try:
                nxt = stdscr.getch()
                if nxt in (ord("["), ord("O")):
                    third = stdscr.getch()
                    arrow = {ord("A"): curses.KEY_UP, ord("B"): curses.KEY_DOWN,
                             ord("C"): curses.KEY_RIGHT, ord("D"): curses.KEY_LEFT}.get(third)
                    if arrow is not None:
                        return arrow
                    pending.extend([nxt] + ([third] if third != -1 else []))
                elif nxt != -1:
                    pending.append(nxt)
            except Exception:
                pass
            finally:
                stdscr.timeout(200)
            return -1

        draw_loading(stdscr)
        load_data()
        while True:
            draw(stdscr)
            key = take_key()
            if key == 27:
                arrow = arrow_from_esc()
                if arrow != -1:
                    key = arrow
            if key == -1:
                if time.time() - state["last"] > 30:
                    refresh_now(stdscr)
                continue
            if state["search"] is not None:
                if key == 27:
                    state["search"] = None
                elif key in (10, 13, curses.KEY_ENTER):
                    state["filter"] = state["search"]
                    state["search"] = None
                    state["sel"][state["tab"]] = 0
                elif key in (curses.KEY_BACKSPACE, 127, 8):
                    state["search"] = state["search"][:-1]
                elif 32 <= key < 127 and len(state["search"]) < 40:
                    state["search"] += chr(key)
                continue
            if state["modal"] is not None:
                m = state["modal"]
                if m.get("kind") == "harness":
                    if key == 27 or key in (ord("h"), ord("H"), ord("q"), ord("Q")):
                        state["modal"] = None
                    elif key in (curses.KEY_UP, ord("k")):
                        m["sel"] = (int(m.get("sel") or 0) - 1) % len(TUI_HARNESS_KEYS)
                    elif key in (curses.KEY_DOWN, ord("j")):
                        m["sel"] = (int(m.get("sel") or 0) + 1) % len(TUI_HARNESS_KEYS)
                    elif key in (ord(" "), 10, 13, curses.KEY_ENTER):
                        toggle_harness(int(m.get("sel") or 0))
                    continue
                if key == 27 or key in (ord("q"), ord("Q")):
                    state["modal"] = None
                elif key in (curses.KEY_UP, ord("k")):
                    m["scroll"] = max(0, int(m.get("scroll") or 0) - 1)
                elif key in (curses.KEY_DOWN, ord("j")):
                    m["scroll"] = int(m.get("scroll") or 0) + 1
                elif key == curses.KEY_PPAGE:
                    m["scroll"] = max(0, int(m.get("scroll") or 0) - 8)
                elif key == curses.KEY_NPAGE:
                    m["scroll"] = int(m.get("scroll") or 0) + 8
                continue
            if key in (ord("q"), ord("Q")):
                break
            if key in (ord("r"), ord("R")):
                refresh_now(stdscr)
            elif key in (ord("o"), ord("O"), ord("w"), ord("W")):
                if not state["url"]:
                    state["msg"] = f"Chrome UI unavailable: no free port at {args.port}-{args.port + 10}"
                else:
                    try:
                        if open_chrome_window(state["url"]):
                            state["msg"] = f"opened {state['url']}"
                        else:
                            state["msg"] = f"could not open a browser for {state['url']}"
                    except Exception as e:
                        state["msg"] = f"open failed: {e}"
            elif key in (ord("t"), ord("T")):
                cycle_range()
            elif key == ord("?"):
                state["modal"] = help_modal()
            elif key in (ord("i"), ord("I")):
                state["modal"] = insights_modal()
            elif key in (ord("h"), ord("H")):
                state["modal"] = {"kind": "harness", "title": "Harnesses",
                                  "sel": 0, "scroll": 0, "lines": []}
            elif key == ord("/"):
                state["search"] = ""
            elif key in (9, curses.KEY_RIGHT):
                tab_step(1)
            elif key == curses.KEY_LEFT:
                tab_step(-1)
            elif ord("1") <= key <= ord("9"):
                idx = tui_tab_for_key(key, state["visible"])
                if idx is None:
                    state["status"] = "no tab there — press h to show a harness"
                else:
                    state["tab"] = idx
            elif key in (curses.KEY_UP, ord("k")):
                move_sel(-1)
            elif key in (curses.KEY_DOWN, ord("j")):
                move_sel(1)
            elif key == curses.KEY_PPAGE:
                move_sel(-8)
            elif key == curses.KEY_NPAGE:
                move_sel(8)
            elif key == curses.KEY_HOME:
                move_sel(-(10 ** 9))
            elif key == curses.KEY_END:
                move_sel(10 ** 9)
            elif key in (10, 13, curses.KEY_ENTER):
                open_detail()
            elif key == 27 and state.get("filter"):
                state["filter"] = ""

    try:
        curses.wrapper(main_loop)
    finally:
        Handler.keep_alive = False
        try:
            if server is not None:
                server.shutdown()
                server.server_close()
            if srv_thread is not None:
                srv_thread.join(timeout=2)
        except Exception:
            pass

def main():
    parser = argparse.ArgumentParser(description="OpenCode usage dashboard")
    parser.add_argument("db", nargs="?", default=str(DB_PATH), help="path to opencode.db")
    parser.add_argument("--port", type=int, default=8765, help="HTTP port to serve on")
    parser.add_argument(
        "--idle-timeout", type=int, default=0, metavar="SECONDS",
        help="shut the server down after this many seconds with no requests (0 = off)",
    )
    parser.add_argument("--quiet", action="store_true", help="suppress per-request logging")
    parser.add_argument("--tui", action="store_true", help="run the beautiful terminal UI (default web server otherwise)")
    parser.add_argument("--router-events", default=str(ROUTER_EVENTS_DEFAULT),
                        help="path to Codex Router usage-events.jsonl")
    parser.add_argument("--router-limits", default=str(ROUTER_LIMITS_DEFAULT),
                        help="path to Codex Router rate-limits.json")
    parser.add_argument("--cline-dir", default=str(CLINE_DIR_DEFAULT),
                        help="path to Cline Desktop data dir (sessions + db)")
    parser.add_argument(
        "--recovered-tokens", type=int, default=None, metavar="N",
        help=(f"recovered OpenCode tokens shown as lifetime metadata and "
              f"excluded from recorded totals (default {RECOVERED_OPENCODE_TOKENS:,} "
              f"for the default db; 0 disables)"),
    )
    args = parser.parse_args()

    global RECOVERED_TOKENS
    RECOVERED_TOKENS = args.recovered_tokens

    if args.tui:
        run_tui(args)
        return
    Handler.db_path = args.db
    Handler.router_events = args.router_events
    Handler.router_limits = args.router_limits
    Handler.cline_dir = args.cline_dir
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
