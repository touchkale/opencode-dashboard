import argparse
import json
import os
import sqlite3
import sys
import threading
from collections import defaultdict
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import pathname2url

DB_PATH = Path(os.environ.get("HOME", "")) / ".local/share/opencode/opencode.db"


def connect(db_path):
    if not Path(db_path).exists():
        sys.exit(f"Database not found: {db_path}")
    uri = f"file:{pathname2url(str(Path(db_path).resolve()))}?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    con.row_factory = sqlite3.Row
    return con


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


def query_stats(con):
    totals = dict(
        con.execute(
            """
            SELECT (SELECT COUNT(*) FROM session) AS sessions,
                   (SELECT COUNT(*) FROM message) AS messages,
                   (SELECT COUNT(*) FROM part WHERE json_extract(data,'$.type')='tool') AS tools,
                   (SELECT COUNT(*) FROM part WHERE json_extract(data,'$.type')='tool'
                      AND json_extract(data,'$.state.status') NOT IN ('completed','running','pending')) AS failed_tools,
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

    tools = defaultdict(int)
    for r in con.execute(
        """
        SELECT json_extract(data,'$.tool') AS t, COUNT(*) AS c
        FROM part WHERE json_extract(data,'$.type')='tool'
        GROUP BY t ORDER BY c DESC
        """
    ):
        tools[r["t"] or "unknown"] += r["c"]

    day_msgs = defaultdict(int)
    for r in con.execute(
        "SELECT date(time_created/1000,'unixepoch','localtime') AS d, COUNT(*) AS c FROM message GROUP BY d"
    ):
        day_msgs[r["d"]] = r["c"]
    day_sessions = defaultdict(int)
    for r in con.execute(
        "SELECT date(time_created/1000,'unixepoch','localtime') AS d, COUNT(*) AS c FROM session GROUP BY d"
    ):
        day_sessions[r["d"]] = r["c"]
    day_tools = defaultdict(int)
    for r in con.execute(
        "SELECT date(time_created/1000,'unixepoch','localtime') AS d, COUNT(*) AS c FROM part WHERE json_extract(data,'$.type')='tool' GROUP BY d"
    ):
        day_tools[r["d"]] = r["c"]

    tokens_by_day = {}
    for r in con.execute(
        """
        SELECT date(time_created/1000,'unixepoch','localtime') AS d,
               COALESCE(SUM(json_extract(data,'$.tokens.input')),0) AS ti,
               COALESCE(SUM(json_extract(data,'$.tokens.output')),0) AS to_,
               COALESCE(SUM(json_extract(data,'$.tokens.reasoning')),0) AS tr,
               COALESCE(SUM(json_extract(data,'$.tokens.total')),0) AS tt,
               COALESCE(SUM(json_extract(data,'$.tokens.cache.read')),0) AS cr,
               COALESCE(SUM(json_extract(data,'$.cost')),0) AS cost
        FROM part WHERE json_extract(data,'$.type')='step-finish'
        GROUP BY d
        """
    ):
        cache = max(0, r["tt"] - r["ti"] - r["to_"] - r["tr"])
        if not cache:
            cache = r["cr"]
        tokens_by_day[r["d"]] = {
            "ti": r["ti"], "to": r["to_"], "tr": r["tr"],
            "cache": cache, "cost": r["cost"],
        }

    days = sorted(set(day_msgs) | set(day_sessions) | set(day_tools) | set(tokens_by_day))

    heat = defaultdict(int)
    for r in con.execute(
        """
        SELECT date(time_created/1000,'unixepoch','localtime') AS d,
               CAST(strftime('%H',time_created/1000,'unixepoch','localtime') AS INTEGER) AS h,
               COUNT(*) AS c
        FROM message GROUP BY d,h
        """
    ):
        heat[(r["d"], r["h"])] = r["c"]

    hours = defaultdict(int)
    for r in con.execute(
        "SELECT CAST(strftime('%H', time_created/1000, 'unixepoch', 'localtime') AS INTEGER) AS h, COUNT(*) AS c FROM message GROUP BY h"
    ):
        hours[r["h"]] = r["c"]

    agents = defaultdict(lambda: {"n": 0, "toks": 0})
    for r in con.execute(
        "SELECT agent, COUNT(*) AS n, COALESCE(SUM(tokens_input)+SUM(tokens_output)+SUM(tokens_reasoning),0) AS t FROM session GROUP BY agent"
    ):
        a = r["agent"] or "default"
        agents[a]["n"] += r["n"]
        agents[a]["toks"] += r["t"]

    models = defaultdict(lambda: {"n": 0, "ti": 0, "to": 0, "tr": 0, "cost": 0})
    for r in con.execute(
        """
        SELECT model, COUNT(*) AS n,
               COALESCE(SUM(tokens_input),0) AS ti,
               COALESCE(SUM(tokens_output),0) AS to_,
               COALESCE(SUM(tokens_reasoning),0) AS tr,
               COALESCE(SUM(cost),0) AS cost
        FROM session GROUP BY model
        """
    ):
        m = parse_model(r["model"])
        models[m]["n"] += r["n"]
        models[m]["ti"] += r["ti"]
        models[m]["to"] += r["to_"]
        models[m]["tr"] += r["tr"]
        models[m]["cost"] += r["cost"]

    prompts_by_session = defaultdict(list)
    for r in con.execute(
        "SELECT session_id, prompt, time_created FROM session_input ORDER BY time_created"
    ):
        prompts_by_session[r["session_id"]].append({"t": r["time_created"], "p": r["prompt"]})

    if not prompts_by_session:
        for r in con.execute(
            """
            SELECT p.session_id, json_extract(p.data,'$.text') AS txt, p.time_created
            FROM part p JOIN message m ON m.id = p.message_id
            WHERE json_extract(p.data,'$.type')='text'
              AND json_extract(m.data,'$.role')='user'
            ORDER BY p.time_created
            """
        ):
            txt = (r["txt"] or "").strip()
            if txt:
                prompts_by_session[r["session_id"]].append({"t": r["time_created"], "p": txt})

    tool_bd = defaultdict(lambda: defaultdict(int))
    for r in con.execute(
        """
        SELECT session_id, json_extract(data,'$.tool') AS t, COUNT(*) AS c
        FROM part WHERE json_extract(data,'$.type')='tool'
        GROUP BY session_id, t
        """
    ):
        tool_bd[r["session_id"]][r["t"] or "unknown"] += r["c"]

    sessions = []
    for r in con.execute(
        """
        SELECT s.id, s.title, s.agent, s.model, s.time_created, s.time_updated,
               s.tokens_input, s.tokens_output, s.tokens_reasoning, s.cost,
               p.worktree,
               (SELECT COUNT(*) FROM message m WHERE m.session_id = s.id) AS msgs,
               (SELECT COUNT(*) FROM part pt WHERE pt.session_id = s.id
                 AND json_extract(pt.data,'$.type')='tool') AS tools
        FROM session s LEFT JOIN project p ON p.id = s.project_id
        ORDER BY s.time_created
        """
    ):
        sid = r["id"]
        bd = dict(tool_bd.get(sid, {}))
        sessions.append(
            {
                "id": sid,
                "title": r["title"],
                "agent": r["agent"] or "default",
                "model": parse_model(r["model"]),
                "time_created": r["time_created"],
                "time_updated": r["time_updated"],
                "duration_min": round(max(0, (r["time_updated"] - r["time_created"]) / 60000.0), 1),
                "tokens_input": r["tokens_input"],
                "tokens_output": r["tokens_output"],
                "tokens_reasoning": r["tokens_reasoning"],
                "cost": r["cost"],
                "worktree": short_dir(r["worktree"]),
                "msgs": r["msgs"],
                "tools": r["tools"],
                "prompts": prompts_by_session.get(sid, [])[-30:],
                "tools_bd": sorted([[k, v] for k, v in bd.items()], key=lambda kv: -kv[1])[:8],
            }
        )

    projects = defaultdict(int)
    for r in con.execute(
        "SELECT p.name, p.worktree, COUNT(s.id) AS n FROM project p LEFT JOIN session s ON s.project_id=p.id GROUP BY p.id"
    ):
        if r["n"]:
            projects[r["name"] or short_dir(r["worktree"])] += r["n"]

    busiest = max(days, key=lambda d: day_msgs.get(d, 0)) if days else None
    peak_hour = max(range(24), key=lambda h: hours.get(h, 0))
    top_tool = sorted(tools.items(), key=lambda kv: -kv[1])[0] if tools else ("—", 0)
    top_agent = sorted(agents.items(), key=lambda kv: -kv[1]["n"])[0] if agents else ("—", {"n": 0})
    longest = max(sessions, key=lambda s: s["duration_min"]) if sessions else None
    gen_tokens = totals["tokens_input"] + totals["tokens_output"]
    cache_mult = (totals["tokens_cache_read"] / gen_tokens) if gen_tokens else 0
    cache_insight = (
        f"Context cache served {totals['tokens_cache_read']:,} tokens — "
        f"{cache_mult:.1f}× everything you generated, so much context was reused."
        if totals["tokens_cache_read"]
        else ""
    )

    insights = [
        f"Across {totals['sessions']} sessions you exchanged {totals['messages']:,} messages and ran {totals['tools']:,} tool calls.",
        f"Your busiest day was {busiest} — {day_msgs[busiest]:,} messages and {day_tools[busiest]:,} tool calls."
        if busiest else "No activity recorded yet.",
        f"Most-used tool: {top_tool[0]} — {top_tool[1]:,} calls."
        if tools else "",
        f"The {top_agent[0]} agent did the heavy lifting with {top_agent[1]['n']} sessions and {top_agent[1]['toks']:,} tokens."
        if agents else "",
        f"You're most active at {peak_hour:02d}:00 ({hours[peak_hour]:,} messages during that hour)." if any(hours.values()) else "",
        f"Longest session: “{longest['title']}” — {fmt_dur_min(longest['duration_min'])}."
        if longest else "",
        cache_insight,
    ]
    insights = [i for i in insights if i]

    stats = {
        "totals": totals,
        "sessions": sessions,
        "tools": sorted([[k, v] for k, v in tools.items()], key=lambda kv: -kv[1]),
        "agents": sorted([[k, v["n"], v["toks"]] for k, v in agents.items()], key=lambda kv: -kv[1]),
        "models": sorted(
            [[k, v["n"], v["ti"] + v["to"] + v["tr"], v["ti"], v["to"], v["cost"]]
             for k, v in models.items()],
            key=lambda kv: -kv[2],
        ),
        "projects": sorted([[k, v] for k, v in projects.items()], key=lambda kv: -kv[1]),
        "days": [
            {
                "date": d, "msgs": day_msgs.get(d, 0), "sessions": day_sessions.get(d, 0),
                "tools": day_tools.get(d, 0),
                **tokens_by_day.get(d, {"ti": 0, "to": 0, "tr": 0, "cache": 0, "cost": 0}),
            }
            for d in days
        ],
        "hours": [hours.get(h, 0) for h in range(24)],
        "heatmap": {
            "cols": days,
            "rows": [[h] + [heat.get((d, h), 0) for d in days] for h in range(24)],
        },
        "insights": insights,
        "range": f"{days[0]} → {days[-1]}" if days else "no activity",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    return stats


PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>OpenCode Dashboard</title>
  <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect x='8' y='30' width='18' height='40' rx='4' fill='%234f46e5'/><rect x='40' y='14' width='18' height='56' rx='4' fill='%230d9488'/><rect x='72' y='44' width='18' height='26' rx='4' fill='%23d97706'/></svg>">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#f7f7f5; --panel:#ffffff; --panel2:#fbfbfa; --line:#e7e5e0; --line2:#efede8;
  --text:#1c1c1a; --muted:#6d6a63; --subtle:#9a968c;
  --accent:#4f46e5; --accent2:#0d9488; --accent3:#9333ea; --cache:#d97706;
  --good:#16a34a; --bad:#dc2626; --chip:#f1efe9;
  --shadow:0 1px 2px rgba(28,28,26,.05),0 8px 24px rgba(28,28,26,.06);
  --mono:"JetBrains Mono","SF Mono",ui-monospace,Menlo,monospace;
  --sans:"Inter",-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
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
.topbar{position:sticky;top:0;z-index:40;backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px);
  background:color-mix(in srgb,var(--bg) 82%,transparent);border-bottom:1px solid var(--line)}
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
.tbl{width:100%;border-collapse:collapse;font-size:13px}
.tbl thead th{position:sticky;top:58px;background:var(--panel);text-align:left;font-size:10.5px;font-weight:700;
  text-transform:uppercase;letter-spacing:.08em;color:var(--subtle);padding:10px 12px;border-bottom:1px solid var(--line);
  white-space:nowrap;cursor:pointer;user-select:none;z-index:10}
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

/* heatmap */
.heat-row{display:flex;gap:3px;margin-bottom:3px;align-items:center}
.heat-lbl{width:30px;font-family:var(--mono);font-size:9.5px;color:var(--subtle);text-align:right;margin-right:4px;flex-shrink:0}
.heat-cell{width:13px;height:13px;border-radius:4px;cursor:pointer;transition:transform .1s}
.heat-cell:hover{transform:scale(1.4);outline:1px solid var(--text)}
.heat-days{display:flex;gap:3px;margin-left:34px}
.heat-days span{width:13px;font-size:8.5px;color:var(--subtle);overflow:hidden;white-space:nowrap}

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
.prompt{background:var(--bg);border:1px solid var(--line);border-left:3px solid var(--accent);border-radius:10px;
  padding:11px 14px;margin-bottom:10px;font-size:13px;color:var(--muted);line-height:1.55;white-space:pre-wrap;word-break:break-word}
.mmeta{display:flex;flex-wrap:wrap;gap:7px;margin:14px 0 4px}

footer{color:var(--subtle);font-size:11.5px;text-align:center;margin-top:44px;font-family:var(--mono)}
.empty{color:var(--subtle);text-align:center;padding:26px;font-size:13px}
::-webkit-scrollbar{width:10px;height:10px}
::-webkit-scrollbar-thumb{background:var(--line);border-radius:8px;border:2px solid var(--bg)}
::-webkit-scrollbar-thumb:hover{background:var(--subtle)}
</style>
</head>
<body>
<div class="topbar">
  <div class="topbar-in">
    <div class="brand"><span class="dot"></span>OpenCode<small>· usage</small></div>
    <div class="spacer"></div>
    <span id="status"></span>
    <button class="tbtn" id="theme">◐&nbsp; Light</button>
    <button class="tbtn" id="export">⤓ JSON</button>
    <button class="tbtn" id="refresh">⟳ Refresh</button>
  </div>
</div>

<div class="wrap">
  <section class="hero">
    <div class="eyebrow" id="range"></div>
    <h1 id="headline"></h1>
    <div class="pills" id="pills"></div>
  </section>

  <section class="kpis" id="kpis"></section>

  <h2 class="sec">Notes on your usage <span class="hint">generated from your local data</span></h2>
  <section class="insights" id="insights"></section>

  <h2 class="sec" id="sec-tokens">Usage per day <span class="hint">hover segments · click a day to filter sessions</span></h2>
  <div class="grid2">
    <div class="card chart-card">
      <div class="ctitle">Tokens <small>input + output</small></div>
      <div id="tok-chart"></div>
      <div class="legend" id="tok-legend"></div>
    </div>
    <div class="card chart-card">
      <div class="ctitle">Messages</div>
      <div id="msg-chart"></div>
    </div>
  </div>

  <h2 class="sec" id="sec-env">Tools &amp; environment <span class="hint">click a tool to filter sessions</span></h2>
  <div class="grid2">
    <div class="card" id="tool-cards"><div class="chips" id="models"></div><div style="margin-top:18px" id="tools"></div></div>
    <div class="card"><div class="chips" id="projects"></div><div style="margin-top:18px"><div class="legend" id="agents"></div></div></div>
  </div>

  <h2 class="sec" id="sec-sessions">Sessions <span class="hint" id="sess-hint"></span></h2>
  <div class="card">
    <div class="tbl-tools">
      <input class="search" id="search" placeholder="Search sessions…">
      <select class="filter" id="agent-f"><option value="">All agents</option></select>
      <select class="filter" id="model-f"><option value="">All models</option></select>
      <span class="badge" id="sess-count"></span>
    </div>
    <div id="fbar" class="fbar" style="display:none"></div>
    <div style="overflow:auto">
      <table class="tbl" id="tbl">
        <thead><tr>
          <th data-k="title">Session</th><th>Agent / Model</th><th data-k="time_created">Started</th>
          <th class="num" data-k="duration_min">Duration</th><th class="num" data-k="msgs">Msgs</th>
          <th class="num" data-k="tools">Tools</th><th class="num" data-k="tokens">Tokens</th>
          <th class="num" data-k="cost">Cost</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <h2 class="sec" id="sec-heat">When you work <span class="hint">hover cells · click a day to filter sessions</span></h2>
  <div class="card"><div id="heat"></div></div>
</div>

<div class="overlay" id="overlay"><div class="modal" id="modal"></div></div>
<div class="tip" id="tip"></div>

<script>const S0 = __DATA__;</script>
<script>
const $=id=>document.getElementById(id);
const ESC=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const F=n=>{const s=Math.round(Number(n)||0).toString();return s.replace(/\B(?=(\d{3})+(?!\d))/g,',');};
const FN=n=>{n=Number(n)||0;return n>=1e6?(n/1e6).toFixed(2)+'M':n>=1e3?(n/1e3).toFixed(1)+'K':String(Math.round(n));};
const COST=c=>'$'+((Number(c)||0)?Number(c).toFixed(4):'0');
const DT=t=>{if(!t)return'—';const d=new Date(t>1e12?t:t*1000);return d.toLocaleDateString('en-CA')+' '+String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0');};
const DS=t=>{if(!t)return'';const d=new Date(t>1e12?t:t*1000);return d.toLocaleDateString('en-CA');};
const DUR=m=>{if(m==null)return'—';if(m<1)return'<1m';if(m<60)return Math.round(m)+'m';const h=Math.floor(m/60);return h+'h '+Math.round(m%60)+'m';};
let S=null; if(typeof S0!=='undefined'&&S0){S=S0;}
let sortK='time_created', sortAsc=false;
const FSTATE={day:null,model:null,tool:null,agent:null};
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

/* theme */
const THEMES={light:{accent:'#4f46e5',accent2:'#0d9488',accent3:'#9333ea',cache:'#d97706'},
              dark:{accent:'#7c86ff',accent2:'#2dd4bf',accent3:'#c084fc',cache:'#fbbf24'}};
let theme=localStorage.getItem('ocd-theme')||(matchMedia('(prefers-color-scheme: dark)').matches?'dark':'light');
document.documentElement.dataset.theme=theme;
const T=()=>THEMES[theme];
$('theme').textContent = theme==='dark' ? '☀ Light' : '◐ Dark';
$('theme').onclick=()=>{theme=theme==='dark'?'light':'dark';document.documentElement.dataset.theme=theme;
  localStorage.setItem('ocd-theme',theme);$('theme').textContent=theme==='dark'?'☀ Light':'◐ Dark';renderAll();};

/* svg */
const svg=(tag,at,html)=>'<'+tag+Object.entries(at).map(([k,v])=>' '+k+'="'+ESC(v)+'"').join('')+'>'+(html||'')+'</'+tag+'>';
function dayTip(d){
  return '<b>'+d.date+'</b>'+
    '<span class="trow"><span>Messages</span><b>'+F(d.msgs)+'</b></span>'+
    '<span class="trow"><span>Sessions</span><b>'+d.sessions+'</b></span>'+
    '<span class="trow"><span>Tool calls</span><b>'+F(d.tools)+'</b></span>'+
    '<span class="trow"><span>Tokens</span><b>'+FN(d.ti+d.to+d.tr+d.cache)+'</b></span>'+
    (d.cost?'<span class="trow"><span>Cost</span><b>'+COST(d.cost)+'</b></span>':'');
}
function dayDim(d){return FSTATE.day&&FSTATE.day!==d.date?' dim':'';}
function areaChart(days,c){
  const w=980,h=230,p=26;
  const vals=days.map(x=>x.msgs),mx=Math.max(...vals,1);
  const X=i=>vals.length>1?p+i*(w-2*p)/(vals.length-1):p+(w-2*p)/2,Y=v=>p+(1-v/mx)*(h-2*p);
  let d='M'+X(0).toFixed(1)+','+Y(vals[0]).toFixed(1);
  for(let i=1;i<vals.length;i++){const xm=(X(i-1)+X(i))/2;d+=' C'+xm.toFixed(1)+','+Y(vals[i-1]).toFixed(1)+' '+xm.toFixed(1)+','+Y(vals[i]).toFixed(1)+' '+X(i).toFixed(1)+','+Y(vals[i]).toFixed(1);}
  const fill='M'+X(0).toFixed(1)+','+(h-p).toFixed(1)+' L'+X(0).toFixed(1)+','+Y(vals[0]).toFixed(1)+d.slice(1)+' L'+X(vals.length-1).toFixed(1)+','+(h-p).toFixed(1)+' Z';
  const step=Math.ceil(days.length/10)||1;
  const hit=days.map((day,i)=>{const bw=(w-2*p)/Math.max(days.length,1);
    return svg('rect',{x:(X(i)-bw/2).toFixed(1),y:p,width:(bw*0.96).toFixed(1),height:(h-2*p).toFixed(1),fill:'transparent',
      class:'inter'+dayDim(day),'data-tip':dayTip(day),'data-f':'day|'+day.date});}).join('');
  const pts=days.map((day,i)=>day.msgs>0?svg('circle',{cx:X(i).toFixed(1),cy:Y(day.msgs).toFixed(1),r:3.6,fill:c,
      class:'inter'+dayDim(day),'data-tip':dayTip(day),'data-f':'day|'+day.date}):'').join('');
  const xl=days.map((day,i)=>i%step===0?svg('text',{x:X(i).toFixed(1),y:h-p+17,'text-anchor':'middle',fill:'#9a968c','font-size':10},day.date.slice(5)):'').join('');
  return '<svg viewBox="0 0 '+w+' '+(h+10)+'" width="100%">'
    +svg('path',{d:fill,fill:c,opacity:.13})+svg('path',{d:d,fill:'none',stroke:c,'stroke-width':2.2})
    +hit+pts+xl+'</svg>';
}
function stackChart(days,keys){
  const val=(d,k)=>k.p==='out'?(d.to||0)+(d.tr||0):(d[k.p]||0);
  const w=980,h=240,p=34;
  const max=Math.max(...days.map(d=>keys.reduce((a,k)=>a+val(d,k),0)),1);
  const Y=v=>p+(1-v/max)*(h-2*p),X=i=>p+i*(w-2*p)/Math.max(days.length-1,1);
  const slot=(w-2*p)/Math.max(days.length,1);
  const sw=Math.min(slot*0.28,p*0.8);
  const step=Math.ceil(days.length/10)||1;
  let cells='',xl='';
  days.forEach((day,i)=>{
    let acc=0,hitTip=dayTip(day);
    const x1=X(i)-sw,x2=X(i)+sw;
    keys.forEach(k=>{const v=val(day,k);if(!v)return;
      const segTip=('<b>'+k.l+' · '+day.date+'</b><span class="trow"><span>Tokens</span><b>'+FN(v)+'</b></span>'+
        '<span class="trow"><span>Share</span><b>'+(Math.round(v/(day.ti+day.to+day.tr||1)*100))+'%</b></span>');
      cells+=svg('path',{d:'M'+x1.toFixed(1)+','+Y(acc+v).toFixed(1)+' L'+x2.toFixed(1)+','+Y(acc+v).toFixed(1)+' L'+x2.toFixed(1)+','+Y(acc).toFixed(1)+' L'+x1.toFixed(1)+','+Y(acc).toFixed(1)+' Z',
        fill:k.c,class:'inter'+dayDim(day),'data-tip':segTip,'data-f':'day|'+day.date});
      acc+=v;});
    cells+=svg('rect',{x:(X(i)-slot/2).toFixed(1),y:p,width:(slot*0.96).toFixed(1),height:(h-2*p).toFixed(1),fill:'transparent',
      class:'inter'+dayDim(day),'data-tip':hitTip,'data-f':'day|'+day.date});
    if(i%step===0)xl+=svg('text',{x:X(i).toFixed(1),y:h-p+17,'text-anchor':'middle',fill:'#9a968c','font-size':10},day.date.slice(5));
  });
  return '<svg viewBox="0 0 '+w+' '+(h+10)+'" width="100%">'+cells+xl+'</svg>';
}
function heatCell(v,mx,h,day){
  if(!v)return'<div class="heat-cell" style="background:var(--chip)"></div>';
  const t=v/mx,bg='color-mix(in srgb,'+T().accent+' '+(8+70*t).toFixed(0)+'%, var(--bg))';
  const tip='<b>'+day+' · '+String(h).padStart(2,'0')+':00</b><span class=\'trow\'><span>Messages</span><b>'+v+'</b></span>';
  return '<div class="heat-cell inter'+(FSTATE.day&&FSTATE.day!==day?' dim':'')+'" style="background:'+bg+'" data-tip="'+tip+'" data-f="day|'+day+'"></div>';
}

/* renderers */
function renderAll(){ if(!S)return; renderHero();renderKpis();renderInsights();renderCharts();renderEnv();renderHeat();renderFilterBar();renderSessions(); }
function renderHero(){
  $('range').textContent='RECORDED '+S.range.toUpperCase()+' · REFRESHES AUTOMATICALLY';
  const t=S.totals;
  const hl=ESC(S.insights[0]||'No activity yet.').replace(/(\d[\d,]*(?:\.\d+)?)/g,'<b>$1</b>');
  $('headline').innerHTML=hl;
  const last=[...S.sessions].sort((a,b)=>b.time_created-a.time_created)[0];
  const lastTip=last?'<b>Latest session</b><span class=\'trow\'><span>'+ESC(last.agent)+' · '+DT(last.time_created)+'</span></span>':'';
  const ltitle=last?last.title:'—';
  $('pills').innerHTML=[
    ['Total tokens',FN(t.tokens_input+t.tokens_output+t.tokens_reasoning+t.tokens_cache_read)],
    ['Cost',COST(t.cost)],
    ['Model',S.models.length>1?S.models.length+' models':ESC((S.models[0]||['—'])[0].split('/').pop())],
    ['Latest',ltitle.length>36?ltitle.slice(0,36)+'…':ltitle]]
    .map(([k,v])=>'<span class="pill'+(k==='Latest'?' accent':'')+'" data-tip="'+(k==='Latest'?lastTip:'')+'">'+
      ESC(k)+' <b style="color:var(--text)">'+v+'</b></span>').join('');
}
function renderKpis(){
  const t=S.totals;
  const ti=t.tokens_input,to=t.tokens_output+t.tokens_reasoning,ca=t.tokens_cache_read;
  const total=ti+to+ca,c=T();
  const pct=v=>total?Math.round(v/total*100):0;
  const row=(lbl,v,color,sub)=>'<div class="krow"><span class="lbl"><i class="dot" style="background:'+color+'"></i>'+lbl+
    (sub?' <span class="dim">'+sub+'</span>':'')+'</span><b>'+v+'</b></div>';
  const tokCard='<div class="kpi"><h3>Tokens</h3><div class="big">'+FN(total)+' <small>combined</small></div>'+
    '<div class="tokbar">'+[[ti,c.accent],[to,c.accent2],[ca,c.cache]].map(([v,k])=>'<i style="width:'+pct(v)+'%;background:'+k+'"></i>').join('')+'</div>'+
    row('Input',FN(ti),c.accent,pct(ti)+'%')+row('Output · incl. reasoning',FN(to),c.accent2,pct(to)+'%')+
    row('Cache read',FN(ca),c.cache,pct(ca)+'%')+'</div>';
  const costCard='<div class="kpi"><h3>Cost by model</h3><div class="big">'+COST(t.cost)+' <small>total</small></div>'+
    (S.models.length?S.models.map(m=>{const n=m[0].split('/').pop(),mc=m[5]||0;
      const share=t.cost?(mc/t.cost*100).toFixed(0):'0';
      return row(ESC(n),mc>0?COST(mc):'—','var(--accent2)',share+'%');}).join(''):row('No models','—','var(--subtle)'))+
    '</div>';
  const actCard='<div class="kpi"><h3>Activity</h3><div class="big">'+F(t.sessions)+' <small>sessions</small></div>'+
    row('Messages',F(t.messages),c.accent)+row('Tool calls',F(t.tools),c.accent2)+
    row('Failed calls',F(t.failed_tools),'var(--bad)')+row('Avg session',DUR(t.avg_duration_min),'var(--subtle)')+'</div>';
  $('kpis').innerHTML=tokCard+costCard+actCard;
}
function renderInsights(){
  $('insights').innerHTML=S.insights.slice(1,5).map(ins=>'<div class="in"><p>'+ins.replace(/(\d[\d,]*(?:\.\d+)?)/g,'<b>$1</b>').replace(/\*\*(.*?)\*\*/g,'<b>$1</b>')+'</p></div>').join('');
}
function renderCharts(){
  const t=T(),d=S.days;
  if(!d.length){$('tok-chart').innerHTML='<div class="empty">Nothing yet.</div>';$('msg-chart').innerHTML='';return;}
  const keys=[{p:'ti',c:t.accent,l:'Input'},{p:'out',c:t.accent2,l:'Output · reasoning'}];
  $('tok-chart').innerHTML=stackChart(d,keys);
  $('tok-legend').innerHTML=keys.map(k=>'<span><i style="background:'+k.c+'"></i>'+k.l+'</span>').join('');
  $('msg-chart').innerHTML=areaChart(d,t.accent);
}
function renderEnv(){
  const dim=(v)=>FSTATE.model&&v!==FSTATE.model?' active':'';
  $('models').innerHTML='<span style="font-size:11px;text-transform:uppercase;letter-spacing:.1em;color:var(--subtle);font-weight:700;align-self:center">Models</span>'+
    S.models.map(m=>'<span class="chip'+(FSTATE.model===m[0]?' active':'')+'" data-f="model|'+ESC(m[0])+'" data-tip="'+
      ('<b>'+ESC(m[0])+'</b><span class=\'trow\'><span>Sessions</span><b>'+m[1]+'</b></span>'+
       '<span class=\'trow\'><span>Total tokens</span><b>'+FN(m[2])+'</b></span>'+
       '<span class=\'trow\'><span>Input / Output</span><b>'+FN(m[3])+' / '+FN(m[4])+'</b></span>'+
       '<span class=\'trow\'><span>Cost</span><b>'+COST(m[5])+'</b></span>')+'">'+
      '<b>'+FN(m[2])+'</b><span class="n">'+ESC(m[0].split('/').pop())+' · '+m[1]+' session'+(m[1]>1?'s':'')+'</span></span>').join('');
  $('projects').innerHTML='<span style="font-size:11px;text-transform:uppercase;letter-spacing:.1em;color:var(--subtle);font-weight:700;align-self:center">Projects</span>'+
    S.projects.slice(0,6).map(p=>'<span class="chip" data-tip="<b>'+ESC(p[0])+'</b><span class=\'trow\'><span>Sessions</span><b>'+p[1]+'</b></span>">'+
      '<b>'+F(p[1])+'</b><span class="n">'+ESC(p[0])+'</span></span>').join('');
  $('agents').innerHTML=S.agents.map(a=>'<span class="chip'+(FSTATE.agent===a[0]?' active':'')+'" data-f="agent|'+ESC(a[0])+'" data-tip="'+
    ('<b>'+ESC(a[0])+' agent</b><span class=\'trow\'><span>Sessions</span><b>'+a[1]+'</b></span>'+
     '<span class=\'trow\'><span>Tokens</span><b>'+FN(a[2])+'</b></span>')+'">'+
    '<i style="width:9px;height:9px;border-radius:3px;background:'+[T().accent,T().accent2,T().accent3][S.agents.indexOf(a)%3]+';display:inline-block"></i>'+
    ESC(a[0])+' · '+a[1]+' · '+FN(a[2])+'</span>').join('');
  const mx=S.tools[0]?S.tools[0][1]:1;
  $('tools').innerHTML=S.tools.length?'<div style="margin-bottom:10px;font-size:11px;text-transform:uppercase;letter-spacing:.1em;color:var(--subtle);font-weight:700">Tool calls</div>'
    +S.tools.map(([n,c])=>'<div class="tool clickable'+(FSTATE.tool===n?'':'')+(FSTATE.tool&&FSTATE.tool!==n?' dim':'')+'" data-f="tool|'+ESC(n)+'" data-tip="'+
      ('<b>'+ESC(n)+'</b><span class=\'trow\'><span>Calls</span><b>'+F(c)+'</b></span>'+
       '<span class=\'trow\'><span>Share</span><b>'+(c/mx*100).toFixed(0)+'%</b></span>')+'">'+
      '<div class="nm">'+ESC(n)+'</div><div class="track"><div class="fill" style="width:'+(c/mx*100).toFixed(1)+'%"></div></div>'+
      '<div class="ct">'+F(c)+' · '+(c/mx*100).toFixed(0)+'%</div></div>').join('')
    :'<div class="empty">No tool calls yet.</div>';
  requestAnimationFrame(()=>{document.querySelectorAll('.tool .fill').forEach(f=>{const w=f.style.width;f.style.width='0';requestAnimationFrame(()=>f.style.width=w);});});
}
function renderHeat(){
  const h=S.heatmap,mx=Math.max(...h.rows.flatMap(r=>r.slice(1)),1),step=Math.ceil(h.cols.length/8)||1;
  $('heat').innerHTML='<div>'+h.rows.map(r=>'<div class="heat-row"><span class="heat-lbl">'+String(r[0]).padStart(2,'0')+'</span>'+r.slice(1).map((v,i)=>heatCell(v,mx,r[0],h.cols[i])).join('')+'</div>').join('')+'</div>'
    +'<div class="heat-days" style="margin-top:6px">'+h.cols.map((c,i)=>'<span>'+(i%step===0?c.slice(5):'')+'</span>').join('')+'</div>';
}
/* sessions */
function sessionRows(){
  const q=($('search').value||'').toLowerCase();
  return S.sessions.filter(s=>{
    if(FSTATE.agent&&s.agent!==FSTATE.agent)return false;
    if(FSTATE.model&&s.model!==FSTATE.model)return false;
    if(FSTATE.day&&DS(s.time_created)!==FSTATE.day)return false;
    if(FSTATE.tool&&!s.tools_bd.some(t=>t[0]===FSTATE.tool))return false;
    if(q&&!(s.title||'').toLowerCase().includes(q)&&!(s.worktree||'').toLowerCase().includes(q))return false;
    return true;});
}
function renderSessions(){
  const rows=sessionRows();
  const mf=$('model-f');if(mf.options.length<=1)S.models.forEach(m=>mf.add(new Option(m[0].split('/').pop(),m[0])));
  mf.value=FSTATE.model||'';
  rows.sort((a,b)=>{let x=a[sortK],y=b[sortK];
    if(sortK==='tokens'){x=a.tokens_input+a.tokens_output+a.tokens_reasoning;y=b.tokens_input+b.tokens_output+b.tokens_reasoning;}
    return(x<y?-1:x>y?1:0)*(sortAsc?1:-1);});
  const fcount=Object.values(FSTATE).filter(Boolean).length;
  $('sess-count').textContent=F(rows.length)+' of '+F(S.sessions.length)+' sessions';
  $('sess-hint').textContent=(fcount?'filtered by '+fcount+' criteria · ':'')+'click a row for details · cost is the model share of tokens';
  const latestTime=[...S.sessions].sort((a,b)=>b.time_created-a.time_created)[0]?.time_created;
  const dayLbl=t=>{const d=new Date(t>1e12?t:t*1000);
    const s=new Date(d),n=new Date();s.setHours(0,0,0,0);n.setHours(0,0,0,0);
    const diff=Math.round((n-s)/86400000);
    const hh=String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0');
    return diff===0?'Today '+hh:diff===1?'Yesterday '+hh:(d.getMonth()+1)+'/'+d.getDate()+' '+hh;};
  const sessCost=s=>{
    if(s.cost>0)return COST(s.cost);
    const m=S.models.find(m=>m[0]===s.model);
    const tot=s.tokens_input+s.tokens_output+s.tokens_reasoning;
    if(!m||!m[2]||!tot||!m[5])return COST(0);
    return COST(m[5]*tot/m[2]);
  };
  const tb=$('tbl').querySelector('tbody');
  tb.innerHTML=rows.map(s=>{const tot=s.tokens_input+s.tokens_output+s.tokens_reasoning;
    const isLatest=s.time_created===latestTime;
    return'<tr data-i="'+rows.indexOf(s)+'"'+(isLatest?' class="latest"':'')+'><td><div class="t">'+ESC(s.title)+'</div>'+
    '<div class="badges"><span class="badge">'+ESC(s.worktree)+'</span>'+(isLatest?'<span class="badge latest">Latest</span>':'')+'</div></td>'
    +'<td><div class="badges mix"><span class="badge agent">'+ESC(s.agent)+'</span><span class="badge model">'+ESC(s.model)+'</span></div></td>'
    +'<td class="mono" style="font-size:11.5px;color:var(--muted)">'+dayLbl(s.time_created)+'</td>'
    +'<td class="num">'+DUR(s.duration_min)+'</td><td class="num">'+F(s.msgs)+'</td><td class="num">'+F(s.tools)+'</td>'
    +'<td class="num" style="font-weight:700">'+FN(tot)+'</td><td class="num">'+sessCost(s)+'</td></tr>';}).join('')
    ||'<tr><td colspan="8"><div class="empty">No sessions match.</div></td></tr>';
  tb.querySelectorAll('tr').forEach(tr=>tr.onclick=()=>{const r=rows[+tr.dataset.i];if(r)openSession(r);});
}
function renderFilterBar(){
  const bar=$('fbar');
  const active=Object.entries(FSTATE).filter(([k,v])=>v);
  if(!active.length){bar.style.display='none';return;}
  bar.style.display='flex';
  const labels={day:'Day',model:'Model',tool:'Tool',agent:'Agent'};
  bar.innerHTML='<span class="fl">Filtering:</span>'+active.map(([k,v])=>
    '<span class="fchip" data-k="'+k+'">'+labels[k]+': '+ESC(v)+' <span class="fx">✕</span></span>').join('')
    +'<button class="fclear">clear all</button>';
  bar.querySelectorAll('.fchip').forEach(ch=>ch.onclick=()=>{FSTATE[ch.dataset.k]=null;renderAll();});
  bar.querySelector('.fclear').onclick=()=>{for(const k in FSTATE)FSTATE[k]=null;$('agent-f').value='';$('model-f').value='';renderAll();};
}
function sortH(e){const k=e.target.dataset.k;if(!k)return;if(k===sortK)sortAsc=!sortAsc;else{sortK=k;sortAsc=false;}
  document.querySelectorAll('#tbl th').forEach(th=>th.classList.remove('asc','desc'));
  e.target.classList.add(sortAsc?'asc':'desc');renderSessions();}
function openSession(s){
  const tot=s.tokens_input+s.tokens_output+s.tokens_reasoning;
  const toolRows=s.tools_bd.map(([t,c])=>'<div class="tool" style="margin-bottom:9px"><div class="nm">'+ESC(t)+'</div><div class="track"><div class="fill" style="width:'+(c/Math.max(s.tools,1)*100).toFixed(1)+'%"></div></div><div class="ct">'+F(c)+'</div></div>').join('');
  $('modal').innerHTML='<button class="modal-x">✕</button>'
  +'<h2 style="font-size:18px;font-weight:700;letter-spacing:-.02em">'+ESC(s.title)+'</h2>'
  +'<div class="mmeta">'+['<span class="badge agent">'+ESC(s.agent)+'</span>','<span class="badge model">'+ESC(s.model)+'</span>',
    '<span class="badge">'+DT(s.time_created)+'</span>','<span class="badge">'+DUR(s.duration_min)+'</span>',
    '<span class="badge">'+F(s.msgs)+' msgs</span>','<span class="badge">'+F(s.tools)+' tool calls</span>',
    '<span class="badge">'+FN(tot)+' tokens</span>','<span class="badge">'+COST(s.cost)+'</span>'].join('')+'</div>'
  +'<div class="card" style="margin-top:16px"><div style="font-size:11px;text-transform:uppercase;letter-spacing:.1em;color:var(--subtle);font-weight:700;margin-bottom:12px">Tool breakdown</div>'+(toolRows||'<div class="empty">No tool calls.</div>')+'</div>'
  +'<div class="card" style="margin-top:12px"><div style="font-size:11px;text-transform:uppercase;letter-spacing:.1em;color:var(--subtle);font-weight:700;margin-bottom:12px">Prompts ('+s.prompts.length+')</div>'
  +(s.prompts.length?s.prompts.slice().reverse().map(p=>'<div class="prompt"><span class="mono" style="font-size:10.5px;color:var(--subtle)">'+DT(p.t)+'</span><br>'+ESC(p.p)+'</div>').join(''):'<div class="empty">No prompts recorded.</div>')+'</div>';
  $('overlay').classList.add('open');
}
/* interactions */
document.addEventListener('mousemove',e=>{
  const el=e.target.closest?e.target.closest('[data-tip]'):null;
  if(el&&el.dataset.tip){tipShow(el.dataset.tip,e);}
  else if(tip.style.display==='block'){tipHide();}
});
document.addEventListener('click',e=>{
  const el=e.target.closest?e.target.closest('[data-f]'):null;
  if(!el)return;
  const [k,v]=el.dataset.f.split('|');
  FSTATE[k]=FSTATE[k]===v?null:v;
  if(k==='agent'&&FSTATE.agent!==null)$('agent-f').value=FSTATE.agent;
  renderAll();
});
$('agent-f').addEventListener('change',e=>{FSTATE.agent=e.target.value||null;renderAll();});
$('model-f').addEventListener('change',e=>{FSTATE.model=e.target.value||null;renderAll();});
/* stop the hidden server when this window closes or quits (Cmd+Q) */
function stopServer(){try{navigator.sendBeacon&&navigator.sendBeacon('/api/close');}catch(e){try{fetch('/api/close',{method:'POST',keepalive:true});}catch(_){}}}
window.addEventListener('beforeunload',stopServer);
window.addEventListener('pagehide',stopServer);
/* boot */
let loading=false;
async function load(){
  if(loading)return;loading=true;$('refresh').disabled=true;
  try{const r=await fetch('/api/stats?_='+Date.now());if(!r.ok)throw new Error('HTTP '+r.status);
    S=await r.json();renderAll();$('status').textContent='Updated '+new Date().toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});}
  catch(e){$('status').textContent='Failed: '+e.message;}
  finally{loading=false;$('refresh').disabled=false;}
}
$('refresh').onclick=load;
$('export').onclick=()=>{const b=new Blob([JSON.stringify(S,null,2)],{type:'application/json'});
  const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='opencode-usage.json';a.click();};
$('overlay').onclick=e=>{if(e.target.id==='overlay')$('overlay').classList.remove('open');};
$('modal').addEventListener('click',e=>{if(e.target.classList.contains('modal-x'))$('overlay').classList.remove('open');});
document.addEventListener('keydown',e=>{if(e.key==='Escape')$('overlay').classList.remove('open');});
$('search').addEventListener('input',renderSessions);
document.querySelectorAll('#tbl th').forEach(th=>th.onclick=sortH);
document.addEventListener('visibilitychange',()=>{if(!document.hidden)load();});
setInterval(load,15000);
if(S){renderAll();$('status').textContent='Updated just now';}
load();
</script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    db_path = None
    server = None
    close_timer = None

    def _cancel_close(self):
        if Handler.close_timer:
            Handler.close_timer.cancel()
            Handler.close_timer = None

    @staticmethod
    def _close_now():
        if Handler.server:
            Handler.server.shutdown()

    def _arm_close(self):
        self._cancel_close()
        Handler.close_timer = threading.Timer(3.0, Handler._close_now)
        Handler.close_timer.daemon = True
        Handler.close_timer.start()

    def do_GET(self):
        if self.path.startswith(("/api/stats", "/")):
            self._cancel_close()
        if self.path in ("/", "/index.html"):
            con = connect(self.db_path)
            try:
                payload = json.dumps(query_stats(con))
            except Exception as e:
                payload = json.dumps({"error": str(e), "sessions": [], "days": [],
                                      "tools": [], "insights": ["Could not read the database."]})
            finally:
                con.close()
            body = PAGE.replace("__DATA__", payload).encode("utf-8")
            self._send(200, "text/html; charset=utf-8", body)
        elif self.path.startswith("/api/stats"):
            con = connect(self.db_path)
            try:
                body = json.dumps(query_stats(con)).encode("utf-8")
            except Exception as e:
                body = json.dumps({"error": str(e)}).encode("utf-8")
            finally:
                con.close()
            self._send(200, "application/json", body)
        else:
            self._send(404, "text/plain", b"not found")

    def do_POST(self):
        if self.path == "/api/close":
            self._send(200, "text/plain", b"bye")
            self._arm_close()
        else:
            self._send(404, "text/plain", b"not found")

    def _send(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print("[%s] %s" % (self.log_date_time_string(), fmt % args))


def main():
    parser = argparse.ArgumentParser(description="OpenCode usage dashboard")
    parser.add_argument("db", nargs="?", default=str(DB_PATH), help="path to opencode.db")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    Handler.db_path = args.db
    connect(args.db)

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    Handler.server = server
    url = f"http://127.0.0.1:{args.port}"
    print(f"Dashboard live at {url}  (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
