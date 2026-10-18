import json
import os
import sqlite3
import time
from datetime import datetime, timedelta

import pytest

import opencode_dashboard as d


def seed_db(path, n_sessions=40, days_back_max=400):
    """Create an opencode-schema-shaped SQLite DB with synthetic data."""
    con = sqlite3.connect(path)
    c = con.cursor()
    c.executescript("""
    CREATE TABLE session(id TEXT PRIMARY KEY, title TEXT, agent TEXT, model TEXT,
      time_created INTEGER, time_updated INTEGER, tokens_input INTEGER, tokens_output INTEGER,
      tokens_reasoning INTEGER, tokens_cache_read INTEGER, tokens_cache_write INTEGER,
      cost REAL, project_id TEXT);
    CREATE TABLE message(id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
    CREATE TABLE part(id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);
    CREATE TABLE project(id TEXT PRIMARY KEY, name TEXT, worktree TEXT);
    CREATE TABLE session_input(session_id TEXT, prompt TEXT, time_created INTEGER);
    """)
    c.execute("INSERT INTO project VALUES ('p1','Demo','/work/demo')")
    now_ms = int(datetime.now().timestamp() * 1000)
    model = json.dumps({"id": "anthropic/claude-sonnet-4-6"})
    for i in range(n_sessions):
        sid = f"s{i:04d}"
        start = now_ms - (i % days_back_max + 1) * 86400000
        c.execute(
            "INSERT INTO session VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (sid, f"Session {i}", "build", model, start, start + 60000,
             1000, 500, 100, 200, 100, 0.01, "p1"),
        )
        for j in range(3):
            mid = f"m{i}-{j}"
            mt = start + j * 10000
            role = "user" if j % 2 == 0 else "assistant"
            c.execute("INSERT INTO message VALUES (?,?,?,?)", (mid, sid, mt, json.dumps({"role": role})))
            if role == "user":
                c.execute("INSERT INTO session_input VALUES (?,?,?)", (sid, f"prompt {i} {j}", mt))
            else:
                c.execute("INSERT INTO part VALUES (?,?,?,?,?)",
                          (f"p{i}-{j}-t", mid, sid, mt, json.dumps(
                              {"type": "tool", "tool": "bash", "state": {"status": "completed"}})))
                c.execute("INSERT INTO part VALUES (?,?,?,?,?)",
                          (f"p{i}-{j}-s", mid, sid, mt + 1, json.dumps(
                              {"type": "step-finish",
                               "tokens": {"input": 100, "output": 50, "reasoning": 10, "total": 400, "cache": {"read": 40}},
                               "cost": 0.001})))
    con.commit()
    con.close()


def seed_db_2_0(path, n_sessions=5):
    """Create an OpenCode 2.0-shaped SQLite DB (session_v2/session_message).

    Also includes legacy `session`/`message` rows plus legacy parts, which a
    2.0 database can carry over; stats must ignore all of it and only count
    session_v2 / session_message / parts belonging to those sessions.
    """
    con = sqlite3.connect(path)
    c = con.cursor()
    c.executescript("""
    CREATE TABLE session_v2(id TEXT PRIMARY KEY, title TEXT, agent TEXT, model TEXT,
      time_created INTEGER, time_updated INTEGER, tokens_input INTEGER, tokens_output INTEGER,
      tokens_reasoning INTEGER, tokens_cache_read INTEGER, tokens_cache_write INTEGER,
      cost REAL, project_id TEXT);
    CREATE TABLE session_message(id TEXT PRIMARY KEY, session_id TEXT, type TEXT,
      time_created INTEGER, data TEXT);
    CREATE TABLE part(id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);
    CREATE TABLE project(id TEXT PRIMARY KEY, name TEXT, worktree TEXT);
    CREATE TABLE session(id TEXT PRIMARY KEY, title TEXT, agent TEXT, model TEXT,
      time_created INTEGER, time_updated INTEGER, tokens_input INTEGER, tokens_output INTEGER,
      tokens_reasoning INTEGER, tokens_cache_read INTEGER, tokens_cache_write INTEGER,
      cost REAL, project_id TEXT);
    CREATE TABLE message(id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
    """)
    c.execute("INSERT INTO project VALUES ('p1','Demo','/work/demo')")
    now_ms = int(datetime.now().timestamp() * 1000)
    model = json.dumps({"id": "opencode-go/deepseek-v4-flash"})
    for i in range(n_sessions):
        sid = f"v{i:04d}"
        start = now_ms - i * 1000
        c.execute(
            "INSERT INTO session_v2 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (sid, f"V2 Session {i}", "build", model, start, start + 60000,
             100, 50, 10, 200, 0, 0.001, "p1"),
        )
        c.execute(
            "INSERT INTO session_message VALUES (?,?,?,?,?)",
            (f"{sid}-u", sid, "user", start, json.dumps({"type": "user", "text": f"prompt {i}"})),
        )
        c.execute(
            "INSERT INTO session_message VALUES (?,?,?,?,?)",
            (f"{sid}-a", sid, "assistant", start + 1, json.dumps({"type": "assistant"})),
        )
        c.execute(
            "INSERT INTO part VALUES (?,?,?,?,?)",
            (f"{sid}-step", f"{sid}-a", sid, start + 1, json.dumps(
                {"type": "step-finish",
                 "tokens": {"input": 100, "output": 50, "reasoning": 10, "total": 400,
                            "cache": {"read": 40}},
                 "cost": 0.001})),
        )
    # s v0000 touched a file, attributed through the part scan
    c.execute(
        "INSERT INTO part VALUES ('v0000-edit','v0000-a','v0000',?,?)",
        (now_ms, json.dumps({"type": "tool", "tool": "edit",
                             "state": {"input": {"filePath": "/work/demo/app/a.py"}}})),
    )
    # legacy rows + legacy parts that must be excluded from every 2.0 aggregate
    c.execute(
        "INSERT INTO session VALUES ('legacy1','Legacy','build',?,?,?,?,?,?,?,?,?,?)",
        (json.dumps({"id": "anthropic/claude-sonnet-4-6"}), now_ms - 999000,
         now_ms - 998000, 900000, 900000, 0, 0, 0, 12.5, "p1"),
    )
    c.execute("INSERT INTO message VALUES ('lm1','legacy1',?,?)",
              (now_ms - 999000, json.dumps({"role": "user"})))
    c.execute(
        "INSERT INTO part VALUES ('lp1','lm1','legacy1',?,?)",
        (now_ms - 999000, json.dumps(
            {"type": "step-finish",
             "tokens": {"input": 900000, "output": 900000, "reasoning": 0, "total": 1800000},
             "cost": 12.5})),
    )
    c.execute(
        "INSERT INTO part VALUES ('lp2','lm1','legacy1',?,?)",
        (now_ms - 999000, json.dumps({"type": "tool", "tool": "edit",
                                      "state": {"input": {"filePath": "/legacy/old.py"}}})),
    )
    con.commit()
    con.close()


@pytest.fixture
def db_path(tmp_path):
    path = str(tmp_path / "opencode.db")
    seed_db(path)
    return path


@pytest.fixture(autouse=True)
def reset_caches():
    with d.LOCK:
        d.LOCAL_CACHE.clear()
        d.ROUTER_CACHE.clear()
        d.CLINE_CACHE.clear()
    yield


def seed_router_events(path):
    """Write a small usage-events.jsonl with ok / 429 / 500 rows."""
    rows = [
        {"at": "2026-09-04T10:00:00.000Z", "model": "opencode-free-responses/muse-spark-1.3-contributor-free",
         "provider": "opencode-free", "status": 200, "durationMs": 2000,
         "inputTokens": 1000, "cachedInputTokens": 200, "outputTokens": 100, "totalTokens": 1100},
        {"at": "2026-09-04T11:00:00.000Z", "model": "opencode-free-responses/muse-spark-1.3-contributor-free",
         "provider": "opencode-free", "status": 200, "durationMs": 3000,
         "inputTokens": 2000, "cachedInputTokens": 500, "outputTokens": 300, "totalTokens": 2300},
        {"at": "2026-09-04T12:00:00.000Z", "model": "gpt-5.6-sol",
         "provider": "openai", "status": 200, "durationMs": 1000,
         "inputTokens": 500, "cachedInputTokens": 0, "outputTokens": 50, "totalTokens": 550},
        {"at": "2026-09-05T09:00:00.000Z", "model": "opencode-go/deepseek-v4-flash",
         "provider": "opencode-go", "status": 429, "durationMs": 48},
        {"at": "2026-09-05T09:01:00.000Z", "model": "opencode-zen/muse-spark-1.3-contributor-free",
         "provider": "opencode-go", "status": 500, "durationMs": 6000},
        "not json at all",
    ]
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(r if isinstance(r, str) else json.dumps(r))
            f.write("\n")
    return path


def test_json_html_escapes_script_breakout():
    payload = d.json_html({"title": "</script><script>alert(1)</script>"})
    assert "</script>" not in payload
    assert "\\u003c/script" in payload
    # round-trips through json.loads to the original string
    assert json.loads(payload)["title"] == "</script><script>alert(1)</script>"


def test_json_html_escapes_unicode_separators():
    payload = d.json_html({"s": "a\u2028b\u2029c"})
    assert json.loads(payload)["s"] == "a\u2028b\u2029c"
    assert "\u2028" not in payload


def test_query_stats_shape_and_aggregates(db_path):
    con = d.connect(db_path)
    try:
        stats = d.query_stats(con)
    finally:
        con.close()
    assert set(("source", "source_label", "totals", "sessions", "tools", "agents",
                "models", "projects", "files", "subsystems", "file_subsys",
                "commits", "session_files", "commit_sessions",
                "days", "hours", "activity", "insights",
                "records", "range", "generated_at")) <= set(stats)
    assert stats["source"] == "local"
    assert stats["totals"]["sessions"] == 40
    assert stats["totals"]["messages"] == 120
    assert stats["totals"]["cost"] == pytest.approx(0.4, abs=0.01)
    # token-first totals (cache = read + write, matching part 'total' fields)
    assert stats["totals"]["tokens_total"] == (
        stats["totals"]["tokens_input"] + stats["totals"]["tokens_output"]
        + stats["totals"]["tokens_reasoning"] + stats["totals"]["tokens_cache_read"]
        + stats["totals"]["tokens_cache_write"]
    )
    assert 0 <= stats["totals"]["cache_rate"] <= 100
    assert len(stats["sessions"]) == 40
    assert len(stats["activity"]) == d.ACTIVITY_DAYS
    assert stats["activity"][-1]["date"] == datetime.now().date().isoformat()
    assert len(stats["hours"]) == 24
    assert "hour_tokens" not in stats
    # files/subsystems/commits present (seed has no edit/write parts, no git)
    assert stats["files"] == []
    assert stats["subsystems"] == []
    assert stats["commits"] == []
    assert "heatmap" not in stats
    assert all("prompts" not in s for s in stats["sessions"])
    assert all("tools_bd" not in s for s in stats["sessions"])
    assert all("tokens_total" in s and "burn" in s and "tokens_cache" in s for s in stats["sessions"])
    assert stats["days"] == sorted(stats["days"], key=lambda x: x["date"])
    assert all("total" in x for x in stats["days"])
    # models: [name, n, total, ti, to, tr, cache, cost, avg, cache_rate]
    assert stats["models"] and len(stats["models"][0]) == 10
    assert stats["models"] == sorted(stats["models"], key=lambda m: -m[2])
    # projects: [name, n, toks]
    assert all(len(p) == 3 for p in stats["projects"])
    # records
    assert stats["records"]["streaks"]["longest"] >= 1
    assert stats["records"]["biggest_day"] is not None


def test_query_stats_caps_sessions(tmp_path):
    path = str(tmp_path / "big.db")
    seed_db(path, n_sessions=800)
    con = d.connect(path)
    try:
        stats = d.query_stats(con)
    finally:
        con.close()
    assert len(stats["sessions"]) == d.MAX_SESSION_ROWS
    assert stats["totals"]["sessions"] == 800


def test_query_stats_opencode_2_0_schema(tmp_path):
    path = str(tmp_path / "opencode.db")
    seed_db_2_0(path, n_sessions=5)
    con = d.connect(path)
    try:
        stats = d.query_stats(con)
        prompts = d.session_prompts(con, "v0000")
    finally:
        con.close()
    # session_v2 / session_message only: the legacy session/message rows are ignored
    assert stats["totals"]["sessions"] == 5
    assert stats["totals"]["messages"] == 10
    assert stats["totals"]["tokens_input"] == 500
    assert stats["totals"]["tokens_output"] == 250
    assert stats["totals"]["tokens_reasoning"] == 50
    assert stats["totals"]["tokens_cache_read"] == 1000
    assert stats["totals"]["tokens_cache_write"] == 0
    assert stats["totals"]["tokens_total"] == 1800
    assert stats["totals"]["cost"] == pytest.approx(0.005)
    # exactly one model row, from session_v2 (legacy model excluded)
    assert len(stats["models"]) == 1
    row = stats["models"][0]
    assert row[0] == "opencode-go/deepseek-v4-flash"
    assert row[1] == 5
    assert row[2] == 1800
    assert row[7] == pytest.approx(0.005)
    # per-day token totals come from parts of session_v2 sessions only:
    # 5 step-finish parts x 400 total tokens (legacy 1.8M part excluded)
    assert sum(day["total"] for day in stats["days"]) == pytest.approx(2000)
    assert sum(day["msgs"] for day in stats["days"]) == 10
    assert sum(day["sessions"] for day in stats["days"]) == 5
    # file attribution is filtered the same way: only the v2 file is reported
    assert "/work/demo/app/a.py" in stats["file_subsys"]
    assert "/legacy/old.py" not in stats["file_subsys"]
    assert [f[0] for f in stats["files"]] == ["/work/demo/app/a.py"]
    assert stats["files"][0][1] == pytest.approx(360)  # v0000 session total
    # 2.0 prompts come from session_message rows with type='user'
    assert prompts and prompts[0]["p"] == "prompt 0"


def test_session_prompts_2_0_text_part_fallback(tmp_path):
    path = str(tmp_path / "opencode.db")
    seed_db_2_0(path, n_sessions=2)
    now = int(datetime.now().timestamp() * 1000)
    con = sqlite3.connect(path)
    # v0000's user session_message loses its data.text, so the session_message
    # scan yields nothing and the part fallback has to recover the prompt.
    con.execute(
        "UPDATE session_message SET data=? WHERE id='v0000-u'",
        (json.dumps({"type": "user"}),),
    )
    con.execute(
        "INSERT INTO part VALUES ('v0000-text','v0000-u','v0000',?,?)",
        (now, json.dumps({"type": "text", "text": "recovered 2.0 prompt"})),
    )
    # assistant text parts must stay excluded (type='assistant' in 2.0)
    con.execute(
        "INSERT INTO part VALUES ('v0000-text-a','v0000-a','v0000',?,?)",
        (now + 1, json.dumps({"type": "text", "text": "assistant text"})),
    )
    # orphan text parts (message id that does not join) are still allowed
    con.execute(
        "INSERT INTO part VALUES ('v0000-text-orphan','missing','v0000',?,?)",
        (now + 2, json.dumps({"type": "text", "text": "orphan text"})),
    )
    con.commit()
    con.close()
    con = d.connect(path)
    try:
        prompts = d.session_prompts(con, "v0000")
    finally:
        con.close()
    assert [p["p"] for p in prompts] == ["recovered 2.0 prompt", "orphan text"]


def test_ledger_marks_all_sessions_deleted_when_source_empty(tmp_path):
    db = str(tmp_path / "opencode.db")
    seed_db(db, n_sessions=3, days_back_max=2)
    ledger = tmp_path / "opencode.db.usage-ledger.sqlite"
    if ledger.exists():
        ledger.unlink()
    with d.LOCK:
        d.LOCAL_CACHE.clear()
    first = d.cached_local_stats(db)
    lcon = d.ledger_connect(db)
    try:
        row = lcon.execute("SELECT COUNT(*) AS n, SUM(deleted) AS d FROM session_usage").fetchone()
    finally:
        lcon.close()
    assert row["n"] == 3 and row["d"] == 0
    con = sqlite3.connect(db)
    con.execute("DELETE FROM session")
    con.execute("DELETE FROM message")
    con.execute("DELETE FROM part")
    con.commit()
    con.close()
    stamp = time.time() + 5
    os.utime(db, (stamp, stamp))
    with d.LOCK:
        d.LOCAL_CACHE.clear()
    second = d.cached_local_stats(db)
    lcon = d.ledger_connect(db)
    try:
        row = lcon.execute("SELECT COUNT(*) AS n, SUM(deleted) AS d FROM session_usage").fetchone()
    finally:
        lcon.close()
    # all three rows stay in the ledger, all marked deleted
    assert row["n"] == 3
    assert row["d"] == 3
    assert second["totals"]["tokens_total"] == first["totals"]["tokens_total"]
    assert second["totals"]["sessions"] == first["totals"]["sessions"]


def test_ledger_rehydrates_historical_rollups(tmp_path):
    db = str(tmp_path / "opencode.db")
    seed_db(db, n_sessions=6, days_back_max=2)
    ledger = tmp_path / "opencode.db.usage-ledger.sqlite"
    if ledger.exists():
        ledger.unlink()
    with d.LOCK:
        d.LOCAL_CACHE.clear()
    first = d.cached_local_stats(db)
    model = "anthropic/claude-sonnet-4-6"
    per_session = 1000 + 500 + 100 + 200 + 100  # 1900 tokens in the seed
    assert [m for m in first["models"] if m[0] == model]
    con = sqlite3.connect(db)
    con.execute("DELETE FROM session")
    con.execute("DELETE FROM message")
    con.execute("DELETE FROM part")
    con.commit()
    con.close()
    stamp = time.time() + 5
    os.utime(db, (stamp, stamp))
    with d.LOCK:
        d.LOCAL_CACHE.clear()
    second = d.cached_local_stats(db)
    # totals are unchanged...
    assert second["totals"]["tokens_total"] == first["totals"]["tokens_total"]
    assert second["totals"]["sessions"] == first["totals"]["sessions"]
    assert second["totals"]["messages"] == first["totals"]["messages"]
    # ...and the historical rollups survive the full deletion
    assert second["models"]
    rows = [m for m in second["models"] if m[0] == model]
    assert rows and len(rows[0]) == 10
    assert rows[0][1] == 6
    assert rows[0][2] == 6 * per_session
    assert second["models"] == sorted(second["models"], key=lambda m: -m[2])
    assert second["agents"]
    assert second["agents"][0][0] == "build"
    assert second["agents"][0][1] == 6
    assert second["agents"][0][2] == 6 * per_session
    assert second["projects"]
    assert second["projects"][0][0] == "Demo"
    assert second["projects"][0][2] == 6 * per_session
    rec = second["records"]
    assert rec["biggest_day"] is not None
    assert rec["biggest_day"]["sessions"] >= 1
    assert rec["biggest_session"] is not None
    assert rec["biggest_session"]["model"] == model
    assert rec["fastest_burn"] is not None
    assert rec["streaks"]["longest"] >= 1
    assert second["range"] != "no activity"
    # daily session/message counts keep the historical numbers too
    assert sum(d_["msgs"] for d_ in second["days"]) == 18
    assert sum(d_["sessions"] for d_ in second["days"]) == 6
    assert sum(slot["msgs"] for slot in second["activity"]) == 18
    # first insight is rebuilt from the ledger totals, not the live DB
    assert "6 sessions" in second["insights"][0]
    assert "11,400" in second["insights"][0]
    assert all("No activity recorded yet" not in i for i in second["insights"])


def test_ledger_retains_tokens_after_chat_deletion(tmp_path):
    db = str(tmp_path / "opencode.db")
    seed_db(db, n_sessions=6, days_back_max=2)
    ledger = tmp_path / "opencode.db.usage-ledger.sqlite"
    if ledger.exists():
        ledger.unlink()
    with d.LOCK:
        d.LOCAL_CACHE.clear()
    first = d.cached_local_stats(db)
    before = first["totals"]["tokens_total"]
    before_messages = first["totals"]["messages"]
    assert before > 0 and before_messages > 0
    assert ledger.exists()
    # delete every chat from the source DB, then make sure the cache miss happens
    con = sqlite3.connect(db)
    con.execute("DELETE FROM session")
    con.execute("DELETE FROM message")
    con.execute("DELETE FROM part")
    con.commit()
    con.close()
    stamp = time.time() + 5
    os.utime(db, (stamp, stamp))
    with d.LOCK:
        d.LOCAL_CACHE.clear()
    second = d.cached_local_stats(db)
    assert second is not first
    assert second["totals"]["tokens_total"] == before
    assert second["totals"]["sessions"] == first["totals"]["sessions"]
    assert second["totals"]["messages"] == before_messages


def test_handler_keep_alive_blocks_shutdown():
    calls = []

    class FakeServer:
        def shutdown(self):
            calls.append(True)

    old_keep = d.Handler.keep_alive
    old_server = d.Handler.server
    try:
        d.Handler.server = FakeServer()
        d.Handler.keep_alive = True
        d.Handler._close_now()
        assert calls == []  # TUI mode: closing the Chrome UI must not stop the server
        d.Handler.keep_alive = False
        d.Handler._close_now()
        assert calls == [True]
    finally:
        d.Handler.keep_alive = old_keep
        d.Handler.server = old_server


def test_api_close_does_not_arm_timer_with_keep_alive(monkeypatch):
    sent = []
    armed = []
    monkeypatch.setattr(d.Handler, "_send", lambda self, *a: sent.append(a))
    monkeypatch.setattr(d.Handler, "_arm_close", lambda self: armed.append(True))
    handler = object.__new__(d.Handler)
    handler.path = "/api/close?wid=w1"
    handler.headers = {}
    old_keep = d.Handler.keep_alive
    try:
        d.Handler.keep_alive = True
        handler.do_POST()
        assert sent and armed == []
        d.Handler.keep_alive = False
        handler.do_POST()
        assert armed == [True]
    finally:
        d.Handler.keep_alive = old_keep


def test_open_chrome_window_matches_launcher(monkeypatch):
    calls = []

    def fake_popen(args, **kwargs):
        calls.append(args)
        return object()

    monkeypatch.setattr(d.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(d.sys, "platform", "darwin")
    monkeypatch.setattr(d.Path, "is_dir", lambda self: True)
    assert d.open_chrome_window("http://127.0.0.1:8765") is True
    cmd = calls[0]
    assert cmd[0:3] == ["open", "-na", "/Applications/Google Chrome.app"]
    assert "--app=http://127.0.0.1:8765" in cmd
    assert "--window-size=1280,1600" in cmd
    profile = [a for a in cmd if a.startswith("--user-data-dir=")]
    assert profile and profile[0].endswith("/.cache/ocd-dashboard-chrome")
    assert d.open_chrome_window(None) is False


def test_open_chrome_window_falls_back_to_webbrowser(monkeypatch):
    import webbrowser

    monkeypatch.setattr(d.sys, "platform", "linux")
    opened = []
    monkeypatch.setattr(webbrowser, "open", lambda url, *a, **k: opened.append(url) or True)
    assert d.open_chrome_window("http://127.0.0.1:8765") is True
    assert opened == ["http://127.0.0.1:8765"]


def test_tui_layout_is_responsive_and_bounded():
    assert d.tui_layout(120, 40)["mode"] == "wide"
    assert d.tui_layout(90, 30)["mode"] == "medium"
    assert d.tui_layout(70, 24)["mode"] == "narrow"
    assert d.tui_layout(50, 16)["mode"] == "compact"
    assert d.tui_layout(40, 10)["cards"] == []
    assert d.tui_layout(40, 10)["chart"] is None
    wide = d.tui_layout(120, 40)
    assert len(wide["cards"]) == 4
    assert wide["chart"] and wide["models"] and wide["sessions"] and wide["records"]
    assert wide["card_h"] >= 5
    medium = d.tui_layout(90, 30)
    assert len(medium["cards"]) == 3
    assert medium["chart"] and medium["models"] and medium["sessions"]
    narrow = d.tui_layout(70, 24)
    assert len(narrow["cards"]) == 2
    assert narrow["chart"] and narrow["models"] and narrow["sessions"]
    # every panel gets room for a header + separator + at least one data row
    assert narrow["sessions"][2] >= 5
    assert medium["sessions"][2] >= 5
    assert wide["sessions"][2] >= 5
    # every rect stays inside the terminal and no two panels overlap
    for w, h in ((120, 40), (90, 30), (70, 24), (60, 20)):
        layout = d.tui_layout(w, h)
        rects = list(layout["cards"]) + [layout[k] for k in
                                         ("chart", "models", "sessions", "records")]
        rects = [r for r in rects if r is not None]
        for y, x, hh, ww in rects:
            assert 0 <= y and 0 <= x and hh > 1 and ww > 1
            assert y + hh <= h and x + ww <= w
        for i, a in enumerate(rects):
            for b in rects[i + 1:]:
                ay, ax, ah, aw = a
                by, bx, bh, bw = b
                overlaps = not (ay + ah <= by or by + bh <= ay
                                or ax + aw <= bx or bx + bw <= ax)
                assert not overlaps, (w, h, a, b)


def test_tui_period_totals_window_and_keys():
    days = [
        {"date": "2026-01-01", "ti": 100, "to": 50, "tr": 10, "cache": 40,
         "msgs": 3, "sessions": 1, "cost": 0.01, "ok": 3, "err": 0},
        {"date": "2026-01-02", "ti": 200, "to": 100, "tr": 20, "cache": 80,
         "msgs": 4, "sessions": 2, "cost": 0.02, "ok": 2, "err": 2, "err429": 2,
         "what_if": 0.3},
    ]
    last = d.tui_period_totals({"days": days}, 1)
    assert last["days_used"] == 1
    assert last["tokens_total"] == 400
    assert last["messages"] == 4
    assert last["sessions"] == 2
    allp = d.tui_period_totals({"days": days}, 0)
    assert allp["tokens_total"] == 600
    assert allp["messages"] == 7
    assert allp["sessions"] == 3
    assert allp["cost"] == pytest.approx(0.03)
    assert allp["err"] == 2 and allp["err429"] == 2
    assert allp["what_if"] == pytest.approx(0.3)
    assert allp["vmax"] == 400
    empty = d.tui_period_totals(None, 14)
    assert empty["tokens_total"] == 0 and empty["days_used"] == 0


def test_tui_scale_blocks_buckets_and_heat():
    assert d.tui_scale_blocks([1, 2, 3, 4], 2) == [3.0, 7.0]
    assert d.tui_scale_blocks([1, 2], 5) == [1.0, 2.0]
    assert d.tui_scale_blocks([], 3) == []
    assert d.tui_scale_blocks([1, 2, 3], 0) == []
    assert d.tui_scale_blocks(["x", "4"], 2) == [0.0, 4.0]
    assert d.tui_heat(0, 100) == 0
    assert d.tui_heat(10, 100) == 1
    assert d.tui_heat(30, 100) == 2
    assert d.tui_heat(60, 100) == 3
    assert d.tui_heat(90, 100) == 4
    assert d.tui_heat(5, 0) == 0


def test_tui_fit_wrap_and_filter():
    assert d.tui_fit("hello", 10) == "hello"
    assert d.tui_fit("hello world", 8).endswith("…")
    assert len(d.tui_fit("hello world", 8)) == 8
    assert d.tui_fit("hello", 0) == ""
    assert d.tui_fit(None, 4) == ""
    assert d.tui_wrap("one two three four", 7) == ["one two", "three", "four"]
    assert d.tui_wrap("", 10) == [""]
    assert d.tui_wrap("supercalifragilistic", 6) == ["superc", "alifra", "gilist", "ic"]
    rows = [{"title": "Fix bug", "model": "a/b"}, {"title": "Ship feature", "model": "c/d"}]
    assert d.tui_filter_sessions(rows, "") == rows
    assert d.tui_filter_sessions(rows, "BUG", ("title",)) == rows[:1]
    assert d.tui_filter_sessions(rows, "C/D", ("title", "model")) == rows[1:]
    assert d.tui_filter_sessions(None, "x") == []


def test_tui_model_badge_and_cost_helpers():
    # OpenCode-shaped rows: index 7 is the real session cost
    assert d.tui_model_badge(["muse-x-free", 1, 10, 0, 0, 0, 0, 0.0001]) == "free"
    assert d.tui_model_badge(["local", 1, 10, 0, 0, 0, 0, 0.0]) == "$0"
    assert d.tui_model_badge(["paid", 1, 10, 0, 0, 0, 0, 1.5]) == ""
    assert d.tui_model_badge(["x", 1, 10, 0, 0, 0, 0, 1.0, 0, 0, "p", True]) == "free"
    assert d.tui_model_badge(None) == ""
    assert d.tui_model_badge(["x"]) == "$0"
    # Codex/Cline-shaped rows: index 7 is the error count, index 12 is cost
    # (what-if for Codex, always 0 for Cline), index 11 is the free flag.
    codex_errors = ["gpt-5.6-sol", 10, 5000, 3000, 1000, 500, 9, 3,
                    500, 40.0, "openai", False, 12.5]
    assert d.tui_model_badge(codex_errors, 12) == ""  # what_if > 0, no $0
    codex_free = ["muse-spark-1.3-contributor-free", 10, 5000, 3000, 1000, 500,
                  10, 3, 500, 40.0, "opencode-free", True, 12.5]
    assert d.tui_model_badge(codex_free, 12) == "free"
    codex_zero = ["local-model", 10, 5000, 3000, 1000, 500, 10, 0,
                  500, 40.0, "local", False, 0.0]
    assert d.tui_model_badge(codex_zero, 12) == "$0"
    # an err=0 row with a positive what-if must not read as $0 (the old bug)
    assert d.tui_model_badge(codex_errors, 12) != "$0"
    assert d.tui_free_model_count([codex_zero, codex_errors, codex_free], 12) == 2
    assert d.tui_free_model_count([["free-x", 1, 1, 0, 0, 0, 0, 0.1],
                                   ["paid", 1, 1, 0, 0, 0, 0, 1.0],
                                   None]) == 1
    assert d.tui_fmt_cost(0) == "$0"
    assert d.tui_fmt_cost(0.0003) == "$0.0003"
    assert d.tui_fmt_cost(1.5) == "$1.50"
    assert d.tui_fmt_cost("bad") == "$0"


def test_activity_window_is_zero_filled(db_path):
    con = d.connect(db_path)
    try:
        stats = d.query_stats(con)
    finally:
        con.close()
    recorded = {x["date"] for x in stats["days"]}
    assert len(stats["activity"]) == 364
    # every recorded day is present in the padded window
    for slot in stats["activity"]:
        if slot["date"] in recorded:
            assert slot["msgs"] > 0
    empty = [s for s in stats["activity"] if s["date"] not in recorded]
    assert empty
    assert all(s["msgs"] == 0 and s["cost"] == 0 and "tools" not in s for s in empty)


def test_session_prompts_primary(db_path):
    con = d.connect(db_path)
    try:
        prompts = d.session_prompts(con, "s0000")
    finally:
        con.close()
    assert prompts and prompts[0]["p"] == "prompt 0 0"


def test_session_prompts_fallback(tmp_path):
    path = str(tmp_path / "opencode.db")
    seed_db(path)
    con = sqlite3.connect(path)
    con.execute("DELETE FROM session_input")
    # add a user text part (older opencode schema) so the fallback can recover it
    con.execute(
        "INSERT INTO part VALUES ('fb-t1','m0-0','s0000',? ,?)",
        (int(datetime.now().timestamp() * 1000), json.dumps({"type": "text", "text": "fallback prompt"})),
    )
    con.commit()
    con.close()
    con = d.connect(path)
    try:
        prompts = d.session_prompts(con, "s0000")
    finally:
        con.close()
    assert any("fallback prompt" in p["p"] for p in prompts)


def test_blank_stats_full_shape():
    stats = d.blank_stats("local", "Local OpenCode data", error="boom", insight="sad")
    assert stats["error"] == "boom"
    assert stats["insights"] == ["sad"]
    assert stats["totals"]["sessions"] == 0
    assert len(stats["activity"]) == 364
    assert len(stats["hours"]) == 24
    assert stats["files"] == []
    assert stats["subsystems"] == []
    assert stats["commits"] == []
    assert stats["records"]["biggest_day"] is None


def test_day_total_and_cache_rate():
    assert d.day_total({"ti": 100, "to": 50, "tr": 10, "cache": 40}) == 200
    assert d.cache_rate(100, 100) == 50.0
    assert d.cache_rate(0, 0) == 0.0
    # Codex Router reports cached input as a subset of total input.
    assert d.router_cache_rate(100, 40) == 40.0
    assert d.router_cache_rate(100, 120) == 100.0
    assert d.router_cache_rate(0, 0) == 0.0


def test_compute_streaks():
    act = d.zero_days_window()
    # last 3 days active
    for slot in act[-3:]:
        slot["msgs"] = 1
    streaks = d.compute_streaks(act)
    assert streaks["current"] == 3
    assert streaks["longest"] >= 3


def test_models_sorted_by_total_and_cache(db_path):
    con = d.connect(db_path)
    try:
        stats = d.query_stats(con)
    finally:
        con.close()
    totals = [m[2] for m in stats["models"]]
    assert totals == sorted(totals, reverse=True)
    # cache per model present
    assert all(m[6] >= 0 for m in stats["models"])


def test_cached_local_stats_invalidation(db_path):
    first = d.cached_local_stats(db_path)
    assert d.cached_local_stats(db_path) is first  # cached
    os.utime(db_path, (time.time(), time.time()))
    second = d.cached_local_stats(db_path)
    assert second is not first


def test_zero_days_window():
    window = d.zero_days_window()
    assert len(window) == 364
    first = datetime.fromisoformat(window[0]["date"]).date()
    last = datetime.fromisoformat(window[-1]["date"]).date()
    assert last - first == timedelta(days=363)
    assert last == datetime.now().date()


def test_router_short_and_free():
    assert d.router_short("opencode-free-responses/muse-spark-1.3-contributor-free") == "muse-spark-1.3-contributor-free"
    assert d.router_short("") == "—"
    assert d.router_is_free("x/muse-spark-1.3-contributor-free") is True
    assert d.router_is_free("gpt-5.6-sol") is False


def test_what_if_cost():
    assert d.what_if_cost("muse-spark-1.3-contributor-free", 1_000_000, 1_000_000) == pytest.approx(0.30)
    assert d.what_if_cost("gpt-5.6-sol", 1_000_000, 1_000_000) == 0.0


def test_parse_router_events_mixed(tmp_path):
    path = seed_router_events(str(tmp_path / "usage-events.jsonl"))
    events = d.parse_router_events(path)
    assert len(events) == 5  # bad line skipped
    ok = [e for e in events if e["ok"]]
    assert len(ok) == 3
    err429 = [e for e in events if e["status"] == 429]
    assert len(err429) == 1
    # error rows carry zero tokens (actuals only, never estimated)
    assert err429[0]["total"] == 0
    spark = [e for e in events if e["short"] == "muse-spark-1.3-contributor-free" and e["ok"]]
    assert len(spark) == 2
    assert spark[0]["free"] is True
    assert spark[0]["what_if"] > 0


def test_query_router_stats_aggregates(tmp_path):
    path = seed_router_events(str(tmp_path / "usage-events.jsonl"))
    stats = d.query_router_stats(path)
    assert stats["source"] == "router"
    assert stats["source_label"] == "Codex data"
    assert stats["totals"]["requests"] == 5
    assert stats["totals"]["ok"] == 3
    assert stats["totals"]["errors"] == 2
    assert stats["totals"]["err429"] == 1
    assert stats["totals"]["err500"] == 1
    assert stats["totals"]["tokens_total"] == (
        stats["totals"]["tokens_input"] + stats["totals"]["tokens_output"]
        + stats["totals"]["tokens_reasoning"]
    )
    assert stats["totals"]["cache_rate"] == pytest.approx(
        stats["totals"]["tokens_cache_read"] / stats["totals"]["tokens_input"] * 100,
        abs=0.1,
    )
    assert len(stats["models"]) == 4
    assert stats["models"] == sorted(stats["models"], key=lambda m: -m[2])
    # model rows: [name, reqs, total, ti, to, cache, ok, err, avg, hit, provider, free, what_if]
    assert all(len(m) == 13 for m in stats["models"])
    assert len(stats["days"]) == 2
    assert all(day["total"] == pytest.approx(day["ti"] + day["to"] + day["tr"])
               for day in stats["days"])
    assert len(stats["hours"]) == 24
    assert len(stats["requests"]) == 5
    assert stats["range"].startswith("2026-09-04")
    assert stats["totals"]["what_if_cost"] > 0
    # mirrored OpenCode records
    rec = stats["records"]
    assert rec["biggest_day"]["date"] == "2026-09-04"
    assert rec["biggest_model"]["name"] == "opencode-free-responses/muse-spark-1.3-contributor-free"
    assert rec["biggest_model"]["requests"] == 2
    assert rec["biggest_request"]["total"] == 2300
    # 11:00Z event carries the most tokens; compare in local time (TZ-robust)
    assert rec["peak_hour"] == d._router_event_day_hour("2026-09-04T11:00:00.000Z")[1]
    assert rec["streaks"]["longest"] >= 1


def test_blank_router_stats_shape():
    stats = d.blank_router_stats(error="boom")
    assert stats["error"] == "boom"
    assert stats["totals"]["requests"] == 0
    assert len(stats["hours"]) == 24
    assert len(stats["activity"]) == d.ACTIVITY_DAYS
    assert stats["records"]["biggest_day"] is None
    assert stats["records"]["biggest_model"] is None
    assert stats["records"]["biggest_request"] is None


def test_cached_router_stats_invalidation(tmp_path):
    path = seed_router_events(str(tmp_path / "usage-events.jsonl"))
    first = d.cached_router_stats(path)
    assert d.cached_router_stats(path) is first  # cached
    os.utime(path, (time.time(), time.time()))
    second = d.cached_router_stats(path)
    assert second is not first


def test_router_missing_file():
    with pytest.raises(RuntimeError):
        d.cached_router_stats("/nonexistent/usage-events.jsonl")
    stats = d.blank_router_stats(error="x")
    assert stats["source"] == "router"


def test_agents_projects_include_cache(db_path):
    con = d.connect(db_path)
    try:
        stats = d.query_stats(con)
    finally:
        con.close()
    # seed: each session has ti=1000, to=500, tr=100, cache_read=200, cache_write=100
    build = [a for a in stats["agents"] if a[0] == "build"][0]
    assert build[1] == 40
    assert build[2] == 40 * (1000 + 500 + 100 + 200 + 100)
    proj = [p for p in stats["projects"] if p[0] == "Demo"][0]
    assert proj[2] == 40 * (1000 + 500 + 100 + 200 + 100)


def test_records_from_full_table_beyond_cap(tmp_path):
    path = str(tmp_path / "big.db")
    seed_db(path, n_sessions=600)
    con = sqlite3.connect(path)
    # an old session (outside the 500 most-recent) with huge tokens
    old = int((datetime.now() - timedelta(days=300)).timestamp() * 1000)
    con.execute(
        "INSERT INTO session VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("sold", "Ancient giant", "build", json.dumps({"id": "m"}),
         old, old + 10 * 60000, 10_000_000, 0, 0, 0, 0, 0.0, "p1"),
    )
    con.commit()
    con.close()
    con = d.connect(path)
    try:
        stats = d.query_stats(con)
    finally:
        con.close()
    assert len(stats["sessions"]) == d.MAX_SESSION_ROWS
    assert stats["records"]["biggest_session"]["title"] == "Ancient giant"
    assert stats["records"]["biggest_session"]["total"] == 10_000_000
    assert stats["records"]["fastest_burn"] is not None


def test_streak_skips_quiet_today():
    act = d.zero_days_window()
    # active 5 days ending yesterday, today quiet
    for slot in act[-6:-1]:
        slot["msgs"] = 1
    streaks = d.compute_streaks(act)
    assert streaks["current"] == 5
    assert streaks["longest"] >= 5
    # all quiet -> 0, not crash
    assert d.compute_streaks(d.zero_days_window()) == {"current": 0, "longest": 0}


def test_router_streak_ignores_error_only_days():
    act = d.zero_days_window()
    yesterday = act[-2]
    yesterday["msgs"] = 3  # requests...
    yesterday["sessions"] = 0  # ...but none ok, no tokens
    yesterday["ti"] = 0
    yesterday["to"] = 0
    act[-3]["msgs"] = 2
    act[-3]["sessions"] = 2
    act[-3]["ti"] = 100
    act[-3]["to"] = 10
    assert d.compute_streaks(act, tokens_only=True)["current"] == 0
    yesterday["ti"] = 50
    assert d.compute_streaks(act, tokens_only=True)["current"] == 2


def test_router_unmetered_and_ok_only_latency(tmp_path):
    rows = [
        {"at": "2026-09-04T10:00:00.000Z", "model": "local", "provider": "local",
         "status": 200, "durationMs": 20},
        {"at": "2026-09-04T11:00:00.000Z", "model": "gpt-5.6-sol", "provider": "openai",
         "status": 200, "durationMs": 4000,
         "inputTokens": 1000, "cachedInputTokens": 0, "outputTokens": 100, "totalTokens": 1100},
        {"at": "2026-09-04T12:00:00.000Z", "model": "gpt-5.6-sol", "provider": "openai",
         "status": 429, "durationMs": 5},
    ]
    path = str(tmp_path / "usage-events.jsonl")
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    stats = d.query_router_stats(path)
    assert stats["totals"]["unmetered"] == 1
    assert stats["totals"]["requests"] == 2  # unmetered excluded from all counts
    assert stats["totals"]["ok"] == 1
    assert stats["totals"]["messages"] == 2
    assert stats["totals"]["tokens_total"] == 1100
    # 20ms unmetered row and 5ms 429 reject excluded from the latency average
    assert stats["totals"]["avg_ms"] == pytest.approx(4000)
    assert len(stats["models"]) == 1  # unmetered 'local' model excluded
    assert any("excluded from all counts" in i for i in stats["insights"])


def test_router_truncation_flag(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "MAX_ROUTER_EVENTS", 3)
    path = str(tmp_path / "usage-events.jsonl")
    with open(path, "w", encoding="utf-8") as f:
        for i in range(5):
            f.write(json.dumps(
                {"at": f"2026-09-04T1{i}:00:00.000Z", "model": "m", "provider": "p",
                 "status": 200, "durationMs": 10,
                 "inputTokens": 10, "outputTokens": 1, "totalTokens": 11}) + "\n")
    stats = d.query_router_stats(path)
    assert stats["truncated"] is True
    assert stats["scanned"] == 3
    assert stats["total_lines"] == 5
    assert any("most recent" in i for i in stats["insights"])


def test_router_day_err_breakdown(tmp_path):
    path = seed_router_events(str(tmp_path / "usage-events.jsonl"))
    stats = d.query_router_stats(path)
    by_date = {x["date"]: x for x in stats["days"]}
    assert by_date["2026-09-05"]["err429"] == 1
    assert by_date["2026-09-05"]["err500"] == 1
    # padded activity keeps the breakdown for heatmap/rollup consistency
    act = [s for s in stats["activity"] if s["date"] == "2026-09-05"][0]
    assert act["reqs"] == 2 and act["ok"] == 0 and act["err"] == 2
    assert act["err429"] == 1 and act["err500"] == 1


def test_router_recent_sorted_by_time(tmp_path):
    path = str(tmp_path / "usage-events.jsonl")
    rows = [
        {"at": "2026-09-04T12:00:00.000Z", "model": "m", "provider": "p", "status": 200,
         "durationMs": 1, "inputTokens": 1, "outputTokens": 1, "totalTokens": 2},
        {"at": "2026-09-05T09:00:00+00:00", "model": "m", "provider": "p", "status": 200,
         "durationMs": 1, "inputTokens": 1, "outputTokens": 1, "totalTokens": 2},
        {"at": "2026-09-04T15:30:00.000Z", "model": "m", "provider": "p", "status": 200,
         "durationMs": 1, "inputTokens": 1, "outputTokens": 1, "totalTokens": 2},
    ]
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    stats = d.query_router_stats(path)
    assert [r["at"] for r in stats["requests"]] == [
        "2026-09-05T09:00:00+00:00",
        "2026-09-04T15:30:00.000Z",
        "2026-09-04T12:00:00.000Z",
    ]


def test_fingerprint_covers_wal(tmp_path):
    db = str(tmp_path / "opencode.db")
    seed_db(db)
    first = d._fingerprint_paths([db])
    # simulate WAL growth without touching the main file
    with open(db + "-wal", "wb") as f:
        f.write(b"\0" * 100)
    second = d._fingerprint_paths([db])
    assert first != second


def test_subsystem_for_file():
    wt = ["/work/demo", "/other/repo"]
    assert d.subsystem_for_file("/work/demo/app/main.py", wt) == "app"
    assert d.subsystem_for_file("/work/demo/README.md", wt) == "README.md"
    assert d.subsystem_for_file("/other/repo/x/y.py", wt) == "x"
    # outside every worktree -> parent folder name, never dropped
    assert d.subsystem_for_file("/tmp/reelwork/build_cuts.py", wt) == "reelwork"
    assert d.subsystem_for_file("lonely.py", wt) == "(root)"


def test_files_equal_split_and_session_filter(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "collect_repo_commits", lambda *a, **k: [])
    path = str(tmp_path / "opencode.db")
    seed_db(path, n_sessions=4)
    con = sqlite3.connect(path)
    # s0000 touches two files; s0001 touches one. Each seed session = 1900 toks.
    con.execute(
        "INSERT INTO part VALUES ('f1','m0-1','s0000',?,?)",
        (int(datetime.now().timestamp() * 1000),
         json.dumps({"type": "tool", "tool": "edit",
                     "state": {"input": {"filePath": "/work/demo/app/a.py"}}})),
    )
    con.execute(
        "INSERT INTO part VALUES ('f2','m0-1','s0000',?,?)",
        (int(datetime.now().timestamp() * 1000),
         json.dumps({"type": "tool", "tool": "write",
                     "state": {"input": {"filePath": "/work/demo/app/b.py"}}})),
    )
    con.execute(
        "INSERT INTO part VALUES ('f3','m1-1','s0001',?,?)",
        (int(datetime.now().timestamp() * 1000),
         json.dumps({"type": "tool", "tool": "edit",
                     "state": {"input": {"filePath": "/work/demo/app/a.py"}}})),
    )
    con.commit()
    con.close()
    con = d.connect(path)
    try:
        stats = d.query_stats(con)
    finally:
        con.close()
    by_file = {r[0]: r for r in stats["files"]}
    # s0000 (1900 toks) split 950/950; s0001 (1900) all on a.py
    assert by_file["/work/demo/app/a.py"][1] == pytest.approx(950 + 1900)
    assert by_file["/work/demo/app/b.py"][1] == pytest.approx(950)
    assert by_file["/work/demo/app/a.py"][2] == 2
    assert stats["subsystems"][0][0] == "app"
    assert stats["subsystems"][0][3] == 2  # two files
    assert stats["session_files"]["s0000"] == ["/work/demo/app/a.py", "/work/demo/app/b.py"]
    assert stats["file_subsys"]["/work/demo/app/a.py"] == "app"


def test_commits_window_attribution(tmp_path, monkeypatch):
    now_ms = int(datetime.now().timestamp() * 1000)
    fake = [
        {"sha": "a" * 40, "subject": "fresh commit", "time": now_ms,
         "date": "2026-09-09", "project": "Demo", "files": 2, "add": 10, "del": 3},
        {"sha": "b" * 40, "subject": "ancient commit", "time": now_ms - 30 * 86400000,
         "date": "2026-08-10", "project": "Demo", "files": 1, "add": 1, "del": 1},
    ]
    monkeypatch.setattr(d, "collect_repo_commits", lambda *a, **k: fake)
    path = str(tmp_path / "opencode.db")
    seed_db(path, n_sessions=4, days_back_max=1)  # all sessions ~1 day old, in window
    con = d.connect(path)
    try:
        stats = d.query_stats(con)
    finally:
        con.close()
    by_sha = {r[0]: r for r in stats["commits"]}
    assert by_sha["a" * 40][7] == pytest.approx(4 * 1900)
    assert by_sha["a" * 40][8] == 4
    assert by_sha["b" * 40][7] == 0  # outside the 24h window
    assert ("a" * 40) in stats["commit_sessions"]
    assert ("b" * 40) not in stats["commit_sessions"]


def test_router_commits_time_matched(tmp_path, monkeypatch):
    events = str(tmp_path / "usage-events.jsonl")
    seed_router_events(events)
    monkeypatch.setattr(
        d, "collect_repo_commits", lambda *a, **k: [
            {"sha": "c" * 40, "subject": "codex commit", "time": int(datetime(2026, 9, 5).timestamp() * 1000),
             "date": "2026-09-05", "project": "Demo", "files": 1, "add": 5, "del": 0},
        ],
    )
    stats = d.query_router_stats(events, worktrees=[("Demo", "/work/demo")])
    assert len(stats["commits"]) == 1
    row = stats["commits"][0]
    assert row[0] == "c" * 40
    # 2026-09-04 events (1100+2300+550) fall in the 24h window before 2026-09-05
    assert row[7] == pytest.approx(1100 + 2300 + 550)
    assert row[8] == 3
    assert "hour_tokens" not in stats


def seed_cline_dir(base, day="2026-09-04"):
    """Create a minimal Cline Desktop data dir: sessions.db + one session."""
    base = str(base)
    os.makedirs(os.path.join(base, "sessions", "sess1"), exist_ok=True)
    os.makedirs(os.path.join(base, "db"), exist_ok=True)
    dbp = os.path.join(base, "db", "sessions.db")
    con = sqlite3.connect(dbp)
    con.execute(
        "CREATE TABLE sessions (session_id TEXT PRIMARY KEY, source TEXT, pid INTEGER,"
        " started_at TEXT, ended_at TEXT, exit_code INTEGER, status TEXT, status_lock INTEGER,"
        " interactive INTEGER, provider TEXT, model TEXT, cwd TEXT, workspace_root TEXT,"
        " team_name TEXT, enable_tools INTEGER, enable_spawn INTEGER, enable_teams INTEGER,"
        " parent_session_id TEXT, parent_agent_id TEXT, agent_id TEXT, conversation_id TEXT,"
        " is_subagent INTEGER, prompt TEXT, metadata_json TEXT, transcript_path TEXT,"
        " hook_path TEXT, messages_path TEXT, updated_at TEXT)"
    )
    con.execute(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("sess1", "desktop", 1, day + "T10:00:00.000Z", None, None, "running", 0,
         1, "cline", "cline-free/deepseek-v4.1-flash", "/work/demo", "/work/demo",
         None, 1, 1, 1, None, None, None, None, 0, "Fix bug", "{}", "", "", "", day + "T10:00:00.000Z"),
    )
    con.commit()
    con.close()
    ts = int(datetime(2026, 9, 4, 10, 0).timestamp() * 1000)
    msgs = {"version": 1, "sessionId": "sess1", "messages": [
        {"id": "u1", "role": "user", "content": [], "ts": ts},
        {"id": "a1", "role": "assistant", "content": [], "ts": ts + 1000,
         "modelInfo": {"id": "cline-free/deepseek-v4.1-flash", "provider": "cline"},
         "metrics": {"inputTokens": 1000, "outputTokens": 100,
                     "cacheReadTokens": 200, "cacheWriteTokens": 50}},
        {"id": "a2", "role": "assistant", "content": [], "ts": ts + 2000,
         "modelInfo": {"id": "cline-free/deepseek-v4.1-flash", "provider": "cline"},
         "metrics": {"inputTokens": 2000, "outputTokens": 300,
                     "cacheReadTokens": 500, "cacheWriteTokens": 0}},
        {"id": "a3", "role": "assistant", "content": [], "ts": ts + 3000,
         "modelInfo": {"id": "cline-free/deepseek-v4.1-flash", "provider": "cline"}},
    ]}
    with open(os.path.join(base, "sessions", "sess1", "sess1.messages.json"), "w") as f:
        json.dump(msgs, f)
    return base


def test_cline_totals_and_cache(tmp_path):
    cline = seed_cline_dir(tmp_path / "cline")
    stats = d.query_cline_stats(cline)
    t = stats["totals"]
    assert t["requests"] == 2  # unmetered a3 excluded
    assert t["unmetered"] == 1
    assert t["tokens_input"] == pytest.approx(3000)
    assert t["tokens_output"] == pytest.approx(400)
    assert t["tokens_cache_read"] == pytest.approx(750)  # read+write combined
    assert t["tokens_total"] == pytest.approx(4150)
    assert t["sessions"] == 1
    assert stats["days"][0]["total"] == pytest.approx(4150)


def test_cline_models_providers_sessions(tmp_path):
    cline = seed_cline_dir(tmp_path / "cline")
    stats = d.query_cline_stats(cline)
    assert len(stats["models"]) == 1
    row = stats["models"][0]
    assert row[0] == "cline-free/deepseek-v4.1-flash"
    assert row[1] == 2 and row[2] == pytest.approx(4150)
    assert stats["providers"][0][0] == "cline"
    assert len(stats["sessions"]) == 1
    s = stats["sessions"][0]
    assert s["id"] == "sess1" and s["tokens_total"] == pytest.approx(4150)
    assert s["msgs"] == 2


def test_blank_cline_stats_shape():
    stats = d.blank_cline_stats(error="x")
    assert stats["source"] == "cline"
    assert stats["totals"]["tokens_total"] == 0
    assert stats["activity"] and len(stats["activity"]) == d.ACTIVITY_DAYS
    assert stats["records"]["streaks"] == {"current": 0, "longest": 0}
    assert stats["error"] == "x"


def test_cached_cline_stats_invalidates(tmp_path):
    cline = seed_cline_dir(tmp_path / "cline")
    with d.LOCK:
        d.CLINE_CACHE.clear()
    try:
        s1 = d.cached_cline_stats(cline)
        assert s1["totals"]["requests"] == 2
        # append another metered message -> fingerprint (newest mtime/size) changes
        import time
        time.sleep(0.02)
        p = os.path.join(cline, "sessions", "sess1", "sess1.messages.json")
        data = json.load(open(p))
        ts = int(datetime(2026, 9, 4, 11, 0).timestamp() * 1000)
        data["messages"].append(
            {"id": "a4", "role": "assistant", "content": [], "ts": ts,
             "modelInfo": {"id": "cline-free/deepseek-v4.1-flash", "provider": "cline"},
             "metrics": {"inputTokens": 500, "outputTokens": 50,
                         "cacheReadTokens": 0, "cacheWriteTokens": 0}})
        json.dump(data, open(p, "w"))
        s2 = d.cached_cline_stats(cline)
        assert s2["totals"]["requests"] == 3
    finally:
        with d.LOCK:
            d.CLINE_CACHE.clear()


def test_cline_missing_dir_raises(tmp_path):
    with pytest.raises(RuntimeError):
        d.query_cline_stats(str(tmp_path / "nope"))
