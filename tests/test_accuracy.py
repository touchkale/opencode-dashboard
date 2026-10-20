"""Accuracy contract for the dashboard payloads.

This file is intentionally written TDD-style: it describes the corrected
contract (metadata-only recovered history, reconciled totals, delta-based
ledger merging, read-only cache rate, and the focus-mode shell) before the
implementation exists.  Failures here are expected until the implementation
catches up.

Fixture note
------------
``tests.test_dashboard.seed_db`` writes one synthetic step-finish part per
session with ``total=400`` while the session row rolls up to 1,900 tokens.
Those parts are deliberately incomplete, so a part-derived daily series
cannot reconcile with the session totals for that fixture.  Reconciliation is
therefore tested with ``seed_consistent_db`` below (one part per session whose
token buckets sum to the session rollup).  ``seed_db`` is still used for the
totals/cache identity tests, which do not depend on part totals.  This is the
one documented fixture exception in this module.
"""

import json
import re
import sqlite3
import sys
from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path

import pytest

import opencode_dashboard as d

sys.path.insert(0, str(Path(__file__).parent))
from test_dashboard import seed_db, seed_db_2_0  # noqa: E402

CACHE_READ_PER_SEED_SESSION = 200
CACHE_WRITE_PER_SEED_SESSION = 100


@pytest.fixture(autouse=True)
def _clear_stats_caches():
    """Keep the module-level caches from leaking between accuracy tests."""
    with d.LOCK:
        d.LOCAL_CACHE.clear()
        d.ROUTER_CACHE.clear()
        d.CLINE_CACHE.clear()
    yield
    with d.LOCK:
        d.LOCAL_CACHE.clear()
        d.ROUTER_CACHE.clear()
        d.CLINE_CACHE.clear()


def _local_day(ms):
    return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d")


def _midday(days_ago):
    dt = (datetime.now() - timedelta(days=days_ago)).replace(
        hour=12, minute=0, second=0, microsecond=0
    )
    return int(dt.timestamp() * 1000)


def _query(db_path):
    con = d.connect(db_path)
    try:
        return d.query_stats(con)
    finally:
        con.close()


def seed_consistent_db(path, sessions):
    """Create a small DB whose step-finish parts agree with session rollups.

    Each session gets two messages and exactly one step-finish part whose
    ``total`` is input+output+reasoning+cache_read+cache_write.  That is the
    part-consistent shape needed to check that daily totals reconcile.
    """
    con = sqlite3.connect(path)
    c = con.cursor()
    c.executescript(
        """
        CREATE TABLE session(id TEXT PRIMARY KEY, title TEXT, agent TEXT, model TEXT,
          time_created INTEGER, time_updated INTEGER, tokens_input INTEGER, tokens_output INTEGER,
          tokens_reasoning INTEGER, tokens_cache_read INTEGER, tokens_cache_write INTEGER,
          cost REAL, project_id TEXT);
        CREATE TABLE message(id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
        CREATE TABLE part(id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT,
          time_created INTEGER, data TEXT);
        CREATE TABLE project(id TEXT PRIMARY KEY, name TEXT, worktree TEXT);
        CREATE TABLE session_input(session_id TEXT, prompt TEXT, time_created INTEGER);
        """
    )
    c.execute("INSERT INTO project VALUES ('p1','Demo','/work/demo')")
    for spec in sessions:
        sid = spec["id"]
        start = int(spec["start"])
        end = int(spec.get("end", start + 60_000))
        ti = int(spec["ti"])
        to = int(spec["to"])
        tr = int(spec["tr"])
        cr = int(spec["cr"])
        cw = int(spec["cw"])
        cost = float(spec.get("cost", 0.01))
        model = json.dumps({"id": spec.get("model", "anthropic/claude-sonnet-4-6")})
        c.execute(
            "INSERT INTO session VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (sid, spec.get("title", sid), "build", model, start, end,
             ti, to, tr, cr, cw, cost, "p1"),
        )
        c.execute(
            "INSERT INTO message VALUES (?,?,?,?)",
            (f"{sid}-u", sid, start, json.dumps({"role": "user"})),
        )
        c.execute(
            "INSERT INTO message VALUES (?,?,?,?)",
            (f"{sid}-a", sid, end, json.dumps({"role": "assistant"})),
        )
        part_time = int(spec.get("part_time", start + 1_000))
        c.execute(
            "INSERT INTO part VALUES (?,?,?,?,?)",
            (
                f"{sid}-part",
                f"{sid}-a",
                sid,
                part_time,
                json.dumps(
                    {
                        "type": "step-finish",
                        "tokens": {
                            "input": ti,
                            "output": to,
                            "reasoning": tr,
                            "total": ti + to + tr + cr + cw,
                            "cache": {"read": cr, "write": cw},
                        },
                        "cost": cost,
                    }
                ),
            ),
        )
    con.commit()
    con.close()


# ---------------------------------------------------------------------------
# 1. Recovered tokens are metadata only
# ---------------------------------------------------------------------------


def _recovered_stats():
    return {
        "totals": {
            "sessions": 4,
            "messages": 10,
            "tokens_input": 100,
            "tokens_output": 50,
            "tokens_reasoning": 10,
            "tokens_cache_read": 30,
            "tokens_cache_write": 10,
            "tokens_cache_total": 40,
            "tokens_total": 200,
            "cache_rate": d.cache_rate(100, 30),
            "avg_tokens_per_session": 50,
            "cost": 1.5,
        },
        "models": [["paid/model", 2, 200, 100, 50, 10, 40, 1.5, 100, d.cache_rate(100, 40)]],
        "days": [{"date": "2026-01-01", "ti": 100, "to": 50, "tr": 10, "cache": 40, "total": 200}],
        "activity": [{"date": "2026-01-01", "total": 200}],
        "records": {"streaks": {"current": 1, "longest": 1}},
        "agents": [["build", 2, 200]],
        "projects": [["Demo", 2, 200]],
        "range": "2026-01-01 → 2026-01-01",
    }


def _snapshot(value):
    return json.loads(json.dumps(value))


def test_recovered_adjustment_is_metadata_only_with_ratio_mix():
    stats = _recovered_stats()
    totals_before = _snapshot(stats["totals"])
    models_before = _snapshot(stats["models"])

    out = d.apply_recovered_adjustment(stats, 1000)

    # Recovered history must never be folded into live totals or model rows.
    assert out["totals"] == totals_before
    assert out["models"] == models_before
    assert not any(row[0] == "Other" for row in out["models"])

    rec = out["recovered"]
    for key in (
        "tokens_total",
        "tokens_input",
        "tokens_output",
        "tokens_reasoning",
        "tokens_cache",
    ):
        assert key in rec, f"recovered metadata is missing {key}"

    # Base mix is 100/50/10/30/10 of 200, so 1000 splits exactly 500/250/50/150/50.
    assert rec["tokens_total"] == 1000
    assert rec["tokens_input"] == 500
    assert rec["tokens_output"] == 250
    assert rec["tokens_reasoning"] == 50
    assert rec["tokens_cache"] == 200
    assert (
        rec["tokens_input"]
        + rec["tokens_output"]
        + rec["tokens_reasoning"]
        + rec["tokens_cache"]
    ) == 1000


def test_recovered_adjustment_rounding_keeps_exact_sum_and_ratios():
    stats = _recovered_stats()
    stats["totals"].update(
        {
            "tokens_input": 3,
            "tokens_output": 2,
            "tokens_reasoning": 1,
            "tokens_cache_read": 1,
            "tokens_cache_write": 1,
            "tokens_cache_total": 2,
            "tokens_total": 8,
        }
    )

    out = d.apply_recovered_adjustment(stats, 101)
    rec = out["recovered"]
    parts_sum = (
        rec["tokens_input"]
        + rec["tokens_output"]
        + rec["tokens_reasoning"]
        + rec["tokens_cache"]
    )

    assert rec["tokens_total"] == 101
    assert parts_sum == 101
    # Rounding may move a token, but every bucket must stay on the recorded mix.
    for key, base in (
        ("tokens_input", 3),
        ("tokens_output", 2),
        ("tokens_reasoning", 1),
        ("tokens_cache", 2),
    ):
        assert rec[key] == pytest.approx(101 * base / 8, abs=1)


def test_recovered_adjustment_nonpositive_amount_is_noop():
    for amount in (0, -5):
        stats = _recovered_stats()
        totals_before = _snapshot(stats["totals"])
        models_before = _snapshot(stats["models"])

        out = d.apply_recovered_adjustment(stats, amount)

        assert out["totals"] == totals_before
        assert out["models"] == models_before
        rec = out.get("recovered")
        assert rec is None or rec.get("tokens_total", 0) == 0


def test_cached_local_stats_keeps_recovered_out_of_live_totals(tmp_path, monkeypatch):
    db_path = str(tmp_path / "opencode.db")
    seed_db(db_path, n_sessions=4)
    live = _query(db_path)

    monkeypatch.setattr(d, "RECOVERED_TOKENS", 123_456)
    with d.LOCK:
        d.LOCAL_CACHE.clear()

    stats = d.cached_local_stats(db_path)

    assert stats["recovered"]["tokens_total"] == 123_456
    for key in (
        "tokens_total",
        "tokens_input",
        "tokens_output",
        "tokens_reasoning",
        "tokens_cache_read",
        "tokens_cache_write",
    ):
        assert stats["totals"][key] == live["totals"][key]
    assert not any(row[0] == "Other" for row in stats["models"])


# ---------------------------------------------------------------------------
# 2. Totals reconcile
# ---------------------------------------------------------------------------


def test_query_stats_totals_identity_and_tokens_cache_total(tmp_path):
    db_path = str(tmp_path / "opencode.db")
    seed_db(db_path, n_sessions=12)

    stats = _query(db_path)
    totals = stats["totals"]

    assert totals["tokens_total"] == (
        totals["tokens_input"]
        + totals["tokens_output"]
        + totals["tokens_reasoning"]
        + totals["tokens_cache_read"]
        + totals["tokens_cache_write"]
    )
    assert "tokens_cache_total" in totals
    assert totals["tokens_cache_total"] == (
        totals["tokens_cache_read"] + totals["tokens_cache_write"]
    )
    assert totals["tokens_cache_total"] == 12 * (
        CACHE_READ_PER_SEED_SESSION + CACHE_WRITE_PER_SEED_SESSION
    )
    # Under the 500-row cap the visible rows are the complete set.
    assert totals["tokens_cache_total"] == sum(
        row["tokens_cache"] for row in stats["sessions"]
    )


def test_query_stats_2_0_totals_identity_and_tokens_cache_total(tmp_path):
    db_path = str(tmp_path / "opencode.db")
    seed_db_2_0(db_path, n_sessions=5)

    stats = _query(db_path)
    totals = stats["totals"]

    assert totals["tokens_total"] == (
        totals["tokens_input"]
        + totals["tokens_output"]
        + totals["tokens_reasoning"]
        + totals["tokens_cache_read"]
        + totals["tokens_cache_write"]
    )
    assert totals["tokens_cache_total"] == (
        totals["tokens_cache_read"] + totals["tokens_cache_write"]
    )


def test_query_stats_day_totals_reconcile_with_totals(tmp_path):
    """Days must sum to totals on a part-consistent seeded DB.

    See the module docstring: plain ``seed_db`` parts are intentionally
    incomplete (400 tokens per part vs 1,900 per session), so this uses the
    consistent seeder.  The fixture spans multiple local dates, including a
    session that starts before midnight and finishes after it.
    """
    db_path = str(tmp_path / "opencode.db")
    rollover_start = int(
        (datetime.now() - timedelta(days=1))
        .replace(hour=23, minute=50, second=0, microsecond=0)
        .timestamp()
        * 1000
    )
    seed_consistent_db(
        db_path,
        [
            {"id": "live-a", "start": _midday(4),
             "ti": 1000, "to": 500, "tr": 100, "cr": 200, "cw": 100},
            {"id": "live-b", "start": _midday(3),
             "ti": 500, "to": 250, "tr": 50, "cr": 100, "cw": 50},
            {"id": "rollover", "start": rollover_start,
             "end": rollover_start + 30 * 60 * 1000,
             "part_time": rollover_start + 20 * 60 * 1000,
             "ti": 400, "to": 200, "tr": 40, "cr": 80, "cw": 40},
        ],
    )

    stats = _query(db_path)
    totals = stats["totals"]

    assert len({entry["date"] for entry in stats["days"]}) >= 2
    assert sum(entry["total"] for entry in stats["days"]) == pytest.approx(
        totals["tokens_total"], abs=1
    )


# ---------------------------------------------------------------------------
# 4. Cache math consistency
# ---------------------------------------------------------------------------


def test_cache_total_covers_all_sessions_not_only_capped_rows(tmp_path):
    db_path = str(tmp_path / "opencode.db")
    n_sessions = d.MAX_SESSION_ROWS + 25
    seed_db(db_path, n_sessions=n_sessions)

    stats = _query(db_path)
    totals = stats["totals"]

    assert len(stats["sessions"]) == d.MAX_SESSION_ROWS
    assert totals["tokens_cache_total"] == n_sessions * (
        CACHE_READ_PER_SEED_SESSION + CACHE_WRITE_PER_SEED_SESSION
    )
    assert totals["tokens_cache_total"] == (
        totals["tokens_cache_read"] + totals["tokens_cache_write"]
    )
    assert totals["tokens_cache_total"] > sum(
        row["tokens_cache"] for row in stats["sessions"]
    )


def test_cache_rate_uses_cache_read_only_not_read_plus_write(tmp_path):
    db_path = str(tmp_path / "opencode.db")
    seed_db(db_path, n_sessions=8)

    totals = _query(db_path)["totals"]
    read_only = d.cache_rate(totals["tokens_input"], totals["tokens_cache_read"])
    read_plus_write = d.cache_rate(
        totals["tokens_input"],
        totals["tokens_cache_read"] + totals["tokens_cache_write"],
    )

    assert totals["cache_rate"] == read_only
    assert totals["cache_rate"] != read_plus_write


# ---------------------------------------------------------------------------
# 3. Ledger daily merge
# ---------------------------------------------------------------------------


def _build_ledger_case(tmp_path):
    """Seed two live dates + a deleted session, then run the ledger merge."""
    db_path = str(tmp_path / "opencode.db")
    live_day_ms = _midday(4)
    deleted_day_ms = _midday(2)
    # Starts at 23:50, finishes at 00:10 the next local day.  Ledger
    # attribution uses the start date; live day tokens use part activity.
    rollover_start = int(
        (datetime.now() - timedelta(days=1))
        .replace(hour=23, minute=50, second=0, microsecond=0)
        .timestamp()
        * 1000
    )
    rollover_part = rollover_start + 20 * 60 * 1000

    deleted_tokens = 300 + 150 + 30 + 60 + 30  # 570
    seed_consistent_db(
        db_path,
        [
            {"id": "live-remaining", "start": live_day_ms,
             "ti": 1000, "to": 500, "tr": 100, "cr": 200, "cw": 100},
            {"id": "deleted-session", "start": deleted_day_ms,
             "ti": 300, "to": 150, "tr": 30, "cr": 60, "cw": 30},
            {"id": "live-rollover", "start": rollover_start,
             "end": rollover_start + 30 * 60 * 1000,
             "part_time": rollover_part,
             "ti": 400, "to": 200, "tr": 40, "cr": 80, "cw": 40},
        ],
    )

    con = d.connect(db_path)
    try:
        pre = d.query_stats(con)
        d.sync_session_ledger(db_path, con)
    finally:
        con.close()

    wcon = sqlite3.connect(db_path)
    try:
        wcon.execute("DELETE FROM session WHERE id = ?", ("deleted-session",))
        wcon.execute("DELETE FROM message WHERE session_id = ?", ("deleted-session",))
        wcon.execute("DELETE FROM part WHERE session_id = ?", ("deleted-session",))
        wcon.commit()
    finally:
        wcon.close()

    live = _query(db_path)
    post = d.merge_ledger_into_stats(deepcopy(live), db_path)

    return {
        "pre": pre,
        "live": live,
        "post": post,
        "deleted_tokens": deleted_tokens,
        "live_day": _local_day(live_day_ms),
        "deleted_day": _local_day(deleted_day_ms),
        "rollover_day": _local_day(rollover_part),
    }


@pytest.fixture
def ledger_case(tmp_path):
    return _build_ledger_case(tmp_path)


def _day_totals(stats):
    return {entry["date"]: entry["total"] for entry in stats["days"]}


def test_ledger_merge_preserves_live_day_totals(ledger_case):
    pre_days = _day_totals(ledger_case["pre"])
    post_days = _day_totals(ledger_case["post"])

    live_day = ledger_case["live_day"]
    assert pre_days.get(live_day, 0) > 0
    assert post_days.get(live_day, 0) == pytest.approx(pre_days[live_day], abs=1)

    # Every live day must survive the merge unchanged.  In particular the
    # rollover day must not be re-attributed to the session-start day.
    for date, total in pre_days.items():
        assert post_days.get(date, 0) == pytest.approx(total, abs=1), (
            f"ledger merge changed the total for {date}"
        )


def test_ledger_merge_adds_deleted_session_tokens_once(ledger_case):
    pre_total = ledger_case["pre"]["totals"]["tokens_total"]
    live_total = ledger_case["live"]["totals"]["tokens_total"]
    post_total = ledger_case["post"]["totals"]["tokens_total"]
    deleted_tokens = ledger_case["deleted_tokens"]

    # Deletion removed the tokens from the live query; the merge restores them.
    assert pre_total - live_total == pytest.approx(deleted_tokens, abs=1)
    assert post_total == pytest.approx(pre_total, abs=1)
    # Explicitly guard against the delta being applied twice.
    assert post_total != pytest.approx(live_total + 2 * deleted_tokens, abs=1)


def test_ledger_merge_keeps_days_reconciled_with_totals(ledger_case):
    post = ledger_case["post"]
    totals = post["totals"]

    assert sum(entry["total"] for entry in post["days"]) == pytest.approx(
        totals["tokens_total"], abs=1
    )
    assert totals["tokens_total"] == (
        totals["tokens_input"]
        + totals["tokens_output"]
        + totals["tokens_reasoning"]
        + totals["tokens_cache_read"]
        + totals["tokens_cache_write"]
    )


# ---------------------------------------------------------------------------
# 5. Focus-mode / static shell contract
# ---------------------------------------------------------------------------


def test_page_shell_exposes_focus_mode_defaults():
    page = d.PAGE

    assert 'id="set-focus"' in page
    assert "ocd-focus" in page
    assert re.search(r"\bFOCUS\s*=\s*true\b", page), (
        "PAGE must default FOCUS to true"
    )


def test_harness_default_is_opencode_only():
    match = re.search(r"\bHARNESS\s*=\s*\{([^}]*)\}", d.PAGE)
    assert match, "PAGE must define a HARNESS default object"

    defaults = {
        key: value == "true"
        for key, value in re.findall(r"(\w+)\s*:\s*(true|false)", match.group(1))
    }
    assert defaults.get("opencode") is True
    assert defaults.get("router") is False
    assert defaults.get("cline") is False


class _IdCollector(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.ids = []

    def handle_starttag(self, tag, attrs):
        for name, value in attrs:
            if name == "id" and value is not None:
                self.ids.append(value)


def test_page_has_no_duplicate_element_ids():
    parser = _IdCollector()
    parser.feed(d.PAGE)
    parser.close()

    duplicates = sorted(
        element_id for element_id, count in Counter(parser.ids).items() if count > 1
    )
    assert parser.ids, "PAGE must contain parseable id attributes"
    assert not duplicates, f"duplicate id attributes in PAGE: {duplicates}"
