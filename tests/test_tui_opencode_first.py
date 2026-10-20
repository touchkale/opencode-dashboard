"""Contract tests for the OpenCode-first TUI helpers.

Pure-function coverage only: no curses, no database, no network.  Every test
that involves dates passes an explicit ``today`` so the result is stable.
"""

from datetime import date, datetime, timedelta

import pytest

import opencode_dashboard as d

TODAY = date(2026, 10, 20)

ALL_VISIBLE = {"opencode": True, "router": True, "cline": True}
OPENCODE_ONLY = {"opencode": True, "router": False, "cline": False}
CODEX_ONLY = {"opencode": False, "router": True, "cline": False}


def _day(offset, **tokens):
    """A day row `offset` days before TODAY."""
    row = {"date": (TODAY - timedelta(days=offset)).isoformat()}
    row.update(tokens)
    return row


# --------------------------------------------------------------- visibility
def test_initial_visibility_defaults_to_opencode_only():
    assert d.tui_initial_visibility(None) == {
        "opencode": True, "router": False, "cline": False}


def test_initial_visibility_explicit_saved_map_wins():
    saved = {"opencode": True, "router": False, "cline": True}
    assert d.tui_initial_visibility(saved) == saved
    # A JSON string (how the ledger stores it) is honoured as well.
    assert d.tui_initial_visibility(
        '{"opencode": false, "router": true, "cline": false}'
    ) == {"opencode": False, "router": True, "cline": False}


def test_initial_visibility_corrupt_value_normalizes_all_visible():
    corrupt = ["not json at all", "[1, 2, 3]", "{broken", 42, object()]
    for value in corrupt:
        assert d.tui_initial_visibility(value) == ALL_VISIBLE


def test_tab_order_follows_visible_tabs_then_recap():
    assert d.tui_tab_order(d.TUI_DEFAULT_VISIBILITY) == [0, 3]
    assert d.tui_tab_order(ALL_VISIBLE) == [0, 1, 2, 3]
    assert d.tui_tab_order(CODEX_ONLY) == [1, 3]


# -------------------------------------------------------------- tab bar/key
def test_tab_items_all_visible():
    assert d.tui_tab_items(ALL_VISIBLE) == [
        (0, "OpenCode", "OC"),
        (1, "Codex", "CX"),
        (2, "Cline", "CL"),
        (3, "Recap", "RC"),
    ]


def test_tab_items_opencode_only():
    assert d.tui_tab_items(OPENCODE_ONLY) == [
        (0, "OpenCode", "OC"),
        (3, "Recap", "RC"),
    ]


def test_tab_items_codex_only():
    assert d.tui_tab_items(CODEX_ONLY) == [
        (1, "Codex", "CX"),
        (3, "Recap", "RC"),
    ]


def test_tab_for_key_opencode_only_bounds():
    # Keys are curses key codes, exactly as run_tui receives them from getch().
    assert d.tui_tab_for_key(ord("1"), OPENCODE_ONLY) == 0
    assert d.tui_tab_for_key(ord("2"), OPENCODE_ONLY) == 3
    assert d.tui_tab_for_key(ord("3"), OPENCODE_ONLY) is None
    assert d.tui_tab_for_key(ord("0"), OPENCODE_ONLY) is None
    assert d.tui_tab_for_key(ord("9"), OPENCODE_ONLY) is None


def test_tab_for_key_codex_only_bounds():
    assert d.tui_tab_for_key(ord("1"), CODEX_ONLY) == 1
    assert d.tui_tab_for_key(ord("2"), CODEX_ONLY) == 3
    assert d.tui_tab_for_key(ord("3"), CODEX_ONLY) is None
    # Non-digit keys never map to a tab.
    assert d.tui_tab_for_key(ord("x"), ALL_VISIBLE) is None


# ------------------------------------------------------------- token split
def test_token_split_legacy_folded_payload():
    stats = {
        "totals": {
            "tokens_input": 700, "tokens_output": 300,
            "tokens_reasoning": 100, "tokens_cache_read": 60,
            "tokens_cache_write": 40, "tokens_total": 1200, "cost": 1.5,
        },
        # Legacy: no tokens_total_recorded / included_in_totals, so the
        # recovered amount is still inside totals.
        "recovered": {
            "tokens_total": 1000, "tokens_input": 600, "tokens_output": 250,
            "tokens_reasoning": 50, "tokens_cache": 100,
        },
    }
    split = d.tui_token_split(stats)
    assert split["recorded"]["total"] == 200
    assert split["recorded"]["input"] == 100
    assert split["recorded"]["output"] == 50
    assert split["recorded"]["reasoning"] == 50
    assert split["recorded"]["cache"] == 0
    assert split["recorded"]["cost"] == 1.5
    assert split["recovered"]["total"] == 1000
    assert split["has_recovered"] is True


def test_token_split_separate_payload_does_not_double_count():
    stats = {
        "totals": {
            "tokens_input": 5000, "tokens_output": 0, "tokens_reasoning": 0,
            "tokens_cache_read": 0, "tokens_cache_write": 0,
            "tokens_total": 5000, "tokens_total_recorded": 5000, "cost": 2.0,
        },
        "recovered": {
            "tokens_total": 1000, "tokens_input": 1000,
            "included_in_totals": False,
        },
        "lifetime": {"tokens_total": 6000},
    }
    split = d.tui_token_split(stats)
    assert split["recorded"]["total"] == 5000
    assert split["recorded"]["input"] == 5000
    assert split["recovered"]["total"] == 1000
    assert split["has_recovered"] is True

    # New payloads need not carry tokens_total_recorded: included_in_totals
    # false is enough to keep totals recorded.
    stats["totals"].pop("tokens_total_recorded")
    split = d.tui_token_split(stats)
    assert split["recorded"]["total"] == 5000
    assert split["recovered"]["total"] == 1000


def test_token_split_none_and_empty_are_zero():
    for payload in (None, {}, {"totals": None}, {"totals": "bad"}):
        split = d.tui_token_split(payload)
        assert split["has_recovered"] is False
        for bucket in ("recorded", "recovered"):
            assert split[bucket]["total"] == 0
            assert split[bucket]["input"] == 0
            assert split[bucket]["output"] == 0
            assert split[bucket]["reasoning"] == 0
            assert split[bucket]["cache"] == 0


# ---------------------------------------------------------- visible models
def test_visible_models_filters_recovered_other_row():
    models = [
        ["claude-sonnet", 10, 900, 5, 4, 3, 2, 0.5],
        ["Other", 1, 1_000_000, 0, 0, 0, 0, 0.0],
        ["free-model", 2, 10, 0, 0, 0, 0, 0.0],
    ]
    names = [row[0] for row in d.tui_visible_models(models)]
    assert names == ["claude-sonnet", "free-model"]


def test_visible_models_tolerates_none_and_junk():
    assert d.tui_visible_models(None) == []
    assert d.tui_visible_models([]) == []
    junk = [None, "Other", 7, {"name": "Other"}, [" OTHER "], ["ok", 1, 2]]
    assert d.tui_visible_models(junk) == [["ok", 1, 2]]


# ------------------------------------------------------------------ forecast
def test_forecast_flat_week():
    rows = [_day(i, ti=10) for i in range(7)]
    fc = d.tui_forecast(rows, today=TODAY)
    assert fc["ok"] is True
    assert fc["avg"] == pytest.approx(10)
    assert fc["projected"] == pytest.approx(70)
    assert fc["recorded"] == pytest.approx(70)
    assert fc["basis"] == 7
    assert fc["end"] == TODAY.isoformat()


def test_forecast_averages_over_calendar_window_with_gaps():
    # Two 70-token days inside the same 7-day window: 140 / 7 = 20/day.
    rows = [_day(0, ti=70), _day(6, ti=70)]
    fc = d.tui_forecast(rows, today=TODAY)
    assert fc["ok"] is True
    assert fc["avg"] == pytest.approx(20)
    assert fc["projected"] == pytest.approx(140)
    assert fc["recorded"] == pytest.approx(140)
    assert fc["basis"] == 2


def test_forecast_empty_and_none():
    empty = d.tui_forecast([], today=TODAY)
    assert empty["ok"] is False
    assert empty["projected"] == 0
    none = d.tui_forecast(None, today=TODAY)
    assert none["ok"] is False
    assert none["projected"] == 0


# -------------------------------------------------------------- chart header
@pytest.mark.parametrize("width", [20, 32, 60])
def test_chart_header_fits_width(width):
    fc = {"ok": True, "window": 7, "horizon": 7, "avg": 10, "projected": 70}
    header = d.tui_chart_header(1200, 300, 7, fc, width)
    assert len(header) <= width


def test_chart_header_shows_forecast_at_wide_width():
    fc = {"ok": True, "window": 7, "horizon": 7, "avg": 10, "projected": 70}
    header = d.tui_chart_header(1200, 300, 7, fc, 60)
    assert "next 7d" in header
    # A failed forecast falls back to the legacy summary.
    legacy = d.tui_chart_header(1200, 300, 7, {"ok": False}, 60)
    assert "next 7d" not in legacy


# -------------------------------------------------------------- insight rows
def test_insight_rows_structured_cards():
    stats = {
        "insight_cards": [
            {"title": "Biggest day", "value": "12.3K", "detail": "Oct 12"},
            {"title": "", "value": "", "detail": ""},
            "not a card",
            {"title": "Cache", "value": "41%", "detail": ""},
        ]
    }
    rows = d.tui_insight_rows(stats)
    assert len(rows) == 2
    assert rows[0] == {
        "title": "Biggest day", "value": "12.3K", "detail": "Oct 12"}
    assert rows[1] == {"title": "Cache", "value": "41%", "detail": ""}


def test_insight_rows_legacy_strings():
    stats = {"insights": ["first insight", None, "   ", "second insight"]}
    rows = d.tui_insight_rows(stats)
    assert [row["detail"] for row in rows] == ["first insight", "second insight"]
    assert all(row["title"] == "" and row["value"] == "" for row in rows)


def test_insight_rows_put_recovered_metadata_first():
    stats = {
        "recovered": {"tokens_total": 1000, "included_in_totals": False},
        "insight_cards": [{"title": "Biggest day", "value": "12.3K"}],
    }
    rows = d.tui_insight_rows(stats)
    assert rows[0]["title"] == "Recovered history"
    assert rows[0]["value"] == "1.0K"
    assert rows[1]["title"] == "Biggest day"


def test_insight_rows_skip_malformed_and_empty():
    malformed = {"insight_cards": [None, 3, [], {"title": None, "value": None}]}
    assert d.tui_insight_rows(malformed) == []
    assert d.tui_insight_rows({}) == []
    assert d.tui_insight_rows(None) == []


# --------------------------------------------------------------- day totals
def test_period_totals_ignores_recovered_metadata():
    stats = {
        "days": [
            _day(1, ti=10, to=5, tr=1, cache=4, sessions=1, msgs=2, cost=0.1),
            _day(0, ti=20, to=0, tr=0, cache=0, sessions=2, msgs=3, cost=0.2),
        ],
        "recovered": {
            "tokens_total": 1000, "tokens_input": 1000,
            "included_in_totals": False,
        },
    }
    full = d.tui_period_totals(stats, 0)
    assert full["tokens_total"] == 40
    assert full["ti"] == 30
    assert full["sessions"] == 3
    assert full["messages"] == 5
    assert full["cost"] == pytest.approx(0.3)
    assert full["days_used"] == 2
    assert d.tui_period_totals(stats, 14)["tokens_total"] == 40
    yesterday = d.tui_period_totals(stats, 1)
    assert yesterday["tokens_total"] == 20
    assert yesterday["ti"] == 20
