#!/usr/bin/env python3
"""Headless browser smoke test for the OpenCode-first dashboard.

This script assumes the dashboard is already running, for example:

    python3 opencode_dashboard.py --port 8811 --quiet
    /usr/bin/python3 tests/ui_smoke.py --port 8811

It never starts or stops the server. The app sends an ``/api/close`` beacon
when a dashboard window unloads; that request is fulfilled by the test browser
so closing the test pages cannot shut down the server.

Exit codes: 0 = all checks passed, 1 = at least one failure.
"""

from __future__ import annotations

import argparse
import re
import socket
import sys
import traceback
from pathlib import Path
from urllib.parse import urlparse

try:
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright

    PLAYWRIGHT_IMPORT_ERROR = None
except ImportError as exc:  # pragma: no cover - depends on the interpreter
    PlaywrightError = Exception
    PlaywrightTimeoutError = Exception
    sync_playwright = None
    PLAYWRIGHT_IMPORT_ERROR = exc


ROOT = Path(__file__).resolve().parents[1]
SHOT_DIR = ROOT / ".playwright-mcp" / "ui-smoke"

VIEWS = [
    ("overview", "Overview"),
    ("models", "Models"),
    ("code", "Code"),
    ("sessions", "Sessions"),
    ("activity", "Activity"),
]
LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}
RAIL_SELECTOR = "#rail, .rail"
RAIL_ITEM_SELECTOR = "button,a,[role='tab'],[role='link']"
MAX_PAGE_HEIGHT = 3000


class Results:
    def __init__(self):
        self.passed = 0
        self.failures = []
        self.skips = []
        self.console_errors = []
        self.page_errors = []
        self.bad_responses = []
        self.external_requests = []

    def ok(self, message):
        self.passed += 1
        print("  PASS  " + message)

    def fail(self, message):
        self.failures.append(message)
        print("  FAIL  " + message)

    def skip(self, message):
        self.skips.append(message)
        print("  SKIP  " + message)

    def check(self, condition, message):
        if condition:
            self.ok(message)
        else:
            self.fail(message)
        return bool(condition)


def section(title):
    print("\n" + title)
    print("-" * len(title))


def dedupe(items):
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def short_error(exc, limit=400):
    text = " ".join(str(exc).split())
    if len(text) > limit:
        text = text[:limit] + "..."
    return text


def normalize(value):
    return re.sub(r"\s+", " ", (value or "").strip()).lower()


def tokenize(value):
    return " ".join(re.findall(r"[a-z0-9]+", (value or "").lower()))


def port_open(host, port):
    try:
        with socket.create_connection((host, port), timeout=2.0):
            return True
    except OSError:
        return False


def is_local_url(url):
    try:
        return urlparse(url).hostname in LOCAL_HOSTS
    except ValueError:
        return False


def install_routes(context, results):
    """Keep all traffic local and stop the app's close beacon reaching the server."""

    def handle(route):
        parsed = urlparse(route.request.url)
        scheme = (parsed.scheme or "").lower()
        try:
            if scheme in ("data", "blob", "about", "chrome-extension"):
                route.continue_()
                return
            if parsed.hostname in LOCAL_HOSTS:
                if parsed.path == "/api/close":
                    route.fulfill(status=200, content_type="text/plain", body="bye")
                else:
                    route.continue_()
                return
            results.external_requests.append(route.request.url)
            route.abort()
        except PlaywrightError:
            # A page can disappear while its close beacon is in flight.
            pass

    context.route("**/*", handle)


def attach_page(page, results):
    def on_console(message):
        if message.type != "error":
            return
        location = message.location or {}
        where = location.get("url") or ""
        line = location.get("lineNumber")
        if where and line is not None:
            text = "{} ({}:{})".format(message.text, where, line)
        elif where:
            text = "{} ({})".format(message.text, where)
        else:
            text = message.text
        results.console_errors.append(text)

    def on_page_error(error):
        results.page_errors.append(str(error))

    def on_response(response):
        try:
            status = response.status
            url = response.url
            method = response.request.method
        except PlaywrightError:
            return
        if status >= 400 and is_local_url(url):
            results.bad_responses.append("{} {} {}".format(status, method, url))

    page.on("console", on_console)
    page.on("pageerror", on_page_error)
    page.on("response", on_response)


def wait_rail(page, timeout=15000):
    rail = page.locator(RAIL_SELECTOR).first
    rail.wait_for(state="visible", timeout=timeout)
    return rail


def goto_ready(page, url, results, label):
    try:
        page.goto(url, wait_until="networkidle", timeout=30000)
        wait_rail(page)
        return True
    except PlaywrightError as exc:
        results.fail("{}: {}".format(label, short_error(exc)))
        return False


def active_state(page):
    return page.evaluate(
        """() => {
            const nodes = Array.from(document.querySelectorAll('.subview.active'));
            return {
                count: nodes.length,
                id: nodes.length ? (nodes[0].getAttribute('data-sub') || '') : ''
            };
        }"""
    )


def wait_active(page, view_id, timeout=8000):
    try:
        page.wait_for_function(
            """(id) => {
                const nodes = Array.from(document.querySelectorAll('.subview.active'));
                return nodes.length === 1
                    && nodes[0].getAttribute('data-sub') === id;
            }""",
            arg=view_id,
            timeout=timeout,
        )
    except PlaywrightTimeoutError:
        pass
    return active_state(page)


def crumb_view_text(page):
    locator = page.locator("#crumb-view")
    if locator.count():
        try:
            return locator.first.inner_text().strip()
        except PlaywrightError:
            pass
    locator = page.locator(".crumbs")
    if locator.count():
        try:
            text = locator.first.inner_text().strip()
            if "/" in text:
                return text.split("/")[-1].strip()
            return text
        except PlaywrightError:
            pass
    return ""


def assert_chrome(page, view_id, label, results, context=""):
    prefix = "{}: ".format(context) if context else ""
    crumb = crumb_view_text(page)
    crumb_ok = (
        normalize(crumb) == normalize(label)
        or normalize(crumb).startswith(normalize(label) + " ")
    )
    results.check(
        crumb_ok,
        "{}breadcrumb is {!r} (got {!r})".format(prefix, label, crumb),
    )

    title = page.title()
    if view_id == "code":
        # "Code" is a substring of "OpenCode"; require it to be a title segment.
        title_ok = (
            re.search(r"(?:^|[\s·|/\-–—])Code(?:$|[\s·|/\-–—])", title, re.I)
            is not None
        )
    else:
        title_ok = normalize(label) in normalize(title)
    results.check(
        title_ok,
        "{}document.title mentions {!r} (got {!r})".format(prefix, label, title),
    )


def find_view_entries(page):
    return page.evaluate(
        r"""() => {
            const rail = document.querySelector('#rail, .rail');
            if (!rail) return [];
            const nodes = Array.from(
                rail.querySelectorAll("button,a,[role='tab'],[role='link']")
            );
            return nodes.map((el, index) => {
                let raw = el.getAttribute('data-view')
                    || el.getAttribute('data-route')
                    || '';
                if (!raw) {
                    const href = el.getAttribute('href') || '';
                    if (href.indexOf('#') >= 0) raw = href.slice(href.indexOf('#') + 1);
                }
                raw = raw.split('#').pop();
                raw = raw.split('?')[0];
                const parts = raw.split('/').filter(Boolean);
                const id = parts.length
                    ? parts[parts.length - 1].toLowerCase()
                    : '';
                const isBrand = el.matches('.rail-brand, #rail-brand, .brand, [data-brand]')
                    || !!el.closest('.rail-brand, #rail-brand');
                const isFooter = !!el.closest('.rail-foot');
                return {
                    index: index,
                    id: id,
                    text: (el.innerText || el.textContent || '')
                        .replace(/\s+/g, ' ').trim(),
                    aria: (el.getAttribute('aria-label') || '').trim(),
                    visible: el.getClientRects().length > 0,
                    eligible: !isBrand && !isFooter
                };
            });
        }"""
    )


def match_view(entries, view_id, label):
    label_tokens = tokenize(label)
    eligible = [entry for entry in entries if entry.get("eligible", True)]
    visible = [entry for entry in eligible if entry["visible"]]

    for pool in (visible, eligible):
        for entry in pool:
            if entry["id"] == view_id:
                return entry
        for entry in pool:
            text = tokenize(entry["text"])
            if text == label_tokens or text.startswith(label_tokens + " "):
                return entry
        for entry in pool:
            aria = tokenize(entry["aria"])
            if aria == label_tokens or aria.endswith(" " + label_tokens):
                return entry
    return None


def click_entry(page, entry, label, results):
    rail = page.locator(RAIL_SELECTOR).first
    item = rail.locator(RAIL_ITEM_SELECTOR).nth(entry["index"])
    try:
        item.click(timeout=5000)
        page.wait_for_timeout(80)
        return True
    except PlaywrightError as exc:
        results.fail(
            "could not click rail view {!r}: {}".format(label, short_error(exc))
        )
        return False


def ensure_opencode(page):
    """Select the OpenCode harness when a multi-harness rail is still present."""
    for selector in ("#rail-tab-opencode", "[data-harness='opencode']"):
        buttons = page.locator(selector)
        count = buttons.count()
        for index in range(min(count, 4)):
            button = buttons.nth(index)
            try:
                if not button.is_visible():
                    continue
                active = button.evaluate(
                    """el => el.getAttribute('aria-current') === 'page'
                        || el.classList.contains('active')"""
                )
                if not active:
                    button.click(timeout=5000)
                    page.wait_for_timeout(120)
                return
            except PlaywrightError:
                continue


def detect_recap(page):
    return bool(
        page.evaluate(
            """() => !!document.querySelector(
                '#view-recap, #rail-tab-recap, [data-harness="recap"], '
                + '.subview[data-sub="recap"]'
            )"""
        )
    )


def check_views(page, results):
    section("[1/6] Rail views and screenshots")
    ensure_opencode(page)
    page.wait_for_timeout(120)

    entries = find_view_entries(page)
    if not entries:
        results.fail("no interactive items found inside #rail/.rail")

    matched = []
    for view_id, label in VIEWS:
        entry = match_view(entries, view_id, label)
        if entry is None:
            results.fail(
                "rail view button missing: {!r} (data-view={!r})".format(
                    label, view_id
                )
            )
        else:
            matched.append((view_id, label, entry))

    results.check(
        len(matched) == len(VIEWS),
        "rail exposes all five OpenCode view buttons ({}/5)".format(len(matched)),
    )

    for view_id, label, entry in matched:
        if not click_entry(page, entry, label, results):
            continue

        state = wait_active(page, view_id)
        results.check(
            state["count"] == 1,
            "{}: exactly one .subview.active (got {})".format(
                label, state["count"]
            ),
        )
        results.check(
            state["id"] == view_id,
            "{}: active subview is {!r} (got {!r})".format(
                label, view_id, state["id"]
            ),
        )
        assert_chrome(page, view_id, label, results)

        shot = SHOT_DIR / "{}.png".format(view_id)
        try:
            page.wait_for_timeout(100)
            page.screenshot(path=str(shot), full_page=True, timeout=20000)
            try:
                shown = shot.relative_to(ROOT)
            except ValueError:
                shown = shot
            print("  SHOT  {}".format(shown))
        except PlaywrightError as exc:
            results.fail(
                "{}: full-page screenshot failed: {}".format(
                    label, short_error(exc)
                )
            )


def check_hash_routes(context, base, results, recap_present):
    section("[2/6] Hash deep links, recap and history")

    for view_id, label in (("sessions", "Sessions"), ("models", "Models")):
        page = context.new_page()
        attach_page(page, results)
        page.set_default_timeout(8000)
        url = "{}/#/opencode/{}".format(base, view_id)
        try:
            if not goto_ready(page, url, results, "deep link {}".format(url)):
                continue
            state = wait_active(page, view_id)
            results.check(
                state["count"] == 1,
                "deep link {}: exactly one active subview (got {})".format(
                    url, state["count"]
                ),
            )
            results.check(
                state["id"] == view_id,
                "deep link {}: active view is {!r} (got {!r})".format(
                    url, view_id, state["id"]
                ),
            )
            assert_chrome(page, view_id, label, results, context="deep link")
            current_hash = page.evaluate("() => location.hash")
            results.check(
                current_hash.startswith("#/opencode/{}".format(view_id)),
                "deep link {}: hash is preserved (got {!r})".format(
                    url, current_hash
                ),
            )
        finally:
            try:
                page.close()
            except PlaywrightError:
                pass

    if recap_present:
        page = context.new_page()
        attach_page(page, results)
        page.set_default_timeout(8000)
        url = base + "/#/recap"
        try:
            if goto_ready(page, url, results, "deep link {}".format(url)):
                state = wait_active(page, "recap")
                results.check(
                    state["id"] == "recap",
                    "deep link #/recap: Recap view is active (got {!r})".format(
                        state["id"]
                    ),
                )
                crumb = crumb_view_text(page)
                harness = page.locator("#crumb-harness")
                harness_text = (
                    harness.first.inner_text().strip() if harness.count() else ""
                )
                results.check(
                    "recap" in normalize(crumb + " " + harness_text),
                    "deep link #/recap: breadcrumb mentions Recap",
                )
                results.check(
                    "recap" in normalize(page.title()),
                    "deep link #/recap: document.title mentions Recap",
                )
                current_hash = page.evaluate("() => location.hash")
                results.check(
                    current_hash.startswith("#/recap"),
                    "deep link #/recap: hash is preserved (got {!r})".format(
                        current_hash
                    ),
                )
        finally:
            try:
                page.close()
            except PlaywrightError:
                pass
    else:
        results.skip("#/recap route is not present in this build")

    page = context.new_page()
    attach_page(page, results)
    page.set_default_timeout(8000)
    try:
        reset = "{}/#/opencode/overview".format(base)
        if not goto_ready(page, reset, results, "history reset"):
            return
        wait_active(page, "overview")

        for view_id in ("sessions", "models"):
            page.evaluate(
                "(hash) => { location.hash = hash; }",
                "#/opencode/{}".format(view_id),
            )
            state = wait_active(page, view_id)
            results.check(
                state["id"] == view_id,
                "history: hash navigation activates {!r} (got {!r})".format(
                    view_id, state["id"]
                ),
            )

        page.go_back()
        state = wait_active(page, "sessions")
        results.check(
            state["id"] == "sessions",
            "history: Back restores the Sessions view (got {!r})".format(
                state["id"]
            ),
        )

        page.go_forward()
        state = wait_active(page, "models")
        results.check(
            state["id"] == "models",
            "history: Forward restores the Models view (got {!r})".format(
                state["id"]
            ),
        )
    except PlaywrightError as exc:
        results.fail("history: back/forward failed: " + short_error(exc))
    finally:
        try:
            page.close()
        except PlaywrightError:
            pass


def palette_active_item(page):
    item = page.locator("#palette .pal-item.active").first
    if not item.count():
        return "", ""
    label_locator = item.locator(".pal-label")
    label = (
        label_locator.first.inner_text().strip()
        if label_locator.count()
        else item.inner_text().strip()
    )
    hint_locator = item.locator(".pal-hint")
    hint = hint_locator.first.inner_text().strip() if hint_locator.count() else ""
    return label, hint


def check_palette(page, base, results):
    section("[3/6] Command palette")

    reset = "{}/#/opencode/overview".format(base)
    if not goto_ready(page, reset, results, "palette reset"):
        return
    wait_active(page, "overview")
    ensure_opencode(page)

    combo = "Meta+k" if sys.platform == "darwin" else "Control+k"

    try:
        page.keyboard.press(combo)
        page.locator("#palette").wait_for(state="visible", timeout=5000)
        results.ok("{} opens #palette".format(combo))
    except PlaywrightError as exc:
        results.fail("{} did not open #palette: {}".format(combo, short_error(exc)))
        return

    try:
        page.locator("#palette-input").fill("sessions")
        page.wait_for_function(
            "() => document.querySelectorAll('#palette .pal-item').length > 0",
            timeout=5000,
        )
    except PlaywrightError as exc:
        results.fail("palette: could not type a query: " + short_error(exc))
        page.keyboard.press("Escape")
        return

    multi_harness = page.evaluate(
        """() => Array.from(document.querySelectorAll('#palette .pal-item'))
            .some((item) => {
                const label = item.querySelector('.pal-label');
                const hint = item.querySelector('.pal-hint');
                const text = ((label && label.textContent) || '').toLowerCase();
                const note = ((hint && hint.textContent) || '').toLowerCase();
                return note.indexOf('view') >= 0
                    && (text.indexOf('codex') >= 0 || text.indexOf('cline') >= 0);
            })"""
    )

    # In the current multi-harness build, "sessions" also matches Codex and
    # Cline. Walk to the OpenCode result before pressing Enter. Focus mode needs
    # no arrows because only OpenCode results exist.
    selected_label = ""
    selected = False
    for _ in range(15):
        label, hint = palette_active_item(page)
        selected_label = label
        tokens = tokenize(label)
        is_view = "view" in hint.lower() or tokens == "sessions" or not hint
        if (
            "sessions" in tokens
            and is_view
            and (not multi_harness or "opencode" in tokens)
        ):
            selected = True
            break
        page.keyboard.press("ArrowDown")
        page.wait_for_timeout(40)

    if not selected:
        results.fail(
            "palette: no Sessions view result found (last active: {!r})".format(
                selected_label
            )
        )
        page.keyboard.press("Escape")
        return

    results.ok("palette: active result is {!r}".format(selected_label))
    page.keyboard.press("Enter")

    try:
        page.locator("#palette").wait_for(state="hidden", timeout=5000)
        results.ok("palette: Enter closes #palette")
    except PlaywrightTimeoutError:
        results.fail("palette: Enter did not close #palette")

    state = wait_active(page, "sessions")
    results.check(
        state["id"] == "sessions",
        "palette: Enter activates the Sessions view (got {!r})".format(
            state["id"]
        ),
    )
    current_hash = page.evaluate("() => location.hash")
    results.check(
        current_hash.startswith("#/opencode/sessions"),
        "palette: Sessions hash route is active (got {!r})".format(current_hash),
    )

    try:
        page.keyboard.press(combo)
        page.locator("#palette").wait_for(state="visible", timeout=5000)
        results.ok("palette: {} reopens it".format(combo))
        page.keyboard.press("Escape")
        page.locator("#palette").wait_for(state="hidden", timeout=5000)
        results.check(
            not page.locator("#palette").is_visible(),
            "palette: Esc closes #palette",
        )
    except PlaywrightError as exc:
        results.fail("palette: Escape check failed: " + short_error(exc))
        try:
            page.keyboard.press("Escape")
        except PlaywrightError:
            pass


def find_focus_toggle(page):
    locator = page.locator("#set-focus")
    return locator.first if locator.count() else None


def open_settings_menu(page):
    """Open the harness settings menu and return a visible #set-focus toggle."""
    toggle = find_focus_toggle(page)
    if toggle is not None and toggle.is_visible():
        return toggle
    click_settings_trigger(page)
    toggle = find_focus_toggle(page)
    if toggle is not None and toggle.is_visible():
        return toggle
    return None


def click_settings_trigger(page):
    selectors = (
        "#settings",
        "[data-settings]",
        "a[href='#settings']",
        "button[aria-label*='settings' i]",
        "button[title*='settings' i]",
    )
    for selector in selectors:
        button = page.locator(selector).first
        if not button.count():
            continue
        try:
            if not button.is_visible():
                continue
            button.click(timeout=3000)
            page.wait_for_timeout(300)
        except PlaywrightError:
            continue
        toggle = find_focus_toggle(page)
        if toggle is not None and toggle.is_visible():
            return True
    return False


def check_settings(context, base, results):
    section("[4/6] Settings and focus mode")
    page = context.new_page()
    attach_page(page, results)
    page.set_default_timeout(8000)
    try:
        if not goto_ready(page, base + "/#settings", results, "settings route #settings"):
            return

        toggle = find_focus_toggle(page)
        if toggle is None:
            click_settings_trigger(page)
            toggle = find_focus_toggle(page)

        if toggle is None:
            # Some builds expose settings at #/settings rather than #settings.
            if goto_ready(
                page, base + "/#/settings", results, "settings route #/settings"
            ):
                toggle = find_focus_toggle(page)
                if toggle is None:
                    click_settings_trigger(page)
                    toggle = find_focus_toggle(page)

        if toggle is None:
            results.skip(
                "#set-focus does not exist yet (settings/focus mode not implemented)"
            )
            return

        checked = toggle.evaluate(
            """el => {
                if (typeof el.checked === 'boolean') return el.checked;
                const aria = el.getAttribute('aria-checked')
                    || el.getAttribute('aria-pressed');
                return aria === 'true';
            }"""
        )
        results.check(
            checked is True,
            "#set-focus exists and is checked by default",
        )

        hidden_rail = (
            ("#rail-tab-router", "Codex"),
            ("#rail-tab-cline", "Cline"),
            ("#rail-tab-recap", "Recap"),
        )

        # Focus mode is on by default: hidden harnesses leave the rail.
        if not goto_ready(
            page,
            base + "/#/opencode/overview",
            results,
            "focus mode overview",
        ):
            return
        ensure_opencode(page)
        for selector, label in hidden_rail:
            results.check(
                not page.locator(selector).first.is_visible(),
                "focus on: {} hidden from the rail".format(label),
            )

        # Turning focus off reveals every harness immediately.
        toggle = open_settings_menu(page)
        if toggle is None:
            results.fail("focus off: could not open the settings menu")
            return
        toggle.uncheck()
        page.wait_for_timeout(400)
        for selector, label in hidden_rail:
            results.check(
                page.locator(selector).first.is_visible(),
                "focus off: {} revealed in the rail".format(label),
            )

        # Turning focus back on hides them again.
        toggle = open_settings_menu(page)
        if toggle is None:
            results.fail("focus on again: could not open the settings menu")
            return
        toggle.check()
        page.wait_for_timeout(500)
        for selector, label in hidden_rail:
            results.check(
                not page.locator(selector).first.is_visible(),
                "focus on again: {} hidden from the rail".format(label),
            )
    finally:
        try:
            page.close()
        except PlaywrightError:
            pass


def check_metrics(page, base, results):
    section("[5/6] Desktop layout metrics")

    if not goto_ready(
        page, "{}/#/opencode/overview".format(base), results, "metrics reset"
    ):
        return
    ensure_opencode(page)

    for width, height, check_height in (
        (1440, 1000, True),
        (1280, 900, False),
    ):
        page.set_viewport_size({"width": width, "height": height})
        page.wait_for_timeout(150)
        ensure_opencode(page)
        entries = find_view_entries(page)

        for view_id, label in VIEWS:
            entry = match_view(entries, view_id, label)
            if entry is None:
                # Already reported by the rail-view check.
                continue
            if not click_entry(page, entry, label, results):
                continue
            state = wait_active(page, view_id)
            if state["id"] != view_id:
                results.fail(
                    "{}px {}: could not activate the view (got {!r})".format(
                        width, label, state["id"]
                    )
                )
                continue

            page.wait_for_timeout(100)
            metrics = page.evaluate(
                """() => ({
                    scrollWidth: document.documentElement.scrollWidth,
                    innerWidth: window.innerWidth,
                    clientWidth: document.documentElement.clientWidth,
                    scrollHeight: document.documentElement.scrollHeight
                })"""
            )
            results.check(
                metrics["scrollWidth"] == metrics["innerWidth"],
                "{}px {}: no horizontal overflow (scrollWidth {} == innerWidth {}, "
                "clientWidth {})".format(
                    width,
                    label,
                    metrics["scrollWidth"],
                    metrics["innerWidth"],
                    metrics["clientWidth"],
                ),
            )
            if check_height:
                results.check(
                    metrics["scrollHeight"] <= MAX_PAGE_HEIGHT,
                    "1440px {}: page height {} <= {}".format(
                        label, metrics["scrollHeight"], MAX_PAGE_HEIGHT
                    ),
                )


def report_runtime_errors(results):
    section("[6/6] Runtime errors and network")
    console_errors = dedupe(results.console_errors)
    page_errors = dedupe(results.page_errors)
    bad_responses = dedupe(results.bad_responses)

    for error in console_errors:
        results.fail("console error: " + error)
    if not console_errors:
        results.ok("no console errors")

    for error in page_errors:
        results.fail("uncaught page error: " + error)
    if not page_errors:
        results.ok("no uncaught page errors")

    for response in bad_responses:
        results.fail("local request failed: " + response)
    if not bad_responses:
        results.ok("no local HTTP 4xx/5xx responses")

    for url in dedupe(results.external_requests):
        print("  WARN  external request blocked: " + url)


def print_summary(results, shot_dir):
    failures = dedupe(results.failures)
    status = "PASS" if not failures else "FAIL"
    print("\n" + "=" * 72)
    print(
        "RESULT: {}  |  {} passed  |  {} failed  |  {} skipped".format(
            status, results.passed, len(failures), len(results.skips)
        )
    )
    if failures:
        print("Failures:")
        for failure in failures:
            print("  - " + failure)
    if results.skips:
        print("Skipped:")
        for skip in results.skips:
            print("  - " + skip)
    print("Screenshots: {}".format(shot_dir))
    print("=" * 72)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Headless Chromium smoke test for a running OpenCode dashboard. "
            "The script does not start the server."
        )
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8811,
        help="dashboard port (default: 8811)",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="dashboard host (default: 127.0.0.1)",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    base = "http://{}:{}".format(args.host, args.port)

    print("OpenCode dashboard UI smoke test")
    print("Base URL    : {}".format(base))
    print("Python      : {}".format(sys.executable))
    print("Viewports   : 1440x1000, 1280x900 (desktop only)")
    print("Screenshots : {}".format(SHOT_DIR))

    if PLAYWRIGHT_IMPORT_ERROR is not None:
        print("\nERROR: playwright is not importable for this interpreter.")
        print("  {}".format(short_error(PLAYWRIGHT_IMPORT_ERROR)))
        print("  Install it for this interpreter:")
        print("    {} -m pip install --user playwright".format(sys.executable))
        print("    {} -m playwright install chromium".format(sys.executable))
        print(
            "  Or run with an interpreter that already has Playwright, "
            "for example /usr/bin/python3."
        )
        return 1

    if not port_open(args.host, args.port):
        print(
            "\nERROR: no server is listening on {}:{}.".format(
                args.host, args.port
            )
        )
        print(
            "  Start it first: python3 opencode_dashboard.py --port {} --quiet".format(
                args.port
            )
        )
        return 1

    SHOT_DIR.mkdir(parents=True, exist_ok=True)
    results = Results()

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            print("Chromium    : {} (headless)".format(browser.version))
            context = browser.new_context(
                viewport={"width": 1440, "height": 1000},
                device_scale_factor=1,
            )
            # The dashboard stops its server when the last window unloads.
            # Defuse that app lifecycle hook so this test is non-destructive.
            context.add_init_script(
                """(() => {
                    try {
                        Object.defineProperty(navigator, 'sendBeacon', {
                            configurable: true,
                            value: () => true
                        });
                    } catch (_) {}
                })();"""
            )
            install_routes(context, results)

            page = context.new_page()
            attach_page(page, results)
            page.set_default_timeout(8000)
            page.set_default_navigation_timeout(30000)

            initial = "{}/#/opencode/overview".format(base)
            if goto_ready(page, initial, results, "initial page load"):
                results.ok("initial page loaded; #rail/.rail is visible")
                recap_present = detect_recap(page)
                check_views(page, results)
                check_hash_routes(context, base, results, recap_present)
                check_palette(page, base, results)
                check_settings(context, base, results)
                check_metrics(page, base, results)
            else:
                results.fail("aborting UI checks because the initial page did not load")

            report_runtime_errors(results)
            browser.close()
    except PlaywrightError as exc:
        results.fail("browser/test error: " + short_error(exc))
    except Exception as exc:  # noqa: BLE001 - report unexpected harness failures
        results.fail("unexpected test error: " + short_error(exc))
        traceback.print_exc()
    finally:
        print_summary(results, SHOT_DIR)

    return 0 if not results.failures else 1


if __name__ == "__main__":
    sys.exit(main())
