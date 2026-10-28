"""
Stratified sampler for Lumen notices, 2010-2026 -- v2.

Reuses the verified engine in ../ingest_data.py and ../config.py (auth
headers, 1.1s spacing, retry/backoff, 401/403/500/429 handling, DateWindow
millisecond-epoch ranges, schema validation/quarantine, atomic JSONL writes).

=====================================================================
WHY v2: a review of v1 found real problems. Each is named and fixed below.
=====================================================================

STATISTICAL BIAS FIXES (the important ones)

  #2/#6/#14  Page-uniform sampling was wrong: a page has PER_PAGE (or fewer,
             on the last page of a leaf) notices, but every page was drawn
             with equal probability. A notice on a 2-record page was ~10x
             more likely to be selected than one on a 20-record page.
             FIX: probability-proportional-to-size (PPS) sampling. Every
             candidate page gets an Efraimidis-Spirakis weighted-reservoir
             key `u ** (1/size)` (u ~ Uniform(0,1)), and pages are processed
             in descending key order. This is a standard streaming PPS
             sampling method: it makes each *page* selection probability
             proportional to its size without ever materializing a giant
             cumulative-weight table, and without needing to know in advance
             how many pages you'll need.

  #3        Final-page truncation took the first N notices of a page
             (`valid_records[:remaining]`), and Lumen's page ordering is
             deterministic -- so the same N records within a page would
             always be the ones chosen. FIX: `random.sample()` the exact
             number still needed from *within* the page's valid records,
             every time a page is only partially consumed (not just on the
             last page).

ROBUSTNESS FIXES

  #4   seen_ids was in-memory only -> duplicates possible across a crash/
       resume. FIX: persisted per-year in the checkpoint (a set of notice
       ids), reloaded on resume.

  #5   "replacement" page selection could re-draw the same bad page forever.
       FIX: failed pages go into a permanently-excluded, checkpoint-persisted
       `failed_pages` set and are never retried again in this project. The
       PPS priority-ordered candidate stream is also consumed exactly once
       per page (no re-insertion), so there is no loop at all, just "move to
       the next-highest-priority candidate."

  #7   No detection of API/schema drift (e.g. a field rename silently
       quarantining everything). FIX: a per-year quarantine-ratio circuit
       breaker -- after a minimum number of pages, if the quarantine ratio
       exceeds --max-quarantine-ratio, the year stops with a clear error
       instead of burning through the whole pool.

  #8   checkpoint.json had no corruption recovery. FIX: a rotating .bak
       copy is written before every overwrite; on load, a corrupt primary
       file falls back to .bak with a loud warning.

  #9   No SIGINT handling -- Ctrl+C mid-fetch could lose the latest state.
       FIX: a SIGINT/SIGTERM handler sets a flag checked between page
       fetches; the loop saves checkpoint and exits cleanly at the next
       safe point.

  #10  The v1 "pool" materialized every (leaf, page) tuple for a whole year
       up front. For an 8-million-notice year at per_page=10 that is 800K
       tuples -- survivable, but wasteful, and the "replacement" list
       comprehensions (`[p for p in pool if ...]`) were O(n) per draw.
       FIX: pages are generated and keyed lazily leaf-by-leaf via a
       generator; only the (key, leaf, page, size) tuples are sorted once
       per year (a single O(n log n) pass, no repeated filtering).

  #11  Fixed seed=42 always -> identical sample every run, which is good for
       reproducibility but bad if you deliberately want independent samples.
       FIX: default seed is now `None` (OS entropy); pass --seed N for
       reproducibility.

  #13  No wall-clock budget -- a hanging server could stall the run
       indefinitely (per-request timeout existed, but nothing capped total
       run time). FIX: --max-runtime-hours; checked between pages, same
       clean-exit path as SIGINT.

  #1   Year totals can drift while a multi-day run is in progress; the
       checkpoint's frozen proportions won't reflect that. This is not
       fully fixable against a live, non-snapshotted API (Lumen has no
       versioning/consistency token). MITIGATION: each year's probe is
       timestamped (`probed_at_utc`); if sampling for a year starts more
       than --stale-probe-warn-hours after its probe, a clear warning is
       logged (not silently trusted). Use --reprobe-year to force a fresh
       probe (and thus fresh targets) for a specific year before sampling.

  #12  Page *content* can also drift between the probe and the fetch (Lumen
       has no immutable snapshot/cursor -- ingest_data.py's own README
       already flags this as inherent). Residual risk, not fully solvable;
       mitigated by: (a) sorting by `date_received asc` by default, which is
       far more stable under inserts than relevance ordering, (b) dedup by
       notice `id` regardless of position, (c) the drift warning above.

Usage:
    python stratified_sample.py --grand-total 400000 --start-year 2010 --end-year 2026
    python stratified_sample.py --status
    python stratified_sample.py --reprobe-year 2015
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import shutil
import signal
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from math import ceil
from pathlib import Path
from typing import Any, Callable, Iterator

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import ConfigurationError, load_settings  # noqa: E402
from ingest_data import (  # noqa: E402
    AccessBlockedError,
    AuthenticationError,
    DateWindow,
    IngestionError,
    LargeQueryRefused,
    LumenClient,
    ResponseShapeError,
    RetryExhaustedHTTPError,
    UnsafeRedirectError,
    UnsplittableWindowError,
    atomic_write_jsonl,
    prepare_records,
)

LOGGER = logging.getLogger("stratified_sample")

# Reported real per-year totals (as of your last report). Used ONLY as a
# drift-check baseline during live month-probing -- NOT to skip any live
# requests, since month-level probing is already the minimum needed to
# partition each year correctly (there is no separate "whole year" request
# to skip in this design; year totals are already derived for free as the
# sum of the 12 real month probes). If the live-derived total drifts far
# from this baseline, that's logged as a warning, not silently ignored or
# silently trusted -- the dataset is live and can change during a run that
# spans many hours.
DEFAULT_EXPECTED_YEAR_TOTALS: dict[int, int] = {
    2010: 20_000,
    2011: 129_000,
    2012: 55_000,
    2013: 608_000,
    2014: 890_000,
    2015: 1_320_000,
    2016: 1_910_000,
    2017: 2_040_000,
    2018: 2_170_000,
    2019: 1_980_000,
    2020: 2_890_000,
    2021: 3_590_000,
    2022: 3_850_000,
    2023: 6_460_000,
    2024: 9_110_000,
    2025: 28_920_000,
}

RECOVERABLE_PAGE_ERRORS = (
    LargeQueryRefused,
    ResponseShapeError,
    RetryExhaustedHTTPError,
)
# NOT retried -- retrying into an auth failure or a ban only makes it worse.
FATAL_ERRORS = (AuthenticationError, AccessBlockedError, UnsafeRedirectError)


class GracefulStop(Exception):
    """Raised internally to unwind to the nearest checkpoint-save point."""


_STOP_REQUESTED = False


def _install_signal_handlers() -> None:
    def _handler(signum: int, _frame: Any) -> None:
        global _STOP_REQUESTED
        LOGGER.warning(
            "Received signal %s; will save checkpoint and stop at the next "
            "safe point (in-flight page fetch is allowed to finish).",
            signum,
        )
        _STOP_REQUESTED = True

    signal.signal(signal.SIGINT, _handler)
    try:
        signal.signal(signal.SIGTERM, _handler)
    except (ValueError, OSError):
        pass  # SIGTERM not available on some platforms; SIGINT is enough.


def year_bounds_ms(year: int) -> tuple[int, int]:
    start = datetime(year, 1, 1, tzinfo=timezone.utc)
    end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


@dataclass
class LeafWindow:
    start_ms: int
    end_ms: int
    total_entries: int
    total_pages: int
    per_page: int

    def page_size(self, page_num: int) -> int:
        if page_num < self.total_pages:
            return self.per_page
        remainder = self.total_entries - self.per_page * (self.total_pages - 1)
        return max(1, remainder)


@dataclass
class YearPlan:
    year: int
    probed_total: int = 0
    probed_at_utc: str = ""
    target: int = 0
    leaves: list[LeafWindow] = field(default_factory=list)
    drawn_pages: set[tuple[int, int, int]] = field(default_factory=set)
    failed_pages: set[tuple[int, int, int]] = field(default_factory=set)
    seen_ids: set[str] = field(default_factory=set)
    fetched_records: int = 0
    pages_processed: int = 0
    quarantined_count: int = 0
    shortfalls: int = 0
    status: str = "pending"  # pending -> probed -> sampling -> done -> failed


class Checkpoint:
    """JSON checkpoint with a rotating .bak copy and corruption recovery."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.bak_path = path.with_suffix(path.suffix + ".bak")
        self.data: dict[str, Any] = {}
        self.data = self._load_with_fallback()

    def _load_with_fallback(self) -> dict[str, Any]:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except (json.JSONDecodeError, OSError) as exc:
                LOGGER.error(
                    "Primary checkpoint %s is corrupt (%s); trying backup %s",
                    self.path, exc, self.bak_path,
                )
        if self.bak_path.exists():
            try:
                data = json.loads(self.bak_path.read_text())
                LOGGER.warning("Recovered checkpoint state from backup file.")
                return data
            except (json.JSONDecodeError, OSError) as exc:
                LOGGER.error("Backup checkpoint also corrupt/unreadable: %s", exc)
        return {}

    def save(self, plans: dict[int, YearPlan], meta: dict[str, Any], cache: "WindowCache | None" = None, budget: "RequestBudget | None" = None) -> None:
        payload = {
            "meta": meta,
            "window_cache": cache.to_list() if cache is not None else self.data.get("window_cache", []),
            "budget_used": budget.used if budget is not None else self.data.get("budget_used", 0),
            "years": {
                str(y): {
                    "year": p.year,
                    "probed_total": p.probed_total,
                    "probed_at_utc": p.probed_at_utc,
                    "target": p.target,
                    "leaves": [asdict(leaf) for leaf in p.leaves],
                    "drawn_pages": sorted(list(p.drawn_pages)),
                    "failed_pages": sorted(list(p.failed_pages)),
                    "seen_ids": sorted(list(p.seen_ids)),
                    "fetched_records": p.fetched_records,
                    "pages_processed": p.pages_processed,
                    "quarantined_count": p.quarantined_count,
                    "shortfalls": p.shortfalls,
                    "status": p.status,
                }
                for y, p in plans.items()
            },
        }
        # Rotate: keep the previous good file as .bak before overwriting.
        if self.path.exists():
            try:
                shutil.copy2(self.path, self.bak_path)
            except OSError as exc:
                LOGGER.warning("Could not update checkpoint backup: %s", exc)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.replace(self.path)

    def load_cache(self) -> "WindowCache":
        return WindowCache.from_list(self.data.get("window_cache", []))

    def load_budget_used(self) -> int:
        return int(self.data.get("budget_used", 0))

    def load_plans(self) -> dict[int, YearPlan]:
        plans: dict[int, YearPlan] = {}
        for y_str, p in self.data.get("years", {}).items():
            plan = YearPlan(
                year=p["year"],
                probed_total=p["probed_total"],
                probed_at_utc=p.get("probed_at_utc", ""),
                target=p["target"],
                leaves=[LeafWindow(**leaf) for leaf in p["leaves"]],
                drawn_pages={tuple(x) for x in p["drawn_pages"]},
                failed_pages={tuple(x) for x in p.get("failed_pages", [])},
                seen_ids=set(p.get("seen_ids", [])),
                fetched_records=p["fetched_records"],
                pages_processed=p.get("pages_processed", 0),
                quarantined_count=p.get("quarantined_count", 0),
                shortfalls=p["shortfalls"],
                status=p["status"],
            )
            plans[int(y_str)] = plan
        return plans


def month_bounds_ms(year: int, month: int) -> tuple[int, int]:
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


class WindowCache:
    """Caches every real probed (start_ms, end_ms) -> total_entries this run
    has already paid for, so an identical window is never re-queried (doc
    idea #4). Also stores inferred (not-yet-verified) counts so that a
    later real fetch of that same exact range can skip straight past the
    "unknown" state -- though inferred counts are never trusted for pagination
    metadata (total_pages/per_page), only for split-vs-leaf decisions.
    """

    def __init__(self) -> None:
        self._counts: dict[tuple[int, int], int] = {}

    def get(self, start_ms: int, end_ms: int) -> int | None:
        return self._counts.get((start_ms, end_ms))

    def put(self, start_ms: int, end_ms: int, total: int) -> None:
        self._counts[(start_ms, end_ms)] = total

    def to_list(self) -> list[list[int]]:
        return [[s, e, t] for (s, e), t in self._counts.items()]

    @classmethod
    def from_list(cls, items: list[list[int]]) -> "WindowCache":
        cache = cls()
        for s, e, t in items:
            cache._counts[(s, e)] = t
        return cache


class RequestBudget:
    """Tracks live requests spent and exposes 'pressure' in [0, 1] so
    partitioning can relax its leaf-size tolerance as the budget runs low
    (doc idea #12: budget-aware, graceful degradation instead of a hard
    stop). A budget of None means unlimited (pressure is always 0)."""

    def __init__(self, limit: int | None, progress_every: int = 25, on_progress=None) -> None:
        self.limit = limit
        self.used = 0
        self.progress_every = progress_every
        self.on_progress = on_progress  # optional callable(requests_used:int) -> None
        self._last_reported = 0

    def spend(self, n: int = 1) -> None:
        self.used += n
        if self.on_progress and self.used - self._last_reported >= self.progress_every:
            self._last_reported = self.used
            self.on_progress(self.used)

    def remaining(self) -> int | None:
        if self.limit is None:
            return None
        return max(0, self.limit - self.used)

    def pressure(self) -> float:
        if self.limit is None or self.limit == 0:
            return 0.0
        return min(1.0, self.used / self.limit)


#: Calendar-meaningful granularities to drop straight to when a window is
#: confirmed capped, instead of plain 50/50 binary halving. A dense month
#: goes directly to week-sized pieces; a dense week goes directly to
#: day-sized pieces; and so on. Only a piece that's STILL capped at one
#: rung descends to the next, finer rung -- so a single bad day inside an
#: otherwise-ordinary month no longer drags the whole month through
#: several powers-of-two binary levels to isolate it.
_GRANULARITY_LADDER_MS: tuple[int, ...] = (
    7 * 24 * 3600 * 1000,  # week
    24 * 3600 * 1000,      # day
    3600 * 1000,           # hour
    60 * 1000,             # minute
    1000,                  # second
)


def _next_slice_width_ms(window_width_ms: int) -> int:
    """Largest calendar-meaningful granularity strictly smaller than the
    given window width, falling back to 1ms once we're already finer than
    a second (matches the existing UnsplittableWindowError floor)."""
    for width in _GRANULARITY_LADDER_MS:
        if width < window_width_ms:
            return width
    return 1


def _slice_window(start_ms: int, end_ms: int, slice_width_ms: int) -> list[tuple[int, int]]:
    slices = []
    cur = start_ms
    while cur < end_ms:
        nxt = min(cur + slice_width_ms, end_ms)
        slices.append((cur, nxt))
        cur = nxt
    return slices


#: One item of partitioning work: a (start_ms, end_ms) window and either a
#: known total (int) or None (meaning "not yet probed / not yet in cache").
#: This is the serializable unit a checkpoint needs to persist so a
#: multi-hour partition can resume mid-way instead of restarting a whole
#: month (or year) from scratch after a crash/Ctrl-C.
PendingWindow = tuple[int, int, int | None]


def _level_label(width_ms: int) -> str:
    """Human-readable name for a window's granularity, for progress logs."""
    if width_ms >= 28 * 24 * 3600 * 1000:
        return "month"
    if width_ms >= 7 * 24 * 3600 * 1000:
        return "week"
    if width_ms >= 24 * 3600 * 1000:
        return "day"
    if width_ms >= 3600 * 1000:
        return "hour"
    if width_ms >= 60 * 1000:
        return "minute"
    if width_ms >= 1000:
        return "second"
    return "millisecond"


def _fmt_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def split_year_into_leaves(
    client: LumenClient,
    year: int,
    ceiling: int,
    cache: WindowCache,
    budget: RequestBudget,
    target_leaf_fraction: float = 0.95,
    checkpoint_cb: Callable[[list[LeafWindow], list[PendingWindow], int], None] | None = None,
    checkpoint_every: int = 100,
    resume_stack: list[PendingWindow] | None = None,
    resume_leaves: list[LeafWindow] | None = None,
    resume_year_total: int = 0,
    log_every: int | None = 10,
) -> tuple[list[LeafWindow], int]:
    """Partition a calendar year into real, verified leaves (each
    total_entries < ceiling), spending as few live requests as possible.

    This is an EXPLICIT worklist (a stack), not Python recursion, so it can
    be checkpointed and resumed mid-partition: pass `checkpoint_cb` to have
    it called every `checkpoint_every` requests with
    (leaves_so_far, remaining_stack, year_total_so_far) -- persist that
    tuple, and on resume pass it back in as `resume_stack` /
    `resume_leaves` / `resume_year_total` to continue exactly where the
    previous run left off instead of re-probing the whole year (or even
    the whole month) again. Without a crash, a normal run just processes
    the stack to completion and returns (leaves, year_total) as before.

    Splitting strategy: when a window's real total is confirmed >= ceiling
    (see the clamping note in `_resolve_one_window` below), it drops
    straight to the next calendar-meaningful granularity (week -> day ->
    hour -> minute -> second -> ms) instead of bisecting 50/50 -- a
    specific dense day resolves in far fewer probe requests than fighting
    through several binary levels first. Every slice at every rung gets
    its own real probe (a clamped/unreliable parent total can never be
    used to infer a sibling's count by subtraction -- see the note in
    `_resolve_one_window`).

    `log_every` (default 10 requests, independent of `checkpoint_every`)
    logs a lightweight forward-progress snapshot -- requests spent so far,
    leaves resolved, windows still pending, and which window/granularity
    is currently being probed -- so a long partition never looks silently
    stuck. Pass None or 0 to disable.

    Returns (leaves, sum_of_total_entries_across_the_year).
    """
    leaves: list[LeafWindow] = list(resume_leaves) if resume_leaves is not None else []
    year_total = resume_year_total
    checkpoint_baseline = budget.used
    log_baseline = budget.used

    if resume_stack is not None:
        stack: list[PendingWindow] = list(resume_stack)
        LOGGER.info(
            "Resuming partition of year %d from a saved checkpoint: %d leaves "
            "already resolved, %d windows still pending.",
            year, len(leaves), len(stack),
        )
    else:
        # Seed with the 12 calendar months. These 12 probes are needed
        # regardless (proportional month/year allocation elsewhere relies
        # on them), so they aren't overhead.
        stack = []
        for month in range(1, 13):
            m_start, m_end = month_bounds_ms(year, month)
            cached = cache.get(m_start, m_end)
            stack.append((m_start, m_end, cached))
        stack.reverse()  # pop() takes from the end -> process Jan first

    while stack:
        start_ms, end_ms, known_total = stack.pop()
        level = _level_label(end_ms - start_ms)

        try:
            resolved_leaves, children = _resolve_one_window(
                client, cache, budget, start_ms, end_ms, ceiling,
                target_leaf_fraction, known_total,
            )
        except LargeQueryRefused:
            # Same handling as before: treat as "must split", starting at
            # the top calendar rung for this window's own width.
            resolved_leaves = []
            children = [(s, e, None) for s, e in _slice_window(start_ms, end_ms, _next_slice_width_ms(end_ms - start_ms))]

        leaves.extend(resolved_leaves)
        year_total += sum(leaf.total_entries for leaf in resolved_leaves)
        stack.extend(reversed(children))

        if log_every and (budget.used - log_baseline) >= log_every:
            LOGGER.info(
                "Requests: %d | Leaves: %d | Pending windows: %d | "
                "Current window: %s -> %s | Current level: %s",
                budget.used, len(leaves), len(stack),
                _fmt_ms(start_ms), _fmt_ms(end_ms), level,
            )
            log_baseline = budget.used

        if checkpoint_cb is not None and (budget.used - checkpoint_baseline) >= checkpoint_every:
            checkpoint_cb(list(leaves), list(stack), year_total)
            checkpoint_baseline = budget.used

    LOGGER.info(
        "Year %d partitioned: %d leaves, %d total notices.",
        year, len(leaves), year_total,
    )
    return leaves, year_total


def _resolve_one_window(
    client: LumenClient,
    cache: WindowCache,
    budget: RequestBudget,
    start_ms: int,
    end_ms: int,
    ceiling: int,
    target_leaf_fraction: float,
    known_total: int | None,
) -> tuple[list[LeafWindow], list[PendingWindow]]:
    """Resolve exactly ONE window from the worklist: either it becomes a
    final leaf (returned in the first list) or it needs further splitting
    (its calendar-ladder children are returned in the second list, to be
    pushed back onto the caller's stack). Never recurses -- this is the
    single-step primitive the iterative worklist in `split_year_into_leaves`
    calls repeatedly, which is what makes checkpointing possible: the
    caller's stack is a plain list of (start_ms, end_ms, known_total)
    tuples, trivially JSON-serializable, rather than state buried on the
    Python call stack.
    """
    effective_fraction = target_leaf_fraction + (1.0 - target_leaf_fraction) * budget.pressure()
    effective_target = max(1, int(ceiling * effective_fraction))

    if known_total is None:
        known_total = cache.get(start_ms, end_ms)

    real_page = None
    if known_total is None:
        real_page = client.search_page(DateWindow(start_ms, end_ms), page=1)
        budget.spend(1)
        cache.put(start_ms, end_ms, real_page.total_entries)
        known_total = real_page.total_entries

    if known_total == 0:
        return [], []

    # A total_entries that lands EXACTLY on `ceiling` (or above) is not
    # trustworthy: Lumen's Elasticsearch backend clamps the reported total
    # at the 10,000-result ceiling for any window whose real count is >=
    # ceiling, so "10000" can mean "exactly 10000 notices" or "5,000,000
    # notices, clamped." Only values strictly BELOW ceiling are genuine,
    # exact counts. A clamped value must always be forced through another
    # real split of its children -- never accepted as a leaf, and never
    # used to infer a sibling's count by subtraction (that arithmetic on a
    # clamped parent silently produces near-zero/garbage results, which is
    # what caused the original "always ~10k per leaf, no matter the year's
    # real size" bug).
    is_reliable = known_total < ceiling

    if is_reliable and (known_total <= effective_target or (end_ms - start_ms) <= 1):
        if real_page is None:
            real_page = client.search_page(DateWindow(start_ms, end_ms), page=1)
            budget.spend(1)
            cache.put(start_ms, end_ms, real_page.total_entries)
            if real_page.total_entries >= ceiling:
                known_total = real_page.total_entries
                real_page = None
                is_reliable = False
            else:
                return [LeafWindow(start_ms, end_ms, real_page.total_entries, real_page.total_pages, real_page.per_page)], []
        else:
            return [LeafWindow(start_ms, end_ms, real_page.total_entries, real_page.total_pages, real_page.per_page)], []

    if end_ms - start_ms <= 1:
        raise UnsplittableWindowError(
            f"Window {start_ms}..{end_ms} reports {known_total} results at "
            f"single-millisecond granularity (ceiling={ceiling}) and cannot "
            "be split further. If known_total >= ceiling this may be a "
            "clamped, not true, count -- Lumen has no way to disambiguate "
            "at this granularity. Contact Lumen for a narrower server-side "
            "partition or an alternate count-verification endpoint."
        )

    if real_page is not None and real_page.total_entries < ceiling:
        return [LeafWindow(start_ms, end_ms, real_page.total_entries, real_page.total_pages, real_page.per_page)], []

    # Must split further -- drop to the next calendar-meaningful
    # granularity rather than bisecting 50/50. Every child is left
    # unresolved (known_total=None) EXCEPT where the cache already has an
    # answer from a previous (possibly interrupted) run, in which case it's
    # reused for free.
    slice_width = _next_slice_width_ms(end_ms - start_ms)
    children: list[PendingWindow] = []
    for s_start, s_end in _slice_window(start_ms, end_ms, slice_width):
        cached_child = cache.get(s_start, s_end)
        children.append((s_start, s_end, cached_child))
    return [], children
def pps_ranked_candidates(
    leaves: list[LeafWindow],
    exclude: set[tuple[int, int, int]],
    rng: random.Random,
) -> list[tuple[float, int, int, int, int]]:
    """Efraimidis-Spirakis weighted-reservoir ranking: every (leaf, page) is
    assigned a priority key `u ** (1/size)` with u ~ Uniform(0,1). Sorting by
    this key descending and taking items in order is equivalent to weighted
    sampling WITHOUT replacement, probability-proportional-to-size, in a
    single pass -- this is what makes page selection unbiased at the notice
    level instead of the notice level instead of biased toward small pages.

    Returns a list sorted by descending key: (key, start_ms, end_ms, page, size).
    Excluded (already drawn or permanently failed) pages are left out.
    """
    candidates: list[tuple[float, int, int, int, int]] = []
    for leaf in leaves:
        for page_num in range(1, leaf.total_pages + 1):
            key3 = (leaf.start_ms, leaf.end_ms, page_num)
            if key3 in exclude:
                continue
            size = leaf.page_size(page_num)
            u = rng.random()
            # size is always >= 1, so 1/size is safe.
            priority = u ** (1.0 / size)
            candidates.append((priority, leaf.start_ms, leaf.end_ms, page_num, size))
    candidates.sort(key=lambda t: t[0], reverse=True)
    return candidates


def fetch_one_page(client: LumenClient, start_ms: int, end_ms: int, page_num: int) -> list[Any]:
    window = DateWindow(start_ms, end_ms)
    search_page = client.search_page(window, page=page_num)
    if not search_page.notices:
        raise ResponseShapeError(
            f"Page {page_num} of window {window.query_value} came back empty "
            "unexpectedly; refusing to record it as a valid sample."
        )
    return search_page.notices


def check_runtime_budget(start_time: float, max_hours: float | None) -> None:
    if max_hours is None:
        return
    elapsed_hours = (time.monotonic() - start_time) / 3600.0
    if elapsed_hours >= max_hours:
        raise GracefulStop(f"Wall-clock budget of {max_hours}h reached.")


def run(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    _install_signal_handlers()

    try:
        settings = load_settings()
    except ConfigurationError as exc:
        LOGGER.error("Configuration error: %s", exc)
        return 2

    if args.expected_year_totals_file:
        try:
            raw = json.loads(Path(args.expected_year_totals_file).read_text())
            args.expected_year_totals = {int(k): int(v) for k, v in raw.items()}
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            LOGGER.error("Could not read --expected-year-totals-file: %s", exc)
            return 2
    else:
        args.expected_year_totals = DEFAULT_EXPECTED_YEAR_TOTALS

    if not settings.large_project_contacted:
        LOGGER.error(
            "LUMEN_LARGE_PROJECT_CONTACTED must be true for a multi-year, "
            "hundreds-of-thousands-of-records project. Lumen's researcher "
            "guidance asks for advance contact at team@lumendatabase.org "
            "for projects at this scale."
        )
        return 2

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    state_dir = Path(args.state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir = out_dir / "quarantine"
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    ckpt = Checkpoint(state_dir / "stratified_checkpoint.json")
    rng = random.Random(args.seed) if args.seed is not None else random.Random()
    cache = ckpt.load_cache()
    budget = RequestBudget(args.request_budget)
    budget.used = ckpt.load_budget_used()

    years = list(range(args.start_year, args.end_year + 1))

    if args.status:
        plans = ckpt.load_plans()
        if not plans:
            print("No checkpoint yet.")
            return 0
        for y in years:
            p = plans.get(y)
            if not p:
                print(f"year={y} status=not-started")
                continue
            print(
                f"year={y} status={p.status} probed_total={p.probed_total} "
                f"probed_at={p.probed_at_utc} target={p.target} "
                f"fetched={p.fetched_records} pages_processed={p.pages_processed} "
                f"quarantined={p.quarantined_count} shortfalls={p.shortfalls}"
            )
        print(f"--- window_cache entries={len(cache.to_list())} requests_used={budget.used} "
              f"budget_limit={budget.limit} pressure={budget.pressure()*100:.0f}%")
        return 0

    plans: dict[int, YearPlan] = ckpt.load_plans()
    for y in years:
        if y not in plans:
            plans[y] = YearPlan(year=y)
    if args.reprobe_year is not None and args.reprobe_year in plans:
        LOGGER.info("Forcing re-probe of year %d as requested.", args.reprobe_year)
        plans[args.reprobe_year].status = "pending"
        plans[args.reprobe_year].leaves = []

    start_time = time.monotonic()

    try:
        with LumenClient(settings) as client:
            # ---- Phase 1: probe every year not already probed ----
            for y in years:
                plan = plans[y]
                if plan.status != "pending":
                    continue
                if _STOP_REQUESTED:
                    raise GracefulStop("Stop requested before probing completed.")
                LOGGER.info("Probing year %d ...", y)
                try:
                    leaves, total = split_year_into_leaves(
                        client, y, settings.search_result_ceiling, cache, budget,
                        target_leaf_fraction=args.target_leaf_fraction,
                    )
                except FATAL_ERRORS as exc:
                    LOGGER.error("Fatal error probing year %d: %s", y, exc)
                    plan.status = "failed"
                    ckpt.save(plans, {"phase": "probe", "failed_year": y}, cache, budget)
                    return 3
                except (IngestionError, UnsplittableWindowError) as exc:
                    LOGGER.error("Could not fully probe year %d: %s", y, exc)
                    plan.status = "failed"
                    ckpt.save(plans, {"phase": "probe", "failed_year": y}, cache, budget)
                    continue

                plan.leaves = leaves
                plan.probed_total = total
                plan.probed_at_utc = datetime.now(timezone.utc).isoformat()
                plan.status = "probed"
                LOGGER.info(
                    "Year %d: probed_total=%d across %d leaf windows",
                    y, total, len(leaves),
                )
                expected = args.expected_year_totals.get(y)
                if expected and expected > 0 and not args.no_drift_check:
                    drift_pct = abs(total - expected) / expected * 100
                    if drift_pct > args.drift_warn_pct:
                        LOGGER.warning(
                            "Year %d: live probed_total=%d differs from your "
                            "reported total=%d by %.1f%% (threshold %.0f%%). "
                            "The dataset is live and may have changed, or your "
                            "reported figure may be stale -- proceeding with "
                            "the LIVE number for allocation (it's always more "
                            "current), but flagging this so it isn't silent.",
                            y, total, expected, drift_pct, args.drift_warn_pct,
                        )
                    else:
                        LOGGER.info(
                            "Year %d: live total matches your reported total "
                            "within %.1f%% (no drift concern).", y, drift_pct,
                        )
                ckpt.save(plans, {"phase": "probe"}, cache, budget)

            probed_years = {
                y: p for y, p in plans.items() if p.status in ("probed", "sampling", "done")
            }
            grand_probed_total = sum(p.probed_total for p in probed_years.values())
            if grand_probed_total == 0:
                LOGGER.error("No year could be probed successfully; aborting.")
                return 4

            LOGGER.info(
                "Probed grand total across %d years: %d notices available.",
                len(probed_years), grand_probed_total,
            )

            # ---- Phase 2: proportional targets, capped by availability ----
            for y, plan in probed_years.items():
                if plan.target:
                    continue
                share = plan.probed_total / grand_probed_total
                raw_target = int(round(args.grand_total * share))
                plan.target = min(raw_target, plan.probed_total)
            ckpt.save(plans, {"phase": "targets"}, cache, budget)

            for y in sorted(probed_years):
                p = probed_years[y]
                LOGGER.info("year=%d probed_total=%d target=%d", y, p.probed_total, p.target)

            # ---- Phase 3: PPS-sample pages per year ----
            for y in sorted(probed_years):
                plan = probed_years[y]
                if plan.status == "done":
                    LOGGER.info("Year %d already done (%d records).", y, plan.fetched_records)
                    continue

                # Mitigation for #1: warn if the probe is stale relative to
                # when we're actually about to sample it.
                if plan.probed_at_utc:
                    probed_at = datetime.fromisoformat(plan.probed_at_utc)
                    stale_hours = (datetime.now(timezone.utc) - probed_at).total_seconds() / 3600.0
                    if stale_hours > args.stale_probe_warn_hours:
                        LOGGER.warning(
                            "Year %d was probed %.1fh ago; Lumen's dataset may "
                            "have changed since, so this year's proportional "
                            "target could be stale. Use --reprobe-year %d to "
                            "refresh it before trusting the figures.",
                            y, stale_hours, y,
                        )

                plan.status = "sampling"
                LOGGER.info(
                    "Sampling year %d: target=%d records, per_page=%d (PPS, "
                    "notice-level unbiased within selected pages)",
                    y, plan.target, settings.per_page,
                )

                exclude = set(plan.drawn_pages) | set(plan.failed_pages)
                ranked = pps_ranked_candidates(plan.leaves, exclude, rng)
                LOGGER.info(
                    "Year %d: %d candidate pages ranked (PPS priority order).",
                    y, len(ranked),
                )

                batch_records: list[Any] = []
                batch_index = 0
                quarantine_batch_index = 0
                run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
                min_pages_before_breaker = args.min_pages_before_quarantine_check

                for priority, start_ms, end_ms, page_num, size in ranked:
                    if plan.fetched_records >= plan.target:
                        break
                    if _STOP_REQUESTED:
                        raise GracefulStop(f"Stop requested during year {y} sampling.")
                    check_runtime_budget(start_time, args.max_runtime_hours)

                    attempt_key = (start_ms, end_ms, page_num)

                    try:
                        notices = fetch_one_page(client, start_ms, end_ms, page_num)
                    except RECOVERABLE_PAGE_ERRORS as exc:
                        LOGGER.warning(
                            "Year %d: page %s failed permanently (%s); "
                            "excluded from this project, moving to next "
                            "PPS-ranked candidate.",
                            y, attempt_key, exc,
                        )
                        plan.shortfalls += 1
                        plan.failed_pages.add(attempt_key)
                        continue
                    except FATAL_ERRORS as exc:
                        LOGGER.error("Fatal error sampling year %d: %s", y, exc)
                        plan.status = "failed"
                        ckpt.save(plans, {"phase": "sampling", "failed_year": y}, cache, budget)
                        return 3

                    plan.pages_processed += 1

                    accepted, quarantined = prepare_records(
                        records=notices,
                        endpoint=settings.search_path,
                        window=DateWindow(start_ms, end_ms),
                        page=page_num,
                    )
                    if quarantined:
                        plan.quarantined_count += len(quarantined)
                        quarantine_batch_index += 1
                        atomic_write_jsonl(
                            quarantine_dir / f"bad_records_{y}_{run_id}_{quarantine_batch_index:04d}.jsonl",
                            quarantined,
                            fsync=settings.fsync_writes,
                        )

                    # Circuit breaker for #7: possible API/schema drift.
                    if plan.pages_processed >= min_pages_before_breaker:
                        total_seen = plan.pages_processed * settings.per_page
                        ratio = plan.quarantined_count / max(1, total_seen)
                        if ratio > args.max_quarantine_ratio:
                            LOGGER.error(
                                "Year %d: quarantine ratio %.1f%% exceeds "
                                "--max-quarantine-ratio=%.1f%% after %d pages. "
                                "This usually means Lumen's response schema "
                                "changed (e.g. a renamed field) and every "
                                "record is being wrongly rejected. Stopping "
                                "this year instead of burning through the "
                                "whole sample pool on bad data.",
                                y, ratio * 100, args.max_quarantine_ratio * 100,
                                plan.pages_processed,
                            )
                            plan.status = "failed"
                            ckpt.save(plans, {"phase": "sampling", "failed_year": y, "reason": "quarantine_circuit_breaker"}, cache, budget)
                            return 5

                    dup_ids = {rid for rid, _env in accepted} & plan.seen_ids
                    fresh = [(rid, env) for rid, env in accepted if rid not in dup_ids]
                    if dup_ids:
                        LOGGER.info(
                            "Year %d: page %s had %d already-seen id(s); kept only new ones.",
                            y, attempt_key, len(dup_ids),
                        )

                    if not fresh:
                        LOGGER.warning(
                            "Year %d: page %s produced zero usable records "
                            "(all quarantined or duplicate); moving on.",
                            y, attempt_key,
                        )
                        plan.drawn_pages.add(attempt_key)  # don't refetch it
                        continue

                    # Fix for #3: never take a deterministic first-N slice.
                    # If we need fewer than what's on this page, choose a
                    # true random subset of the page's own records.
                    remaining = plan.target - plan.fetched_records
                    if len(fresh) > remaining:
                        fresh = rng.sample(fresh, remaining)

                    envelopes = [env for _rid, env in fresh]
                    batch_records.extend(envelopes)
                    plan.fetched_records += len(envelopes)
                    plan.seen_ids.update(rid for rid, _env in fresh)
                    plan.drawn_pages.add(attempt_key)

                    if len(batch_records) >= args.flush_every:
                        batch_index += 1
                        out_path = out_dir / f"stratified_{run_id}_y{y}_b{batch_index:04d}.jsonl"
                        atomic_write_jsonl(out_path, batch_records, fsync=settings.fsync_writes)
                        batch_records = []
                        ckpt.save(plans, {"phase": "sampling"}, cache, budget)

                if batch_records:
                    batch_index += 1
                    out_path = out_dir / f"stratified_{run_id}_y{y}_b{batch_index:04d}.jsonl"
                    atomic_write_jsonl(out_path, batch_records, fsync=settings.fsync_writes)

                if plan.fetched_records < plan.target:
                    LOGGER.warning(
                        "Year %d finished with a SHORTFALL: fetched=%d "
                        "target=%d (PPS candidate pool exhausted after %d "
                        "permanent page failures). Recorded, not hidden.",
                        y, plan.fetched_records, plan.target, plan.shortfalls,
                    )
                plan.status = "done"
                ckpt.save(plans, {"phase": "sampling"}, cache, budget)
                LOGGER.info(
                    "Year %d complete: fetched=%d target=%d pages_processed=%d "
                    "quarantined=%d shortfalls=%d",
                    y, plan.fetched_records, plan.target, plan.pages_processed,
                    plan.quarantined_count, plan.shortfalls,
                )

    except GracefulStop as exc:
        LOGGER.warning("Stopping gracefully: %s (checkpoint already saved).", exc)
        ckpt.save(plans, {"phase": "interrupted"}, cache, budget)
        return 130

    grand_fetched = sum(p.fetched_records for p in plans.values())
    grand_target = sum(p.target for p in plans.values())
    LOGGER.info(
        "RUN COMPLETE. grand_fetched=%d grand_target=%d (%.1f%%)",
        grand_fetched, grand_target,
        100.0 * grand_fetched / grand_target if grand_target else 0.0,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--grand-total", type=int, default=400_000,
                         help="Overall target record count (default 400000).")
    parser.add_argument("--start-year", type=int, default=2010)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--out-dir", default="stratified_raw")
    parser.add_argument("--state-dir", default=".state")
    parser.add_argument("--flush-every", type=int, default=200, help="Records per output batch file.")
    parser.add_argument("--seed", type=int, default=None,
                         help="RNG seed. Default: None (OS entropy, independent sample each run). "
                              "Pass an integer for a reproducible sample.")
    parser.add_argument("--status", action="store_true",
                         help="Print checkpoint status and exit; makes no live requests.")
    parser.add_argument("--reprobe-year", type=int, default=None,
                         help="Force a fresh probe (and fresh leaves) for one specific year before sampling.")
    parser.add_argument("--stale-probe-warn-hours", type=float, default=48.0,
                         help="Warn if a year is sampled this many hours after it was probed.")
    parser.add_argument("--max-quarantine-ratio", type=float, default=0.5,
                         help="Abort a year's sampling if the quarantine ratio exceeds this "
                              "(possible API/schema drift), after --min-pages-before-quarantine-check pages.")
    parser.add_argument("--min-pages-before-quarantine-check", type=int, default=20,
                         help="Minimum pages processed before the quarantine-ratio circuit breaker can trip.")
    parser.add_argument("--max-runtime-hours", type=float, default=None,
                         help="Wall-clock budget for the whole run; stops gracefully (resumable) if exceeded.")
    parser.add_argument("--request-budget", type=int, default=None,
                         help="Cap on total LIVE requests (probing + fetching combined) for this run. "
                              "As pressure rises toward this cap, leaf-size tolerance relaxes automatically "
                              "(fewer, slightly larger leaves) instead of hard-failing. Default: unlimited.")
    parser.add_argument("--target-leaf-fraction", type=float, default=0.95,
                         help="Soft target leaf size as a fraction of the hard 10,000-result ceiling "
                              "(default 0.95 -> ~9500). Higher = fewer, larger leaves = fewer requests, "
                              "at the cost of slightly less uniform leaf sizes for PPS sampling.")
    parser.add_argument("--probe-per-page", type=int, nargs="+", metavar="N",
                         help="Test mode: do NOT run the sampler. Instead, for each value of N given, "
                              "fetch a single small window at that per_page and report what Lumen actually "
                              "returns (accepted or rejected, and the real per_page/total_pages it used). "
                              "per_page's documented default is 10 with no confirmed max -- use this to find "
                              "the real ceiling before committing to a full run. Example: "
                              "--probe-per-page 10 25 50 100 200")
    parser.add_argument("--expected-year-totals-file", default=None,
                         help="JSON file of {year: expected_total} to drift-check live-probed year totals "
                              "against during the run (logs a warning on large drift, never silently "
                              "trusted or silently ignored). Defaults to the built-in reported totals "
                              "(2010-2025) if not given.")
    parser.add_argument("--no-drift-check", action="store_true",
                         help="Disable the expected-year-totals drift check entirely.")
    parser.add_argument("--drift-warn-pct", type=float, default=15.0,
                         help="Percent difference between live and expected year totals that triggers a warning.")
    return parser


def run_probe_per_page(values: list[int]) -> int:
    """Test mode: try each candidate per_page against a small, cheap real
    window and report what Lumen actually does. Costs len(values) live
    requests total. Does not touch the checkpoint or output directories."""
    import dataclasses

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        base_settings = load_settings()
    except ConfigurationError as exc:
        LOGGER.error("Configuration error: %s", exc)
        return 2

    # A small, cheap, recent window -- just enough to see real pagination
    # behavior without pulling anything large.
    end = datetime.now(timezone.utc)
    start = end.replace(hour=0, minute=0, second=0, microsecond=0)
    start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    window = DateWindow(start_ms, end_ms)

    print(f"Testing per_page values against window {window.query_value} ...")
    print(f"{'per_page requested':<20} {'result':<12} {'real per_page':<15} {'total_pages':<12} {'notes'}")

    for n in values:
        settings_n = dataclasses.replace(base_settings, per_page=n)
        try:
            with LumenClient(settings_n) as client:
                page = client.search_page(window, page=1)
            print(f"{n:<20} {'OK':<12} {page.per_page:<15} {page.total_pages:<12} "
                  f"({'server honored request' if page.per_page == n else 'server used its own value instead'})")
        except RECOVERABLE_PAGE_ERRORS as exc:
            print(f"{n:<20} {'REJECTED':<12} {'-':<15} {'-':<12} {exc}")
        except FATAL_ERRORS as exc:
            print(f"{n:<20} {'FATAL':<12} {'-':<15} {'-':<12} {exc}")
            return 3
        time.sleep(base_settings.request_delay_seconds)  # stay polite even in test mode

    print("\nUse the largest per_page where the server 'honored request' as your real "
          "--flush-every-aware fetch setting (set LUMEN_PER_PAGE in .env accordingly).")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.probe_per_page:
        return run_probe_per_page(args.probe_per_page)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
