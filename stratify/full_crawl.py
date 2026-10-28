"""
full_crawl.py -- Exhaustive full-corpus crawler for the Lumen Database.

This is NOT the sampler. It reuses the verified, tested building blocks from
stratify/stratified_sample.py (year -> leaf partitioning via
split_year_into_leaves, which recursively splits any date window whose
total_entries exceeds the 10,000 Elasticsearch ceiling, down to millisecond
granularity if truly necessary) and ingest_data.py (LumenClient: auth
headers, 1.1s request spacing, retry/backoff, 401/403/429/500 handling,
schema validation + quarantine, atomic JSONL writes).

What's DIFFERENT from the sampler, and why:

  1. EXHAUSTIVE, not sampled. For every leaf window, every single page
     (1..leaf.total_pages) is fetched and kept -- there is no PPS ranking,
     no --grand-total cap, no random subsampling of a partial last page.
     Every accepted record is written.

  2. Checkpoint is O(leaves), not O(notices). The sampler's checkpoint
     persists a `seen_ids` set per year for dedup safety -- fine at
     hundreds of thousands of records, but at 65.94M total notices that set
     alone would be gigabytes of JSON rewritten on every checkpoint save.
     Full-crawl dedup instead relies on deterministic exhaustive paging: a
     fixed (start_ms, end_ms) leaf window sorted by `date_received asc` with
     a fixed per_page returns the same page contents every time it's
     fetched, so each (leaf, page) is fetched at most once, ever, and the
     checkpoint only needs to store ONE INTEGER per leaf: how many of its
     pages are already done. Resuming leaf i means "start at page
     leaf_pages_done[i] + 1".
     Residual risk (same one flagged in stratified_sample.py's docstring,
     item #12): Lumen has no immutable snapshot/cursor, so if new notices
     are inserted *inside* an already-completed window while the crawl is
     running elsewhere, that's a real gap. Mitigation: closed/historical
     years are stable in practice; the current year should be crawled last
     and re-verified with a final drift-check pass (--verify-year) once the
     rest of the corpus is done.

  3. Bounded runtime per invocation (--max-runtime-seconds), returning a
     clear "COMPLETE" vs "MORE_WORK_REMAINING" exit status, because this is
     designed to run as a chain of bounded Modal Function calls (Modal's
     hard cap is 24h per call), not one single multi-day process. See
     modal_app.py for the chaining/orchestration layer.

Usage (local, for testing against a small slice before deploying to Modal):
    python full_crawl.py --start-year 2010 --end-year 2010 \
        --out-dir ./full_raw --state-dir ./full_state \
        --max-runtime-seconds 600

Usage (as imported by modal_app.py):
    from full_crawl import run_bounded, CrawlPaths
    status = run_bounded(paths, start_year=2010, end_year=2026,
                          max_runtime_seconds=21*3600)
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import signal
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import load_settings  # noqa: E402
from ingest_data import (  # noqa: E402
    AccessBlockedError,
    AuthenticationError,
    DateWindow,
    LargeQueryRefused,
    LumenClient,
    ResponseShapeError,
    RetryExhaustedHTTPError,
    UnsafeRedirectError,
    atomic_write_jsonl,
    prepare_records,
)
from stratified_sample import (  # noqa: E402
    LeafWindow,
    RequestBudget,
    WindowCache,
    split_year_into_leaves,
    year_bounds_ms,
)

LOGGER = logging.getLogger("full_crawl")

RECOVERABLE_PAGE_ERRORS = (LargeQueryRefused, ResponseShapeError, RetryExhaustedHTTPError)
FATAL_ERRORS = (AuthenticationError, AccessBlockedError, UnsafeRedirectError)

STATUS_COMPLETE = "COMPLETE"
STATUS_MORE_WORK = "MORE_WORK_REMAINING"
STATUS_FAILED = "FAILED"


class GracefulStop(Exception):
    pass


_STOP_REQUESTED = False


def _install_signal_handlers() -> None:
    def _handler(signum: int, _frame: Any) -> None:
        global _STOP_REQUESTED
        LOGGER.warning("Received signal %s; stopping at next safe checkpoint.", signum)
        _STOP_REQUESTED = True

    signal.signal(signal.SIGINT, _handler)
    try:
        signal.signal(signal.SIGTERM, _handler)
    except (ValueError, OSError):
        pass


@dataclass
class CrawlPaths:
    out_dir: Path
    state_dir: Path
    quarantine_dir: Path

    @classmethod
    def under(cls, root: Path) -> "CrawlPaths":
        return cls(
            out_dir=root / "notices",
            state_dir=root / "state",
            quarantine_dir=root / "quarantine",
        )

    def year_batch_path(self, year: int, run_id: str, batch_index: int) -> Path:
        """e.g. <out_dir>/2010/full_<run_id>_b0001.jsonl -- one subfolder per
        year, so the Volume (or local disk) reads as
        lumen-database/2010/..., lumen-database/2011/..., etc."""
        return self.out_dir / str(year) / f"full_{run_id}_b{batch_index:04d}.jsonl"

    def year_quarantine_path(self, year: int, run_id: str, leaf_index: int, page_num: int) -> Path:
        return self.quarantine_dir / str(year) / f"bad_{run_id}_leaf{leaf_index}_p{page_num}.jsonl"

    def ensure(self) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class YearState:
    year: int
    status: str = "pending"  # pending -> partitioning -> partitioned -> crawling -> done -> failed
    year_total: int = 0
    leaves: list[LeafWindow] = field(default_factory=list)
    pages_done: list[int] = field(default_factory=list)  # parallel to leaves
    fetched_records: int = 0
    quarantined_count: int = 0
    shortfalls: int = 0
    # Resumable mid-partition state (doc idea: "save partition checkpoints").
    # While status == "partitioning", these hold the in-progress worklist so
    # a crash/Ctrl-C loses at most `checkpoint_every` requests' worth of
    # partitioning work instead of the entire year having to restart from
    # month 1. `partition_stack` entries are (start_ms, end_ms, known_total)
    # tuples -- known_total is null in JSON when not yet probed.
    partition_stack: list[list[int | None]] = field(default_factory=list)
    partition_leaves_so_far: list[LeafWindow] = field(default_factory=list)
    partition_year_total_so_far: int = 0


class FullCrawlCheckpoint:
    """Same corruption-recovery pattern as the sampler's Checkpoint class
    (rotating .bak, atomic tmp-then-replace), but O(leaves) not O(notices)."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.bak_path = path.with_suffix(path.suffix + ".bak")
        self.data: dict[str, Any] = self._load_with_fallback()

    def _load_with_fallback(self) -> dict[str, Any]:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except (json.JSONDecodeError, OSError) as exc:
                LOGGER.error("Primary checkpoint corrupt (%s); trying backup.", exc)
        if self.bak_path.exists():
            try:
                data = json.loads(self.bak_path.read_text())
                LOGGER.warning("Recovered checkpoint from backup file.")
                return data
            except (json.JSONDecodeError, OSError) as exc:
                LOGGER.error("Backup checkpoint also unreadable: %s", exc)
        return {}

    def save(self, years: dict[int, YearState], cache: WindowCache, budget: RequestBudget, meta: dict[str, Any]) -> None:
        payload = {
            "meta": meta,
            "window_cache": cache.to_list(),
            "budget_used": budget.used,
            "years": {
                str(y): {
                    "year": s.year,
                    "status": s.status,
                    "year_total": s.year_total,
                    "leaves": [asdict(leaf) for leaf in s.leaves],
                    "pages_done": s.pages_done,
                    "fetched_records": s.fetched_records,
                    "quarantined_count": s.quarantined_count,
                    "shortfalls": s.shortfalls,
                    "partition_stack": s.partition_stack,
                    "partition_leaves_so_far": [asdict(leaf) for leaf in s.partition_leaves_so_far],
                    "partition_year_total_so_far": s.partition_year_total_so_far,
                }
                for y, s in years.items()
            },
        }
        if self.path.exists():
            try:
                shutil.copy2(self.path, self.bak_path)
            except OSError as exc:
                LOGGER.warning("Could not rotate checkpoint backup: %s", exc)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.replace(self.path)

    def load_years(self) -> dict[int, YearState]:
        out: dict[int, YearState] = {}
        for y_str, s in self.data.get("years", {}).items():
            out[int(y_str)] = YearState(
                year=s["year"],
                status=s.get("status", "pending"),
                year_total=s.get("year_total", 0),
                leaves=[LeafWindow(**leaf) for leaf in s.get("leaves", [])],
                pages_done=s.get("pages_done", []),
                fetched_records=s.get("fetched_records", 0),
                quarantined_count=s.get("quarantined_count", 0),
                shortfalls=s.get("shortfalls", 0),
                partition_stack=s.get("partition_stack", []),
                partition_leaves_so_far=[LeafWindow(**leaf) for leaf in s.get("partition_leaves_so_far", [])],
                partition_year_total_so_far=s.get("partition_year_total_so_far", 0),
            )
        return out

    def load_cache(self) -> WindowCache:
        return WindowCache.from_list(self.data.get("window_cache", []))

    def load_budget_used(self) -> int:
        return int(self.data.get("budget_used", 0))


def fetch_one_page(client: LumenClient, start_ms: int, end_ms: int, page_num: int) -> list[Any]:
    page = client.search_page(DateWindow(start_ms, end_ms), page=page_num)
    return page.notices


def run_bounded(
    paths: CrawlPaths,
    start_year: int,
    end_year: int,
    max_runtime_seconds: float,
    ceiling: int = 10_000,
    flush_every: int = 2_000,
    checkpoint_every_pages: int = 50,
    on_checkpoint: Any = None,
    progress_log_seconds: float = 20.0,
) -> str:
    """Run the exhaustive crawl for up to `max_runtime_seconds`, then return
    one of STATUS_COMPLETE / STATUS_MORE_WORK / STATUS_FAILED. Safe to call
    again immediately after a MORE_WORK_REMAINING return -- it resumes
    exactly where it left off via the on-disk checkpoint.

    `on_checkpoint`, if given, is called (no args) right after every
    checkpoint save -- e.g. pass a Modal Volume's `.commit` method so
    progress becomes durable/visible beyond the current container the
    moment it's saved, not just at the end of a multi-hour chunk."""

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    _install_signal_handlers()
    paths.ensure()

    settings = load_settings()
    client = LumenClient(settings)
    ckpt = FullCrawlCheckpoint(paths.state_dir / "full_crawl_checkpoint.json")

    years = ckpt.load_years()
    cache = ckpt.load_cache()

    partition_start = [time.monotonic()]

    def _log_partition_progress(requests_used: int) -> None:
        elapsed = time.monotonic() - partition_start[0]
        rate = requests_used / elapsed if elapsed > 0 else 0.0
        LOGGER.info(
            "Partitioning in progress: %d probe requests spent so far "
            "(%.2f req/sec, ~%.0f sec elapsed this year). Each request is a "
            "real network call spaced >=1.1s apart per Lumen's rate limit, "
            "so large years can legitimately take a while here before any "
            "notices start being fetched.",
            requests_used, rate, elapsed,
        )

    budget = RequestBudget(limit=None, progress_every=25, on_progress=_log_partition_progress)
    budget.used = ckpt.load_budget_used()

    for y in range(start_year, end_year + 1):
        if y not in years:
            years[y] = YearState(year=y)

    start_time = time.monotonic()

    def time_left() -> float:
        return max_runtime_seconds - (time.monotonic() - start_time)

    def save_all(extra_meta: dict[str, Any] | None = None) -> None:
        meta = {"phase": "full_crawl", "last_saved_utc": datetime.now(timezone.utc).isoformat()}
        if extra_meta:
            meta.update(extra_meta)
        ckpt.save(years, cache, budget, meta)
        if on_checkpoint is not None:
            try:
                on_checkpoint()
            except Exception as exc:  # noqa: BLE001 -- never let a commit failure crash the crawl
                LOGGER.warning("on_checkpoint callback failed (%s); continuing crawl regardless.", exc)

    try:
        for y in range(start_year, end_year + 1):
            state = years[y]
            if state.status == "done":
                continue
            if time_left() <= 0:
                LOGGER.info("Runtime budget exhausted before starting/resuming year %d.", y)
                save_all()
                return STATUS_MORE_WORK

            # --- Partition (resumable: a crash/Ctrl-C here loses at most
            # `checkpoint_every` requests' worth of work, not the whole
            # year -- see YearState.partition_stack).
            if state.status in ("pending", "partitioning"):
                LOGGER.info("Partitioning year %d into leaves (exhaustive mode)...", y)
                partition_start[0] = time.monotonic()

                def _save_partition_progress(leaves_so_far, stack_so_far, year_total_so_far, _year=y, _state=state):
                    _state.status = "partitioning"
                    _state.partition_stack = [list(item) for item in stack_so_far]
                    _state.partition_leaves_so_far = leaves_so_far
                    _state.partition_year_total_so_far = year_total_so_far
                    save_all({"phase": "full_crawl", "partitioning_year": _year})

                resume_kwargs: dict[str, Any] = {}
                if state.status == "partitioning" and state.partition_stack:
                    resume_kwargs = {
                        "resume_stack": [tuple(item) for item in state.partition_stack],
                        "resume_leaves": list(state.partition_leaves_so_far),
                        "resume_year_total": state.partition_year_total_so_far,
                    }

                leaves, year_total = split_year_into_leaves(
                    client, y, ceiling, cache, budget,
                    checkpoint_cb=_save_partition_progress,
                    checkpoint_every=100,
                    **resume_kwargs,
                )
                state.leaves = leaves
                state.year_total = year_total
                state.pages_done = [0] * len(leaves)
                state.partition_stack = []
                state.partition_leaves_so_far = []
                state.partition_year_total_so_far = 0
                state.status = "partitioned"
                save_all()
                LOGGER.info("Year %d: %d leaves, %d total notices to fetch.", y, len(leaves), year_total)

            state.status = "crawling"
            run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            batch_records: list[Any] = []
            batch_index = 0
            pages_since_checkpoint = 0
            last_progress_log = time.monotonic()
            pages_since_progress_log = 0

            for leaf_index, leaf in enumerate(state.leaves):
                pages_already_done = state.pages_done[leaf_index]
                if pages_already_done >= leaf.total_pages:
                    continue  # this leaf was fully fetched in a prior run

                for page_num in range(pages_already_done + 1, leaf.total_pages + 1):
                    if _STOP_REQUESTED:
                        if batch_records:
                            batch_index += 1
                            out_path = paths.year_batch_path(y, run_id, batch_index)
                            atomic_write_jsonl(out_path, batch_records, fsync=settings.fsync_writes)
                        save_all({"stop_reason": "signal"})
                        return STATUS_MORE_WORK

                    if time_left() <= 0:
                        if batch_records:
                            batch_index += 1
                            out_path = paths.year_batch_path(y, run_id, batch_index)
                            atomic_write_jsonl(out_path, batch_records, fsync=settings.fsync_writes)
                        save_all({"stop_reason": "runtime_budget"})
                        return STATUS_MORE_WORK

                    try:
                        notices = fetch_one_page(client, leaf.start_ms, leaf.end_ms, page_num)
                    except RECOVERABLE_PAGE_ERRORS as exc:
                        LOGGER.warning(
                            "Year %d leaf %d page %d failed permanently (%s); "
                            "recording as a shortfall and moving on (a gap here "
                            "means re-running --verify-year %d later).",
                            y, leaf_index, page_num, exc, y,
                        )
                        state.shortfalls += 1
                        state.pages_done[leaf_index] = page_num
                        continue
                    except FATAL_ERRORS as exc:
                        LOGGER.error("Fatal error crawling year %d: %s", y, exc)
                        if batch_records:
                            batch_index += 1
                            out_path = paths.year_batch_path(y, run_id, batch_index)
                            atomic_write_jsonl(out_path, batch_records, fsync=settings.fsync_writes)
                        state.status = "failed"
                        save_all({"stop_reason": "fatal_error", "error": str(exc)})
                        return STATUS_FAILED

                    accepted, quarantined = prepare_records(
                        records=notices,
                        endpoint=settings.search_path,
                        window=DateWindow(leaf.start_ms, leaf.end_ms),
                        page=page_num,
                    )
                    if quarantined:
                        state.quarantined_count += len(quarantined)
                        atomic_write_jsonl(
                            paths.year_quarantine_path(y, run_id, leaf_index, page_num),
                            quarantined,
                            fsync=settings.fsync_writes,
                        )

                    envelopes = [env for _rid, env in accepted]
                    batch_records.extend(envelopes)
                    state.fetched_records += len(envelopes)
                    state.pages_done[leaf_index] = page_num
                    pages_since_checkpoint += 1
                    pages_since_progress_log += 1

                    now = time.monotonic()
                    elapsed_since_log = now - last_progress_log
                    if elapsed_since_log >= progress_log_seconds:
                        rate = pages_since_progress_log / elapsed_since_log
                        pct = (state.fetched_records / state.year_total * 100) if state.year_total else 0.0
                        remaining_records = max(0, state.year_total - state.fetched_records)
                        eta_str = ""
                        if rate > 0:
                            # rough: assume similar records/page going forward
                            avg_records_per_page = max(1, state.fetched_records / max(1, sum(state.pages_done)))
                            remaining_pages = remaining_records / avg_records_per_page if avg_records_per_page else 0
                            eta_seconds = remaining_pages / rate
                            eta_str = f", ETA for this year ~{eta_seconds / 60:.0f} min"
                        LOGGER.info(
                            "Year %d progress: %d/%d records (%.1f%%), leaf %d/%d, page %d/%d "
                            "(%.2f pages/sec%s)",
                            y, state.fetched_records, state.year_total, pct,
                            leaf_index + 1, len(state.leaves), page_num, leaf.total_pages,
                            rate, eta_str,
                        )
                        last_progress_log = now
                        pages_since_progress_log = 0

                    if len(batch_records) >= flush_every:
                        batch_index += 1
                        out_path = paths.year_batch_path(y, run_id, batch_index)
                        atomic_write_jsonl(out_path, batch_records, fsync=settings.fsync_writes)
                        batch_records = []

                    if pages_since_checkpoint >= checkpoint_every_pages:
                        save_all()
                        pages_since_checkpoint = 0

            if batch_records:
                batch_index += 1
                out_path = paths.year_batch_path(y, run_id, batch_index)
                atomic_write_jsonl(out_path, batch_records, fsync=settings.fsync_writes)

            state.status = "done"
            save_all()
            LOGGER.info(
                "Year %d COMPLETE: fetched=%d quarantined=%d shortfalls=%d (of %d total).",
                y, state.fetched_records, state.quarantined_count, state.shortfalls, state.year_total,
            )

        return STATUS_COMPLETE

    except GracefulStop:
        save_all({"stop_reason": "graceful_stop"})
        return STATUS_MORE_WORK


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start-year", type=int, default=2010)
    p.add_argument("--end-year", type=int, default=2026)
    p.add_argument("--out-dir", default="full_raw")
    p.add_argument("--state-dir", default="full_state")
    p.add_argument("--max-runtime-seconds", type=float, default=3600.0)
    p.add_argument("--flush-every", type=int, default=2000)
    p.add_argument("--checkpoint-every-pages", type=int, default=50)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root_out = Path(args.out_dir)
    paths = CrawlPaths(
        out_dir=root_out,
        state_dir=Path(args.state_dir),
        quarantine_dir=root_out / "quarantine",
    )
    status = run_bounded(
        paths,
        start_year=args.start_year,
        end_year=args.end_year,
        max_runtime_seconds=args.max_runtime_seconds,
        flush_every=args.flush_every,
        checkpoint_every_pages=args.checkpoint_every_pages,
    )
    LOGGER.info("Run finished with status: %s", status)
    return {"COMPLETE": 0, "MORE_WORK_REMAINING": 0, "FAILED": 1}[status]


if __name__ == "__main__":
    sys.exit(main())