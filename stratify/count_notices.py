"""
full_metadata_scan.py -- Reconnaissance pass across 2010-2026 (hardcoded):

  1. Per-YEAR start/end notice ID (first and last notice chronologically
     within that calendar year, by date_received).
  2. Per-MONTH notice counts (12 requests/year).
  3. Per-DAY notice counts (up to 366 requests/year).

This is a METADATA/COUNTING pass only -- it does not fetch or store actual
notice content, just counts and boundary IDs. Total cost across all 17
years is roughly ~6,400-6,500 requests (~2 hours at 1.1s/request), which is
why this is a separate script from the full corpus crawl (that one fetches
every record and takes ~9 days).

WHY START/END ID PER YEAR IS CHEAP HERE (2 requests/year, not a full scan):
  - START id: query the whole year as one window, sort ASCENDING
    (date_received asc, the default), page=1 -- the very first record
    returned IS the chronologically-first notice of the year, regardless
    of how many total notices that year has. Elasticsearch's 10,000-result
    reporting ceiling caps the *total_entries count*, not page-1 content,
    so this is accurate even for a 28-million-notice year.
  - END id: same idea, but sort DESCENDING, page=1 -- the first record
    returned is the chronologically-LAST notice of the year. This avoids
    ever needing to page deep into a multi-million-record result set.
  - Both use the SAME LumenClient instance (just with sort_by swapped via
    the `temporary_sort_order` context manager below), so the built-in
    rate limiter's spacing state stays shared and correct -- there's no
    risk of two requests landing in the same second just because we're
    juggling two "logical" query directions.

IMPORTANT CAVEAT ON THESE START/END IDs (carried over from earlier in this
project): notice IDs are NOT reliably monotonic with date_received for the
early years (2010-2013 showed overlapping ID ranges across different
years, most likely from a bulk historical migration). Treat start_id/end_id
as DESCRIPTIVE METADATA for your records, not as a partition key for
crawling -- the actual crawl (full_crawl.py) partitions by date window
only, never by ID range, precisely because of this.

Every count in the output is either a real number or an explicit
{"error": "..."} -- never a silent 0. Any total_entries that lands exactly
on 10,000 is flagged as `"possibly_capped": true`, since Elasticsearch's
reporting ceiling means the true count could be higher than what's shown
(this happens routinely at DAY granularity during 2023-2025's high-volume
months, e.g. the ~9.9M-notice month in 2025 averages ~320K/day).

Usage:
    python full_metadata_scan.py                  # everything, 2010-2026
    python full_metadata_scan.py --skip-days       # just start/end id + months
    python full_metadata_scan.py --only-year 2025  # just one year, all levels
    python full_metadata_scan.py --only-year 2025 --only-month 8  # one hot month, day-level

Safe to interrupt (Ctrl+C) and re-run -- every already-successful request is
cached in --out and skipped on the next run; only errors and not-yet-done
items are retried/continued.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import json
import sys
from datetime import datetime, timedelta, timezone
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
)
from stratified_sample import month_bounds_ms, year_bounds_ms  # noqa: E402

RECOVERABLE = (LargeQueryRefused, ResponseShapeError, RetryExhaustedHTTPError)
FATAL = (AuthenticationError, AccessBlockedError, UnsafeRedirectError)

# Hardcoded per your request -- change here if you ever need a different span.
YEARS = list(range(2024, 2027))  # 2010..2026 inclusive
CEILING = 20_000


def day_bounds_ms(year: int, month: int, day: int) -> tuple[int, int]:
    start = datetime(year, month, day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def days_in_month(year: int, month: int) -> int:
    nxt = datetime(year + 1, 1, 1, tzinfo=timezone.utc) if month == 12 else datetime(year, month + 1, 1, tzinfo=timezone.utc)
    this = datetime(year, month, 1, tzinfo=timezone.utc)
    return (nxt - this).days


@contextlib.contextmanager
def temporary_sort_order(client: LumenClient, sort_by: str):
    """Temporarily swap client.settings.sort_by (a frozen dataclass field)
    for one call, then restore it -- always, even on error. Uses the SAME
    client instance throughout so the rate limiter's spacing state is never
    split across two objects."""
    original = client.settings
    object.__setattr__(client, "settings", dataclasses.replace(original, sort_by=sort_by))
    try:
        yield
    finally:
        object.__setattr__(client, "settings", original)


def probe_count(client: LumenClient, start_ms: int, end_ms: int) -> dict[str, Any]:
    try:
        page = client.search_page(DateWindow(start_ms, end_ms), page=1)
        result: dict[str, Any] = {"total_entries": page.total_entries}
        if page.total_entries == CEILING:
            result["possibly_capped"] = True
        return result
    except RECOVERABLE as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}
    except FATAL:
        raise


def probe_boundary_id(client: LumenClient, start_ms: int, end_ms: int, *, first: bool) -> dict[str, Any]:
    """first=True -> first (earliest) notice id in the window (ascending).
    first=False -> last (latest) notice id in the window (descending)."""
    try:
        if first:
            page = client.search_page(DateWindow(start_ms, end_ms), page=1)
        else:
            with temporary_sort_order(client, "date_received desc"):
                page = client.search_page(DateWindow(start_ms, end_ms), page=1)

        if not page.notices:
            return {"id": None, "note": "window has zero notices"}
        record = page.notices[0]
        record_id = record.get("id") if isinstance(record, dict) else None
        if record_id is None:
            return {"error": "first record in response has no 'id' field"}
        return {"id": record_id}
    except RECOVERABLE as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}
    except FATAL:
        raise


def load_existing(out_path: Path) -> dict[str, Any]:
    if out_path.exists():
        try:
            return json.loads(out_path.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save(out_path: Path, data: dict[str, Any]) -> None:
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(out_path)


def is_done(entry: Any) -> bool:
    return isinstance(entry, dict) and ("error" not in entry)


def scan_year_boundaries(client: LumenClient, year: int, year_data: dict[str, Any], out_path: Path, all_data: dict[str, Any]) -> None:
    start_ms, end_ms = year_bounds_ms(year)

    existing_start = year_data.get("start")
    if not is_done(existing_start):
        result = probe_boundary_id(client, start_ms, end_ms, first=True)
        year_data["start"] = result
        save(out_path, all_data)
        print(f"  [{year}] start_id: {result}")
    else:
        print(f"  [{year}] start_id: {existing_start} (cached, skipped)")

    existing_end = year_data.get("end")
    if not is_done(existing_end):
        result = probe_boundary_id(client, start_ms, end_ms, first=False)
        year_data["end"] = result
        save(out_path, all_data)
        print(f"  [{year}] end_id:   {result}")
    else:
        print(f"  [{year}] end_id:   {existing_end} (cached, skipped)")


def scan_months(client: LumenClient, year: int, year_data: dict[str, Any], out_path: Path, all_data: dict[str, Any]) -> None:
    months = year_data.setdefault("months", {})
    for month in range(1, 13):
        key = f"{month:02d}"
        existing = months.get(key)
        if is_done(existing):
            print(f"  [{year}-{key}] {existing.get('total_entries', '?'):>12,}  (cached)"
                  if "total_entries" in existing else f"  [{year}-{key}] cached: {existing}")
            continue
        start, end = month_bounds_ms(year, month)
        result = probe_count(client, start, end)
        months[key] = result
        save(out_path, all_data)
        if "error" in result:
            print(f"  [{year}-{key}] ERROR -- {result['error']}")
        else:
            cap_flag = " (POSSIBLY CAPPED)" if result.get("possibly_capped") else ""
            print(f"  [{year}-{key}] {result['total_entries']:>12,}{cap_flag}")


def scan_days(
    client: LumenClient,
    year: int,
    year_data: dict[str, Any],
    out_path: Path,
    all_data: dict[str, Any],
    only_month: int | None,
) -> None:
    days = year_data.setdefault("days", {})
    target_months = [only_month] if only_month else list(range(1, 13))
    for month in target_months:
        mkey = f"{month:02d}"
        month_days = days.setdefault(mkey, {})
        n_days = days_in_month(year, month)
        for day in range(1, n_days + 1):
            dkey = f"{day:02d}"
            existing = month_days.get(dkey)
            if is_done(existing):
                continue
            start, end = day_bounds_ms(year, month, day)
            result = probe_count(client, start, end)
            month_days[dkey] = result
            save(out_path, all_data)
            if "error" in result:
                print(f"    [{year}-{mkey}-{dkey}] ERROR -- {result['error']}")
            else:
                cap_flag = " (POSSIBLY CAPPED)" if result.get("possibly_capped") else ""
                print(f"    [{year}-{mkey}-{dkey}] {result['total_entries']:>10,}{cap_flag}")

        day_total = sum(r["total_entries"] for r in month_days.values() if "total_entries" in r)
        errored = [k for k, r in month_days.items() if "error" in r]
        print(f"  --- {year}-{mkey} day-sum: {day_total:,} ---" + (f"  [{len(errored)} error(s): {errored}]" if errored else ""))


def print_final_summary(all_data: dict[str, Any]) -> None:
    print("\n" + "=" * 90)
    print("FINAL SUMMARY")
    print("=" * 90)
    header = f"{'Year':<6}{'Start ID':<14}{'End ID':<14}{'Month-sum':<16}{'Day-sum':<16}{'Match?':<8}"
    print(header)
    print("-" * 90)
    for year in YEARS:
        y = all_data.get(str(year), {})
        start_id = (y.get("start") or {}).get("id", "-")
        end_id = (y.get("end") or {}).get("id", "-")
        months = y.get("months", {})
        month_sum = sum(r["total_entries"] for r in months.values() if "total_entries" in r)
        days = y.get("days", {})
        day_sum = 0
        for mdays in days.values():
            day_sum += sum(r["total_entries"] for r in mdays.values() if "total_entries" in r)
        match = "n/a" if day_sum == 0 else ("YES" if month_sum == day_sum else "MISMATCH")
        print(f"{year:<6}{str(start_id):<14}{str(end_id):<14}{month_sum:<16,}{day_sum:<16,}{match:<8}")
    print("=" * 90)
    print("Note: 'MISMATCH' can mean either a real discrepancy worth investigating, or")
    print("that day-level scanning simply hasn't been run for that year yet.")
    print("Any 'possibly_capped' flags in the raw JSON mean the true count may be")
    print("higher than shown -- Elasticsearch's 10,000 reporting ceiling applies at")
    print("whatever granularity you queried, including single days during hot periods.")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", default="full_metadata_scan.json")
    p.add_argument("--skip-boundaries", action="store_true", help="Skip per-year start/end id lookup")
    p.add_argument("--skip-months", action="store_true", help="Skip per-month counts")
    p.add_argument("--skip-days", action="store_true", help="Skip per-day counts (the expensive part)")
    p.add_argument("--only-year", type=int, default=None, help="Restrict to a single year (still 2010-2026 hardcoded universe otherwise)")
    p.add_argument("--only-month", type=int, default=None, help="Restrict day-level scan to a single month (1-12); requires --only-year")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.only_month and not args.only_year:
        print("--only-month requires --only-year", file=sys.stderr)
        return 2

    out_path = Path(args.out)
    all_data = load_existing(out_path)

    settings = load_settings()
    client = LumenClient(settings)

    years_to_scan = [args.only_year] if args.only_year else YEARS

    try:
        for year in years_to_scan:
            print(f"\n########## YEAR {year} ##########")
            year_key = str(year)
            year_data = all_data.setdefault(year_key, {})

            if not args.skip_boundaries:
                print(f"-- start/end notice id --")
                scan_year_boundaries(client, year, year_data, out_path, all_data)

            if not args.skip_months:
                print(f"-- per-month counts --")
                scan_months(client, year, year_data, out_path, all_data)

            if not args.skip_days:
                print(f"-- per-day counts --")
                scan_days(client, year, year_data, out_path, all_data, args.only_month)

    except FATAL as exc:
        print(f"\nFATAL: {exc}\nStopping immediately -- do not keep probing with a broken/banned token.", file=sys.stderr)
        save(out_path, all_data)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted -- progress saved, re-run the same command to resume.")
        save(out_path, all_data)
        return 130

    print_final_summary(all_data)
    print(f"\nFull results saved to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())