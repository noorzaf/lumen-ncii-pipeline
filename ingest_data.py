#!/usr/bin/env python3
"""Polite, resumable JSONL ingestion for the Lumen research API.

The notice exporter uses the documented page-based search endpoint and
``date_submitted`` millisecond ranges. It snapshots a fixed upper bound, splits
ranges that exceed Lumen's 10,000-result search ceiling, streams one atomic
JSONL batch per response page, and stores durable progress in checkpoint.json
plus SQLite.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import logging
import os
import random
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence
from urllib.parse import urlparse

import requests

from config import ConfigurationError, Settings, load_settings

LOGGER = logging.getLogger("lumen_ingestion")
CHECKPOINT_VERSION = 3
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_BACKOFF_SECONDS = 300.0
ONE_DAY_MS = 24 * 60 * 60 * 1000
ONE_HOUR_MS = 60 * 60 * 1000
API_ATTRIBUTION_NOTICE = (
    "This product uses the Lumen API but is not endorsed or certified by Lumen."
)


class IngestionError(RuntimeError):
    """Base ingestion failure."""


class AuthenticationError(IngestionError):
    """The configured token was rejected or not applied correctly."""


class AccessBlockedError(IngestionError):
    """Lumen appears to have blocked the client or source IP."""


class UnsafeRedirectError(IngestionError):
    """A redirect attempted to leave the configured Lumen origin."""


class ResponseShapeError(IngestionError):
    """The server response did not match the documented JSON shape."""


class LargeQueryRefused(IngestionError):
    """Lumen refused an oversized search that should be subdivided."""


class RetryExhaustedHTTPError(IngestionError):
    """A retryable response persisted after all retries."""


class ConcurrentRunError(IngestionError):
    """Another process appears to own the same local state directory."""


class UnsplittableWindowError(IngestionError):
    """A one-millisecond range still exceeds the 10,000-result ceiling."""


class SnapshotDriftError(IngestionError):
    """Search totals changed while paging through a fixed range."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat(timespec="seconds").replace("+00:00", "Z")


def epoch_ms_now() -> int:
    return int(time.time() * 1000)


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _fsync_directory(directory: Path) -> None:
    try:
        fd = os.open(directory, os.O_RDONLY)
    except (AttributeError, OSError):
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_text(path: Path, text: str, *, fsync: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with contextlib.suppress(OSError):
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            if fsync:
                os.fsync(handle.fileno())
        os.replace(temporary, path)
        if fsync:
            _fsync_directory(path.parent)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def atomic_write_json(path: Path, value: dict[str, Any], *, fsync: bool) -> None:
    atomic_write_text(
        path,
        json.dumps(value, indent=2, ensure_ascii=False) + "\n",
        fsync=fsync,
    )


def atomic_write_jsonl(
    path: Path, records: Iterable[dict[str, Any]], *, fsync: bool
) -> int:
    """Write records line-by-line to a temporary file and atomically rename it."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    count = 0
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with contextlib.suppress(OSError):
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            for record in records:
                handle.write(compact_json(record))
                handle.write("\n")
                count += 1
            handle.flush()
            if fsync:
                os.fsync(handle.fileno())
        os.replace(temporary, path)
        if fsync:
            _fsync_directory(path.parent)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
    return count


def parse_optional_int(value: Any, *, field: str) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ResponseShapeError(f"Response field {field!r} is not an integer")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ResponseShapeError(
            f"Response field {field!r} is not an integer: {value!r}"
        ) from exc


def chunks(values: Sequence[str], size: int = 800) -> Iterator[Sequence[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


@dataclass(frozen=True, slots=True)
class DateWindow:
    start_ms: int
    end_ms: int

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "DateWindow":
        return cls(start_ms=int(value["start_ms"]), end_ms=int(value["end_ms"]))

    def to_mapping(self) -> dict[str, int]:
        return {"start_ms": self.start_ms, "end_ms": self.end_ms}

    @property
    def query_value(self) -> str:
        return f"{self.start_ms}..{self.end_ms}"

    def split_with_boundary_overlap(self) -> tuple["DateWindow", "DateWindow"]:
        """Split an undocumented-inclusive range while retaining one boundary overlap."""

        if self.start_ms >= self.end_ms:
            raise UnsplittableWindowError(
                f"Cannot split point window {self.start_ms}..{self.end_ms}"
            )
        if self.end_ms - self.start_ms == 1:
            return (
                DateWindow(self.start_ms, self.start_ms),
                DateWindow(self.end_ms, self.end_ms),
            )
        midpoint = (self.start_ms + self.end_ms) // 2
        return (
            DateWindow(self.start_ms, midpoint),
            DateWindow(midpoint, self.end_ms),
        )


@dataclass(frozen=True, slots=True)
class SearchPage:
    notices: list[Any]
    current_page: int
    next_page: int | None
    previous_page: int | None
    per_page: int
    total_entries: int
    total_pages: int
    request_params: dict[str, Any]


@dataclass(frozen=True, slots=True)
class CompletedPage:
    next_page: int | None
    total_entries: int
    total_pages: int
    response_count: int
    accepted_count: int
    quarantined_count: int
    batch_path: str


class ExclusiveRunLock:
    """Single-writer lock based on atomic file creation."""

    def __init__(self, path: Path, *, force: bool = False) -> None:
        self.path = path
        self.force = force
        self._held = False

    def __enter__(self) -> "ExclusiveRunLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.force:
            with contextlib.suppress(FileNotFoundError):
                self.path.unlink()
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            owner = "unknown"
            with contextlib.suppress(OSError):
                owner = self.path.read_text(encoding="utf-8").strip() or "unknown"
            raise ConcurrentRunError(
                f"Lock file {self.path} already exists ({owner}). Use --force-lock only after verifying the previous process is not running."
            ) from exc
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(f"pid={os.getpid()} started_at={utc_now_iso()}\n")
            handle.flush()
            os.fsync(handle.fileno())
        self._held = True
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self._held:
            with contextlib.suppress(FileNotFoundError):
                self.path.unlink()
            self._held = False


class CheckpointManager:
    def __init__(self, settings: Settings) -> None:
        self.path = settings.checkpoint_path
        self.fsync = settings.fsync_writes

    def load(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise IngestionError(f"Could not read checkpoint {self.path}: {exc}") from exc
        if not isinstance(value, dict):
            raise IngestionError(f"Checkpoint {self.path} is not a JSON object")
        if value.get("version") != CHECKPOINT_VERSION:
            raise IngestionError(
                f"Unsupported checkpoint version {value.get('version')!r}; expected {CHECKPOINT_VERSION}"
            )
        return value

    def save(self, checkpoint: dict[str, Any]) -> None:
        checkpoint["updated_at_utc"] = utc_now_iso()
        atomic_write_json(self.path, checkpoint, fsync=self.fsync)


class StateStore:
    """SQLite-backed deduplication and exact page completion state."""

    def __init__(self, settings: Settings) -> None:
        settings.state_dir.mkdir(parents=True, exist_ok=True)
        self.settings = settings
        self.connection = sqlite3.connect(settings.sqlite_path, timeout=60.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self._create_schema()
        self.export_quarantine()

    def __enter__(self) -> "StateStore":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.connection.close()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS seen_records (
                record_id TEXT PRIMARY KEY,
                first_seen_at_utc TEXT NOT NULL,
                batch_path TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS completed_pages (
                window_start_ms INTEGER NOT NULL,
                window_end_ms INTEGER NOT NULL,
                page INTEGER NOT NULL,
                next_page INTEGER,
                total_entries INTEGER NOT NULL,
                total_pages INTEGER NOT NULL,
                response_count INTEGER NOT NULL,
                accepted_count INTEGER NOT NULL,
                quarantined_count INTEGER NOT NULL,
                batch_path TEXT NOT NULL,
                completed_at_utc TEXT NOT NULL,
                PRIMARY KEY (window_start_ms, window_end_ms, page)
            );

            CREATE TABLE IF NOT EXISTS window_record_ids (
                window_start_ms INTEGER NOT NULL,
                window_end_ms INTEGER NOT NULL,
                record_id TEXT NOT NULL,
                first_page INTEGER NOT NULL,
                PRIMARY KEY (window_start_ms, window_end_ms, record_id)
            );

            CREATE TABLE IF NOT EXISTS quarantine_records (
                quarantine_key TEXT PRIMARY KEY,
                quarantined_at_utc TEXT NOT NULL,
                source_endpoint TEXT NOT NULL,
                error_reason TEXT NOT NULL,
                record_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS artifacts (
                artifact_key TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                record_count INTEGER NOT NULL,
                completed_at_utc TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_completed_pages_window
              ON completed_pages(window_start_ms, window_end_ms, page);

            CREATE INDEX IF NOT EXISTS idx_window_record_ids_window
              ON window_record_ids(window_start_ms, window_end_ms);
            """
        )
        # Migration from the first package version, if somebody reuses state.
        columns = {
            row["name"]
            for row in self.connection.execute("PRAGMA table_info(completed_pages)")
        }
        if "response_count" not in columns:
            self.connection.execute(
                "ALTER TABLE completed_pages ADD COLUMN response_count INTEGER NOT NULL DEFAULT 0"
            )
        self.connection.commit()

    def completed_page(self, window: DateWindow, page: int) -> CompletedPage | None:
        row = self.connection.execute(
            """
            SELECT next_page, total_entries, total_pages, response_count,
                   accepted_count, quarantined_count, batch_path
            FROM completed_pages
            WHERE window_start_ms = ? AND window_end_ms = ? AND page = ?
            """,
            (window.start_ms, window.end_ms, page),
        ).fetchone()
        if row is None:
            return None
        return CompletedPage(
            next_page=row["next_page"],
            total_entries=int(row["total_entries"]),
            total_pages=int(row["total_pages"]),
            response_count=int(row["response_count"]),
            accepted_count=int(row["accepted_count"]),
            quarantined_count=int(row["quarantined_count"]),
            batch_path=str(row["batch_path"]),
        )

    def _seen_ids(self, record_ids: Sequence[str]) -> set[str]:
        if not record_ids:
            return set()
        found: set[str] = set()
        for group in chunks(record_ids):
            placeholders = ",".join("?" for _ in group)
            rows = self.connection.execute(
                f"SELECT record_id FROM seen_records WHERE record_id IN ({placeholders})",
                tuple(group),
            ).fetchall()
            found.update(str(row["record_id"]) for row in rows)
        return found

    def commit_page(
        self,
        *,
        run_id: str,
        window: DateWindow,
        page: SearchPage,
        accepted: list[tuple[str, dict[str, Any]]],
        quarantined: list[dict[str, Any]],
    ) -> CompletedPage:
        existing = self.completed_page(window, page.current_page)
        if existing is not None:
            return existing

        ordered_unique: list[tuple[str, dict[str, Any]]] = []
        local_ids: set[str] = set()
        for record_id, envelope in accepted:
            if record_id in local_ids:
                continue
            local_ids.add(record_id)
            ordered_unique.append((record_id, envelope))

        already_seen = self._seen_ids([record_id for record_id, _ in ordered_unique])
        new_records = [item for item in ordered_unique if item[0] not in already_seen]

        filename = (
            f"data_batch_{run_id}_s{window.start_ms}_e{window.end_ms}"
            f"_p{page.current_page:06d}.jsonl"
        )
        batch_path = self.settings.raw_dir / filename
        atomic_write_jsonl(
            batch_path,
            (envelope for _, envelope in new_records),
            fsync=self.settings.fsync_writes,
        )

        completed_at = utc_now_iso()
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO seen_records(record_id, first_seen_at_utc, batch_path)
                VALUES (?, ?, ?)
                """,
                [
                    (record_id, completed_at, str(batch_path))
                    for record_id, _ in new_records
                ],
            )
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO window_record_ids(
                    window_start_ms, window_end_ms, record_id, first_page
                ) VALUES (?, ?, ?, ?)
                """,
                [
                    (
                        window.start_ms,
                        window.end_ms,
                        record_id,
                        page.current_page,
                    )
                    for record_id, _ in ordered_unique
                ],
            )
            for item in quarantined:
                serialized = compact_json(item)
                identity = compact_json(
                    {
                        "source_endpoint": item["source_endpoint"],
                        "source_window": item.get("source_window"),
                        "source_page": item.get("source_page"),
                        "source_index": item.get("source_index"),
                        "error_reason": item["error_reason"],
                        "record_payload": item["record_payload"],
                    }
                )
                quarantine_key = hashlib.sha256(identity.encode("utf-8")).hexdigest()
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO quarantine_records(
                        quarantine_key, quarantined_at_utc, source_endpoint,
                        error_reason, record_json
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        quarantine_key,
                        item["quarantined_at_utc"],
                        item["source_endpoint"],
                        item["error_reason"],
                        serialized,
                    ),
                )
            self.connection.execute(
                """
                INSERT INTO completed_pages(
                    window_start_ms, window_end_ms, page, next_page,
                    total_entries, total_pages, response_count, accepted_count,
                    quarantined_count, batch_path, completed_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    window.start_ms,
                    window.end_ms,
                    page.current_page,
                    page.next_page,
                    page.total_entries,
                    page.total_pages,
                    len(page.notices),
                    len(new_records),
                    len(quarantined),
                    str(batch_path),
                    completed_at,
                ),
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            with contextlib.suppress(FileNotFoundError):
                batch_path.unlink()
            raise

        if quarantined:
            self.export_quarantine()

        return CompletedPage(
            next_page=page.next_page,
            total_entries=page.total_entries,
            total_pages=page.total_pages,
            response_count=len(page.notices),
            accepted_count=len(new_records),
            quarantined_count=len(quarantined),
            batch_path=str(batch_path),
        )

    def window_response_count(self, window: DateWindow) -> int:
        row = self.connection.execute(
            """
            SELECT COALESCE(SUM(response_count), 0)
            FROM completed_pages
            WHERE window_start_ms = ? AND window_end_ms = ?
            """,
            (window.start_ms, window.end_ms),
        ).fetchone()
        return int(row[0])

    def window_unique_valid_count(self, window: DateWindow) -> int:
        row = self.connection.execute(
            """
            SELECT COUNT(*)
            FROM window_record_ids
            WHERE window_start_ms = ? AND window_end_ms = ?
            """,
            (window.start_ms, window.end_ms),
        ).fetchone()
        return int(row[0])

    def window_quarantine_occurrences(self, window: DateWindow) -> int:
        row = self.connection.execute(
            """
            SELECT COALESCE(SUM(quarantined_count), 0)
            FROM completed_pages
            WHERE window_start_ms = ? AND window_end_ms = ?
            """,
            (window.start_ms, window.end_ms),
        ).fetchone()
        return int(row[0])

    def artifact(self, key: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT path, record_count, completed_at_utc FROM artifacts WHERE artifact_key = ?",
            (key,),
        ).fetchone()

    def register_artifact(self, key: str, path: Path, record_count: int) -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO artifacts(artifact_key, path, record_count, completed_at_utc)
            VALUES (?, ?, ?, ?)
            """,
            (key, str(path), record_count, utc_now_iso()),
        )
        self.connection.commit()

    def export_quarantine(self) -> None:
        rows = self.connection.execute(
            "SELECT record_json FROM quarantine_records ORDER BY rowid"
        ).fetchall()
        path = self.settings.quarantine_dir / "bad_records.jsonl"
        atomic_write_text(
            path,
            "".join(f"{row['record_json']}\n" for row in rows),
            fsync=self.settings.fsync_writes,
        )

    def totals(self) -> dict[str, int]:
        return {
            "records_written": int(
                self.connection.execute("SELECT COUNT(*) FROM seen_records").fetchone()[0]
            ),
            "quarantined_records": int(
                self.connection.execute(
                    "SELECT COUNT(*) FROM quarantine_records"
                ).fetchone()[0]
            ),
            "batches_completed": int(
                self.connection.execute(
                    "SELECT COUNT(*) FROM completed_pages"
                ).fetchone()[0]
            ),
        }


class TokenSafeSession(requests.Session):
    """Keep the custom token only on the configured Lumen host/www alias."""

    def __init__(self, *, api_key: str, allowed_hosts: set[str]) -> None:
        super().__init__()
        self.api_key = api_key
        self.allowed_hosts = {host.lower() for host in allowed_hosts if host}

    def rebuild_auth(
        self, prepared_request: requests.PreparedRequest, response: requests.Response
    ) -> None:
        super().rebuild_auth(prepared_request, response)
        new_host = (urlparse(prepared_request.url or "").hostname or "").lower()
        if new_host in self.allowed_hosts:
            prepared_request.headers["X-Authentication-Token"] = self.api_key
        else:
            prepared_request.headers.pop("X-Authentication-Token", None)


class LumenClient:
    """Requests client with one-second spacing, retry/backoff, and header auditing."""

    def __init__(self, settings: Settings, session: requests.Session | None = None) -> None:
        self.settings = settings
        configured_host = (urlparse(settings.api_base_url).hostname or "").lower()
        allowed_hosts = {configured_host}
        if configured_host == "lumendatabase.org":
            allowed_hosts.add("www.lumendatabase.org")
        elif configured_host == "www.lumendatabase.org":
            allowed_hosts.add("lumendatabase.org")
        self.allowed_hosts = allowed_hosts
        self.session = session or TokenSafeSession(
            api_key=settings.api_key, allowed_hosts=allowed_hosts
        )
        self.session.headers.update(
            {
                "X-Authentication-Token": settings.api_key,
                "User-Agent": settings.user_agent,
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
            }
        )
        self._last_request_started: float | None = None

    def __enter__(self) -> "LumenClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.session.close()

    def _respect_spacing(self) -> None:
        if self._last_request_started is not None:
            remaining = self.settings.request_delay_seconds - (
                time.monotonic() - self._last_request_started
            )
            if remaining > 0:
                time.sleep(remaining)
        self._last_request_started = time.monotonic()

    @staticmethod
    def _parse_retry_after(value: str | None) -> float | None:
        if not value:
            return None
        value = value.strip()
        try:
            return max(0.0, float(value))
        except ValueError:
            pass
        try:
            target = parsedate_to_datetime(value)
            if target.tzinfo is None:
                target = target.replace(tzinfo=timezone.utc)
            return max(0.0, (target - utc_now()).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None

    @staticmethod
    def _parse_reset_delay(value: str | None) -> float | None:
        if not value:
            return None
        value = value.strip()
        try:
            numeric = float(value)
        except ValueError:
            return LumenClient._parse_retry_after(value)
        if numeric > 10_000_000_000:
            numeric /= 1000.0
        now = time.time()
        if numeric > now - 60:
            return max(0.0, numeric - now)
        return max(0.0, numeric)

    def _audit_rate_headers(self, response: requests.Response) -> float:
        headers = {key.lower(): value for key, value in response.headers.items()}
        interesting = {
            key: headers[key]
            for key in (
                "retry-after",
                "x-ratelimit-limit",
                "x-ratelimit-remaining",
                "x-ratelimit-reset",
                "ratelimit-limit",
                "ratelimit-remaining",
                "ratelimit-reset",
                "x-rate-limit-limit",
                "x-rate-limit-remaining",
                "x-rate-limit-reset",
            )
            if key in headers
        }
        if interesting:
            LOGGER.debug("Rate-limit headers: %s", interesting)

        proactive_pause = self._parse_retry_after(headers.get("retry-after")) or 0.0
        remaining_raw = (
            headers.get("x-ratelimit-remaining")
            or headers.get("ratelimit-remaining")
            or headers.get("x-rate-limit-remaining")
        )
        reset_raw = (
            headers.get("x-ratelimit-reset")
            or headers.get("ratelimit-reset")
            or headers.get("x-rate-limit-reset")
        )
        if remaining_raw is not None:
            try:
                remaining = int(float(remaining_raw))
            except ValueError:
                LOGGER.warning("Could not parse quota remaining header: %r", remaining_raw)
            else:
                if remaining <= self.settings.rate_limit_low_watermark:
                    reset_delay = self._parse_reset_delay(reset_raw)
                    proactive_pause = max(
                        proactive_pause,
                        reset_delay
                        if reset_delay is not None
                        else self.settings.request_delay_seconds,
                    )
                    LOGGER.warning(
                        "Server-reported quota is low (remaining=%d); pausing %.2fs",
                        remaining,
                        proactive_pause,
                    )
        return proactive_pause

    def request_json(self, url: str, *, params: dict[str, Any] | None = None) -> Any:
        attempts = self.settings.max_retries + 1
        last_error: Exception | None = None

        for attempt in range(attempts):
            self._respect_spacing()
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.settings.request_timeout_seconds,
                    verify=self.settings.verify_ssl,
                    allow_redirects=True,
                )
            except requests.RequestException as exc:
                last_error = exc
                if attempt == attempts - 1:
                    break
                delay = min(
                    MAX_BACKOFF_SECONDS,
                    self.settings.backoff_base_seconds * (2**attempt)
                    + random.uniform(0.0, self.settings.jitter_max_seconds),
                )
                LOGGER.warning(
                    "Network error (%s); retry %d/%d in %.2fs",
                    exc,
                    attempt + 1,
                    self.settings.max_retries,
                    delay,
                )
                time.sleep(delay)
                continue

            proactive_pause = self._audit_rate_headers(response)
            final_host = (urlparse(response.url).hostname or "").lower()
            if final_host not in self.allowed_hosts:
                raise UnsafeRedirectError(
                    f"Lumen request redirected to an unapproved host {final_host!r}; the token was not retained for that host and the run has stopped"
                )
            if response.history:
                LOGGER.info(
                    "Request redirected to %s",
                    response.url.split("?", 1)[0],
                )

            if response.status_code in {401, 403}:
                raise AuthenticationError(
                    f"Lumen returned HTTP {response.status_code}. Verify the research token, apply it to every request, and use the approved custom User-Agent."
                )

            body_upper = response.text[:4000].upper()
            if response.status_code == 500 and "REQUEST REFUSED" in body_upper:
                raise LargeQueryRefused(
                    "Lumen returned REQUEST REFUSED (500) for this search; the date window must be narrowed"
                )
            if response.status_code == 500 and any(
                marker in body_upper
                for marker in (
                    "AUTOMATICALLY BANNED",
                    "EXCESSIVE SCRAPING",
                    "UNAUTHORIZED VULNERABILITY TESTING",
                )
            ):
                raise AccessBlockedError(
                    "Lumen returned a 500 response indicating possible automated blocking. Stop requests and contact Lumen with the API key, project details, source IP, and custom User-Agent."
                )

            if response.status_code in RETRYABLE_STATUS_CODES:
                if attempt == attempts - 1:
                    snippet = response.text[:500].replace("\n", " ")
                    raise RetryExhaustedHTTPError(
                        f"HTTP {response.status_code} persisted after retries for {response.url.split('?', 1)[0]}: {snippet}"
                    )
                retry_after = self._parse_retry_after(response.headers.get("Retry-After")) or 0.0
                delay = max(
                    retry_after,
                    min(
                        MAX_BACKOFF_SECONDS,
                        self.settings.backoff_base_seconds * (2**attempt)
                        + random.uniform(0.0, self.settings.jitter_max_seconds),
                    ),
                )
                LOGGER.warning(
                    "HTTP %d; retry %d/%d in %.2fs",
                    response.status_code,
                    attempt + 1,
                    self.settings.max_retries,
                    delay,
                )
                time.sleep(delay)
                continue

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                snippet = response.text[:500].replace("\n", " ")
                raise IngestionError(
                    f"Non-retryable HTTP {response.status_code} from {url}: {snippet}"
                ) from exc

            try:
                payload = response.json()
            except requests.JSONDecodeError as exc:
                last_error = exc
                if attempt == attempts - 1:
                    snippet = response.text[:500].replace("\n", " ")
                    raise IngestionError(
                        f"Lumen returned non-JSON content after retries: {snippet}"
                    ) from exc
                delay = min(
                    MAX_BACKOFF_SECONDS,
                    self.settings.backoff_base_seconds * (2**attempt)
                    + random.uniform(0.0, self.settings.jitter_max_seconds),
                )
                LOGGER.warning(
                    "Invalid JSON response; retry %d/%d in %.2fs",
                    attempt + 1,
                    self.settings.max_retries,
                    delay,
                )
                time.sleep(delay)
                continue

            if proactive_pause > 0:
                LOGGER.info("Honoring rate header pause of %.2fs", proactive_pause)
                time.sleep(proactive_pause)
            return payload

        raise IngestionError(f"Request failed after {attempts} attempts: {last_error}")

    def search_page(self, window: DateWindow, page: int) -> SearchPage:
        params: dict[str, Any] = dict(self.settings.extra_query_params)
        params.update(
            {
                "date_submitted": window.query_value,
                "page": page,
                "per_page": self.settings.per_page,
            }
        )
        if self.settings.sort_by:
            params["sort_by"] = self.settings.sort_by

        payload = self.request_json(self.settings.search_url, params=params)
        if not isinstance(payload, dict):
            raise ResponseShapeError("Search response must be a JSON object")
        notices = payload.get("notices")
        meta = payload.get("meta")
        if not isinstance(notices, list):
            raise ResponseShapeError("Search response field 'notices' must be a list")
        if not isinstance(meta, dict):
            raise ResponseShapeError("Search response field 'meta' must be an object")

        current_page = parse_optional_int(meta.get("current_page"), field="meta.current_page")
        per_page = parse_optional_int(meta.get("per_page"), field="meta.per_page")
        total_entries = parse_optional_int(meta.get("total_entries"), field="meta.total_entries")
        total_pages = parse_optional_int(meta.get("total_pages"), field="meta.total_pages")
        if None in {current_page, per_page, total_entries, total_pages}:
            raise ResponseShapeError(
                "Search meta must include current_page, per_page, total_entries, and total_pages"
            )
        assert current_page is not None
        assert per_page is not None
        assert total_entries is not None
        assert total_pages is not None

        if current_page != page:
            raise ResponseShapeError(
                f"Requested page {page}, but response says current_page={current_page}"
            )
        if per_page <= 0 or total_entries < 0 or total_pages < 0:
            raise ResponseShapeError("Search pagination values are invalid")
        if len(notices) > per_page:
            raise ResponseShapeError(
                f"Response contains {len(notices)} notices but meta.per_page={per_page}"
            )

        next_page = parse_optional_int(meta.get("next_page"), field="meta.next_page")
        previous_page = parse_optional_int(
            meta.get("previous_page"), field="meta.previous_page"
        )
        if next_page is not None and next_page <= current_page:
            raise ResponseShapeError("meta.next_page must advance beyond current_page")
        if total_entries == 0 and notices:
            raise ResponseShapeError("Search returned notices while meta.total_entries is zero")
        if total_entries > 0 and total_pages == 0:
            raise ResponseShapeError(
                "Search meta.total_pages cannot be zero when total_entries is positive"
            )
        if current_page < total_pages and next_page is None:
            raise ResponseShapeError(
                "Search meta.next_page is null before the documented final page"
            )
        if current_page >= total_pages and next_page is not None:
            raise ResponseShapeError(
                "Search meta.next_page is present at or beyond meta.total_pages"
            )
        if total_entries > 0 and current_page <= total_pages and not notices:
            raise ResponseShapeError(
                "Search returned an empty notice page inside a non-empty result set"
            )

        return SearchPage(
            notices=notices,
            current_page=current_page,
            next_page=next_page,
            previous_page=previous_page,
            per_page=per_page,
            total_entries=total_entries,
            total_pages=total_pages,
            request_params=params,
        )

    def fetch_topics(self) -> list[Any]:
        payload = self.request_json(self.settings.topics_url)
        if isinstance(payload, dict):
            topics = payload.get("topics")
        else:
            topics = payload
        if not isinstance(topics, list):
            raise ResponseShapeError(
                "Topics response must be a list or an object containing a 'topics' list"
            )
        return topics


def validate_notice(record: Any) -> tuple[str, dict[str, Any]]:
    """Validate only fields clearly fundamental in the documented search schema."""

    if not isinstance(record, dict):
        raise ValueError(f"record must be an object, got {type(record).__name__}")

    missing = [field for field in ("id", "type") if field not in record]
    if missing:
        raise ValueError(f"missing required field(s): {', '.join(missing)}")

    record_id = record["id"]
    if isinstance(record_id, bool) or not isinstance(record_id, (str, int)):
        raise ValueError("field 'id' must be a string or integer")
    normalized_id = str(record_id).strip()
    if not normalized_id:
        raise ValueError("field 'id' cannot be empty")

    notice_type = record["type"]
    if not isinstance(notice_type, str) or not notice_type.strip():
        raise ValueError("field 'type' must be a non-empty string")

    if "title" in record and record["title"] is not None and not isinstance(
        record["title"], str
    ):
        raise ValueError("field 'title', when present, must be a string or null")

    for field in ("topics", "tags", "jurisdictions", "works"):
        if field in record and record[field] is not None and not isinstance(
            record[field], list
        ):
            raise ValueError(f"field {field!r}, when present, must be a list or null")

    if isinstance(record.get("works"), list):
        for index, work in enumerate(record["works"]):
            if not isinstance(work, dict):
                raise ValueError(f"works[{index}] must be an object")
            for url_field in ("copyrighted_urls", "infringing_urls"):
                if url_field in work and work[url_field] is not None and not isinstance(
                    work[url_field], list
                ):
                    raise ValueError(
                        f"works[{index}].{url_field} must be a list or null"
                    )

    return normalized_id, record


def validate_topic(record: Any) -> tuple[str, dict[str, Any]]:
    if not isinstance(record, dict):
        raise ValueError(f"topic must be an object, got {type(record).__name__}")
    missing = [field for field in ("id", "name") if field not in record]
    if missing:
        raise ValueError(f"missing required topic field(s): {', '.join(missing)}")
    topic_id = record["id"]
    if isinstance(topic_id, bool) or not isinstance(topic_id, (str, int)):
        raise ValueError("topic field 'id' must be a string or integer")
    normalized_id = str(topic_id).strip()
    if not normalized_id:
        raise ValueError("topic field 'id' cannot be empty")
    if not isinstance(record["name"], str) or not record["name"].strip():
        raise ValueError("topic field 'name' must be a non-empty string")
    if "parent_id" in record and record["parent_id"] is not None:
        parent_id = record["parent_id"]
        if isinstance(parent_id, bool) or not isinstance(parent_id, (str, int)):
            raise ValueError(
                "topic field 'parent_id', when present, must be a string, integer, or null"
            )
    return normalized_id, record


def prepare_records(
    *,
    records: list[Any],
    endpoint: str,
    window: DateWindow,
    page: int,
) -> tuple[list[tuple[str, dict[str, Any]]], list[dict[str, Any]]]:
    accepted: list[tuple[str, dict[str, Any]]] = []
    quarantined: list[dict[str, Any]] = []
    ingested_at = utc_now_iso()

    for index, record in enumerate(records):
        try:
            record_id, validated = validate_notice(record)
        except ValueError as exc:
            quarantined.append(
                {
                    "quarantined_at_utc": ingested_at,
                    "source_endpoint": endpoint,
                    "error_reason": str(exc),
                    "source_window": window.to_mapping(),
                    "source_page": page,
                    "source_index": index,
                    "record_payload": record,
                }
            )
            continue

        accepted.append(
            (
                record_id,
                {
                    "_ingestion_meta": {
                        "ingested_at_utc": ingested_at,
                        "source_endpoint": endpoint,
                        "source_window": window.to_mapping(),
                        "source_page": page,
                        "source_record_id": record_id,
                    },
                    "record_data": validated,
                },
            )
        )
    return accepted, quarantined


def prepare_topics(
    records: list[Any], endpoint: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []
    ingested_at = utc_now_iso()
    for index, record in enumerate(records):
        try:
            topic_id, validated = validate_topic(record)
        except ValueError as exc:
            quarantined.append(
                {
                    "quarantined_at_utc": ingested_at,
                    "source_endpoint": endpoint,
                    "error_reason": str(exc),
                    "source_index": index,
                    "record_payload": record,
                }
            )
            continue
        accepted.append(
            {
                "_ingestion_meta": {
                    "ingested_at_utc": ingested_at,
                    "source_endpoint": endpoint,
                    "source_record_id": topic_id,
                },
                "record_data": validated,
            }
        )
    return accepted, quarantined


def config_fingerprint(settings: Settings) -> str:
    material = {
        "api_base_url": settings.api_base_url,
        "search_path": settings.search_path,
        "topics_path": settings.topics_path,
        "per_page": settings.per_page,
        "search_result_ceiling": settings.search_result_ceiling,
        "initial_window_days": settings.initial_window_days,
        "start_ms": settings.start_ms,
        "configured_end_ms": settings.end_ms,
        "sort_by": settings.sort_by,
        "extra_query_params": settings.extra_query_params,
        "fetch_topics": settings.fetch_topics,
        "raw_dir": str(settings.raw_dir.resolve()),
        "quarantine_dir": str(settings.quarantine_dir.resolve()),
    }
    return hashlib.sha256(compact_json(material).encode("utf-8")).hexdigest()


def seed_windows(start_ms: int, end_ms: int, initial_window_days: int) -> list[dict[str, int]]:
    if end_ms < start_ms:
        raise ConfigurationError("Snapshot end is earlier than snapshot start")
    if start_ms == end_ms:
        return [{"start_ms": start_ms, "end_ms": end_ms}]

    step_ms = initial_window_days * ONE_DAY_MS
    windows: list[dict[str, int]] = []
    cursor = start_ms
    while cursor < end_ms:
        window_end = min(end_ms, cursor + step_ms)
        windows.append({"start_ms": cursor, "end_ms": window_end})
        if window_end == end_ms:
            break
        cursor = window_end  # intentional overlap; global ID dedup removes duplicates
    return windows


def new_checkpoint(settings: Settings) -> dict[str, Any]:
    snapshot_end_ms = settings.end_ms if settings.end_ms is not None else epoch_ms_now()
    run_id = utc_now().strftime("%Y%m%dT%H%M%S%fZ")
    return {
        "version": CHECKPOINT_VERSION,
        "run_id": run_id,
        "status": "running",
        "created_at_utc": utc_now_iso(),
        "updated_at_utc": utc_now_iso(),
        "config_fingerprint": config_fingerprint(settings),
        "source_endpoint": settings.search_path,
        "snapshot_start_ms": settings.start_ms,
        "snapshot_end_ms": snapshot_end_ms,
        "pending_windows": seed_windows(
            settings.start_ms, snapshot_end_ms, settings.initial_window_days
        ),
        "current_window": None,
        "completed_windows": 0,
        "records_written": 0,
        "quarantined_records": 0,
        "batches_completed": 0,
        "topics_completed": False,
        "topics_batch_path": None,
        "last_batch_path": None,
        "last_error": None,
    }


def reconcile_totals(checkpoint: dict[str, Any], store: StateStore) -> None:
    checkpoint.update(store.totals())


class IngestionRunner:
    def __init__(
        self,
        *,
        settings: Settings,
        client: LumenClient,
        store: StateStore,
        checkpoints: CheckpointManager,
    ) -> None:
        self.settings = settings
        self.client = client
        self.store = store
        self.checkpoints = checkpoints
        self.stop_requested = False
        self.session_started = time.monotonic()
        self.session_records = 0

    def request_stop(self, signum: int, frame: Any) -> None:
        del frame
        LOGGER.warning("Received signal %s; stopping after the current durable batch", signum)
        self.stop_requested = True

    def _save(self, checkpoint: dict[str, Any]) -> None:
        reconcile_totals(checkpoint, self.store)
        self.checkpoints.save(checkpoint)

    def _fetch_topics_once(self, checkpoint: dict[str, Any]) -> None:
        if not self.settings.fetch_topics:
            checkpoint["topics_completed"] = True
            self._save(checkpoint)
            return

        existing = self.store.artifact("topics")
        if existing is not None:
            path = Path(str(existing["path"]))
            if not path.exists():
                raise IngestionError(
                    f"SQLite marks topics complete, but the artifact is missing: {path}"
                )
            checkpoint["topics_completed"] = True
            checkpoint["topics_batch_path"] = str(path)
            self._save(checkpoint)
            return

        topics = self.client.fetch_topics()
        accepted, quarantined = prepare_topics(topics, self.settings.topics_path)
        filename = f"topics_snapshot_{checkpoint['run_id']}.jsonl"
        path = self.settings.raw_dir / filename
        atomic_write_jsonl(path, accepted, fsync=self.settings.fsync_writes)

        if quarantined:
            # Reuse the same durable quarantine table without pretending topics are pages.
            self.store.connection.execute("BEGIN IMMEDIATE")
            try:
                for item in quarantined:
                    serialized = compact_json(item)
                    key = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
                    self.store.connection.execute(
                        """
                        INSERT OR IGNORE INTO quarantine_records(
                            quarantine_key, quarantined_at_utc, source_endpoint,
                            error_reason, record_json
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            key,
                            item["quarantined_at_utc"],
                            item["source_endpoint"],
                            item["error_reason"],
                            serialized,
                        ),
                    )
                self.store.connection.commit()
            except Exception:
                self.store.connection.rollback()
                with contextlib.suppress(FileNotFoundError):
                    path.unlink()
                raise
            self.store.export_quarantine()

        self.store.register_artifact("topics", path, len(accepted))
        checkpoint["topics_completed"] = True
        checkpoint["topics_batch_path"] = str(path)
        self._save(checkpoint)
        LOGGER.info(
            "Topics snapshot complete: accepted=%d quarantined=%d path=%s",
            len(accepted),
            len(quarantined),
            path,
        )

    def _split_current_window(
        self, checkpoint: dict[str, Any], window: DateWindow, reason: str
    ) -> None:
        left, right = window.split_with_boundary_overlap()
        LOGGER.info(
            "Splitting window=%s into %s and %s (%s)",
            window.query_value,
            left.query_value,
            right.query_value,
            reason,
        )
        checkpoint["pending_windows"] = [
            left.to_mapping(),
            right.to_mapping(),
            *checkpoint["pending_windows"],
        ]
        checkpoint["current_window"] = None
        self._save(checkpoint)

    def _complete_window(
        self,
        checkpoint: dict[str, Any],
        window: DateWindow,
        expected_total_entries: int,
    ) -> None:
        fetched_count = self.store.window_response_count(window)
        if fetched_count != expected_total_entries:
            raise SnapshotDriftError(
                f"Window {window.query_value} reports total_entries={expected_total_entries}, but completed pages contained {fetched_count} records. The page-based result set changed or was inconsistent; preserve state and rerun with a narrower date range or contact Lumen."
            )
        unique_valid_count = self.store.window_unique_valid_count(window)
        quarantine_occurrences = self.store.window_quarantine_occurrences(window)
        if unique_valid_count + quarantine_occurrences != expected_total_entries:
            raise SnapshotDriftError(
                f"Window {window.query_value} returned {expected_total_entries} records, but only {unique_valid_count} unique valid IDs plus {quarantine_occurrences} quarantined records were observed. This indicates duplicate/unstable page results or an inconsistent response; preserve state and contact Lumen or retry with a narrower range."
            )
        checkpoint["current_window"] = None
        checkpoint["completed_windows"] = int(checkpoint.get("completed_windows", 0)) + 1
        self._save(checkpoint)

    def _log_progress(
        self,
        *,
        checkpoint: dict[str, Any],
        window: DateWindow,
        page: int,
        completed: CompletedPage,
    ) -> None:
        self.session_records += completed.accepted_count
        elapsed = max(0.001, time.monotonic() - self.session_started)
        LOGGER.info(
            "batch=%d window=%s page=%d/%d response=%d new=%d quarantined=%d total_unique=%d rate=%.2f records/s next_page=%s",
            checkpoint["batches_completed"],
            window.query_value,
            page,
            completed.total_pages,
            completed.response_count,
            completed.accepted_count,
            completed.quarantined_count,
            checkpoint["records_written"],
            self.session_records / elapsed,
            completed.next_page,
        )

    def run(self, checkpoint: dict[str, Any]) -> dict[str, Any]:
        if checkpoint.get("status") == "completed":
            LOGGER.info(
                "Checkpoint is already complete: %d unique notices in %d batches",
                checkpoint.get("records_written", 0),
                checkpoint.get("batches_completed", 0),
            )
            return checkpoint

        checkpoint["status"] = "running"
        checkpoint["last_error"] = None
        self._save(checkpoint)

        if not checkpoint.get("topics_completed"):
            self._fetch_topics_once(checkpoint)

        while checkpoint.get("current_window") or checkpoint.get("pending_windows"):
            if self.stop_requested:
                checkpoint["status"] = "stopped"
                self._save(checkpoint)
                return checkpoint

            current = checkpoint.get("current_window")
            if current is None:
                mapping = checkpoint["pending_windows"].pop(0)
                window = DateWindow.from_mapping(mapping)
                current = {
                    "start_ms": window.start_ms,
                    "end_ms": window.end_ms,
                    "next_page": 1,
                    "probed": False,
                    "total_entries": None,
                    "total_pages": None,
                }
                checkpoint["current_window"] = current
                self._save(checkpoint)
            else:
                window = DateWindow.from_mapping(current)

            page_number = int(current.get("next_page") or 1)

            prior = self.store.completed_page(window, page_number)
            if prior is not None:
                if not Path(prior.batch_path).exists():
                    raise IngestionError(
                        f"SQLite marks page {page_number} of {window.query_value} complete, but its raw batch is missing: {prior.batch_path}"
                    )
                current["probed"] = True
                current["total_entries"] = prior.total_entries
                current["total_pages"] = prior.total_pages
                current["next_page"] = prior.next_page
                checkpoint["last_batch_path"] = prior.batch_path
                if prior.next_page is None:
                    self._complete_window(
                        checkpoint, window, expected_total_entries=prior.total_entries
                    )
                else:
                    self._save(checkpoint)
                continue

            LOGGER.info(
                "Fetching window=%s page=%d pending_windows=%d",
                window.query_value,
                page_number,
                len(checkpoint["pending_windows"]),
            )
            try:
                search_page = self.client.search_page(window, page_number)
            except LargeQueryRefused:
                if window.start_ms == window.end_ms:
                    raise UnsplittableWindowError(
                        f"Lumen refused the one-millisecond window {window.query_value}; contact Lumen for a server-side partition or approved bulk access"
                    )
                self._split_current_window(
                    checkpoint,
                    window,
                    "server returned REQUEST REFUSED (500)",
                )
                continue

            if not current.get("probed"):
                if search_page.total_entries > self.settings.search_result_ceiling:
                    if window.start_ms == window.end_ms:
                        raise UnsplittableWindowError(
                            f"Window {window.query_value} contains {search_page.total_entries} records, above the documented {self.settings.search_result_ceiling}-result ceiling, and cannot be split below one millisecond"
                        )
                    self._split_current_window(
                        checkpoint,
                        window,
                        f"total_entries={search_page.total_entries} exceeds ceiling={self.settings.search_result_ceiling}",
                    )
                    continue
                current["probed"] = True
                current["total_entries"] = search_page.total_entries
                current["total_pages"] = search_page.total_pages
            else:
                if (
                    int(current["total_entries"]) != search_page.total_entries
                    or int(current["total_pages"]) != search_page.total_pages
                ):
                    raise SnapshotDriftError(
                        f"Pagination metadata changed in window {window.query_value}: expected total_entries={current['total_entries']} total_pages={current['total_pages']}, received total_entries={search_page.total_entries} total_pages={search_page.total_pages}"
                    )

            accepted, quarantined = prepare_records(
                records=search_page.notices,
                endpoint=self.settings.search_path,
                window=window,
                page=search_page.current_page,
            )
            completed = self.store.commit_page(
                run_id=str(checkpoint["run_id"]),
                window=window,
                page=search_page,
                accepted=accepted,
                quarantined=quarantined,
            )
            current["next_page"] = completed.next_page
            checkpoint["last_batch_path"] = completed.batch_path
            self._save(checkpoint)
            self._log_progress(
                checkpoint=checkpoint,
                window=window,
                page=search_page.current_page,
                completed=completed,
            )

            if completed.next_page is None:
                self._complete_window(
                    checkpoint,
                    window,
                    expected_total_entries=completed.total_entries,
                )

        checkpoint["status"] = "completed"
        self._save(checkpoint)
        LOGGER.info(
            "Ingestion completed: %d unique notices, %d quarantined records, %d notice batches, %d windows",
            checkpoint["records_written"],
            checkpoint["quarantined_records"],
            checkpoint["batches_completed"],
            checkpoint["completed_windows"],
        )
        return checkpoint


def configure_logging(level: str) -> None:
    numeric = getattr(logging, level.upper(), None)
    if not isinstance(numeric, int):
        raise ConfigurationError(f"Unknown log level: {level}")
    logging.basicConfig(
        level=numeric,
        format="%(asctime)sZ %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logging.Formatter.converter = time.gmtime


def load_or_initialize_checkpoint(
    settings: Settings, manager: CheckpointManager
) -> dict[str, Any]:
    checkpoint = manager.load()
    if checkpoint is None:
        if settings.sqlite_path.exists():
            raise IngestionError(
                f"SQLite state exists at {settings.sqlite_path}, but checkpoint.json is missing. Restore the checkpoint or use --reset-state --yes after preserving raw data."
            )
        checkpoint = new_checkpoint(settings)
        manager.save(checkpoint)
        return checkpoint

    if not settings.sqlite_path.exists() and int(
        checkpoint.get("batches_completed", 0)
    ) > 0:
        raise IngestionError(
            f"Checkpoint records completed pages, but SQLite state is missing at {settings.sqlite_path}"
        )

    expected = config_fingerprint(settings)
    if checkpoint.get("config_fingerprint") != expected:
        raise ConfigurationError(
            "Current ingestion scope/output configuration does not match checkpoint.json. Restore the original settings to resume, or use --reset-state --yes to start a new run."
        )
    return checkpoint


def reset_state(settings: Settings) -> None:
    for path in (
        settings.checkpoint_path,
        settings.sqlite_path,
        settings.sqlite_path.with_name(settings.sqlite_path.name + "-wal"),
        settings.sqlite_path.with_name(settings.sqlite_path.name + "-shm"),
        settings.state_dir / "ingest.lock",
    ):
        with contextlib.suppress(FileNotFoundError):
            path.unlink()
    LOGGER.warning(
        "State reset completed. Existing raw files were intentionally retained; archive or remove them before a fresh run if duplicate historical output is undesirable."
    )


def probe(settings: Settings) -> int:
    """Make a bounded one-hour probe rather than an unsafe corpus-wide search."""

    end_ms = settings.end_ms if settings.end_ms is not None else epoch_ms_now()
    start_ms = max(settings.start_ms, end_ms - ONE_HOUR_MS)
    window = DateWindow(start_ms, end_ms)
    with LumenClient(settings) as client:
        page = client.search_page(window, 1)
    first_record_keys: list[str] = []
    if page.notices and isinstance(page.notices[0], dict):
        first_record_keys = sorted(str(key) for key in page.notices[0])
    print(
        json.dumps(
            {
                "endpoint": settings.search_path,
                "probe_window": window.to_mapping(),
                "current_page": page.current_page,
                "next_page": page.next_page,
                "per_page": page.per_page,
                "total_entries": page.total_entries,
                "total_pages": page.total_pages,
                "records_returned": len(page.notices),
                "first_record_keys": first_record_keys,
                "would_require_partitioning": page.total_entries
                > settings.search_result_ceiling,
            },
            indent=2,
        )
    )
    return 0


def status(settings: Settings) -> int:
    manager = CheckpointManager(settings)
    checkpoint = manager.load()
    if checkpoint is None:
        print("No checkpoint exists.")
        return 0
    print(json.dumps(checkpoint, indent=2, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Resumable JSONL ingestion for the Lumen research API.",
        epilog=API_ATTRIBUTION_NOTICE,
    )
    parser.add_argument(
        "--probe",
        action="store_true",
        help="Make one authenticated, one-hour search request without writing state.",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print checkpoint.json and exit without contacting Lumen.",
    )
    parser.add_argument(
        "--skip-topics",
        action="store_true",
        help="Do not fetch the finite /topics.json reference dataset for this run.",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Delete checkpoint and SQLite state. Raw files are not deleted.",
    )
    parser.add_argument("--yes", action="store_true", help="Required with --reset-state.")
    parser.add_argument(
        "--force-lock",
        action="store_true",
        help="Remove a stale lock only after confirming no other process is running.",
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Override LUMEN_LOG_LEVEL.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        settings = load_settings()
        if args.log_level:
            settings = replace(settings, log_level=args.log_level)
        if args.skip_topics:
            settings = replace(settings, fetch_topics=False)
        configure_logging(settings.log_level)
        LOGGER.info(API_ATTRIBUTION_NOTICE)

        if settings.per_page != 10:
            LOGGER.warning(
                "LUMEN_PER_PAGE=%d is an override; the supplied documentation confirms a default of 10 but does not state a supported maximum.",
                settings.per_page,
            )

        if settings.start_ms == 0 and not settings.large_project_contacted:
            LOGGER.warning(
                "Full-history scope is configured. Lumen asks researchers planning large scraping projects to contact the team in advance; set LUMEN_LARGE_PROJECT_CONTACTED=true after doing so."
            )

        settings.raw_dir.mkdir(parents=True, exist_ok=True)
        settings.quarantine_dir.mkdir(parents=True, exist_ok=True)
        settings.state_dir.mkdir(parents=True, exist_ok=True)

        if args.status:
            return status(settings)
        if args.probe:
            return probe(settings)
        if args.reset_state:
            if not args.yes:
                raise ConfigurationError("--reset-state requires --yes")
            reset_state(settings)
            return 0

        with ExclusiveRunLock(
            settings.state_dir / "ingest.lock", force=args.force_lock
        ):
            manager = CheckpointManager(settings)
            checkpoint = load_or_initialize_checkpoint(settings, manager)
            with StateStore(settings) as store, LumenClient(settings) as client:
                runner = IngestionRunner(
                    settings=settings,
                    client=client,
                    store=store,
                    checkpoints=manager,
                )
                with contextlib.suppress(ValueError):
                    signal.signal(signal.SIGINT, runner.request_stop)
                    signal.signal(signal.SIGTERM, runner.request_stop)
                try:
                    result = runner.run(checkpoint)
                except Exception as exc:
                    checkpoint["status"] = "failed"
                    checkpoint["last_error"] = {
                        "at_utc": utc_now_iso(),
                        "type": type(exc).__name__,
                        "message": str(exc),
                    }
                    runner._save(checkpoint)
                    raise
                return 0 if result.get("status") == "completed" else 130

    except (
        ConfigurationError,
        IngestionError,
        requests.RequestException,
        sqlite3.Error,
    ) as exc:
        LOGGER.error("%s: %s", type(exc).__name__, exc)
        return 1
    except KeyboardInterrupt:
        LOGGER.warning("Interrupted")
        return 130


if __name__ == "__main__":
    sys.exit(main())
