from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import requests

import ingest_data
from config import ConfigurationError, Settings, load_settings
from ingest_data import (
    AccessBlockedError,
    AuthenticationError,
    CheckpointManager,
    DateWindow,
    IngestionRunner,
    LargeQueryRefused,
    LumenClient,
    ResponseShapeError,
    SearchPage,
    SnapshotDriftError,
    StateStore,
    UnsafeRedirectError,
    new_checkpoint,
    prepare_records,
    prepare_topics,
    seed_windows,
    validate_notice,
    validate_topic,
)


def make_settings(tmp_path: Path, **overrides: Any) -> Settings:
    state_dir = tmp_path / ".state"
    values: dict[str, Any] = {
        "api_base_url": "https://lumendatabase.org",
        "api_key": "secret-token",
        "user_agent": "test-lumen-study/1.0 (+researcher@example.edu)",
        "search_path": "/notices/search.json",
        "topics_path": "/topics.json",
        "raw_dir": tmp_path / "raw",
        "quarantine_dir": tmp_path / "quarantine",
        "state_dir": state_dir,
        "checkpoint_path": state_dir / "checkpoint.json",
        "sqlite_path": state_dir / "ingestion_state.sqlite3",
        "request_delay_seconds": 1.0,
        "request_timeout_seconds": 10.0,
        "max_retries": 2,
        "backoff_base_seconds": 2.0,
        "jitter_max_seconds": 0.0,
        "rate_limit_low_watermark": 1,
        "per_page": 10,
        "search_result_ceiling": 10_000,
        "initial_window_days": 30,
        "start_ms": 0,
        "end_ms": 10,
        "extra_query_params": {},
        "sort_by": "date_received asc",
        "fetch_topics": True,
        "terms_accepted": True,
        "large_project_contacted": True,
        "log_level": "INFO",
        "verify_ssl": True,
        "fsync_writes": False,
    }
    values.update(overrides)
    return Settings(**values)


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        payload: Any = None,
        *,
        text: str | None = None,
        headers: dict[str, str] | None = None,
        url: str = "https://lumendatabase.org/notices/search.json",
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text if text is not None else json.dumps(payload)
        self.headers = headers or {}
        self.url = url
        self.history: list[Any] = []

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.headers: dict[str, str] = {}
        self.calls: list[dict[str, Any]] = []
        self.closed = False

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        if not self.responses:
            raise AssertionError("Unexpected HTTP call")
        return self.responses.pop(0)

    def close(self) -> None:
        self.closed = True


def notice(record_id: str, **extra: Any) -> dict[str, Any]:
    return {"id": record_id, "type": "DMCA", "title": f"Notice {record_id}", **extra}


def page(
    window: DateWindow,
    number: int,
    records: list[Any],
    *,
    next_page: int | None,
    total_entries: int,
    total_pages: int,
) -> SearchPage:
    return SearchPage(
        notices=records,
        current_page=number,
        next_page=next_page,
        previous_page=None if number == 1 else number - 1,
        per_page=10,
        total_entries=total_entries,
        total_pages=total_pages,
        request_params={"date_submitted": window.query_value, "page": number},
    )


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_seed_windows_overlap_boundaries() -> None:
    day = ingest_data.ONE_DAY_MS
    windows = seed_windows(0, 3 * day, 1)
    assert windows == [
        {"start_ms": 0, "end_ms": day},
        {"start_ms": day, "end_ms": 2 * day},
        {"start_ms": 2 * day, "end_ms": 3 * day},
    ]


def test_date_window_split_retains_boundary_overlap() -> None:
    left, right = DateWindow(0, 10).split_with_boundary_overlap()
    assert left == DateWindow(0, 5)
    assert right == DateWindow(5, 10)


def test_notice_validation_is_defensive_without_overrequiring_title() -> None:
    record_id, record = validate_notice({"id": 7, "type": "CourtOrder"})
    assert record_id == "7"
    assert record["type"] == "CourtOrder"

    with pytest.raises(ValueError, match="missing required"):
        validate_notice({"id": 7})
    with pytest.raises(ValueError, match=r"works\[0\]"):
        validate_notice({"id": 7, "type": "DMCA", "works": ["bad"]})


def test_prepare_records_quarantines_bad_payload() -> None:
    accepted, quarantined = prepare_records(
        records=[notice("1"), {"id": "2"}, "broken"],
        endpoint="/notices/search.json",
        window=DateWindow(0, 10),
        page=1,
    )
    assert [record_id for record_id, _ in accepted] == ["1"]
    assert len(quarantined) == 2
    assert quarantined[0]["source_page"] == 1
    assert "missing required" in quarantined[0]["error_reason"]


def test_topic_validation_and_quarantine() -> None:
    topic_id, _ = validate_topic({"id": 1, "name": "Copyright", "parent_id": None})
    assert topic_id == "1"
    accepted, quarantined = prepare_topics(
        [{"id": 1, "name": "Copyright", "parent_id": None}, {"name": "bad"}],
        "/topics.json",
    )
    assert len(accepted) == 1
    assert len(quarantined) == 1


def test_client_sends_authentication_headers_and_parses_search(tmp_path: Path) -> None:
    payload = {
        "notices": [notice("491")],
        "meta": {
            "query": {},
            "facets": {},
            "current_page": 1,
            "next_page": None,
            "offset": 0,
            "per_page": 10,
            "previous_page": None,
            "total_entries": 1,
            "total_pages": 1,
        },
    }
    session = FakeSession([FakeResponse(payload=payload)])
    settings = make_settings(tmp_path, extra_query_params={"language_facet": "en"})
    client = LumenClient(settings, session=session)  # type: ignore[arg-type]

    result = client.search_page(DateWindow(100, 200), 1)

    assert result.total_entries == 1
    assert session.headers["X-Authentication-Token"] == "secret-token"
    assert session.headers["User-Agent"].startswith("test-lumen-study")
    assert session.headers["Accept-Encoding"] == "gzip"
    call = session.calls[0]
    assert call["url"].endswith("/notices/search.json")
    assert call["params"] == {
        "language_facet": "en",
        "date_submitted": "100..200",
        "page": 1,
        "per_page": 10,
        "sort_by": "date_received asc",
    }


def test_client_rejects_inconsistent_pagination_shape(tmp_path: Path) -> None:
    payload = {
        "notices": [notice("1")],
        "meta": {
            "current_page": 1,
            "next_page": None,
            "per_page": 10,
            "previous_page": None,
            "total_entries": 20,
            "total_pages": 2,
        },
    }
    client = LumenClient(
        make_settings(tmp_path), session=FakeSession([FakeResponse(payload=payload)])  # type: ignore[arg-type]
    )
    with pytest.raises(ResponseShapeError, match="next_page is null"):
        client.search_page(DateWindow(0, 10), 1)


def test_client_honors_retry_after_on_429(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = {
        "notices": [],
        "meta": {
            "current_page": 1,
            "next_page": None,
            "per_page": 10,
            "previous_page": None,
            "total_entries": 0,
            "total_pages": 0,
        },
    }
    session = FakeSession(
        [
            FakeResponse(429, {"error": "slow down"}, headers={"Retry-After": "3"}),
            FakeResponse(200, payload),
        ]
    )
    sleeps: list[float] = []
    monotonic_values = iter([0.0, 10.0, 10.0])
    monkeypatch.setattr(ingest_data.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(ingest_data.time, "monotonic", lambda: next(monotonic_values))
    client = LumenClient(make_settings(tmp_path), session=session)  # type: ignore[arg-type]

    result = client.search_page(DateWindow(0, 10), 1)

    assert result.total_entries == 0
    assert any(seconds >= 3.0 for seconds in sleeps)
    assert len(session.calls) == 2


def test_client_fails_fast_for_auth_and_blocking(tmp_path: Path) -> None:
    
    auth_client = LumenClient(
        make_settings(tmp_path), session=FakeSession([FakeResponse(401, {})])  # type: ignore[arg-type]
    )
    with pytest.raises(AuthenticationError):
        auth_client.request_json("https://lumendatabase.org/notices/search.json")

    blocked_client = LumenClient(
        make_settings(tmp_path),
        session=FakeSession(
            [FakeResponse(500, {}, text="You may have been automatically banned for excessive scraping")]
        ),  # type: ignore[arg-type]
    )
    with pytest.raises(AccessBlockedError):
        blocked_client.request_json("https://lumendatabase.org/notices/search.json")


def test_client_stops_on_cross_host_redirect(tmp_path: Path) -> None:
    client = LumenClient(
        make_settings(tmp_path),
        session=FakeSession(
            [
                FakeResponse(
                    200,
                    {"ok": True},
                    url="https://example.invalid/redirected",
                )
            ]
        ),  # type: ignore[arg-type]
    )
    with pytest.raises(UnsafeRedirectError):
        client.request_json("https://lumendatabase.org/notices/search.json")


def test_client_recognizes_documented_request_refused(tmp_path: Path) -> None:
    client = LumenClient(
        make_settings(tmp_path),
        session=FakeSession([FakeResponse(500, {}, text="REQUEST REFUSED (500)")]),  # type: ignore[arg-type]
    )
    with pytest.raises(LargeQueryRefused):
        client.request_json("https://lumendatabase.org/notices/search.json")


def test_state_store_deduplicates_overlapping_windows(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    first = DateWindow(0, 5)
    second = DateWindow(5, 10)
    with StateStore(settings) as store:
        accepted1, bad1 = prepare_records(
            records=[notice("a"), notice("boundary")],
            endpoint=settings.search_path,
            window=first,
            page=1,
        )
        store.commit_page(
            run_id="run",
            window=first,
            page=page(first, 1, [notice("a"), notice("boundary")], next_page=None, total_entries=2, total_pages=1),
            accepted=accepted1,
            quarantined=bad1,
        )

        accepted2, bad2 = prepare_records(
            records=[notice("boundary"), notice("b")],
            endpoint=settings.search_path,
            window=second,
            page=1,
        )
        completed = store.commit_page(
            run_id="run",
            window=second,
            page=page(second, 1, [notice("boundary"), notice("b")], next_page=None, total_entries=2, total_pages=1),
            accepted=accepted2,
            quarantined=bad2,
        )

        assert store.totals()["records_written"] == 3
        assert len(read_jsonl(Path(completed.batch_path))) == 1
        assert store.window_unique_valid_count(second) == 2


class MappingClient:
    def __init__(
        self,
        responses: dict[tuple[int, int, int], SearchPage | Exception],
        *,
        topics: list[Any] | None = None,
    ) -> None:
        
        self.responses = responses
        self.topics = topics or []
        self.calls: list[tuple[int, int, int]] = []
        self.topic_calls = 0

    def fetch_topics(self) -> list[Any]:
        self.topic_calls += 1
        return self.topics

    def search_page(self, window: DateWindow, page_number: int) -> SearchPage:
        key = (window.start_ms, window.end_ms, page_number)
        self.calls.append(key)
        result = self.responses[key]
        if isinstance(result, Exception):
            raise result
        return result


def run_with_client(
    settings: Settings, client: MappingClient, checkpoint: dict[str, Any] | None = None
) -> dict[str, Any]:
    manager = CheckpointManager(settings)
    checkpoint = checkpoint or new_checkpoint(settings)
    manager.save(checkpoint)
    with StateStore(settings) as store:
        runner = IngestionRunner(
            settings=settings,
            client=client,  # type: ignore[arg-type]
            store=store,
            checkpoints=manager,
        )
        return runner.run(checkpoint)


def test_end_to_end_mocked_pagination_topics_and_checkpoint(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, start_ms=0, end_ms=10, fetch_topics=True)
    window = DateWindow(0, 10)
    client = MappingClient(
        {
            (0, 10, 1): page(
                window,
                1,
                [notice("1"), notice("2")],
                next_page=2,
                total_entries=3,
                total_pages=2,
            ),
            (0, 10, 2): page(
                window,
                2,
                [notice("3")],
                next_page=None,
                total_entries=3,
                total_pages=2,
            ),
        },
        topics=[{"id": 1, "name": "Copyright", "parent_id": None}],
    )

    result = run_with_client(settings, client)

    assert result["status"] == "completed"
    assert result["records_written"] == 3
    assert result["batches_completed"] == 2
    assert result["topics_completed"] is True
    assert Path(result["topics_batch_path"]).exists()
    assert CheckpointManager(settings).load()["status"] == "completed"
    notice_lines = []
    for output in settings.raw_dir.glob("data_batch_*.jsonl"):
        notice_lines.extend(read_jsonl(output))
    assert {line["record_data"]["id"] for line in notice_lines} == {"1", "2", "3"}


def test_runner_splits_searches_over_documented_ceiling_and_deduplicates(
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path, start_ms=0, end_ms=10, fetch_topics=False)
    whole = DateWindow(0, 10)
    left = DateWindow(0, 5)
    right = DateWindow(5, 10)
    client = MappingClient(
        {
            (0, 10, 1): page(
                whole,
                1,
                [notice("probe")],
                next_page=2,
                total_entries=10_001,
                total_pages=1001,
            ),
            (0, 5, 1): page(
                left,
                1,
                [notice("boundary")],
                next_page=None,
                total_entries=1,
                total_pages=1,
            ),
            (5, 10, 1): page(
                right,
                1,
                [notice("boundary"), notice("right")],
                next_page=None,
                total_entries=2,
                total_pages=1,
            ),
        }
    )

    result = run_with_client(settings, client)

    assert result["status"] == "completed"
    assert result["completed_windows"] == 2
    assert result["records_written"] == 2
    assert client.calls == [(0, 10, 1), (0, 5, 1), (5, 10, 1)]


def test_runner_recovers_completed_page_from_sqlite_without_refetching(
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path, start_ms=0, end_ms=10, fetch_topics=False)
    window = DateWindow(0, 10)
    manager = CheckpointManager(settings)
    checkpoint = new_checkpoint(settings)
    checkpoint["topics_completed"] = True
    checkpoint["pending_windows"] = []
    checkpoint["current_window"] = {
        "start_ms": 0,
        "end_ms": 10,
        "next_page": 1,
        "probed": False,
        "total_entries": None,
        "total_pages": None,
    }
    manager.save(checkpoint)

    with StateStore(settings) as store:
        first_records = [notice("1")]
        accepted, bad = prepare_records(
            records=first_records,
            endpoint=settings.search_path,
            window=window,
            page=1,
        )
        store.commit_page(
            run_id=checkpoint["run_id"],
            window=window,
            page=page(window, 1, first_records, next_page=2, total_entries=2, total_pages=2),
            accepted=accepted,
            quarantined=bad,
        )

        client = MappingClient(
            {
                (0, 10, 2): page(
                    window,
                    2,
                    [notice("2")],
                    next_page=None,
                    total_entries=2,
                    total_pages=2,
                )
            }
        )
        runner = IngestionRunner(
            settings=settings,
            client=client,  # type: ignore[arg-type]
            store=store,
            checkpoints=manager,
        )
        result = runner.run(checkpoint)

    assert result["status"] == "completed"
    assert client.calls == [(0, 10, 2)]
    assert result["records_written"] == 2


def test_runner_detects_duplicate_ids_across_pages(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, start_ms=0, end_ms=10, fetch_topics=False)
    window = DateWindow(0, 10)
    client = MappingClient(
        {
            (0, 10, 1): page(
                window,
                1,
                [notice("same")],
                next_page=2,
                total_entries=2,
                total_pages=2,
            ),
            (0, 10, 2): page(
                window,
                2,
                [notice("same")],
                next_page=None,
                total_entries=2,
                total_pages=2,
            ),
        }
    )

    with pytest.raises(SnapshotDriftError, match="unique valid IDs"):
        run_with_client(settings, client)


def test_load_settings_enforces_terms_spacing_and_reserved_parameters(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    for key in list(__import__("os").environ):
        if key.startswith("LUMEN_"):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LUMEN_API_KEY", "token")
    monkeypatch.setenv("LUMEN_USER_AGENT", "project/1.0 (+contact@example.edu)")

    with pytest.raises(ConfigurationError, match="TERMS_ACCEPTED"):
        load_settings()

    monkeypatch.setenv("LUMEN_TERMS_ACCEPTED", "true")
    monkeypatch.setenv("LUMEN_REQUEST_DELAY_SECONDS", "0.5")
    with pytest.raises(ConfigurationError, match="at least 1.0"):
        load_settings()

    monkeypatch.setenv("LUMEN_REQUEST_DELAY_SECONDS", "1.1")
    monkeypatch.setenv(
        "LUMEN_EXTRA_QUERY_PARAMS_JSON", '{"authentication_token":"do-not-do-this"}'
    )
    with pytest.raises(ConfigurationError, match="pipeline-managed"):
        load_settings()


def test_reset_state_cli_exits_without_starting_ingestion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = make_settings(tmp_path)
    settings.state_dir.mkdir(parents=True)
    settings.checkpoint_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(ingest_data, "load_settings", lambda: settings)
    monkeypatch.setattr(ingest_data, "configure_logging", lambda level: None)

    assert ingest_data.main(["--reset-state", "--yes"]) == 0
    assert not settings.checkpoint_path.exists()
