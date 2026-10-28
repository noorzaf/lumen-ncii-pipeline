"""Configuration for the Lumen research API ingestion pipeline."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


class ConfigurationError(ValueError):
    """Raised when environment configuration is missing or invalid."""


def _load_dotenv() -> None:
    if load_dotenv is not None:
        load_dotenv(override=False)


def _env_int(name: str, default: int | None = None) -> int | None:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be numeric, got {raw!r}") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"{name} must be true or false, got {raw!r}")


def _env_json_object(name: str) -> dict[str, Any]:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigurationError(f"{name} must contain valid JSON") from exc
    if not isinstance(value, dict):
        raise ConfigurationError(f"{name} must be a JSON object")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    api_base_url: str
    api_key: str
    user_agent: str
    search_path: str
    topics_path: str

    raw_dir: Path
    quarantine_dir: Path
    state_dir: Path
    checkpoint_path: Path
    sqlite_path: Path

    request_delay_seconds: float
    request_timeout_seconds: float
    max_retries: int
    backoff_base_seconds: float
    jitter_max_seconds: float
    rate_limit_low_watermark: int

    per_page: int
    search_result_ceiling: int
    initial_window_days: int
    start_ms: int
    end_ms: int | None
    extra_query_params: dict[str, Any]
    sort_by: str

    fetch_topics: bool
    terms_accepted: bool
    large_project_contacted: bool
    log_level: str
    verify_ssl: bool
    fsync_writes: bool

    @property
    def search_url(self) -> str:
        return f"{self.api_base_url}{self.search_path}"

    @property
    def topics_url(self) -> str:
        return f"{self.api_base_url}{self.topics_path}"


def load_settings() -> Settings:
    """Load and validate settings from ``.env`` and the process environment."""

    _load_dotenv()

    api_key = os.getenv("LUMEN_API_KEY", "").strip()
    if not api_key:
        raise ConfigurationError("LUMEN_API_KEY is required")

    user_agent = os.getenv("LUMEN_USER_AGENT", "").strip()
    placeholder_agents = {
        "",
        "someuseragenthere",
        "your-user-agent",
        "lumen-jsonl-ingestor/2.0 (+researcher@example.edu)",
        "lumen-jsonl-ingestor/3.0 (+researcher@example.edu)",
    }
    if user_agent.lower() in placeholder_agents:
        raise ConfigurationError(
            "LUMEN_USER_AGENT is still the placeholder value shipped in "
            ".env.example (or empty). This is rejected on purpose: Lumen's "
            "docs require a custom user agent that identifies YOUR project "
            "and a real contact address, since default/generic agents are "
            "blocked server-side. If you just copied .env.example to .env, "
            "edit LUMEN_USER_AGENT to something like "
            "'my-actual-project-name/1.0 (+your-real-email@domain.com)' "
            "before continuing."
        )

    terms_accepted = _env_bool("LUMEN_TERMS_ACCEPTED", False)
    if not terms_accepted:
        raise ConfigurationError(
            "Set LUMEN_TERMS_ACCEPTED=true only after reviewing and accepting Lumen's API Terms of Use"
        )

    api_base_url = os.getenv(
        "LUMEN_API_BASE_URL", "https://lumendatabase.org"
    ).strip().rstrip("/")
    if not api_base_url.startswith(("https://", "http://")):
        raise ConfigurationError("LUMEN_API_BASE_URL must start with http:// or https://")

    search_path = os.getenv("LUMEN_SEARCH_PATH", "/notices/search.json").strip()
    topics_path = os.getenv("LUMEN_TOPICS_PATH", "/topics.json").strip()
    if not search_path.startswith("/") or not topics_path.startswith("/"):
        raise ConfigurationError("LUMEN_SEARCH_PATH and LUMEN_TOPICS_PATH must start with '/'")

    request_delay = _env_float("LUMEN_REQUEST_DELAY_SECONDS", 1.10)
    if request_delay < 1.0:
        raise ConfigurationError(
            "LUMEN_REQUEST_DELAY_SECONDS must be at least 1.0 seconds; Lumen asks automated clients to sleep at least one second between requests"
        )

    timeout = _env_float("LUMEN_REQUEST_TIMEOUT_SECONDS", 60.0)
    if timeout <= 0:
        raise ConfigurationError("LUMEN_REQUEST_TIMEOUT_SECONDS must be greater than zero")

    max_retries = _env_int("LUMEN_MAX_RETRIES", 6)
    if max_retries is None or max_retries < 0:
        raise ConfigurationError("LUMEN_MAX_RETRIES must be zero or greater")

    backoff_base = _env_float("LUMEN_BACKOFF_BASE_SECONDS", 2.0)
    jitter_max = _env_float("LUMEN_JITTER_MAX_SECONDS", 1.0)
    if backoff_base <= 0 or jitter_max < 0:
        raise ConfigurationError("Backoff base must be positive and jitter must be non-negative")

    per_page = _env_int("LUMEN_PER_PAGE", 10)
    if per_page is None or per_page <= 0:
        raise ConfigurationError("LUMEN_PER_PAGE must be greater than zero")

    ceiling = _env_int("LUMEN_SEARCH_RESULT_CEILING", 10_000)
    if ceiling is None or ceiling <= 0 or ceiling > 10_000:
        raise ConfigurationError(
            "LUMEN_SEARCH_RESULT_CEILING must be between 1 and the documented maximum of 10000"
        )

    initial_window_days = _env_int("LUMEN_INITIAL_WINDOW_DAYS", 30)
    if initial_window_days is None or initial_window_days <= 0:
        raise ConfigurationError("LUMEN_INITIAL_WINDOW_DAYS must be greater than zero")

    start_ms = _env_int("LUMEN_START_MS", 0)
    if start_ms is None or start_ms < 0:
        raise ConfigurationError("LUMEN_START_MS must be zero or greater")

    end_ms = _env_int("LUMEN_END_MS", None)
    if end_ms is not None and end_ms < start_ms:
        raise ConfigurationError("LUMEN_END_MS must be greater than or equal to LUMEN_START_MS")

    extra_query_params = _env_json_object("LUMEN_EXTRA_QUERY_PARAMS_JSON")
    reserved = {
        "authentication_token",
        "page",
        "per_page",
        "date_submitted",
        "sort_by",
    }
    collisions = reserved.intersection(extra_query_params)
    if collisions:
        raise ConfigurationError(
            "LUMEN_EXTRA_QUERY_PARAMS_JSON contains pipeline-managed keys: "
            + ", ".join(sorted(collisions))
        )

    allowed_sorts = {
        "",
        "date_received asc",
        "date_received desc",
        "relevancy asc",
        "relevancy desc",
    }
    sort_by = os.getenv("LUMEN_SORT_BY", "date_received asc").strip()
    if sort_by not in allowed_sorts:
        raise ConfigurationError(
            "LUMEN_SORT_BY must be one of: date_received asc, date_received desc, relevancy asc, relevancy desc, or blank"
        )

    state_dir = Path(os.getenv("LUMEN_STATE_DIR", ".state")).expanduser()

    return Settings(
        api_base_url=api_base_url,
        api_key=api_key,
        user_agent=user_agent,
        search_path=search_path,
        topics_path=topics_path,
        raw_dir=Path(os.getenv("LUMEN_RAW_DIR", "raw")).expanduser(),
        quarantine_dir=Path(os.getenv("LUMEN_QUARANTINE_DIR", "quarantine")).expanduser(),
        state_dir=state_dir,
        checkpoint_path=state_dir
        / os.getenv("LUMEN_CHECKPOINT_FILE", "checkpoint.json"),
        sqlite_path=state_dir
        / os.getenv("LUMEN_SQLITE_FILE", "ingestion_state.sqlite3"),
        request_delay_seconds=request_delay,
        request_timeout_seconds=timeout,
        max_retries=max_retries,
        backoff_base_seconds=backoff_base,
        jitter_max_seconds=jitter_max,
        rate_limit_low_watermark=_env_int("LUMEN_RATE_LIMIT_LOW_WATERMARK", 1)
        or 1,
        per_page=per_page,
        search_result_ceiling=ceiling,
        initial_window_days=initial_window_days,
        start_ms=start_ms,
        end_ms=end_ms,
        extra_query_params=extra_query_params,
        sort_by=sort_by,
        fetch_topics=_env_bool("LUMEN_FETCH_TOPICS", True),
        terms_accepted=terms_accepted,
        large_project_contacted=_env_bool("LUMEN_LARGE_PROJECT_CONTACTED", False),
        log_level=os.getenv("LUMEN_LOG_LEVEL", "INFO").strip().upper(),
        verify_ssl=_env_bool("LUMEN_VERIFY_SSL", True),
        fsync_writes=_env_bool("LUMEN_FSYNC_WRITES", True),
    )
