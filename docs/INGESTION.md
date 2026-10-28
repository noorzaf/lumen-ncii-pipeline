# Lumen Research API JSONL Ingestion

A polite, resumable Python pipeline for landing Lumen notice-search records as line-delimited JSON in a local raw zone.

> **Required Lumen notice:** This product uses the Lumen API but is not endorsed or certified by Lumen.

## Scope and important limitations

This package exports:

1. Notice records returned by the authenticated notice-search API.
2. The finite topics reference dataset from `/topics.json` (enabled by default).

It does **not** claim to enumerate every resource exposed anywhere on the site:

- `/entities/search.json` is documented as a term-based full-text search endpoint, not as an enumerate-all endpoint.
- A notice search result is documented as containing the same notice data as an individually requested notice, plus `score`, so the pipeline does not make an additional request for every notice ID.
- Lumen's search API is page-based and does not provide a transactional snapshot or documented `updated_since` cursor. The script freezes a `date_submitted` upper bound and detects changing pagination totals, but a corpus-scale export is still a best-effort API snapshot rather than a database dump.
- Lumen cannot return results beyond the 10,000th Elasticsearch result. The exporter partitions by documented `date_submitted` ranges to avoid silently truncating a broad query.
- Lumen's researcher guidance says the service is not intended to become part of a particular business model's operational workflow. This package is designed for approved research ingestion, not commercial takedown monitoring.

Lumen's researcher guidance recommends narrowing million-record projects and contacting `team@lumendatabase.org` in advance. The script emits a warning for full-history scope until `LUMEN_LARGE_PROJECT_CONTACTED=true` is set.

## Sources reviewed

The package was corrected against the four supplied official Lumen documents:

- **Documentation - Lumen Database**, especially pages 1-24.
- **Troubleshooting - Lumen Database**, pages 1-4.
- **API Terms of Use - Lumen**, pages 1-4.
- **Researchers - Lumen**, especially pages 3-8.

## Verified API findings

### Authentication and required headers

Search and notice-data requests require an Authentication Token. The documented header form is:

```http
X-Authentication-Token: <token>
```

The token can also be supplied as the `authentication_token` URL parameter, but this package deliberately uses the header so secrets are not placed in query strings, access logs, or exception URLs.

Redirect handling keeps the token only for the configured Lumen hostname and its `www` alias. A redirect ending on any other host stops the run.

A meaningful custom `User-Agent` is mandatory for scripted requests. Generic/default agents may be blocked.

The client also sends:

```http
Accept: application/json
Accept-Encoding: gzip
```

### Endpoint topology

| Method | Endpoint | Purpose | Used by this package |
|---|---|---|---|
| `GET` | `/notices/search.json` | Page-based full-text/faceted notice search | Yes |
| `GET` | `/notices/<notice_id>.json` | One selected notice representation | No; search results are documented as containing the same notice data plus `score` |
| `GET` | `/topics.json` | Topic ID/name/parent reference data | Yes, once per run by default |
| `GET` | `/entities/search.json?term=...` | Term-based entity search | No; not documented as exhaustive enumeration |
| `POST` | `/notices` | Submit a notice | No; ingestion is read-only |

### Rate limits

The official documents describe two different contexts:

- Unauthenticated/general site JSON access: up to **5 JSON requests per day**.
- General notice pages: up to **10 requests per minute or 30 per hour**.
- Authenticated API data requests: a higher throttle of approximately **one request per second**, with explicit guidance to sleep for **at least one second between automated requests**.

This package therefore:

- Requires an API key and applies it to every request.
- Rejects a configured request delay below `1.0` second.
- Defaults to `1.10` seconds between request starts.
- Retries `429`, `500`, `502`, `503`, and `504`, plus network failures and transient invalid JSON.
- Uses exponential backoff with jitter:

  ```text
  delay = backoff_base * (2 ** attempt) + uniform(0, jitter_max)
  ```

- Gives `Retry-After` precedence when present.
- Audits common `X-RateLimit-*`, `RateLimit-*`, and `X-Rate-Limit-*` header variants defensively.

The supplied documentation does **not** promise a specific quota-header contract, so the pipeline never depends on those headers being present.

### Pagination

Notice search is page-based:

```text
page=<integer>
per_page=<integer>  # documented default: 10
```

The response contains:

```json
{
  "notices": [],
  "meta": {
    "query": {},
    "facets": {},
    "current_page": 1,
    "next_page": 2,
    "offset": 0,
    "per_page": 10,
    "previous_page": null,
    "total_entries": 123,
    "total_pages": 13
  }
}
```

No cursor or `updated_since` parameter is documented.

### Search parameters used for partitioning

The documentation lists `date_submitted` as a range in Unix epoch **milliseconds**, separated by `..`:

```text
date_submitted=1546344000000..1546603200000
```

The pipeline uses this field only as a server-side partition filter. It does not assume that every returned record contains a `date_submitted` output field.

Other documented search parameters include:

- Text/field search: `term`, `title`, `topics`, `tags`, `jurisdictions`, `sender_name`, `principal_name`, `recipient_name`, `works`, `action_taken`, `entities_country_codes`.
- `*-require-all` variants for several text fields.
- Facets: `topic_facet`, `sender_name_facet`, `principal_name_facet`, `recipient_name_facet`, `tag_list_facet`, `country_code_facet`, `language_facet`, `action_taken_facet`, `date_received_facet`.
- `page`, `per_page`, and `sort_by`.
- Documented sort values: `date_received asc`, `date_received desc`, `relevancy asc`, `relevancy desc`.

Optional filters can be supplied through `LUMEN_EXTRA_QUERY_PARAMS_JSON`. Authentication, `page`, `per_page`, `date_submitted`, and `sort_by` are pipeline-managed and rejected if supplied there.

The default `sort_by` is `date_received asc`. This avoids relevance ordering for a range-only export, but Lumen does not document a unique-ID tiebreaker. The checkpoint, page-total, and per-window unique-ID checks therefore remain necessary.

### 10,000-result search ceiling

Lumen documents that it cannot return past the 10,000th Elasticsearch result. It also warns that very broad searches may return `REQUEST REFUSED (500)`.

For every run, the pipeline:

1. Captures one immutable `snapshot_end_ms` in `checkpoint.json`.
2. Seeds the requested history into bounded time windows (30 days by default).
3. Requests page 1 of each window.
4. Recursively splits any window whose `total_entries` exceeds 10,000.
5. Immediately splits a window when the server returns the documented `REQUEST REFUSED (500)` large-search response.
6. Pages through only leaf windows at or below 10,000 results.
7. Overlaps split boundaries because the documentation does not state whether endpoints are inclusive.
8. Deduplicates globally by notice `id` in SQLite.

If more than 10,000 notices fall within one exact millisecond, the range cannot be subdivided further. The script stops explicitly instead of pretending the export is complete.

### Record schema

The documented notice-search example contains flat notice objects under `notices`, including:

- `id` (shown as a string in the search example).
- `type`.
- `title`, `body`, `date_received`, `language`.
- `topics`, `tags`, `jurisdictions`.
- `sender_name`, `recipient_name`.
- `works`, with nested `copyrighted_urls` and `infringing_urls`.
- Search-only `score`.

Individual notice responses are shown under a type-specific root key such as `dmca`. Search results are flat objects, which is the shape the exporter validates.

Only `id` and `type` are treated as universally required by the validator. `title` is accepted as a string, `null`, or absent because the supplied documentation does not explicitly state that it is mandatory for every notice type.

## Terms and use safeguards

The supplied API Terms state that API use is for research purposes, require compliance with applicable law and third-party rights, prohibit unreasonable bandwidth or adverse impact on Lumen's service, prohibit selling/sublicensing the API key, and require the following notice in an application:

> This product uses the Lumen API but is not endorsed or certified by Lumen.

The program requires `LUMEN_TERMS_ACCEPTED=true` before it will run. This is an acknowledgement mechanism, not legal advice.

## Output layout

```text
raw/
  topics_snapshot_<run_id>.jsonl
  data_batch_<run_id>_s<start_ms>_e<end_ms>_p<page>.jsonl

quarantine/
  bad_records.jsonl

.state/
  checkpoint.json
  ingestion_state.sqlite3
  ingest.lock
```

Each accepted notice line is wrapped as:

```json
{
  "_ingestion_meta": {
    "ingested_at_utc": "2026-07-23T19:00:00Z",
    "source_endpoint": "/notices/search.json",
    "source_window": {
      "start_ms": 0,
      "end_ms": 1000
    },
    "source_page": 1,
    "source_record_id": "491"
  },
  "record_data": {
    "id": "491",
    "type": "DMCA"
  }
}
```

Malformed records are written to `quarantine/bad_records.jsonl` with the error, source page/window, index, and original payload.

Raw, quarantine, and checkpoint files are atomically created with owner-only file permissions (`0600`) on operating systems that support POSIX permission bits. Lumen data can include legally sensitive material and links; store the output on access-controlled, encrypted storage and do not automatically visit returned URLs.

## Durability and idempotency

### `checkpoint.json`

Atomically replaced after every durable page. It records:

- Run ID and status.
- Fixed snapshot bounds.
- Pending/current time windows.
- Exact next page.
- Initial pagination totals.
- Completed windows and artifact paths.
- Record, quarantine, and batch totals.
- Last error.

### SQLite state

SQLite provides stronger recovery than JSON alone:

- `seen_records`: global unique notice IDs, preventing duplicates across overlapping ranges and restarts.
- `window_record_ids`: unique IDs observed inside each leaf range, used to detect unstable or duplicate pagination.
- `completed_pages`: exact completed range/page and the API's `next_page` value.
- `quarantine_records`: deterministic quarantine registry.
- `artifacts`: one-time reference datasets such as topics.

If a batch file and SQLite transaction complete before `checkpoint.json` is refreshed, the next run advances from SQLite without re-requesting the completed page.

### Snapshot drift checks

For every leaf window, the runner verifies that:

- `total_entries` and `total_pages` remain constant across pages.
- The sum of response records across completed pages equals `total_entries`.
- Unique valid IDs plus quarantined record occurrences also equal `total_entries`, detecting duplicate or unstable page results.

A mismatch stops the run with `SnapshotDriftError` instead of silently accepting a moving or inconsistent page set.

## Installation

Python 3.10 or newer is required.

```bash
python -m venv .venv

# Linux/macOS
source .venv/bin/activate

# Windows PowerShell
# .venv\Scripts\Activate.ps1

pip install -r requirements.txt
cp .env.example .env
```

Edit `.env`:

```dotenv
LUMEN_API_KEY=your_real_research_token
LUMEN_USER_AGENT=my-lumen-study/1.0 (+your-monitored-email@example.edu)
LUMEN_TERMS_ACCEPTED=true
LUMEN_LARGE_PROJECT_CONTACTED=true
```

Never commit `.env`.

## Probe authentication and response shape

`--probe` deliberately searches only the most recent one-hour range inside the configured snapshot bounds. It does not create state or issue a corpus-wide query.

```bash
python ingest_data.py --probe
```

## Run and resume

```bash
python ingest_data.py
```

After a crash or planned stop, run the same command. The exporter resumes from the saved range and exact next page.

```bash
python ingest_data.py
```

Print local progress without contacting Lumen:

```bash
python ingest_data.py --status
```

Skip the topics reference request:

```bash
python ingest_data.py --skip-topics
```

Reset only checkpoint/SQLite state (raw files remain):

```bash
python ingest_data.py --reset-state --yes
```

## Common failures

### HTTP 401/403

Verify the API key and confirm it is authorized for research access. The script applies `X-Authentication-Token` to every request.

### HTTP 429

Possible causes include exceeding the active throttle, an incorrectly applied token, or an unacceptable User-Agent. The client honors `Retry-After`, backs off with jitter, and eventually stops rather than retrying forever.

### HTTP 500

- `REQUEST REFUSED (500)`: the window is split automatically.
- A 500 response containing Lumen's documented blocking language stops immediately instead of retrying and worsening the block.
- Other persistent 500 responses: may indicate a server problem; the job stops after bounded retries.

### One-millisecond range exceeds 10,000

The documented partition key is exhausted. Contact Lumen for a narrower server-side partition, approved bulk access, or a research scope change.

### Configuration changed during resume

The checkpoint stores a fingerprint of scope, endpoint, sort, output paths, and query filters. Restore the original settings or intentionally start over with `--reset-state --yes`.

## Verification

Install development dependencies and run:

```bash
pip install -r requirements-dev.txt
python -m compileall -q .
pytest -q
```

The included tests cover:

- Date-window splitting and overlap.
- Notice/topic validation and quarantine.
- Search response parsing.
- API authentication headers.
- End-to-end mocked pagination, raw JSONL writes, topics capture, checkpoint completion, and deduplication.

A live authenticated test is intentionally not included because the package does not contain your API key.
