"""
modal_app.py -- Runs the exhaustive Lumen full-corpus crawl on Modal.

WHY THIS SHAPE (read before deploying):

  * Modal caps any single Function call at 24h (hard platform limit). Your
    full-corpus crawl is estimated at ~205-215 hours (~8.5-9 days) of
    continuous 1-req/sec fetching. So this can't be one call -- it's a
    CHAIN of bounded calls. `crawl_chunk` runs for up to CHUNK_SECONDS,
    checkpoints to the Volume, and if the crawl isn't done yet, spawns the
    *next* chunk itself before returning. You kick off the first one and
    then can walk away -- no laptop, no open terminal required.

  * You have exactly ONE Lumen API token, rate-limited to ~1 req/sec. That
    limit is per-token, not per-machine -- running things in parallel
    doesn't get you more throughput, it just risks two requests landing in
    the same second and getting you rate-limited or flagged. So
    `max_containers=1` here is not a cost-saving default, it's a
    correctness requirement: it guarantees Modal never runs two overlapping
    containers against your single token.

  * Storage is a Modal Volume, mounted at /data. JSONL batch files land in
    /data/notices, checkpoint state in /data/state. `volume.commit()` is
    called after every checkpoint save inside full_crawl.py's save_all()
    equivalent -- actually see note below, commits happen at the chunk
    boundary here, which is the safe/simple version; see "COMMIT FREQUENCY"
    below if you want tighter durability during a single chunk.

Deploy:
    modal secret create lumen-crawler-secret \\
        LUMEN_API_KEY=<your real token> \\
        LUMEN_USER_AGENT="lumen-sample-research/1.0 (+you@example.com)" \\
        LUMEN_TERMS_ACCEPTED=true \\
        LUMEN_LARGE_PROJECT_CONTACTED=true

    modal volume create lumen-database

    modal deploy modal_app.py

Kick off the crawl (starts the self-chaining run, then returns immediately):
    modal run modal_app.py::start

Check progress any time (reads the checkpoint off the Volume, no live
requests spent):
    modal run modal_app.py::status

Pull data down locally once you want to work with it / load it elsewhere:
    modal volume get lumen-database /notices ./full_raw_local
"""

import json
import os
from pathlib import Path

import modal

APP_NAME = "lumen-full-crawl"
VOLUME_NAME = "lumen-database"
SECRET_NAME = "lumen-crawler-secret"

MOUNT_PATH = "/data"
CHUNK_SECONDS = 21 * 3600  # 21h chunks, leaving buffer under Modal's 24h cap
FUNCTION_TIMEOUT = 23 * 3600  # hard ceiling for the container itself

START_YEAR = 2010
END_YEAR = 2026

volume = modal.Volume.from_name(VOLUME_NAME, create_if_missing=True)

# Bring your existing, verified project files into the image rather than
# re-implementing anything. Adjust local_dir if your repo layout differs --
# this expects modal_app.py to sit at the project root, next to config.py,
# ingest_data.py, and the stratify/ folder (which now also contains
# full_crawl.py).
image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install_from_requirements("requirements.txt")
    # Never ship the local .env (the token comes from the Modal secret) or any
    # locally crawled notice data into the image.
    .add_local_dir(
        ".",
        remote_path="/root/app",
        ignore=[".env", ".venv", "**/*.jsonl", "**/*.sqlite3", "**/*_raw", "**/*_state", ".state", ".git"],
    )
)

app = modal.App(APP_NAME, image=image)


@app.function(
    volumes={MOUNT_PATH: volume},
    secrets=[modal.Secret.from_name(SECRET_NAME)],
    timeout=FUNCTION_TIMEOUT,
    max_containers=1,  # correctness requirement -- see module docstring
    retries=modal.Retries(max_retries=5, initial_delay=10.0, backoff_coefficient=2.0),
)
def crawl_chunk(chunk_seconds: float = CHUNK_SECONDS) -> str:
    import sys
    sys.path.insert(0, "/root/app")
    sys.path.insert(0, "/root/app/stratify")

    from full_crawl import CrawlPaths, run_bounded

    paths = CrawlPaths.under(Path(MOUNT_PATH))

    status = run_bounded(
        paths,
        start_year=START_YEAR,
        end_year=END_YEAR,
        max_runtime_seconds=chunk_seconds,
        # Durability: commit the Volume every time full_crawl.py saves a
        # checkpoint (roughly every `checkpoint_every_pages` pages), not
        # just at the end of this ~21h chunk. This protects against a
        # mid-chunk crash/OOM/host eviction losing more than one checkpoint
        # interval's worth of progress -- worth the extra commit overhead
        # given this runs unattended for over a week.
        on_checkpoint=volume.commit,
    )

    volume.commit()  # final commit for this chunk, belt-and-suspenders

    print(f"[crawl_chunk] finished chunk with status={status}")

    if status == "MORE_WORK_REMAINING":
        print("[crawl_chunk] spawning next chunk...")
        crawl_chunk.spawn(chunk_seconds=chunk_seconds)
    elif status == "COMPLETE":
        print("[crawl_chunk] FULL CORPUS CRAWL COMPLETE.")
    else:
        print(f"[crawl_chunk] FAILED -- check logs above. Not auto-retrying "
              f"further; fix the underlying issue and re-run modal_app.py::start.")

    return status


@app.local_entrypoint()
def start():
    """Kicks off the crawl chain and returns immediately -- the rest runs
    entirely on Modal, chunk after chunk, with no local process required."""
    call = crawl_chunk.spawn()
    print(f"Started. First chunk call id: {call.object_id}")
    print("This will keep re-spawning itself (~21h chunks) until the full "
          "corpus is crawled. Check progress with: modal run modal_app.py::status")


@app.function(volumes={MOUNT_PATH: volume})
def _read_status() -> dict:
    ckpt_path = Path(MOUNT_PATH) / "state" / "full_crawl_checkpoint.json"
    if not ckpt_path.exists():
        return {"status": "not started yet"}
    data = json.loads(ckpt_path.read_text())
    years = data.get("years", {})
    summary = {}
    total_fetched = 0
    total_expected = 0
    for y, s in sorted(years.items(), key=lambda kv: int(kv[0])):
        fetched = s.get("fetched_records", 0)
        expected = s.get("year_total", 0)
        total_fetched += fetched
        total_expected += expected
        summary[y] = {
            "status": s.get("status"),
            "fetched": fetched,
            "expected": expected,
            "quarantined": s.get("quarantined_count", 0),
            "shortfalls": s.get("shortfalls", 0),
        }
    summary["_TOTAL"] = {"fetched": total_fetched, "expected": total_expected}
    return summary


@app.local_entrypoint()
def status():
    """Read-only progress check -- costs zero Lumen API requests, just
    reads the checkpoint file off the Volume."""
    result = _read_status.remote()
    print(json.dumps(result, indent=2))