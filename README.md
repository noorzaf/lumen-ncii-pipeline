# Lumen NCII Exposure: Data Collection Pipeline

> **Content note:** This project concerns takedown notices about non-consensual intimate imagery (NCII). This repository contains no notice text, names, images, or links. It holds code and aggregate numbers only.

> This product uses the Lumen API but is not endorsed or certified by Lumen.

## The problem

When people ask Google to remove intimate images posted without their consent, they often file a DMCA copyright takedown. Those notices are then archived in public transparency databases such as [Lumen](https://lumendatabase.org) (Berkman Klein Center, Harvard). That archive is valuable for research, but for NCII it can create a lasting public record tied to the abuse. That record can name the person who asked for removal, describe the images, and point back to where they were posted.

This project set out to measure that problem:

1. What kinds of notices does Lumen hold, and which are most common?
2. What share of them concern NCII?
3. How much personally identifiable information (PII) do NCII notices expose?
4. Is the delisted content still reachable, including through AI systems and their training data?

This repository is the **data collection layer** I built for a supervised university research project: the code that gets notices out of Lumen reliably, politely, and at scale.

## The engineering problem

Lumen holds tens of millions of notices, but its API makes bulk collection hard:

- **It won't return past the 10,000th result of any search.** A month can hold close to 10 million notices, so a naive query silently misses almost all of it.
- **It allows about one request per second per token**, and blocks clients that push harder.
- **It has no snapshot or cursor.** Data can shift while you are paging through it.

A full crawl at that rate takes about 9 days of continuous requests, so the pipeline also has to survive crashes, restarts and machine limits without losing or duplicating data.

## What's here

**`ingest_data.py`: core API client and exporter**
- Spaces requests at least 1.1 s apart. Retries with exponential backoff and jitter, and honors `Retry-After`.
- Treats rejected tokens, blocking responses and redirects to other hosts as hard stops instead of retrying into a ban.
- Sends the token only in a header, and only to Lumen's own host, so it never ends up in logs or URLs.
- Splits date windows recursively until each one is under the 10,000-result ceiling.
- Writes atomic JSONL batches and stores progress in a checkpoint plus SQLite, so a killed run resumes from the exact page it stopped on.
- Stops the run if page totals change mid-window (snapshot drift) instead of accepting an inconsistent page set.
- Moves malformed records to a quarantine folder instead of dropping them silently.

**`stratify/count_notices.py`: reconnaissance pass**
- Gets notice counts per year, month and day for 2010–2026 from metadata alone, without downloading any notice content.
- Finds each year's first and last notice ID with just two requests per year, using ascending and descending sorts.
- These counts show how unevenly volume is spread (one month had 14 times the notices of another) and set up the sampling design.

**`stratify/stratified_sample.py`: proportional sampler**
- Splits a target sample across years and months in proportion to their real notice volume.
- Infers sibling window sizes from parent minus child, so it only queries one side of each split. In simulation this cut partitioning requests by about 36%.
- Draws pages with probability proportional to size (Efraimidis–Spirakis weighted reservoir), which fixes a bias that made records on small pages more likely to be picked.
- Samples randomly within each page instead of taking the first N records.
- Respects a request budget: under pressure it accepts larger leaf windows but never exceeds the 10,000 ceiling.
- See [`docs/SAMPLING_METHODOLOGY.md`](docs/SAMPLING_METHODOLOGY.md) for the full reasoning and its known limitations.

**`stratify/full_crawl.py` + `modal_app.py`: exhaustive crawl**
- Fetches every page of every leaf window.
- Keeps the checkpoint small: one integer per leaf window rather than a set of every notice ID seen, which would run to gigabytes at about 66 million notices.
- Runs on [Modal](https://modal.com) as a chain of 21-hour jobs that checkpoint and start the next job themselves. That works around Modal's 24-hour limit and needs no laptop left running.
- Caps the crawl at one container, so two machines can never share the single rate-limited token.

**`tests/`**: 18 tests with a mocked API covering auth headers, pagination, `Retry-After`, 401/403/500 handling, cross-host redirects, quarantine, deduplication across overlapping windows, recursive splitting and checkpoint recovery.

## What the first analysis taught me

The notices this pipeline collected went into a first exploratory analysis: PII extraction with Microsoft Presidio, a rule-based classifier that sorted notices into mass-submission, commercial-NCII, personal-NCII and other, and BERTopic topic models. That analysis code isn't in this repository. Its first pass on 2020 data found 288 commercial and 228 personal NCII notices out of about 271,000. Reviewing it showed why those numbers can't be trusted:

- **The order of the rules hid most NCII.** The classifier applied the volume rule first, so every notice filed by a removal agency landed in "mass submission." Much NCII is filed by agencies on behalf of victims, so those counts are far too low. *Who sent* a notice and *what it is about* need to be two separate labels.
- **The topics mostly captured form boilerplate.** The largest clusters came from Google's web-form text and agency templates rather than from what each notice actually said.
- **The tools were never checked against human labels.** Without a hand-labeled sample, there is no way to tell how often Presidio or the keyword rules are right.
- **Notices from the same day are not independent.** Notices arrive in batches, so the unit for uncertainty estimates has to be the sampled day, not the individual notice.

The project has since moved to a new study design with ethics review. It builds on this collection pipeline.

## Ethics and data handling

- **No notice data is in this repository.** That includes raw notices, extracted entities, URLs and checkpoints. `.gitignore` excludes all of them.
- Output files are written owner-only (`0600`). The pipeline never visits URLs found in notices.
- API keys come from `.env` or a Modal secret and are never committed.
- To run the code you need your own Lumen researcher API token, used under Lumen's API Terms of Use. Lumen asks researchers to contact them before large projects.

## Running it

Requires Python 3.10+.

```bash
pip install -r requirements.txt
cp .env.example .env        # add your token and a real contact in the User-Agent

python ingest_data.py --probe                     # one-hour test query, no state written
python stratify/count_notices.py --only-year 2024 # metadata-only counts
python stratify/stratified_sample.py --grand-total 50000 --seed 1
python ingest_data.py --status                    # local progress, no API calls
```

Tests (no token needed):

```bash
pip install -r requirements-dev.txt
pytest -q
```

More detail on the API behavior the client is built around is in [`docs/INGESTION.md`](docs/INGESTION.md).

## Related work

- Qiwei et al., *A Law of One's Own: The Inefficacy of the DMCA for Non-Consensual Intimate Media*, CHI 2025.
- Ibosiola et al., *Who Watches the Watchmen: Exploring Complaints on the Web*, WWW 2019.
- Levendowski, *Using Copyright to Combat Revenge Porn*, NYU JIPEL, 2014.
