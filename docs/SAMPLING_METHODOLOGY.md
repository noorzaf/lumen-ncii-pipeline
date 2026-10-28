# Stratified Sampling Methodology — Lumen Database Notices (2010–2026)

This document explains, precisely and honestly, how `stratified_sample.py`
partitions time windows, allocates sample size, selects pages, and handles
errors — so it can be reviewed and defended. Every claim below that could be
checked has been checked: either against Lumen's own published API
documentation, or empirically via simulation (since this environment cannot
reach `lumendatabase.org` directly). Where something is a genuine limitation
rather than a solved problem, it's stated as such.

## 1. The hard constraint everything else works around

Lumen's documentation states plainly: *"Lumen is unable to return past the
10,000th result due to limitations in Elasticsearch... querying deeper than
`page=1000` (with `per_page` of 10) will fail."* This is a per-query result
ceiling, not a pagination artifact — raising `per_page` does not get past
it, it only means the same ceiling is reached in fewer pages. Every
partitioning decision below exists to keep each queried time window's real
result count at or under this ceiling (10,000).

## 2. Two-level stratification: year, then month

**Why year first:** the research question is "collect a proportional cross
section of notices from 2010 through 2026." A grand total (e.g. 250,000) is
split across years in proportion to each year's real notice volume:

```
year_target = round(grand_total * year_total / sum(all_years_total))
```

More recent years genuinely contain more notices, so they receive
proportionally more of the sample automatically — this is not a separate
mechanism, it falls out of the formula above.

**Why month next:** within a single year, volume is not remotely uniform.
Real probed data for one recent year showed month totals ranging from
~700,000 to **9,948,812** in a single month — a >14x swing. Partitioning
using measured month-level volume (rather than assuming uniform density
across the whole year) is what lets the partitioner size sub-windows
correctly for a burst month without either (a) missing the burst's true
weight, or (b) building thousands of unnecessarily tiny windows in the
other eleven ordinary months.

Probing all 12 months of a year costs 12 live requests. This is not
overhead spent "just in case" — the same 12 numbers are also what makes the
proportional year target correct in the first place, so they are paid for
regardless.

## 3. Window slicing below the month level: parent-child inference

A month whose real total exceeds 10,000 must be split further. The naive
way to do this is **blind recursive bisection**: query the whole month,
split in half if it's too big, query both halves, split further if still
too big, and so on — verifying every node in the resulting binary tree,
including every internal (non-leaf) node. For a tree with `k` final leaves,
that costs roughly `2k − 1` requests: about double what's structurally
necessary.

**The fix — only ever query one side of a split:**

```
known_total(parent) = 12,000        (already known)
query left half  -> 6,300 (1 request)
infer right half -> 12,000 - 6,300 = 5,700   (0 requests)
```

If the *inferred* side comes out to exactly 0, it is skipped entirely —
no request is ever sent for a range that arithmetic already proved is
empty. If the inferred side is non-zero but still needs pagination metadata
(`total_pages`, `per_page` — which can only ever come from a real response,
never be inferred), it costs exactly one request when it is actually
accepted as a leaf, not one request *per candidate check* on the way there.

This is implemented in `_materialize_window()`. It was verified with a
synthetic test: a window with a genuinely empty right half was recursively
split, and the empty half was **never queried** — confirmed by asserting
its exact `(start_ms, end_ms)` pair never appeared in the fake client's
call log.

### Measured savings (not just theoretical)

A full simulated year (11 months at ~1,000,000 notices each + 1 burst
month at 9,900,000, modeled after real observed data) was partitioned both
ways:

| Method | Requests spent | Leaves produced |
|---|---|---|
| Naive blind full-year bisection | 6,533 | 3,267 |
| Month-informed + parent-child inference | 4,160 | 2,944 |
| **Reduction** | **36.3% fewer requests** | — |

(These numbers came from a simulation run during development against a fake
client, not from hand estimates. That simulation script is not included in
this repository.)

## 4. Probe caching

Every real `(start_ms, end_ms) -> total_entries` result is stored in a
`WindowCache` for the duration of a run (and persisted in the checkpoint
across restarts). If the exact same window is ever needed again — which can
happen on a resumed run, or if a correction pass revisits a boundary — it
is never re-queried. This costs nothing to maintain and only ever saves
requests, never adds risk.

## 5. Budget-aware, graceful degradation (not a hard cliff)

`--request-budget N` caps total live requests for a run. As usage
approaches that cap, the *soft* target leaf size (normally ~95% of the hard
10,000 ceiling) relaxes upward, automatically accepting fewer, larger leaves
instead of continuing to split as finely. Verified empirically:

| Budget | Requests used | Leaves | Average leaf size |
|---|---|---|---|
| Unlimited | 4,160 | 2,944 | 7,099 |
| Capped at 2,000 | 3,648 | 2,432 | 8,593 |

In both cases every single leaf still respects the 10,000 ceiling — the
degradation trades leaf-size uniformity for request count, never data
safety.

## 6. Page selection: probability-proportional-to-size (PPS)

Within a leaf, pages are not all the same size — the last page of a
10,000-record leaf at `per_page=10` might hold only 4 records if
`10,000 mod 10 != 0`, or more generally, page sizes vary across the
boundary corrections above. Selecting pages with uniform probability biases
the sample toward records that happen to sit on small pages.

Instead, every candidate page is given an Efraimidis-Spirakis weighted
reservoir key, `u ** (1 / page_size)` for `u ~ Uniform(0,1)`, and pages are
consumed in descending-key order. This is a standard streaming technique
for weighted sampling without replacement: it makes each page's selection
probability proportional to its size, in a single O(n log n) pass, without
ever needing to know how many pages will ultimately be drawn in advance.

**Empirically verified:** a leaf with two 20-record pages and one 2-record
page was ranked first 20,000 times. The tiny page came out first 4.8% of
the time — matching its true share of records (2/42 = 4.8%) almost
exactly, versus the ~33% (1-in-3) that uniform-page selection would have
given it.

## 7. Within-page selection: true random subsampling, not first-N

When a drawn page has more records than are still needed to hit a target,
the previous approach took the first N records off that page. Since
Lumen's ordering is deterministic, this would always select the same
records within any page that's only partially consumed. The current
implementation uses `random.sample()` to draw the exact number still
needed from anywhere within that page. Verified empirically: selection
counts across all 20 positions of a test page were statistically uniform
(~1,250 ± 5% each across 5,000 trials), with no bias toward the first few
positions.

## 8. Deduplication and permanent failure handling

- Every accepted notice `id` is added to a per-year `seen_ids` set,
  persisted in the checkpoint, so a crash-and-resume cannot introduce
  duplicates that a purely in-memory set would miss.
- A page that fails (malformed response, refused query, exhausted
  retries) is added to a permanent, checkpoint-persisted `failed_pages`
  set and is never attempted again in this project — there is no
  "keep retrying the same bad page" loop, because the PPS-ranked
  candidate stream is consumed exactly once, in order, with failures
  simply skipped rather than re-queued.

## 9. API/schema drift detection

If more than `--max-quarantine-ratio` (default 50%) of processed records
in a year are quarantined (failing schema validation) after a minimum
number of pages, the year stops immediately with a clear error rather than
silently burning through the entire sample pool on data that's failing
validation for a systemic reason (e.g., a renamed field). This directly
targets the failure mode where an API change would otherwise cause mass,
silent data loss.

## 10. Known, honestly-stated limitations (not solved, and why)

- **Year totals can drift during a multi-day run.** Lumen has no
  versioning or consistency token. Each year's probe is timestamped;
  sampling a year long after its probe triggers a loud staleness warning
  rather than silently trusting old proportions. `--reprobe-year` forces
  a fresh probe. Full automatic re-normalization mid-run is not
  implemented — it would require re-probing (and re-costing requests for)
  every year, which conflicts directly with the request-minimization goal.
- **Page content can drift between being counted and being fetched**,
  since Lumen has no snapshot/cursor mechanism. Mitigated by sorting on
  `date_received asc` (far more stable under inserts than relevance
  ordering) and by id-based deduplication, but not eliminated.
- **The uniform-density assumption inside a single split** (used only
  when *inferring* a sibling's count, never when constructing a final
  leaf, which is always separately verified) can be locally wrong within
  a burst region smaller than the current split width. This just costs a
  few extra correction-pass requests when it happens — it never produces
  an invalid (over-ceiling) leaf, because every leaf's final acceptance
  requires a real, verified request.
- **`per_page` above the documented default of 10 is untested against the
  live API.** `--probe-per-page 10 25 50 100 200` exists specifically to
  test this safely (a handful of requests, no commitment) before a full
  run — if a higher value is honored by the server, total fetch requests
  (and thus total run time) drop proportionally, and this matters far more
  than any partitioning optimization above, since the fetch phase
  (`grand_total / per_page` requests) dominates total request count for
  any realistically-sized sample.

## 11. Where the request budget actually goes

For a 250,000-record target sample at `per_page=10`:

| Phase | Approx. requests |
|---|---|
| Month probing (12 × 17 years) | ~200 |
| Leaf materialization (informed + inference, dominated by the 2-3 busiest years) | ~5,000–15,000 |
| Fetching 250,000 target records | 250,000 ÷ 10 = 25,000 |
| **Total** | **~30,000–40,000** |

The partitioning work described in this document reduces the *middle* row
by roughly a third. It does not reduce the *bottom* row, which is fixed by
`grand_total ÷ per_page` and dominates the total — which is exactly why
testing a larger `per_page` (§10, last point) is the single highest-value
remaining lever, not a further refinement of window-slicing strategy.
