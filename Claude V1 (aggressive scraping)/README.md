# charities.gov.sg scraper

Scrapes the Singapore Charity Portal Advance Search for each Status of Charity and
enriches every charity with its **Organisation Profile** and **Financial Information**
detail panels, then writes an enriched `.xlsx`.

## How it works
- **List** comes from `POST /_layouts/15/CPInternet/AdvanceSearchHandler.ashx`
  (a DataTables request). The status filter is `?advType=0&type=<CODE>` inside the
  `query` field; pagination is `start`/`length`. Each record carries a GUID
  (`CharityAccountCRMRecordID`).
- **Detail panels** come from `GET /_layouts/15/CPInternet/SearchResultHandler.ashx?query=<Q>&type=<PANEL>`,
  where `<Q>` is `base64(GUID)` and `<PANEL>` is `Organisation Profile` or
  `Financial Information`. (Confirmed: the base64 of GUID `d5916f1a-…` reproduces
  your example URL exactly.)

## Install
```bash
pip install requests openpyxl
```

## Run
```bash
# 1) Offline sanity check (no network) — proves URL/base64 logic
python charity_scraper.py selftest

# 2) Confirm which `type` code maps to which status (run once, needs network)
python charity_scraper.py discover
#    -> update STATUS_TYPE_CODES at the top of the script with what it prints

# 3) Scrape + build Excel for a status (resumable)
python charity_scraper.py all --status registered
python charity_scraper.py all --status ipc
python charity_scraper.py all --status deregistered
python charity_scraper.py all --status exempt
python charity_scraper.py all --status deexempted
```
Output: `Registered_Charities_enriched.xlsx`, etc.

## Output shape
The exported `.xlsx` has three sheets, built from the real field names of the
`Organisation Profile` and `Financial Information` responses (not a generic flatten,
and no raw JSON blobs anywhere):

- **Charities** — one row per charity: the list fields (with `NameofURL`/`OtherURL`/
  `Profile_Website` placed prominently near the front, not buried at column 60+) plus
  `Profile_*` and `Financial_*` fields. People (`Profile_GoverningBoard_Names`,
  `_KeyOfficers_Names`, `_Patrons_Names`) and `Financial_Summary_Text` render as genuine
  multi-line paragraphs — one person/year per line, wrapped, with the row height
  auto-sized to fit — not JSON and not a semicolon-joined blob. Any field the API
  returns that isn't explicitly mapped still shows up as `Profile_extra_*` /
  `Financial_extra_*` — nothing is silently dropped even if the schema changes.
- **Financial Detail** — tidy long format, one row per (charity, table, line item,
  financial year): e.g. `Receipts / Total Donations in Cash / Oct 2024-Sep 2025 / 3143420`.
  Covers all four breakdown tables (Receipts, Expenses, Balance Sheet, Other Information)
  for any number of years without exploding into hundreds of columns.
- **Governance** — one row per person (Board Member / Key Officer / Patron) per
  charity, with name and designation.

## Speed / multithreading
Detail-panel fetching (the ~4,900-request part for the full Registered list) runs on a
thread pool via `--workers` (default 6). List-fetching stays single-threaded (only ~50
requests, and inherently sequential pagination). Roughly:
- Sequential (old behavior): ~2 requests/charity, ~1s each ≈ 80+ minutes for 2,446 charities.
- `--workers 6` (default): ~6x throughput ≈ 12-15 minutes, typically.
- Higher `--workers` = faster, but more aggressive against the site's WAF. The built-in
  **adaptive circuit breaker** pauses *all* threads for a cooldown period if requests
  start failing in a row across multiple threads at once (a strong throttling signal),
  so pushing `--workers` up is safe to experiment with — it backs off automatically
  instead of hammering a server that's already pushing back.

## Troubleshooting: it looks stuck / list stopped early / crashed with HTTP 429
The site sits behind a WAF (DOSarrest) with a fairly strict rate limit — confirmed
directly: after ~5-6 rapid requests it starts returning either silently-empty pages
or an explicit **HTTP 429**. Both are handled automatically now:
- Output is flushed immediately (no more silent-looking gaps).
- A short/empty list page OR a request that fails outright (including a 429 that
  exhausts its own internal retries) is treated as throttling and retried with
  backoff — **not** a crash and **not** mistaken for "the list is done". A 429 is
  a stronger signal than an empty page, so it escalates the backoff faster.
- If the server sends a `Retry-After` header, it's honored directly instead of guessing.
- Both the list AND every per-charity detail panel are cached incrementally, so
  **Ctrl+C, a crash, or hitting the stall limit never loses progress** — re-run the
  exact same command and it resumes instead of starting over.
- Any other unexpected error is now caught at the top level too: you'll get a clean
  one-line message instead of a Python traceback, and the same "just re-run it"
  guarantee applies.

**Given the confirmed strict limit**, if you see heavy throttling, dial things back:
`--workers 3 --delay 1.0` (or even `--workers 2 --delay 1.5`) trades some speed for
being gentler — multithreading only helps throughput up to the point the server's
rate limit allows; past that, more workers just means hitting 429s faster (the
adaptive throttle absorbs this safely, but it's still wasted effort). `--verbose`
shows every single detail-panel request as it's sent, useful for pinpointing exactly
which request is slow/stalling.

## Good to know
- **Only `type=100000000` (Registered) is confirmed.** The other four are best-guess
  placeholders — run `discover` once and fix them. `discover` prints the actual
  `CharityStatus` returned by each code.
- **Resumable / cached** at both the list level and per-charity-panel level. Every
  response is cached under `./charity_cache/`. Use `--refresh` to force a full re-fetch.
- **Test small first:** add `--limit 5` to any `scrape`/`all` command.
- **Column mapping was reverse-engineered from a real captured example** (CARITAS
  Humanitarian Aid), not guessed. If another charity type (e.g. an IPC, or one with
  Patrons) returns extra fields, they'll land in the `*_extra_*` columns — worth a
  quick look after your first full scrape in case anything's worth promoting into a
  named column.
- **`NameofURL`/`OtherURL` being blank is real data**, not a bug — confirmed against
  the live API response for essentially every registered charity sampled so far.
  `Profile_Website` (a different field, from the Organisation Profile panel) is the
  one that's usually actually populated.


## Sub-commands
| Command | Network? | What it does |
|---|---|---|
| `selftest` | no | Validates base64/URL construction against a known example |
| `discover` | yes | Probes `type` codes and prints the status each returns |
| `scrape --status X` | yes | Fetches list + both panels for a status; caches everything |
| `export --status X` | no | Builds the enriched `.xlsx` from cache |
| `all --status X` | yes | `scrape` then `export` |
