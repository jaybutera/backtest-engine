# viz

The web UI for reading a backtest. It renders every trade on the candles it was
graded against, marked with entry, stop, target and fill, plus an equity curve
and a control panel that can launch a run.

    cargo build --release
    scripts/backtest.sh --from 2024-01-01 --to 2024-12-31
    uv run viz serve

Then open the printed URL.

Nothing here talks to a broker, an exchange, or any network service. Both of
its inputs are files on local disk: the JSON report the engine writes, and the
parquet candle files that report was produced from. If the report is missing,
the pages render empty with a hint rather than erroring.

## Pages

| Route | What it is |
|---|---|
| `/` | Trade list plus chart, for the latest run |
| `/<run>` | Same page for a named run (see "Multiple runs") |
| `/equity` | Cumulative P&L in R, or a dollar curve when the strategy compounds |
| `/exhibit` | A self-contained chart permalink: markers travel in the URL |

`/exhibit` is the citation target. Every marker, level and label is encoded in
the query string, so a link to one trade renders from the URL alone and stays
readable after the run that produced it has been overwritten.

Every page works on a phone. One breakpoint at 860px (in `theme.css`, shared by
all of them) switches to the narrow layout: controls grow to 44px targets, the
label/field grids collapse to one column and trade rows stack. On the dashboard
the 500px sidebar and the chart become two panes with a tab bar between them,
and the backtest config folds behind its section header so the trade list is
above the fold. Hover styling is gated on `hover: hover` rather than on width,
since on a touchscreen `:hover` sticks to whatever was tapped last.

## The report contract

`scripts/backtest.sh` passes `--json-sidecar` to the binary, which writes
`data/backtest_trades.json`. That file is the entire interface between engine
and UI; anything writing the same shape can be visualized here.

Top level:

```json
{
  "label": "example",
  "rr_target": 2.0,
  "min_score": 1.0,
  "opportunities_seen": 812,
  "opportunities_taken": 96,
  "trades_decided": 94,
  "wins": 41,
  "losses": 53,
  "inconclusive": 2,
  "win_rate": 43.6,
  "expectancy": 0.11,
  "total_r_pnl": 10.4,
  "gross_r_pnl": 14.9,
  "total_fees": 4.5,
  "use_fees": true,
  "by_signal_type": { "<name>": { "wins": 0, "losses": 0, "inconclusive": 0 } },
  "by_asset":       { "<name>": { "win": 0,  "loss": 0,   "inconclusive": 0 } },
  "skips":          { "<reason>": 0 },
  "compound": null,
  "hybrid_fill_paths": { "<counter>": 0 },
  "resting_intervals": [["2024-03-04T14:30:00", "2024-03-04T14:41:00"]],
  "sources": null,
  "trades": []
}
```

Each element of `trades`:

| Field | Meaning |
|---|---|
| `opportunity_id` | Stable id; what a permalink names the trade by |
| `signal_type` | Which signal opened it |
| `asset` | Asset id — also the candle file stem, unless a source override redirects it |
| `timeframe` | Timeframe the signal fired on |
| `direction` | `bull`/`bear` or `long`/`short`; both spellings are accepted |
| `entry` | The level the order was placed at |
| `fill` | Where it actually filled; may differ from `entry` under slippage |
| `stop`, `tp` | Stop and target levels |
| `score` | Strategy's own score for the setup |
| `opened_at` | Signal time, naive ISO 8601, UTC |
| `filled_at` | When the entry filled; can be many bars after `opened_at` |
| `closed_at` | Exit time; null while open |
| `result` | `win` \| `loss` \| `inconclusive` |
| `r_pnl` | Net P&L in R-multiples |
| `fee_r` | Fees charged, in R |
| `gross_r_pnl` | `r_pnl + fee_r` |
| `equity`, `pnl_dollars` | Balance after the trade and its dollar P&L; null unless the strategy compounds |
| `ready_at` | When the setup became final; null unless the strategy stamps it |

Timestamps are naive ISO 8601 read as UTC. The UI displays them in the timezone
set by `CENTRAL_TZ` in `dashboard.html`.

### Reverse-computed exit prices

The report carries no exit *price* — R is the engine's unit of account. The UI
derives one so the chart markers land on the right candle:

    risk  = |entry − stop|
    exit  = fill ± gross_r_pnl × risk        (+ for long, − for short)

Anchoring at `fill` rather than `entry` matters when the two differ, and using
*gross* R rather than net matters because net R folds fees into the distance —
which would paint a stop-out slightly past the actual stop.

### `sources`

Optional. When a run redirects an asset to different candle files, the engine
records the mapping so the chart renders the same series the trades were
generated on:

```json
"sources": {
  "EXAMPLE": { "files": ["OTHER_STEM"], "scale": 1.0, "offset": 0.0 }
}
```

The UI applies `price = raw × scale + offset` when charting, splices multiple
stems in declared order (first file to provide a timestamp wins), and labels
the series with the stems underneath — so an asset id never passes for a feed
it was not graded on. Absent or null means every asset reads its own file raw.

## Chart data

Candles come from `data/<STEM>_1m.parquet`, with `data/historical/` as a
fallback so archival files can be kept out of the working directory. Column
names resolve case-insensitively, matching the engine's loader:

| Column | Accepted names | |
|---|---|---|
| timestamp | `timestamp`, `ts`, `time`, `datetime`, `date` | required |
| open | `open`, `o` | required |
| high | `high`, `h` | required |
| low | `low`, `l` | required |
| close | `close`, `c` | required |
| volume | `volume`, `vol`, `v` | optional, defaults to 0 |

Timestamps are naive UTC. A file missing any required column is skipped rather
than raising, since a run can splice several stems and only some need to exist.

## Multiple runs

The default page reads `data/backtest_trades.json`, or whatever `BT_JSON`
points at. Any file named `data/backtest_trades.<name>.json` becomes a second
run, served at `/<name>` and listed in the run picker. Keeping a comparison
around is one copy:

    cp data/backtest_trades.json data/backtest_trades.baseline.json

Discovery happens per request, so a file dropped in while the server is up
appears on the next reload. `.meta.json` and `.seg<N>.json` are the server's
own side-files and never show as runs.

## Running a backtest from the page

The control panel crosses three preset axes and POSTs them to
`/api/backtest/run`, which launches `scripts/backtest.sh` in the background and
reports progress through `/api/backtest/status`. The three axes are independent
by construction:

* **strategy** — `config/strategy/*.toml`: algorithm parameters and the asset list
* **fill** — `config/fill/*.toml`: how entry fills are simulated
* **dataset** — `config/datasets/*.toml`: which files back each asset

Each dropdown entry's description is the leading `#` comment block of the
preset file, so documenting a preset is a matter of commenting it.

Two combinations are refused rather than run:

* A strategy declaring its own `[[source]]` tables paired with a dataset that
  also declares sources. Composition would silently drop the strategy's
  mapping and grade its parameters on the other feed.
* A strategy whose `[viz] requires_dataset` names the datasets its numbers were
  measured on, paired with one not in that list.

Both are overridable with `allow_dataset_override`, which records the choice on
the run so the result is badged as a deliberate mix rather than passing for the
preset's published pairing.

Warmup depth is never a UI choice. It comes from the strategy's
`[viz] warmup_days`, because shifting the warmup window changes what detector
state exists at the first real candle, which changes which setups are visible
at all. A window longer than a year on a warmup-annotated preset runs as
stitched year segments, each re-warmed before its start, so every year of the
chart is the same object as a single-band run rather than a years-deep engine
state.

The `[viz]` table is read here and ignored by the engine.

### A run belongs to the server, not to the browser

`POST /api/backtest/run` returns as soon as the subprocess is spawned. Nothing
after that depends on the caller staying connected, so closing the tab, locking
the phone or dropping off wifi cannot interrupt a run — and a page that comes
back re-attaches by asking `/api/backtest/status` what is going on.

State lives in two places, and the second is what makes it durable:

* in memory on the server, and
* mirrored to `<report-stem>.run.json` next to the sidecar, rewritten on every
  state change (launch, each segment, outcome).

`.run.json` describes the RUN — progress, pid, outcome — from the moment it is
launched. `.meta.json` beside it describes the RESULT and is written only on
success. `<report-stem>.run.log` holds the run's stderr, truncated at the start
of each run. None of them is committed; `data/` is ignored.

A run does not write the report itself. `BT_JSON` points the engine at
`<report-stem>.run-<run_id>.json`, and that file is moved into place only once
the run has succeeded — a rename within one directory, so a page reading the
report gets either the previous result or this one, never the half-written file
the engine is still filling in. It also makes authorship provable: a sidecar at
the published path could have been written by anything, `scripts/backtest.sh`
run by hand while the server was down most of all, and reconciling an orphan by
asking whether the report is newer than the run would then label a manual run's
numbers with the dead run's axes and uid. A file only that run's `BT_JSON`
names cannot be confused for one. The consequence to know about: a run
orphaned by a server that never comes back leaves its result staged rather than
published, until a server does come back and settles it.

Three things, together, are what let the run outlive the server. Any one of
them missing kills it:

* **`start_new_session=True`** — so a Ctrl-C or SIGTERM aimed at the server's
  process group does not reach the child.
* **a plain `subprocess.Popen`, not an asyncio child** — when the loop shuts
  down, asyncio finalizes the child's transport, and
  `BaseSubprocessTransport.close()` **SIGKILLs a child that is still running**.
  That kill comes from us, so a new session is no defense against it. Exit is
  awaited by polling `poll()`, which owns nothing and so kills nothing.
* **stderr to a file, not a pipe** — a pipe's only reader is the server. Once
  the server is gone the read end closes, and the engine logs progress to
  stderr for the whole run, so the orphan would panic on its next line (Rust
  ignores SIGPIPE, so the write raises rather than being dropped). A file has
  no reader to lose. It is also a tighter memory bound than the pipe it
  replaced, which buffered a whole run's stderr in the server.

On startup the server reconciles whatever `.run.json` says was in flight:

| Found | Reported as |
|---|---|
| pid alive, single segment | `running`, `adopted: true` — watched to completion, then settled below |
| pid alive, stitched run | held until it exits, then `interrupted` — the later segments and the merge were the dead server's job |
| pid gone, staged result present | `done` — a run that finished while nothing was watching is still a result: it is published now, and gets its `.meta.json` |
| pid gone, nothing staged | `interrupted` |

The last two rows are the same decision whichever path reaches them, which is
why both go through `_settle_orphan_run`: there is no exit code to read for a
process we did not fork, so "did it produce a result?" is answered by whether
the run's own staged file is there. A record from before staging existed (no
`staged` marker) falls back to comparing the report's mtime against the run's
start, which is a guess — that is what staging replaced.

`status` is one of `idle` / `running` / `done` / `error` / `interrupted`.
`interrupted` exists so half a run is never mistaken for a result, and never
silently mistaken for nothing having happened. A finished run stays reportable
for 24h (`RUN_RESULT_TTL_SECONDS`), so a phone that slept through a long run
still gets its outcome on return.

`run_id` is the handle. A page stores the id of the run it launched, which is
how it tells "the run I started finished while I was away" from a run started
on another device — the latter it attaches to and labels as such. Launching
while a run is in flight returns `409` carrying that run's id, so the second
page attaches instead of reporting a failure nobody caused. The refused page
does not adopt that id as its own — a run it did not start is labeled as
somebody else's for as long as it watches it.

## API

| Endpoint | Returns |
|---|---|
| `GET /api/runs` | Discovered runs, in picker order |
| `GET /api/trades` | A run's trades, its `compound` block, `sources`, and the axes that produced it |
| `GET /api/chart` | Candles for one trade's window |
| `GET /api/sources` | Strategy, fill and dataset presets with descriptions |
| `GET /api/backtest/range` | Date range the selected axes can cover |
| `POST /api/backtest/run` | Launch a run; returns immediately with a `run_id` and an ETA, or `409` naming the run already in flight |
| `GET /api/backtest/status` | `status`, progress, `run_id`, and the last run's axes — this is the re-attach endpoint |

`/api/traders` is an alias of `/api/runs`, kept because the frontend still asks
for it by that name.
