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

## API

| Endpoint | Returns |
|---|---|
| `GET /api/runs` | Discovered runs, in picker order |
| `GET /api/trades` | A run's trades, its `compound` block, `sources`, and the axes that produced it |
| `GET /api/chart` | Candles for one trade's window |
| `GET /api/sources` | Strategy, fill and dataset presets with descriptions |
| `GET /api/backtest/range` | Date range the selected axes can cover |
| `POST /api/backtest/run` | Launch a run; returns immediately with an ETA |
| `GET /api/backtest/status` | Progress, and the last run's axes |

`/api/traders` is an alias of `/api/runs`, kept because the frontend still asks
for it by that name.
