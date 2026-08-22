"""aiohttp web server rendering an engine run's trades on candle charts.

Everything served here is read off local disk: the JSON sidecar the replay
binary writes, and the parquet candle files the run was graded on. The one
thing the server WRITES is a backtest run — `/api/backtest/run` launches
`scripts/backtest.sh` in the background and `/api/backtest/status` reports its
progress, so a run can be kicked off and watched from the page.

Three orthogonal axes compose a run, each a directory of TOML presets:

  strategy  config/strategy/*.toml  — pure algorithm: gating params + assets
  fill      config/fill/*.toml      — how entry fills are simulated
  dataset   config/datasets/*.toml  — which candle files back each asset

The UI picks each independently. A dataset with `[[source]]` tables is composed
onto the chosen strategy by writing a temporary child preset (`_write_ui_strategy`);
a source-free dataset runs the strategy exactly as written.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import tomllib
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl
from aiohttp import web

from viz.ingest import load_backtest_trades
from viz.registry import RunSpec, default_run, get_runs, resolve_run

# ── Layout ───────────────────────────────────────────────────────────────────

DATA_DIR = Path("data")
BACKTEST_PATH = Path("data/backtest_trades.json")

# viz/server.py sits one level under the repo root; the runner script sits at
# scripts/backtest.sh. Anchor to the root so the subprocess works from any cwd.
REPO_ROOT = Path(__file__).resolve().parents[1]
BACKTEST_SCRIPT = REPO_ROOT / "scripts" / "backtest.sh"

# Strategy presets: pure algorithm (trade-gating params + the asset list), no
# data sources. The run endpoint composes a strategy with a dataset by writing
# a temp child preset that `base = `s the strategy and appends the dataset's
# `[[source]]` tables. Kept in this dir so the child's relative `base` path
# resolves exactly as a hand-written preset's would.
STRATEGY_DIR = Path("config/strategy")
DEFAULT_STRATEGY = "example.toml"
# Temp child preset the UI writes; lives alongside the presets so `base` resolves.
UI_STRATEGY_PATH = STRATEGY_DIR / "_viz_session.toml"

# Dataset presets declare ONLY data sources (which candle files back each asset,
# with an optional scale/offset transform), never algorithm params and never the
# asset list. A dataset with no `[[source]]` table means "each asset loads its
# own data/<asset>_1m.parquet".
DATASET_DIR = Path("config/datasets")
DEFAULT_DATASET = "local.toml"

# Fill-lens presets: pure simulation policy — how the backtest models entry
# fills (the `[fill]` table) — orthogonal to both strategy and dataset. Passed
# to the replay binary as `--fill config/fill/<name>` via backtest.sh's
# BT_FILL env var.
FILL_DIR = Path("config/fill")
DEFAULT_FILL = "market_hybrid.toml"

# Fitted duration model (seconds vs. candle count) driving the progress bar's
# ETA. Absent or stale is fine — the estimator falls back to a linear guess.
TIMING_MODEL_PATH = Path("config/backtest_timing.json")

HTML_PATH = Path(__file__).parent / "dashboard.html"
EQUITY_HTML_PATH = Path(__file__).parent / "equity.html"
EXHIBIT_HTML_PATH = Path(__file__).parent / "exhibit.html"
# Shared stylesheet, linked by every page so the token block and the common
# component styles live in one place.
THEME_CSS_PATH = Path(__file__).parent / "theme.css"

logger = logging.getLogger(__name__)


def _backtest_uid(meta: dict) -> str:
    """Canonical single-token descriptor of a run.

    Format: `<strategy>×<fill>×<dataset>@<from>..<to>` with `.toml` stripped.
    No spaces — the UI renders it as a click-to-copy chip so a run can be
    quoted verbatim with no ambiguity about what produced it. The date range
    is part of the setup, since a window run and a full-range run of the same
    three axes report very different totals. Missing axes render as `?` rather
    than being dropped, keeping the shape parseable.
    """
    def strip(v: str | None) -> str:
        s = (v or "?").strip() or "?"
        return s.removesuffix(".toml")

    uid = (
        f"{strip(meta.get('strategy'))}×{strip(meta.get('fill'))}"
        f"×{strip(meta.get('dataset'))}"
    )
    frm, to = meta.get("from"), meta.get("to")
    if frm or to:
        uid += f"@{frm or '?'}..{to or '?'}"
    return uid


# ── TOML helpers ─────────────────────────────────────────────────────────────


def _load_toml(path: Path) -> dict:
    """Parse a TOML file, returning {} for missing or malformed input.

    Preset parsing is best-effort everywhere in this module: a broken file
    should degrade one dropdown entry, not 500 the page that lists it.
    """
    try:
        with path.open("rb") as f:
            return tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError):
        return {}


def _sources_from(doc: dict) -> dict:
    """`[[source]]` tables → {asset: {files, scale, offset}}."""
    return {
        s["asset"]: {
            "files": s.get("files", []),
            "scale": float(s.get("scale", 1.0)),
            "offset": float(s.get("offset", 0.0)),
        }
        for s in (doc.get("source") or [])
        if "asset" in s
    }


# ── Candle files and date ranges ─────────────────────────────────────────────


def _candle_path(stem: str) -> Path | None:
    """Resolve a source stem to its parquet, or None if absent.

    `data/<stem>_1m.parquet` wins; `data/historical/<stem>_1m.parquet` is the
    fallback so archival files can be kept out of the working directory.
    """
    for cand in (DATA_DIR / f"{stem}_1m.parquet", DATA_DIR / "historical" / f"{stem}_1m.parquet"):
        if cand.exists():
            return cand
    return None


def _asset_range(stem: str) -> tuple[datetime, datetime] | None:
    """(min, max) timestamp in a stem's parquet, or None when it has no rows.

    Only the timestamp column is read, and its name is resolved through the
    same alias table the candle loader uses.
    """
    path = _candle_path(stem)
    if path is None:
        return None
    try:
        schema = pl.read_parquet_schema(path)
    except Exception:
        return None
    cols = _column_map(list(schema))
    if cols is None:
        return None
    ts = cols["timestamp"]
    df = pl.read_parquet(path, columns=[ts])
    if df.height == 0:
        return None
    return df[ts].min(), df[ts].max()


def _asset_range_for(asset: str, dataset_sources: dict) -> tuple[datetime, datetime] | None:
    """(min, max) timestamp for `asset` under the selected dataset.

    When the dataset redirects the asset to donor file stem(s), span the union
    of those stems' ranges, matching the loader's splice. Otherwise fall back
    to the asset's own parquet.
    """
    src = dataset_sources.get(asset)
    stems = src.get("files") if src else None
    if stems:
        mins: list[datetime] = []
        maxs: list[datetime] = []
        for stem in stems:
            r = _asset_range(stem)
            if r is not None:
                mins.append(r[0])
                maxs.append(r[1])
        if not mins:
            return None
        return min(mins), max(maxs)
    return _asset_range(asset)


def _range_over(
    assets: list[str], dataset_sources: dict | None, union: bool,
) -> tuple[date | None, date | None]:
    """Date range covered by `assets` under `dataset_sources`.

    `union=False` is the INTERSECTION — the window where every asset has data
    (max of the mins, min of the maxes). It is the conservative default: a run
    over it grades every asset over the same calendar.

    `union=True` spans the full history of ANY asset (min of mins, max of
    maxes). Picking it runs each asset over its own whole file, matching a
    no-date CLI run, at the cost of assets being absent for stretches where
    they have no data — which the engine simply skips.

    Source-only semantics: when a dataset declares `[[source]]` tables it IS
    the complete asset universe, so an undeclared asset does not run and must
    neither clamp nor extend the range.
    """
    dataset_sources = dataset_sources or {}
    mins: list[datetime] = []
    maxs: list[datetime] = []
    for asset in assets:
        if dataset_sources and asset not in dataset_sources:
            continue
        r = _asset_range_for(asset, dataset_sources)
        if r is None:
            continue
        mins.append(r[0])
        maxs.append(r[1])
    if not mins or not maxs:
        return None, None
    if union:
        return min(mins).date(), max(maxs).date()
    return max(mins).date(), min(maxs).date()


# ── Preset introspection ─────────────────────────────────────────────────────


def _read_strategy_assets_sources(name: str) -> dict:
    """Parse a strategy preset → {assets: [...], sources: {asset: {...}}}.

    Resolves one level of `base` inheritance, matching the engine's loader: a
    child's `assets` / `[[source]]` override the base's wholesale. This lets
    the UI seed its source editor from the SELECTED strategy's own assets and
    declared source mappings rather than from the last run's leftovers.
    """
    path = STRATEGY_DIR / name
    if not path.exists():
        return {"assets": [], "sources": {}}
    doc = _load_toml(path)
    base_doc: dict = {}
    base_rel = doc.get("base")
    if base_rel:
        base_doc = _load_toml(path.parent / base_rel)

    assets = doc.get("assets") or base_doc.get("assets") or []
    raw = doc.get("source") or base_doc.get("source") or []
    return {"assets": assets, "sources": _sources_from({"source": raw})}


def _list_presets(directory: Path) -> list[str]:
    """Preset filenames in `directory`, underscore-prefixed files excluded.

    The underscore prefix marks a file that is machinery rather than a choice
    — the composed session preset, a label registry — so it never appears in
    a picker.
    """
    if not directory.exists():
        return []
    return sorted(p.name for p in directory.glob("*.toml") if not p.name.startswith("_"))


def _list_datasets() -> list[str]:
    return _list_presets(DATASET_DIR)


def _list_fills() -> list[str]:
    return _list_presets(FILL_DIR)


def _list_strategies() -> list[str]:
    return _list_presets(STRATEGY_DIR)


def _preset_description(path: Path, cap: int = 200) -> str:
    """Short description from a preset's leading `#` comment block.

    Reads the run of `#` lines at the very top of the file, strips the comment
    markers, joins them into one sentence and caps the length, so the UI can
    show what each preset means without a separate metadata file. Returns ""
    when the file is unreadable or opens with non-comment content.
    """
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return ""
    parts: list[str] = []
    started = False
    for raw in lines:
        stripped = raw.strip()
        if stripped.startswith("#"):
            started = True
            text = stripped.lstrip("#").strip()
            if text:
                parts.append(text)
            elif parts:
                break  # blank `#` line after content ends the leading block
        elif started:
            break  # first non-comment line ends the leading block
        elif not stripped:
            continue
        else:
            break  # file starts with non-comment content
    desc = " ".join(parts)
    if len(desc) > cap:
        desc = desc[: cap - 1].rstrip() + "…"
    return desc


def _presets_with_desc(names: list[str], directory: Path) -> list[dict]:
    """[{name, desc}] for each preset filename in `directory`."""
    return [{"name": n, "desc": _preset_description(directory / n)} for n in names]


def _strategy_constraints(name: str) -> dict:
    """A strategy's dataset-axis constraints → {requires_dataset, own_sources}.

    Two independent guards against a meaningless strategy×dataset pairing:

    * `requires_dataset` — the preset's optional `[viz]` table names the only
      datasets its numbers mean anything on. DECLARED, because it expresses an
      intent the file structure cannot show.
    * `own_sources` — the preset carries its own `[[source]]` tables. This one
      is STRUCTURAL and needs no annotation: `_write_ui_strategy` copies only
      `engine` / `assets` / `[strategy]` when composing, so the preset's own
      sources would be SILENTLY DISCARDED in favour of the dataset's. Such a
      preset is already a complete strategy+data pairing and must either run
      standalone (against a source-free dataset) or not at all.

    The engine ignores `[viz]` entirely — it exists for this module.
    """
    path = STRATEGY_DIR / name
    if not path.exists():
        return {"requires_dataset": [], "own_sources": False}
    doc = _load_toml(path)
    viz = doc.get("viz")
    req: list[str] = []
    if isinstance(viz, dict):
        raw = viz.get("requires_dataset") or []
        if isinstance(raw, str):
            raw = [raw]
        # Normalise to ".toml" so the comparison works however the preset
        # spelled the dataset name.
        req = [
            (r if r.endswith(".toml") else f"{r}.toml")
            for r in raw
            if isinstance(r, str) and r
        ]
    return {"requires_dataset": req, "own_sources": bool(doc.get("source"))}


def _strategy_viz_warmup(name: str) -> int:
    """The strategy's `[viz] warmup_days` annotation, or 0.

    Warmup depth — how many days of candles are replayed before `--from` to
    build engine state, with those trades dropped — is part of a preset's
    published setup, not a per-run user choice: shifting it by a few days
    moves results, so the preset declares it and the UI offers no knob. 0 (or
    no annotation) means the binary's full-history default. A garbage value is
    treated as unannotated rather than failing the run.
    """
    doc = _load_toml(STRATEGY_DIR / name)
    viz = doc.get("viz")
    if not isinstance(viz, dict):
        return 0
    try:
        return max(0, int(viz.get("warmup_days")))
    except (TypeError, ValueError):
        return 0


def _read_dataset_sources(name: str) -> dict:
    """A dataset's `[[source]]` tables → {asset: {files, scale, offset}}.

    Datasets are flat source-only files (no `base`). A dataset with no
    `[[source]]` returns {}, meaning every asset loads its own parquet.
    Accepts both "donor" and "donor.toml" so callers spelling it either way
    resolve to the same preset.
    """
    if not name.endswith(".toml"):
        name = f"{name}.toml"
    return _sources_from(_load_toml(DATASET_DIR / name))


def _dataset_fee_schedule(name: str) -> str | None:
    """A dataset's top-level `fee_schedule` (venue fee model), or None.

    The dataset pins the fee model because the venue is a property of the
    DATA, not the strategy: a composed run must not price one venue's fees on
    another venue's prices. Propagated into the temp preset's `[strategy]`
    table by `_write_ui_strategy`.
    """
    if not name.endswith(".toml"):
        name = f"{name}.toml"
    fs = _load_toml(DATASET_DIR / name).get("fee_schedule")
    return fs if isinstance(fs, str) and fs else None


def _dataset_conflict(strategy: str, dataset: str) -> str | None:
    """Why `strategy` must not run on `dataset`, or None when the pair is valid.

    The single source of truth for the pairing rule — the run endpoint rejects
    on it, and `/api/sources` exposes the same inputs so the UI can pin its
    picker identically. A source-free dataset is always a valid partner for a
    source-carrying strategy: nothing is dropped and the preset runs as written.
    """
    c = _strategy_constraints(strategy)
    if c["own_sources"] and _read_dataset_sources(dataset):
        return (
            f"{strategy} declares its own [[source]] tables, so pairing it with "
            f"dataset {dataset} (which also declares sources) would SILENTLY "
            f"DISCARD the strategy's own data mapping and grade its parameters "
            f"on {dataset}'s feed instead. Run it against a source-free dataset "
            f"(it is self-contained), or use a source-free twin of the preset."
        )
    req = c["requires_dataset"]
    if req and dataset not in req:
        return (
            f"{strategy} is only meaningful on dataset(s) {', '.join(req)} — its "
            f"published result was measured there. Running it on {dataset} "
            f"produces a number unrelated to that baseline."
        )
    return None


def _strategy_presets_with_desc(names: list[str]) -> list[dict]:
    """Strategies as [{name, desc, requires_dataset, own_sources}].

    The two constraint fields let the UI pin the dataset axis up front, rather
    than the user discovering the mismatch inside a silently-wrong result.
    """
    out = []
    for name in names:
        c = _strategy_constraints(name)
        out.append({
            "name": name,
            "desc": _preset_description(STRATEGY_DIR / name),
            "requires_dataset": c["requires_dataset"],
            "own_sources": c["own_sources"],
        })
    return out


def _sourceless_datasets(names: list[str]) -> list[str]:
    """Datasets contributing no `[[source]]` tables.

    These are the only valid partners for a strategy that carries its own
    sources: with nothing to append, composition is skipped entirely and the
    preset runs exactly as written. The UI uses this to pin the picker.
    """
    return [n for n in names if not _read_dataset_sources(n)]


def _fill_is_tick(fill: str | None) -> bool:
    """True when the fill preset selects tick-resolution mode.

    A tick lens reads a raw trade-print archive instead of OHLC bars, which is
    both slower and only meaningful on datasets whose stems have matching tick
    files — so both the ETA and the UI's dataset pinning key off this flag.
    """
    if not fill:
        return False
    path = FILL_DIR / fill
    if not path.exists():
        return False
    return _load_toml(path).get("fill", {}).get("entry_fill_mode") == "tick"


def _fill_presets_with_desc(names: list[str]) -> list[dict]:
    """Fill presets as [{name, desc, tick}] — see `_fill_is_tick` for `tick`."""
    return [
        {"name": n, "desc": _preset_description(FILL_DIR / n), "tick": _fill_is_tick(n)}
        for n in names
    ]


# ── Run-duration estimate (drives the progress bar) ──────────────────────────


def _parquet_rows(stem: str) -> int:
    """Row count of a stem's parquet, read from metadata rather than the data.

    The replay loads each asset's file in full, so total rows across a run's
    source files tracks the wall-time that drives the progress bar. Reading
    only the footer stays cheap on multi-million-row files.
    """
    path = _candle_path(stem)
    if path is None:
        return 0
    try:
        import pyarrow.parquet as pq
        return pq.ParquetFile(path).metadata.num_rows
    except Exception:
        try:
            return pl.scan_parquet(path).select(pl.len()).collect().item()
        except Exception:
            return 0


def _estimate_run_candles(strategy: str, dataset: str) -> int:
    """Total candles a run of (strategy × dataset) will load."""
    info = _read_strategy_assets_sources(strategy)
    assets = info.get("assets") or []
    sources = _read_dataset_sources(dataset) or info.get("sources") or {}
    total = 0
    for asset in assets:
        src = sources.get(asset)
        if src and src.get("files"):
            total += sum(_parquet_rows(stem) for stem in src["files"])
            continue
        # Source-only semantics: a dataset with overrides is the complete asset
        # universe, so an undeclared asset loads nothing.
        if sources:
            continue
        total += _parquet_rows(asset)
    return total


def _load_timing_model() -> dict | None:
    """Read the fitted timing model, or None when there isn't one."""
    if not TIMING_MODEL_PATH.exists():
        return None
    try:
        with TIMING_MODEL_PATH.open() as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


# A tick lens walks every raw trade print rather than one bar per minute, so a
# tick run takes several times longer than the candle-count model predicts.
# Scale the estimate up so the bar doesn't fill and then sit there.
_TICK_LENS_ETA_MULT = 4.0


def _estimate_backtest_seconds(strategy: str, dataset: str, fill: str | None = None) -> float:
    """Predicted wall-time for a run of (strategy × dataset × fill).

    Uses the fitted model `seconds = k*candles^p + c` when one exists; without
    a model file, a coarse linear guess keeps the bar animating sensibly.
    Floored at a few seconds so a tiny run still shows movement.
    """
    n = _estimate_run_candles(strategy, dataset)
    coeffs = (_load_timing_model() or {}).get("coeffs", {})
    if "k" in coeffs and "p" in coeffs:
        est = coeffs["k"] * (n ** coeffs["p"]) + coeffs.get("c", 0.0)
    else:
        est = n / 30000.0 + 3.0
    if _fill_is_tick(fill):
        est *= _TICK_LENS_ETA_MULT
    return max(3.0, est)


# ── Stitched long runs ───────────────────────────────────────────────────────
#
# A single continuous multi-year replay is a state no live instance can reach:
# a live process re-warms `warmup_days` of history at every restart, and any
# walk-forward model inside the strategy trains on an ever-expanding sample.
# After several years of uninterrupted replay, both diverge from the shorter
# band-scoped runs that produce a preset's citable numbers. So a long window on
# a warmup-annotated preset runs as stitched year segments, each re-warmed
# before its start — every year of the chart is then the same object as a
# single-band run.
_REWARM_MAX_CONTINUOUS_DAYS = 366


def _anniversary(d: date, years: int) -> date:
    """`d` shifted by whole years (Feb 29 falls back to Feb 28)."""
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        return d.replace(year=d.year + years, day=28)


def _rewarm_segments(from_str: str, to_str: str, warmup_days: int) -> list[tuple[str, str]]:
    """Split [from, to] into re-warmed yearly segments, or keep it whole.

    Segmentation applies only when the preset declares a viz warmup AND the
    window exceeds a band year. Boundaries fall on anniversaries of `from`, and
    `--to` is inclusive in the replay binary, so each segment ends the day
    before the next begins: contiguous, with no double-counted boundary day.
    Unparseable dates degrade to a single un-split run, letting the binary
    reject them with its own error message.
    """
    try:
        d0 = date.fromisoformat(from_str)
        d1 = date.fromisoformat(to_str)
    except ValueError:
        return [(from_str, to_str)]
    if warmup_days <= 0 or (d1 - d0).days <= _REWARM_MAX_CONTINUOUS_DAYS:
        return [(from_str, to_str)]
    segs: list[tuple[str, str]] = []
    k = 0
    while True:
        s = _anniversary(d0, k)
        e = _anniversary(d0, k + 1) - timedelta(days=1)
        if e >= d1:
            segs.append((s.isoformat(), d1.isoformat()))
            return segs
        segs.append((s.isoformat(), e.isoformat()))
        k += 1


def _sum_int_tree(docs: list, node: object) -> object:
    """Sum a nested dict-of-(dict-of-)numbers across parallel structures.

    `node` is the first document's subtree; `docs` are the parallel subtrees
    from every segment, with a missing key counting as 0. Non-numeric leaves
    keep the first document's value.
    """
    if isinstance(node, dict):
        keys: list = []
        for d in docs:
            if isinstance(d, dict):
                keys.extend(k for k in d if k not in keys)
        return {
            k: _sum_int_tree(
                [d.get(k) for d in docs if isinstance(d, dict)],
                next((d[k] for d in docs if isinstance(d, dict) and k in d), None),
            )
            for k in keys
        }
    if isinstance(node, (int, float)) and not isinstance(node, bool):
        return sum(d for d in docs if isinstance(d, (int, float)) and not isinstance(d, bool))
    return node


def _merge_backtest_sidecars(docs: list[dict]) -> dict:
    """Stitch per-segment sidecars into one document for the UI.

    Static config fields (sources, label, rr_target, …) come from the first
    segment; trades and intervals concatenate; counters and R totals sum; win
    rate and expectancy are recomputed from the merged counters rather than
    averaged, so a short segment doesn't weigh as much as a long one.
    """
    if len(docs) == 1:
        return docs[0]
    out = dict(docs[0])
    out["trades"] = sorted(
        (t for d in docs for t in d.get("trades") or []),
        key=lambda t: t.get("opened_at") or "",
    )
    out["resting_intervals"] = [iv for d in docs for iv in d.get("resting_intervals") or []]
    for k in (
        "total_r_pnl", "gross_r_pnl", "total_fees", "wins", "losses",
        "inconclusive", "opportunities_seen", "opportunities_taken",
        "trades_decided",
    ):
        out[k] = sum(d.get(k) or 0 for d in docs)
    for k in ("by_asset", "by_signal_type", "hybrid_fill_paths"):
        if isinstance(out.get(k), dict):
            out[k] = _sum_int_tree([d.get(k) for d in docs], out[k])
    decided = out.get("trades_decided") or 0
    graded = (out.get("wins") or 0) + (out.get("losses") or 0)
    out["win_rate"] = (100.0 * (out.get("wins") or 0) / graded) if graded else 0.0
    if decided:
        out["expectancy"] = (
            sum((d.get("expectancy") or 0.0) * (d.get("trades_decided") or 0) for d in docs)
            / decided
        )
    return out


# ── Trade serialization + candle loading ─────────────────────────────────────


def _trades_to_json(df: pl.DataFrame) -> list[dict]:
    """Serialize a `_SCHEMA` frame for the frontend.

    Every timestamp goes out twice: ISO for display, epoch seconds for the
    chart library, which indexes by integer time.
    """
    def _iso(dt: datetime | None) -> str | None:
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.isoformat()

    def _epoch(dt: datetime | None) -> int | None:
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return int(dt.timestamp())

    out: list[dict] = []
    for row in df.iter_rows(named=True):
        out.append({
            "entry_time": _iso(row["entry_time"]),
            "entry_epoch": _epoch(row["entry_time"]),
            "asset": row["asset"],
            "direction": row["direction"],
            "entry_price": row["entry_price"],
            "exit_time": _iso(row["exit_time"]),
            "exit_epoch": _epoch(row["exit_time"]),
            "exit_price": row["exit_price"],
            "size": row["size"],
            "closed_pnl": row["closed_pnl"],
            "fee": row["fee"],
            "outcome": row["outcome"],
            "opportunity_id": row.get("opportunity_id"),
            "score": row.get("score"),
            "intended_entry": row.get("intended_entry"),
            "intended_size": row.get("intended_size"),
            "entry_crossed": row.get("entry_crossed"),
            "exit_crossed": row.get("exit_crossed"),
            "stop_price": row.get("stop_price"),
            "tp_price": row.get("tp_price"),
            "closed_via": row.get("closed_via"),
            "flag": row.get("flag"),
            "flag_detail": row.get("flag_detail"),
            "r_pnl": row.get("r_pnl"),
            "signal_type": row.get("signal_type"),
            "equity": row.get("equity"),
            "pnl_dollars": row.get("pnl_dollars"),
        })
    return out


# Candle columns are matched case-insensitively against these aliases, the
# same set the engine's loader accepts, so one parquet feeds both. Volume is
# optional and defaults to zero.
_CANDLE_COL_ALIASES: dict[str, tuple[str, ...]] = {
    "timestamp": ("timestamp", "ts", "time", "datetime", "date"),
    "open": ("open", "o"),
    "high": ("high", "h"),
    "low": ("low", "l"),
    "close": ("close", "c"),
}
_REQUIRED_CANDLE_COLS = tuple(_CANDLE_COL_ALIASES)
_VOLUME_ALIASES = ("volume", "vol", "v")


def _column_map(columns: list[str]) -> dict[str, str] | None:
    """Map canonical candle column names to a file's actual spelling.

    Returns None when a required column has no alias present, which the caller
    treats as "this stem contributes nothing" rather than an error — a run can
    splice several stems and only some of them need to exist.
    """
    lower = {c.lower(): c for c in columns}
    out: dict[str, str] = {}
    for canon, aliases in _CANDLE_COL_ALIASES.items():
        match = next((lower[a] for a in aliases if a in lower), None)
        if match is None:
            return None
        out[canon] = match
    vol = next((lower[a] for a in _VOLUME_ALIASES if a in lower), None)
    if vol is not None:
        out["volume"] = vol
    return out


def _load_candles_from_parquet(
    asset: str, start: datetime, end: datetime, override: dict | None = None,
) -> list[dict]:
    """Load 1m candles for `asset` from local parquet.

    `override` (from a run's `sources` map) redirects to donor file stem(s) and
    applies the same price transform the engine used, `price = raw*scale + offset`,
    so a chart shows the exact scaled series the trades were generated on and
    the markers land on the right candles. Without an override the asset's own
    file is read raw.

    Stems are loaded in declared order and the FIRST to provide a timestamp
    wins, matching the engine loader's splice precedence.
    """
    if override and override.get("files"):
        stems = override["files"]
        scale = float(override.get("scale", 1.0))
        offset = float(override.get("offset", 0.0))
    else:
        stems = [asset]
        scale, offset = 1.0, 0.0

    start_naive = start.astimezone(UTC).replace(tzinfo=None)
    end_naive = end.astimezone(UTC).replace(tzinfo=None)

    seen_ts: set = set()
    rows: list[dict] = []
    for stem in stems:
        path = _candle_path(stem)
        if path is None:
            continue
        df = pl.read_parquet(path)
        cols = _column_map(df.columns)
        if cols is None:
            logger.warning("skipping %s: missing OHLC columns", path)
            continue
        ts_col = cols["timestamp"]
        df = df.filter(
            (pl.col(ts_col) >= start_naive) & (pl.col(ts_col) <= end_naive)
        ).sort(ts_col)
        vol_col = cols.get("volume")
        for row in df.iter_rows(named=True):
            ts = row[ts_col]
            if ts in seen_ts:
                continue
            seen_ts.add(ts)
            rows.append({
                "time": int(ts.replace(tzinfo=UTC).timestamp()),
                "open": row[cols["open"]] * scale + offset,
                "high": row[cols["high"]] * scale + offset,
                "low": row[cols["low"]] * scale + offset,
                "close": row[cols["close"]] * scale + offset,
                "volume": row[vol_col] if vol_col else 0.0,
            })
    rows.sort(key=lambda c: c["time"])
    return rows


def _dedupe_and_sort(candles: list[dict]) -> list[dict]:
    """Collapse duplicate times and sort ascending.

    The chart library rejects duplicate or out-of-order times outright, so this
    runs on everything before it reaches the wire.
    """
    by_time: dict[int, dict] = {c["time"]: c for c in candles}
    return [by_time[t] for t in sorted(by_time)]


# ── Server ───────────────────────────────────────────────────────────────────


class BacktestVizServer:
    """The web app. One instance owns one sidecar path and one run at a time."""

    def __init__(self, backtest_path: Path = BACKTEST_PATH) -> None:
        self.backtest_path = backtest_path
        self._backtest_lock = asyncio.Lock()
        # Live run state, polled by /api/backtest/status to drive the progress
        # bar. Set when a run starts, updated when it finishes or fails.
        self._bt_run: dict | None = None
        self._app = web.Application()
        # Page routes come from the registry (the single source of truth): "/"
        # serves the default run and "/<key>" serves each registry entry. No run
        # name is hardcoded here — adding one to the registry gives it a page.
        self._app.router.add_get("/", self._index)
        for key in get_runs():
            self._app.router.add_get(f"/{key}", self._index)
        self._app.router.add_get("/equity", self._equity)
        # Exhibit: a self-contained chart permalink (markers quoted in the URL,
        # drawn on local candles) — the citation target for write-ups.
        self._app.router.add_get("/exhibit", self._exhibit_page)
        self._app.router.add_get("/theme.css", self._theme_css)
        self._app.router.add_get("/api/runs", self._api_runs)
        # Legacy token the frontend still asks for; same payload.
        self._app.router.add_get("/api/traders", self._api_runs)
        self._app.router.add_get("/api/trades", self._api_trades)
        self._app.router.add_get("/api/chart", self._api_chart)
        self._app.router.add_post("/api/backtest/run", self._api_backtest_run)
        self._app.router.add_get("/api/backtest/status", self._api_backtest_status)
        self._app.router.add_get("/api/backtest/range", self._api_backtest_range)
        self._app.router.add_get("/api/sources", self._api_sources)

    # ── Pages ────────────────────────────────────────────────────────────────

    def _runs_payload(self) -> list[dict]:
        """Ordered run list for the dashboard (registry = source of truth).

        `kind` is always "backtest" here. The field exists because the frontend
        branches on it, and keeping it means a future run kind needs no page
        change to be listed.
        """
        default_key = default_run().key
        return [
            {
                "key": spec.key,
                "label": spec.label,
                "kind": "backtest",
                "configured": True,
                "default": spec.key == default_key,
                "archived": False,
                "exists": spec.exists,
            }
            for spec in get_runs().values()
        ]

    async def _index(self, request: web.Request) -> web.Response:
        """Serve dashboard.html with the registry's run list spliced inline.

        The page needs that list synchronously — it builds the nav and resolves
        the current run before it fetches anything — so the JSON goes into a
        placeholder at serve time rather than making the page fetch its own
        identity and race its init. Read per request; the file is small and
        caching is disabled anyway.
        """
        html = HTML_PATH.read_text()
        payload = json.dumps({"traders": self._runs_payload()})
        html = html.replace("/*__TRADERS__*/[]", payload, 1)
        resp = web.Response(text=html, content_type="text/html")
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return resp

    @staticmethod
    def _no_cache(path: Path) -> web.FileResponse:
        resp = web.FileResponse(path)
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return resp

    async def _equity(self, request: web.Request) -> web.Response:
        return self._no_cache(EQUITY_HTML_PATH)

    async def _exhibit_page(self, request: web.Request) -> web.Response:
        return self._no_cache(EXHIBIT_HTML_PATH)

    async def _theme_css(self, request: web.Request) -> web.Response:
        return self._no_cache(THEME_CSS_PATH)

    # ── Run list + trades ────────────────────────────────────────────────────

    async def _api_runs(self, request: web.Request) -> web.Response:
        """The ordered dashboard run list. See `_runs_payload`."""
        payload = self._runs_payload()
        # "traders" is the key the frontend reads; "runs" is the honest name.
        # Both are served so either spelling works.
        return web.json_response({"traders": payload, "runs": payload})

    def _sidecar_for(self, request: web.Request) -> Path:
        """Which sidecar this request reads.

        A `?run=`/`?trader=` token selects a registry entry; anything absent or
        unknown falls back to this server's own path, so a stale bookmark still
        renders the current run rather than 404ing.
        """
        token = request.query.get("run") or request.query.get("trader") or ""
        spec: RunSpec | None = resolve_run(token)
        if spec is None or spec.default:
            return self.backtest_path
        return spec.sidecar

    async def _api_trades(self, request: web.Request) -> web.Response:
        """Every trade in a run's sidecar, plus the run's own metadata.

        Alongside the trades the payload carries three things the page needs to
        report a result honestly: `compound` (the engine's equity block, present
        only when the strategy compounds), `sources` (the per-asset data mapping
        the replay stamped in, so an asset id can never pass for a feed it
        wasn't graded on), and `meta` (the strategy × fill × dataset axes that
        produced the sidecar).
        """
        path = self._sidecar_for(request)
        df = load_backtest_trades(path)
        compound: dict | None = None
        run_sources: dict | None = None
        if path.exists():
            try:
                with path.open() as f:
                    doc = json.load(f)
                compound = doc.get("compound")
                run_sources = doc.get("sources") or None
            except (OSError, ValueError):
                logger.exception("sidecar unreadable: %s", path)
        return web.json_response({
            "trades": _trades_to_json(df),
            "count": df.height,
            "source": "backtest",
            "generated_at": int(path.stat().st_mtime) if path.exists() else None,
            "exists": path.exists(),
            "ingest_error": None,
            "compound": compound,
            "freshness": None,
            "meta": self._load_backtest_meta(),
            "sources": run_sources,
        })

    # ── Chart data ───────────────────────────────────────────────────────────

    def _backtest_source_override(self, asset: str) -> dict | None:
        """Per-asset data-source override embedded in the last run's sidecar.

        The engine writes a `sources` map (asset → {files, scale, offset}) for
        any asset backed by a redirected feed. Returning it makes the chart
        render the same scaled candles the engine traded on. None for assets
        with no override, which are read raw.
        """
        if not self.backtest_path.exists():
            return None
        try:
            with self.backtest_path.open() as f:
                report = json.load(f)
        except (json.JSONDecodeError, OSError):
            return None
        return (report.get("sources") or {}).get(asset)

    async def _api_chart(self, request: web.Request) -> web.Response:
        """Candles for one trade's window, from local parquet.

        Query params:
          asset          (required) the asset id, matching a parquet stem
          entry          (required) epoch seconds of entry
          exit           (optional) epoch seconds of exit; omitted means the
                         window runs from entry to the latest available bar
          pad_minutes    (optional, default 60) context either side
          source         (optional) "backtest" applies the source mapping from
                         the last run's sidecar
          files          (optional) comma-separated stem(s) overriding that
                         mapping for THIS request, so the frontend's source
                         editor can preview a different feed without a re-run
          scale, offset  (optional) the price transform paired with `files`
        """
        try:
            asset = request.query["asset"]
            entry = int(request.query["entry"])
        except (KeyError, ValueError):
            return web.json_response({"error": "asset and entry are required"}, status=400)

        # An explicit `files` param wins over the saved mapping, so an edit
        # re-renders without a re-run.
        files_raw = request.query.get("files", "").strip()
        if files_raw:
            try:
                override = {
                    "files": [s for s in files_raw.split(",") if s],
                    "scale": float(request.query.get("scale", "1") or 1.0),
                    "offset": float(request.query.get("offset", "0") or 0.0),
                }
            except ValueError:
                return web.json_response({"error": "scale/offset must be numeric"}, status=400)
        elif request.query.get("source", "backtest") == "backtest":
            override = self._backtest_source_override(asset)
        else:
            override = None

        exit_raw = request.query.get("exit", "").strip()
        try:
            exit_ = int(exit_raw) if exit_raw else None
        except ValueError:
            exit_ = None
        try:
            pad_min = int(request.query.get("pad_minutes", 60))
        except ValueError:
            pad_min = 60

        window_start = datetime.fromtimestamp(entry - pad_min * 60, tz=UTC)
        if exit_ is not None:
            window_end = datetime.fromtimestamp(exit_ + pad_min * 60, tz=UTC)
        else:
            # Open trade: chart through now, so the price action since entry shows.
            window_end = datetime.now(tz=UTC)

        candles = _dedupe_and_sort(
            _load_candles_from_parquet(asset, window_start, window_end, override=override)
        )
        return web.json_response({
            "asset": asset,
            "candles": candles,
            "source": "parquet" if candles else "none",
            "window_start": int(window_start.timestamp()),
            "window_end": int(window_end.timestamp()),
        })

    # ── Backtest range + presets ─────────────────────────────────────────────

    async def _api_backtest_range(self, request: web.Request) -> web.Response:
        """The date range a run of the selected axes can cover.

        `earliest`/`latest` bound the INTERSECTION (every asset has data) and
        are the safe default the UI pre-fills. `earliest_full`/`latest_full`
        bound the UNION, backing a "max history" option that runs each asset
        over its own whole file.
        """
        dataset = request.query.get("dataset") or DEFAULT_DATASET
        dataset_sources = _read_dataset_sources(dataset)
        strat = request.query.get("strategy") or ""
        assets: list[str] = []
        if strat and Path(strat).name == strat and (STRATEGY_DIR / strat).is_file():
            info = _read_strategy_assets_sources(strat)
            assets = list(info["assets"])
            # An own-sources strategy runs standalone, so with a source-free
            # dataset the range must come from ITS files — the live parquets it
            # never loads would report the wrong window.
            if not dataset_sources and info["sources"]:
                dataset_sources = info["sources"]
                assets = list(info["sources"]) or assets
        if not assets:
            assets = sorted(dataset_sources) if dataset_sources else []

        earliest, latest = _range_over(assets, dataset_sources, union=False)
        earliest_full, latest_full = _range_over(assets, dataset_sources, union=True)
        return web.json_response({
            "earliest": earliest.isoformat() if earliest else None,
            "latest": latest.isoformat() if latest else None,
            "earliest_full": earliest_full.isoformat() if earliest_full else None,
            "latest_full": latest_full.isoformat() if latest_full else None,
            "assets": assets,
            "sync_error": None,
            "freshness": None,
        })

    async def _api_sources(self, request: web.Request) -> web.Response:
        """Strategy + fill + dataset presets for the backtest controls.

        Without `?strategy=`: the three preset lists, each entry carrying a
        short `desc` read from the file's leading comment block, plus the
        defaults. With `?strategy=<name>`: that strategy's own asset list too,
        shown read-only, because assets belong to the strategy and not to the
        dataset.
        """
        strategies = _list_strategies()
        datasets = _list_datasets()
        fills = _list_fills()

        base = {
            "strategies": _strategy_presets_with_desc(strategies),
            "fills": _fill_presets_with_desc(fills),
            "datasets": _presets_with_desc(datasets, DATASET_DIR),
            "default_strategy": DEFAULT_STRATEGY,
            "default_fill": DEFAULT_FILL,
            "default_dataset": DEFAULT_DATASET,
            "sourceless_datasets": _sourceless_datasets(datasets),
        }

        strat = request.query.get("strategy")
        if not strat:
            return web.json_response(base)
        if strat not in strategies:
            return web.json_response({"error": f"unknown strategy preset: {strat}"}, status=400)
        info = _read_strategy_assets_sources(strat)
        own_stems = sorted({f for s in info["sources"].values() for f in s["files"]})
        return web.json_response({
            **base,
            "strategy": strat,
            "assets": info["assets"],
            # Stems of the strategy's OWN `[[source]]` tables, empty for a
            # feed-native strategy. Lets the UI label the pinned dataset option
            # with what a standalone run actually loads.
            "own_source_stems": own_stems,
            "own_dataset_label": "+".join(own_stems) if own_stems else None,
        })

    # ── Running a backtest ───────────────────────────────────────────────────

    @staticmethod
    def _write_ui_strategy(
        strategy: str, sources: dict, fee_schedule: str | None = None,
    ) -> Path:
        """Compose `strategy` (algorithm + assets) with `sources` into a preset.

        The engine's loader allows exactly ONE level of `base` inheritance, and
        a strategy may already be a child — so we cannot simply write
        `base = strategy`, which would be a second level. Instead we FLATTEN:
        inherit the strategy's OWN base (its grandparent, or the strategy
        itself when it has none), inline the strategy's own `engine` / `assets`
        / `[strategy]` overrides, then append the dataset's `[[source]]`
        tables. The result is exactly one level deep.

        Written into STRATEGY_DIR so relative `base` paths resolve identically
        to a hand-written preset's.
        """
        doc = _load_toml(STRATEGY_DIR / strategy)
        own_base = doc.get("base")  # grandparent, or None

        lines = [
            "# AUTO-GENERATED by the viz server; overwritten on each UI-driven run.",
            "# Flattened compose of strategy + dataset (one inheritance level).",
            f'base = "{own_base or strategy}"',
        ]
        # Inline the strategy's own top-level overrides. Only meaningful when
        # the strategy was itself a child; harmless duplication otherwise.
        if own_base:
            if "engine" in doc:
                lines.append(f'engine = "{doc["engine"]}"')
            if "assets" in doc:
                lines.append("assets = [" + ", ".join(f'"{a}"' for a in doc["assets"]) + "]")
        lines.append("")

        # The strategy's own `[strategy]` overrides, plus the dataset's venue
        # fee model when it declares one. A child's fields win over the base,
        # so the dataset's fee_schedule overrides both — the venue is a
        # property of the data, not of the algorithm.
        strat_tbl = dict((doc.get("strategy") or {}) if own_base else {})
        if fee_schedule:
            strat_tbl["fee_schedule"] = fee_schedule
        if strat_tbl:
            lines.append("[strategy]")
            lines += [f"{k} = {json.dumps(v)}" for k, v in strat_tbl.items()]
            lines.append("")

        for asset, cfg in sources.items():
            files = cfg.get("files") or []
            if not files:
                continue
            lines += [
                "[[source]]",
                f'asset = "{asset}"',
                "files = [" + ", ".join(f'"{f}"' for f in files) + "]",
                f"scale = {float(cfg.get('scale', 1.0))}",
                f"offset = {float(cfg.get('offset', 0.0))}",
                "",
            ]
        UI_STRATEGY_PATH.parent.mkdir(parents=True, exist_ok=True)
        UI_STRATEGY_PATH.write_text("\n".join(lines))
        return UI_STRATEGY_PATH

    async def _api_backtest_run(self, request: web.Request) -> web.Response:
        """Kick off `scripts/backtest.sh` in the background; return immediately.

        Body: `{from, to, strategy?, dataset?, fill?, allow_dataset_override?}`.
        The run grades the chosen strategy (algorithm + assets) over the chosen
        dataset (data sources) through the chosen fill lens.

        This does NOT wait for the run to finish — it launches a background
        task and returns `{started, eta_seconds}` so the UI can poll
        `/api/backtest/status` and animate a progress bar.
        """
        if self._backtest_lock.locked():
            return web.json_response({"error": "backtest already running"}, status=409)
        try:
            payload = await request.json() if request.body_exists else {}
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

        # Validate every axis against its directory listing, so a bad or
        # path-traversing name can never reach an argv slot.
        base_strategy = payload.get("strategy") or DEFAULT_STRATEGY
        if base_strategy not in _list_strategies():
            return web.json_response(
                {"error": f"unknown strategy preset: {base_strategy}"}, status=400,
            )
        dataset = payload.get("dataset") or DEFAULT_DATASET
        if dataset not in _list_datasets():
            return web.json_response({"error": f"unknown dataset preset: {dataset}"}, status=400)
        fill = payload.get("fill") or DEFAULT_FILL
        if fill not in _list_fills():
            return web.json_response({"error": f"unknown fill preset: {fill}"}, status=400)

        # Warmup depth comes from the STRATEGY, never from the caller: it is
        # part of the preset's published setup (see `_strategy_viz_warmup`).
        # Any value in the payload is ignored.
        warmup_days = _strategy_viz_warmup(base_strategy)

        # Strategy×dataset pairing guard. The UI pins its picker, but that is
        # cosmetic — a stale tab or a direct POST would otherwise build a run
        # whose sources were silently discarded. Refusing here means the
        # invalid combination cannot produce a sidecar at all.
        # `allow_dataset_override` is the deliberate escape hatch: mixing stays
        # possible, it just has to be asked for, and the choice is recorded on
        # the run so the result is never mistaken for the preset's baseline.
        override = bool(payload.get("allow_dataset_override"))
        conflict = _dataset_conflict(base_strategy, dataset)
        if conflict and not override:
            return web.json_response(
                {"error": conflict, "conflict": True, "overridable": True}, status=400,
            )

        # Fallback range, used only when the payload omits a bound. An omitted
        # bound means "as wide as possible", so it falls back to the UNION
        # span: the engine runs one thread per asset over its own range, so an
        # asset whose data starts late simply joins mid-run instead of clamping
        # every other asset's start to its first bar.
        sources = _read_dataset_sources(dataset)
        strat_info = _read_strategy_assets_sources(base_strategy)
        range_sources = sources or strat_info["sources"]
        range_assets = strat_info["assets"] or sorted(range_sources)
        earliest, latest = _range_over(range_assets, range_sources, union=True)
        from_str = payload.get("from") or (
            earliest.isoformat() if earliest
            else (date.today() - timedelta(days=14)).isoformat()
        )
        to_str = payload.get("to") or (latest.isoformat() if latest else date.today().isoformat())

        # A long window on a warmup-annotated preset runs as stitched re-warmed
        # year segments, so the chart shows what a band-scoped run produces
        # rather than a years-deep engine state nothing else can reach.
        segments = _rewarm_segments(from_str, to_str, warmup_days)

        # Compose: a dataset with `[[source]]` overrides gets a temp child
        # preset inheriting the strategy and appending those sources. A
        # source-free dataset runs the strategy directly.
        env = dict(os.environ)
        if sources:
            env["BT_STRATEGY"] = str(
                self._write_ui_strategy(base_strategy, sources, _dataset_fee_schedule(dataset))
            )
        else:
            env["BT_STRATEGY"] = str(STRATEGY_DIR / base_strategy)

        # The data axis the run will ACTUALLY load: the dataset's sources when
        # applied, else the strategy's own stems for a standalone run. Both the
        # label and the preflight work off this, so a source-carrying preset is
        # never labeled with a dataset it does not read.
        effective_sources = range_sources
        dataset_label = dataset
        if not sources and effective_sources:
            stems = sorted({f for s in effective_sources.values() for f in s["files"]})
            dataset_label = "+".join(stems)

        # Preflight: every backing parquet must exist on THIS host. Without the
        # check the engine replays an empty tape and the sidecar comes back with
        # zero trades posing as a result.
        missing = sorted({
            f"{f}_1m.parquet"
            for s in effective_sources.values()
            for f in s["files"]
            if _candle_path(f) is None
        })
        if missing:
            return web.json_response(
                {"error": "source data missing on this host: " + ", ".join(missing)
                          + " — seed data/ before running"},
                status=400,
            )

        # backtest.sh reads these and passes them to the replay binary.
        env["BT_FILL"] = str(FILL_DIR / fill)
        env["BT_JSON"] = str(self.backtest_path)

        eta = _estimate_backtest_seconds(base_strategy, dataset, fill)
        loop = asyncio.get_running_loop()
        self.backtest_path.parent.mkdir(parents=True, exist_ok=True)
        # Seed the run state BEFORE launching, so a status poll racing the
        # spawn still sees "running". The lock is taken inside the task and
        # released when it completes, so `locked()` tracks the real subprocess.
        self._bt_run = {
            "running": True,
            "started_at": loop.time(),
            "eta_seconds": eta,
            "strategy": base_strategy,
            "fill": fill,
            "dataset": dataset_label,
            "from": from_str,
            "to": to_str,
            "warmup_days": warmup_days,
            "error": None,
            "finished_at": None,
            "segments_total": len(segments),
            "segments_done": 0,
            # Non-null only when the dataset axis was explicitly unlocked for a
            # constrained strategy, so the UI can badge the result as a
            # deliberate mix rather than the preset's published pairing.
            "dataset_override": conflict if (conflict and override) else None,
        }
        asyncio.create_task(self._run_backtest_task(
            env, base_strategy, fill, dataset_label, bool(sources), warmup_days, segments,
        ))
        return web.json_response({
            "started": True,
            "eta_seconds": eta,
            "from": from_str,
            "to": to_str,
            "strategy": base_strategy,
            "fill": fill,
            "dataset": dataset_label,
            "warmup_days": warmup_days,
            "segments": len(segments),
        })

    async def _run_backtest_task(
        self, env: dict, strategy: str, fill: str, dataset: str,
        sources_applied: bool, warmup_days: int, segments: list[tuple[str, str]],
    ) -> None:
        """Background worker: run backtest.sh to completion, update `_bt_run`.

        Holds `_backtest_lock` for the subprocess lifetime, so a second run is
        rejected with 409. On exit it marks the run finished, or records an
        error for the status endpoint to report.

        Multi-segment runs execute sequentially, each into its own temporary
        sidecar; the merged document replaces the real sidecar only after every
        segment succeeds, so a mid-stitch failure never leaves a partial run
        posing as the result.
        """
        async with self._backtest_lock:
            stitched = len(segments) > 1
            seg_docs: list[dict] = []
            seg_paths: list[Path] = []
            run_error: str | None = None
            try:
                for i, (seg_from, seg_to) in enumerate(segments):
                    if self._bt_run is not None:
                        self._bt_run["segments_done"] = i
                    seg_env = env
                    seg_path = self.backtest_path
                    if stitched:
                        seg_path = self.backtest_path.with_name(
                            self.backtest_path.stem + f".seg{i}.json"
                        )
                        seg_paths.append(seg_path)
                        seg_env = dict(env)
                        seg_env["BT_JSON"] = str(seg_path)
                    # backtest.sh forwards unrecognized args to the replay
                    # binary, so --warmup-days rides along after the dates.
                    # 0 omits the flag, leaving the binary's own default.
                    argv = [str(BACKTEST_SCRIPT), "--from", seg_from, "--to", seg_to]
                    if warmup_days > 0:
                        argv += ["--warmup-days", str(warmup_days)]
                    try:
                        proc = await asyncio.create_subprocess_exec(
                            *argv,
                            stdout=asyncio.subprocess.DEVNULL,
                            stderr=asyncio.subprocess.PIPE,
                            env=seg_env,
                            cwd=str(REPO_ROOT),
                        )
                        _, stderr = await proc.communicate()
                    except Exception as e:
                        logger.exception("backtest launch failed")
                        if self._bt_run is not None:
                            self._bt_run.update(running=False, error=f"{type(e).__name__}: {e}")
                        return
                    if proc.returncode != 0:
                        tail = stderr.decode("utf-8", errors="replace")[-800:]
                        note = (
                            f" (segment {i + 1}/{len(segments)} {seg_from}..{seg_to})"
                            if stitched else ""
                        )
                        run_error = f"backtest failed (rc={proc.returncode}){note}: {tail}"
                        break
                    if stitched:
                        try:
                            with seg_path.open() as f:
                                seg_docs.append(json.load(f))
                        except (OSError, ValueError) as e:
                            run_error = (
                                f"segment sidecar unreadable ({seg_from}..{seg_to}): "
                                f"{type(e).__name__}: {e}"
                            )
                            break
                if run_error is None and stitched:
                    try:
                        with self.backtest_path.open("w") as f:
                            json.dump(_merge_backtest_sidecars(seg_docs), f)
                    except (OSError, ValueError) as e:
                        run_error = f"segment merge failed: {type(e).__name__}: {e}"
            finally:
                for p in seg_paths:
                    p.unlink(missing_ok=True)

            if self._bt_run is None:
                return
            loop = asyncio.get_running_loop()
            self._bt_run["running"] = False
            self._bt_run["finished_at"] = loop.time()
            self._bt_run["segments_done"] = len(segments)
            if run_error is not None:
                self._bt_run["error"] = run_error
                return
            self._bt_run["sources_applied"] = sources_applied
            try:
                self._bt_run["generated_at"] = int(self.backtest_path.stat().st_mtime)
            except OSError:
                self._bt_run["error"] = "backtest wrote no sidecar"
                return
            # Record which axes produced this sidecar in a small side JSON, so
            # results stay labeled with their strategy × fill × dataset even
            # after a page reload (the engine's sidecar has no fill slot).
            try:
                meta_path = self._backtest_meta_path()
                meta_path.parent.mkdir(parents=True, exist_ok=True)
                meta = {
                    "strategy": strategy,
                    "fill": fill,
                    "dataset": dataset,
                    "from": segments[0][0],
                    "to": segments[-1][1],
                    # Recorded for reproducibility; NOT part of the uid.
                    "warmup_days": warmup_days,
                    "rewarm_segments": len(segments) if stitched else None,
                    "generated_at": self._bt_run["generated_at"],
                    # Non-null when this run deliberately ignored the
                    # strategy's dataset constraint. Persisted with the axes so
                    # the badge survives a reload.
                    "dataset_override": self._bt_run.get("dataset_override"),
                }
                meta["uid"] = _backtest_uid(meta)
                with meta_path.open("w") as f:
                    json.dump(meta, f)
            except OSError:
                logger.exception("failed to write backtest meta sidecar")

    def _backtest_meta_path(self) -> Path:
        """Side JSON, next to the sidecar, recording the last run's axes."""
        return self.backtest_path.with_name(self.backtest_path.stem + ".meta.json")

    def _load_backtest_meta(self) -> dict | None:
        """The last run's strategy × fill × dataset, or None if unrecorded.

        Injects `uid` at load time (rather than reading a stored one) so meta
        files written before the uid format existed still carry one.
        """
        p = self._backtest_meta_path()
        if not p.exists():
            return None
        try:
            with p.open() as f:
                meta = json.load(f)
        except (OSError, ValueError):
            return None
        if isinstance(meta, dict):
            meta["uid"] = _backtest_uid(meta)
            return meta
        return None

    async def _api_backtest_status(self, request: web.Request) -> web.Response:
        """Progress and outcome of the current or last run.

        While a run is live, `progress` is time-based (elapsed / eta) and held
        below 1.0 until the subprocess actually exits, so the bar never claims
        done early. Past the estimate it creeps asymptotically toward 0.99,
        which is what an undershooting model looks like from the page.
        """
        exists = self.backtest_path.exists()
        running = self._backtest_lock.locked()
        out: dict = {
            "exists": exists,
            "generated_at": int(self.backtest_path.stat().st_mtime) if exists else None,
            "running": running,
        }
        run = self._bt_run
        if run is not None:
            out["dataset_override"] = run.get("dataset_override")
            axes = {
                "strategy": run.get("strategy"),
                "fill": run.get("fill"),
                "dataset": run.get("dataset"),
                "warmup_days": run.get("warmup_days"),
            }
            if running:
                eta = run.get("eta_seconds") or 1.0
                elapsed = max(0.0, asyncio.get_running_loop().time() - run["started_at"])
                raw = elapsed / eta
                progress = raw if raw < 0.9 else 0.9 + 0.09 * (1 - 1 / (1 + (raw - 0.9)))
                out.update({
                    "elapsed_seconds": round(elapsed, 2),
                    "eta_seconds": round(eta, 2),
                    "progress": round(min(progress, 0.99), 4),
                    **axes,
                })
                # Stitched long run: which segment the subprocess is on, so the
                # bar can say "year 3/5".
                if (run.get("segments_total") or 1) > 1:
                    out["segments_total"] = run["segments_total"]
                    out["segments_done"] = run.get("segments_done", 0)
            else:
                out.update({"progress": 1.0, "error": run.get("error"), **axes})
        # Always surface the persisted axes of the last completed run — this
        # survives a server or page reload where `_bt_run` is None, so results
        # never go unlabeled.
        out["meta"] = self._load_backtest_meta()
        return web.json_response(out)

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def start(self, host: str, port: int) -> web.AppRunner:
        """Bind and serve. Returns the runner so a caller can clean it up."""
        runner = web.AppRunner(self._app)
        await runner.setup()
        await web.TCPSite(runner, host, port).start()
        logger.info("backtest viz running at http://%s:%d", host, port)
        return runner


def main() -> None:
    """CLI entry point: `viz serve [--host H] [--port P] [--report PATH]`.

    `serve` is the only command, and it is optional: bare `viz` serves too.
    """
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        prog="viz", description="Serve the backtest visualizer.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="bind address")
    parser.add_argument("--port", "-p", type=int, default=8090, help="bind port")
    parser.add_argument(
        "--report", "--sidecar", dest="report", type=Path, default=BACKTEST_PATH,
        help="JSON report written by the engine (default: data/backtest_trades.json)",
    )
    # Accept (and ignore) a leading "serve" so the documented invocation and
    # the bare one both work, without pretending there are other commands.
    argv = sys.argv[1:]
    if argv and argv[0] == "serve":
        argv = argv[1:]
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    server = BacktestVizServer(backtest_path=args.report)

    async def run() -> None:
        await server.start(args.host, args.port)
        print(f"Backtest viz at http://{args.host}:{args.port}")
        while True:
            await asyncio.sleep(3600)

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
