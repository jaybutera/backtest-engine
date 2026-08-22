# Configuration

A run is three independent files crossed together:

    backtest --strategy config/strategy/example.toml \
             --fill     config/fill/market_hybrid.toml \
             --from 2024-01-01 --to 2024-12-31

They are separate on purpose. Crossing them is the whole point — the same
strategy graded through a pessimistic fill lens, or against a different venue's
data, should be one flag and not a fork.

## Strategy — which trades to take

`config/strategy/*.toml`. An asset list, a pointer to an engine config, a
`[strategy]` table of parameters, and a few top-level keys:

| Key | Meaning |
|---|---|
| `factory = "name"` | Which registered strategy factory builds this preset. Optional when the binary registers only one. |
| `base = "other.toml"` | Inherit from another preset (one level). |
| `engine = "path"` | Passed through to the factory untouched. |
| `assets = [...]` | The watchlist. |
| `[[source]]` | Map an asset onto other parquet stems (see Dataset). |
| `[[contract]]` | Flat per-contract fee spec: `asset`, `point_value`, `round_turn`, `schedule`. |
| `roll_adjust = true` | Back-adjust contract-roll gaps out of every loaded series. |
| `[script]` | Free-form parameters for a scripted strategy (`factory = "rhai"`). Not validated; handed to the script as `cfg.script`. |

Every key in `[strategy]` is validated against a registry at load time: the
engine's own knobs plus the ones the selected factory declares. A key that is
in neither is a **hard load error**, not a warning — a typo'd parameter name
should fail loudly rather than silently grade a different strategy than you
meant to run.

Parameters absent from a preset take their registry default, and the defaults
are chosen so that an unmentioning preset behaves as though the feature does
not exist. Adding a gate to the engine cannot change the behavior of a preset
written before it.

## Fill lens — how entries are simulated

`config/fill/*.toml`. Backtest-only.

Ordered roughly from optimistic to pessimistic about fills:

| Lens | Models |
|---|---|
| `limit_only` | Every resting limit fills at its price, signal bar excluded |
| `limit_only_conservative` | Same, paying 0.1R at the door |
| `market_hybrid` | Resting GTC with a chase watchdog: maker, taker, or abandon |
| `worst_case_bound` | Signal bar eligible, but every fill 0.1R worse |
| `tick` | Real trade prints decide fill order; needs tick data |
| `tick_nochase` | `tick` with the chase disabled |

None of these is the true one. Run at least two, and treat the spread between
them as part of the result — a strategy whose sign depends on the lens does not
have a measured edge, it has a fill assumption.

## Dataset — which files back each asset

`config/datasets/*.toml`. Sources only: no parameters, no asset list. Each
entry maps an asset to candle file stems, optionally with a `scale` and
`offset` applied at load when splicing one instrument's history onto another's
price level.

An asset with no source entry is **dropped from the run** rather than falling
back to its default file. That is deliberate: silently mixing feeds is a good
way to grade a strategy against data you did not intend, and not notice.

Note there is no `--dataset` flag. On the command line, a strategy preset
carries its own `[[source]]` tables and is self-contained; the dataset axis is
composed by the visualizer, which merges a dataset file into the strategy
before launching a run. Pairing a source-declaring strategy with a
source-declaring dataset would silently discard one of them, so that
combination is refused rather than resolved.
