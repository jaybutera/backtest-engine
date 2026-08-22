"""Registry of the runs the dashboard can show — the page list's single source
of truth.

The frontend hardcodes no run names. `get_runs()` returns an insertion-ordered
dict of `RunSpec`; the server registers one page route per key, injects the
list into the HTML at serve time, and the page builds its nav from that. Adding
a run to this registry gives it a page and a pill, with no frontend change.

A run is a JSON sidecar on disk plus a label. `default` (key "backtest") always
exists and points at whatever the last engine run wrote — `data/backtest_trades.json`
unless `BT_JSON` overrides it. Every other entry is discovered: any
`data/backtest_trades.<name>.json` becomes a run keyed `<name>`, so keeping a
comparison run around is a matter of copying a sidecar to a new filename:

    cp data/backtest_trades.json data/backtest_trades.baseline.json

That file then shows up as the "baseline" page on the next request. Discovery
happens at call time, not import, so a sidecar written while the server is up
appears without a restart.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

# Where sidecars live and how a named one is spelled. The default run has no
# infix; a named run carries its key between the stem and the extension.
DATA_DIR = Path("data")
DEFAULT_SIDECAR_NAME = "backtest_trades.json"
_NAMED_SIDECAR_RE = re.compile(r"^backtest_trades\.([A-Za-z0-9_-]+)\.json$")

# Env var overriding where the default run's sidecar is read from. Matches the
# variable `scripts/backtest.sh` writes to, so pointing both at one path keeps
# the runner and the viz in agreement.
SIDECAR_ENV = "BT_JSON"

# Key of the always-present default run.
DEFAULT_KEY = "backtest"

# Sidecars that are not runs: the meta side-file the server writes next to a
# sidecar, and the per-segment temporaries a stitched run leaves behind while
# it executes. Neither should surface as a page.
_RESERVED_INFIXES = ("meta",)
_SEGMENT_RE = re.compile(r"^seg\d+$")


@dataclass(frozen=True)
class RunSpec:
    """One selectable run: a sidecar path plus how the page names it."""
    key: str        # URL segment and API token, e.g. "backtest" or "baseline"
    label: str      # header/display name
    sidecar: Path   # JSON report the engine wrote
    default: bool = False  # served at "/"

    @property
    def exists(self) -> bool:
        """Has this run actually produced a sidecar yet?

        False is a normal state for the default run on a fresh checkout: the
        page renders empty with a "no run yet" hint rather than erroring.
        """
        return self.sidecar.is_file()


def default_sidecar() -> Path:
    """Path the default run's sidecar is read from (`BT_JSON` wins)."""
    override = os.environ.get(SIDECAR_ENV, "").strip()
    return Path(override) if override else DATA_DIR / DEFAULT_SIDECAR_NAME


def _label_for(key: str) -> str:
    """Human label for a discovered run key ("my_run" -> "my run")."""
    return key.replace("_", " ").replace("-", " ")


def _discover_named() -> dict[str, Path]:
    """Named sidecars in DATA_DIR, keyed by their infix, sorted by key."""
    if not DATA_DIR.is_dir():
        return {}
    found: dict[str, Path] = {}
    for p in sorted(DATA_DIR.glob("backtest_trades.*.json")):
        m = _NAMED_SIDECAR_RE.match(p.name)
        if not m:
            continue
        key = m.group(1)
        if key in _RESERVED_INFIXES or _SEGMENT_RE.match(key):
            continue
        found[key] = p
    return found


def get_runs() -> dict[str, RunSpec]:
    """Build the run registry, resolving paths at call time.

    Insertion order is the dashboard's pill order and the FIRST entry — the
    default run — is what "/" serves. Called fresh on every request so a
    sidecar copied in while the server runs shows up without a restart.
    """
    runs: dict[str, RunSpec] = {
        DEFAULT_KEY: RunSpec(
            key=DEFAULT_KEY,
            label="latest run",
            sidecar=default_sidecar(),
            default=True,
        )
    }
    for key, path in _discover_named().items():
        if key == DEFAULT_KEY:
            continue
        runs[key] = RunSpec(key=key, label=_label_for(key), sidecar=path)
    return runs


def default_run() -> RunSpec:
    """The run served at "/" — the first registry entry."""
    for spec in get_runs().values():
        if spec.default:
            return spec
    # Unreachable while get_runs seeds a default; explicit so a future registry
    # that drops it fails loudly instead of serving a page with no data source.
    raise RuntimeError("no default run in the registry")


def resolve_run(token: str | None) -> RunSpec | None:
    """Resolve a `?run=` token (or a URL path segment) to a `RunSpec`.

    An absent or empty token means "no run specified" and follows the default.
    Anything not in the registry returns None so the caller can 400 rather than
    silently charting a different run than the one that was asked for.
    """
    if not (token or "").strip():
        return default_run()
    return get_runs().get(token)
