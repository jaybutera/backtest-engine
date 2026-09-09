"""How the viz server finds presets under a strategies root.

The root is the seam between this public repo and someone's private preset
tree, so the rules that matter are the ones a caller cannot see by reading the
listing: which names the picker hides, which it still runs, and what a name
looks like once it crosses into a deep link. Each test builds a throwaway root
and re-imports the module against it, because the paths are resolved once at
import time — the same thing that happens when the server starts.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


def _make_root(tmp_path: Path) -> Path:
    root = tmp_path / "strategies"
    (root / "alpha").mkdir(parents=True)
    (root / "beta").mkdir(parents=True)
    (root / "fill").mkdir()
    (root / "datasets").mkdir()
    (root / "tools").mkdir()
    (root / ".git").mkdir()

    (root / "alpha" / "one.toml").write_text("# alpha one\nassets = [\"ES\"]\n")
    (root / "alpha" / "two.toml").write_text(
        "# alpha two\nassets = [\"NQ\"]\n\n[viz]\nfill = \"pessimistic\"\n"
    )
    (root / "beta" / "solo.toml").write_text("# beta solo\nassets = [\"GC\"]\n")
    (root / "top.toml").write_text("# a preset at the root\nassets = [\"ES\"]\n")

    # Machinery, never a choice.
    (root / "_scratch.toml").write_text("assets = []\n")
    # The other two axes, plus tooling and git metadata — not strategies.
    (root / "fill" / "optimistic.toml").write_text("# optimistic\n[fill]\n")
    (root / "fill" / "pessimistic.toml").write_text("# pessimistic\n[fill]\n")
    (root / "datasets" / "live.toml").write_text("# live: no overrides\n")
    (root / "datasets" / "hist.toml").write_text(
        "# historical\n[[source]]\nasset = \"ES\"\nfiles = [\"ES\"]\n"
    )
    (root / "tools" / "helper.toml").write_text("assets = []\n")
    (root / ".git" / "config.toml").write_text("assets = []\n")
    return root


def _load(root: Path, monkeypatch: pytest.MonkeyPatch):
    """Import viz.server with `root` configured, as a fresh server would."""
    monkeypatch.setenv("BT_STRATEGIES_DIR", str(root))
    monkeypatch.delenv("ICT_STRATEGIES_DIR", raising=False)
    sys.modules.pop("viz.server", None)
    return importlib.import_module("viz.server")


@pytest.fixture
def server(tmp_path, monkeypatch):
    mod = _load(_make_root(tmp_path), monkeypatch)
    yield mod
    sys.modules.pop("viz.server", None)


def test_presets_are_named_by_their_path_under_the_root(server):
    """No prefix of any kind: the name is what the tree calls the preset, so a
    deep link made on one machine means the same file on another."""
    assert server._list_strategies(all_presets=True) == [
        "alpha/one.toml",
        "alpha/two.toml",
        "beta/solo.toml",
        "top.toml",
    ]


def test_the_other_axes_are_not_on_the_strategy_axis(server):
    """fill/, datasets/, tools/ and .git/ hold something other than strategies."""
    listed = server._list_strategies(all_presets=True)
    assert not [n for n in listed if n.split("/")[0] in server.NON_STRATEGY_DIRS]
    # They are still the fill and dataset axes, read from the same root.
    assert server._list_fills() == ["optimistic.toml", "pessimistic.toml"]
    assert server._list_datasets() == ["hist.toml", "live.toml"]


def test_underscore_files_are_machinery_not_choices(server):
    assert "_scratch.toml" not in server._list_strategies(all_presets=True)


def test_the_picker_sets_both_contents_and_order(server, tmp_path):
    """`presets = [...]` is the dropdown, verbatim — not a filter over a sort."""
    (server.STRATEGY_DIR / "_picker.toml").write_text(
        'presets = ["beta/solo.toml", "alpha/one.toml"]\n'
    )
    assert server._list_strategies() == ["beta/solo.toml", "alpha/one.toml"]


def test_a_hidden_preset_is_still_runnable_by_name(server):
    """Hiding is a picker concern. The run endpoint validates against the full
    set, so a reference port or a candidate stays reachable by name."""
    (server.STRATEGY_DIR / "_picker.toml").write_text('presets = ["alpha/one.toml"]\n')
    assert server._list_strategies() == ["alpha/one.toml"]
    assert "beta/solo.toml" in server._list_strategies(all_presets=True)


def test_a_picker_name_with_no_file_is_dropped_not_invented(server):
    (server.STRATEGY_DIR / "_picker.toml").write_text(
        'presets = ["alpha/one.toml", "gone/missing.toml"]\n'
    )
    assert server._list_strategies() == ["alpha/one.toml"]


def test_an_unreadable_picker_lists_everything(server):
    """Degrading to the full list beats showing an empty dropdown."""
    (server.STRATEGY_DIR / "_picker.toml").write_text("presets = 3\n")
    assert server._list_strategies() == server._list_strategies(all_presets=True)


def test_the_default_is_the_pickers_first_entry(server):
    """The tree declares what it opens on; this repo does not name it."""
    (server.STRATEGY_DIR / "_picker.toml").write_text(
        'presets = ["beta/solo.toml", "alpha/one.toml"]\n'
    )
    assert server._default_strategy() == "beta/solo.toml"


def test_a_preset_can_pin_the_lens_its_numbers_were_graded_under(server):
    assert server._strategy_viz_fill("alpha/two.toml") == "pessimistic.toml"
    assert server._strategy_viz_fill("alpha/one.toml") is None


def test_the_session_preset_stays_out_of_the_strategies_tree(server, tmp_path, monkeypatch):
    """A root is normally a git checkout; a file rewritten per run must not
    dirty it. Leaving the tree means `base` has to be absolute."""
    monkeypatch.chdir(tmp_path)
    written = server.BacktestVizServer._write_ui_strategy(
        "alpha/one.toml", {"ES": {"files": ["ES"], "scale": 1.0, "offset": 0.0}},
    )
    assert server.STRATEGY_DIR not in Path(written).resolve().parents
    base_line = next(
        ln for ln in Path(written).read_text().splitlines() if ln.startswith("base =")
    )
    assert str(server.STRATEGY_DIR / "alpha" / "one.toml") in base_line


def test_a_root_that_is_not_a_directory_is_ignored(tmp_path, monkeypatch):
    monkeypatch.setenv("BT_STRATEGIES_DIR", str(tmp_path / "nope"))
    monkeypatch.setenv("ICT_STRATEGIES_DIR", str(_make_root(tmp_path)))
    sys.modules.pop("viz.server", None)
    mod = importlib.import_module("viz.server")
    try:
        assert mod.STRATEGIES_ROOT == (tmp_path / "strategies").resolve()
    finally:
        sys.modules.pop("viz.server", None)


def test_with_no_root_the_repo_falls_back_to_its_own_demos(tmp_path, monkeypatch):
    """A fresh clone with no private checkout still has a working picker."""
    monkeypatch.delenv("BT_STRATEGIES_DIR", raising=False)
    monkeypatch.delenv("ICT_STRATEGIES_DIR", raising=False)
    sys.modules.pop("viz.server", None)
    mod = importlib.import_module("viz.server")
    try:
        empty = tmp_path / "bare-repo"
        empty.mkdir()
        monkeypatch.setattr(mod, "REPO_ROOT", empty)
        assert mod._resolve_strategies_root() is None
    finally:
        sys.modules.pop("viz.server", None)
