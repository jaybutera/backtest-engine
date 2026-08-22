"""Backtest visualizer: renders an engine run's trades on candle charts.

The package is a thin read-only view over two artifacts the engine produces:

  * `data/backtest_trades.json` — the JSON sidecar written by the replay
    binary (`--json-sidecar`), holding every decided trade plus run summary
    counters. See `viz/README.md` for the exact shape.
  * `data/<stem>_1m.parquet` — the OHLC candle files the run was graded on.

Nothing here talks to a broker or an exchange. Everything it renders comes
off local disk.
"""

__all__ = ["ingest", "fills", "registry", "server"]
