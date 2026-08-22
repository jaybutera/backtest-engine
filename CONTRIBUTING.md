# Contributing

## Before you open a PR

    ./scripts/check-leaks.sh
    cargo test
    cargo clippy -- -D warnings

CI runs all three.

## What the leak check is for

This repo was extracted from a private trading system. The signal logic and the
research behind it stayed private; the harness came here. `check-leaks.sh`
enforces that boundary, and it will fail a PR for things that look harmless in
isolation:

- **Measured results.** A comment reporting what a change earned over a sample is a research finding, not documentation. State what the code does. <!-- leak-check: ok teaching the rule -->
- **Dated annotations.** A date introducing a decision reads as a lab notebook. Say why the code is the way it is, without the date. <!-- leak-check: ok teaching the rule -->
- **Campaign and preset names** from the private repo.
- **Account identifiers, wallets, hostnames, personal paths.**

If a check fires on something legitimate, narrow the pattern in the script
rather than deleting the rule, and say why in the PR.

## Comment style

Explain mechanism and intent. The fill models especially are full of decisions
that look arbitrary until you know what they model — those comments are worth
writing, and worth writing without reference to any particular instrument or
result.

Good:

    // A chase that would fill worse than the cap abandons instead: a watchdog
    // that chases without limit turns a missed entry into a bad one.

Not good:

    // Chasing past 0.3R lost 4.1R over the 2024 sample, see the fill campaign.

## Tests

The fill-model tests in `src/paper.rs` are the most valuable thing in this
repo, and they are hermetic — hand-built candles, no fixtures, no market data.
Keep them that way. A test that needs a data file is a test nobody else can
run.

Any change to fill behavior needs a test that fails before it and passes after.
"It looked right in a backtest" is not evidence; the whole premise here is that
backtest results are the thing under suspicion.

## Adding a fill lens

New lenses are welcome, especially ones modeling venues or order types the
bundled set does not. A lens should document what it is optimistic about and
what it is pessimistic about — every model is wrong somewhere, and saying where
is what makes it useful.

## Scope

This is a harness, not a strategy library. PRs adding signal detectors, entry
rules, or indicator libraries are out of scope; the `Strategy` trait is there
so those can live in your own crate. Improvements to fills, fees, accounting,
data loading, reporting, and the visualizer are all in scope.
