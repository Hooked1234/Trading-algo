# Next steps requiring external setup

These steps deliberately remain manual because they require personal accounts or secrets:

1. Copy `.env.example` to `.env` and set a compliant SEC User-Agent locally.
2. Create an Alpaca Basic data account only when historical SIP download begins.
3. Open and fund an IBKR account, activate its paper user and verify non-professional
   Network A/B/C pricing before subscribing.
4. Install Docker Desktop before enabling the isolated Hermes sidecar.
5. Label the 100-filing reference set before selecting any model for actionable use.
6. Supply an auditable corporate-action source for the third eligibility confirmation;
   see the sequenced plan below for why nothing else can fill that column.

Until the research gate passes, use `run-shadow`, `run-shadow-once` or replay only.
There is no `execution_enabled` setting: the order path exists solely in the
`run-paper` composition root, which refuses to build without a passed promotion.

## Remaining internal gates

Gate B is complete in code and tests. Starting it needs a configured SEC user agent
and a local IB Gateway session; `run-shadow-once` refuses with the exact missing
prerequisite until both exist. The IBKR bar volume scale (shares versus lots) must be
verified once against a known session before the liquidity filter is trusted; the hook
exposes `volume_multiplier` for that and defaults to shares.

Gate C — IBKR paper execution hardening — is complete in code and automated tests.
The implementation includes the versioned fill ledger, monotone `execId` callback
aggregation, commission finalization, operation-specific readiness profiles,
restart-safe single-generation replacement orders, exact pre-exit position matching,
`PaperRecoveryCoordinator`, `build_paper_runtime` and `run-paper` (ADR-020 to ADR-025).

A review on 2026-08-28 found and closed two blockers that this claim did not yet hold
against: an unnormalized weighted average fill price that raised inside the IBKR
callback thread on any multi-fill order whose average did not divide evenly, and a
default `TRADING_IBKR_CLIENT_ID` of 71 against a readiness check that requires client 0
for every profile. Paper mode could not have started with the shipped configuration.
Both are fixed, regression-tested and covered by a mutation check.

Remaining external acceptance only:

- Verify native `orderStatus`, `execDetails` and `commissionReport` ordering against a
  real local IB Gateway paper session, including duplicate and delayed callbacks.
- Perform the controlled Gateway restart test with open, partially filled and
  unknown-outcome orders; every unknown result must remain in manual hold.
- Verify the IBKR bar volume multiplier against a known session before relying on the
  liquidity filter.
- Complete the 30-session/50-closed-paper-trade acceptance window before treating the
  paper runtime as operationally proven.
- Verify the `_NativeClient` callback guard against a real session. `ibapi` is not
  installed locally, so the class the decorator wraps does not exist here and the
  wiring cannot be exercised. The fault latch it feeds is fully covered; only the
  connection between the two is unverified.

## Sequenced plan to a research-gate decision

Gates 0/A/B/C are complete in code and tests, but nothing has run against real data yet.
The path to a gate decision is blocked first by two point-in-time reference datasets the
code requires and did not build, not by accounts or subscriptions:

1. `historical_eligibility.csv`. Without it the backfill's eligibility resolver is unset,
   every coverage record becomes `MISSING_POINT_IN_TIME_ELIGIBILITY`, and
   `build-research-cases` excludes every case. Note that the backfill resolves eligibility
   *after* it has already fetched market data, so running it without the manifest spends
   the whole Alpaca budget and still yields no tradable coverage.
2. The trading-state manifest that `build-research-cases --trading-state-manifest`
   requires. `TradingStateManifest` is only ever read, never built. An unknown halt state
   excludes the case, and the entry's `as_of` may precede the decision instant by at most
   five seconds — so this is one entry per symbol and decision instant, not a maintained
   per-day reference file.

### Gate D — manifest builders (no external account needed)

- Done: `collect-cover-page-facts` and `build-eligibility-manifest` derive
  `common_stock` and `us_listing` from the filing's own Section 12(b) cover page
  (ADR-028). `corporate_actions_complete` stays unset by design.
- Open: an auditable corporate-action source for the third column. Until it exists the
  derived manifest keeps every filing an explicit coverage gap, which is honest but still
  yields no research cases.
- Open: a builder for the trading-state manifest — one entry per coverage record at the
  evaluation instant, `halted` from an auditable halt history. Leaving `shortable` unset
  restricts the evaluation to long trades, which ADR-015 permits and the promotion gate
  allows because it requires 50 trades per *enabled* direction. That avoids buying
  historical borrow data for the first research cycle.

### Gate E — backfill (SEC user agent free, Alpaca SIP paid)

Run one pilot quarter from the development range first and measure filing volume,
runtime, rate limits and data quality before committing to all 30 quarters. Do not touch
the validation or holdout ranges for a pilot. `data-quality` gates everything downstream.

### Gate F — research decision

Run the `quant-only` variant first. If the baseline fails, the reference labels, the
Hermes sidecar and Docker are all unnecessary. Open the holdout exactly once.

### Gate G — IBKR acceptance (only after Gate F passes)

Account, paper user, market-data subscriptions and Gateway, then the external acceptance
items listed above. Shadow mode validates operation, not edge; running it before a passed
research gate hardens a strategy that has no evidence behind it.
