# Next steps requiring external setup

These steps deliberately remain manual because they require personal accounts or secrets:

1. Copy `.env.example` to `.env` and set a compliant SEC User-Agent locally.
2. Create an Alpaca Basic data account only when historical SIP download begins.
3. Open and fund an IBKR account, activate its paper user and verify non-professional
   Network A/B/C pricing before subscribing.
4. Install Docker Desktop before enabling the isolated Hermes sidecar.
5. Label the 100-filing reference set before selecting any model for actionable use.

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
