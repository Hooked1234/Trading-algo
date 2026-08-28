# Next steps requiring external setup

These steps deliberately remain manual because they require personal accounts or secrets:

1. Copy `.env.example` to `.env` and set a compliant SEC User-Agent locally.
2. Create an Alpaca Basic data account only when historical SIP download begins.
3. Open and fund an IBKR account, activate its paper user and verify non-professional
   Network A/B/C pricing before subscribing.
4. Install Docker Desktop before enabling the isolated Hermes sidecar.
5. Label the 100-filing reference set before selecting any model for actionable use.

Until the research gate passes, keep `execution_enabled=false` and use shadow/replay only.

## Remaining internal gates

Gate B is complete in code and tests. Starting it needs a configured SEC user agent
and a local IB Gateway session; `run-shadow-once` refuses with the exact missing
prerequisite until both exist. The IBKR bar volume scale (shares versus lots) must be
verified once against a known session before the liquidity filter is trusted; the hook
exposes `volume_multiplier` for that and defaults to shares.

Gate C — IBKR paper execution hardening — is complete in code and automated tests.
The implementation now includes the versioned fill ledger, monotone `execId` callback
aggregation, commission finalization, operation-specific readiness profiles,
restart-safe single-generation replacement orders, exact pre-exit position matching,
`PaperRecoveryCoordinator`, `build_paper_runtime` and `run-paper` (ADR-020 to ADR-025).

Remaining external acceptance only:

- Verify native `orderStatus`, `execDetails` and `commissionReport` ordering against a
  real local IB Gateway paper session, including duplicate and delayed callbacks.
- Perform the controlled Gateway restart test with open, partially filled and
  unknown-outcome orders; every unknown result must remain in manual hold.
- Verify the IBKR bar volume multiplier against a known session before relying on the
  liquidity filter.
- Complete the 30-session/50-closed-paper-trade acceptance window before treating the
  paper runtime as operationally proven.
