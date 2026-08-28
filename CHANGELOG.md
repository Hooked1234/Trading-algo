# Changelog

## 0.1.0 — Unreleased

- Initial paper-only SEC 8-K event-trading MVP foundation.
- Added resumable 2019-01-01 through 2026-06-30 SEC/Alpaca-SIP backfill and DuckDB-ready
  immutable Parquet datasets.
- Added quant-only and keyword baselines, quarantined Hermes adapter, net-cost backtester,
  direction-specific research gates and model benchmarking.
- Added IBKR live market-data and paper broker adapters with recovery holds, reconciliation,
  one-reprice execution and no-resubmit idempotency.
- Added deterministic, contract, property, replay and recovery tests.
- Added a hashed research evidence chain: dataset manifest, versioned trading-state
  manifest, insight-free research cases, pinned insight artifacts and variant run
  artifacts, all content-addressed and written without overwriting.
- Made `case_input_sha256` independent of insights, local paths and ingestion
  timestamps, so quant-only, keyword and AI runs share one case identity.
- Replaced the single-case backtest loop with a portfolio runner that marks open
  positions to market and refreshes exposure, daily loss and drawdown before every
  decision.
- Restricted research gates to complete run artifacts and added the
  `build-dataset-manifest`, `build-research-cases`, `build-insight-artifact`,
  `run-backtest`, `research-gate --run` and `paired-ai-gate` commands.
- Pruned Parquet reads to the required date/symbol and quarter partitions and extended
  the data-quality report to per-filing accounting across all 1/3/5/10-minute lags.
- Added a deterministic candidate gate before every model call, an immutable
  per-event analysis store keyed by event, documents, input, model, prompt and
  schema, and a transactional insight/outcome/outbox completion.
- Added an IBKR-only runtime feature provider with native bar callbacks, and a
  shadow composition root with `run-shadow-once`, `run-shadow`, `session-report`
  and `acceptance-from-reports`.
- Replaced the serial runtime loop with supervised SEC, entry and exit tasks, a
  SQLite singleton daemon lease and a hashed session report written from durable
  state.
- Fixed `hermes_max_input_chars` exceeding the insight provider's hard input cap.
- Added a versioned operational schema: `PRAGMA user_version` with an ordered,
  transactional migration chain, a fail-closed refusal to open a newer schema and a
  store that closes its connection when a migration fails.
- Added `ExecutionFill` and a per-fill ledger keyed by the broker execution id, with
  idempotent replay and exactly one later commission finalization.
- Extended `ExecutionReport` with fill accounting and a broker update sequence, and
  `OrderIntent` with an explicit reprice lineage that replaces the `:r1` key suffix as
  the source of truth.
- Extended monotonicity to counted fills and update sequence, and closed a gap where
  runtime transitions did not enforce fee monotonicity.
- Made native IBKR callbacks monotone and idempotent across delayed `orderStatus`,
  `execDetails` and `commissionReport` delivery; finalized broker commissions now flow
  through execution fees into net P&L.
- Added explicit submit, exit, cancel and reconcile readiness profiles, restart-safe
  one-generation replacement recovery and an exact broker-position check immediately
  before every exit submission.
- Added `PaperRecoveryCoordinator`, the order-capable `build_paper_runtime` composition
  root and the fail-closed `run-paper` command bound to a DU paper account, promotion
  artifact, runtime manifests and live preflight.
- Added an automated schema inventory check for the exactly 16 operational tables and
  removed obsolete local Temp-workaround documentation and artifacts.
- Now 474 deterministic, contract, property, replay and recovery tests with 83.57 %
  branch coverage.
