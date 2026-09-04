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
- Added `domain.money()` as the single normalization for every computed amount, and
  applied it across the IBKR callback, bar, quote, portfolio and composition paths.
  An unrepresentable amount now raises `ArithmeticError`, which those paths already
  treat as a fail-closed fact.
- Fixed an unnormalized weighted average fill price that raised a `ValidationError`
  inside the IBKR reader thread for any multi-fill order whose average did not divide
  evenly, and five further occurrences of the same pattern.
- Replaced the non-validating `model_copy` commission finalization with a validating
  construction.
- Added a broker callback fault latch: a callback that cannot build a valid fact makes
  `ready_for_orders()` false and `reconcile_orders()` refuse, reproducing the
  fail-closed effect that a raised callback used to have by killing the reader thread.
- Callbacks discarded outside a reconciliation window are remembered and folded into
  the next one instead of vanishing, and a contract failure in `updatePortfolio` no
  longer drops a whole position behind a blanket `except`.
- Changed the default IBKR client id to 0 and made `build_paper_runtime` refuse any
  other value; only client 0 observes manual and API orders in one authoritative scope.
- Unified the `DU<digits>` paper-account rule in one strict function; configuration and
  composition previously accepted any `DU` prefix.
- Removed `PaperStartupGate`, `_ensure_execution_state()` and the internal `getattr`
  fallbacks, and replaced five `object.__new__` test doubles with
  `NativeIBAPIBackend.without_transport()`.
- Narrowed `# pragma: no cover` from the whole native backend to the three methods that
  need a live Gateway, so the callback reducers that carry order and fill truth are
  measured. The reported branch coverage fell about one point as a result.
- Made an aborted IBKR reconciliation lossless and repeatable: remembered discards are
  handed back instead of vanishing with the failed run, and the session no longer stays
  marked as reconciling after a single timeout.
- Applied the strict `DU<digits>` rule in the orchestrator, which was the last check
  still accepting any `DU` prefix, and moved the client-id-0 requirement ahead of the
  market stack so paper mode refuses before opening a Gateway connection.
- Covered the pre-submit market re-check, which previously carried no test at all:
  removing it left the whole suite green. All ten of its rejection reasons are now
  asserted, and the three that the strategy and risk engine also emit are asserted on
  the exit path, where only the guard can produce them.
- Recorded the callback fault latch and the single `money()` normalization as ADR-026
  and ADR-027, and completed ADR-023 with the shared mandatory readiness set.
- Added `collect-cover-page-facts` and `build-eligibility-manifest`: the point-in-time
  eligibility manifest is now derived from the Section 12(b) cover page of the filings
  themselves instead of being hand-filled from a security master the project does not
  have (ADR-028). Collection is SEC-only, appends one durable line per filing and
  resumes after an interruption; derivation is offline, deterministic and never
  overwrites an existing manifest.
- Grouped cover-page facts by XBRL context, so a filer with several registered classes
  cannot have the common-stock title of one class attributed to the symbol of another.
  A unit or warrant whose own title names common stock is classified as non-common.
- Left `corporate_actions_complete` unset in every derived interval. No cover page
  establishes it, so the affected coverage stays an explicit gap, never a silent pass.
- Indexed `CsvEligibilityResolver` by CIK and symbol. It scanned every interval on every
  lookup, which a full backfill would have turned into a quadratic cost against a
  manifest carrying one row per registered class.
- Kept a contradicted cover-page fact unknown across every later agreeing context. The
  facts were merged pairwise, so a third context could overwrite an earlier conflict and
  the outcome depended on the order the filing tagged its classes in.
- Terminated a complete but unterminated final record before appending to the collected
  evidence. An interruption between a record and its newline made the next append
  concatenate onto it, and the torn-line recovery then discarded both records at once.
- Now 534 deterministic, contract, property, replay and recovery tests with 83.26 %
  branch coverage, `ruff check` and `ruff format` clean. Every new rule is
  mutation-checked: ten reverted rules, each colouring at least one test red.
