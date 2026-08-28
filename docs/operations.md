# Local operations runbook

## Before every shadow or paper session

1. Start IB Gateway manually and select the Paper Trading session.
2. Run `uv run event-trader doctor`.
3. Confirm the reported account starts with `DU`, is allowlisted, and live execution is false.
4. Reconcile broker orders and positions before processing any new event.
5. Confirm Networks A/B/C are live rather than delayed and system time is synchronized.
6. Keep the research gate false until its immutable result artifact passes.

## Shadow mode

```bash
uv run event-trader run-shadow-once --variant keyword
uv run event-trader run-shadow --variant keyword
```

Shadow mode uses a broker that cannot submit, cancel or reprice, and a virtual
`SHADOW-VIRTUAL` account. It needs a configured SEC user agent and a local IB
Gateway session for runtime market data; without them the command refuses to
start and names the missing prerequisite.

Three supervised tasks run concurrently: SEC polling, the entry/insight pipeline
and exit supervision. Each entry worker leases exactly one event, so a slow model
call never holds a queue of unrelated filings. A SQLite singleton lease refuses a
second daemon against the same state file.

When the run ends, the session report is written automatically from durable
state as an immutable markdown file plus a hashed JSON artifact:

```bash
uv run event-trader session-report --session 2026-08-25
uv run event-trader acceptance-from-reports
```

## Paper mode

Paper mode is the only order-capable runtime. Before starting it, configure the
SEC user agent, install the `ibkr` extra, authenticate a local IB Gateway Paper
Trading session, set an allowlisted `TRADING_PAPER_ACCOUNT_ID=DU<digits>` and point
`TRADING_PROMOTION_ARTIFACT_PATH` to the immutable promotion artifact.

The three supplied manifest files must hash to the exact experiment, dataset and
code fingerprints recorded in that artifact:

```bash
uv run event-trader run-paper \
  --experiment-manifest data/state/experiment-manifest.json \
  --dataset-manifest data/state/dataset-manifest.json \
  --code-manifest data/state/code-manifest.json \
  --variant quant-only
```

Startup is fail-closed and ordered: restore persisted orders, reconcile broker
orders/executions/positions, persist fills and reports, resume only confirmed
workflows, then reconcile again. Missing promotion, a manifest mismatch, a non-DU
or absent account, incomplete reconciliation, delayed market data, unresolved
orders or a failed live preflight abort the order path.

Immediately before every exit, the runtime fetches a fresh broker portfolio. The
position must match the exit symbol, direction and quantity exactly; any difference
blocks the submission for manual reconciliation.

## Immediate stop conditions

Stop new orders and reconcile before resuming when market data is stale, SEC reconciliation
has a gap, the broker disconnects, a position differs, a duplicate id appears, a symbol is
halted, daily loss reaches 1.5%, or drawdown reaches 5%.

The SEC Latest-Filings feed is a rolling page, not a durable stream offset. The poller reads
up to 100 entries; monitor successful poll intervals and reconcile every session against the
Daily Index/Submissions data. A cursor prevents duplicates but cannot prove completeness.

## Gateway lifecycle

IB Gateway/TWS requires periodic restart and manual authentication. Treat connection,
managed-paper-account discovery, live-data confirmation and a valid next order id as
separate readiness checks. A connected socket alone is not readiness.

Readiness is evaluated per operation: Reconcile establishes broker truth, Cancel
does not depend on market data, Exit requires live data plus reconciled state, and
Submit additionally requires all restored workflows to be terminal.

Nach einem Prozessneustart muss `restore_from_storage()` vor `reconcile()` ausgeführt
werden. Nicht vom Broker bestätigte oder noch offene wiederhergestellte Orders sperren
neue Orders. Ein unbekannter Ausgang wird niemals durch erneutes Senden aufgelöst.

## Incident recovery

1. Disable paper execution and leave ingestion running if safe.
2. Reconnect and fetch broker orders/executions/positions.
3. Compare with the SQLite ledger; never re-submit an unknown-outcome order.
4. Close or reconcile unintended exposure manually in the paper account.
5. Record the incident in the daily report and add a deterministic replay before resuming.

Der native IBKR-Callback-Layer muss vor Paper-Freigabe einmal gegen eine echte lokale
Gateway-Paper-Sitzung geprüft werden. Unit-Tests ersetzen weder Datenberechtigungen noch
die Broker-seitige Fill- und Restart-Semantik.
