# Local operations runbook

## Before every shadow or paper session

1. Start IB Gateway manually and select the Paper Trading session. Set
   `TRADING_IBKR_PORT` to match: 4002 for IB Gateway, 7497 for TWS (the default).
2. Run `uv run event-trader doctor`.
3. Confirm the reported account matches `DU<digits>`, is allowlisted, and live
   execution is false. For paper mode also confirm `TRADING_IBKR_CLIENT_ID=0`;
   `doctor` does not report it, so it has to be checked in `.env`.
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
`TRADING_PROMOTION_ARTIFACT_PATH` to the immutable promotion artifact. Set
`TRADING_IBKR_CLIENT_ID=0`; paper recovery requires client 0 so manual TWS orders
are included in the authoritative order scope.

The dataset manifest is produced by `build-dataset-manifest`. The experiment and
code manifests are bring-your-own files: no project command generates them, and
`create-promotion` only hashes their exact bytes. Freeze all three files before
promotion and pass the same byte-identical files to both commands. Omitting
`--output` writes the promotion artifact to `TRADING_PROMOTION_ARTIFACT_PATH`:

```bash
uv run event-trader create-promotion data/state/research-result.json \
  --research-trades-json data/state/research-trades.json \
  --experiment-manifest data/state/experiment-manifest.json \
  --dataset-manifest data/state/dataset-manifest.json \
  --code-manifest data/state/code-manifest.json \
  --experiment-id <experiment-id>
```

The runtime then re-hashes those same files and requires the experiment, dataset
and code fingerprints to match the promotion artifact exactly:

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

After a process restart, `restore_from_storage()` must run before `reconcile()`.
Restored orders that are still open or are not confirmed by the broker block new
orders. An unknown outcome is never resolved by submitting the order again.

## Incident recovery

1. Disable paper execution and leave ingestion running if safe.
2. Reconnect and fetch broker orders/executions/positions.
3. Compare with the SQLite ledger; never re-submit an unknown-outcome order.
4. Close or reconcile unintended exposure manually in the paper account.
5. Record the incident in the daily report and add a deterministic replay before resuming.

Before paper approval, validate the native IBKR callback layer once against a real
local Gateway paper session. Unit tests do not replace market-data permissions or
broker-side fill and restart semantics.
