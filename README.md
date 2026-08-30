# Event Trader

Lokales, ereignisgetriebenes Research- und **Paper-Trading-System** für SEC-8-K-Filings.
Deterministischer Python-Code entscheidet über Strategie, Risiko und Orders; ein optionaler,
isolierter Hermes-Agent darf ausschließlich strukturierte Filing-Analysen liefern.

> **Sicherheitsgrenze:** Diese Version akzeptiert nur IBKR-Paper-Konten mit `DU`-Kennung.
> Echtgeldhandel ist weder per Schalter noch per Umgebungsvariable aktivierbar.

## Architektur

```text
SEC Atom/Archive ─┐
Alpaca history ───┼─> normalized event snapshot ─> strategy ─> risk ─> broker
IBKR live data ───┘              ▲                                  │
                                 └── isolated Hermes insight         └─> IBKR paper
```

Raw filings remain immutable and hashed. Parquet/DuckDB are used for research data;
SQLite is the operational ledger and durable outbox. Replays and paper sessions call
the same strategy and risk code.

## Local setup

Prerequisites: Windows, `uv`, Python 3.12 (managed automatically), and optionally Docker
Desktop for Hermes plus IB Gateway for paper trading.

```powershell
uv sync --extra dev
Copy-Item .env.example .env
uv run event-trader doctor
uv run pytest
uv run pytest --cov=event_trader
```

Für den offiziellen IBKR-Adapter zusätzlich `uv sync --extra dev --extra ibkr`.

Set `TRADING_SEC_USER_AGENT` locally to a project name and contact address as required by
the SEC. Never commit `.env`, API keys or account identifiers. Keep the default paper
port and add the actual `DU<digits>` account to the explicit allowlist before using IBKR.

Paper mode additionally requires `TRADING_IBKR_CLIENT_ID=0`. Only client 0 observes
manual and API orders in one authoritative scope, so any other value makes `run-paper`
refuse before it opens a Gateway connection.

## Operating sequence

1. Run deterministic tests and historical replay.
2. Prüfe den Backfill offline mit `uv run event-trader backfill-plan` und starte ihn
   nach lokaler SEC-/Alpaca-Konfiguration mit `uv run event-trader historical-backfill`.
3. Build the historical data-quality report; it accounts for every filing across all
   registered 1/3/5/10-minute availability lags.
4. Build the hashed research evidence chain (`build-dataset-manifest`,
   `build-research-cases`, `build-insight-artifact`, `run-backtest`) and evaluate the
   pre-registered gate with `research-gate --run` plus `paired-ai-gate`.
5. Run real-time shadow mode.
6. After the gate passes, create the immutable promotion artifact and run paper mode
   only with matching experiment, dataset and code manifests; see the operations
   runbook for the fail-closed `run-paper` command.

Paper fills validate operation, not profitability. A failed hypothesis stays archived;
it is not tuned against the holdout.

See [research protocol](docs/research-protocol.md), [security model](docs/security.md),
[test strategy](docs/testing.md), [operations runbook](docs/operations.md), and
[next steps](NEXT_STEPS.md).
