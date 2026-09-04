# Pre-registered research protocol

## Primary hypothesis

A high-materiality, directional SEC 8-K followed by aligned abnormal return, relative
volume and VWAP confirmation has positive net continuation expectancy over 60 minutes.
Long and short directions are evaluated separately.

## Frozen sample design

- Development/calibration: 2019-01-01 through 2023-12-31.
- Validation: 2024-01-01 through 2024-12-31.
- Untouched holdout: 2025-01-01 through 2026-06-30.
- Primary historical website-availability lag: five minutes after SEC acceptance.
- Sensitivity lags: one, three and ten minutes.
- One primary strategy version and one change per later research cycle.

The current constituent list of an index must never define the historical universe.
CIK/accession and the symbol reported with the historical filing are the event keys.
Missing/delisted symbols, corporate-action uncertainty and unavailable quotes are
coverage failures, not zero-return trades.

Out-of-sample labels are derived only from the frozen calendar ranges above. A record
outside 2024-01-01 through 2026-06-30 cannot be relabelled as out of sample. Once the
holdout has been inspected, later observations require a new experiment and remain
forward/paper evidence rather than silently extending this holdout.

## Fixed candidate and execution rules

- Form 8-K only; amendments are stored but not traded.
- Relevant items: 1.01, 2.01, 2.02, 5.02, 7.01 and 8.01.
- Price >= USD 5; trailing 20-session median dollar volume >= USD 20 million.
- Five-minute beta-adjusted return z-score aligned with insight and magnitude >= 1.5.
- Same-clock relative volume >= 2; correct side of session VWAP; spread <= 20 bps.
- Entry window 09:40-14:45 America/New_York; flat by 15:55; primary exit 60 minutes.
- Historical transaction costs include observed spread, commission and five basis
  points of additional slippage per side; the stress case doubles all costs.

## Promotion gate

- At least 200 out-of-sample trades and 50 for each enabled direction.
- Day-blocked bootstrap lower 95% bound of net expectancy above zero.
- At least three of four chronological windows positive after costs.
- Doubled-cost result non-negative.
- No symbol or event category contributes more than 25% of positive P&L.
- An AI-gated variant must improve the quant-only baseline out of sample before its
  insight may affect orders. Otherwise Hermes remains explanatory shadow output.

Viewing the holdout freezes it. Any subsequent change requires a new experiment ID and
future, previously unseen data.

The backfill requests `feed=sip` explicitly and records provider plus feed on every row.
It must never fall back to IEX. Provider entitlement, time semantics, gaps and coverage
are part of the data-quality gate, not setup assumptions.

## Reproducible evidence chain

The backfill needs point-in-time eligibility before it runs; without it every coverage
record is a gap and no case is ever built. The manifest is derived from the Section 12(b)
cover page of the filings themselves (ADR-028):

```bash
event-trader collect-cover-page-facts --output cover-page-facts.jsonl
event-trader build-eligibility-manifest cover-page-facts.jsonl --output historical_eligibility.csv
```

Every gate decision is then reconstructed from hashed artifacts, in this order:

```bash
event-trader build-dataset-manifest <lake> --output dataset.json
event-trader build-research-cases coverage.json --lake-root <lake> --raw-root <raw> \
  --trading-state-manifest halts.json --dataset-manifest dataset.json \
  --lag-minutes 5 --output cases.json
event-trader build-insight-artifact insights.json --cases cases.json \
  --variant ai --model-id <provider/model> --output insights-ai.json
event-trader run-backtest --cases cases.json --variant quant-only --output run-quant.json
event-trader run-backtest --cases cases.json --variant ai \
  --insights insights-ai.json --output run-ai.json
event-trader research-gate --run run-quant.json
event-trader paired-ai-gate --quant-run run-quant.json --ai-run run-ai.json
```

Artifacts are written exclusively: an existing target is an error, never an overwrite.
`research-gate` and `paired-ai-gate` accept only complete run artifacts and re-verify
their content addresses first. The paired gate additionally requires both runs to share
one case artifact and identical case hashes, so an AI variant cannot be compared against
a differently-built baseline.

Insight candidates are preselected deterministically from price and volume evidence
only. A case the deterministic gate already rejects never reaches a model, and the
quant-only variant never constructs an insight at all.
