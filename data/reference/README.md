# Filing reference labels

`labels.csv` is the manually reviewed source of truth for the 100-filing model benchmark.
Do not place customer data, broker information or credentials here.

For each sampled filing, read only documents that were publicly available by the recorded
historical time and assign:

- `category`: stable lowercase code such as `earnings`, `guidance`, `m_and_a`, `management`,
  `legal`, `capital_allocation`, `distress` or `other`;
- `direction`: `long`, `short` or `neutral` based on the document, not later price action;
- `materiality`: `low`, `medium` or `high`;
- `annotator_confidence`: decimal from 0 to 1;
- `notes`: short factual ambiguity note, without adding future market information.

Labels through 2024 may calibrate the model benchmark. Do not label the return outcome of
holdout filings while changing prompts or rules.

## Historical eligibility manifest

`historical_eligibility.csv` is generated, not hand-filled. The filing's own Section 12(b)
cover page is the authoritative point-in-time source, and two commands derive the manifest
from it (ADR-028):

```bash
uv run event-trader collect-cover-page-facts --output data/reference/cover-page-facts.jsonl
```

```bash
uv run event-trader build-eligibility-manifest data/reference/cover-page-facts.jsonl --output data/reference/historical_eligibility.csv
```

The first command is the only one that contacts SEC. It appends one durable line per
filing and is resumable, so an interrupted multi-hour run continues where it stopped;
re-run it until the reported `failed` count stops changing. The second is offline and
deterministic, and it refuses to overwrite an existing manifest.

`corporate_actions_complete` is deliberately left blank: no cover page establishes that a
symbol's corporate-action history is complete, so every derived interval stays an explicit
coverage gap until an auditable corporate-action source fills that column. Do not fill it
by hand to make a backtest run.

Each row is matched by CIK, symbol and inclusive validity dates. `known_at` is the
timestamp at which the classification evidence was publicly knowable and must not be later
than the filing. `historical_eligibility.example.csv` documents the exact header.

The three confirmation columns accept `true`, `false`, `unknown` or blank. Unknown or
missing rows remain explicit coverage gaps. Current index membership is not acceptable
evidence, and the file must not be filled with inferred values merely to make a backtest
run. Its SHA-256 is embedded in each resolved coverage record.
