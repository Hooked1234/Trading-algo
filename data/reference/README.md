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

Copy `historical_eligibility.example.csv` to `historical_eligibility.csv` only after
populating it from an authoritative, auditable historical security master. Each row is
matched by CIK, symbol and inclusive validity dates. `known_at` is the timestamp at which
the classification evidence was publicly knowable and must not be later than the filing.

The three confirmation columns accept `true`, `false`, `unknown` or blank. Unknown or
missing rows remain explicit coverage gaps. Current index membership is not acceptable
evidence, and the file must not be filled with inferred values merely to make a backtest
run. Its SHA-256 is embedded in each resolved coverage record.
