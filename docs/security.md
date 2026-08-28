# Security and execution boundary

## Paper-only invariant

- Application settings accept only the literal environment `paper`.
- Account identifiers must begin with IBKR's paper prefix `DU` and be allowlisted.
- The broker adapter revalidates both conditions immediately before every submission.
- No live credentials, live endpoint or generic `enable_live` flag exists in version 1.

A future live release must use a separate deployment, credentials, configuration model,
manual approval checklist and additional acceptance evidence. It must not weaken this
package in place.

## Untrusted filing text and Hermes

SEC documents are untrusted input even though their source is official. Filing text is
sanitized, length-limited and sent as data inside a fixed prompt. Model output is parsed
again by the Python core. Invalid, late or event-mismatched output becomes `Abstain`.

The prepared Hermes lab profile uses a separate container with no broker secrets or
shared writable trading volumes. It is configured for no terminal, filesystem, memory,
delegation or self-modification tools and exposes only a loopback analysis endpoint.
This boundary is not yet operationally certified: Docker and egress/capability checks
must pass before the sidecar is started. Until then Hermes remains disabled or shadow-only.

## Operational fail-closed conditions

New orders are blocked for stale data, trading halt, broker disconnect, reconciliation
mismatch, duplicate signal/order, risk-limit breach, invalid filing or invalid insight.
All order intents use deterministic idempotency keys and are persisted before submission.

A content-addressed promotion artifact is also mandatory. It binds the passing research
evidence to exact experiment, dataset and code hashes; a boolean configuration value is
insufficient. Daily-loss and drawdown stops are durable latches with audited manual reset.
