# Architecture decisions

## ADR-001 — Deterministic Python control plane

Status: accepted, 2026-08-25.

Hermes may classify filing text but cannot decide risk or execute orders. Strategy, risk,
order state and reconciliation remain deterministic Python components.

## ADR-002 — Paper-only version 1

Status: accepted, 2026-08-25.

IBKR is the target broker. Version 1 accepts only allowlisted `DU...` paper accounts.
The live-capable interface is retained, but live configuration is intentionally absent.

## ADR-003 — Local modular monolith

Status: accepted, 2026-08-25.

Use one asynchronous local Python process, immutable raw data, Parquet/DuckDB research
storage and SQLite operational state. Distributed infrastructure is unjustified for MVP.

## ADR-004 — Gate-driven promotion

Status: accepted, 2026-08-25.

Failure to demonstrate the pre-registered edge keeps execution in shadow mode. Paper
orders are not used to bypass an unsuccessful historical result.

## ADR-005 — Unknown order outcome is a manual hold

Status: accepted, 2026-08-25.

Persisted open orders are restored locally before IBKR reconciliation. Missing remote
confirmation, an open restored order or a truncated recovery set blocks new submissions.
The system never resolves uncertainty by automatically resending an order.

## ADR-006 — Quant-only is the mandatory comparator

Status: accepted, 2026-08-25.

The price/volume-only strategy is evaluated without text semantics or model calls. An
AI-gated strategy can influence paper orders only after positive paired out-of-sample
improvement over that comparator.

## ADR-007 — Durable strategy risk state and latched halt

Status: accepted, 2026-08-25.

Risk limits use the fixed USD 100,000 strategy NAV, not the broker account NAV. Realized
and unrealized strategy P&L, peak equity and pending entry exposure are persisted and
included before every approval. A daily-loss or drawdown breach latches durably and can
only be cleared by an explicit, audited manual reset.

## ADR-008 — Promotion is a content-addressed artifact

Status: accepted, 2026-08-25.

A boolean research flag cannot authorize paper orders. Promotion requires an immutable
artifact binding the passed research result to experiment, dataset and code hashes,
strategy version, directions and—when applicable—paired AI and model evidence. Runtime
hashes must match exactly; absence or disagreement means shadow mode.

## ADR-009 — Paper submission requires refreshed evidence

Status: accepted, 2026-08-25.

Model latency invalidates the pre-model snapshot for execution. Immediately before an
order, the core refreshes market and portfolio state and rechecks live NBBO, staleness,
halt, shortability, SEC daily reconciliation, strategy rules, risk and quantity. Any
missing or contradictory fact blocks submission.

## ADR-010 — Exit supervision is deterministic and restart-safe

Status: accepted, 2026-08-25.

Hermes cannot manage positions. A deterministic supervisor owns timed exits, ATR stops,
the 15:55 New York flattening rule and the single permitted reprice. Persist-before-send,
idempotency and broker reconciliation take precedence over automatic recovery; an
unknown outcome is a manual hold.

## ADR-011 — Hermes remains a lab-only sidecar

Status: accepted, 2026-08-25.

The Python adapter and a hardened container profile are prepared, but Docker isolation
is not considered proven until capability, mount, secret and egress tests pass locally.
Until then Hermes is optional shadow analysis and cannot authorize orders.

## ADR-012 — Research evidence is a chain of hashed artifacts

Status: accepted, 2026-08-27.

A gate is never given a hand-assembled list of trades. Case building, insight pinning
and every variant run produce content-addressed artifacts that carry their own
`artifact_sha256`, are written with exclusive create semantics and are re-verified on
read. A case-building run accounts for every selected coverage record with exactly one
case, one typed exclusion or one typed integrity error, so no filing and no availability
scenario can quietly disappear between backfill and gate.

## ADR-013 — Case identity is insight-free

Status: accepted, 2026-08-27.

`case_input_sha256` is computed from an explicit allowlist: coverage, the verified
filing documents, the market path, eligibility, portfolio start state and the
point-in-time trading state. Model answers, local file paths and process timestamps are
excluded by construction. The quant-only, keyword and AI variants therefore evaluate
byte-identical cases with identical hashes, and the paired comparison is verified
against those hashes rather than trusted.

## ADR-014 — Historical availability replaces ingestion timestamps

Status: accepted, 2026-08-27.

`first_seen_at` and `retrieved_at` record when the ingestion process ran. A historical
case overwrites both with the registered counterfactual availability instant (SEC
acceptance plus the selected lag), which the decision schedule already uses. This keeps
the live staleness rule measuring the same distance it measures in production — the
previous behaviour measured acceptance-to-decision and therefore double-counted the
availability lag — and keeps the case hash free of process timestamps.

## ADR-015 — Halt and borrow evidence come from a versioned manifest

Status: accepted, 2026-08-27.

Halt and shortability are not derivable from OHLCV. A versioned trading-state manifest
supplies `symbol`, `as_of`, `known_at`, source, halt status and borrow evidence, and it
is hashed into the case artifact. An entry that only became knowable after the decision
is ignored. An unknown halt state excludes the case; unknown borrow evidence still
allows long evaluation but disables short. Missing evidence is never estimated.

## ADR-016 — A deterministic candidate gate precedes every model call

Status: accepted, 2026-08-27.

Price, volume, session, security and borrow evidence decide whether an event is a
candidate at all. Only an accepted candidate reaches an insight provider, and the
same gate runs again on a fresh snapshot once the model latency has elapsed. The
candidate direction is derived from the price reaction and VWAP alone; an insight
whose direction contradicts it stops the event. A quant-only runtime never
constructs an insight at all, so its comparator is genuinely text-free and free
of model cost.

## ADR-017 — One analysis per event, stored immutably

Status: accepted, 2026-08-27.

An analysis key binds event, filing document hashes, model input, model, prompt
and schema. The insight, the final pipeline outcome and the outbox completion are
written in one SQLite transaction. A retried event therefore reuses its stored
answer instead of paying for a second model call, a changed document or prompt is
never served from the old answer, and a crash can never leave a paid analysis
without its recorded outcome.

## ADR-018 — Runtime features come from IBKR only

Status: accepted, 2026-08-27.

Research uses Alpaca SIP; the runtime uses IBKR minute history plus aggregated
live bars for both the event symbol and SPY. A snapshot that mixes the two
sources, spans two feeds, or contains delayed, stale, future-dated, gapped or
self-contradictory bars fails closed instead of producing a decision. Every
feature set carries its provider, feed and the content address of the exact bar
set it was computed from. Live bars are published only as complete minutes.

## ADR-019 — Shadow mode is harmless by construction

Status: accepted, 2026-08-27.

Shadow mode is not a disabled flag. Its composition root injects a broker whose
submission, cancellation and repricing paths raise, plus a virtual account id
that no IBKR allowlist can contain. The SEC poll, the entry/insight pipeline and
the exit supervisor run as separate supervised tasks, so a thirty-second model
timeout cannot delay the one-second exit tick. A SQLite singleton lease refuses a
second daemon on the same state, and a critical failure in any task blocks new
entries only — exit supervision and warnings keep running.

## ADR-020 — The database carries its schema version

Status: accepted, 2026-08-28.

Operational state is the only source from which a restart reconstructs open orders.
A schema based only on `CREATE TABLE IF NOT EXISTS` cannot distinguish an older file
from a complete one. `PRAGMA user_version` therefore drives an ordered migration
chain, with every step running in a transaction. The application refuses to open a
database whose version is newer than the code, and a store that cannot guarantee its
schema version closes its connection instead of continuing in a partially opened
state.

## ADR-021 — Fills are separate immutable rows

Status: accepted, 2026-08-28.

`ExecutionReport` carries only the aggregate. The quantity, price and commission of
one execution event are known together only in the fill, so `ExecutionFill` is stored
as its own row, identified by the broker's `execId`. A fill delivered again after a
reconnect is a no-op rather than an error. Commission arrives in a separate, later
callback and is the only value a stored fill may gain, exactly once. Any other
difference under the same `execId` represents two facts under one identity and is
never resolved by overwriting. Conversely, an aggregate may lag its fills:
`orderStatus` reports a cumulative quantity without naming the fills that produced it.

## ADR-022 — Replacement-order provenance is a field, not a key suffix

Status: accepted, 2026-08-28.

The single permitted reprice was previously recognizable only by the `:r1` suffix of
the idempotency key. A safety rule that depends on a string cannot survive refactoring.
`OrderIntent` now uses `replaces_order_id` and `reprice_generation` to state explicitly
which order it replaces, and the contract forbids a second generation. The suffix
remains as a durable lookup handle, but it is no longer the source of truth. Existing
databases are migrated once when opened; a replacement order without a persisted
predecessor makes the migration fail closed.

## ADR-023 — Readiness is operation-specific

Status: accepted, 2026-08-28.

Submit, Exit, Cancel and Reconcile have different safety requirements. Fixed exclusion
sets scattered across methods are not auditable and can silently become stale when a
new check is introduced. The broker therefore evaluates an explicit profile centrally.
Reconcile may establish the not-yet-reconciled state; Cancel must remain possible
without live market data; entry submission additionally requires confirmed market data
and completed recovery. Exit requires fresh broker and market data but must not fail on
a recovery block that applies only to new entries.

Every profile shares one mandatory set: a `DU<digits>` account id, the optional `ibapi`
dependency, an open connection, the allowlisted account being present in the session,
and an authoritative order scope — which binds paper mode to client id 0. Cancel adds
`reconciled` to that set. That has a consequence worth naming: a latched callback fault
makes the next reconciliation fail, which clears `reconciled`, so a cancel is possible
until that attempt and not afterwards. Whether a purely exposure-reducing cancel should
outlive its session is an open question for a later revision of this decision.

## ADR-024 — Broker callbacks only add facts monotonically

Status: accepted, 2026-08-28.

`orderStatus` is a cumulative, repeatable signal, not the sole truth about individual
executions. `execDetails.execId` therefore identifies fill facts, and
`commissionReport.execId` finalizes their exact costs. A delayed callback may never
reduce quantity, fill count, fees or sequence. Identical replays are no-ops; conflicting
reuse of the same identity blocks reconciliation; and a terminal report may gain only
additional fill or commission evidence afterward.

See ADR-026 for what happens when a callback cannot be turned into a fact at all.

## ADR-025 — Paper startup and exits follow broker truth immediately

Status: accepted, 2026-08-28.

The paper process always starts with restore, authoritative broker reconciliation and
persistence, followed only then by bounded resumption. Missing confirmation creates a
manual hold and is never a reason to resubmit. Immediately before every exit, the broker
position is fetched again and must exactly confirm the exit intent's symbol, direction
and quantity. `run-paper` is the only order-capable composition root and is built only
with a DU paper account, the matching promotion artifact, identical runtime manifests
and active live preflight.


## ADR-026 — A broken callback latches instead of killing the reader thread

Status: accepted, 2026-08-29.

`EClient.run` catches only `KeyboardInterrupt`, `SystemExit` and `BadMessage`. Any other
error ends the reader thread — and with it every further order, fill and cancel callback.
That was fail-closed only by accident: the thread's `finally` disconnects, and the
`connected` check then refuses every later operation.

Catching such an error is therefore not free. Handling it must not be cheaper than
crashing was, so the fault is latched: `ready_for_orders()` turns false and
`reconcile_orders()` refuses as its very first check, because a callback layer that
cannot build a fact invalidates everything the session could report. The latch is
monotone and has no reset; the session has to be rebuilt.

A discarded callback is a weaker case and is only remembered: outside a reconciliation
window it lands in a deferred set that the next reconciliation folds into its result.
An aborted reconciliation hands those tokens back rather than deciding anything.


## ADR-027 — One normalization for every computed amount

Status: accepted, 2026-08-29.

`Money` permits eight decimal places and twenty digits. Decimal division and float
conversion both work at the context precision of twenty-eight, so any amount derived
from a broker value — a weighted average fill price, a VWAP, an average cost — leaves
that contract by construction rather than by accident.

`domain.money()` is the single place that puts such an amount back onto the contract.
An amount that cannot be represented at all raises `InvalidOperation`, which is an
`ArithmeticError` and therefore already fail-closed in every broker parsing path. It is
refused, never rounded into something plausible.

The rule is deliberately narrow: `money()` never accepts a value the model would have
rejected, so it moves an existing refusal earlier and gives it a clearer cause. It does
not create a new failure mode.
