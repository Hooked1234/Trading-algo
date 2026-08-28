# Architecture decisions

## ADR-020 — Das Schema trägt seine Version in der Datenbank

Status: accepted, 2026-08-28.

Der operative Zustand ist die einzige Quelle, aus der ein Neustart offene Orders
rekonstruiert. Ein Schema, das nur aus `CREATE TABLE IF NOT EXISTS` besteht,
kann eine ältere Datei nicht von einer vollständigen unterscheiden. Ab jetzt
führt `PRAGMA user_version` eine geordnete Migrationskette; jeder Schritt läuft
in einer Transaktion. Eine Datenbank mit höherer Version als der Code wird nicht
geöffnet, und ein Store, der seine Schemaversion nicht garantieren kann,
schließt seine Verbindung, statt halb geöffnet weiterzulaufen.

## ADR-021 — Fills sind eigene unveränderliche Zeilen

Status: accepted, 2026-08-28.

`ExecutionReport` trägt nur das Aggregat. Menge, Preis und Kommission eines
einzelnen Ausführungsereignisses sind ausschließlich im Fill zusammen bekannt,
deshalb ist `ExecutionFill` mit der `execId` des Brokers als Identität eine
eigene Zeile. Ein nach Reconnect erneut gelieferter Fill ist ein No-op statt
eines Fehlers. Die Kommission kommt in einem eigenen späteren Callback und ist
der einzige Wert, den ein gespeicherter Fill noch gewinnen darf — genau einmal.
Jede andere Abweichung unter derselben `execId` bedeutet zwei verschiedene
Tatsachen unter einer Identität und wird nie durch Überschreiben aufgelöst.
Umgekehrt darf ein Aggregat hinter seinen Fills zurückliegen: `orderStatus`
meldet eine kumulierte Menge, ohne die Fills zu nennen, aus denen sie entstand.

## ADR-022 — Die Herkunft einer Ersatzorder ist ein Feld, kein Schlüssel-Suffix

Status: accepted, 2026-08-28.

Der einzige erlaubte Reprice war bisher nur am Suffix `:r1` des
Idempotenzschlüssels erkennbar. Eine Sicherheitsregel, die von einer
Zeichenkette abhängt, hält keinem Refactoring stand. `OrderIntent` benennt jetzt
mit `replaces_order_id` und `reprice_generation` explizit, welche Order ersetzt
wird; eine zweite Generation ist durch den Vertrag ausgeschlossen. Das Suffix
bleibt als dauerhafter Nachschlage-Handle bestehen, ist aber nicht länger die
Wahrheit. Bestehende Datenbanken werden beim Öffnen einmalig nachgezogen; eine
Ersatzorder ohne persistierten Vorgänger lässt die Migration fail-closed
scheitern.

## ADR-023 — Readiness ist operationsspezifisch

Status: accepted, 2026-08-28.

Submit, Exit, Cancel und Reconcile haben unterschiedliche Sicherheitsbedürfnisse.
Ein festes Ausschluss-Set in einzelnen Methoden ist nicht auditierbar und kann bei
einer neuen Prüfung unbemerkt veralten. Der Broker wertet daher zentral ein explizites
Profil aus. Reconcile darf den noch nicht reconciled Zustand herstellen; Cancel muss
auch ohne Live-Marktdaten möglich bleiben; Entry-Submit verlangt zusätzlich bestätigte
Marktdaten und abgeschlossene Recovery. Exit verlangt frische Broker- und Marktdaten,
aber darf nicht an einer nur für neue Entries geltenden Recovery-Sperre scheitern.

## ADR-024 — Broker-Callbacks ergänzen Tatsachen nur monoton

Status: accepted, 2026-08-28.

`orderStatus` ist ein kumulatives, wiederholbares Signal und keine alleinige Wahrheit
über einzelne Ausführungen. `execDetails.execId` identifiziert deshalb die Fill-
Tatsachen; `commissionReport.execId` finalisiert genau deren Kosten. Ein verspäteter
Callback darf Menge, Fill-Zahl, Gebühr oder Sequenz nie verkleinern. Identische Replays
sind No-ops, eine widersprüchliche Wiederverwendung derselben Identität sperrt die
Reconciliation, und ein terminaler Report darf nachträglich nur noch zusätzliche
Fill- oder Kommissionsevidenz gewinnen.

## ADR-025 — Paper-Start und Exit folgen unmittelbar der Broker-Wahrheit

Status: accepted, 2026-08-28.

Der Paper-Prozess beginnt immer mit Restore, autoritativem Broker-Reconcile,
Persistenz und erst danach einer begrenzten Wiederaufnahme. Fehlende Bestätigung ist
ein manueller Hold und niemals ein Anlass zum Resubmit. Unmittelbar vor jedem Exit wird
die Brokerposition erneut geladen und muss Symbol, Richtung und Menge des Exit-Intents
exakt bestätigen. `run-paper` ist die einzige orderfähige Composition Root und wird nur
mit DU-Paper-Konto, passendem Promotion-Artefakt, identischen Laufzeit-Manifests und
aktivem Live-Preflight gebaut.

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
