# Test- und Abnahmestrategie

## Automatisierte Ebenen

- Unit- und Property-Tests prüfen Domänenverträge, Indikatoren, Kosten, Strategie,
  Position Sizing sowie Brutto-, Netto-, Tagesverlust- und Drawdown-Grenzen.
- Contract-Tests simulieren SEC, Alpaca, Hermes und IBKR ohne echte Orders oder
  Zugangsdaten. Fehler, Timeouts, 429, ungültige Schemas, Delayed Data, Halts,
  Partial Fills, Reconnects und widersprüchliche Zustände müssen fail-closed enden.
- Integrations- und Replay-Tests verwenden dieselben Strategie-, Risiko-, Kosten-
  und Orderfunktionen wie Shadow- und Paper-Betrieb.
- Restart-Tests laden offene Orders aus SQLite, gleichen sie mit dem Broker ab und
  senden bei unbekanntem Ausgang niemals automatisch erneut.
- Schema-Tests prüfen die Migrationskette gegen eine Datenbank im Gate-B-Zustand:
  Fill-Ledger wird angelegt, die Reprice-Herkunft nachgezogen, eine Ersatzorder ohne
  Vorgänger schlägt fail-closed fehl und eine neuere Schemaversion wird nie geöffnet.
  Zusätzlich wird die tatsächliche SQLite-Inventarliste gegen exakt 16 eigene Tabellen
  geprüft; Tabellenzahlen stammen damit nicht mehr aus manuellen Prüfnotizen.

## IBKR-Paper-Ausführung

- Verspätete, doppelte und widersprüchliche `orderStatus`-/`execDetails`-Callbacks
  dürfen Menge, Gebühren, Fill-Zähler und Broker-Sequenz nie verkleinern.
- `commissionReport` vor oder nach `execDetails`, identische Wiederholung und ein
  Store-Neustart ergeben denselben finalisierten Fill und dieselbe Gesamtgebühr.
- Submit-, Exit-, Cancel- und Reconcile-Readiness werden getrennt auf notwendige und
  bewusst nicht notwendige Prüfungen getestet.
- Ein Neustart zwischen Cancel und Reprice verwendet die persistierte Ersatzorder;
  eine zweite Submission oder Reprice-Generation ist ausgeschlossen.
- Jeder Exit-Preflight vergleicht die frische Brokerposition exakt. Abweichung oder
  fehlende Position sperrt den Exit.
- Recovery persistiert Broker-Fills und -Reports vor jeder Wiederaufnahme. Ein
  unbekanntes Submission-Ergebnis bleibt ohne Resubmit im manuellen Hold.
- Die Paper-Composition Root besitzt zwingend Promotion, Laufzeit-Manifeste,
  Live-Preflight und Recovery; die CLI verweigert fehlendes DU-Konto oder Artefakt.

## Research-Artefaktkette

- Der Case-Builder wird gegen einen vollständigen Fixture-Pfad geprüft: Coverage,
  Parquet-Partitionen, Rohdokumente, Trading-State-Manifest, Case-Artefakt,
  Insight-Artefakt, Run-Artefakt und Gate.
- Manipulierte Artefakte, entfernte Outcomes, doppelte Coverage-Einträge, unbegründete
  Evidenz und nicht zusammengehörige Artefakte müssen fail-closed fehlschlagen.
- Quant-only und AI müssen exakt dieselben `case_input_sha256`-Werte besitzen; lokale
  Pfade und Ingestion-Zeitstempel dürfen den Hash nicht verändern.
- Unbekannter Halt schließt den Case aus, unbekannte Borrow-Evidenz sperrt nur Short.
- Punkt-in-Zeit-Lesevorgänge öffnen ausschließlich die benötigten Datums- und
  Symbolpartitionen; eine unlesbare Partition außerhalb des Fensters bleibt folgenlos.

## Shadow-Betrieb

- Jeder deterministische Ablehnungsgrund muss null Modellaufrufe erzeugen; ein Retry
  verwendet die gespeicherte Analyse erneut.
- Quant-only konstruiert überhaupt keinen Insight; eine widersprüchliche
  Modellrichtung stoppt das Event.
- Jedes Outbox-Event endet dauerhaft als Outcome oder als typisierter, wiederholbarer
  Fehler; ein aufgezeichnetes Outcome ist unveränderlich.
- Kein Testpfad erreicht eine Broker-Submission: der Shadow-Broker wirft in jedem
  Schreibpfad.
- Ein blockierender Modellaufruf darf den Exit-Takt nicht verzögern; ein zweiter
  Daemon wird durch die Singleton-Lease abgewiesen; kritische Fehler sperren Entries
  und erzeugen dauerhafte Warnungen.
- Laufzeit-Features: gemischte Quellen, zwei Feeds, Delayed-, Stale-, Zukunfts-,
  Lücken- und Widerspruchsdaten schlagen fail-closed fehl.
- IBKR-Bar-Callbacks: unvollständige Historie bleibt unsichtbar, eine Teilminute wird
  nie veröffentlicht, ein Request-Fehler entwertet die Historie.

## Lokale Qualitätsroutine

Vor jedem Abschluss werden Formatierung, Lint und die vollständige Testsuite in dieser
Reihenfolge ausgeführt:

```bash
uv run ruff format .
uv run ruff check .
uv run pytest --cov=event_trader --cov-branch --cov-report=term-missing
```

## Coverage-Regel

Der lokale Build erzwingt mindestens 82 % Branch-Coverage über den testbaren Kern.
CLI-Verdrahtung und Logging sind aus dieser Kennzahl ausgenommen; sie werden über
Smoke-Tests geprüft.

Ausgenommen sind nur noch die drei Methoden, die ohne laufendes IB Gateway nicht
ausführbar sind — `connect`, `submit_order` und `portfolio_state` von
`NativeIBAPIBackend` — sowie die Import-Guards des optionalen `ibapi`-Extras. Die
Callback-Reduzierer (`_on_order_status`, `_on_exec_details`, `_on_commission_report`,
`_on_remote_order`, `_on_portfolio`) tragen die Order- und Fill-Wahrheit des Systems
und werden gemessen; sie sind ohne Socket über `NativeIBAPIBackend.without_transport()`
direkt aufrufbar.

Bis 28.08.2026 lag `# pragma: no cover` auf der gesamten Klasse und nahm damit rund
830 Zeilen aus Zähler *und* Nenner. Die gemeldete Zahl beschrieb den sicherheits-
kritischsten Teil von Gate C also gar nicht — genau dort verbarg sich ein
reproduzierbarer Aggregationsfehler bei mehreren Fills. Die Einschränkung des Pragmas
kostet rund einen Prozentpunkt; das ist der Preis für eine Kennzahl, die hält, was sie
behauptet.

**Coverage ist kein Abnahmekriterium für Korrekturen in diesem Layer.** Solange ein
Pragma greift, kann ein Test grün sein, ohne die Zahl zu bewegen. Maßgeblich sind die
Regressionstests selbst und die Mutationsprobe: jede zurückgenommene Normalisierung und
jeder entfernte Fail-closed-Pfad muss mindestens einen Test rot färben.

## Externe Abnahme

Vor Paper Orders sind zusätzlich erforderlich:

1. Research-Gate mit mindestens 200 OOS-Trades und 50 je aktivierter Richtung.
2. Modell-Gate und gepaarte Verbesserung gegenüber Quant-only, falls KI Orders
   beeinflussen soll.
3. Kontrollierter IB-Gateway-Restarttest mit offenen, teilgefüllten und unbekannten
   Orders im fest hinterlegten Paper-Konto.
4. Mindestens 30 vollständige Sitzungen und 50 geschlossene Paper-Trades bei
   mindestens 99 % Verfügbarkeit, vollständiger SEC-/Positionsabstimmung, null
   Doppelorders und null ungeklärten kritischen Fehlern.
