# Worklog

## 2026-08-28

Gate C — IBKR-Paper-Execution-Härtung, Etappe 1 (Verträge und Persistenz):

- Lokaler Testumgebungs-Blocker vom 27.08. aufgelöst: `.test-cache/` ist entfernt,
  das Betriebssystem-Tempverzeichnis ist wieder nutzbar und pytest läuft ohne
  `PYTEST_DEBUG_TEMPROOT`; 19 verbliebene, ausschließlich temporäre Verzeichnisse
  unter `tmp/` wurden anschließend bereinigt.
- Ausgangslage vor Etappe 1 verifiziert statt übernommen: 446 Tests grün,
  84,07 % Branch-Coverage, Ruff sauber. Zu diesem damaligen Startzeitpunkt gab es
  noch keine Gate-C-Konstrukte aus dem abgebrochenen Lauf.
- `ExecutionFill` als eigener Vertrag ergänzt: Broker-`execId` als Identität,
  Menge, Preis, kumulierte Menge, Kommission und Kommissions-Finalität.
- `ExecutionReport` um `fill_count`, `pending_commission` und `update_sequence`
  erweitert. Ein Aggregat darf hinter seinen Fills zurückliegen, ihnen aber nie
  widersprechen: mehr gezählte Fills als Aktien, Gebühren ohne Fill und eine
  ausstehende Kommission ohne Fill sind Vertragsverletzungen.
- `OrderIntent` um `replaces_order_id` und `reprice_generation` erweitert; die
  Ersatzorder trägt ihre Herkunft jetzt selbst (ADR-022). `PaperExecutionService`
  setzt sie beim Reprice, `ExitMonitor` setzt sie bei einem neuen Exit-Versuch
  bewusst zurück, damit ein Folgeversuch keine fremde Herkunft erbt.
- Monotonie auf die neuen Zähler ausgedehnt — in der Broker-Zustandsmaschine und
  in der Persistenz. Dabei eine Altlast geschlossen: `transition()` prüfte
  Gebühren-Monotonie bisher nicht, nur `restore()` tat es.
- `PRAGMA user_version` mit geordneter, transaktionaler Migrationskette
  eingeführt (ADR-020). Schritt 1 legt den Fill-Ledger an und zieht die
  Reprice-Herkunft bestehender Datenbanken nach. Eine neuere Schemaversion wird
  nicht geöffnet; scheitert die Migration, schließt der Store seine Verbindung.
- Fill-Persistenz mit Idempotenz je `execId` und genau einer nachträglichen
  Kommissions-Finalisierung ergänzt (ADR-021), inklusive Abgleich gegen den
  Order-Intent: Symbol, Seite, kumulierte Menge und Zeitpunkt müssen passen.
- Qualitätslauf nach Etappe 1: 463 Tests grün, 84,25 % Branch-Coverage,
  Ruff sauber.

Gate C — IBKR-Paper-Execution-Härtung, Etappe 2 (Callbacks und Runtime):

- `orderStatus` wird nicht mehr überschreibend verarbeitet. Kumulative Menge,
  Durchschnittspreis, Fill-Zähler, Gebühren und Sequenz werden monoton aus den
  `execId`-Fills abgeleitet; verspätete oder doppelte Callbacks können keinen Wert
  zurückdrehen. Reine Terminal-Replays bleiben No-ops, nachträgliche Fill- oder
  Kommissionstatsachen werden dagegen noch angenommen.
- `commissionReport` ist angebunden. Kommission-vor-Fill und Kommission-nach-Fill
  führen idempotent zum selben Fill-Ledger und zu `ExecutionReport.fees`; die
  bestehende Portfolio-Abrechnung zieht diese Gebühren vom Netto-P&L ab.
- Readiness ist in die Profile Submit, Exit, Cancel und Reconcile geteilt. Jeder
  Brokerpfad fordert nur seine expliziten, zentral definierten Voraussetzungen.
- Ersatzorders werden erst aus einer bestätigten Cancelled-Order und ihrer tatsächlichen
  Restmenge erzeugt. Eine persistierte Generation 1 wird nach Neustart wiederaufgenommen,
  nie erneut gesendet; eine zweite Generation ist vertraglich und im Service gesperrt.
- Jeder Exit lädt unmittelbar vor der Submission eine frische, reconciled
  Brokerposition. Fehlende, falsche oder mengenmäßig abweichende Positionen blockieren
  fail-closed.
- `PaperRecoveryCoordinator` erzwingt die Reihenfolge Restore → Reconcile → Fill-/Report-
  Persistenz → Resume → Abschluss-Reconcile. Ein unbekanntes Submission-Ergebnis bleibt
  im manuellen Hold.
- `build_paper_runtime` und `run-paper` verdrahten ausschließlich ein allowlistetes
  `DU<digits>`-Konto, das passende Promotion-Artefakt, Laufzeit-Manifeste, Live-Preflight,
  Recovery und die überwachten Runtime-Schleifen.
- Die Zahl der operativen Tabellen wird nun im Schematest aus `sqlite_master`
  ermittelt und gegen die exakt 16 projekt-eigenen Tabellen geprüft.
- Qualitätslauf nach Etappe 2: 474 Tests grün, 83,57 % Branch-Coverage,
  Ruff sauber; `tmp/` enthält keine alten Testverzeichnisse mehr.

## 2026-08-25

- Initialized Python 3.12 project and strict immutable domain contracts.
- Added paper-only settings, NYSE calendar rules, indicators, continuation strategy,
  portfolio risk engine, transaction-cost model and statistical research gate.
- Added research, security and architectural decision records.
- Implemented SEC live ingestion and quarterly historical backfill with immutable raw,
  Filing-, Bar- and Quote storage plus explicit 1/3/5/10-minute availability scenarios.
- Implemented Alpaca historical SIP, IBKR live NBBO/shortability and hard paper-only
  execution adapters, including durable idempotency and fail-closed restart recovery.
- Added quant-only, keyword and isolated Hermes paths, deterministic features,
  historical cost simulation, research/model gates, shadow orchestration and reports.
- Added operator CLI, security/operations documentation and automated acceptance checks.
- Final local quality run: 157 tests passed; branch coverage 82.88 %; Ruff clean.

## 2026-08-27

Gate 0 — reproduzierbare Projektbasis:

- Nicht standardmäßiges `cache_dir = ".test-cache"` aus `pyproject.toml` entfernt; pytest
  nutzt wieder `.pytest_cache` und das laufbezogene Betriebssystem-Tempverzeichnis.
- `tmp/` als Ablage alter Testläufe ignoriert.
- Interne Baseline vor der Gate-A-Arbeit: 329 Tests grün, 82,19 % Branch-Coverage,
  Ruff sauber.
- Historischer, inzwischen erledigter Blocker: `%TEMP%\pytest-of-felix`, `.test-cache/` und `tmp/*` wurden
  von einem früheren Lauf mit unzugänglichen ACLs angelegt. Sie lassen sich weder lesen,
  übernehmen noch löschen und müssen einmalig mit Administratorrechten entfernt werden.
  Status 28.08.2026: vollständig aufgelöst; pytest läuft ohne Temp-Workaround und die
  19 verbliebenen Testverzeichnisse wurden gelöscht.

Gate A — manipulationssichere Research-Pipeline:

- `BacktestCase` ist insight-frei; Insights werden erst zur Laufzeit je Variante
  zugeordnet. `case_input_sha256` entsteht aus einer expliziten Allowlist und ist für
  Quant-only, Keyword und AI identisch.
- Historische Fälle ersetzen `first_seen_at`/`retrieved_at` durch die registrierte
  Verfügbarkeits-Counterfactual (siehe ADR-014).
- Neues `artifacts.py`: kanonisches Hashing, exklusives Schreiben, Re-Verifikation
  beim Lesen.
- `TradingStateManifest` mit `known_at`, `ResearchCaseBuildArtifact` mit vollständiger
  Coverage-Buchhaltung (Case, typisierter Ausschluss oder Integritätsfehler).
- `InsightArtifact` mit genau einer gepinnten Antwort oder Abstention je deterministisch
  vorselektiertem Kandidaten; Quant-only ruft nie einen InsightProvider.
- Portfolio-fähiger Backtest-Runner: stabile Sortierung gleichzeitiger Events,
  Mark-to-Market offener Positionen, aktuelle Exposure-, Tagesverlust- und
  Drawdown-Werte vor jeder Entscheidung; `BacktestRunArtifact` bindet Case-, Insight-,
  Strategie- und Kostenmodellversion.
- Research- und Paired-AI-Gate akzeptieren nur vollständige Run-Artefakte mit
  identischem Case-Bestand.
- Parquet-Lesevorgänge sind auf die benötigten Datums-/Symbol- bzw. Quartalspartitionen
  beschränkt; der Datenqualitätsbericht rechnet je Filing über alle Lags 1/3/5/10 ab.
- Neue CLI-Kette: `build-dataset-manifest`, `build-research-cases`,
  `build-insight-artifact`, `run-backtest --variant`, `research-gate --run`,
  `paired-ai-gate`.
- Qualitätslauf nach Gate A: 373 Tests grün, 82,97 % Branch-Coverage, Ruff sauber.

Gate B — dauerhafter Shadow-Betrieb:

- `CandidateGate` vor jedem Modellaufruf; deterministische Richtung aus Preisreaktion
  und VWAP; zweite Prüfung mit frischem Snapshot nach der Modelllatenz.
- Quant-only überspringt den InsightProvider vollständig; widersprüchliche
  Insight-Richtung stoppt das Event.
- `AnalysisKey` bindet Event, Dokumenthashes, Modelleingabe, Modell, Prompt und Schema.
  Insight, finales Outcome und Outbox-Abschluss laufen in einer SQLite-Transaktion;
  ein Retry nutzt die gespeicherte Analyse ohne zweiten kostenpflichtigen Aufruf.
- Neue Tabellen: `insights`, `pipeline_outcomes`, `runtime_leases`, `critical_events`,
  `runtime_heartbeats`.
- `IBKRLiveFeatureProvider` plus `IBAPIBarHook`: IBKR-Minutenhistorie und zu vollen
  Minuten aggregierte Live-Bars für Aktie und SPY, mit Provider-, Feed- und
  Input-Hash-Lineage. Alpaca- und IBKR-Bars werden nie gemischt.
- Composition Root `build_shadow_runtime` mit nicht submit-fähigem Broker und
  virtuellem Shadow-Konto; CLI `run-shadow-once`, `run-shadow`, `session-report`,
  `acceptance-from-reports`.
- SEC-, Entry- und Exit-Schleifen laufen als getrennte überwachte Tasks; ein
  30-Sekunden-Modelltimeout blockiert den Exit-Takt nicht. SQLite-Singleton-Lease,
  ein Event je Entry-Worker, Entry-Sperre bei kritischen Fehlern.
- Automatisch nach der Sitzung erzeugter, gehashter Tagesbericht aus dem operativen
  Zustand.
- Zwei Altlasten mitkorrigiert: `hermes_max_input_chars` lag mit 120 000 über der
  harten Providergrenze von 100 000 und hätte den Provider unkonstruierbar gemacht
  (jetzt Default 40 000, Obergrenze 100 000); der Entry-Aufruf hatte einen
  TypeError-Fallback, der einen echten TypeError verschluckt und den Batch erneut
  ausgeführt hätte.
- Qualitätslauf nach Gate B: 446 Tests grün, 84,07 % Branch-Coverage, Ruff sauber.
