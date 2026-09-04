# Worklog

## 2026-08-31

Weiteres Vorgehen geplant und Gate D begonnen (Erzeuger für die fehlenden
Punkt-in-Zeit-Manifeste).

- Ausgangslage im Worktree verifiziert statt übernommen: 501 Tests grün,
  `backfill-plan` läuft offline und meldet 30 Quartale 2019-Q1 bis 2026-Q2.
- Befund der Planung: Die Forschungskette ist nicht zuerst durch Konten oder
  Abonnements blockiert, sondern durch zwei Referenzdatensätze, die der Code
  zwingend verlangt und für die es keinen Erzeuger gab. Beide fehlten in
  `NEXT_STEPS.md`.
  1. Ohne `historical_eligibility.csv` ist der Eligibility-Resolver `None`, jede
     Coverage-Zeile wird `MISSING_POINT_IN_TIME_ELIGIBILITY` und
     `build-research-cases` schließt jeden Case aus. Verschärfend: der Backfill
     löst Eligibility erst *nach* dem Marktdatenabruf auf, ein Lauf ohne Manifest
     verbraucht also das gesamte Alpaca-Budget und liefert trotzdem keine
     handelbare Coverage.
  2. `TradingStateManifest` wird im ganzen Repo nur gelesen, nie gebaut, ist aber
     Pflichtargument von `build-research-cases`. Unbekannter Halt-Status schließt
     den Case aus, und `as_of` darf höchstens fünf Sekunden vor dem
     Entscheidungszeitpunkt liegen — also ein Eintrag je Symbol und
     Entscheidungsinstant, keine pflegbare Referenzdatei.
- Reihenfolge in `NEXT_STEPS.md` festgeschrieben: Gate D (Manifest-Erzeuger, ohne
  externe Konten) vor Gate E (Backfill, Alpaca kostenpflichtig) vor Gate F
  (Forschungsentscheid, quant-only zuerst) vor Gate G (IBKR-Abnahme). Alle teuren
  Posten liegen damit hinter dem Entscheidungspunkt.

Gate D, erster Schritt — Eligibility aus der Cover Page:

- Quelle ist das Filing selbst. Seit der Cover-Page-Tagging-Einfuehrung 2019 trägt
  ein 8-K `Security12bTitle`, `TradingSymbol` und `SecurityExchangeName` je nach
  Section 12(b) registrierter Gattung. Die Evidenz ist zum Acceptance-Zeitpunkt
  bekannt, braucht keinen heutigen Security Master, und ihre Abdeckung beginnt genau
  dort, wo der registrierte Sample-Bereich beginnt (ADR-028).
- `sec_history.py` gruppiert Cover-Page-Fakten jetzt über `contextRef`. Vorher
  wurden nur Symbole flach eingesammelt; ohne Kontextbindung hätte ein SPAC mit
  Aktie, Unit und Warrant den Titel der einen Gattung dem Symbol der anderen
  zugeordnet. Widersprechen sich zwei Kontexte zum selben Symbol, bleibt die
  Tatsache unbekannt statt willkuerlich gewählt.
- Neue Klassifikation prueft Nicht-Stammaktien-Marker *vor* den Stammaktien-Markern:
  "Units, each consisting of one share of Class A common stock and one-half of one
  warrant" nennt Stammaktien im eigenen Titel und wäre sonst als Stammaktie in die
  Stichprobe gelaufen.
- Eine unbekannte Börsenbezeichnung wird `unknown`, nicht `false`. Das Feld ist
  Freitext; eine Schreibweise, die die Liste nicht kennt, ist kein Beleg für eine
  ausländische Notierung. Beides schließt aus, aber die Buchhaltung unterscheidet
  fehlende von widerlegter Evidenz.
- `corporate_actions_complete` bleibt bewusst leer. Keine Cover Page belegt die
  Vollständigkeit der Kapitalmaßnahmen-Historie; die Spalte zu füllen, damit ein
  Backtest läuft, wäre genau der erfundene Wert, den das Forschungsprotokoll
  verbietet. Jedes abgeleitete Intervall bleibt damit eine ausgewiesene
  Coverage-Lücke — ehrlich, aber noch ohne Research-Cases.
- Zwei Kommandos statt einem: `collect-cover-page-facts` ist der einzige mit
  Netzzugriff, schreibt je Filing eine dauerhafte Zeile und ist wiederaufnehmbar;
  ein gescheitertes Filing wird bewusst nicht geschrieben, damit ein späterer Lauf
  es erneut versucht statt einen Transportfehler ins Manifest einzufrieren.
  `build-eligibility-manifest` ist offline, deterministisch und überschreibt nie.
- Altlast mitkorrigiert: `CsvEligibilityResolver` scannte je Abfrage alle Intervalle.
  Bei einem Manifest mit einer Zeile je registrierter Gattung und einer Abfrage je
  Filing wäre das quadratisch geworden. Der Lookup ist jetzt nach CIK und Symbol
  indiziert; die Nicht-Überlappungsprüfung baut denselben Index auf.
- Neun Mutationsproben, alle rot: vertauschte Klassifikationsreihenfolge, behauptete
  Kapitalmaßnahmen-Vollständigkeit, um einen Tag verschobenes Intervallende,
  Willkürauswahl bei Tageskonflikt, überschreibendes Schreiben, Verlust des
  Wiederaufnahme-Zustands, nicht gekürzte abgerissene Zeile, kontextloses Mischen
  der Gattungen, unnormalisierter CIK im neuen Index.
- Dabei fielen zuerst zwei überlebende Mutationen auf, beide wegen zu schwacher
  Zusicherungen in meinen eigenen Tests: die Prüfung auf die gekürzte Datei zählte
  nur Zeilenumbrüche (die abgerissene Zeile hat keinen), und die
  Wiederaufnahme-Mutation war innerhalb eines Laufs verhaltensgleich. Beide Tests
  sind nachgeschärft.
- Qualitätslauf: 532 Tests grün, 83,25 % Branch-Coverage, `ruff check` und
  `ruff format` sauber.

Review-Nacharbeit am selben Tag (zwei bestätigte P1-Befunde aus dem PR):

- Cover-Page-Fakten wurden paarweise gemischt. Bei drei Kontexten zum selben Symbol
  überschrieb ein späterer, zustimmender Kontext einen früheren Widerspruch, weil
  „nicht gemeldet“ und „widersprüchlich gemeldet“ nach dem Mischen derselbe leere
  Wert sind. Titel Common Stock / Warrants / Common Stock ergab Common Stock statt
  unbekannt — reproduziert, nicht vermutet. Jetzt werden erst alle gemeldeten Werte je
  Symbol gesammelt und nur ein einziger, unbestrittener übernommen; das Ergebnis hängt
  nicht mehr von der Tag-Reihenfolge des Filings ab.
- Ein Abbruch nach dem vollständigen JSON-Objekt, aber vor dem Zeilenumbruch, war nicht
  abgedeckt: der nächste Append hängte direkt an den letzten Record an, und die
  Torn-Line-Rettung verwarf die verschmolzene Zeile — also zwei Records still verloren,
  schwerer als im Review beschrieben. `read_fact_records` terminiert einen gültigen,
  unterminierten Schluss-Record jetzt, bevor der Collector irgendetwas anhängt.
- Zehn Mutationsproben, alle rot. Qualitätslauf: 534 Tests grün, 83,26 %
  Branch-Coverage, Ruff sauber.

Offen und ausdrücklich nicht geschlossen: die dritte Eligibility-Spalte braucht eine
auditierbare Kapitalmaßnahmen-Quelle, und der Erzeuger für das Trading-State-Manifest
steht noch aus. Ohne beides liefert die Kette weiterhin null Research-Cases.
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

Gate-C-Review und Nacharbeit:

- Das Projekt ist erstmals versioniert (`Trading-algo`, Commit `825bdb6`). Bis dahin
  lagen Gates 0/A/B/C ausschließlich ungetrackt im Arbeitsverzeichnis — ein Review
  musste deshalb den Vollstand lesen statt eine Änderung.
- Review von Gate C fand zwei Blocker. Beide sind reproduziert, nicht vermutet:
  1. Der gewichtete Durchschnittspreis in `_on_order_status` wurde ungerundet in
     `ExecutionReport.average_fill_price` (acht Nachkommastellen) gesteckt. Zwei Fills
     zu 1 Stück @ 10,00 und 2 Stück @ 10,01 lösten eine `ValidationError` im
     `ibapi`-Reader-Thread aus. Der bestehende Mehrfach-Fill-Test benutzte 6 @ 99,80
     und 4 @ 100,00 — ein Durchschnitt, der glatt aufgeht.
  2. `TRADING_IBKR_CLIENT_ID` stand auf 71, `order_scope_authoritative()` verlangt
     aber Client 0, und diese Prüfung liegt im `common`-Set jedes Readiness-Profils.
     `run-paper` konnte mit ausgelieferter Konfiguration nie starten.
- Der Fehler war kein Einzelfall: derselbe Umgang mit Broker-Rohwerten fand sich an
  sechs weiteren Stellen (`_on_exec_details`, `_on_commission_report`,
  `_on_remote_order`, `_on_portfolio`, `ibkr_bars`, `portfolio`). `money()` liegt jetzt
  in `domain.py` neben der `Money`-Definition und ist der Projektstandard.
- Kommission-Finalisierung geht nicht mehr über `model_copy(update=…)`, das die
  Validierung umgeht und den vertragswidrigen Wert erst stromabwärts hat auffliegen
  lassen, sondern über eine validierende Konstruktion.
- Callback-Fehler werden nicht mehr nur gefangen, sondern rasten in einem Latch ein:
  `ready_for_orders()` wird falsch und `reconcile_orders()` verweigert als erste
  Prüfung. Das ist bewusst so gebaut — eine geworfene Callback-Exception beendet heute
  den Reader-Thread, dessen `finally: self.disconnect()` das einzige Fail-closed-Signal
  liefert. Bloßes Abfangen mit Logging hätte daraus fail-silent gemacht.
- Verwerfungen außerhalb eines Reconciliation-Fensters waren stille No-ops. Sie werden
  jetzt gemerkt und in die nächste Reconciliation eingefaltet.
- `_on_portfolio` verwarf bei einem Vertragsfehler eine ganze Position hinter einem
  pauschalen `except Exception`. Der Fehler wird jetzt gelatcht statt verschluckt.
- Aufräumarbeiten: `PaperStartupGate` entfernt (durch `PaperRecoveryCoordinator`
  abgelöst), `_ensure_execution_state()` und die internen `getattr`-Rückfallpfade
  ersatzlos gestrichen, fünf `object.__new__`-Testdoubles auf den echten Konstruktor
  `NativeIBAPIBackend.without_transport()` umgestellt, die DU-Regel auf eine einzige
  strikte Funktion zusammengezogen (`DUMMY123` passierte vorher Config und Composition),
  `_pending_commissions.pop()` hinter die Übernahme des Fills verschoben.
- `# pragma: no cover` liegt nicht mehr auf der ganzen `NativeIBAPIBackend`, sondern nur
  noch auf `connect`, `submit_order` und `portfolio_state`. Die gemeldete Coverage fiel
  dadurch von 83,49 % auf 82,49 % — die Kennzahl beschrieb den sicherheitskritischsten
  Teil von Gate C vorher überhaupt nicht.
- Repo-weites `ruff format` (61 Dateien) und in `docs/testing.md` als feste
  Qualitätsroutine hinterlegt.
- Jede Korrektur ist durch eine Mutationsprobe abgesichert: neun zurückgenommene Fixes,
  jeder färbt mindestens einen Test rot. Dabei fiel eine eigene Normalisierung als
  redundant auf und wurde wieder entfernt, statt sie als toten Verteidigungscode
  stehen zu lassen.
- Eine unabhängige Abschlussprüfung fand danach noch drei Spuren der Parallelarbeit,
  alle bestätigt und behoben: `orchestrator.py` prüfte als einzige Stelle weiter nur das
  `DU`-Präfix; die Client-0-Pflicht war erst in `build_paper_runtime` erzwungen, also
  erst nachdem die CLI bereits eine Gateway-Verbindung mit der falschen Client-ID
  geöffnet hatte; und ein `getattr`-Rückfall war im Orchestrator stehengeblieben.
- Dabei fiel ein Fail-open im neuen Verwerfungsmechanismus selbst auf: `reconcile_orders`
  übernahm die gemerkten Verwerfungen in sein Ergebnis und leerte
  `_deferred_inconsistencies`, *bevor* es auf die Endmarker des Brokers wartete. Ein
  Timeout hätte die Tokens verloren. Schwerer noch: `_reconciliation_account` wurde nur
  auf dem Erfolgspfad zurückgesetzt, ein einzelner Timeout hätte den Adapter dauerhaft
  mit „reconciliation is already active" verklemmt. Beides ist jetzt in `try/finally`
  gefasst und durch einen Test abgedeckt, der beide Mutationen rot färbt.
- Eine zweite unabhängige Prüfung fand dann den schwersten Befund der ganzen Runde,
  und zwar in der Korrektur vom selben Tag: das `try/finally` in `reconcile_orders`
  deckte seinen eigenen Setup-Block nicht ab. `_reconciliation_account` wurde als erste
  Anweisung gesetzt, zwei werfende Schritte folgten davor — ein Abbruch dort verklemmte
  die Sitzung dauerhaft. Die Markierung steht jetzt am Ende des Setups, die Freigabe im
  selben Lock wie der Ergebnis-Snapshot.
- Ebenfalls gefunden: der komplette Markt-Recheck des Pre-Submit-Guards war von *keinem*
  Test gedeckt. `_market_reasons` ließ sich ersatzlos entfernen und alle 490 Tests
  blieben grün — der letzte Guard vor jeder Live-Submission trug kein einziges Testgewicht.
  Zehn Ablehnungsgründe sind jetzt abgedeckt, alle zehn Mutationen färben rot.
- Zwei meiner eigenen neuen Tests waren dabei zahnlos: Strategie und Risiko-Engine
  liefern dieselben Codes `SYMBOL_HALTED`, `SPREAD_TOO_WIDE` und `MARKET_DATA_STALE` wie
  der Preflight, sodass eine End-to-End-Assertion sie dem Guard nicht zuordnen konnte.
  Sie laufen jetzt über den Exit-Pfad, der Strategie und Risiko überspringt, bzw. direkt
  gegen `_market_reasons`.
- Der Historien-Bar-Pfad und der 20-Ziffern-Guard in `money()` waren ungemessen; beides
  ließ sich entfernen, ohne einen Test zu brechen. Jetzt nicht mehr.
- Doku nachgezogen: Client-0-Pflicht in README und Runbook, strikte `DU<digits>`-Form in
  `security.md`, die nicht existierende Einstellung `execution_enabled` aus
  `NEXT_STEPS.md` entfernt, ADR-023 um das gemeinsame Pflicht-Set und die Cancel-Folge
  ergänzt, ADR-026 (Callback-Latch) und ADR-027 (`money()`) neu.
- Qualitätslauf am Ende: 501 Tests grün, 82,88 % Branch-Coverage, `ruff check` und
  `ruff format` sauber. Jede Korrektur ist mutationsgeprüft.

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
