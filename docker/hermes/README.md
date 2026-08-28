# Hermes als isolierter Insight-Adapter

**Empfehlung:** Für den deterministischen Trading-MVP Hermes nicht als
Orchestrator oder Entscheidungsinstanz aktivieren. Die HTTP-Grenze ist
vorbereitet, bleibt aber standardmäßig aus. Zuerst werden Keyword-Baseline,
Backtest, Risk Engine und Paper-Ausführung validiert. Hermes kann danach als
optionaler, abstain-fähiger Klassifikator in einem isolierten Labortest folgen.

Mit „Hermes“ ist hier
[NousResearch/hermes-agent](https://github.com/NousResearch/hermes-agent)
gemeint. Der Begriff ist mehrdeutig: Die Nous-Hermes-Modellfamilie ist ein
LLM, aber kein eigenständiger Multi-Agent-Orchestrator.

## Warum nur als Adapter

Hermes Agent bietet Gateway, Sessions, Memory, Skills, Sub-Agents und eine
OpenAI-kompatible Schnittstelle (`POST /v1/chat/completions`). Das eignet sich
für allgemeine Agentenarbeit, ist für diesen MVP aber eine zu breite
Vertrauensgrenze. Die offizielle API-Dokumentation weist ausdrücklich darauf
hin, dass der API-Server standardmäßig Zugriff auf das Hermes-Toolset bis hin
zu Terminal- und Dateioperationen gibt. Ein System-Prompt ist deshalb keine
Sicherheitsgrenze.

Der Adapter in `src/event_trader/providers/insight.py` reduziert die Rolle auf
Textklassifikation:

- gesendet werden nur Event-ID, Accession Number, Formular, Annahmezeit,
  Dokument-Hashes und bereinigter SEC-Text;
- Markt-, Portfolio-, Broker-, Konto-, Order- und Credential-Daten werden nie
  serialisiert;
- die Anfrage enthält keine Tool-Definitionen;
- nach 30 Sekunden, bei HTTP-/JSON-/Schemafehlern, fremder Event-ID oder nicht
  belegbarer Evidenz wird deterministisch `abstain` zurückgegeben;
- das Ergebnis wird lokal mit striktem Pydantic-Schema erneut validiert. Erst
  nach dieser Grenze darf deterministischer Strategie- und Risikocode es lesen.

Hermes darf niemals direkt die Risk Engine, Broker-API oder Orderausführung
aufrufen.

## Containergrenze

`compose.yaml` verwendet das offizielle Image und pinnt den vorgesehenen
Lab-Release `v2026.8.19`. Der Dienst liegt absichtlich hinter
dem Compose-Profil `lab-only`, bindet den API-Port nur an Host-Loopback und
mountet ausschließlich ein eigenes Hermes-State-Verzeichnis nach `/opt/data`.
Das eingecheckte `config.yaml` wird darüber read-only eingeblendet; eine im
State-Verzeichnis abgelegte, abweichende Konfiguration kann es dadurch nicht
ersetzen. Der Container läuft zusätzlich mit read-only Root-Dateisystem,
sämtlichen Linux-Capabilities entfernt, `no-new-privileges` sowie CPU-, RAM-
und Prozessgrenzen.

Nicht zulässig sind:

- Mounts des Repositories, des Docker-Sockets, von SSH-Verzeichnissen oder
  Broker-/Trading-Konfiguration;
- Broker-, Marktfeed- oder Trading-Credentials in `/opt/data/.env` oder in der
  Containerumgebung;
- eine gemeinsame Docker-Netzwerkverbindung mit Broker, Execution, Datenbank
  oder Risk Engine;
- CORS-Freigaben, Dashboard und Messaging-Gateways.

Das Bridge-Netz in Compose trennt nur Docker-Dienste voneinander. Ausgehenden
Netzverkehr begrenzt Compose nicht zuverlässig. In einem echten Labornetz muss
eine Host-/Cloud-Firewall Egress ausschließlich zum gewählten Inference-
Provider erlauben. Der Hermes-State liegt außerhalb des Repositories und darf
nur den dedizierten Hermes-API-Key sowie den unbedingt nötigen
Inference-Provider-Key enthalten.

## Erforderliche Hermes-Konfiguration

Das offizielle Image speichert `config.yaml`, `.env`, Sessions, Memory, Skills
und Logs unter `/opt/data`. Das Verzeichnis muss neu und ausschließlich für
diesen Adapter angelegt werden. Vor einem Start ist dort ein Modellprovider
nach der [offiziellen Hermes-Konfiguration](https://hermes-agent.nousresearch.com/docs/user-guide/configuration/)
einzurichten. Keine bestehende persönliche Hermes-Installation wiederverwenden.

Die verbindliche Konfiguration liegt unter `docker/hermes/config.yaml` und
wird unveränderlich nach `/opt/data/config.yaml` gemountet. Sie enthält:

```yaml
platform_toolsets:
  api_server: []

agent:
  disabled_toolsets:
    - browser
    - clarify
    - code_execution
    - computer_use
    - context_engine
    - cronjob
    - delegation
    - discord
    - discord_admin
    - feishu_doc
    - feishu_drive
    - file
    - homeassistant
    - image_gen
    - kanban
    - memory
    - project
    - safe
    - search
    - session_search
    - skills
    - spotify
    - terminal
    - todo
    - tts
    - video
    - video_gen
    - vision
    - web
    - x_search
    - yuanbao

memory:
  memory_enabled: false
  user_profile_enabled: false

mcp_servers: {}
```

Die leere plattformspezifische Auswahl ist die eigentliche Fail-closed-
Vorgabe; `disabled_toolsets` ist Defense in Depth. Keine Plugins, Skills, Hooks,
Cronjobs oder MCP-Server in diesem State-Verzeichnis installieren. Da Hermes
Konfigurationen beim Upgrade migriert und die aktuelle Toolset-Auflistung
dynamische MCP-/Plugin-Werkzeuge nicht vollständig belegt, muss die Grenze nach
jeder Image- oder Konfigurationsänderung erneut geprüft werden. Ohne überprüfbar
null Werkzeuge bleibt der Dienst aus.

## Lab-Start und Prüfung

Voraussetzungen sind ein bereits vorhandenes offizielles Image, ein separates
State-Verzeichnis mit der obigen Konfiguration und zwei lokale Umgebungswerte:

- `HERMES_STATE_DIR`: absoluter Pfad zum dedizierten State-Verzeichnis;
- `HERMES_API_SERVER_KEY`: zufälliger, nur für diesen Adapter verwendeter Key
  mit mindestens acht Zeichen.

Vor dem Start muss das Image anhand des dokumentierten Release-Artefakts lokal
vorhanden sein (`pull_policy: never`). Docker Desktop ist in der aktuellen
Entwicklungsumgebung nicht installiert; deshalb ist die Containergrenze noch
nicht praktisch abgenommen. Der spätere Lab-Start ist bewusst explizit:

```text
docker compose -f docker/hermes/compose.yaml --profile lab-only up -d
```

Danach sind mindestens folgende Prüfungen erforderlich:

1. Container-Mounts und -Umgebung enthalten keine Trading-/Brokerdaten.
2. Der authentifizierte Hermes-Endpunkt `GET /v1/toolsets` meldet keine
   aktivierten statischen Toolsets.
3. `/opt/data/config.yaml`, Plugins, Skills und MCP-Konfiguration ergeben keine
   dynamischen Tools; zusätzlich wird verhaltensbasiert geprüft, dass Terminal,
   Datei, Web, Memory und Delegation nicht aufrufbar sind.
4. Nur `http://127.0.0.1:8642/v1` ist vom Trading-Prozess erreichbar.
5. Timeout-, Schema-, Identitäts- und Prompt-Injection-Tests bleiben grün.

`GET /v1/toolsets` allein ist derzeit kein vollständiger Capability-Nachweis,
weil dynamische MCP-Werkzeuge nicht vollständig aufgeführt werden. Diese
Einschränkung ist ein weiterer Grund, Hermes vorerst nur als vorbereiteten
Adapter zu behandeln.

## Offizielle Quellen und bekannte Risiken

- [Docker-Betrieb und `/opt/data`](https://hermes-agent.nousresearch.com/docs/user-guide/docker/)
- [API-Server, Authentifizierung und Vollzugriff auf Tools](https://hermes-agent.nousresearch.com/docs/user-guide/features/api-server/)
- [Toolsets-Referenz](https://hermes-agent.nousresearch.com/docs/reference/toolsets-reference/)
- [Konfiguration: Memory und globales `disabled_toolsets`](https://hermes-agent.nousresearch.com/docs/user-guide/configuration/)
- [Release v2026.8.19 / Hermes Agent 0.20.5](https://github.com/NousResearch/hermes-agent/releases/tag/v2026.8.19)
- [Offene Lücke der Tool-Auflistung](https://github.com/NousResearch/hermes-agent/issues/92711)

Hermes Agent entwickelt sich schnell; API-, Toolset- und Migrationsverhalten
können sich zwischen Releases ändern. Deshalb pinnt der Lab-Container einen
Release, während der produktive MVP ohne Hermes funktionsfähig bleibt.
