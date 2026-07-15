# Plan: INI-Konfiguration modernisieren

**Status: abgeschlossen.** Dieser Plan ersetzt das pauschale Ziel, einen
`IniWatcher` in jeden konfigurationsabhängigen Dienst einzubauen.

## Ziel

PBGui soll `pbgui.ini` prozessübergreifend sicher ändern, als konsistenten
Snapshot lesen und nur dort live neu laden, wo eine Änderung ohne Neustart
fachlich und technisch sicher ist.

Das Zielbild umfasst:

- einen kanonischen absoluten INI-Pfad,
- gesperrte atomare Read-Modify-Write-Transaktionen,
- konsistente, vollständig validierte Konfigurations-Snapshots,
- robuste Änderungserkennung für atomar ersetzte Dateien,
- Last-known-good-Verhalten bei ungültigen Änderungen,
- gezielten Live Reload für PBData, PBCoinData und VPSMonitor,
- explizite Restart-required-Metadaten für Startup-Invarianten,
- deterministischen Lifecycle aller Watcher, Tasks und Events,
- Tests für Konkurrenz, Fehler, Löschen, Wiederherstellung und Shutdown.

## Nicht-Ziele

- Kein `IniWatcher` in jedem Prozess.
- Kein globales, prozessübergreifendes Konfigurations-Singleton.
- Kein Live-Rebind von Ports, Middleware oder laufenden Child-Prozess-Pfaden.
- Kein Watcher für Werte, die bereits pro Request, Operation oder Zyklus frisch
  gelesen werden.
- Keine Überwachung entfernter VPS-INIs durch einen lokalen Dateiwatcher.
- Keine Änderung von Rate-Limit-, Backoff- oder Retry-Sleeps durch Config-Wakeups.
- Keine Callback-Ausführung im Watcher-Thread.

## Bestandsaufnahme

### Aktuelle Mechanismen

| Komponente | Aktuelles Verhalten | Zielentscheidung |
| --- | --- | --- |
| VPSMonitor | Einziger produktiver `IniWatcher` | Behalten und härten |
| PBData | Mehrere eigene mtime-Prüfungen | Auf einen Snapshot-Reload konsolidieren |
| PBCoinData | mtime-Prüfung pro Hauptzyklus | Auf validierten Snapshot-Reload umstellen |
| PBMonitorAgent | Frische Reads pro Statuszyklus | Kein Watcher |
| PBRun | `pbname`, `pb7dir`, `pb7venv` beim Start | Restart required |
| PBCluster | Keine eigene relevante INI-Abhängigkeit | Kein Watcher |
| API Host/Port/CORS | Startup-/Bind-Konfiguration | Restart required |
| Logging-Rotation | Read bei Rotation oder Öffnen | Kein Watcher |
| VPSManager | Read pro Operation/Verbindung | Kein Watcher |
| Task Worker | Read bei Jobstart oder Chunk-Grenze | Kein globaler Watcher |
| API-Endpunkte | Read pro Request | Kein Watcher |

### Bestätigte Risiken

1. Nicht alle lokalen INI-Writer verwenden denselben Cross-Process-Lock.
2. Mehrere Einzelupdates können sich gegenseitig überschreiben oder als
   Zwischenstände beobachtet werden.
3. `IniWatcher` setzt seine Baseline asynchron und kann eine Änderung beim
   Startup verpassen.
4. Die Erkennung verwendet nur `st_mtime` und modelliert Dateilöschung nicht.
5. VPSMonitor leert das Ereignis vor einem vollständig erfolgreichen Parse.
6. VPSMonitor wartet über den Default Executor auf ein `threading.Event`.
7. PBData und PBCoinData veröffentlichen Werte teilweise inkrementell statt als
   vollständig validierten Snapshot.
8. Gelöschte Schlüssel oder Overrides stellen nicht überall Defaults wieder
   her.
9. Parserfehler werden uneinheitlich protokolliert und teilweise nicht erneut
   versucht.
10. Reloadable- und Restart-required-Einstellungen sind weder zentral
     klassifiziert noch konsistent in API und GUI sichtbar.

### Explizite Reader-Ausnahmen nach Abschlussprüfung

Die folgenden direkten `ConfigParser`-Reads sind keine lokalen Reader der
kanonischen PBGui-INI und bleiben deshalb absichtlich außerhalb von
`load_ini_snapshot()`:

- Remote-SFTP-Inhalte und eingebettete Remote-Probes in `master/async_pool.py`,
  `master/async_monitor.py`, `vps_manager_core.py`, `vps_manager_service.py` und
  `api/db_tools.py`; ihr `pbgui.ini` gehört jeweils zum entfernten Host.
- AWS-Profile in `market_data.py`; gelesen werden `~/.aws/credentials` und
  `~/.aws/config`, nicht die PBGui-INI.
- Das Einmal-Installationsprogramm `setup/installer/core.py`, das eine explizit
  übergebene Zielinstallation initialisiert.
- Test- und Audit-Hilfen unter `tests/` und `scripts/`, die ausschließlich mit
  isolierten oder ausdrücklich gewählten Dateien arbeiten.

Lokale produktive Reader verwenden den kanonischen Snapshot-Pfad. Der
operation-scoped Cluster-SSH-Reader übergibt seinen absoluten Installationspfad
explizit an `load_ini_snapshot()` und ist damit weder CWD-abhängig noch ein
zweiter Parserpfad.

## Zielarchitektur

### 1. Einheitliche Schreibtransaktion

In `pbgui_purefunc.py` entsteht eine zentrale Batch-Operation, beispielsweise:

```python
update_ini(mutator)
```

Vertrag:

1. Cross-Process-Lock für `pbgui.ini` erwerben.
2. Genau einen aktuellen Snapshot lesen.
3. Alle Änderungen im Speicher anwenden.
4. Vollständige Konfiguration validieren bzw. serialisieren.
5. Mit privater temporärer Datei und `os.replace()` atomar veröffentlichen.
6. Lock erst nach erfolgreicher Veröffentlichung freigeben.

`save_ini()` und `save_ini_section()` bleiben als kompatible Wrapper erhalten,
verwenden intern aber dieselbe Transaktion. Mehrere zusammengehörige UI-Werte
müssen in einem Batch gespeichert werden.

### 2. Konsistenter Snapshot

Eine neue Snapshot-API liest die Datei genau einmal:

```python
snapshot = load_ini_snapshot()
```

Der Snapshot enthält:

- einen vollständig geparsten `ConfigParser` oder eine unveränderliche Sicht,
- die kanonische absolute Quelldatei,
- eine Signatur der Dateigeneration,
- typisierte Zugriffe mit Unterscheidung zwischen fehlend und ungültig,
- keine schreibbare globale Parser-Instanz.

Empfohlene Signatur:

```text
(exists, st_mtime_ns, st_size, st_ino)
```

Falls `st_ino` auf einer Plattform nicht sinnvoll ist, bleiben Existenz,
Nanosekunden-Zeitstempel und Größe verpflichtend.

### 3. Robuster IniWatcher

Der Watcher meldet ausschließlich, dass eine neue Generation vorhanden sein
könnte. Parsing und Runtime-Mutation bleiben beim Dienst-Owner.

Erforderliche Eigenschaften:

- Baseline synchron in `start()` erfassen,
- vorhandene, fehlende, gelöschte und wieder erzeugte Datei unterscheiden,
- atomare Ersetzungen erkennen,
- Lifecycle mit Lock serialisieren,
- `start()` und `stop()` idempotent machen,
- eine noch lebende Thread-Referenz nach Join-Timeout nicht verwerfen,
- Änderungen level-triggered zusammenfassen,
- Shutdown-Waits wecken,
- keine Callbacks im Watcher-Thread ausführen.

Für Async-Owner wird das Signal über `loop.call_soon_threadsafe()` in ein
`asyncio.Event` übertragen. Wiederholte `run_in_executor(Event.wait)`-Aufrufe
entfallen.

### 4. Dienstlokale Anwendung

Jeder echte Live-Reload-Owner bekommt genau einen Anwendungspfad:

```python
_apply_config_snapshot(snapshot)
```

Ablauf:

1. neuen Snapshot vollständig lesen,
2. alle dienstrelevanten Werte konvertieren,
3. Wertebereiche und Kombinationen validieren,
4. Kandidatenzustand vollständig erzeugen,
5. Runtime-State atomar im Owner-Kontext austauschen,
6. erfolgreiche Signatur als angewendet markieren.

Bei einem Fehler:

- letzten gültigen Zustand behalten,
- keine Teilwerte veröffentlichen,
- Fehler mit Section/Key und Operation protokollieren, ohne Secrets auszugeben,
- dieselbe fehlerhafte Generation mit begrenztem Backoff erneut versuchen oder
  spätestens nach der nächsten Dateigeneration neu bewerten.

### 5. Setting-Metadaten

Konfigurierbare Werte erhalten explizite Laufzeitsemantik:

```text
owner
reloadable
restart_required
apply_timing
default
validation
sensitive
```

Die API liefert nach dem Speichern zurück:

- sofort wirksam,
- im nächsten Arbeitszyklus wirksam,
- Dienstneustart erforderlich,
- API-Neustart erforderlich.

Die GUI verwendet diese Information für Hinweise und den vorhandenen
Restart-required-Status. Secrets werden weder zurückgegeben noch in URLs,
Browser Storage oder Logs aufgenommen.

## Phasen

## Phase 0: Verträge und Inventar

### Aufgaben

1. Alle lokalen `pbgui.ini`-Writer und Long-lived-Reader in einem Testinventar
   erfassen.
2. Für jede GUI-editierbare Einstellung Owner und Apply-Semantik dokumentieren.
3. Werte in `reloadable`, `next_cycle` und `restart_required` klassifizieren.
4. Direkte Parser für Remote-INIs, AWS-Dateien und Einmalskripte ausdrücklich
   als zulässige Ausnahmen markieren.
5. Veraltete oder ungenutzte Keys separat identifizieren; nicht stillschweigend
   migrieren oder entfernen.

### Ergebnis

- versioniertes Setting-Inventar,
- keine unklassifizierten produktiven INI-Writer,
- präzise Scope-Liste für die folgenden Phasen.

## Phase 1: Sichere INI-Grundlage

### Betroffene Dateien

- `pbgui_purefunc.py`
- `file_lock.py`
- `secure_files.py`
- neue/fokussierte INI-Tests

### Aufgaben

1. `load_ini_snapshot()` implementieren.
2. `update_ini()` als gesperrte atomare Batch-Transaktion implementieren.
3. `save_ini()` und `save_ini_section()` darauf aufbauen.
4. Einheitliche Parserfehler und sichere Fehlermeldungen definieren.
5. Section-/Key-Großschreibung und bestehende Serialisierung unverändert
   erhalten.
6. Tests ausschließlich mit temporären INI-Dateien ausführen.

### Tests

- parallele Prozesse ändern verschiedene Keys ohne Lost Update,
- parallele Threads ändern dieselbe Section deterministisch,
- fehlende Datei und erste Erstellung,
- ungültige Datei verändert den letzten gültigen Snapshot nicht,
- atomare Ersetzung und private Dateirechte,
- Writer-Fehler lässt Originaldatei unverändert,
- vollständiger Section-Batch ist nie als Zwischenzustand sichtbar.

### Done wenn

- alle PBGui-kontrollierten lokalen Writer dieselbe Transaktion verwenden
  können,
- Snapshot und Batch-Write vollständig isoliert getestet sind.

## Phase 2: Writer vereinheitlichen

### Priorisierte Writer

- `PBData.py`
- `PBCoinData.py`
- `api/api_keys.py`
- Logging-Settings in `logging_helpers.py`
- API-Service-Settings in `api/services.py`

### Aufgaben

1. Eigene `ConfigParser`-Read-Modify-Write-Blöcke durch `update_ini()` ersetzen.
2. PBData-Settings pro Save-Request in einer Transaktion speichern.
3. PBCoinData-Config, Copy-Trading-User und Legacy-Section-Cleanup sperren.
4. TradFi-Profiländerungen über denselben Lock veröffentlichen.
5. Logging-Settings mit dem allgemeinen INI-Lock koordinieren.
6. Tests für konkurrierende Writer ergänzen.

### Done wenn

- kein produktiver lokaler `pbgui.ini`-Writer außerhalb der Allowlist eine
  eigene ungesperrte Read-Modify-Write-Sequenz besitzt,
- AST-/Policy-Test neue unsichere Writer verhindert.

## Phase 3: IniWatcher und VPSMonitor härten

### Betroffene Dateien

- `ini_watcher.py`
- `master/async_monitor.py`
- `master/async_store.py`
- `tests/test_ini_watcher.py`
- VPSMonitor-Integrationstests

### Aufgaben

1. Signaturmodell und synchrone Baseline implementieren.
2. Löschen, Wiedererstellen und atomare Ersetzung erkennen.
3. Lifecycle-Lock und verlässliches Join-Verhalten ergänzen.
4. Async-Bridge mit `asyncio.Event` statt Default Executor verwenden.
5. VPSMonitor lädt pro Event genau einen Snapshot.
6. Host-, Alert-, Threshold- und UI-Settings erst nach vollständiger
   Validierung anwenden.
7. `[vps_monitor_ui]` bei externen Änderungen ebenfalls aktualisieren.
8. Reload-erzeugte Tasks beim Monitor registrieren und beim Shutdown abwarten.

### Tests

- Änderung direkt während Startup geht nicht verloren,
- `os.replace()` wird erkannt,
- Datei löschen und wieder erstellen erzeugt beide Zustandswechsel,
- schnelle Änderungen werden sicher auf den neuesten Snapshot zusammengefasst,
- Parserfehler behält letzten gültigen Zustand und Recovery funktioniert,
- Host enable/disable führt exakt die erwarteten Ressourcenaktionen aus,
- Shutdown hinterlässt weder Watcher-Thread noch Executor-Wait oder Task.

### Done wenn

- VPSMonitor jede gültige Generation konsistent übernimmt,
- ungültige Generationen keinen Teilzustand erzeugen,
- der Lifecycle deterministisch und idempotent ist.

## Phase 4: PBData konfigurativ konsolidieren

### Betroffene Bereiche

- Polling-Intervalle
- WebSocket-Limit und Log-Level
- Latest-1m-Einstellungen aller Exchanges
- REST-Pausen und Exchange-Overrides
- `fetch_users` und `trades_users`

### Aufgaben

1. Alle Defaults und Validatoren in einem PBData-Konfigurationsmodell bündeln.
2. Einen vollständigen PBData-Snapshot laden und validieren.
3. Wiederholte mtime-Prüfungen aus einzelnen Loops entfernen.
4. Ein Owner-Signal nur für Scheduling-/Config-Waits verwenden.
5. Geänderte Polling-Intervalle kontrolliert auf laufende Tasks anwenden.
6. Gelöschte Keys auf dokumentierte Defaults zurücksetzen.
7. Exchange-Pausen-Overrides ersetzen statt in alten Zustand einzumischen.
8. Ungültige `fetch_users`/`trades_users` behalten den letzten gültigen Wert.
9. Rate-Limit-, Retry- und Backoff-Waits nicht durch INI-Änderungen verkürzen.

### Tests

- jeder reloadable Key wird übernommen,
- entfernte Keys und Overrides stellen Defaults wieder her,
- malformed JSON und Userlisten behalten Last-known-good,
- Intervalländerung erzeugt weder doppelte noch verlorene Poller,
- Shutdown während Config-Wakeup ist sauber,
- gleichzeitiger API-Save verliert keine unabhängige Änderung.

### Done wenn

- PBData nur einen Konfigurations-Anwendungspfad besitzt,
- keine dienstlokalen mtime-Poller mehr notwendig sind,
- alle Reload-Semantiken getestet sind.

## Phase 5: PBCoinData konfigurativ konsolidieren

### Reloadable Keys

- CoinMarketCap API-Key
- Fetch-Limit
- Fetch-Intervall
- Metadata-Intervall
- Mapping-Intervall

### Aufgaben

1. Defaults und Wertebereiche zentral definieren.
2. Einen vollständigen Kandidaten-Snapshot validieren.
3. Generation erst nach erfolgreicher Anwendung übernehmen.
4. Hauptschlaf durch einen Config-/Stop-interruptiblen Scheduling-Wait ersetzen.
5. Lange Mapping-/Fetch-Operationen nicht künstlich abbrechen.
6. Entfernte Keys auf Defaults zurücksetzen.
7. Copy-Trading-User weiterhin operationell lesen oder explizit in das Modell
   aufnehmen; keine zweite versteckte Cache-Semantik behalten.

### Tests

- gültige Änderung weckt den nächsten Scheduling-Schritt,
- ungültiger Integer beendet keinen Hauptzyklus,
- fehlerhafte Generation wird erneut bewertet,
- Recovery nach korrigierter Datei,
- Key-Löschung stellt Defaults wieder her,
- kein zusätzlicher Fetch oder paralleler Mapping-Zyklus durch Reload.

### Done wenn

- PBCoinData Änderungen deterministisch spätestens vor dem nächsten Zyklus
  übernimmt,
- ungültige Konfiguration den letzten gültigen Betrieb nicht unterbricht.

## Phase 6: Restart-required UX und Dokumentation

### Restart-required

- API Host und Port
- CORS-Middleware
- API-/AsyncSSH-Log-Level-Wiring
- PBMaster-WebSocket Host und Port
- PBRun `pbname`, `pb7dir`, `pb7venv`

### Kein Watcher erforderlich

- PBCluster/ClusterSyncWorker
- PBMonitorAgent
- Logging-Rotation
- VPSManager-Operationswerte
- API-Request-Werte
- Remote-INIs
- Job-lokale Task-Worker-Einstellungen

### Aufgaben

1. API-Save-Antworten um Apply-Semantik ergänzen.
2. GUI zeigt „sofort“, „nächster Zyklus“ oder „Neustart erforderlich“.
3. Vorhandenen API-Restart-Status weiterverwenden.
4. Dienstneustarts nur dem tatsächlichen Owner zuordnen.
5. EN-/DE-Guides aktualisieren.
6. Den Begriff `IniWatcher in allen Diensten` vollständig entfernen.

### Done wenn

- Benutzer vor dem Speichern oder unmittelbar danach erkennen, wann eine
  Änderung wirksam wird,
- kein Dienst fälschlich Hot Reload verspricht.

**Phase-6-Status: implementiert und abgenommen.**

## Rollout-Reihenfolge

1. Phase 0 und 1 ohne Änderung der Runtime-Semantik veröffentlichen.
2. Writer in Phase 2 migrieren und Konkurrenztests stabilisieren.
3. VPSMonitor als ersten echten Watcher-Consumer härten.
4. PBData separat migrieren und längere Offline-Suite ausführen.
5. PBCoinData separat migrieren und Mapping-/Scheduler-Tests ausführen.
6. UX und Dokumentation erst auf tatsächlich implementierte Semantik umstellen.

Jede Phase muss einzeln review- und rollbackfähig bleiben. Keine Phase darf
eine automatische Remote-Änderung, einen Service-Neustart oder ein Deployment
auf Bot-/VPS-Hosts auslösen.

## Sicherheits- und Betriebsregeln

- Keine Secrets in Logs, API-Antworten, URLs oder Browser Storage.
- Sensitive Werte bei Validierungsfehlern nur über Section/Key benennen.
- Keine produktive `pbgui.ini` in Tests lesen oder schreiben.
- Keine unsicheren Fallback-Writer für alte INI-Pfade einführen.
- Ein Parsefehler darf keinen laufenden Dienst mit Teilkonfiguration stoppen.
- Ein Reload darf keine aktiven Child-Prozesse auf neue Pfade umhängen.
- Jeder Watcher und jede Async-Bridge besitzt genau einen Lifecycle-Owner.
- Shutdown bleibt idempotent und wartet registrierte Ressourcen ab.

## Verifikationsmatrix

| Ebene | Verifikation |
| --- | --- |
| Unit | Snapshot, Typkonvertierung, Signatur, Defaults, Parserfehler |
| Concurrency | Threads und Prozesse, verschiedene und gleiche Keys |
| Watcher | Replace, Delete, Recreate, Startup-Race, Coalescing |
| VPSMonitor | Hosts, Alerts, Thresholds, UI-State, Shutdown |
| PBData | Intervalle, Overrides, Userlisten, Task-Neustart |
| PBCoinData | Invalid/Recovery, Defaults, Scheduling-Wakeup |
| API | Batch-Save, Apply-Metadaten, Auth, keine Secrets |
| Policy | Keine neuen ungesperrten lokalen INI-Writer |
| Regression | vollständige Offline-Suite `python -m pytest tests/` |

## Gesamtabnahme

Der Plan ist abgeschlossen, wenn:

1. alle lokalen PBGui-INI-Writer gesperrt und atomar arbeiten,
2. zusammengehörige Einstellungen transaktional gespeichert werden,
3. VPSMonitor, PBData und PBCoinData nur vollständig validierte Snapshots
   anwenden,
4. ungültige Änderungen den letzten gültigen Runtime-State erhalten,
5. Löschen und Wiederherstellen von Datei oder Keys definierte Semantik hat,
6. keine unnötigen Watcher in PBRun, PBCluster, PBMonitorAgent, Logging,
   VPSManager oder Task Worker existieren,
7. Restart-required-Einstellungen in API, GUI und Dokumentation korrekt
   ausgewiesen werden,
8. Watcher, Events, Tasks und Threads sauber beendet werden,
9. Konkurrenz-, Integrations- und vollständige Regressionstests erfolgreich
   sind.

**Abnahme:** Alle Kriterien sind erfüllt. Die vollständige Offline-Suite lief
mit 1.335 bestandenen und 39 erwartungsgemäß übersprungenen opt-in Tests durch.

## Bewusst vertagte Entscheidungen

- Ob ein formales Setting-Schema als Python-Datentypen oder deklarative
  Registry umgesetzt wird, wird in Phase 0 anhand der bestehenden API-Modelle
  entschieden.
- Ein zentraler Inotify-/Watchdog-basierter Dienst ist nicht vorgesehen;
  Metadaten-Polling reicht für die kleine lokale INI aus.
- Remote-Konfigurationsänderungen bleiben explizite VPSManager-Operationen und
  werden nicht Teil des lokalen Watchers.
