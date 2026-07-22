# Plan: Passivbot v8 in PBGui integrieren

**Status: vereinfacht, vorgeschlagen.** PB8 wird nach demselben Grundmuster wie
PB7 integriert. Es entstehen getrennte PB8-Pfade und PBv8-Seiten, aber keine
neue allgemeine Runtime-, Queue- oder Release-Plattform.

Die ersten beiden Schritte sind:

1. PB8 auf PBGui-Mastern installieren und aktualisieren.
2. V7-Backtest-Configs nach V8 konvertieren und PB8-Backtests verwalten.

PB8 auf Slave-VPS, PB8 Live und PB8 Optimize sind nicht Bestandteil dieser
beiden Schritte.

## Grundsaetze

1. PB7 bleibt unveraendert und weiterhin auf seinem bekannten V7-Commit.
2. PB8 verwendet die festen Pfade `<install_dir>/pb8` und
   `<install_dir>/venv_pb8`.
3. PB8 ist nicht gepinnt. Install und Update verwenden den neuesten V8-Stand
   von Upstream `origin/master`.
4. PB8 wird in Schritt 1 nur auf Hosts mit Rolle `master` installiert. Slave-
   VPS werden nicht veraendert.
5. PBv7 und PBv8 erhalten getrennte Menues, Seiten, APIs, Configs, Queues, Logs
   und Resultate.
6. Gemeinsame vorhandene UI-Helfer werden wiederverwendet. Die V7-Seiten werden
   dafuer nicht vorab gross refaktoriert.
7. PB8-Configs werden mit den Loadern der installierten PB8-Version geladen und
   vorbereitet.
8. Eine V7-zu-V8-Konvertierung verwendet das offizielle PB8-Migrationstool und
   veraendert die V7-Quelle nicht.
9. PB8-Jobs werden nicht an einen Commit gebunden. Ein neuer Start verwendet
   immer die zu diesem Zeitpunkt installierte PB8-Version.
10. Ein laufender PB8-Backtest wird durch `Update PB8` nicht gestoppt, nicht neu
    gequeued und nicht neu gestartet. Er laeuft einfach bis zum Ende.

## Bewusst nicht vorgesehen

Fuer die ersten beiden Schritte bauen wir nicht:

- commitbezogene PB8-Release-Verzeichnisse,
- aktive Release-Symlinks,
- Job-Pinning auf alte PB8-Versionen,
- einen neuen allgemeinen Job-Supervisor,
- eine gemeinsame V7/V8-Queue-Plattform,
- ein neues Cluster-Schema,
- PB8 auf Slave-VPS,
- native V8-Config-Erzeugung,
- PB8 Live oder Optimize.

## Bereits umgesetzte Voraussetzung

PB7 ist gegen einen unbeabsichtigten Wechsel auf PB8 abgesichert. Die PB7-
Installer und Standard-Updates verwenden den bekannten V7-Commit
`befaa9b7aa89e00ee55704221b39621ad700ac36`. PB8 wird daneben in `pb8` und
`venv_pb8` installiert.

## Verifizierte PB8-Vertraege

### Installation

PB8 verwendet Python 3.12 und auf dem Master das Full-Profil:

```bash
python3.12 -m venv venv_pb8
venv_pb8/bin/python -m pip install --upgrade pip
venv_pb8/bin/python -m pip install -e "./pb8[full]"
```

### CLI

```text
venv_pb8/bin/passivbot --help
venv_pb8/bin/passivbot backtest <config>
venv_pb8/bin/passivbot tool migrate-config-v7 <v7> <v8> --report <report>
```

### Config

PB8 verwendet unter anderem:

```text
config_version: "v8.0.0"
live.strategy_kind
bot.long.strategy.<strategy_kind>.*
bot.short.strategy.<strategy_kind>.*
```

PB8 stellt in `src/config/load.py` `load_prepared_config()` und
`prepare_config()` bereit. Diese Funktionen bleiben die Quelle fuer Laden,
Normalisierung und Validierung.

### Migration

```bash
passivbot tool migrate-config-v7 \
  config_v7.json \
  config_v8.json \
  --report migration_report.json
```

Eine erfolgreiche Migration erzeugt eine V8-Config mit
`live.strategy_kind = "trailing_grid_v7"`. Gibt das Tool wegen manueller oder
verworfener Felder keine Config aus, zeigt PBGui den Report und legt keine
V8-Config an. Der unsichere Schalter `--allow-manual-review-output` wird in den
ersten beiden Schritten nicht automatisch angeboten.

# Schritt 1: PB8 auf Mastern installieren und aktualisieren

## Zielverhalten im VPS Manager

Im Kontext eines lokalen oder verwalteten Remote-Masters erscheint unter
`Tasks` genau eine PB8-Aktion:

- `Install PB8`, wenn PB8 noch nicht vollstaendig installiert ist.
- `Update PB8`, wenn PB8 installiert ist.
- Warnungsfarbe, wenn der installierte Commit nicht dem aktuellen
  `origin/master` entspricht.

Bei einem Host mit Rolle `vps` oder `slave` wird keine PB8-Aktion angezeigt.
Das Backend und das Playbook pruefen die Master-Rolle ebenfalls. Die Aktion wird
nicht in die Bulk-Aktionen aufgenommen.

## Runtime-Pfade

PBGui erhaelt analog zu PB7:

```text
[main]
pb8dir = <install_dir>/pb8
pb8venv = <install_dir>/venv_pb8/bin/python
```

Die Werte werden nach erfolgreicher Erstinstallation gesetzt. Es gibt keine
versionierten PB8-Verzeichnisse und keine Umschalt-Symlinks.

## Installationsstatus

Der Status enthaelt mindestens:

```text
installed
version
commit
python_version
config_version
update_available
error
```

PB8 gilt als installiert, wenn:

1. `<install_dir>/pb8` existiert und ein Passivbot-Git-Checkout ist.
2. `<install_dir>/venv_pb8/bin/python` existiert.
3. `<install_dir>/venv_pb8/bin/passivbot` existiert.
4. `passivbot --help` funktioniert.
5. `passivbot_rust` importiert werden kann.
6. Der Config-Loader eine V8-Config meldet.

Ein unvollstaendiger Stand bleibt `Install PB8` und kann durch erneutes Starten
der Aktion repariert werden.

## Commands und Playbooks

```text
master-update-pb8
vps-update-pb8
```

Die Playbooks folgen dem bestehenden PB7-Muster:

1. Zielhost und Master-Rolle pruefen.
2. Python 3.12, Rust und Build-Tools pruefen beziehungsweise installieren.
3. Repository nach `<install_dir>/pb8` klonen oder den bestehenden Checkout auf
   den neuesten `origin/master` aktualisieren.
4. Pruefen, dass der geladene Stand weiterhin PB8 ist.
5. `venv_pb8` anlegen, falls es fehlt.
6. Pip aktualisieren und `.[full]` installieren.
7. CLI, Rust-Import und Config-Loader pruefen.
8. `pb8dir` und `pb8venv` speichern.
9. Taskstatus und Log aktualisieren.

Ein fehlgeschlagener Lauf wird im vorhandenen VPS-Manager-Tasklog angezeigt.
Die Aktion kann danach erneut gestartet werden. PB7-Pfade und PB7-Prozesse
werden nicht angefasst.

## Verhalten bei laufenden Backtests

`Update PB8` greift nicht in laufende PB8-Backtests ein. Der bereits gestartete
Prozess laeuft weiter. Wartende und neue Jobs verwenden beim naechsten Start die
aktualisierte PB8-Installation.

## Monitoring

Der bestehende Monitor-Status wird additiv um PB8-Version, Commit, Python und
Config-Version erweitert. Der VPS Manager vergleicht den installierten Commit
mit dem aktuellen Upstream-Commit und setzt `update_available`.

## Voraussichtlich betroffene Dateien

```text
master-update-pb8.yml
vps-update-pb8.yml
vps_manager_service.py
vps_manager_core.py
frontend/vps_manager.html
master/async_monitor.py
pbgui_purefunc.py
ini_settings.py
tests/test_pb8_installation.py
tests/ui/test_vps_manager_frontend_logic.py
docs/help/32_vps_manager.md
docs/help_de/32_vps_manager.md
```

## Tests

- Lokaler Master ohne PB8 zeigt `Install PB8`.
- Installierter lokaler Master zeigt `Update PB8`.
- Remote-Master ohne PB8 zeigt `Install PB8`.
- Installierter Remote-Master zeigt `Update PB8`.
- Slave-VPS zeigt keine PB8-Aktion.
- Ein direkter PB8-Request fuer einen Slave wird abgelehnt.
- Install erstellt `pb8` und `venv_pb8`.
- Update verwendet den neuesten `origin/master`.
- Unvollstaendige Installation kann repariert werden.
- PB8-Install und -Update veraendern PB7 nicht.
- Ein laufender PB8-Backtest wird durch Update nicht gestoppt.
- Ein nach dem Update gestarteter Job verwendet die aktualisierte Installation.

## Exit-Kriterien

- `Install PB8` und `Update PB8` funktionieren auf lokalem und Remote-Master.
- Auf Slave-VPS gibt es keine PB8-Installation.
- PB8 CLI, Rust und Config-Loader funktionieren im eigenen Venv.
- PB8-Version und Update-Status sind im VPS Manager sichtbar.
- PB7 bleibt unveraendert.

# Schritt 2: PBv8 Backtest

## Navigation

PBv8 erhaelt ein eigenes Hauptmenue. In Schritt 2 wird nur der implementierte
Punkt angezeigt:

```text
PBv8
  Backtest
```

Der Page-Key lautet `v8_backtest` und verweist auf
`/api/backtest-v8/main_page`. Noch nicht implementierte Punkte werden nicht als
Platzhalter angezeigt.

Langfristig ist vorgesehen:

```text
PBv8
  Run
  Backtest
  Optimize
  Strategy Explorer
  Pareto Explorer
```

## Convert to V8

Auf der bestehenden PBv7-Backtest-Seite erscheint bei einer gespeicherten
V7-Config in der Sidebar `Convert to V8`.

Ablauf:

1. V7-Config speichern, falls noch Aenderungen offen sind.
2. Zielnamen abfragen; Standard ist `<v7-name>_v8`.
3. Offizielles PB8-Migrationstool ausfuehren.
4. Migration-Report speichern.
5. Erzeugte Config mit dem PB8-Loader laden und validieren.
6. Config unter `data/bt_v8/<name>/backtest.json` speichern.
7. Zur PBv8-Backtest-Seite wechseln und die Config oeffnen.

Die V7-Quelle wird nicht geaendert. Existiert der V8-Zielname bereits, wird die
Konvertierung mit HTTP 409 abgelehnt. Bei einem Migrationsfehler zeigt PBGui den
Report und speichert keine V8-Config.

## PB8-Config-Adapter

PB7 und PB8 besitzen Module mit gleichen Namen. Deshalb wird PB8 nicht direkt
in den API-Prozess importiert. Ein kleiner Helper wird mit `pb8venv` gestartet
und bietet:

```text
status
load
prepare
migrate_v7
```

Der Helper liest JSON ueber stdin/stdout, verwendet die offiziellen PB8-Loader
und liefert JSON oder eine klare Fehlermeldung zurueck. Er bekommt ein Timeout
und wird nach dem Request beendet. Weitere Helper-Infrastruktur ist nicht
vorgesehen.

## Datenpfade

Analog zu V7:

```text
data/bt_v8/<config>/backtest.json
data/bt_v8/<config>/migration_report.json
data/bt_v8_queue/
data/logs/backtests_v8/
<pb8dir>/backtests/pbgui/<config>/
```

V7 und V8 duerfen denselben Config-Namen verwenden, weil die Roots getrennt
sind.

## PBv8-Backtest-Seite

Die PBv8-Route rendert direkt `frontend/v7_backtest.html`. Dadurch verwenden V7
und V8 nicht nur denselben grundsaetzlichen Aufbau, sondern exakt dieselbe
Editor-, Sidebar-, Tabellen- und Panelimplementierung:

- Configs,
- Queue,
- Results,
- Editor-Sidebar,
- Log-Panel.

In Schritt 2 werden nicht uebernommen:

- Archive,
- Legacy Results,
- Add to Run,
- Optimize,
- Strategy Explorer.

Die gemeinsame Seite verwendet vorhandene Helfer wie `editor_shared.js`,
`pbgui_dialogs.js`, `sidebar_resize.js`, `json_panel.js` und
`log_viewer_panel.js`. Es gibt keine kopierte `v8_backtest.html` und keine
zweite Editorimplementierung. `backtest_editor_adapter.js` kapselt nur die
unterschiedlichen V7/V8-Pfade, API-Endpunkte, Navigation und Logpfade.

## Editor

Der erste PBv8-Editor unterstuetzt migrierte
`live.strategy_kind = "trailing_grid_v7"`-Configs.

Wiederverwendbare Felder werden mit V8-Pfaden angebunden:

- Backtest-Zeitraum,
- Exchanges,
- Startbalance und Gebuehren,
- Approved/Ignored Coins,
- gemeinsame Live- und Backtest-Felder,
- Long/Short-Einstellungen,
- Raw JSON fuer noch nicht strukturierte V8-Felder.

V8-spezifische Pfade liegen unter anderem unter:

```text
bot.<side>.strategy.trailing_grid_v7.*
bot.<side>.risk.*
bot.<side>.unstuck.*
bot.<side>.forager.*
bot.<side>.hsl.*
```

Laden, neue Configs und Import laufen durch den PB8-Loader. Vor Save und Queue
wird die Config mit der aktuell installierten PB8-Version vorbereitet und
validiert. `New Config` verwendet das Template der installierten PB8-Version.

## API

Additive Routen unter `/api/backtest-v8`:

```text
GET    /main_page
GET    /runtime
GET    /settings
POST   /settings
GET    /configs
GET    /configs/new-config
POST   /configs/prepare
GET    /configs/{name}
PUT    /configs/{name}
DELETE /configs/{name}
POST   /migrate-v7
GET    /bot-params
GET    /override-params
GET    /override-config/{config_name}/{filename}
PUT    /override-config/{config_name}/{filename}
GET    /queue
POST   /queue
POST   /queue/{id}/start
POST   /queue/{id}/stop
POST   /queue/{id}/restart
DELETE /queue/{id}
GET    /queue/{id}/log
GET    /results
GET    /results/config
GET    /results/analysis
GET    /results/equity
GET    /results/fills
GET    /results/files
DELETE /results
WS     /ws/bt7
```

Browserzugriffe verwenden die HttpOnly-Session-Cookie. Namen und Pfade werden
wie bei den aktuellen FastAPI-Seiten validiert.

## Queue und Worker

Die V8-Queue folgt dem einfachen V7-Muster:

1. Queue-Eintrag als JSON unter `data/bt_v8_queue` speichern.
2. Config-Snapshot im Queue-Eintrag beziehungsweise Queue-Arbeitsordner
   speichern.
3. Worker liest queued Eintraege und beachtet das konfigurierte CPU-Limit.
4. Vor dem Start Snapshot mit dem aktuellen PB8-Loader validieren.
5. PB8 starten:

```text
cwd: <pb8dir>
command: <venv_pb8>/bin/passivbot backtest <snapshot_config>
```

6. PID und Logdatei speichern.
7. Start, Stop, Restart, Delete und Clear Finished wie bei V7 anbieten.
8. Beim API-Start Queue-, PID- und Logstatus erneut einlesen.
9. Beim API-Shutdown nur den Queue-Worker stoppen; laufende Backtests laufen
   weiter.

Queue-Eintraege speichern keinen PB8-Commit. Jeder neue Start verwendet die
aktuell installierte PB8-Version. Der beim Start verwendete Commit kann fuer
Diagnose und Resultanzeige geloggt werden.

## Update waehrend Backtests

- Laufende PB8-Backtests laufen unveraendert weiter.
- Wartende Jobs bleiben in der Queue.
- Neue Starts nach dem Update verwenden die aktualisierte PB8-Version.
- Es gibt kein automatisches Stoppen, Requeue oder Wiederholen.

## Results

Vor der Implementierung der Results-Ansicht wird ein kleiner echter PB8-
Backtest ausgefuehrt. Danach werden die tatsaechlichen PB8-Dateien und Metriken
in einem einfachen V8-Resultparser abgebildet.

Die Results-Seite zeigt in Schritt 2 nur die grundlegenden vorhandenen Daten.
PB7-Parser und PB7-Archive werden nicht fuer V8 verwendet.

## Startup und Shutdown

`api/backtest_v8.py` erhaelt wie `api/backtest_v7.py`:

```text
startup()
shutdown()
```

Der API-Start startet den Queue-Worker. Der API-Shutdown beendet den Worker und
laesst laufende Backtest-Prozesse bestehen. Die Hooks werden in
`PBApiServer.py` registriert.

## Voraussichtlich betroffene Dateien

```text
pb8_config.py
pb8_config_helper.py
api/backtest_v8.py
PBApiServer.py
frontend/pbgui_nav.js
frontend/v7_backtest.html
frontend/js/backtest_editor_adapter.js
tests/test_pb8_config.py
tests/test_backtest_v8_api.py
tests/ui/test_backtest_v8_frontend_logic.py
docs/help/35_pbv7_backtest.md
docs/help_de/35_pbv7_backtest.md
docs/help/42_pbv8_backtest.md
docs/help_de/42_pbv8_backtest.md
```

API- und Startup-Aenderungen erhoehen `api/serial.txt`. Neue oder geaenderte
Browserassets erhalten die projektuebliche Cache-Busting-Version.

## Tests

### Config und Migration

- V7-Quelle bleibt unveraendert.
- Saubere Migration erzeugt `trailing_grid_v7` und einen Report.
- Migrationsfehler erzeugt keine V8-Config.
- Namenskonflikt liefert HTTP 409.
- V8-Config wird mit dem aktuell installierten PB8-Loader geladen.
- Save schreibt nie nach `data/bt_v7`.

### Frontend und Navigation

- PBv8-Menue zeigt in Schritt 2 nur `Backtest`.
- `Convert to V8` erscheint bei gespeicherter V7-Config.
- Nach erfolgreicher Migration wird die PBv8-Seite geoeffnet.
- PBv8 verwendet nur `/api/backtest-v8`.
- EN-/DE-Guide-Mapping bleibt vollstaendig.

### Queue und Prozess

- V7- und V8-Queue kollidieren nicht.
- V8 startet ueber `<venv_pb8>/bin/passivbot backtest`.
- Queue verwendet den gespeicherten Config-Snapshot.
- Stop/Restart/Delete eines V8-Jobs trifft keinen V7-Prozess.
- Laufende Jobs werden nach API-Neustart wieder erkannt.
- `Update PB8` stoppt keinen laufenden V8-Backtest.
- Ein nach Update gestarteter Job verwendet die neue Installation.

### Results

- PB8-Resultate werden nur unter dem PB8-Resultroot gesucht.
- PB8-Resultate werden nicht mit dem PB7-Parser gelesen.
- Grundlegende PB8-Metriken werden korrekt angezeigt.

## Exit-Kriterien

- Eine V7-Backtest-Config kann nach V8 konvertiert werden.
- Die konvertierte Config oeffnet sich unter `PBv8 > Backtest`.
- Die Config wird mit dem PB8-Loader geladen und gespeichert.
- Ein PB8-Backtest kann gequeued, gestartet, gestoppt und erneut gestartet
  werden.
- Laufende PB8-Backtests ueberstehen API-Neustart und PB8-Update.
- Grundlegende PB8-Resultate werden angezeigt.
- PB7 bleibt unveraendert nutzbar.

# PBv8 Optimize und versionsgetrennte Archive

Dieser Abschnitt ist der verbindliche Implementierungsplan fuer die naechste
Lieferung. Die Umsetzung wird erst als abgeschlossen betrachtet, wenn der
PBv8-Optimizer funktional die PBv7-Implementierung abdeckt und alle unten
genannten Exit-Kriterien erfuellt sind.

## Festgelegte Produktentscheidungen

- `frontend/v7_optimize.html` bleibt die einzige Optimize-Seite. PBv7 und PBv8
  verwenden denselben Editor, dieselben Panels und dieselbe visuelle Sprache
  ueber einen kleinen versionsspezifischen Adapter. Es wird keine separate
  PBv8-Optimize-Oberflaeche kopiert oder neu entworfen.
- Alle von der installierten PB8-Runtime angebotenen Strategien werden
  dynamisch unterstuetzt, aktuell `trailing_martingale`, `ema_anchor` und
  `trailing_grid_v7`.
- PB8-spezifische Funktionen werden vollstaendig angebunden: echter
  Checkpoint-Resume, RNG Seed, Fine-Tune-Parameter, Polish Percentage und
  Polish Bounds Mode. Seltene Expert-Parameter duerfen unter Additional
  Parameters liegen, muessen aber erhalten, validiert und nutzbar bleiben.
- Die offizielle V7-zu-V8-Migration wird auch fuer Optimize-Configs und
  Pareto-Configs verwendet, weil PB8 sie ueber `tool migrate-config-v7`
  anbietet. Der Migrationsreport wird wie beim PB8-Backtest gespeichert und
  ungeloeste Migrationen werden nicht stillschweigend akzeptiert.
- Mehrere Exchanges behalten die PB8-Semantik eines kombinierten Datensatzes.
  Separate Exchange-Auswertungen werden explizit als Suite-Szenarien
  konfiguriert und nicht automatisch erzeugt.
- PB8 Backtest und PB8 Optimize erhalten vollstaendige Archive-Funktionen.
  Archive bleiben physisch und logisch von PB7 getrennt; archivierte Configs
  werden unter ihrer vorhandenen `config_version` gespeichert und geladen.
- Autostart darf global hoechstens einen Optimize-Job aus PB7 oder PB8 laufen
  lassen. Manuell gestartete PB7- und PB8-Jobs duerfen parallel laufen.
- Innerhalb von PB8 startet Autostart ebenfalls nur einen Job. Jeder Job
  verwaltet seine Parallelitaet ueber `optimize.n_cpus`.

## Runtime- und Config-Vertrag

- PB8-Module werden weiterhin nur ueber den isolierten
  `pb8_config_helper.py`-Prozess geladen, damit keine PB7/PB8-Modulnamen im API-
  Prozess kollidieren.
- Der Helper liefert ein zusammenhaengendes Optimize-Metadatenmodell aus der
  installierten PB8-Runtime: Template, Strategien, aktive verschachtelte
  Bounds, Backend- und Pymoo-Optionen, Scoring-Metriken und Default-Ziele,
  Limits, Operatoren, Statistiken, Runtime Overrides und Bot-Parameterpfade.
- PB8-Defaults werden nicht aus PB7 kopiert und nicht statisch im Browser
  nachgebaut.
- Verschachtelte PB8-Pfade unter `bot`, `optimize.bounds`, `scoring`, `limits`,
  `fixed_params`, `fixed_runtime_overrides` und `enable_overrides` werden ohne
  Informationsverlust zwischen Visual Editor, Raw JSON und gespeicherter Config
  synchronisiert.
- Configs werden als recoverable Bundles unter `data/opt_v8/<name>/` gespeichert.
  Queue-Eintraege besitzen unveraenderliche Snapshots unter einem getrennten
  PB8-Queue-Root.

## Queue, Worker und Prozesslebenszyklus

- Die PB8-Queue deckt Add, Reorder, Start, Stop, Requeue Fresh, Delete, Clear
  Finished, Log, Status, Repair und API-Neustart-Recovery ab.
- `Requeue Fresh`, `Continue from Pareto` (`--start`) und `Resume Checkpoint`
  (`--resume`) sind getrennte Aktionen mit eindeutigen Statusmeldungen.
- Resume akzeptiert nur verwaltete lokale PB8-Resultverzeichnisse. Fremde
  `checkpoint.pkl`-Dateien werden wegen des Pickle-Ausfuehrungsrisikos nie
  angenommen.
- Der Runner verwendet PID plus Prozess-Startzeit, Ready-Handshake, private
  atomare State-Dateien, verifizierte Prozessgruppen und den gemeinsamen
  PB8-Update-/Launch-Lock nach dem PB8-Backtest-Muster.
- Laufende Optimize-Prozesse ueberstehen API-Neustarts und werden beim normalen
  API-Shutdown nicht beendet.
- Die globale Autostart-Arbitrierung prueft PB7 und PB8. Nur automatische Starts
  blockieren einander; explizite manuelle Starts bleiben erlaubt.

## Gemeinsamer Editor und Funktionsparitaet

- Die gemeinsamen Panels bleiben Configs, Queue, Results und Paretos.
- Config Create, Duplicate, Rename, Save, Delete, Raw JSON, Suite Editor,
  OHLCV-Preflight/Preload, Starting Configs, Runtime Overrides, Scoring, Limits,
  Bounds, Backends und alle bestehenden PB7-Editorfunktionen werden fuer PB8
  angebunden.
- Der Adapter kapselt API-/WebSocket-Pfade, Navigation, Log-Namespace,
  verschachtelte Config-Pfade, Result-/Pareto-Routen und versionsspezifische
  Aktionen. Fachlogik wird nicht durch eine zweite HTML-Datei dupliziert.
- Neue PB8-Felder werden sichtbar und passend gruppiert, wenn sie fuer den
  normalen GUI-Workflow sinnvoll sind. Seltene Expertenfelder liegen in
  Additional Parameters und bleiben round-trip-sicher.

## Results, Paretos und Handoffs

- PB8-Results werden ausschliesslich unter dem PB8-Optimize-Resultroot gelesen.
- Resultliste, Config-Ansicht, Delete, Fortschritt, Pareto-Dateien, Seed-Bundles,
  Preset Builder und Pareto Explorer erhalten PB7-Funktionsparitaet.
- Der bestehende PBGui Pareto Explorer wird intern versionsfaehig gemacht und
  ueber getrennte PB7-/PB8-Routen und erlaubte Roots abgesichert. Es wird kein
  zweiter Explorer im Frontend erfunden.
- Der Parser versteht PB8s verschachtelte Bounds, Bot-Pfade, Suite-Metriken,
  Scoring-Ziele und inkrementelle `all_results.bin`-Eintraege.
- Pareto-Config zu PB8 Backtest, Pareto-Auswahl zu PB8 Optimize Seeds,
  Optimize-Result zu Preset und offizielle V7-zu-V8-Handoffs bleiben
  versionssicher.

## Archive

- PB8 Backtest und PB8 Optimize erhalten dieselben produktiven Archivaktionen
  wie PB7, aber getrennte Roots und versionierte Config-Eintraege.
- Archive-Listen, Import/Export, Resultansicht, Restore, Delete und alle
  vorhandenen Handoffs respektieren `config_version` und duerfen PB7- und
  PB8-Dateien nicht mit dem falschen Parser laden.
- Gemeinsame Archivansichten duerfen beide Versionen anzeigen, muessen jede
  Aktion jedoch an den besitzenden Backend- und Config-Vertrag routen.

## Tests und Exit-Kriterien

- PB7 Optimize bleibt unveraendert funktionsfaehig und visuell identisch.
- PB8 bietet alle PB7-Config-, Queue-, Results-, Pareto-, Preset-, Explorer-,
  Handoff- und Archivfunktionen.
- Alle drei PB8-Strategien und alle Runtime-gemeldeten Optimize-Parameter sind
  round-trip-sicher konfigurierbar.
- Fresh Requeue, Pareto Seed und Checkpoint Resume sind getrennt getestet.
- Autostart startet niemals gleichzeitig einen PB7- und PB8-Optimize-Job;
  parallele manuelle Starts sind getestet und erlaubt.
- Laufende PB8-Jobs ueberstehen API-Neustart und PB8-Update.
- PB7/PB8 Configs, Queue-Snapshots, Logs, Results und Archive kollidieren nicht.
- EN/DE-Guides, Navigation, API-Lifecycle, Restart-Serial und Release Notes sind
  aktualisiert.
- Die vollstaendige Offline-Test-Suite ist gruen, bevor die Lieferung als fertig
  gemeldet wird.

# Spaetere PBv8-Menuepunkte

Nach Optimize und Pareto Explorer bleiben separat:

1. `PBv8 > Run`
2. `PBv8 > Strategy Explorer`

## Balance Calculator

Der Balance Calculator liegt als gemeinsame PBv7-/PBv8-Funktion unter
`Information`. V7- und V8-Backtest-Configs werden über denselben Draft-Flow
übergeben; der Calculator liest die Parameter über versionsspezifische
Config-Pfade.

# Umsetzungsreihenfolge

## Lieferung A: Installation

1. PB8-Pfade und Runtime-Status ergaenzen.
2. Lokales Install-/Update-Playbook bauen.
3. Remote-Master-Playbook bauen.
4. VPS-Manager-Status und Button anbinden.
5. Tests und VPS-Manager-Guides aktualisieren.

## Lieferung B: PB8-Config und Migration

1. Kleinen PB8-Helper und Client bauen.
2. PB8 Load/Prepare testen.
3. Migration und Report speichern.
4. Getrennte PB8-Config-Routen bauen.

## Lieferung C: PBv8 Backtest

1. Echten kleinen PB8-Backtest ausfuehren und Resultformat pruefen.
2. V8-Queue und Worker nach dem V7-Muster bauen.
3. V8-Resultparser bauen.
4. Minimale PBv8-Backtest-Seite bauen.
5. `Convert to V8` in PBv7 ergaenzen.
6. Navigation, Guides und Tests abschliessen.

## Lieferung D: PBv8 Optimize, Pareto und Archive

Status 2026-07-21: umgesetzt und nach dem abschliessenden Security-, Lifecycle- und Funktionsaudit mit der vollstaendigen Offline-Suite verifiziert (`4484 passed, 39 skipped`). Die installierte PB8-Runtime meldet drei Strategien, zwei Optimizer-Backends und 113 Optimize-Parameter; ein echter Chromium-DOM-Test rendert 84 Bounds fuer `trailing_martingale`, 58 fuer `ema_anchor` und 86 fuer `trailing_grid_v7` und schaltet Bot-Defaults und Bounds verlustfrei pro Strategie um. Die PB8-API deckt die gemeinsame Optimize-Oberflaeche einschliesslich nativer PB8-OHLCV-Pruefung, sicherem Pareto-Dash-Proxy, Queue-Recovery, suite-faehigen Pareto-/3D-Daten, transaktionalem Checkpoint-Resume und artifact-gesteuerten Result-Aktionen ab. PB7 und PB8 verwenden eine gemeinsam persistierte Optimize-Queue-Konfiguration fuer Autostart-CPU, CPU-Override und PBGui-Market-Data; beide Optimize-Autostart-Worker teilen genau einen globalen Prozess-Slot. Die Backtest-Queues verwenden ebenfalls eine gemeinsame Settings-Konfiguration und behandeln die CPU-Zahl als globales automatisches PB7/PB8-Prozesslimit. Der gemeinsame Backtest-Settings-Dialog erscheint sofort und aktualisiert verbindliche Host-Werte im Hintergrund, ohne Benutzereingaben zu ueberschreiben. PB8s Materialized-Cleanup koordiniert sich mit Root- und Run-Locks und bewahrt aktive, fremde oder nicht sicher lesbare Locks. Die Overrides werden nur auf Launch-Kopien angewendet. VPS Manager zeigt PB8-Version, Branch und Upstream-Status in Overview und Details.

1. PB8-Optimize-Metadaten und Config-Bundles implementieren.
2. Queue, Runner, Recovery und globale PB7/PB8-Autostart-Arbitrierung bauen.
3. Den bestehenden PB7-Optimize-Editor ueber einen V8-Adapter gemeinsam nutzen.
4. Results, Paretos, Presets, Explorer und Backtest-Handoffs versionsfaehig machen.
5. PB8 Backtest- und Optimize-Archive mit `config_version` vollstaendig anbinden.
6. Navigation, Guides, Regressionstests und komplette Offline-Suite abschliessen.

# Referenzen

- Passivbot v8.0.0 Release:
  `https://github.com/enarjord/passivbot/releases/tag/v8.0.0`
- Offizielle v8.0.0 Release Notes:
  `https://github.com/enarjord/passivbot/blob/v8.0.0/docs/release_notes_v8.0.0.md`
- Offizielle Installation:
  `https://github.com/enarjord/passivbot/blob/v8.0.0/docs/installation.md`
- Offizieller PB8-Config-Loader:
  `https://github.com/enarjord/passivbot/blob/v8.0.0/src/config/load.py`
- Offizielles Migrationstool:
  `https://github.com/enarjord/passivbot/blob/v8.0.0/src/tools/migrate_config_v7.py`
- Bestehendes PBGui-PB7-Inventar: `docs/pb7_integration_inventory.md`
