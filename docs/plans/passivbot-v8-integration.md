# Plan: Passivbot v8 in PBGui integrieren

**Status: vorgeschlagen.** Passivbot v8.0.0 ist ein inkompatibler neuer
Runtime- und Config-Vertrag. Die Integration wird additiv neben PB7 aufgebaut;
PB7 wird nicht in-place auf PB8 aktualisiert.

**Umsetzungsstand:** Die erste Sicherheitsmassnahme ist implementiert, aber
nicht auf entfernte Hosts ausgerollt. Alle PB7-Installer und Standard-Updates
verwenden den exakten bekannten V7-Commit
`befaa9b7aa89e00ee55704221b39621ad700ac36`. Branch-Switches pruefen den
Ziel-Ref vor dem Checkout und lehnen andere Major-Versionen ab.

## Entscheidung

1. PB7 und PB8 erhalten getrennte Checkouts, Python-Umgebungen, Configs,
   Queues, Ergebnisse, Logs, Caches und Cluster-Identitaeten.
2. Auf dem Master ist der Parallelbetrieb vorgesehen. Standardpfade sind
   `pb7`/`venv_pb7` und `pb8`/`venv_pb8`.
3. Auf kleinen VPS mit etwa 10 GB Disk und 1 GB RAM wird kein Parallelbetrieb
   angeboten. Bestehende kleine PB7-Nodes bleiben PB7-Nodes, bis ihre Workloads
   auf neu installierte, dedizierte PB8-Nodes migriert werden.
4. Fuer PB8 werden eigene Run-, Backtest- und Optimize-Oberflaechen sowie eigene
   serverseitig aus PB8 erzeugte v8.0.0-Defaults angeboten. PB7-Formulare und
   PB7-Defaults werden nicht wiederverwendet.
5. Das offizielle Migrationstool wird als kontrollierter Import angeboten. Eine
   Migration erzeugt eine deprecated `trailing_grid_v7`-Kompatibilitaetsconfig,
   keine native v8-Strategie und keine automatisch live-faehige Config.
6. PBGui importiert PB7- und PB8-Module mit identischen Namen nicht gemeinsam in
   den API-Prozess. Generationseigene Schema-, Migrations- und Metadatenzugriffe
   laufen ueber den jeweils konfigurierten Interpreter in einem isolierten
   Helper-Subprozess.
7. Eine Passivbot-Generation wird pro Slave-Node zugewiesen. Dual-faehige grosse
   Hosts bleiben eine Ausnahme, nicht der Standard fuer die Bot-Flotte.

## Sofortige Schutzmassnahme

Upstream `origin/master` zeigt seit v8.0.0 auf PB8. Die bestehenden Installer-
und Update-Playbooks klonen oder aktualisieren jedoch `master` in den Pfad
`pb7` und bauen danach dieselbe `venv_pb7` neu auf. Damit kann ein normales
PB7-Update derzeit unbeabsichtigt:

1. PB8 in den PB7-Checkout ziehen,
2. die PB7-Umgebung und das Rust-Modul ueberschreiben,
3. alle PB7-Bots stoppen,
4. PB8 mit nicht migrierten PB7-Configs starten,
5. einen vollstaendigen Rollback erschweren.

Vor jeder PB8-Funktion muss deshalb der bestehende PB7-Installations- und
Updatepfad auf einen bekannten PB7-Commit oder einen gepflegten PB7-Branch
gepinnt werden. Update- und Branch-Aktionen muessen die Major-Version pruefen
und bei einem Wechsel von 7 auf 8 hart abbrechen.

## Verifizierte PB8-Vertraege

### Installation und Entry Points

PB8.0.0 unterstuetzt Python 3.12 und pinnt Rust 1.90.0. Die kanonische
Installation erfolgt in einer eigenen Umgebung:

```bash
python3.12 -m venv venv_pb8
venv_pb8/bin/python -m pip install --upgrade pip
venv_pb8/bin/python -m pip install -e "./pb8[full]"
```

Auf reinen Live-Slaves reicht das Live-Profil:

```bash
venv_pb8/bin/python -m pip install -e ./pb8
```

Die bevorzugten Entry Points sind:

```text
venv_pb8/bin/passivbot live <config>
venv_pb8/bin/passivbot backtest <config>
venv_pb8/bin/passivbot optimize <config>
venv_pb8/bin/passivbot download <config>
```

Die direkten Skripte `src/main.py`, `src/backtest.py` und `src/optimize.py`
existieren weiterhin. PBGui sollte fuer PB8 dennoch den installierten CLI-Pfad
verwenden, damit Installation, Diagnose und Migration denselben Paketvertrag
nutzen.

### Config-Schema

PB8 verwendet `config_version: "v8.0.0"` und die Top-Level-Bereiche
`backtest`, `bot`, `coin_overrides`, `live`, `logging`, `monitor` und
`optimize`. Die Strategieparameter sind nicht mehr flach, sondern nach
Strategie geschachtelt:

```text
live.strategy_kind
bot.long.strategy.<strategy_kind>.*
bot.short.strategy.<strategy_kind>.*
optimize.bounds.long.strategy.<strategy_kind>.*
optimize.bounds.short.strategy.<strategy_kind>.*
```

PB8.0.0 bietet `trailing_martingale`, `ema_anchor` und das deprecated
`trailing_grid_v7`. Der native PB8-Default ist `trailing_martingale`. Alte
flache PB7-Felder duerfen daher nicht in einem wiederverwendeten PB7-Editor
bearbeitet oder stillschweigend als PB8 gespeichert werden.

### Offizielle Migration

Der verifizierte Befehl lautet:

```bash
passivbot tool migrate-config-v7 \
  config_v7.json \
  config_v8.json \
  --report migration_report.json
```

Die Migration:

- erkennt nur passende PB7-Trailing-Grid-Configs,
- setzt `live.strategy_kind` auf `trailing_grid_v7`,
- protokolliert neue PB8-Defaults, manuell zu pruefende Felder und verworfene
  nicht unterstuetzte Felder,
- schreibt standardmaessig keine Ausgabe, solange manuelle oder verworfene
  Felder offen sind,
- kann mit `--allow-manual-review-output` eine unsichere Best-Effort-Ausgabe
  erzeugen, die ausdruecklich nicht live-ready ist.

PBGui darf den unsicheren Modus nicht automatisch verwenden. Ein Import wird
als PB8-Draft neben der PB7-Quelle gespeichert und ueberschreibt nie die
PB7-Config. Der Report bleibt dauerhaft mit dem Draft verknuepft.

### Datenpfade

PB8 nutzt unter anderem:

```text
caches/ohlcvs/
caches/hlcvs_data/
caches/ohlcv/
backtests/
optimize_results/
logs/
monitor/
```

Das moderne `caches/ohlcvs`-Layout existierte bereits in spaeten PB7-Versionen,
ist aber nicht als gemeinsam beschreibbarer Cross-Major-Store garantiert.
PBGui teilt deshalb keine schreibbaren Passivbot-Caches zwischen PB7 und PB8.

## Impact auf PBGui

### Gemessener Blast-Radius

Der aktualisierte GitNexus-Index zeigt fuer zentrale PB7-Vertraege:

| Symbol | Direkte Nutzer | Betroffene Symbole | Prozesse | Risiko |
| --- | ---: | ---: | ---: | --- |
| `pb7_runtime_status` | 6 | 72 | 13 | CRITICAL |
| `load_pb7_config` | 47 | 83 | 12 | CRITICAL |
| `_apply_v7` | 1 | 121 | 8 | CRITICAL |
| `RunV7` | 3 | 16 | 0 | LOW |
| `BacktestWorker._launch_backtest` | 3 | 5 | 0 | LOW |
| `OptimizeWorker` | 2 | 12 | 0 | LOW |
| `run_local_master_install` | 2 | 3 | 0 | LOW |

Die hohe Gefahr liegt nicht im einzelnen Prozessstart, sondern in gemeinsam
genutzter Runtime-Erkennung, Config-Normalisierung und Cluster-Zustandslogik.
Diese Bereiche werden deshalb nicht zuerst verallgemeinert. PB8 entsteht als
separater Vertrag; gemeinsame Abstraktionen werden erst nach funktionierender
PB8-Paritaet extrahiert.

### Betroffene Bereiche

| Bereich | Heutige PB7-Annahme | PB8-Ziel |
| --- | --- | --- |
| Runtime-Config | nur `pb7dir`, `pb7venv` | zusaetzlich `pb8dir`, `pb8venv` |
| API-Imports | globale Module `config`, `utils`, `passivbot_rust` | generationseigene Helper-Subprozesse |
| Live | `data/run_v7`, `RunV7`, `src/main.py` | `data/run_v8`, `RunV8`, PB8 CLI |
| Backtest | `data/bt_v7`, `bt_v7_queue`, PB7 results | getrennte PB8 Configs, Queue und Results |
| Optimize | `data/opt_v7`, `opt_v7_queue`, PB7 Pareto | getrennte PB8 Configs, Queue und Parser |
| Cluster | V7-Operationen und ein `instances`-Namensraum | zusaetzliche V8-Operationen und Identitaet |
| Prozesse | Erkennung ueber `main.py`/`backtest.py` | absolute Runtime- und Config-Pfade |
| API Keys | ein `<pb7dir>/api-keys.json` | getrennte Materialisierung je Runtime |
| Logs | PB7-Pfade und V7-Fehlerlog | generationseigene, validierte Log-Pfade |
| Monitoring | nur `pb7v`, PB7 Schema und Commit | PB8 Version, Schema, Commit und Capability |
| Installer | ein Checkout und ein Venv | selektierbare PB7/PB8-Profile |
| Updates | ein Passivbot-Update | strikt getrennte Major-Version-Aktionen |
| Frontend | `/api/v7` und PBv7-Seiten | `/api/v8` und eigenstaendige PBv8-Seiten |

## Zielarchitektur

### Runtime-Registry

PBGui erhaelt eine kleine generationseigene Runtime-Beschreibung mit mindestens:

```text
generation
checkout_dir
python_path
cli_path
config_adapter
run_root
backtest_root
optimize_root
result_roots
log_roots
```

PB7 behaelt zunaechst seine bestehenden Funktionen. PB8 verwendet die neue
Beschreibung ab dem ersten Schritt. Erst nach abgeschlossener PB8-Integration
kann entschieden werden, ob PB7 ebenfalls ohne Verhaltensaenderung auf die
Registry umgestellt wird.

### Prozessisolierter PB8-Adapter

Ein schmaler PB8-Helper wird mit `pb8venv` ausgefuehrt und tauscht JSON ueber
stdin/stdout aus. Er kapselt:

- Runtime- und Schema-Status,
- kanonische Default-Config,
- Load, Normalize und Validate,
- Bot-, Bounds-, Metric- und Override-Metadaten,
- V7-zu-V8-Migration samt Report.

Der Helper bekommt begrenzte Laufzeit und Ausgabegroesse. Secrets werden weder
ueber Kommandozeilenargumente noch Logs transportiert. Diese Grenze verhindert
Kollisionen, weil PB7 und PB8 beide `config`, `passivbot`, `passivbot_rust` und
den CLI-Namen `passivbot` bereitstellen.

### Persistenz

PB8 verwendet von Anfang an eigene Pfade:

```text
data/run_v8/<instance>/
data/backup/v8/<instance>/
data/bt_v8/<config>/
data/bt_v8_queue/
data/opt_v8/
data/opt_v8_queue/
data/logs/backtests_v8/
data/logs/optimizes_v8/
<pb8dir>/backtests/pbgui/
<pb8dir>/optimize_results/
```

Drafts, Browser-View-State, WebSockets, Archive-Metadaten, Preload-Jobs und
Result-Caches enthalten die Generation explizit. Gleiche Instanz- oder
Config-Namen duerfen in PB7 und PB8 gleichzeitig existieren.

### Gemeinsame Ressourcen

| Ressource | Entscheidung |
| --- | --- |
| Python 3.12 und OS-Buildpakete | gemeinsam |
| Rust 1.90 Toolchain und Cargo Registry | gemeinsam |
| Checkout, Venv und Rust Build-Artefakt | getrennt |
| PBGui Coin-Mappings | gemeinsam, mit Generation-Capability |
| PBGui `data/ohlcv` | gemeinsam nur als explizite Quelle |
| Passivbot writable caches | getrennt |
| Configs, Results, Logs und Monitor-State | getrennt |
| Credentials | eine PBGui-Quelle, getrennte Runtime-Dateien |
| Exchange-Account | nie gleichzeitig durch PB7 und PB8 steuern |

Auf dem Master soll grosse historische Rohdaten nicht verdoppelt werden.
PB7 und PB8 koennen PBGui `data/ohlcv` ueber einen generationseigenen Adapter
lesen; materialisierte HLCV-Caches und Resultate bleiben jeweils lokal im
PB7- bzw. PB8-Checkout.

### Cluster-Modell

Wegen des CRITICAL Blast-Radius von `_apply_v7` wird der bestehende V7-Vertrag
nicht in-place umgebaut. Zunaechst entstehen additive `V8_OPS`, ein separater
`instances_v8`-Desired-State und `materialize-v8`:

- Node-Capabilities: installierte Generationen, Version, Config-Schema,
  Interpreter und freier Speicher,
- generationseigene Manifest-Hashes, Tombstones und Backups,
- generationseigene API-Key-Materialisierung,
- Ablehnung einer V8-Zuweisung an einen Node ohne passende Capability,
- Ablehnung einer gleichzeitigen PB7/PB8-Aktivierung desselben Accounts,
- exakte Prozesssteuerung ueber Checkout-, Interpreter- und Config-Pfad.

Ein spaeteres gemeinsames `instances`-Schema ist eine optionale Nacharbeit und
kein Bestandteil der ersten PB8-Produktionsfreigabe.

## Betriebsmodell fuer Master und Slaves

### Messwerte

Die lokale Master-Installation hat ausreichend Reserve fuer einen zweiten
Checkout und ein zweites Full-Venv. Die vorhandenen grossen Datenmengen liegen
vor allem in historischen Daten, PB7-Caches und Optimize-Ergebnissen; sie
sollen nicht pauschal dupliziert werden.

Vier read-only gepruefte kleine Slaves besitzen jeweils etwa 8.7 GB nutzbaren
Root-Speicher und 848 MB RAM. Verfuegbar waren nur etwa 0.9 bis 1.8 GB Disk und
59 bis 69 MB RAM; alle Nodes nutzten bereits deutlich Swap. Ein zusaetzliches
Live-Venv plus Checkout, Cache, Logs und Update-Reserve benoetigt geschaetzt
0.75 bis 1.15 GB, ein weiterer Bot-Prozess typischerweise etwa 50 bis 250 MB
RAM. Damit fehlt sowohl Disk- als auch RAM-Sicherheitsreserve.

### Empfohlene Topologie

| Hostklasse | PB7 + PB8 parallel | Empfehlung |
| --- | --- | --- |
| Master mit grosser Disk/RAM-Reserve | ja | PB7 produktiv, PB8 Full-Venv fuer Migration, Backtest und Optimize |
| Kleiner 10-GB-/1-GB-Slave | nein | genau eine Generation; bestehend vorerst PB7 |
| Neuer PB8-Live-Slave | nicht erforderlich | mindestens 20-30 GB Disk und 2 GB RAM, bevorzugt 4 GB |
| Grosser Spezial-Slave | technisch ja | nur bei begruendetem Bedarf, standardmaessig generationsexklusiv |

Die bevorzugte Migration ist daher nicht, PB8 auf allen vorhandenen Slaves
zusaetzlich zu installieren. Stattdessen werden ein oder mehrere neue PB8-Nodes
aufgesetzt. Accounts werden kontrolliert von einem PB7-Node auf einen PB8-Node
verschoben. Kleine Nodes koennen spaeter sauber als Single-Version-PB8-Node
neu installiert werden, bleiben mit 1 GB RAM aber nur eingeschraenkt geeignet.

### Account-Umschaltung und Rollback

1. PB8-Config migrieren oder nativ erstellen und Report abschliessen.
2. PB8-Backtest und fachlichen Vergleich ausfuehren.
3. PB8 auf einem getrennten Test-Account oder ohne Live-Aktivierung pruefen.
4. PB7-Instanz stoppen und offene Orders/Positionen verifizieren.
5. Erst danach PB8 fuer denselben Account aktivieren.
6. Fuer Rollback PB8 vollstaendig stoppen und Zustand erneut verifizieren.
7. Danach die unveraenderte PB7-Instanz auf dem erhaltenen PB7-Node starten.

PB7 und PB8 duerfen denselben Account nie ueberlappend steuern. PB8 erkennt
konkurrierende Passivbot-Orders und stoppt nach wiederholter Erkennung; PBGui
soll die Fehlkonfiguration bereits vor dem Start verhindern.

## Integrationsphasen

## Phase 0: PB7 einfrieren und Major-Version absichern

### Aufgaben

1. Alle PB7-Installer und -Updater auf einen expliziten PB7-Ref umstellen.
2. Vor Pull, Switch und Rebuild die erwartete Major-Version pruefen.
3. Einen Major-Wechsel in `pb7`/`venv_pb7` mit klarer Fehlermeldung blockieren.
4. PB7-Update, PB8-Update und kombiniertes Update als getrennte Aktionen
   modellieren.
5. Update- und Rollback-Metadaten um Checkout-Commit, Paketversion,
   Rust-Fingerprint und Interpreter erweitern.

### Exit-Kriterien

- Kein bestehender PB7-Updatepfad kann `origin/master` ungeprueft nach v8 folgen.
- Ein fehlgeschlagener PB8-Installationsversuch veraendert PB7 nicht.
- PB7-Update und -Rollback sind mit dem gepinnten Ref getestet.

## Phase 1: PB8 Runtime, Installer und isolierter Config-Adapter

### Aufgaben

1. `pb8dir` und `pb8venv` als restart-required Settings einfuehren.
2. Local-, Remote-Master- und VPS-Installer um getrennte PB8-Profile erweitern.
3. Auf dem Master `.[full]`, auf Live-Slaves das Live-Profil installieren.
4. PB8 auf einen Release-Tag oder Commit pinnen, nicht auf bewegliches `master`.
5. PB8-Version, Schema, CLI, Rust-Import und Full-Abhaengigkeiten pruefen.
6. Den JSON-Helper fuer Defaults, Validierung, Metadaten und Migration bauen.
7. Uninstaller, Cleanup und Disk-Preflight generationseigen erweitern.

### Exit-Kriterien

- PB7 und PB8 sind gleichzeitig installiert und unabhaengig diagnostizierbar.
- Beide Rust-Module funktionieren in ihren eigenen Venvs.
- Der API-Prozess importiert PB8 nicht direkt.
- Install, Update und Remove einer Generation veraendern die andere nicht.

## Phase 2: PB8 Configs und Migration UI

### Aufgaben

1. Eigene `/api/v8`-Routen und PBv8-Editorseite anlegen.
2. Native v8.0.0-Defaults aus dem PB8-Helper laden.
3. Strategieauswahl und geschachtelte Parameter fuer `trailing_martingale`,
   `ema_anchor` und `trailing_grid_v7` abbilden.
4. Unbekannte PB8-Felder beim Roundtrip erhalten.
5. Migration einer ausgewaehlten PB7-Config in einen neuen PB8-Draft anbieten.
6. Reportbereiche `inserted_v8_defaults`, `manual_review_fields` und
   `dropped_unsupported_fields` in English darstellen.
7. Speichern/Aktivieren blockieren, solange harte Reportpunkte offen sind.
8. PB7-Quelle, PB8-Draft und Report revisionssicher miteinander verknuepfen.

### Exit-Kriterien

- Native PB8-Configs und migrierte Kompatibilitaetsconfigs sind klar getrennt.
- Kein PB8-Speichervorgang schreibt nach `data/run_v7`.
- Eine Migration ueberschreibt nie die PB7-Quelle.
- Fehlerhafte oder manuell zu pruefende Migrationen werden nicht live aktiviert.

## Phase 3: PB8 Backtest

### Aufgaben

1. Eigene Backtest-Config-, Queue-, Log-, PID- und Result-Pfade einfuehren.
2. PB8 ueber `venv_pb8/bin/passivbot backtest` starten.
3. Prozesswiederaufnahme ueber absolute Pfade und Generation absichern.
4. PBGui OHLCV als explizite Quelle anbinden; PB8-Caches getrennt halten.
5. PB8-Ergebnisformat, Statusmeldungen, Archive und Retest-Vertrag separat
   validieren.
6. Einen Vergleichsworkflow PB7 versus migriertes PB8 `trailing_grid_v7`
   anbieten, ohne Ergebnisgleichheit vorauszusetzen.

### Exit-Kriterien

- PB7- und PB8-Backtests laufen parallel ohne Queue- oder Result-Kollision.
- Stop, Restart und Cleanup treffen nur die gewaehlte Generation.
- Migrierte Configs koennen vor jeder Live-Freigabe reproduzierbar getestet
  werden.

## Phase 4: PB8 Optimize und Pareto

### Aufgaben

1. Eigene Optimize-Configs, Queue, Logs und Result-Roots einfuehren.
2. Bounds und Scoring ausschliesslich aus PB8-Metadaten erzeugen.
3. PB8 ueber `venv_pb8/bin/passivbot optimize` starten.
4. Result-, Pareto- und Seed-Formate gegen PB8.0.0 separat implementieren.
5. PB7 Pareto Explorer und PB7 `pareto_dash.py` nicht stillschweigend fuer PB8
   wiederverwenden.
6. CPU-, Disk- und Cache-Preflights generationseigen ausgeben.

### Exit-Kriterien

- PB7 und PB8 Optimize koennen unabhaengig verwaltet werden.
- Kein PB8-Ergebnis wird vom PB7-Parser aufgrund aehnlicher Dateinamen
  fehlklassifiziert.
- Aus einem PB8-Ergebnis entsteht eine valide PB8-Config.

## Phase 5: PB8 Live auf dem Master

### Aufgaben

1. `RunV8` mit PB8 CLI, eigenem Working Directory und `data/run_v8` einfuehren.
2. Runtime-Dateien, Coin Overrides, Forced Modes und Logging gegen PB8.0.0
   modellieren.
3. Prozessdetektoren in PBRun, API, Monitor und Logs generationseigen machen.
4. API Keys aus der PBGui-Quelle getrennt nach `<pb7dir>` und `<pb8dir>`
   materialisieren.
5. Einen Account-Ownership-Guard vor Start und Aktivierung einfuehren.
6. PB8-Version, Config-Schema, Commit, Python und Prozessstatus publizieren.
7. Update und Stop nur ueber exakte PB8-Pfade ausfuehren.

### Exit-Kriterien

- PB7 und PB8 laufen auf dem Master gleichzeitig mit verschiedenen Accounts.
- PB7 Update/Restart beeinflusst PB8 nicht und umgekehrt.
- Gleichzeitige PB7/PB8-Aktivierung desselben Accounts wird blockiert.
- API-Neustart kann beide Runner-Zustaende korrekt rekonstruieren.

## Phase 6: Cluster und dedizierte PB8-Slaves

### Aufgaben

1. V8 Desired State, Operationen, Materialisierung, Tombstones und Backups
   additiv implementieren.
2. Node-Capabilities und Generation-Zuweisung im VPS Manager anzeigen.
3. PB8-Live-Installer fuer dedizierte Slaves bereitstellen.
4. Disk- und RAM-Preflight vor Installation und Zuweisung erzwingen.
5. Remote Logs, Monitor, Start/Stop und Update generationseigen absichern.
6. Einen Canary-Node und einen Canary-Account migrieren.
7. Rollback auf einen unveraenderten PB7-Node praktisch testen.

### Exit-Kriterien

- Ein PB8-Manifest kann niemals in `run_v7` materialisiert werden.
- Ein Node ohne PB8-Capability akzeptiert keine PB8-Zuweisung.
- Kleine Nodes werden nicht versehentlich dual installiert.
- Canary-Betrieb und Rollback sind dokumentiert und erfolgreich getestet.

## Phase 7: Produktionsfreigabe und Nacharbeit

### Aufgaben

1. English/German Guides fuer PB8 Run, Backtest, Optimize und Migration
   veroeffentlichen.
2. Betriebsdokumentation fuer Node-Groessen, Account-Umschaltung und Rollback
   ergaenzen.
3. PB7/PB8-Telemetrie, Logs, Diskwachstum und Fehler fuer eine definierte
   Canary-Periode beobachten.
4. Erst danach weitere Accounts und Nodes schrittweise migrieren.
5. Gemeinsame Abstraktionen nur dort extrahieren, wo PB7- und PB8-Vertraege
   nachweislich identisch sind.
6. PB7-Rueckbau als separates spaeteres Projekt behandeln; keine automatische
   Frist aus der PB8-Einfuehrung ableiten.

### Exit-Kriterien

- Mindestens ein kompletter PB8 Run/Backtest/Optimize-Lebenszyklus ist
  produktionsnah validiert.
- PB7 bleibt unveraendert nutzbar und rollback-faehig.
- Monitoring zeigt Generation, Version und Node-Capability eindeutig.

## Teststrategie

### Offline-Vertragstests

- Runtime-Erkennung fuer PB7 und PB8,
- Helper-Protokoll, Timeout, ungueltiges JSON und grosse Ausgabe,
- PB8 Default-, Load-, Save- und unbekannte-Felder-Roundtrips,
- Migrationsreports mit Erfolg, Defaults, Manual Review und Dropped Fields,
- gleiche Instanznamen in `run_v7` und `run_v8`,
- getrennte Backtest-/Optimize-Queues und Resultate,
- Prozessdetektor-Kollisionen bei gleichnamigen `main.py`/`backtest.py`,
- getrennte API-Key-Materialisierung,
- Cluster-Manifest, Tombstone, Backup und Materialisierung je Generation,
- Update-/Stop-Kommandos duerfen die jeweils andere Generation nicht treffen,
- Disk-/RAM-Preflight fuer kleine Nodes.

### Externe PB8-Integration

PB8-abhaengige Tests erhalten wie bestehende externe PB7-Tests einen expliziten
Marker und laufen gegen einen gepinnten PB8.0.0-Checkout in temporaeren
Verzeichnissen. Standard-Pytest bleibt offline und schreibt keine realen
Runtime-Daten.

### Manuelle Freigabe

1. Native PB8-Default-Config erstellen, speichern und erneut laden.
2. Eine representative PB7-Config migrieren und jeden Reportpunkt pruefen.
3. PB7/PB8-Backtestvergleich mit derselben historischen Quelle ausfuehren.
4. PB8 Optimize starten, stoppen, fortsetzen und Resultat laden.
5. PB7 und PB8 auf dem Master mit getrennten Test-Accounts parallel starten.
6. PB8 auf Canary-Slave verschieben und Remote-Logs/Monitoring pruefen.
7. Canary kontrolliert auf PB7 zurueckrollen.

## Hauptrisiken und Gegenmassnahmen

| Risiko | Gegenmassnahme |
| --- | --- |
| PB7 wird versehentlich auf PB8 aktualisiert | PB7 sofort pinnen und Major-Guard einfuehren |
| Python-/Rust-Modulkollision | getrennte Venvs und Helper-Subprozess |
| Config wird still falsch interpretiert | eigene PB8-Pipeline und serverseitige Defaults |
| Migration vermittelt falsche Sicherheit | Reportpflicht, kein Auto-Live, Backtest-Gate |
| PB7/PB8 steuern denselben Account | Ownership-Guard und kontrollierter Handover |
| Prozess wird falscher Generation zugeordnet | absolute Interpreter-, Script- und Config-Pfade |
| Cluster ueberschreibt gleichnamige Instanz | additive V8-Identitaet und getrennte Roots |
| Kleine VPS laufen voll | kein Dualbetrieb, Preflight, dedizierte PB8-Nodes |
| Historische Daten verdoppeln Diskbedarf | PBGui OHLCV read-only teilen, writable Caches trennen |
| Rollback setzt nur Git zurueck | komplette Runtime als Rollback-Einheit erhalten |

## Empfohlene Umsetzungsreihenfolge

Die Freigabereihenfolge lautet bewusst:

```text
PB7 absichern
-> PB8 installieren und Config isolieren
-> Migration UI
-> Backtest
-> Optimize
-> Live auf Master
-> Cluster/Canary-Slave
-> schrittweiser Produktionsrollout
```

Damit werden inkompatible Configs zuerst offline sichtbar und getestet. Live-
und Cluster-Aenderungen folgen erst, wenn PB8-Erzeugung, Migration, Backtest und
Optimize belastbar sind.

## Referenzen

- Passivbot v8.0.0 Release:
  `https://github.com/enarjord/passivbot/releases/tag/v8.0.0`
- Offizielle v8.0.0 Release Notes und Migration:
  `https://github.com/enarjord/passivbot/blob/v8.0.0/docs/release_notes_v8.0.0.md`
- Offizielle Installation:
  `https://github.com/enarjord/passivbot/blob/v8.0.0/docs/installation.md`
- Lokal verifizierter PB8-Commit:
  `a0897f83932db5e6888c1c96f8f1c668d452013f`
- Bestehendes PBGui PB7-Inventar: `docs/pb7_integration_inventory.md`
