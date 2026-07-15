# Logging

Die Logging-Seite bietet einen Echtzeit-Log-Viewer für alle PBGui-Dienste.  
Logs werden live per WebSocket gestreamt — kein Neuladen der Seite erforderlich.

## Aufbau

- **Sidebar (links)**: Liste aller verfügbaren Log-Dateien in `data/logs/`, mit anpassbarer Breite auf dem Desktop
- **Toolbar**: Level-Filter, Zeilenanzahl, Versionsauswahl, Stream-Steuerung
- **Log-Bereich**: scrollbares Terminal-Ausgabefeld mit Suche und Hervorhebung

## Log-Datei auswählen

Auf einen Dateinamen in der Sidebar klicken, um ihn zu laden.  
Dateiname und Größe werden in der Toolbar angezeigt.  
Das Streaming startet automatisch — neue Zeilen werden live angehängt.

## Lines-Dropdown

Legt fest, wie viele Zeilen beim Öffnen oder Wechseln einer Datei geladen werden.  
Optionen: 200 / 500 / 1000 / 2000 / 5000 / 10000 / 25000 / All.  
Beim Ändern des Werts wird die Datei mit der neuen Anzahl neu geladen.

## Version-Dropdown

Erscheint, wenn die gewählte Log-Datei rotierte Backups hat (`.1`, `.old` usw.).  
- **Current**: aktive Datei mit Live-Streaming
- **.1 / .old / …**: archivierter Snapshot, einmalig geladen (kein Streaming)

Zurückschalten auf **Current** setzt das Live-Streaming fort.

## Level-Filter

Einzelne Log-Level ein- und ausblenden:

| Schaltfläche | Level    |
|--------------|----------|
| DBG          | DEBUG    |
| INF          | INFO     |
| WRN          | WARNING  |
| ERR          | ERROR    |
| CRT          | CRITICAL |

Zeilen, die keinem aktiven Level entsprechen, werden sofort ausgeblendet — ohne Nachlade-Vorgang.

## Suche

- Text im **Search**-Feld eingeben, um passende Zeilen zu filtern oder hervorzuheben
- **Filter**-Checkbox: aktiviert → nicht passende Zeilen ausblenden; deaktiviert → nur hervorheben
- **▲ / ▼**-Schaltflächen zum Springen zwischen Treffern
- **Preset**-Dropdown: häufige Suchmuster (Errors, Warnings, Traceback …)

## Stream-Steuerung

| Schaltfläche | Aktion                                                   |
|--------------|----------------------------------------------------------|
| ⏸ Pause     | Keine neuen Zeilen empfangen (Puffer bleibt erhalten)    |
| ▶ Stream    | Live-Streaming ab aktueller Position fortsetzen          |
| 🗑 Clear     | Anzeigepuffer leeren (Datei wird nicht gelöscht)         |
| ⬇ Download  | Aktuellen Pufferin-halt als Textdatei speichern          |
| ## Lines    | Zeilennummern ein-/ausblenden                            |

## Einstellungen

Auf **⚙ Settings** in der Sidebar klicken, um die Log-Rotation zu konfigurieren:

- **Standard-Rotation**: maximale Dateigröße (MB) und Anzahl der Backup-Dateien für alle Dienste
- **Einzelne Log-Rotation**: Größe und Backup-Anzahl pro Dienst überschreiben
- **Managed Logs**: Größe und Backup-Anzahl für dynamische Log-Familien wie
  API-Konsole, Jobs, Backtests, Optimierungen, VPS-Manager-Runs,
  OHLCV-Preloads, Monitor-Agent-Livedaten, Pareto-Sessions und API-Handoff

Änderungen werden beim nächsten Log-Schreibvorgang oder vor dem Öffnen des
nächsten verwalteten Transkripts gelesen. Die Log-Rotation besitzt bewusst
keinen Watcher und benötigt keinen Dienstneustart.

## Fehlerbehebung

- **Keine Log-Dateien aufgelistet**: Sicherstellen, dass PBGui-Dienste mindestens einmal gestartet wurden
- **Streaming stoppt**: API-Server-WebSocket-Verbindung unterbrochen — der Viewer verbindet sich automatisch neu
- **„All" ist langsam**: Sehr große Dateien können einen Moment brauchen; bei großen Logs ein Zeilenlimit setzen

---

## Wo finde ich was?

Alle Log-Dateien liegen im kanonischen PBGui-Verzeichnis `data/logs/`. Der Pfad
ist am PBGui-Installationsverzeichnis verankert und hängt nicht vom aktuellen
Arbeitsverzeichnis eines Prozesses ab. In der Sidebar kann jede Datei direkt
geöffnet werden.

PBGui serialisiert paralleles Schreiben, Rotieren und Leeren über Threads und
Prozesse hinweg. Rotationseinstellungen werden atomar in der PBGui-Datei
`pbgui.ini` gespeichert. Eine Einstellung gilt für die physische Log-Datei;
alle in `PBGui.log` gruppierten Helfer verwenden daher dieselbe Regel.

PBGui-eigene Transkripte verwenden eigene Unterverzeichnisse unter demselben
Root:

```text
data/logs/jobs/
data/logs/backtests/
data/logs/optimizes/
data/logs/vps-manager/
data/logs/ohlcv-preloads/
data/logs/monitor-agent/
```

Die Managed-Logs-Einstellungen gelten bereits, bevor eine Familie ihre erste
Datei erzeugt. Child-Prozess-Logs werden nur vor dem Öffnen einer neuen
Capture-Datei rotiert, damit keine Datei umbenannt wird, solange der
Child-Prozess noch einen offenen Deskriptor besitzt.

PB7-native Botlogs bleiben im PB7-eigenen `logs/`-Verzeichnis; Legacy-
Passivbot-stderr bleibt im Runtime-Verzeichnis der Instanz. PBGui kann diese
Dateien anzeigen, übernimmt aber weder Speicher- noch Rotationsverantwortung.

### Automatische Migrationsbereinigung

Beim ersten API-Start nach einem Update entfernt eine prozesssichere
Startup-Migration ausschließlich explizit stillgelegte PBGui-Lognamen und alte
`income_other_*.json`-Diagnosen. Der Abschluss wird atomar in
`data/state/startup_migrations.json` gespeichert, sodass jeder Master die
Migration genau einmal ausführt. Fehlgeschlagene Migrationen bleiben für den
nächsten API-Start offen. Symlinks und Pfade außerhalb der freigegebenen Roots
werden nie entfernt.

### Sicherheit und Kontext

Die Logging-Seite authentifiziert Browser ausschließlich über das
Same-Origin-HttpOnly-Session-Cookie. Session-Tokens werden nicht in HTML oder
JavaScript gerendert.

Der zentrale Logger entfernt übliche Zugangsdaten aus Meldungen, Tags, Codes,
URLs, Exceptions, Tracebacks und verschachtelten Metadaten. Dazu gehören
Passwörter, API-Keys und Secrets, Access-/Session-/Refresh-Tokens,
Authorization- und Cookie-Header, sensible URL-Parameter und Private-Key-Blöcke.
Redaction ist nur die letzte Schutzschicht; bekannte Secrets dürfen weiterhin
nicht absichtlich geloggt werden.

Operationale Ereignisse können strukturierten JSON-Kontext am Zeilenende
enthalten:

- `request_id` und `operation` bei API-Requests
- `host` bei Remote-/VPS-Aktionen
- `instance` oder `user` bei botbezogenen Aktionen

API-Antworten enthalten `X-Request-ID`, damit Fehler ohne Offenlegung einer
Session-ID den passenden Logs zugeordnet werden können.

### Log-Zuordnung

PBGui verwendet drei Stufen:

1. Eigenständige Daemons schreiben eigene Service-Logs.
2. Datenpipelines und detached Jobs verwenden eigene Pipeline-Logs oder
   dokumentierte Transkripte.
3. API-/UI-Helfer ohne eigenen Lifecycle teilen sich `PBGui.log`.

Maschinenlesbare Worker-Ausgaben, Installer-/Wartungs-CLI-Ausgaben, rohes
Child-stderr und sichtbare VPS-/Job-Transkripte sind beabsichtigte Ausnahmen.
Sie gelten nicht als alternative Anwendungslogger und werden durch Policy-Tests
abgesichert.

### PBGui.log

Enthält Meldungen gruppierter API- und GUI-Hilfskomponenten:

| Komponente | Was steht dort |
|------------|---------------|
| VPSManager | VPS-Verbindungen und Task-Koordination |
| Config | Fehler in Konfigurationshelfern |
| ParetoDataLoader | Pareto-Ergebnisse laden |
| Status | Ereignisse der Status-Helfer |
| HyperliquidAWS | Hyperliquid-AWS-Integration |
| API-/UI-Helfer | Authentifizierung, Live-Sessions, Benutzer, API-Key-State, Logging, Balance, Coin Data, Dashboard, Services, V7-Instanzen, Market Data und PB7-OHLCV-Aktionen |

### Eigene Log-Dateien

| Datei | Dienst | Was steht dort |
|-------|--------|---------------|
| `PBCluster.log` | PBCluster | Cluster-Sync-Daemon und Peer-Sync-Diagnose |
| `PBRun.log` | PBRun | Live-Bot Start/Stop, Order-Loop |
| `PBCoinData.log` | PBCoinData | CMC-Daten-Updates, Symbol-Listen |
| `VPSMonitor.log` | VPS Monitor | SSH-Verbindungen, Host-Metriken, Service-Auto-Heal |
| `PBApiServer.log` | PBAPIServer | FastAPI-Start, REST/WebSocket-Requests |
| `Database.log` | Database | DB-Abfragen, Verbindungsfehler |
| `Exchange.log` | Exchange | Marktdaten, Symbol-Infos, CCXT-Fehler |
| `PBData.log` | PBData | OHLCV-Download, Marktdaten-Pipeline |
| `SSH.log` | SSH-Pool | AsyncSSH-Verbindungen und Host-Key-Diagnose |
| `tradfi_sync.log` | TradFi Sync | TradFi-Symbol-Mapping und Synchronisierung |

Weitere Exchange-Downloader, Queues und detached Pipelines können eigene Logs
oder Job-Transkripte besitzen. `OptimizeQueueAPI` bleibt absichtlich separat
und wird nicht in `PBGui.log` gruppiert.
