# VPS Monitor

Der VPS Monitor ist das Echtzeit-Operations-Dashboard für alle konfigurierten VPS-Hosts. PBAPIServer sendet den Status über einen Cookie-authentifizierten WebSocket. Dadurch aktualisiert sich die Seite ohne Polling und ohne die Browser-Session in HTML, JavaScript, URLs oder WebSocket-Parametern offenzulegen.

## Was du überwachen kannst

- SSH-Verbindungsstatus jedes Hosts
- Live-Metriken für CPU, RAM, Disk, Swap und Bot-Prozesse
- Service-Status für PBCluster, PBRun, PBData, PBCoinData und PBMonitorAgent
- Bot-Instanzen und Synchronisierungsdetails über alle Hosts
- Live-Logs für Services und Bots
- Monitor-Agent-Heartbeat, Cache-Dateistatus und Collector-Fehler

## Monitor-Agent-Datenquelle

Auf jedem VPS läuft `pbgui-monitor-agent.service`. Der Service misst den lokalen Host einmal und schreibt Cache-Dateien für alle verbundenen PBGui-Master. Der VPS Monitor verwendet ausschließlich diesen Monitor-Agent-Cache. Bei fehlenden, veralteten oder ungültigen Cache-Daten gibt es keinen direkten Collector-Fallback.

Der kanonische Live-Stream ist:

```text
data/monitor_agent/live_metrics.ndjson
```

Der vollständige Cache besteht aus:

```text
data/monitor_agent/live_metrics.ndjson
data/monitor_agent/live_metrics.latest.json
data/monitor_agent/instance_snapshot.json
data/monitor_agent/host_meta.json
data/monitor_agent/service_status.json
data/monitor_agent/package_status.json
data/monitor_agent/collector_status.json
```

Snapshot-JSON-Dateien werden atomar ersetzt. Der NDJSON-Stream nutzt die von PBGui verwaltete, Byte-basierte Aufbewahrung und Rotation. Dadurch bleibt die Plattenbelegung begrenzt, während Leser dem Stream weiter folgen können.

## Agent-Gesundheit

Jede Host-Karte zeigt **Monitor Agent: OK, Stale, Missing, Error oder Unknown** und nennt als Quelle immer `monitor-agent cache`.

- Live-Telemetrie gilt bis **15 Sekunden** nach dem letzten Sample als gesund.
- Der Collector-Heartbeat gilt bis **30 Sekunden** als gesund.
- **Stale** bedeutet, dass ein effektives Alter einen dieser Grenzwerte überschritten hat.
- **Missing** bedeutet, dass eine erforderliche Cache-Datei als fehlend gemeldet wurde.
- **Error** bedeutet, dass Agent, erforderliche Datei oder Collector-Schleife einen Fehler gemeldet hat.
- **Unknown** bedeutet, dass noch keine nutzbare Monitor-Agent-Diagnose vorliegt.

Der Detailbereich zeigt Heartbeat- und effektive Alter, den Status jeder erforderlichen Datei und begrenzte Collector-Fehler. Die SSH-Verbindung ist davon unabhängig: Ein Host kann über SSH weiterhin **connected** sein, während seine Monitor-Agent-Telemetrie stale ist.

## Tabs und Workflow

- **Dashboard**: Host-, SSH-, Telemetrie- und Monitor-Agent-Gesundheit
- **Instances**: laufende und bereitgestellte Bot-Instanzen mit Statusdetails
- **Services**: Status und Neustartaktionen für PBCluster, PBRun, PBData, PBCoinData und PBMonitorAgent
- **Live Logs**: Echtzeit-Streams für Service- und Bot-Logs

## Live-Log-Features

- Echte Datei-Zeilennummern
- Blockweises Ein- und Ausklappen von Logblöcken
- Volltextsuche mit Hervorhebung
- Auto-Scroll und Compact Mode
- Host- und Service-Auswahl mit Stream-Steuerung

## Voraussetzungen

- PBAPIServer muss laufen.
- Ziel-VPS-Hosts müssen in den PBAPIServer-Einstellungen aktiviert sein (`System → Services → PBAPIServer → Settings`).
- `pbgui-monitor-agent.service` muss auf jedem VPS installiert sein und laufen.
- Der PBAPIServer-WebSocket-Endpunkt muss für den Browser erreichbar sein.

## Fehlersuche

Prüfe den Monitor Agent auf dem betroffenen VPS:

```bash
systemctl --user status pbgui-monitor-agent.service
journalctl --user -u pbgui-monitor-agent.service
```

Starte ihn bei Bedarf neu:

```bash
systemctl --user restart pbgui-monitor-agent.service
```

- **Keine Daten sichtbar**: PBAPIServer unter `System → Services` starten.
- **Ein Host fehlt**: prüfen, ob der Host in den PBAPIServer-Einstellungen aktiviert ist.
- **Agent Missing**: VPS-Installation aktualisieren oder migrieren, damit `pbgui-monitor-agent.service` und alle Cache-Dateien installiert sind.
- **Agent Stale**: Service-Status und Journal prüfen. SSH kann verbunden bleiben, während die Telemetrie stale ist.
- **Agent Error**: die begrenzten Collector-Fehler in den Host-Details prüfen und danach den vollständigen lokalen Fehler im Service-Journal lesen.
- **Logs streamen nicht**: `PBApiServer.log` auf Verbindungsfehler prüfen.
