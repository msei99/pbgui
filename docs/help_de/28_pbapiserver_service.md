# PBAPIServer Service

PBAPIServer ist das FastAPI-Backend, das alle Echtzeit-Funktionen von PBGui antreibt. Es stellt REST-Endpunkte, WebSocket-Streams bereit und liefert die Frontend-Seiten (Dashboard, Services, VPS Monitor usw.) aus.

## Was PBAPIServer macht

- Betreibt den FastAPI-Server (Standard-Port 8000) mit REST-, WebSocket- und SSE-Endpunkten
- Versorgt das Dashboard mit einer 3-Schichten-Datenarchitektur:
  - **Schicht 1 (Hintergrund):** PBData pollt REST-APIs und schreibt in die Datenbank; benachrichtigt den API-Server über interne Localhost-Endpunkte
  - **Schicht 2 (On-Demand):** `api/live.py` öffnet private ccxtpro-WebSocket-Verbindungen zu Exchanges (für Positionen/Balances) wenn ein Browser sich verbindet — ref‑counted und wird geschlossen wenn kein Browser verbunden ist
  - **Schicht 3 (Browser):** Vanilla JS empfängt Updates via SSE (Server-Sent Events)
- Versorgt die Services-Seite (Start/Stop/Restart aller PBGui-Dienste)
- Proxyt VPS-Monitor-State und -Kommandos, während der unabhängige Dienst `pbgui-vps-monitor.service` persistente SSH-Verbindungen, Live-Metriken und Remote-Log-Streams besitzt
- Verwaltet die Job-Queue (Backtests, Optimierungen) mit Echtzeit-Status-Updates
- Stellt API-Key-Verwaltung bereit
- Stellt Market-Data-Pipeline-Status und -Steuerung bereit
- Bietet Live-Log-Streaming aus `data/logs/` via WebSocket
- Hostet die Heatmap-Daten-Endpunkte
- Liefert alle Vanilla-JS-Frontend-Seiten aus dem `frontend/`-Verzeichnis aus

## Konfiguration

PBAPIServer-Einstellungen werden in `pbgui.ini` unter `[api_server]` gespeichert:

| Einstellung | Standard | Beschreibung |
|---|---|---|
| `host` | `0.0.0.0` | Bind-Adresse (`0.0.0.0` = alle Interfaces, `127.0.0.1` = nur localhost) |
| `port` | `8000` | API-Server-Port (1024–65535) |

Host und Port können auf der **PBAPIServer-Detailseite** geändert werden (`System → Services → PBAPIServer → Settings`-Tab).

## Starten und Stoppen

- **Start**: Über den Start-Button auf der Services-Übersicht oder der Detailseite. PBAPIServer startet als Hintergrundprozess.
- **Stop**: Nicht über die GUI möglich (der Server kann sich nicht selbst stoppen, während er die Seite ausliefert). Bei Bedarf über Terminal stoppen.
- **Restart**: Über den Restart-Button. PBGui startet aktive verwaltete Dienste mit älterer Code-Serial neu, danach zuletzt den API-Server, und lädt anschließend die Seite neu. Der dedizierte VPS-Monitor-Daemon ist nicht Teil eines normalen API-Restarts, damit seine SSH-Sessions verbunden bleiben.

Fehlt bei einer bestehenden systemd-Installation nach dem Update noch `pbgui-vps-monitor.service`, führt der nächste Restart nach dem Laden des Migrationscodes eine einmalige Migration aus. PBGui installiert die Unit ohne den Zustand optionaler Dienste zu verändern, stoppt den alten API-eigenen Monitor, prüft den RPC-Endpunkt des Daemons und startet erst danach die API. Bei diesem ersten Handoff verbinden sich die vorhandenen SSH-Sessions einmal neu; spätere API-Restarts lassen sie verbunden. Wurde Code ersetzt, während noch der alte API-Prozess lief, lädt das Restart-Overlay zuerst die neue API, erkennt zusätzliche erst vom neuen Prozess gemeldete veraltete Dienste und führt vor dem Neuladen der Seite genau einen automatischen Folge-Restart aus.

Die Navigationsleiste zeigt einen orangefarbenen **Restart**-Button, wenn der API-Prozess oder ein aktiver PBCluster-, PBRun-, PBData-, PBCoinData- oder PBMonitorAgent-Prozess noch einen älteren Wert aus `api/serial.txt` verwendet. Der Bestätigungsdialog listet die betroffenen Dienste auf. Detached Bots, Backtests, Optimierungen und Market-Data-Jobs werden nicht neu gestartet.

## WebSocket-Endpunkte

PBAPIServer bietet mehrere Echtzeit-WebSocket-Streams:

| Endpunkt | Server-Nachrichtenformat | Client-Eingabe |
|---|---|---|
| `/ws/jobs` | `{"type":"jobs","data":[...],"timestamp":...}` mit bis zu 50 wartenden/laufenden Jobs | Keine |
| `/ws/dashboard` | Envelopes `balance_updated`, `income_updated`, `positions_updated`, `nav_request` oder `dashboard_action` | Keine |
| `/ws/candles` | Envelopes `candle`, `position`, `orders` oder `ping` | Query: `user`, `symbol`, optional `tf`, `side` |
| `/ws/market-data` | Flaches `market_data_status`-Envelope mit Exchange, Running-/Queued-Status, Zählern und `coin_rows` | Query: `exchange` |
| `/ws/vps` | `state`, `log_lines`, `local_log_lines`, Kommandoergebnisse oder `error` | JSON-Kommandos mit `cmd` |
| `/ws/heatmap-watch` | `{"type":"updated","mtime":...}` | Query: `exchange`, `dataset`, `coin` |
| `/api/v7/ws/v7` | `{"type":"instances","data":[...]}` | Empfangener Text wird ignoriert |
| `/api/backtest-v7/ws/bt7` | `queue_update` oder `archive_update` | `{"type":"refresh"}` |
| `/api/optimize-v7/ws/opt7` | `queue_update` | `{"type":"refresh"}` |
| `/api/vps-manager/ws` | `state`, `detail`, `result`, `error` und kommandospezifische Envelopes | JSON-Kommandos mit `cmd` |

Browser-WebSockets authentifizieren sich über das HttpOnly-Cookie `pbgui_session`. Ungültige oder widerrufene Sessions werden mit Code `4001` geschlossen.

## Authentifizierung

Browser-Seiten und WebSockets verwenden das HttpOnly-Cookie `pbgui_session`. API-Clients können für REST-Requests weiterhin `Authorization: Bearer xxx` verwenden.

Tokens werden beim Login generiert und laufen nach 24 Stunden ab. Alle FastAPI-Seiten erneuern Tokens automatisch alle 30 Minuten. Bei abgelaufenem Token leitet die Seite zum Login-Bildschirm weiter.

## Logs

PBAPIServer schreibt nach `data/logs/PBApiServer.log`. Log-Einträge umfassen:
- Server-Start- und Shutdown-Events
- HTTP-Request-Logging (von uvicorn)
- WebSocket-Verbindungs-Events
- Serial-Datei-Änderungserkennung (`[serial-watcher]`)
- Task-Worker-Watchdog-Events (`[watchdog]`)

## Hintergrund-Watcher

PBAPIServer betreibt mehrere interne Hintergrund-Tasks:

- **Task-Worker-Watchdog**: Prüft alle 60 Sekunden, ob der Job-Queue-Worker lebt; startet ihn automatisch neu, falls abgestürzt
- **Serial-Watcher**: Überwacht `api/serial.txt` via inotify auf Änderungen; sendet eine Restart-Benachrichtigung an alle verbundenen Clients via SSE
- **VPS-Monitor-Client**: Liest den owner-only lokalen Unix-RPC-State aus `pbgui-vps-monitor.service`; beim API-Shutdown werden nur dieser lokale Client und sein lazy Aktionspool geschlossen
- **File Sync Worker**: Überwacht lokale Konfigurationsdateien und synchronisiert Änderungen zu Remote-VPS-Hosts via inotifywait

## Fehlerbehebung

| Symptom | Prüfen |
|---|---|
| Server startet nicht | Prüfen ob Port bereits belegt ist (`lsof -i :8000`); `data/pid/api_server.pid` auf veraltete PID prüfen |
| „Address already in use" | Vorheriger Server wurde nicht sauber beendet — einige Sekunden warten oder alten Prozess beenden |
| Orangener Restart-Button verschwindet nicht | Öffnen, um den verwalteten Dienst mit älterem `api/serial.txt`-Wert zu sehen; diesen Dienst prüfen, falls der koordinierte Neustart ihn nicht aktualisiert |
| WebSocket-Verbindungsabbrüche | `PBApiServer.log` auf `[ERROR]`-Zeilen prüfen; Token-Gültigkeit verifizieren |
| Dashboard lädt nicht | Prüfen ob PBAPIServer läuft; Browser-Konsole auf Verbindungsfehler prüfen |
