# PBAPIServer Service

PBAPIServer ist das FastAPI-Backend, das alle Echtzeit-Funktionen von PBGui antreibt. Es stellt REST-Endpunkte, WebSocket-Streams bereit und liefert die Frontend-Seiten (Dashboard, Services, VPS Monitor usw.) aus.

## Was PBAPIServer macht

- Betreibt den FastAPI-Server (Standard-Port 8000) mit REST-, WebSocket- und SSE-Endpunkten
- Versorgt das Dashboard mit einer 3-Schichten-Datenarchitektur:
  - **Schicht 1 (Hintergrund):** PBData pollt REST-APIs und schreibt in die Datenbank; benachrichtigt den API-Server über interne Localhost-Endpunkte
  - **Schicht 2 (On-Demand):** `api/live.py` öffnet private ccxtpro-WebSocket-Verbindungen zu Exchanges (für Positionen/Balances) wenn ein Browser sich verbindet — ref‑counted und wird geschlossen wenn kein Browser verbunden ist
  - **Schicht 3 (Browser):** Vanilla JS empfängt Updates via SSE (Server-Sent Events)
- Versorgt die Services-Seite (Start/Stop/Restart aller PBGui-Dienste)
- Versorgt den VPS Monitor (SSH-Verbindungen, Live-Metriken, Remote-Log-Streaming, Datei-Sync)
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
- **Restart**: Über den Restart-Button. Der Server fährt sauber herunter und startet nach einer kurzen Verzögerung (3 Sekunden) neu, um sicherzustellen, dass der Port freigegeben wird.

Die Navigationsleiste zeigt einen orangefarbenen **Restart**-Button, wenn sich API-Code geändert hat (erkannt über `api/serial.txt`). Ein Klick löst einen sauberen Neustart und Seiten-Reload aus.

## WebSocket-Endpunkte

PBAPIServer bietet mehrere Echtzeit-WebSocket-Streams:

| Endpunkt | Zweck |
|---|---|
| `/ws/jobs` | Job-Queue-Updates (alle 2 s) |
| `/ws/dashboard` | Balance- und Positions-Änderungsbenachrichtigungen (von PBData über interne Endpunkte gepusht) |
| `/ws/candles` | Live-Chart-Candle-Daten via ccxtpro-Streams mit Polling-Fallback |
| `/ws/market-data` | Datenpipeline-Status pro Exchange |
| `/ws/vps` | VPS-Metriken, Logs, Service-Steuerung |
| `/ws/heatmap-watch` | Benachrichtigungen bei Datei-Änderungen |

Alle WebSocket-Verbindungen erfordern ein gültiges Authentifizierungs-Token.

## Authentifizierung

Alle API-Endpunkte und WebSocket-Verbindungen erfordern ein gültiges Token:
- Query-Parameter: `?token=xxx`
- Oder HTTP-Header: `Authorization: Bearer xxx`

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
- **VPS Monitor**: Verwaltet SSH-Verbindungspool, Live-Metriken und Remote-Log-Streaming für verbundene VPS-Hosts
- **File Sync Worker**: Überwacht lokale Konfigurationsdateien und synchronisiert Änderungen zu Remote-VPS-Hosts via inotifywait

## Fehlerbehebung

| Symptom | Prüfen |
|---|---|
| Server startet nicht | Prüfen ob Port bereits belegt ist (`lsof -i :8000`); `data/pid/api_server.pid` auf veraltete PID prüfen |
| „Address already in use" | Vorheriger Server wurde nicht sauber beendet — einige Sekunden warten oder alten Prozess beenden |
| Orangener Restart-Button verschwindet nicht | Klicken zum Neustarten; `api/serial.txt` wurde nach einer Code-Änderung inkrementiert |
| WebSocket-Verbindungsabbrüche | `PBApiServer.log` auf `[ERROR]`-Zeilen prüfen; Token-Gültigkeit verifizieren |
| Dashboard lädt nicht | Prüfen ob PBAPIServer läuft; Browser-Konsole auf Verbindungsfehler prüfen |
