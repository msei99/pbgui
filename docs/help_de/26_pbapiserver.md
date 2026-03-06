# PBAPIServer

Der **PBAPIServer** betreibt ein FastAPI-Backend (REST + WebSocket), das Echtzeit-Funktionen in PBGui ermöglicht — darunter VPS Monitor, Market-Data-Status und das Gap/Coverage-Heatmap.

## Status-Schaltfläche

Die **🟢 PBAPIServer ●** / **🔴 PBAPIServer ○** Schaltfläche in der Sidebar zeigt den aktuellen Status. Klick darauf startet den Server neu. Eine Bestätigung erscheint kurz eingeblendet.

## Einstellungen

**Settings** in der Sidebar öffnen, um Folgendes zu konfigurieren:

- **Endpoints** — aktuelle URLs für API, Docs, WebSocket und Frontend (schreibgeschützt, aktualisiert sich nach Speichern + Neustart)
- **Bind address / Port** — Netzwerkschnittstelle und Port (Standard: `0.0.0.0:8000`). Erfordert Neustart.
  - `0.0.0.0` — über alle Netzwerkschnittstellen erreichbar (Fernzugriff)
  - `127.0.0.1` — nur lokal erreichbar
- **VPS Monitoring — Auto-restart** — überwachte Dienste bei Ausfall automatisch neu starten
- **VPS Monitoring — Monitored VPS Hosts** — auswählen, welche VPS-Server überwacht werden sollen

Alle Einstellungen mit der **💾** Schaltfläche in der Sidebar speichern.

## Log

Alle PBAPIServer-Aktivitäten werden in `PBApiServer.log` protokolliert, sichtbar im Log-Viewer des Haupttabs.

## Fehlerbehebung

- **🔴 Status**: Schaltfläche klicken zum Neustart; `PBApiServer.log` auf Fehler prüfen
- **VPS Monitor zeigt keine Daten**: sicherstellen, dass PBAPIServer läuft und der WebSocket-Endpunkt erreichbar ist
- **Port-Konflikt**: Port in den Einstellungen ändern, speichern, dann neu starten
