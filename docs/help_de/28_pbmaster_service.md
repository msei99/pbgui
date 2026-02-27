# PBMaster Service Details

PBMaster ist der SSH-basierte VPS-Management-Service von PBGui.

Er hält persistente SSH-Verbindungen zu aktivierten VPS-Hosts, überwacht wichtige Services und liefert die Echtzeitdaten für die VPS-Monitor-Seite.

## Was PBMaster macht

- Hält persistente SSH-Sessions zu aktivierten VPS-Hosts
- Sammelt fortlaufend Host-Status und Service-Zustand
- Unterstützt Remote-Service-Neustarts aus der Monitor-UI
- Streamt Live-Logs über einen lokalen WebSocket-Server
- Schreibt Logs nach `data/logs/PBMaster.log`

## Voraussetzungen

- Dieser Service ist für den **Master**-Node vorgesehen
- VPS-Hosts müssen im VPS Manager konfiguriert sein
- SSH-Key-Setup muss pro VPS abgeschlossen sein

## PBMaster-Detailseite

Auf `System → Services → PBMaster → Show Details` kannst du:

- PBMaster starten/stoppen
- `Auto-restart services` konfigurieren
- `Monitor interval (seconds)` setzen
- `WebSocket port` für das VPS-Monitor-Frontend setzen
- Überwachte VPS-Hosts aktivieren/deaktivieren
- Den integrierten gefilterten PBMaster-Log-Viewer öffnen

## Verhalten bei Host-Aktivierung

- Hosts sind für PBMaster-Monitoring standardmäßig deaktiviert
- Hosts müssen in den PBMaster-Einstellungen explizit aktiviert werden
- Änderungen greifen nach Speichern und Neustart von PBMaster

## Schnelle Fehlersuche

- **PBMaster läuft nicht**: die VPS-Monitor-Seite zeigt Warnung und keine Live-Daten
- **Host bleibt disconnected**: SSH-Keys und Erreichbarkeit vom Master prüfen
- **Keine Live-Updates im VPS Monitor**: PBMaster-WebSocket-Port prüfen und PBMaster nach Port-Änderung neu starten
