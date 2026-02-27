# VPS Monitor

Der VPS Monitor ist ein Echtzeit-Operations-Dashboard für alle konfigurierten VPS-Hosts.

Er nutzt PBMaster als Backend und erhält Updates per WebSocket, sodass die UI ohne Streamlit-Reruns live aktualisiert wird.

## Was du überwachen kannst

- Verbindungsstatus je Host (connected/disconnected)
- Systemmetriken pro Host (CPU, RAM, Disk, Swap)
- Service-Status je Host (PBRun, PBRemote, PBCoinData)
- Bot-Instanzen über alle Hosts hinweg
- Live-Logs für Services und Bots

## Tabs und Workflow

- **Dashboard**: schneller Gesundheitsüberblick für alle Hosts
- **Instances**: laufende Bot-Instanzen mit Statusdetails
- **Services**: Services auf gewähltem Host neu starten
- **Live Logs**: Logs in Echtzeit streamen

## Live-Log-Features

- Echte Datei-Zeilennummern
- Blockweise Collapse/Expand für Logblöcke
- Volltextsuche mit Highlighting
- Auto-Scroll und Compact Mode
- Host/Service-Auswahl und Stream-Toggle

## Voraussetzungen

- PBMaster muss laufen
- Ziel-VPS-Hosts müssen in den PBMaster-Einstellungen aktiviert sein
- Der PBMaster-WebSocket-Port muss für den UI-Prozess erreichbar sein

## Schnelle Fehlersuche

- **Keine Daten sichtbar**: PBMaster unter `System → Services` starten
- **Ein Host fehlt**: prüfen, ob der Host in PBMaster aktiviert ist
- **Logs streamen nicht**: WebSocket-Port und PBMaster-Log auf Verbindungsfehler prüfen
