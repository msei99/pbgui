# PBRun Service Details

PBRun ist der lokale Service-Orchestrator von PBGui. Er hält Bot-Prozesse mit den konfigurierten Instanzen synchron und aktualisiert Laufzeitdateien für Passivbot.

## Was PBRun macht

PBRun führt eine Daemon-Schleife alle 5 Sekunden aus:

- Startet/stoppt lokale Passivbot-Prozesse für konfigurierte Instanzen (PB7, PB6 Multi, PB6 Single)
- Überwacht den Ressourcenverbrauch (CPU, Speicher) jedes laufenden Bots und sammelt PnL/Error/Traceback-Zähler aus den Log-Dateien
- Überwacht dynamische Coin-Filter (über PBCoinData-Mappings) und schreibt `ignored_coins.json` / `approved_coins.json`
- Reagiert auf Aktivierungs- und Status-Dateien von PBRemote (Dateisystem-basierte Nachrichtenwarteschlange in `data/cmd/`)
- Führt einen Speicher-Watchdog aus: fällt der freie Systemspeicher unter 250 MB, wird der Bot mit dem höchsten Speicherverbrauch neu gestartet
- Schreibt Service-Logs nach `data/logs/PBRun.log`

## PBRun-Detail-Panel

Klicke auf die PBRun-Kachel in der Services-Übersicht (oder nutze die Sidebar), um das Detail-Panel zu öffnen:

- Der Control-Strip oben zeigt den aktuellen Status (läuft/gestoppt) und Start/Stop/Restart-Buttons
- Der Log-Tab zeigt einen Live-gefilterten PBRun-Log-Viewer

## Typisches Startverhalten

Nach Restart oder Erststart können große `Change ignored_coins` / `Change approved_coins`-Logs erscheinen. Das ist normal, solange die dynamischen Coin-Listen aus den aktuellen Mapping-Daten initialisiert werden.

## Schnelle Fehlersuche

- Prüfen, ob PBRun in Services läuft
- `data/logs/PBRun.log` auf aktuelle `ERROR`-Einträge prüfen
- Sicherstellen, dass `data/run_v7/<instance>/ignored_coins.json` und `approved_coins.json` existieren und gültige JSON-Listen sind
- Wenn dynamische Listen alt wirken: PBRun nach abgeschlossenen PBCoinData-Mapping-Updates einmal neu starten
