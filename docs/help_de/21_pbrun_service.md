# PBRun Service Details

PBRun ist der lokale Service-Orchestrator von PBGui. Er hält Bot-Prozesse mit den konfigurierten Instanzen synchron und aktualisiert Laufzeitdateien für Passivbot.

## Was PBRun macht

- Startet/stoppt lokale Passivbot-Prozesse für konfigurierte Instanzen
- Überwacht dynamische Coin-Filter und schreibt `ignored_coins.json` / `approved_coins.json`
- Reagiert auf Aktivierungs-/Status-Dateien von PBRemote
- Schreibt Service-Logs nach `data/logs/PBRun.log`

## PBRun-Detailseite

Auf `System → Services → PBRun → Show Details` kannst du:

- Den aktuellen PBRun-Status prüfen (läuft/gestoppt)
- Den Service ein-/ausschalten
- Den integrierten gefilterten PBRun-Log-Viewer im Detailbereich nutzen (kein separates `Show logfile`-Toggle)

## Typisches Startverhalten

Nach Restart oder Erststart können große `Change ignored_coins` / `Change approved_coins`-Logs erscheinen. Das ist normal, solange die dynamischen Coin-Listen aus den aktuellen Mapping-Daten initialisiert werden.

## Schnelle Fehlersuche

- Prüfen, ob PBRun in Services läuft
- `data/logs/PBRun.log` auf aktuelle `ERROR`-Einträge prüfen
- Sicherstellen, dass `data/run_v7/<instance>/ignored_coins.json` und `approved_coins.json` existieren und gültige JSON-Listen sind
- Wenn dynamische Listen alt wirken: PBRun nach abgeschlossenen PBCoinData-Mapping-Updates einmal neu starten
