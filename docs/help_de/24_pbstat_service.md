# PBStat Service Details

PBStat ist ein Legacy-Hintergrunddienst, der Live-Handelsstatistiken (PnL, Fills) für die alten **v6 Single Bot** Spot-Trading-Instanzen sammelt.

> **Hinweis:** Dieser Service ist nur relevant, wenn du noch alte v6 Spot-Bots betreibst. Er wird nicht für v7 Perpetual-Futures-Instanzen verwendet.

## Was PBStat macht

PBStat führt eine Daemon-Schleife alle 60 Sekunden aus. Jeder 5. Zyklus führt einen vollständigen Fetch durch, andere Zyklen nur eine leichtere Status-Prüfung.

- Lädt Position, Balance, Preis und offene Orders von der Exchange für jede aktive Spot-Instanz
- Lädt Trade-History seit dem letzten bekannten Trade und hängt neue Trades an die `trades.json` der Instanz an
- Nach jedem Zyklus werden alle Instanzen von der Festplatte neu geladen, um hinzugefügte/entfernte Instanzen zu erkennen
- Schreibt Service-Logs nach `data/logs/PBStat.log`

## PBStat-Detail-Panel

Klicke auf die PBStat-Kachel in der Services-Übersicht (oder nutze die Sidebar), um das Detail-Panel zu öffnen:

- Der Control-Strip zeigt den aktuellen Status (läuft/gestoppt) und Start/Stop/Restart-Buttons
- Der Log-Tab zeigt einen Live-gefilterten PBStat-Log-Viewer

## Schnelle Fehlersuche

- Wenn du nur v7-Instanzen betreibst, kannst du PBStat bedenkenlos gestoppt lassen.
