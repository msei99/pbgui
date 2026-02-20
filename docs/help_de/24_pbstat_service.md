# PBStat Service Details

PBStat ist ein Legacy-Hintergrunddienst, der Live-Handelsstatistiken (PnL, Fills) für die alten **v6 Single Bot** Spot-Trading-Instanzen sammelt.

> **Hinweis:** Dieser Service ist nur relevant, wenn du noch alte v6 Spot-Bots betreibst. Er wird nicht für v7 Perpetual-Futures-Instanzen verwendet.

## Was PBStat macht

- Verbindet sich mit aktiven v6 Single Bot-Instanzen
- Sammelt und aggregiert Handelsstatistiken (PnL, Fills)
- Schreibt Service-Logs nach `data/logs/PBStat.log`

## PBStat-Detailseite

Auf `System → Services → PBStat → Show Details` kannst du:

- Den aktuellen PBStat-Status prüfen (läuft/gestoppt)
- Den Service ein-/ausschalten
- Den integrierten gefilterten PBStat-Log-Viewer nutzen

## Schnelle Fehlersuche

- Wenn du nur v7-Instanzen betreibst, kannst du PBStat bedenkenlos gestoppt lassen.
