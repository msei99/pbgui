# PBv7 Balance Calculator

Der Balance Calculator schätzt das erforderliche Wallet-Guthaben für eine PBv7-Konfiguration basierend auf den genehmigten Coins, Positionsgrößen und Hebel-Einstellungen.

## Wie öffnet man ihn

Der Balance Calculator benötigt Exchange-Kontext — öffne ihn über:

- **PBv7 → Run**: **Balance Calculator**-Schaltfläche bei einer Instanz klicken
- **PBv7 → Backtest**: eine Config öffnen und in der Editor-Sidebar **💰 Balance Calculator** klicken

Für einen schnellen Inline-Check ohne die eigenständige Seite zu öffnen, gibt es im Backtest-Editor zusätzlich **⚡ Calc Balance**.

Direktes Öffnen über die Navigation ohne Kontext zeigt eine Fehlermeldung.

## Aufbau

| Bereich | Inhalt |
|---------|--------|
| Linke Spalte | Bearbeitbares Konfigurations-JSON |
| Rechte Spalte | Exchange-Auswahl, Calculate-Schaltfläche, Ergebnisse |

## Workflow

1. Balance Calculator aus Run oder Backtest öffnen.
2. Die Konfiguration wird automatisch aus der gewählten Instanz oder Backtest-Config geladen.
3. **Exchange** auswählen, falls mehrere Exchanges konfiguriert sind.
4. Optional das Config-JSON in der linken Textarea bearbeiten.
5. **Calculate** klicken, um die Guthaben-Anforderungen zu berechnen.

## Exchange-Auswahl

- Wenn die Konfiguration nur einen Exchange hat, wird er automatisch gesetzt.
- Bei mehreren Exchanges fragt ein Dialog nach der Auswahl.
- Der Exchange kann jederzeit über das **Exchange**-Dropdown geändert werden.

## Config bearbeiten

- Die linke Textarea zeigt die vollständige Konfiguration als JSON.
- Änderungen werden angewendet, wenn du auf **Calculate** klickst.
- Ungültiges JSON zeigt ein Fehler-Popup — die letzte gültige Konfiguration wird wiederhergestellt.

## Ergebnisse

Nach dem Klick auf **Calculate** zeigt die rechte Spalte:

- Erforderliches Guthaben für Long-Positionen
- Erforderliches Guthaben für Short-Positionen
- Gesamt-Guthabenbedarf (Schätzwert)

Die Berechnung verwendet die Coin-Liste aus `approved_coins` in der Config, gefiltert durch CoinData (Marktkapitalisierung, Volumen usw.) wenn Dynamic Ignore aktiviert ist.

## Fehlerbehebung

- **„Missing exchange context"**: Balance Calculator nicht direkt über die Navigation öffnen — die Schaltfläche in RunV7 oder BacktestV7 verwenden.
- **CoinData nicht konfiguriert**: CoinMarketCap-API-Schlüssel unter **System → API Keys** einrichten.
- **Unerwartete Coin-Liste**: Bei aktiviertem Dynamic Ignore wird die Coin-Liste durch CoinData-Einstellungen gefiltert (Marktkapitalisierung, Volumen, Tags).
