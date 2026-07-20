# Balance Calculator

Der gemeinsame Balance Calculator schätzt das erforderliche Wallet-Guthaben für eine PBv7- oder PBv8-Konfiguration anhand genehmigter Coins, Positionszahl, Wallet-Exposure-Limit, initialer Entry-Größe und Exchange-Mindestordergrößen.

## Wie öffnet man ihn

Die eigenständige Seite lässt sich öffnen über:

- **Information → Balance Calculator**: eine PBv7-Instanz laden oder eine PBv7-/PBv8-Config einfügen
- **PBv7 → Run**: bei einer Instanz die Aktion **$** anklicken
- **PBv7 → Backtest**: eine Config öffnen oder ein Result auswählen und **Balance Calculator** anklicken
- **PBv8 → Backtest**: eine Config öffnen oder ein PBv8-Result auswählen und **Balance Calculator** anklicken

Beide Backtest-Seiten bieten zusätzlich **Calc Balance** für eine schnelle Inline-Berechnung ohne Seitenwechsel.

Bei PBv8 wird ein exakter `approved_coins`-Wert `all` anhand des lokalen Mappings der gewählten Exchange erweitert. Berücksichtigt werden nur aktive lineare Swap-Märkte mit PB8s Standard-Quote; seitenspezifisch ignorierte Coins werden vor der Berechnung entfernt.

## Aufbau

| Bereich | Inhalt |
|---------|--------|
| Linke Spalte | Bearbeitbares Konfigurations-JSON |
| Toolbar | Optionale PBv7-Instanz, Exchange-Auswahl und Calculate-Schaltfläche |
| Rechte Spalte | Empfehlung, Guthaben je Seite und Coin-Mindestorderinformationen |

## Workflow

1. Den Calculator über Information, Run oder Backtest öffnen.
2. Eine PBv7-Instanz laden, einen Backtest-Absprung verwenden oder eine PBv7-/PBv8-Config einfügen.
3. **Exchange** auswählen, falls mehrere Exchanges konfiguriert sind.
4. Optional das Config-JSON in der linken Textarea bearbeiten.
5. **Calculate** klicken, um die Guthaben-Anforderungen zu berechnen.

## Exchange-Auswahl

- Backtest- und Run-Absprünge wählen den erkannten Exchange vor.
- Beim direkten Öffnen gilt die aktuelle Dropdown-Auswahl.
- Der Exchange kann jederzeit über das **Exchange**-Dropdown geändert werden.

## Config bearbeiten

- Die linke Textarea zeigt die vollständige Konfiguration als JSON.
- Änderungen werden angewendet, wenn du auf **Calculate** klickst.
- Ungültiges JSON zeigt einen Fehler und wird nicht zur Berechnung gesendet.

## Ergebnisse

Nach dem Klick auf **Calculate** zeigt die rechte Spalte:

- Empfohlenes Wallet-Guthaben mit 10 % Puffer, auf die nächsten 10 USDT aufgerundet
- Erforderliches Guthaben je Long- und Short-Coin
- Für die Berechnung verwendete Coin-Preise und Mindestorderinformationen

Bei PBv7 werden Bot-Parameter aus `bot.<side>` gelesen. Bei PBv8 kommen Positionszahl und Exposure aus `bot.<side>.risk`; die initiale Entry-Größe wird aus `bot.<side>.strategy.<live.strategy_kind>.entry.initial_qty_pct` gelesen. PBv7 Dynamic Ignore bleibt unterstützt. Beide Versionen lösen Markt-Mindestwerte über das lokale CoinData-Mapping auf.

## Fehlerbehebung

- **Kein Ergebnis für eine Seite**: Approved Coins, positive Positionszahl, positives Exposure-Limit und positive initiale Entry-Größe prüfen.
- **CoinData nicht konfiguriert**: Unter **System -> Services -> PBCoinData -> Pool** einen CMC-Pool-Key anlegen oder aktivieren und die lokale Materialisierung abwarten.
- **Unerwartete PBv7-Coin-Liste**: Bei aktiviertem Dynamic Ignore können CoinData-Einstellungen die Approved Coins filtern.
