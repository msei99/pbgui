# Tutorial: Live vs Backtest (PBv7)

Diese Seite vergleicht **Live-Performance** (aus der Exchange-Income-History, gespeichert in der PBGui-DB) mit einem **PB7 Backtest Resultat** (aus `fills.csv`).

Ziel:
- “Passt die Backtest Equity Curve grob zur Live Curve?”
- “Welche Coins/Symbole verursachen die grössten Abweichungen?”

---

## Was wird verglichen?

### Live
Live wird aus **Income-Events** berechnet, die PBGui von der Exchange abruft und in der Datenbank speichert (`history` Tabelle). Je nach Exchange/Account umfasst das typischerweise:
- Realized PnL
- Kommissionen/Fees
- Funding Fees

### Backtest
Backtest wird aus dem gewählten PB7 Result-Ordner (`fills.csv`) berechnet.

PBGui verwendet:
- `net = pnl + fee_paid` (pro Fill)

---

## Voraussetzungen
- API-Keys sind in **API-Keys** konfiguriert.
- Der User hat Live-Income Daten in der DB (sonst ist die Live Curve leer).
- Ein PB7 Backtest existiert im PB7 Result-Folder (`backtests/pbgui/...`).

---

## Schritt-für-Schritt

### 1) User wählen
- Standardmässig werden nur User angezeigt, die bereits Live-Income-Daten haben.
- Mit **All users** kannst du alle API-Keys User anzeigen.

### 2) Exchange für den Compare-Backtest wählen
- Diese Exchange bestimmt, welches PB7 Markt-Universum für den Compare-Run verwendet wird.
- Wenn du Live auf Hyperliquid bist, willst du oft gegen Binance backtesten (weil Hyperliquid 1m OHLCV Snapshots historisch nicht immer sauber sind).

### 3) Zeitraum wählen
- **Start** und **End** setzen.
- Optional **Select range** aktivieren und im Chart eine Box ziehen, um Start/End zu setzen.

### 4) Backtest Result wählen (optional)
- Wenn du ein Result wählst, wird die Backtest Curve über die Live Curve gelegt.
- Mit **Sync Start/End to backtest** kannst du den Zeitraum an den Backtest anpassen.

### 5) (Optional) Coins/Symbole wählen
- Leer: Total vergleichen.
- Selektiert: nur die gewählten Coins/Symbole vergleichen.

### 6) Starting Balance
- PBGui zeigt eine vorgeschlagene Starting Balance basierend auf der DB.
- Du kannst diese Starting Balance für den Compare überschreiben.

### 7) Compare Backtest starten
- **Run Compare Backtest** enqueued einen PB7 Run.
- Für neue Compare Runs setzt PBGui `combine_ohlcvs = false`.

---

## Resultate interpretieren
- Wenn die Curves nur einen konstanten Offset haben, ist es oft einfach ein **Starting Balance** Thema.
- Wenn die Abweichung über die Zeit wächst und auf wenige Coins konzentriert ist, ist es meist **anderer Trade-/Fill-Pfad** (nicht nur Fees).

---

## Bekannte Limitationen (aktuell)
- Live basiert auf **Income-Events**, Backtest auf **fills-basierter net** Berechnung. Das sind nicht identische Datenquellen.
- Trade-Level Matching (Missed Orders + Slippage via Matching Live Trades ↔ Backtest Fills) ist geplant, aber noch nicht in diese Seite integriert.
- “Combined” Results können verglichen werden, aber von dieser Seite aus wird kein neuer Compare-Backtest im “combined” Mode gestartet.

---

## Troubleshooting
- **Keine Live Curve:** sicherstellen, dass Income History vorhanden ist (Services laufen lassen / Exchange History Fetch prüfen).
- **Keine Backtest Resultate:** einen PB7 Backtest via PBGui starten, dann hier Refresh.
- **Leere Coin-Liste:** zuerst Total vergleichen; Coin-Erkennung hängt von den vorhandenen Daten ab.
