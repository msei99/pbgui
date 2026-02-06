# Tutorial: Live vs Backtest (PBv7)

Diese Seite vergleicht **Live-Performance** (aus Exchange-History, gespeichert in der PBGui-DB) mit einem **PB7 Backtest Resultat** (aus `fills.csv`).

PBGui kann mehrere Live-Datenquellen nutzen:
- **Income Rows** (Funding/Realized PnL/Fees je nach Exchange) aus der PBGui-DB (`history` Tabelle)
- **Executions** (Trade-Fills) aus der Trades-DB (`executions` Tabelle)

Ziel:
- “Passt die Backtest Equity Curve grob zur Live Curve?”
- “Welche Coins/Symbole verursachen die grössten Abweichungen?”

---

## Was wird verglichen?

### Live (Income Rows)
Live wird aus **Income-Events** berechnet, die PBGui von der Exchange abruft und in der Datenbank speichert (`history` Tabelle). Je nach Exchange/Account umfasst das typischerweise:
- Realized PnL
- Kommissionen/Fees
- Funding Fees

### Live (Executions)
Executions sind einzelne Fills/Trades, die von der Exchange abgerufen und in PBGui’s Trades-Datenbank gespeichert werden (`executions` Tabelle). PBGui berechnet daraus:
- `exec_net = realized_pnl - fee` (Fee ist als positiver Kostenwert gespeichert)

### Backtest
Backtest wird aus dem gewählten PB7 Result-Ordner (`fills.csv`) berechnet.

PBGui verwendet:
- `net = pnl + fee_paid` (pro Fill)

---

## Voraussetzungen
- API-Keys sind in **API-Keys** konfiguriert.
- Der User hat Live-Income Daten in der DB (sonst ist die Live Curve leer).
- Für **Execution/Trade Matching** müssen ausserdem Executions in der Trades-DB vorhanden sein.
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

### Details / Diagnostics (neu)
Unter **Details / Diagnostics** findest du Tools um *wo* die Abweichung beginnt zu debuggen.

Wichtigste Features:
- **Deviation Day Inspector**: springe Tag-für-Tag durch die grössten Abweichungen.
- **Missed fills / price_distance_threshold**: zeigt, wann das Initial-Entry-Gating nur kurz offen war (Dip-only Minutes) und kann **Backtest `entry_initial` Fills** überlagern.
- Tabs pro Tag/Scope:
	- **Live income rows**
	- **Backtest fills**
	- **Live executions**
	- **BT vs Live (matched)**: matcht Backtest-Fills gegen die nächsten Live-Executions (Zeitstempel + Toleranz), um **missed orders** und **Slippage** sichtbar zu machen.

---

## Bekannte Limitationen (aktuell)
- Der Haupt-Chart vergleicht **Income-Events** (Live) vs **fills-basierte net** Berechnung (Backtest). Das sind nicht identische Datenquellen.
- Execution/Trade Matching ist **zeitbasiert** (nächster Zeitstempel innerhalb der Toleranz). In schnellen Märkten/bei Partial Fills ist Interpretation + evtl. Toleranz-Anpassung nötig.
- “Combined” Results können verglichen werden, aber von dieser Seite aus wird kein neuer Compare-Backtest im “combined” Mode gestartet.

---

## Troubleshooting
- **Keine Live Curve:** sicherstellen, dass Income History vorhanden ist (Services laufen lassen / Exchange History Fetch prüfen).
- **Keine Backtest Resultate:** einen PB7 Backtest via PBGui starten, dann hier Refresh.
- **Leere Coin-Liste:** zuerst Total vergleichen; Coin-Erkennung hängt von den vorhandenen Daten ab.
