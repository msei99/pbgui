# Tutorial: Long vs Short Grids verstehen

Dieses Tutorial erklärt, wie Strategy Explorer Long- und Short-Grids darstellt.

---

## 1) Long Grids
### Long entry grid
- **Buy** Orders zum Öffnen/Erhöhen einer Long-Position.
- Meist **unterhalb** des aktuellen Preises.

### Long close grid
- **Sell/Close** Orders zum Reduzieren/Schließen.
- Meist **oberhalb** des Entry/Preises (TP-Ladder).

---

## 2) Short Grids
### Short entry grid
- **Sell** Orders zum Öffnen/Erhöhen einer Short-Position.
- Meist **oberhalb** des aktuellen Preises.

### Short close grid
- **Buy/Close** Orders zum Reduzieren/Schließen.
- Meist **unterhalb** des Entry/Preises.

---

## 3) Beide Seiten aktiv
Wenn Long und Short aktiv sind:

- Snapshot kann beide Grid-Sets zeigen.
- Movie Builder hat einen Side-Selector.

Wenn nur eine Seite Fills hat:
- Das ist normal, wenn der Backtest nur eine Richtung gehandelt hat.

---

## 4) Trailing-Linien
Trailing ist pfadabhängig.

- Threshold/Retracement-Linien geben Intuition, wann Trailing eligible wird und wann es triggert.
- Die exakte Fill-Sequenz kann trotzdem vom Candle-Pfad abhängen.

---

## 5) Debug-Checkliste
Wenn Grids „invertiert“ wirken:

- Side prüfen (Long vs Short)
- Exchange/Coin prüfen
- Analysis Time im richtigen Zeitraum?
- Für Backtest-Vergleich: Strategy Explorer aus Backtest Results starten
