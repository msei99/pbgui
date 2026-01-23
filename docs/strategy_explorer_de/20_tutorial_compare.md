# Tutorial: Compare (PB7 vs B vs C)

Compare hilft dir Abweichungen zu verstehen zwischen:

- PB7 Backtest Fills (`fills.csv`)
- Strategy Explorer lokale Simulation (Mode B)
- Strategy Explorer PB7-engine-basierter Visualisierung (Mode C)

---

## 1) Am besten: aus einem Backtest starten
1. Starte einen PB7 Backtest in PBGui.
2. In den Backtest Results klick **Strategy Explorer**.

Wenn du so startest:
- Compare ist automatisch auf den richtigen Backtest-Ordner gesetzt.
- Strategy Explorer springt automatisch in den Fill-Zeitraum.

---

## 2) Compare starten
1. Wähle den Compare Mode (z. B. PB7 vs B vs C).
2. Klick **Start Compare**.

Interpretation:
- PB7 hat Fills, B/C nicht → Zeitfenster/Market stimmt vermutlich nicht.
- B weicht ab, C matcht → Simulation vs Engine-Unterschied.
- C weicht ab → Startzeit/State-Injection oder Config-Mismatch prüfen.

---

## 3) Typischer Debug-Workflow
1. Markt prüfen (Exchange/Coin).
2. Zeitüberlappung prüfen (Fills-Timestamps vs ausgewähltes Fenster).
3. Movie Builder mit **PB7 fills.csv** nutzen, um sicherzustellen, dass Fills korrekt geladen werden.
4. Erst dann B/C vergleichen.
