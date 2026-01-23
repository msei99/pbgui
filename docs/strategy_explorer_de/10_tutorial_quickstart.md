# Tutorial: Strategy Explorer Quickstart

Dieses Tutorial bringt dich von „Strategy Explorer öffnen“ zu „ich kann alles im Chart lesen“.

---

## 1) Markt auswählen
1. Öffne Strategy Explorer.
2. In **Data + Time & View**:
   - Wähle **Exchange**
   - Wähle **Coin**
3. Du solltest eine Meldung sehen wie „Loaded N candles…“.

Wenn „No candles found“ erscheint, fehlen lokale OHLCV-Daten im PB7 Cache.

---

## 2) Analysis Time setzen
1. Nutze Day-Slider/Datum/Uhrzeit zur Auswahl.
2. Setze **Context days** am Anfang eher klein (z. B. 3–10), damit es übersichtlich bleibt.

Merksatz:
- Analysis Time = Kerze am rechten Rand, aus der Grids berechnet werden.

---

## 3) Snapshot lesen
Achte auf:

- Entry-Grid Levels (wo eröffnet/aufgestockt würde)
- Close-Grid Levels (wo reduziert/geschlossen würde)
- Trailing Thresholds/Triggers

Fragen:

- Sind Entry-Level dort, wo ich sie erwarte?
- Sind die Close-Schritte zu aggressiv/zu konservativ?
- Aktiviert Trailing wie gedacht (Threshold erreicht, dann Retrace)?

---

## 4) (Optional) Historical Simulation
Wenn du Fills als Marker sehen willst:

1. Aktiviere **Historical Simulation**.
2. Verschiebe Analysis Time vor/zurück und beobachte, wie Fills entstehen.

Das ist v. a. für Debugging/Intuition; 1:1 Matching mit PB7 Backtest ist nicht garantiert.

---

## 5) Nächste Schritte
- Backtest-Abgleich: Tutorial „Compare (PB7 vs B vs C)“.
- Animation: Tutorial „Movie Builder“.
