# Tutorial: Strategy Explorer Quickstart

Dieses Tutorial bringt dich im gemeinsamen PB7/PB8-Seitenrahmen und Workflow von "Strategy Explorer oeffnen" zu "ich kann die Anzeige interpretieren".

---

## 1) Markt auswaehlen
1. Oeffne Strategy Explorer direkt oder nutze **Strategy Explorer** aus PB8 Run, PB8 Backtest, Backtest Results oder Pareto Explorer.
2. In den gemeinsamen Controls:
   - Waehle **Exchange**
   - Waehle **Coin**
3. Pruefe, dass Candles fuer das gewaehlte Fenster geladen werden.

PB7 verwendet seine bestehenden lokalen OHLCV-Optionen. PB8 bezeichnet seine Quelle als **PB8 native candles** und verwendet native PB8-Candle-Aufbereitung. Ein Handoff laedt seine Config und passende Overrides vor.

---

## 2) Analysis Time setzen
1. Waehle mit **Start Date** und **Start Time** einen Zeitpunkt.
2. Halte **Chart Context** anfangs eher klein, zum Beispiel 3-10 Tage.

Merksatz:
- Start Date/Start Time waehlen die erste angezeigte Candle. Chart Context laeuft vorwaerts; die rechte Candle ist Analysis Time und liefert den Snapshot-State.
- PB8-Entry-Orders verwenden eine bereitgestellte flache Position; PB8-Close-Orders verwenden eine repraesentative hypothetische Position zu diesem Preis.

Der PB8-Snapshot ist weder Live-Account-State noch Prognose.

---

## 3) Snapshot lesen
Achte auf:

- Entry-Order-Level
- Close-Order-Level
- Verfuegbare Strategie-Referenz- oder Trailing-Linien
- Long/Short-Parameter und Summary-Werte

PB7 behaelt sein bestehendes lokales/PB7-Engine-Snapshot-Verhalten. PB8 zeigt native ideale Orders fuer die bereitgestellten States, nicht exakte Orders, die historisch auf einer Exchange lagen.

Fragen:

- Sind Entry-Level dort, wo ich sie erwarte?
- Ist die repraesentative Close-Ausgabe zu aggressiv oder zu konservativ?
- Hat eine Parameteraenderung die erwartete Wirkung?

---

## 4) (Optional) Simulation starten
Wenn du historische Fills sehen willst:

1. Oeffne **Simulation**.
2. Waehle bei PB7 **PBGui Simulation** oder **PB7 Backtest Engine**.
3. Starte bei PB8 **PB8 Native Replay**.

PB8-Replay ist ein begrenzter nativer Backtest ueber die gewaehlten Candles mit Server-Limits von 20.000 Candles und 2.000 angezeigten Fills. Sie prognostiziert keinen Live-Account.

---

## 5) Naechste Schritte
- Zum Abgleich eines gespeicherten Resultats mit Berechnungen: Tutorial "Compare".
- Fuer eine Animation eines Candle-Fensters: Tutorial "Movie Builder".
