# Strategy Explorer – Hilfe

> HINWEIS: Dieses Dokument wurde nach `pbgui/docs/strategy_explorer_de` verschoben. Bitte nutze die Strategy Explorer-Dokumentation stattdessen.

Strategy Explorer ist ein Debug-/Analyse-Tool für PB7 Grid-Strategien. Es kombiniert:

- Eine **Snapshot-Ansicht** (Entry-/Close-Grids, Trailing-Linien) zu einer gewählten **Analysis Time**.
- Optional eine **Historical Simulation** (lokaler Candle-Walk), um Fills/Orders als Marker zu sehen.
- Einen **Compare**-Workflow, um GridVis und PB7-Backtest-Ausgaben zu vergleichen.
- Einen **Movie Builder**, um eine zeitbasierte Animation zu erzeugen.

Dieses Dokument erklärt alle verfügbaren Varianten und wie die Long/Short-Grid-Anzeige zu lesen ist.

---

## Grundbegriffe

### Exchange / Coin
Strategy Explorer arbeitet immer auf einem konkreten Markt:

- **Exchange**: z. B. `bybit`
- **Coin**: Coin/Market-Code aus deinem lokalen PB7 OHLCV-Cache

Wenn keine Candles gefunden werden, kann Strategy Explorer nichts rendern.

### Analysis Time (wichtigster Regler)
Strategy Explorer berechnet Grids/Trailing-State für einen Zeitpunkt: **Analysis Time**.

- Das ist praktisch die Candle am **rechten Rand** des Charts.
- Alle Grid-/Level-Linien sind „was der Bot als nächstes platzieren würde“, *unter der Annahme des injizierten States zu diesem Zeitpunkt*.

### Context window
Das Chart zeigt einen Kontext-Ausschnitt rund um die Analysis Time:

- **Context days** bestimmt, wie viel Historie angezeigt wird.

---

## Varianten / Modi

### 1) Visualizer Snapshot (einzelne Ansicht)
Standardverhalten von Strategy Explorer.

Du wählst die Analysis Time und Strategy Explorer rendert u. a.:

- **Entry grid** Levels (potenzielle Entries)
- **Close grid** Levels (TP/Close-Ladder)
- Trailing Thresholds/Triggers + Referenzlinien

Damit beantwortest du z. B.:

- „Welche Grid-Ladder würde PB7 jetzt stellen?“
- „Warum triggert Trailing hier?“
- „Warum sind meine Close-Orders so eng/weit?“

### 2) Historical Simulation (lokaler Candle-Walk)
Wenn aktiviert, läuft Strategy Explorer candle-by-candle vorwärts und zeichnet Fills auf.

- Das ist eine *lokale Simulation* für Intuition/Debugging.
- Sie muss nicht 1:1 mit PB7 Backtests übereinstimmen (Rundung, Engine-Details, Exchange-Semantik).

Fills erscheinen als Marker und in einer Tabelle.

### 3) Compare (PB7 vs B vs C)
Compare dient zum Reconciliaten / Debuggen von Abweichungen.

Typische Bedeutung:

- **PB7**: Fills aus dem PB7 Backtest Result (meist `fills.csv`).
- **B**: Strategy Explorer lokale Simulation (Mode B).
- **C**: Strategy Explorer PB7-engine-basierter Pfad (Mode C, „upcoming fills“ Stil).

Nutze Compare wenn:

- Du sicherstellen willst, dass dein Zeitfenster dem Backtest-Zeitfenster entspricht.
- Du sehen willst, ob Abweichungen von Startzeit/State-Injection vs Engine-Unterschieden kommen.

### 4) Movie Builder
Movie Builder erzeugt eine Animation über die Zeit.

Es gibt drei Engines:

- **Local (B) – full grids**
  - Zeigt die sich entwickelnden Grids + Trailing-Linien.
  - Fills kommen aus lokaler Simulation.

- **PB7 backtest engine (C) – upcoming fills**
  - Nutzt den PB7 Engine-Pfad, den Strategy Explorer verwendet.
  - Fokus: Fills und Vorschau „upcoming fills“; keine vollständigen Grid-Ladders pro Candle.

- **PB7 fills.csv (from backtest)**
  - Visualisiert ein existierendes PB7 Backtest Result (`fills.csv`) als Ground Truth.
  - Keine Neuberechnung der Engine.

---

## Long/Short Grid Anzeige (wie lesen?)
GridVis kann Long und/oder Short anzeigen – abhängig von deiner Config.

### Long
- **Long entry grid**: typischerweise Buy-Level unterhalb des Preises.
- **Long close grid**: typischerweise Sell/Close-Level oberhalb (TP-Ladder).

### Short
- **Short entry grid**: typischerweise Sell-Level oberhalb des Preises.
- **Short close grid**: typischerweise Buy/Close-Level unterhalb.

### Beide Seiten aktiv
Wenn Long und Short aktiv sind:

- Snapshot kann beide Grid-Sets zeigen.
- Im Movie Builder gibt es einen **Side**-Selector:
  - `Auto` (bevorzugt Long)
  - `Long`
  - `Short`

Tipp: Wenn dein Backtest nur eine Seite traded, selektiere diese Seite.

---

## Häufige Probleme

### „Ich sehe keine Orders/Marker“
Fast immer ist es ein Zeitfenster-Problem:

- Deine Analysis Time / Movie Window überlappt nicht mit dem Zeitraum der Fills.
- Wenn du Strategy Explorer aus Backtest Results startest, sollte Strategy Explorer automatisch in den Fill-Zeitraum springen.

---

## Nächste Schritte
- Nutze die Tutorials im Docs-Selector direkt im Strategy Explorer.
