# Tutorial: Movie Builder

Movie Builder erzeugt eine Animation über die Zeit.

---

## 1) Fenster so wählen, dass es alles abdeckt
Movie Builder wird gesteuert über:

- **Step Size** (z. B. 1m, 5m, 1h, 4h)
- **Duration** (Preset) oder **Frames** (Custom)

Abdeckung:

- Bei Duration-Preset: Frames werden aus Duration / Step Size berechnet
- Bei Custom (Frames): Abdeckung ≈ Frames × Step Size

Tipp: Wenn du Strategy Explorer aus Backtest Results startest, werden Step Size und Duration automatisch so gesetzt, dass das Backtest-Fill-Zeitfenster abgedeckt ist.

> HINWEIS: Dieses Tutorial wurde nach `pbgui/docs/strategy_explorer_de` verschoben. Bitte nutze die neue Dokumentation.

---

## 2) Movie engine auswählen
### Local (B) – full grids
Nutzen wenn:
- Du volle Grid-Ladders und Trailing-Linien sehen willst.

Pro:
- Maximal visuelle Details.

Contra:
- Lokale Simulation; kann vom PB7 Backtest abweichen.

### PB7 backtest engine (C) – upcoming fills
Nutzen wenn:
- Du eine PB7-engine-basierte Fill-Vorschau willst.

Pro:
- Näher an PB7 Logik.

Contra:
- Keine vollständigen „open grids“ pro Candle.

### PB7 fills.csv (from backtest)
Nutzen wenn:
- Du exakt die Fills aus einem existierenden Backtest visualisieren willst.

Pro:
- Ground Truth für Fills.

Contra:
- Erzeugt keine neuen Fills; nur Visualisierung.

---

## 3) Long/Short wählen
Wenn beide Seiten aktiv sind:

- Auto (bevorzugt Long)
- Long
- Short

---

## 4) Generieren & prüfen
1. Klick **Generate Movie**.
2. Prüfe die Tabelle unter dem Movie:
   - Timestamp
   - Order type
   - Price/Qty
   - Wallet Balance Verlauf

---

## 5) Export (optional)
Mit **Export video (mp4)** kannst du ein mp4 rendern.

Wenn es langsam ist:
- Größere Step Size wählen (weniger Frames)
- Frames/Duration reduzieren
- Export preset auf „Fast“
