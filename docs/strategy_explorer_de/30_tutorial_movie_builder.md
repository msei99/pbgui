# Tutorial: Movie Builder

Movie Builder erzeugt eine schrittweise Replay im gemeinsamen PB7/PB8-Strategy-Explorer-Seitenrahmen.

---

## 1) Fenster so waehlen, dass es alles abdeckt
Movie Builder wird gesteuert ueber:

- **Step Size** (zum Beispiel 1m, 5m, 1h, 4h)
- **Duration** (Preset) oder **Frames** (Custom)

Gesamtabdeckung:

- Bei einem Duration-Preset werden Frames aus Duration / Step Size berechnet.
- Bei Custom entspricht die Abdeckung ungefaehr Frames x Step Size.

Ein PB7-Result-Handoff kann das aufgezeichnete Fill-Fenster ausrichten. Die PB8-Movie-Erzeugung verwendet den gewaehlten Start und die Duration fuer eine frische begrenzte native Replay; sie ist weder Wiedergabe des gespeicherten Resultats noch eines Live-Accounts.

---

## 2) Movie engine auswaehlen
### PBGui Simulation (PB7)
Nutzen wenn:
- Du eine lokale PB7-Replay mit sich entwickelnden Grid-Ladders, Trailing-Linien und Fills sehen willst.

Abwaegung:
- Es ist eine lokale Simulation und sie kann von der PB7-Backtest-Semantik abweichen.

### PB7 Backtest Engine
Nutzen wenn:
- Du PB7-Engine-Fills und die bestehende Upcoming-Fill-Ansicht sehen willst.

Abwaegung:
- Sie stellt nicht fuer jede Candle vollstaendige Open-Grid-Ladders bereit.

### PB7 fills.csv (from backtest)
Nutzen wenn:
- Du aufgezeichnete Fills eines abgeschlossenen PB7-Backtests ohne Neuberechnung visualisieren willst.

Abwaegung:
- Sie kann weder neue Fills noch einen historischen Ideal-Order-Trace erzeugen.

### PB8 Native Replay
Nutzen wenn:
- Du Movie-Frames aus echten Candles und Fills einer frischen begrenzten nativen PB8-Replay sehen willst.

Wichtige Begrenzung:
- PB8 upstream stellt historische Ideal Orders nicht fuer jeden Replay-Frame bereit. PBGui laesst deshalb Entry-/Close-Order-Ladders pro Frame leer, statt exakte historische resting Orders zu behaupten. Candle-Pfad und Fills sind echte Replay-Ausgabe; aus Fills abgeleitete Positionsangaben enden beim Anzeigelimit. Untersuche **Snapshot** separat fuer native ideale Orders zu einem bereitgestellten State.
- Gestrichelte **Upcoming Entries** und **Upcoming Closes** sind aus angezeigten Replay-Fills abgeleitete Vorschauen und keine nativen historischen Resting-Order-Ladders. EMA-High/Low-Traces erscheinen nur, wenn die gewaehlte Engine echte EMA-Band-Werte liefert; PB8 Native Replay laesst sie normalerweise weg.

PB8-Generierung ist auf 2.000 Frames, 20.000 Replay-Candles und 2.000 angezeigte Fills begrenzt.

---

## 3) Long/Short waehlen
Wenn beide Seiten aktiv sind, waehle **Side**:

- `Auto` (bevorzugt Long)
- `Long`
- `Short`

Wenn eine Seite keine Fills hat, waehle die andere. Bei PB8 ist eine leere Order-Ladder auch dann erwartet, wenn Fill-Marker vorhanden sind.

---

## 4) Generieren und pruefen
1. Klicke **Generate Movie**.
2. Starte die Wiedergabe mit Plotlys **Play**, **Slow** oder **Very Slow** unterhalb des Charts. **Pause** stoppt beim aktuellen Frame.
3. Ziehe Plotlys Frame-Slider unterhalb des Charts, um eine bestimmte Candle, einen Order-Zustand oder Fill-Marker zu pruefen.
4. Fahre nahe einem Endpunkt ueber eine gestrichelte rote oder gruene Entry-/Close-Linie, um ihren exakten Preis zu sehen.
5. Solange die Wiedergabe pausiert ist, kannst du mit den Pfeiltasten links und rechts exakt einen Frame zurueck oder vor gehen. Wenn ein Formularfeld fokussiert ist, werden die Pfeiltasten nicht abgefangen.
6. Pruefe in der Fills-Tabelle:
   - Timestamps
   - Order Type
   - Preis und Menge
   - Verlauf von Wallet Balance und Position

Bei PB8 bleiben die Entry/Close-Orderzaehler der Frame-Tabelle null, weil kein historischer nativer Order-State verfuegbar ist. Gruene `B`- und rote `S`-Marker im Chart sind tatsaechliche Replay-Fills an ihren UTC-Zeitstempeln. Mehrminuetige Candles sind vollstaendige OHLCV-Aggregationen ihres einminuetigen Replay-Intervalls und keine einzeln gesampelten Minuten.
Die Movie-X-Achse ist auf das sichtbare Candle-Fenster jedes Frames fixiert, damit Resize oder Refresh den Anfang der gestrichelten Linien nicht vom linken Chartrand weg verschieben.

Bei PB8 behaelt ein Browser-Refresh im selben Tab den aktiven Bereich, freigegebene nicht sensible Config-Bereiche, Movie-Regler und bis zu ungefaehr 3 MiB generierte Movie-Daten fuer 24 Stunden in `sessionStorage`. Ein sensibler Config-Key verhindert das Caching dieser Config. Der gecachte Bereich wird vor dem wiederhergestellten Plotly-Chart sichtbar geschaltet; danach wird dessen Groesse erneut berechnet. Authentifizierungsdaten und der owner-gebundene Source-Result-Provenance-Pfad werden nie gespeichert. Fehlt der Server-Draft, baut PBGui den Snapshot aus diesem Cache neu auf. Ein zu grosser Movie wird nur dann automatisch neu generiert, wenn Movie Builder der wiederhergestellte aktive Bereich war und der Cache einen zuvor generierten Movie vermerkt. Simulation- und Compare-Resultate werden nicht wiederhergestellt.

**Stop Movie Builder** bricht die aktive Helper-Operation oder den MP4-Export der aktuellen Session ab. Aenderungen an Movie-Controls ersetzen ein aelteres Resultat.

Interpretiere einen PB8-Movie weder als Live-Account-Prognose noch als exakte Aufzeichnung historisch resting Orders.

---

## 5) Export (optional)
Mit **Export MP4** kannst du ein eigenstaendiges Video rendern.

PB7 und PB8 teilen einen Encoder-Slot. Wiederhole den Versuch, wenn bereits ein Export laeuft. Der Export ist auf 2.500 Frames und 512 MiB Ausgabe begrenzt; PB8 lehnt zusaetzlich Export-Payloads ueber 16 MiB ab.

Wenn der Export langsam ist:
- Groessere Step Size waehlen.
- Frames oder Duration reduzieren.
- Das Export-Preset **Fast** verwenden.
