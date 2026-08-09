# Tutorial: Long vs Short Grids verstehen

Dieses Tutorial erklaert, wie die gemeinsame PB7/PB8 Strategy Explorer GUI Long- und Short-Ausgaben darstellt und wie sich typische Fehlinterpretationen vermeiden lassen.

---

## 1) Long Grids
### Long entry grid
- Repraesentiert Buy-Orders, die eine Long-Position oeffnen oder erhoehen.
- Erscheint je nach Strategie und State meistens unter dem aktuellen Preis.

### Long close grid
- Repraesentiert Sell-Orders, die eine Long-Position reduzieren oder schliessen.
- Erscheint meistens ueber dem Position-Preis.

Bei einem PB8-Snapshot stammen Long-Entries aus dem bereitgestellten flachen State. Long-Closes stammen aus einer separaten repraesentativen hypothetischen Long-Position zum gewaehlten Preis.

---

## 2) Short Grids
### Short entry grid
- Repraesentiert Sell-Orders, die eine Short-Position oeffnen oder erhoehen.
- Erscheint je nach Strategie und State meistens ueber dem aktuellen Preis.

### Short close grid
- Repraesentiert Buy-Orders, die eine Short-Position reduzieren oder schliessen.
- Erscheint meistens unter dem Position-Preis.

Bei einem PB8-Snapshot stammen Short-Entries aus dem bereitgestellten flachen State. Short-Closes stammen aus einer separaten repraesentativen hypothetischen Short-Position zum gewaehlten Preis.

---

## 3) Beide Seiten aktiv
Wenn Long und Short in der Config aktiviert sind:

- Snapshot kann die Ausgabe beider Seiten zeigen.
- Movie Builder laesst dich `Auto`, `Long` oder `Short` waehlen.

Wenn nur eine Seite Fills hat, hat die native oder lokale Replay moeglicherweise nur eine Richtung gehandelt. Die repraesentativen PB8-Snapshot-Positionen bedeuten nicht, dass diese Positionen historisch oder in einem Live-Account existierten.

---

## 4) Trailing-Linien
Trailing ist pfadabhaengig.

- Snapshot-Referenzlinien erklaeren einen bereitgestellten State zur Analysis Time.
- Die lokalen/PB7-Engine-Tools behalten ihr bestehendes Verhalten.
- PB8 Native Replay liefert echte Candles und Fills, aber PB8 upstream stellt weder historische Ideal-Order-Ladders pro Frame noch einen vollstaendigen Resting-Order-Trace bereit.

Leite aus einem PB8-Snapshot oder Movie keine exakte historische Order-Sequenz ab.

---

## 5) Debug-Checkliste
Wenn Grids invertiert oder falsch wirken:

- **Side** (`Long` oder `Short`) pruefen.
- **Exchange** und **Coin** pruefen.
- Pruefen, ob Analysis Time im relevanten Zeitraum liegt.
- Bei PB8-Closes die Annahme der repraesentativen hypothetischen Position beachten.
- Fuer einen Result-Vergleich den direkten Backtest-Results-Handoff verwenden, damit die gespeicherte PB8-Provenance hinter dem opaken Draft bleibt.
