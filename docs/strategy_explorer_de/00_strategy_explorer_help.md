# Strategy Explorer - Hilfe

Strategy Explorer ist die gemeinsame visuelle Debugging- und Analyse-GUI fuer PB7- und PB8-Strategien. Beide Versionen verwenden denselben Seitenrahmen und Workflow; versionsspezifische Engines, Felder, Labels und nicht verfuegbare Controls passen sich automatisch an.

- Ein **Snapshot** zu einer gewaehlten **Analysis Time**.
- Eine begrenzte **Simulation** oder native Replay mit Fills.
- Ein **Compare**-Workflow fuer gespeicherte Resultate und frische Berechnungen.
- Ein **Movie Builder** fuer eine schrittweise Replay.

PB7 behaelt sein bestehendes lokales/PB7-Engine-Verhalten. PB8 verwendet native PB8-Berechnungen und Candle-Aufbereitung.

---

## Grundbegriffe

### Exchange / Coin
Strategy Explorer arbeitet immer auf einem Markt:

- **Exchange**: zum Beispiel `bybit`
- **Coin**: ein Markt, der fuer die gewaehlte Config und die lokalen Engine-Daten verfuegbar ist

PB7 verwendet seine konfigurierten lokalen OHLCV-Quellen. PB8 verwendet **PB8 native candles** und lehnt ein explizites OHLCV-Verzeichnis ausserhalb der freigegebenen PB8/PBGui-Roots ab. Ohne passende Candles rendert Strategy Explorer eine ausdrueckliche Unavailable-Snapshot-Shell mit uebergebener Config und Tuning-Feldern, kann aber keine nativen Ideal Orders berechnen oder das gewaehlte Fenster abspielen.

Exchange und Coin sind Analyse-Selektoren. Eine Aenderung schreibt weder die Exchanges der Config noch ihre moeglicherweise unterschiedlichen Long- und Short-Approved-Coin-Listen um.

### Analysis Time (wichtigster Regler)
**Start Date** und **Start Time** waehlen die erste Candle des Snapshot-Kontextfensters. **Analysis Time** ist die rechte Candle, an der der Snapshot berechnet wird.

- **Chart Context** laeuft vom gewaehlten Start vorwaerts.
- Die Candle am rechten Rand dieses begrenzten Fensters liefert Preis, Indikatoren und Order-State fuer den Snapshot.
- PB7 behaelt sein bestehendes lokales/PB7-Engine-State-Verhalten.
- PB8 berechnet native ideale Entry-Orders aus einer bereitgestellten **flachen Position**.
- PB8 berechnet Close-Orders separat aus einer **repraesentativen hypothetischen Position** zum gewaehlten Preis.

Der PB8-Snapshot erklaert das Strategieverhalten fuer diese bereitgestellten States. Er ist keine Prognose eines Live-Accounts, seiner Positionen oder seiner zukuenftigen Orders.

### Context window
Das Chart zeigt das begrenzte Fenster ab Start Date/Start Time:

- **Chart Context** bestimmt, wie viel vorwaerts laufende Candle-Historie angezeigt und wo der Snapshot-State berechnet wird.

---

## Varianten / Modi

### 1) Snapshot (einzelne Ansicht)
Der gemeinsame Snapshot rendert die Entry- und Close-Orders, Referenzlinien und Strategieparameter, welche die gewaehlte Version liefert.

PB8 leitet Tuning-Gruppen, Feldtypen, Auswahlwerte und Bereiche aus der installierten PB8-Runtime ab. Aenderungen werden in kanonische verschachtelte PB8-Pfade geschrieben, waehrend die vollstaendige uebergebene Config erhalten bleibt.

Bei PB7 ist dies die bestehende PB7/Rust-Berechnungsansicht. Bei PB8 stammen Entry- und Close-Ausgabe aus den beiden oben beschriebenen Supplied-State-Berechnungen. PB8-Close-Orders veranschaulichen daher eine repraesentative Position, nicht den historischen oder aktuellen Live-Account-State.

Dieser Modus hilft zum Beispiel bei folgenden Fragen:

- "Wie formen diese Parameter Entries und Closes?"
- "Warum ist ein Order-Level eng oder weit?"
- "Wie wirkt sich die Aenderung eines Strategieparameters auf den Snapshot aus?"

### 2) Simulation / native Replay
Die Stage **Simulation** durchlaeuft ein gewaehltes Candle-Fenster und zeichnet Fills auf.

- **PBGui Simulation** ist der bestehende lokale PB7-Candle-Walk.
- **PB7 Backtest Engine** verwendet den bestehenden PB7-Engine-Pfad.
- **PB8 Native Replay** fuehrt einen nativen PB8-Backtest im Speicher aus, ohne einen Result-Ordner zu schreiben.

PB8-Replay ist durch das gewaehlte Fenster und Server-Limits bewusst begrenzt: maximal 20.000 Replay-Candles und 2.000 angezeigte Fills. Sie startet mit PB8s nativem Flat-State; manuelle Startpositionen sind nicht verfuegbar, weil die native Replay-API sie nicht annimmt. Sie ist eine historische Replay, keine Live-Account-Prognose.

### 3) Compare
PB7 behaelt beide bestehenden Compare-Optionen:

- **PB7 Backtest Result vs PBGui Simulation vs PB7 Backtest Engine**
- **PBGui Simulation vs PB7 Backtest Engine**

PB8 bietet **Stored PB8 Result vs Fresh PB8 Replay**. Ein PB8-Result-Handoff behaelt den validierten Result-Pfad serverseitig hinter einer owner-gebundenen opaken Draft-ID; der Browser erhaelt oder bearbeitet diesen Pfad nicht. Compare liest die gespeicherten Fills und startet eine frische begrenzte native Replay fuer die uebergebene Config und das Zeitfenster. Compare meldet nur mit einem validierten Stored Result oder einer zur Laufzeit unterschiedlichen fixierten Config Erfolg und warnt, wenn das Candle-Limit nur einen Teil des gespeicherten Fill-Bereichs abdeckt.

### 4) Movie Builder
PB7 behaelt seine drei bestehenden Engines:

- **PBGui Simulation**: lokale Replay mit sich entwickelnden Grids und Fills.
- **PB7 Backtest Engine**: PB7-Engine-Fills und Upcoming-Fill-Ansicht.
- **PB7 fills.csv (from backtest)**: Visualisierung aufgezeichneter Result-Fills ohne Neuberechnung.

PB8 bietet **PB8 Native Replay**. Der Movie verwendet echte, nach Step aggregierte Candles und Fills aus der nativen PB8-Replay. PB8 upstream stellt keinen historischen Ideal-Order-Trace fuer jeden Frame bereit; PBGui kann deshalb keine exakten historischen resting Entry-/Close-Ladders pro Candle zeigen. Leere Order-Ladders pro Frame sind beabsichtigt. Aus Fills abgeleitete Positionsangaben sind nur im angezeigten Fill-Bereich verfuegbar und enden beim Fill-Anzeigelimit.

---

## Direkte PB8-Handoffs
Du kannst den gemeinsamen PB8 Strategy Explorer direkt oeffnen aus:

- **PB8 Run**
- **PB8 Backtest**
- PB8 **Backtest Results**
- Resultaten im PB8 **Pareto Explorer**

Diese Handoffs uebergeben die kanonische Config und passende Overrides ueber einen authentifizierten opaken Draft. Koennen referenzierte Sparse Overrides nicht geladen werden, blockiert PBGui den Handoff, statt eine unvollstaendige Config zu oeffnen. Der PB8-Backtest-Results-Handoff behaelt zusaetzlich die validierte Stored-Result-Provenance fuer **Compare**, ohne einen Dateisystempfad in Seite oder URL offenzulegen. Initial werden die validierte Source-Exchange des Result-Datasets, der erste Approved Coin mit gespeicherten Fills und dessen UTC-Zeit gewaehlt; validierte Dataset-Metadaten dienen als Fallback.

PB8-Parameter innerhalb von 5% ihrer aktiven unteren oder oberen Optimize-Bound werden direkt in den Tuning-Panels markiert. Fehlt fuer ein Feld eine Optimize-Bound, wird der Parameterbereich der installierten Runtime verwendet. Fuer den Vergleich zweier Pareto-Kandidaten waehle den ersten Kandidaten, klicke **Pin Explorer Baseline**, waehle einen anderen Kandidaten desselben Results und oeffne **Strategy Explorer**. Die seitenlokale Baseline wird bei Result-Wechsel oder Reload geloescht. Der owner-gebundene Draft enthaelt beide Configs; **Compare** fuehrt beide ueber denselben nativen PB8-Replay-Vertrag aus.

PB8-Strategy-Explorer-Drafts gehoeren zur aktuellen authentifizierten Session und verfallen nach 10 Minuten ohne Nutzung; ein API-Neustart loescht sie sofort. Browser-Requests verwenden das Same-Origin-HttpOnly-Session-Cookie, niemals ein gerendertes oder gespeichertes Bearer-Token. Maximal zwei native PB8-Helper-Operationen laufen gleichzeitig. Runtime-Update-Konflikte oder belegte Helper-Slots liefern einen wiederholbaren Busy-Fehler. Aenderungen an Config oder Operations-Controls ersetzen aeltere Browser-Requests, damit spaete Antworten die aktuelle Auswahl nicht ueberschreiben.

---

## Long/Short Grid Anzeige (wie lesen?)
Strategy Explorer kann je nach Config Long und/oder Short anzeigen.

### Long
- **Long entry grid**: Buy-Orders, die eine Long-Position oeffnen oder erhoehen.
- **Long close grid**: Sell-Orders, die eine Long-Position reduzieren oder schliessen.

### Short
- **Short entry grid**: Sell-Orders, die eine Short-Position oeffnen oder erhoehen.
- **Short close grid**: Buy-Orders, die eine Short-Position reduzieren oder schliessen.

### Beide Seiten aktiv
Wenn Long und Short aktiv sind:

- Snapshot kann die Ausgabe beider Seiten zeigen.
- Movie Builder bietet fuer **Side** die Werte `Auto`, `Long` und `Short`.

Beachte, dass PB8-Snapshot-Closes fuer jede Seite eine separate repraesentative hypothetische Position verwenden.

---

## Haeufige Probleme

### "Ich sehe keine Orders/Marker"
Pruefe zuerst den gewaehlten Markt, die Side und das Zeitfenster:

- Analysis Time oder Movie-Fenster ueberlappt moeglicherweise keine Fills.
- Eine PB8-Side kann durch ihre Risk-/Position-Einstellungen deaktiviert sein.
- PB8 Movie Builder enthaelt keine historischen Ideal-Order-Ladders pro Frame; nutze Snapshot, um native ideale Orders fuer einen bereitgestellten State zu untersuchen.
- Ein PB8-Backtest-Results-Handoff liefert die Stored-Result-Provenance fuer Compare; pruefe, dass das gewaehlte Fresh-Replay-Fenster die gespeicherten Fills abdeckt.

---

## Naechste Schritte
- Nutze die Tutorials im Docs-Selector direkt im Strategy Explorer.
