# Tutorial: Compare

Compare verwendet dieselbe gemeinsame GUI, aber versionsspezifische Quellen:

- PB7-Backtest-Fills, **PBGui Simulation** und **PB7 Backtest Engine**
- Ein **Stored PB8 Result** und eine **Fresh PB8 Replay**

---

## 1) Aus einem Result starten (empfohlen)
1. Oeffne PB7 oder PB8 **Backtest Results**.
2. Waehle genau ein Result und klicke **Strategy Explorer**.

Du kannst auch einen PB8-Kandidaten im **Pareto Explorer** oeffnen. PB8 Run und PB8 Backtest bieten direkte Config-Handoffs, aber nur ein Result-Handoff liefert die gespeicherte Result-Provenance fuer **Stored PB8 Result vs Fresh PB8 Replay**.

Fuer den Vergleich zweier PB8-Pareto-Kandidaten waehle den ersten Kandidaten und klicke **Pin Explorer Baseline**. Waehle danach einen anderen Kandidaten desselben Results und klicke **Strategy Explorer**. Ein Result-Pfad ist nicht erforderlich; der owner-gebundene Draft enthaelt beide Configs und Compare bezeichnet sie als aktuelle Config und fixierte Baseline. Die Baseline ist seitenlokal und wird bei Result-Wechsel oder Reload geloescht. Fehlende referenzierte Override-Dateien blockieren den Handoff, statt ignoriert zu werden.

Bei PB8 speichert der authentifizierte Handoff den validierten Result-Pfad serverseitig und oeffnet Strategy Explorer mit einer owner-gebundenen opaken Draft-ID. Der Result-Pfad erscheint weder in der Browser-Seite noch in der URL.

---

## 2) Compare starten
1. Oeffne **Compare**.
2. PB7-Nutzer waehlen einen der bestehenden PB7-Compare-Modi.
3. PB8-Result-Nutzer waehlen **Stored PB8 Result vs Fresh PB8 Replay**. Ein Pareto-Handoff mit zwei Kandidaten verwendet dasselbe Panel fuer Replay A gegen Replay B.
4. Bei einem gespeicherten PB8-Result startet PBGui die Replay automatisch eine konfigurierte Candle vor dem ersten gespeicherten Fill des gewaehlten Coins. Setze **Compare max candles** hoch genug, um den letzten zu vergleichenden Fill zu erreichen.
5. Klicke **Start Compare**.

PB8 Compare liest begrenzte Fills aus dem uebergebenen gespeicherten Result und fuehrt eine frische begrenzte native Replay mit dessen uebergebener Config aus. Gespeicherte Fills werden zuerst auf den gewaehlten Coin und das exakte Fresh-Replay-Fenster gefiltert; erst danach wird das Fill-/Order-Limit angewendet, sodass aeltere oder ausserhalb liegende Fills es nicht verbrauchen. Compare verwendet Fill-Datensaetze und rekonstruiert keine exakten historischen resting Orders.
Ein Compare ohne Quelle oder mit zur Laufzeit identischen Configs wird abgelehnt. Fills stimmen ueberein, wenn ihre Zeitstempel innerhalb der konfigurierten Toleranz von standardmaessig 1.000 ms liegen, die Order Types gleich sind und Preis sowie Menge auf dieselben nativen Exchange-Steps fallen. Die Dateireihenfolge dient nicht als Fill-Identitaet. Eine sichtbare Teilabdeckungswarnung bedeutet, dass das Candle-Limit den letzten gespeicherten Fill nicht erreicht hat.

Interpretation:
- Fills nur im Stored Result oder nur in der Fresh Replay koennen auf Unterschiede bei Config, Markt, Startzeit, Daten oder Engine-Version hinweisen.
- Stored-only- oder Fresh-only-Zeilen bedeuten, dass auf der Gegenseite innerhalb der Timestamp-Toleranz kein Event mit passendem Order Type sowie Exchange-quantisiertem Preis und Menge existiert.
- Uebereinstimmende Fills validieren diesen begrenzten Replay-Vergleich, nicht kuenftiges Live-Account-Verhalten.

---

## 3) Typischer Debug-Workflow
1. Dieselben Werte fuer **Exchange** und **Coin** pruefen.
2. Ueberlappung zwischen gespeicherten Fill-Timestamps und gewaehltem Replay-Fenster pruefen.
3. Pruefen, dass uebergebene Config und Coin-Overrides die beabsichtigten Versionen sind.
4. Compare mit aktiviertem **Mismatches only** starten und fuer passenden Kontext bei Bedarf deaktivieren.
5. Mit Movie Builder Candles und Fills untersuchen; keine historischen PB8-Ideal-Order-Ladders pro Frame erwarten.
