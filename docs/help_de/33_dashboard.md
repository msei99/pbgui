# Dashboards

Die **Dashboards**-Seite bietet eine vollständig anpassbare Portfolio-Übersicht für deine aktiven Passivbot-Instanzen.
Du kannst mehrspaltige Widget-Layouts erstellen, Daten mehrerer Benutzerkonten kombinieren und alles frei verschieben.

---

## Dashboard anzeigen

- Existiert nur ein Dashboard, wird es automatisch geöffnet.
- Bei mehreren Dashboards erscheint eine **Auswahlbox** — das gewünschte Dashboard auswählen.
- Alle Dashboards sind als Schnellzugriff-Schaltflächen in der Sidebar aufgelistet.
- Widgets aktualisieren sich automatisch im Hintergrund. Mit ↻ kann sofort neu geladen werden.

---

## Sidebar-Aktionen

| Schaltfläche | Aktion |
|--------|--------|
| ↻ | Aktuelles Dashboard neu laden |
| ➕ | Neues leeres Dashboard erstellen |
| ✎ | Aktuell angezeigtes Dashboard bearbeiten |
| 🗑 | Aktuelles Dashboard löschen |
| 📋 | Templates-Panel öffnen |

Im **Bearbeitungsmodus** wechselt die Sidebar zu:

| Schaltfläche | Aktion |
|--------|--------|
| 💾 | Änderungen auf Disk speichern |
| ✕ | Alle Änderungen verwerfen und vorherigen Zustand wiederherstellen |
| 🗑 | Aktuelles Dashboard dauerhaft löschen |

---

## Der Editor

Mit ✎ öffnet sich der Dashboard-Editor in einem eigenen Vollbild-Tab.

### Layout — Zeilen und Spalten

Die Toolbar oben steuert die Rasterstruktur:

- **Name** — Name des Dashboards (wird in der Sidebar angezeigt).
- **1 COL / 2 COL** — Raster auf 1 oder 2 Spalten umschalten.
  - **1 COL**: eine breite Zelle pro Zeile — ideal für breite Charts.
  - **2 COL**: zwei Zellen nebeneinander — z.B. Positions links, Orders rechts.
- **Zeilen** — Zeilen mit `+` / `−` hinzufügen oder entfernen.

### Widgets den Zellen zuweisen

Jede Zelle zeigt eine Kopfleiste mit einem **Typ-Badge** (z.B. `NONE`, `📊 PNL`, `📋 POSITIONS`, …).
Auf das Badge klicken oder einen Typ aus dem Dropdown wählen, um ein Widget zuzuweisen.
Alternativ einen Widget-Typ aus der **Palette** rechts in der Toolbar per Drag & Drop auf eine Zelle ziehen.

### Drag & Drop — Widgets verschieben

- **Im Editor**: Zelle an der dunklen Kopfleiste (mit Typ-Badge und 🗑-Icon) greifen und auf eine andere Zelle ziehen — die beiden Zellen tauschen die Position.
- **In der Live-Ansicht**: Widget am farbigen Titelbalken (z.B. „Positions", „Orders") greifen und auf ein anderes Widget ablegen. Der Tausch wird automatisch gespeichert.

### Zellgröße anpassen

Jede Zelle hat einen **Resize-Griff** unten rechts. Ziehen macht die Zelle höher oder kürzer. Die Höhe wird pro Zelle im Dashboard gespeichert.

### Zellkonfiguration

Unter der Kopfleiste zeigt jede Zelle ein kompaktes Konfigurationspanel:

- **Users** — von welchem/n Konto(en) Daten geladen werden. `ALL` = alle Konten.
- **Period / From / To / To now** — Zeitbereich (je nach Widget-Typ).
- **Link to Positions** (nur ORDERS) — Chip-Auswahl, um den Orders-Chart mit einem bestimmten Positions-Widget zu verknüpfen.
- **Mode** (PNL / ADG) — Chart-Stil umschalten (Balken, Linie, …).

---

## Widget-Typen

### ⚖️ BALANCE

Zeigt **aktuelles USDT/USDC-Guthaben, offenes PnL und Gesamtkapital** für ein oder alle Konten.
Der Benutzer-Selektor ist direkt im Widget-Kopf integriert — kein separates Konfigurationsfeld.
Am besten als kompakte Übersichtszelle ganz oben im Dashboard platzieren.

### 📊 PNL — Tägliches PnL

Ein **Balkendiagramm** des realisierten PnL pro Kalendertag.
Grüne Balken = profitabler Tag, rote Balken = Verlusttag.
Eine dünne kumulative Linie überlagert die Balken.

Konfiguration:
- **Mode** — `bar` (Standard) oder `line`
- **Period** — Vorgaben (`ALL_TIME`, `1_MONTH`, `3_MONTHS`, …) oder eigener Von-/Bis-Zeitraum
- **Users** — nach Konto filtern

### 📈 ADG — Durchschnittlicher Tagesgewinn

Ein **Liniendiagramm** des kumulativen USDT-Guthabens über die Zeit, zeigt die Wachstumskurve.
Der ADG-Wert (durchschnittlicher Tagesgewinn in %) wird im Widget-Kopf angezeigt.

Gleiche Zeitraum- und Benutzer-Steuerung wie PNL.

### 📉 P+L — Kumulatives PnL pro Symbol

Zeichnet **separate kumulative PnL-Linien** für jedes Symbol in einem Chart.
Zeigt auf einen Blick, welche Coins Gewinne oder Verluste über einen Zeitraum treiben.

### 💰 INCOME — Einnahmen nach Symbol

Ein **Liniendiagramm** der kumulativen Einnahmen über die Zeit, mit je einer Linie pro Symbol.
Nützlich, um die besten und schlechtesten Coins im gewählten Zeitraum zu identifizieren.

Zusätzliche Steuerung:
- **Last N** — nur die Top-N-Symbole nach absolutem Wert anzeigen
- **Filter** — Symbole unterhalb eines Mindestbetrags ausblenden

### 🏆 TOP — Top-Symbole

Rankt alle Symbole nach Gesamteinnahmen und zeigt die Top-N als horizontales Balkendiagramm.
Negative Einnahmen erscheinen rot. Gut für einen schnellen Performance-Überblick.

Konfiguration:
- **Top N** — Anzahl der anzuzeigenden Symbole
- **Period / From / To** — Zeitraum

### 📋 POSITIONS

Eine **Live-Tabelle** aller aktuell offenen Positionen der ausgewählten Konten.

Spalten: User · Symbol · Side · Size · uPnL · Entry · Price · DCA · Next DCA · Next TP · Positionswert

- Zeilen aktualisieren sich automatisch mit neuen Daten von der Börse.
- **Auf eine Zeile klicken** wählt diese Position aus — das verknüpfte **📝 ORDERS**-Widget lädt sofort den Kurs-Chart für dieses Symbol mit Order-Markierungen.
- Das Benutzer-Dropdown im Widget-Kopf filtert nach Konto.

### 📝 ORDERS

Ein **Candlestick-Chart** mit eingeblendeten Order-Markierungen (Entries, Take-Profits, DCA-Orders) für die im Positions-Widget ausgewählte Position.

Einrichtung:
1. Eine 📋 POSITIONS-Zelle und eine 📝 ORDERS-Zelle hinzufügen (2-Spalten-Layout empfohlen).
2. In der ORDERS-Zellkonfiguration den **Link to Positions**-Chip der passenden Positions-Zelle aktivieren (z.B. „Row 3 · Col 1").
3. In der Live-Ansicht eine Positions-Zeile anklicken — der Chart lädt automatisch.

Steuerung:
- **Timeframe-Schaltflächen** (1m 5m 15m 30m 1h 2h 4h 6h 12h 1d 1w) — Kerzenauflösung ändern.
- **Nach links scrollen** im Chart lädt ältere historische Kerzen.

---

## Templates

Templates sind **vorgefertigte Dashboard-Layouts**, die als Ausgangspunkt genutzt werden können.
Statt ein Raster von Grund auf aufzubauen, lädt man ein Template und die Zellen werden mit einer sinnvollen Widget-Anordnung befüllt.

### Template verwenden

1. In der Sidebar 📋 **Templates** klicken.
2. Ein Panel öffnet sich mit allen verfügbaren Templates als kleine Vorschau.
3. Auf ein Template klicken — das aktuelle Raster wird durch das Template-Layout ersetzt.
4. Benutzer, Zeiträume und andere Einstellungen in den einzelnen Zellen anpassen.
5. 💾 klicken zum Speichern.

> **Hinweis:** Ein Template anwenden überschreibt das aktuelle Raster. Zuvor ungespeicherte Änderungen gehen verloren.

### Verfügbare Templates

| Template | Layout | Inhalt |
|----------|--------|--------|
| **Overview 2×3** | 2 Spalten, 3 Zeilen | ⚖️ Balance, 💰 Income, 📊 PNL, 📈 ADG, 📋 Positions, 📝 Orders |
| **Single user** | 1 Spalte, 4 Zeilen | ⚖️ Balance, 📈 ADG, 📊 PNL, 📋 Positions übereinander |
| **Positions & Orders** | 2 Spalten, 1 Zeile | 📋 Positions links, 📝 Orders rechts — automatisch verknüpft |

---

## Dashboard erstellen

1. ➕ in der Sidebar klicken.
2. Einen **Namen** eingeben und **Create & Edit** klicken.
3. **1 oder 2 Spalten** sowie die Anzahl der **Zeilen** wählen.
4. Jeder Zelle einen Widget-Typ zuweisen (aus Palette ziehen oder Typ-Badge klicken).
5. Jedes Widget konfigurieren (Benutzer, Zeitraum, Verknüpfungen zwischen Zellen).
6. 💾 klicken — das Dashboard wird gespeichert und sofort in der Live-Ansicht angezeigt.

---

## Dashboard löschen

1. Dashboard im Editor öffnen (✎).
2. 🗑 in der Sidebar klicken.
3. Löschen bestätigen. Das Dashboard wird dauerhaft entfernt.

---

## Tipps

- Ein **2-spaltiges Layout** verwenden und ⚖️ **BALANCE** in der obersten Zeile platzieren für einen sofortigen Kapital-Überblick.
- Im 2-spaltigen Layout 📋 **POSITIONS** und 📝 **ORDERS** nebeneinander platzieren — ein Klick auf eine Position öffnet den Chart direkt im benachbarten Widget.
- 📈 **ADG** und 📊 **PNL** zusammen nutzen, um langfristiges Wachstum mit täglichen Schwankungen zu vergleichen.
- In der Live-Ansicht können Widgets durch Ziehen an der Titelleiste umsortiert werden — ohne den Editor zu öffnen.
- Dashboards mit vielen Zellen und `ALL`-Benutzer können langsamer laden — für bessere Performance einzelne Benutzer zuweisen.
