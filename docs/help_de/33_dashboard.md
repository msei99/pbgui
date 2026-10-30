# Dashboards

Öffne **Information → Dashboards** und anschließend **System → PBGUI Services**, um zu Services zu wechseln. Die Hauptnavigation erhält den Anwendungspfad (zum Beispiel `/mounted`), auch wenn die API-Basis des Dashboards auf `/api` endet. Implizite Präfixe werden nur aus API-Basen desselben Ursprungs abgeleitet; ein ausdrücklich konfigurierter API-Ursprung bleibt unterstützt.

**Guide** öffnet sich über dem aktuellen Dashboard, ohne dessen Adresse oder Seitenleistenfilter zu ändern. Die lokalen Skripte und Hilfeabrufe behalten denselben Anwendungspfad bei.

Ein Browser-Refresh behält das ausgewählte Dashboard, den Anzeige-/Bearbeitungsmodus und den Seitenleistenfilter bei und lädt aktuelle Inhalte. Die Dashboard-Liste aktualisiert sich automatisch; ein manueller Listen-Refresh ist nicht nötig. Ist das ausgewählte Dashboard nicht mehr vorhanden, zeigt die Seite einen Hinweis und die Dashboard-Liste.

Beim Löschen des aktiven Dashboards wird dessen Inhaltsrahmen geleert, bevor der leere Zustand angezeigt wird.

Cancel verwirft den Layout-Entwurf nach Abschluss laufender automatischer Speicherungen. Schlägt das Verwerfen fehl, bleibt der Editor für einen erneuten Versuch geöffnet.

Beim Erstellen mit ausgewählten Benutzern werden die Benutzerfilter der Widgets auf den jeweiligen Zielbenutzer gesetzt. Bei mehreren Benutzern und einem eigenen Namen entstehen Namen nach `Name_Benutzername`; bei einem Benutzer bleibt der eigene Name erhalten. Ohne Benutzerauswahl bleiben die Vorlagenfilter erhalten.

Nach einer freigegebenen PBGui-AI-Erstellung oder Layout-Aenderung aktualisiert die offene Dashboard-Seite ihre Liste und laedt das betroffene Dashboard sofort neu. Der Reload ist generation-sicher: Ein manueller Dashboard-Wechsel waehrend des laufenden Refreshs wird niemals durch den verspaeteten AI-Abschluss ueberschrieben.

Die **Dashboards**-Seite bietet eine vollständig anpassbare Portfolio-Übersicht für deine aktiven Passivbot-Instanzen.
Du kannst mehrspaltige Widget-Layouts erstellen, Daten mehrerer Benutzerkonten kombinieren und alles frei verschieben.

Dashboard-APIs, Renderer-Skripte und WebSockets bleiben auf dem eigenen PBGui-Origin und behalten einen serverseitig konfigurierten Mount-Praefix. Ein `api_base`-Queryparameter kann keinen anderen Server auswaehlen. Editor-/Template-Nachrichten werden nur vom erwarteten Same-Origin-Parent oder Iframe akzeptiert; fremde Seiten koennen die Seiten nicht einbetten.

Balance-TWE addiert die absoluten Positionswerte von Long- und Short-Positionen. Bei älteren Positionen ohne Side wird Long oder Short aus dem Vorzeichen der Größe bestimmt; der Positionswert wird positiv angezeigt.

Das ADG-Diagramm füllt alle Tage des gewählten Zeitraums vor dem ersten Trade auf. Im PNL zeigt Daily die täglichen Erträge und Cumulative deren laufende Summe. P+L hält Datums- und Wertbeschriftungen sichtbar und verarbeitet Zeiträume ohne Trades.

---

## Dashboard anzeigen

- Existiert nur ein Dashboard, wird es automatisch geöffnet.
- Bei mehreren Dashboards erscheint eine **Auswahlbox** — das gewünschte Dashboard auswählen.
- Alle Dashboards sind als Schnellzugriff-Schaltflächen in der Sidebar aufgelistet.
- Widgets aktualisieren sich automatisch im Hintergrund. Mit ↻ kann sofort neu geladen werden.
- Wird in der Live-Ansicht ein User-, Period- oder anderes Karten-Control geändert, erscheint 💾 in der Sidebar. Damit werden diese Ansichts- und Layout-Einstellungen gespeichert.
- Im Edit Mode verschieben Live-Updates den Neuaufbau eines Widgets, solange darin ein Select, Datumsfeld, Zahlenfeld oder Button aktiv ist. Geoeffnete Period-/User-Controls bleiben dadurch offen; die neuesten ausstehenden Daten werden nach dem Fokusverlust einmalig nachgezogen.
- Bitunix- und WEEX-PB8-User verwenden PB8s eigene read-only Clients fuer Wallet-Balance, Positionen, Orders, Preise/OHLCV und aus Fills abgeleitetes realisiertes Trade-PnL. Die Clients laufen in isolierten kurzlebigen Prozessen und werden nach jedem Request geschlossen; ein begrenzter Snapshot vermeidet doppelte Account-Aufrufe.
- Funding-Income ist im Bitunix-Connector von PB8 v8.1.0 nicht verfuegbar. Die Performance-Widgets zeigen dort daher fill-basiertes realisiertes PnL und Fees, ohne vollstaendige Funding-Paritaet zu behaupten. Fuer die Produktionsabnahme bleiben reale read-only Credentials zur Pruefung von Responses und Pagination erforderlich.
- Direkter **Market close** bleibt fuer Bitunix und WEEX sichtbar, aber deaktiviert, bis Hedge-/One-Way- und Close-Order-Vertraege je Exchange separat live getestet sind. Panic, Graceful Stop und Take Profit Only bleiben verfuegbar, da sie die Passivbot-Config aktualisieren und keine Exchange-Order-Parameter raten.

---

## Bitget Classic und UTA

PBGui unterstuetzt die native Bitget-UTA-USDT-Perpetual-Wallet-Balance sowie vollstaendige REST-Snapshots von Positionen und Orders. Die Balance ist der USDT-Wallet-Bestand, nicht die Multi-Asset-Account-Equity. Private Bitget-WebSockets sind in diesem ersten Release fuer Classic und UTA bewusst deaktiviert: Chart-Positionen und -Orders im Dashboard werden alle 5 Sekunden per vollstaendigem REST-Snapshot aktualisiert; andere Account-Ansichten nutzen PBData-Polling, ohne globale Fuenf-Sekunden-Latenzgarantie. Oeffentliche Candle-WebSockets bleiben unveraendert.

Direkter **Market close** ist fuer UTA mit Begruendung deaktiviert, da UTA-Close-Orders nicht Teil dieses ersten Rollouts sind. Bei verifiziertem Classic-Modus bleibt die Aktion verfuegbar; ein unbekannter Account-Modus blockiert sie. Grenzen der UTA-Income- und Execution-Historie beschreibt der PBData-Guide.

## Sidebar-Aktionen

| Schaltfläche | Aktion |
|--------|--------|
| ↻ | Aktuelles Dashboard neu laden |
| ➕ | Neues leeres Dashboard erstellen |
| ✎ | Aktuell angezeigtes Dashboard bearbeiten |
| 🗑 | Aktuelles Dashboard löschen |
| 📋 | Templates-Panel öffnen |
| 💾 | Änderungen an Live-Ansicht, Layout oder Karten-Controls speichern (nur bei Bedarf sichtbar) |

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

Loslassen ausserhalb des Editors oder Verlassen des Browserfensters beendet die Groessenaenderung und gibt das Scrollen wieder frei.

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
Cumulative zeigt stattdessen eine Linie mit der laufenden Summe verfügbarer gültiger Tageswerte.
Fehlende oder ungültige Werte erscheinen als Lücken ohne Wertbeschriftung, nicht als Null. Die Summe wird mit späteren gültigen Tagen fortgesetzt; sie garantiert keine vollständige Historie. Bei einem Summenüberlauf bleiben die weiteren kumulativen Werte nicht verfügbar, während gültige Tageswerte sichtbar bleiben.
Echte tägliche Nullwerte zeigen `0.00` an der Nulllinie und bleiben dadurch von Lücken unterscheidbar.

Konfiguration:
- **Mode** — `bar` (Tageswerte, Standard) oder `line` (kumulative Summe)
- **Period** — Vorgaben (`ALL_TIME`, `1_MONTH`, `3_MONTHS`, …) oder eigener Von-/Bis-Zeitraum
- **Users** — nach Konto filtern

#### Sichere manuelle Prüfung (#427)

1. Eine isolierte Testinstallation oder einen Fixture-Server ohne Weiterleitung zu Produktivsystemen oder Providern verwenden. Der Fixture-Betreiber bereitet mindestens vier synthetische Dashboards (**Alpha**, **PNL gaps**, **Beta**, **Gamma**) vor, damit die Seitenleistensuche verfügbar ist. **PNL gaps** benötigt ein PNL-Widget, Testbenutzer `alice` und Zeitraum `CUSTOM:2026-09-01:2026-09-04`. Die synthetische Antwort von `/api/dashboard/pnl_data` liefert für diese vier Tage Tageswerte `[5, null, "3", 0]` und kumulative Werte `[5, null, "8", 8]`. Keine Kontohistorie verändern, um Fehlerfälle zu erzeugen.
2. **Information → Dashboards → PNL gaps** öffnen. Unter **Daily PNL** den Wert **Mode → bar** wählen. Erwartet: sichtbare Beschriftungen `5.00` und `3.00` am 1. und 3. September, keine Wertbeschriftung am 2. September und sichtbares `0.00` an der Nulllinie des 4. September. Das Diagramm bleibt ohne Fehler bedienbar.
3. **Mode → line** wählen. Erwartet: zwei getrennte Linienabschnitte/Pfade mit einer Lücke am 2. September; die späteren Werte am 3. und 4. September sind jeweils `8`. Zurück zu **bar** wechseln: Die Nullbeschriftung erscheint wieder, die Lücke bleibt unbeschriftet.
4. Den Stiftbutton mit Titel **Edit current dashboard** anklicken. Schritte 2–3 in der Editorvorschau wiederholen. Erwartet: dieselben Beschriftungen und Lücken. Mit **Cancel edit** ohne Speichern verlassen; **Save dashboard** nicht verwenden.
5. Falls die isolierte Fixture Ereignisse senden kann, das Diagramm um diese Tage zoomen, **bar** ausgewählt lassen und `PNL` in die Dashboard-Suche der Seitenleiste eingeben. Der Fixture-Betreiber ändert ausschließlich den ersten Tages-/Summenwert auf `6` (spätere Summen auf `9`) und sendet ein synthetisches `income_updated` für `alice`. Erwartet: Die erste Beschriftung wird automatisch `6.00`; am 4. September bleibt `0.00` sichtbar. Lücke, Zoom, Modus, Dashboardauswahl und Suche bleiben erhalten. Dieser Schritt benötigt Fixture-Unterstützung; keinen echten Trade erzeugen.
6. Den Browser bei ausgewähltem **PNL gaps** neu laden. Erwartet: dasselbe Dashboard und dieselbe Seitenleistensuche mit aktuellen Fixture-Werten, einschließlich Nullbeschriftung und Lücke. Eine Aktualisierungsaktion innerhalb der Anwendung ist nicht nötig.

Diese Schritte stehen als Nutzerabnahme aus und behaupten keine durchgeführte Live-GUI-Prüfung. Nichtendliche JavaScript-Werte, fehlerhafte Aggregate und Summenüberlauf sind durch isolierte automatische Tests abgedeckt; dafür niemals eine echte Datenbank beschädigen.

### 📈 ADG — Durchschnittlicher Tagesgewinn

Ein **Liniendiagramm** des kumulativen USDT-Guthabens über die Zeit, zeigt die Wachstumskurve.
Der ADG-Wert (durchschnittlicher Tagesgewinn in %) wird im Widget-Kopf angezeigt.

Gleiche Zeitraum- und Benutzer-Steuerung wie PNL.

### 📉 P+L — Gewinne und Verluste

Zeigt **getrennte Gewinn- und Verlustbalken** für die gewählten Benutzer und den gewählten Zeitraum.

- **Period** legt den Datumsbereich fest.
- **Sum Period** legt die Aggregation fest: `DAY`, `WEEK`, `MONTH`, `YEAR` oder `ALL_TIME`.
- `YEAR` gruppiert Erträge innerhalb des gewählten Zeitraums nach UTC-Kalenderjahr.
- `ALL_TIME` zeigt ein einziges Aggregat für den gewählten Zeitraum und die gewählten Benutzer; **Period** bleibt wirksam. Ohne passende Erträge sind Gewinne und Verluste jeweils null.

Dashboard oder Ansichtslayout speichern, damit die gewählte Aggregation nach dem Neuladen erhalten bleibt.
Beim Wechsel von **Sum Period** wird das Diagramm neu skaliert, damit die neuen Summen sichtbar bleiben. Automatische Datenaktualisierungen erhalten den aktuellen Zoom.

### 💰 INCOME — Einnahmen nach Symbol

Ein **Liniendiagramm** der kumulativen Einnahmen über die Zeit, mit je einer Linie pro Symbol.
Nützlich, um die besten und schlechtesten Coins im gewählten Zeitraum zu identifizieren.

Im Table-Modus stehen die neuesten Income-Zeilen zuerst. Ein Klick auf **Date** kehrt die Sortierrichtung um.

#### Income-Backup wiederherstellen

Die Restore-Auswahl stellt den gesamten lokalen Snapshot von `pbgui.db` wieder her, nicht nur die sichtbaren Income-Zeilen. Die separate Trades-Datenbank bleibt unveraendert.

- Backups enthalten bestaetigte WAL-Daten. Vor einer Aenderung werden eine private Kopie, die SQLite-Integritaet und das PBGui-Schema geprueft; verlinkte Dateien und unvollstaendige WAL-abhaengige Backups werden abgelehnt.
- Das Snapshot-Schema wird vor der Ausfuehrung von Integritaetsausdruecken geprueft. Trigger, Views, virtuelle Tabellen und nicht unterstuetzte Ausdruecke werden abgelehnt; SQLite-Parsing- und Speicherlimits begrenzen auch uebergrosse Inhalte. Restore ist fuer kompatible PBGui-Snapshots gedacht, nicht fuer beliebige SQLite-Dateien.
- Restore erfolgt in einer SQLite-Transaktion, ohne die aktive Datenbankdatei auszutauschen oder WAL-/SHM-Dateien zu loeschen. Bestehende Leser koennen ihren aktuellen Snapshot beenden; anschliessende Abfragen sehen den wiederhergestellten Stand.
- Restore stoppt PBData nicht. Inkrementelle History-Abfragen, lokale DB-Sync-Zyklen und Benutzerimporte koennen nicht gleichzeitig mit einem Restore zugelassen werden. Eine belegte Datenbank, aktive Abfrage, lokale Installation oder Master-Operation kann HTTP 409 ausloesen; vor einem neuen Versuch deren Ende abwarten.
- Der API-Neustart ist waehrend des Restores gesperrt. Fehlgeschlagene oder durch Timeout abgebrochene, noch nicht abgeschlossene Restores werden zurueckgerollt; temporaere Dateien und Sperren werden freigegeben.
- Die Live-Erfassung laeuft danach weiter und kann aktuelle Balances, Preise, Positionen und History sofort wieder aktualisieren. Nach Installation dieses Fixes API und PBData neu laden, bevor Restore verwendet wird, damit alle Prozesse die neue Koordination nutzen; aeltere abgekoppelte DB-Sync-Worker zuerst fertig laufen lassen.
- Income-Loeschungen werden abgelehnt, wenn ihr automatisches Sicherheitsbackup nicht erstellt werden kann.

Zusätzliche Steuerung:
- **Last N** — `0` zeigt den kumulativen Chart pro Symbol; Werte groesser als `0` zeigen die neuesten N Income-Zeilen als Tabelle
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

#### Positionen verwalten

Der **Manage**-Button neben dem Positions-Titel öffnet einen verschiebbaren und skalierbaren Dialog für die aktuell sichtbaren Positionen.

Mit **Escape**, **✕** oder **Close** wird er geschlossen und der Tastaturfokus kehrt zu **Manage** zurück. **Tab** und **Shift+Tab** bewegen den Fokus innerhalb des aktiven Dialogs, auch bei automatisch aktualisierten Positionsfeldern. Eine Config-Vorschau wird mit **Escape** zuerst geschlossen; der Fokus kehrt zu ihrem Vorschau-Button zurück. Ein offener Guide oder Bestätigungsdialog hat Vorrang vor dem Manage-Dialog, auch in der umgebenden Dashboard-Seite. Escape aus dem eingebetteten Dialog schließt nur dieses höhere Fenster; Tab setzt den Fokus auf dessen Schließen-Steuerung. Ein Klick auf den dunklen Hintergrund schließt Dialoge bewusst nicht, entsprechend der PBGui-Vorgabe zum expliziten Schließen.

Aktionen pro Zeile:
- **Market close amount** sendet eine direkte reduce-only Market-Order für die ausgewählte Menge dieser Position.
- **Panic symbol** speichert einen Runtime-bezogenen symbolbezogenen Forced-Mode-Override und synchronisiert die Config.
- **Graceful stop symbol** speichert einen Runtime-bezogenen symbolbezogenen `graceful_stop` Forced-Mode-Override und synchronisiert die Config.
- **Take profit only symbol** speichert einen Runtime-bezogenen symbolbezogenen `tp_only` Forced-Mode-Override und synchronisiert die Config.

Aktionen für alle Positionen unten im Dialog:
- **Preview Panic**, **Preview Graceful stop**, **Preview TP only** zeigen die Config, die gespeichert würde, ohne zu schreiben oder zu synchronisieren.
- **Panic**, **Graceful stop**, **Take Profit Only** speichern den passenden globalen PB7- oder PB8-Forced-Mode für Long und Short. PB7 wird sofort auf den zugewiesenen Cluster-Bot-Host übertragen und materialisiert. PB8 verwendet den validierten Bundle-Save und veröffentlicht eine exakte Cluster-Operation für den normalen PB8-Abgleich. PB8-Symbolaktionen aktualisieren die referenzierte Sparse-Override-JSON, statt fälschlich V7-artige Override-Felder in die Hauptconfig einzubetten.

Der Exchange-User muss genau einer lokalen PB7- oder PB8-Instanz zugeordnet sein. Ist derselbe User mehreren Instanzen oder Runtimes zugeordnet, verweigert PBGui die Config-Aktion, statt den zuständigen Bot zu erraten.

Panic, Graceful Stop und Take Profit Only sind Passivbot-Config-/Sync-Aktionen. Sie senden keine direkten Exchange-Orders. **Market close amount** verwenden, wenn PBGui explizit eine direkte reduce-only Market-Order senden soll.

### 📝 ORDERS

Ein **Candlestick-Chart** mit eingeblendeten Order-Markierungen (Entries, Take-Profits, DCA-Orders) für die im Positions-Widget ausgewählte Position.

Einrichtung:
1. Eine 📋 POSITIONS-Zelle und eine 📝 ORDERS-Zelle hinzufügen (2-Spalten-Layout empfohlen).
2. In der ORDERS-Zellkonfiguration den **Link to Positions**-Chip der passenden Positions-Zelle aktivieren (z.B. „Row 3 · Col 1").
3. In der Live-Ansicht eine Positions-Zeile anklicken — der Chart lädt automatisch.

Steuerung:
- **Timeframe-Schaltflächen** (1m 5m 15m 30m 1h 2h 4h 6h 12h 1d 1w) — Kerzenauflösung ändern.
- **Nach links scrollen** im Chart lädt ältere historische Kerzen.
- Die Kerzenhistorie wird alle 30 Sekunden und beim Zurückkehren zur Seite automatisch mit der Börse abgeglichen. Verpasste Abschlusswerte werden korrigiert, ohne Zoom oder nachgeladene Historie zurückzusetzen. Snapshots erhalten den anfangs angepassten Bereich einschließlich seines exakten gebrochenen rechten Abstands.
- Die Anfangsansicht reserviert Platz für die tatsächliche Breite der Beschriftungen Entry und Price, mit mindestens sechs Pixeln Abstand zur rechten Kante der neuesten Kerze. Mindestens acht freie Kerzenplätze bleiben das Ziel, soweit Chartbreite und Zoom dies erlauben. Schmale Charts zeigen anfangs gegebenenfalls weniger Historie, damit die neueste Kerze lesbar bleibt. Live-Updates und Snapshots folgen der neuesten Kerze mit gleicher Zoomspanne, soweit die minimale Kerzenbreite der Chartbibliothek dies bei Größenänderungen zulässt. Manuelles Verschieben und Zoomen bleibt bei Updates, nachgeladener Historie und Vollbildwechseln erhalten, auch wenn du Kerzen bewusst unter die Beschriftungen schiebst. Das automatische Folgen beginnt erst wieder, wenn die rechte Bereichsgrenze höchstens sechs Pixel von der aktuellen Latest-Position entfernt ist. Größenänderungen passen den Rand nur beim automatischen Folgen an. Ein Wechsel von **Timeframe** beginnt eine neue angepasste Anfangsansicht.

---

## Templates

Templates sind **vorgefertigte Dashboard-Layouts**, die als Ausgangspunkt genutzt werden können.
Statt ein Raster von Grund auf aufzubauen, lädt man ein Template und die Zellen werden mit einer sinnvollen Widget-Anordnung befüllt.

Nach dem Erstellen von Dashboards aus einem Template schliesst sich das Panel und das zuletzt erfolgreich erstellte Dashboard wird geoeffnet. Fehlgeschlagene oder uebersprungene Erstellungen werden nicht ausgewaehlt.

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

Beim Abbrechen eines neuen, ungespeicherten Dashboards bleibt der Inhaltsbereich ohne Ladespinner im Ruhezustand.

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


Die Aktionen oben in der Dashboard-Sidebar bilden eine kompakte Icon-Leiste. Die Buttons sind 32 × 32 px groß und brechen bei schmaler Sidebar in eine weitere Zeile um. Beim Überfahren eines Icons erscheint die zugehörige Aktion; der Tastaturfokus wird wie bei anderen Sidebar-Bedienelementen sichtbar umrandet.
