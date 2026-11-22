# PB9 Backtest

Raw-JSON- und Formularänderungen werden in Bearbeitungsreihenfolge zusammengeführt, auch beim sofortigen Speichern. Unvollständiges JSON blockiert Save, ohne ausstehende Formularänderungen zu verwerfen.

Coin-Auswahl und Apply Filters verwenden die PB8-Katalog- und Filterlogik mit
dem nativen PB9-Resolver. Normale Coins erscheinen einmal unter ihrem Namen;
exakte Kennungen bleiben für mehrdeutige Märkte und bestehende exakte Auswahlen
erhalten. Die Auswahl liest nur vorhandene lokale öffentliche Marktmetadaten.

Die Exchange-Auswahl zeigt alle von den installierten PB9-Konnektoren und CCXT unterstützten Börsen für historische Daten. Das Template legt nur die Vorauswahl fest. Suite Mode und Coin Sources verwenden denselben Katalog.

Öffne **PBv9 → Backtest** oder wähle **Backtest** im PB9-Run-Editor. Dabei wird die aktuelle, auch ungespeicherte native Konfiguration als privater Entwurf mit zehn Minuten Gültigkeit übernommen. Die gemeinsame PB8-Backtest-Seite verwendet getrennte PB9-Verzeichnisse für Konfigurationen, Warteschlange und Ergebnisse.

## Konfigurieren und einreihen

Der Editor folgt dem PB8-Aufbau: Börsen/Name/Zeitraum, Zeilen für Guthaben und Gebühren, OHLCV-Einstellungen, Quellen-Zuordnungen, **Coins & Filters**, **Coin Overrides**, **Suite Mode** und anschließend **Long** und **Short** nebeneinander. Die üblichen Zahlenfelder verwenden die gemeinsamen Plus-/Minus-Schaltflächen. Die Felder behalten ihre nativen PB9-Namen. Zusätzliche native Einstellungen stehen unter **Advanced Settings**.

Coin-Auswahl, Filter, Quellen-Chips, Market-Settings-Overrides und Suite-Editor verwenden die bestehenden Backtest-Komponenten. Apply Filters schreibt die eigenen Approved-/Ignored-Listen jeder PB9-Seite. Suite Mode verwendet native PB9-Parameterpfade und speichert die Aggregation als `backtest.reducer`. Die Börsenauswahl stammt aus der installierten nativen Vorlage und dem aktuellen Dokument.

Jede Seite zeigt zunächst **Full Config JSON** einschließlich geerbter Vorgaben. Änderungen erzeugen nur die tatsächlich geänderten Seiten-Overrides. **Overrides only** zeigt eigene Werte; lösche dort einen Override, um wieder zu erben. Auch die kleinen **Explicit value**-Kontrollkästchen unter Advanced Settings stellen beim Entfernen die Vererbung wieder her. Raw JSON enthält die ursprüngliche Konfiguration. Offene Abschnitte und JSON-Ansichten bleiben nach Neuladen erhalten. Ungespeicherte Entwürfe werden geschützt im jeweiligen Browser-Tab wiederhergestellt; Felder mit Zugangsdaten werden dabei abgelehnt. Die Run-Instanz wird durch die Übergabe nicht geändert.

**Additional Parameters** direkt vor Raw JSON verwendet den PB8-Fallback für jeden Backtest-Schlüssel ohne eigenes Bedienelement. Er berücksichtigt Vorgaben der installierten Vorlage/des Feldregisters sowie zusätzliche Schlüssel der geladenen Konfiguration und bietet passende Zahlen-, Checkbox-, Text-, Null- und JSON-Felder. Vorgaben bleiben bis zur Bearbeitung geerbt; vorhandene Werte und Datentypen bleiben erhalten. Neue Bot-/Logging-Parameter erscheinen unter Advanced Settings, auch wenn das Feldregister noch keinen Eintrag hat. Ungültiges JSON blockiert Save und Queue. Das schreibgeschützte `base_dir` folgt dem Konfigurationsnamen; PB9-Aufträge erhalten getrennte verwaltete Ergebnisverzeichnisse. Welche Parameter gültig sind, entscheidet weiterhin die installierte native PB9-Version.

**Add to Run** übernimmt das aktuelle Formular einschließlich ungespeicherter Änderungen, Filter und Notizen in einen deaktivierten PB9-Run-Entwurf. Dabei wird keine Instanz gespeichert oder gestartet.

**Save** speichert die native Konfiguration. **Save & Queue** erzeugt einen unveränderlichen Snapshot des aktuellen Formulars mit Schema, Paketversion, Quell-Commit und Snapshot-Hash. Vorhandene Konfigurationen benötigen die beim Öffnen geladene Revision. Bei Konflikten erneut öffnen oder einen anderen Namen wählen. Browser-Neuladen erhält den Entwurf oder die geöffnete gespeicherte Konfiguration.

## Ausführen und prüfen

Gemeinsame Backtest-Slots gehören zur aktuellen PBGui-Installation. Backtests einer getrennten Installation wie `pbgui-share` belegen deren Slots nicht, auch wenn beide dasselbe Passivbot-Verzeichnis verwenden.

Für PB9 fehlen derzeit noch **Strategy Explorer** und die gesonderte AI-Loops-Anbindung. Noch nicht angebundene Werkzeuge werden ausgeblendet und leiten native Konfigurationen nicht an PB7-/PB8-Handler weiter. Editor, manuelle Warteschlange, Ergebnisdiagramme und versionsübergreifendes Compare sind verfügbar.

Filter und Notizen bleiben beim Speichern, bei Entwürfen, Migration und Wiederverwenden eines Ergebnisses erhalten. Die native Ausführung erhält keine GUI-Metadaten. Save & Queue verhindert doppelte Klicks und verwendet bei einer Wiederholung nach unklarer Serverantwort dieselbe Auftragskennung.

**Balance Calculator** und **Calc Balance** verwenden den gemeinsamen Rechner mit aktiven nativen Seiten, geerbten Risikowerten und Einstiegsmenge. Diese Analysewerte werden niemals als Bot-Konfiguration gespeichert.

Stop erscheint als **stopped**. Restart erzeugt aus dem eingefrorenen Snapshot einen neuen Auftrag mit eigenem Ergebnisverzeichnis; der vorherige Lauf bleibt erhalten. Beim Löschen einer Konfiguration wird **Also delete results** berücksichtigt; ausgewählte Daten landen im verwalteten Papierkorb. Queue-Cleanup entfernt den privaten Workspace, folgt keinen Marktdaten-Links und behält Ergebnisse.

Öffne **Queue** und nutze Start in der jeweiligen Zeile oder aktiviere **Autostart** unter **Settings** und wähle **Backtest CPU Slots**. PB9 speichert seine Queue-Einstellungen getrennt; standardmäßig ist Autostart aus und das Limit beträgt einen Slot. Automatisch startet der älteste Eintrag zuerst. Laufende PB7-/PB8-/PB9-Backtests teilen sich die Kapazität. Autostart auszuschalten oder das Limit zu senken verhindert weitere Starts, ohne laufende Jobs zu stoppen. Änderungen erscheinen automatisch in offenen Tabs; ungespeicherte Dialogeingaben bleiben erhalten. PB9 benötigt eine getrennte installierte Runtime, unveränderte Quellen und eine dazu passende Rust-Erweiterung. Geänderte Quellen oder Queue-Snapshots werden abgelehnt; reihe dann bewusst einen neuen Auftrag ein. Backtest und Optimize laufen wie PB8 im konfigurierten PB9-Verzeichnis und teilen dessen dauerhaften Ordner `caches/`. Queue-Cleanup entfernt diesen Cache nicht. Der Backtest läuft unabhängig und übersteht API-Neustarts. Beim API-Ende werden nur deren Controller beendet, nicht der native Auftrag.

Stop/Restart betreffen ausschließlich den zugehörigen verwalteten Prozess. Das gemeinsame Logfenster zeigt dessen Ausgabe. Nach Abschluss findest du unter **Results** native Kennzahlen, Fills und Balance-/Equity-Charts. Die Anzeige übersetzt PB9-NPZ-Zeitstempel nach UTC und interpretiert sie nicht als PB8-Minutenindizes. **Backtest** aus einem Ergebnis öffnet dessen ursprüngliche Konfiguration für einen bewusst neu eingerichteten Lauf, nicht stillschweigend den aktuellen gespeicherten Stand. Jedes erfolgreiche Ergebnis behält eine private Kopie seines ursprünglichen Snapshots samt Herkunftsnachweis auch nach dem Leeren der Warteschlange. Fehlende native Balance-Werte bleiben Lücken im Chart. Ergebnisfilter, Auswahl und geöffnete Chart-Ansichten bleiben beim Neuladen erhalten.

Diese Anbindung unterstützt lokale native PB9-Aufträge mit optionalem Queue-Autostart. VPS-Ausführung bleibt nicht verfügbar. Native Optimierung verwendet die gemeinsame Optimize-Seite; die Voraussetzungen für Cloud-Ausführung stehen im Optimize-Guide. Ein Backtest startet keinen Live-Trading-Bot.

Unter **Results** funktionieren PB9-Aktionen auch mit Versionsfilter **All**, wenn
die ausgewählten Zeilen zu PB9 gehören. Wähle ein Ergebnis für **Add to Run**,
**Backtest**, **Optimize from Result** oder eines der Balance-Werkzeuge.
**Add to Run** öffnet einen ungespeicherten, deaktivierten PB9-Run-Entwurf und
startet keinen Handel. Bei gemischten Versionen bleiben **Compare** und
**Delete Selected** verfügbar. Neue PB9-Ergebnis-Hüllen und ältere flache Configs
verwenden denselben Metadaten- und Config-Leser, auch im Archiv. Anzeige und
Übergaben schreiben keine Ergebnisdateien um.
Ergebnisse werden automatisch in Seiten mit höchstens 100 Einträgen geladen.
Dadurch bleiben native Aktionen nutzbar, während Ergebnisse einer anderen
Version noch laden. Filterwechsel und Navigation brechen überholte Anfragen ab;
verspätete Antworten können die aktuelle Ansicht nicht ersetzen.
Große Tabellen werden gebündelt ergänzt, während der Ladezähler weiterläuft.
Geöffnete Diagramme und Auswahl bleiben erhalten. Vorübergehend fehlgeschlagene
Seitenanfragen werden automatisch bis zu zweimal wiederholt; Konfigurationsfehler
werden sofort gemeldet.

## Mit einem PB8-Backtest beginnen

Wähle unter **PBv8 → Backtest** bei einer gespeicherten Konfiguration oder einem lokalen Ergebnis **V9 — Open as PB9 Backtest**. PBGui nutzt den offiziellen Konverter deiner installierten PB9-Laufzeit und zeigt vor der Übernahme alle Best-effort-Änderungen und Warnungen. Risiko-Speicher und Metrikformeln können anders abgeleitet werden; teste das Ergebnis neu. PB8-Dateien werden nicht überschrieben und es startet kein Job automatisch.

Nach **Open PB9 draft** zeigt der native Editor den Migrationsbericht, Zeitraum, Kontostand und die migrierten Einstellungen. Der private Entwurf gehört zu deiner Anmeldung und läuft nach zehn Minuten ab; speichere ihn rechtzeitig. Ein Neuladen stellt den noch gültigen Entwurf wieder her. Unterstützt werden die PB8-Schemas `v8.0.0`, `v8.1.0` und `v8.6.0`. Lokale JSON-Overrides werden eingebettet; externe Verweise und alte Coin-Flags müssen vor der Konvertierung aufgelöst werden.

## OHLCV-Datenquelle

Der Editor verwendet dasselbe Feld **ohlcv_source_dir** und denselben Button **PBGui Data** wie PB7/PB8. Neue PB9-Backtests und Entwürfe aus Run verwenden standardmäßig das gemeinsame PBGui-OHLCV-Verzeichnis. Gespeicherte und importierte Konfigurationen behalten ihre gewählte Quelle. Leere das Feld für die eigene Candle-Verwaltung von Passivbot oder gib ein anderes Quellverzeichnis ein. Die Auswahl wird in Konfiguration und unveränderlichem Queue-Snapshot gespeichert; der Start ersetzt sie nicht stillschweigend. Die getrennte Einstellung **offline** steht unter **Additional Parameters**. Ein Wechsel der Quelle ändert den Offline-Modus nicht automatisch.

PB9 liest bestehende PBGui-Candles direkt. Candle-Verzeichnisse und Metadatendateien werden nicht kopiert. Offline-Ausführung liest die eigenen gespeicherten Markt-/Listing-Metadaten und den Abdeckungskatalog der PB9-Installation. Bereite diesen nativen Cache vor dem Offline-Einsatz online vor. Die Ausführung erzeugt keine Links zu Metadaten anderer Runtimes. PB9 prüft weiterhin Cache-Versionen und Marktkennungen.

Fehlen Offline-Marktmetadaten, bleibt der Job mit einer konkreten Meldung in der Warteschlange. Bereite den nativen PB9-Cache online vor oder deaktiviere offline bewusst. Offline lädt fehlende Daten nicht stillschweigend nach. Vorhandene Quelldateien bleiben unverändert.

## Kursüberlagerung

Öffne das Balance-/Equity-Diagramm über das Auge. **Price (PBGui MarketData)** liest vorhandene PBGui-Candles mit demselben Leser wie PB7/PB8 und zeigt den Schlusskurs auf der rechten Achse. Kombinierte Ergebnisse bieten jede konfigurierte Börse einzeln zur Auswahl. Die anfängliche Auswahl bevorzugt eine Börse, die den gesamten Diagrammzeitraum abdeckt. Fehlende oder teilweise Daten werden ausdrücklich angezeigt. Beim Neuladen werden das geöffnete Diagramm und deine Börsenauswahl wiederhergestellt. Native HSL-Diagramme sind über die Plot-Bilder verfügbar; die PB8-spezifische HSL-Rekonstruktion wird nicht auf native PB9-Ergebnisse angewendet. Diese Ansicht lädt oder kopiert keine Candle-Daten.

## PB7, PB8 und PB9 vergleichen

PB9 erweitert die bestehende Ansicht **Compare**. Wähle unter **Results** PB7, PB8, PB9 oder **All**, markiere mindestens zwei Zeilen und drücke **Compare**. Jedes Ergebnis lädt seine eigene versionsgerechte Equity-CSV; das bestehende Diagramm überlagert Equity und Balance auf einer gemeinsamen UTC-Achse. Unterschiedlich definierte Metriken werden nicht in vermeintlich gleiche Scores umgerechnet. Versionsfilter, Auswahl und geöffneter Vergleich bleiben beim Neuladen erhalten.

Neue Konvertierungen aus einem lokalen PB8-**Ergebnis** behalten eine PBGui-Vergleichsreferenz über Entwurf, gespeicherte Konfiguration und Queue bis zum fertigen Ergebnis. **Compare with PB8 source** in der Ergebniszeile wählt dieses konkrete Paar und öffnet dasselbe Diagramm direkt. Die Referenz bleibt nach Entfernen des Queue-Eintrags erhalten. Sie bezeichnet das Vergleichsziel und bestätigt keine Gleichwertigkeit nachträglich geänderter PB9-Parameter. Wurde das Original entfernt oder seine Konfiguration/Analyse geändert, meldet der direkte Vergleich dies, statt ein anderes Ergebnis zu erraten. Diese GUI-Metadaten bleiben von den nativen Bot-Parametern getrennt.

Ältere PB9-Läufe und Konvertierungen aus gespeicherten Konfigurationen ohne konkretes Ergebnis lassen sich über die normale manuelle Compare-Auswahl vergleichen. Dafür ist kein neuer Backtest nötig; Ergebnis- und Candle-Daten werden nicht verändert.

## OHLCV Readiness und 1st/All

**OHLCV Readiness** in der Backtest-Seitenleiste prüft das aktuelle native PB9-Formular. Panel und Berichtslogik stammen von PB8; Coin-Auswahl, Seitenaktivierung, Mindestalter und Warmup kommen aus der installierten PB9-Version. Das Panel aktualisiert sich nach Änderungen, bei Rückkehr zum Fenster und regelmäßig automatisch. Browser-Neuladen stellt ein geöffnetes Panel für dieselbe Konfiguration bzw. denselben Entwurf wieder her. Es gibt keinen manuellen Refresh-Knopf.

Standard für **ohlcv_source_dir** bleiben die gemeinsamen PBGui-Candles. Diese Werkzeuge kopieren, importieren oder laden keine Candle-Historie vor. Readiness akzeptiert absolute eigene Quellen unterhalb des PBGui-Marktdatenverzeichnisses oder des PB9-Checkouts. Ein leeres Feld prüft den dauerhaften nativen PB9-Cache, den auch die Ausführung verwendet; fehlende Daten kann der native Backtest bei ausdrücklich deaktiviertem Offline-Modus beziehen. Fehlende Historie der PBGui-Quelle wird über **Market Data** beschafft.

**1st** verwendet den frühesten bekannten OHLCV-Beginn eines gewählten Markts zuzüglich nativem PB9-Warmup. **All** verwendet den gemeinsamen Beginn aller gewählten Börsen-/Coin-Paare zuzüglich Warmup. Wähle explizite Approved Coins auf einer aktiven Seite mit positivem Exposure und Positionslimit. `all` und mehr als 200 Börsen-/Coin-Paare benötigen eine engere Auswahl. Fortschritt und Stop verwenden denselben Controller wie PB8. Ein verspätetes Ergebnis ändert das Datum nicht mehr, nachdem Formulareingaben geändert wurden.

Offline werden öffentliche Börsen-/Inception-Metadaten aus vorhandenen Caches gelesen. Bei deaktiviertem Offline-Modus können fehlende Metadaten in einem privaten temporären Cache ermittelt werden. Öffentliche Metadaten-Links werden nur lesend verwendet und vor einem Online-Fallback entfernt. Gemeinsame Metadaten und Candles werden nicht überschrieben. Readiness prüft denselben dauerhaften nativen Cache wie die Ausführung und lässt den nativen Loader den ersten Tag einer externen Quelle prüfen; Runtime, Dataset und Herkunft prüft der native Backtest weiterhin beim Start. Bei einem vorbereiteten Dataset über **hlcvs_data_dir** ist die Rohdatenprüfung nicht anwendbar und 1st/All nicht verfügbar; verwende dessen Datumsbereich.


## Gemeinsames Archiv

**Add to Archive** unter Results exportiert ausgewählte PB9-Ergebnisse in das bestehende eigene Archiv aus **Archive → Setup**. PB7, PB8 und PB9 verwenden dieselbe Archivliste, Transaktionen, versionierte Struktur und Manifeste. Native Konfiguration, Originalquelle, Herkunftsnachweis, NPZ-Equity, Fills und Berichte bleiben als Ergebnisdateien erhalten; Candle-Daten werden nicht kopiert. Export startet keinen Auftrag und führt keinen Git-Push aus.

**Archive** in der Seitenleiste öffnet die Ergebnisse. PB9-Zeilen verwenden dieselben Kennzahlenformeln wie lokale PB9-Results und laden Charts, Bilder, Dateien, Kursüberlagerung und Originalkonfiguration über den PB9-Leser. Browsen aus PB9 migriert kein Archiv automatisch. Auswahl, Such-/Konfigurations-/Coinfilter und geöffnetes Archiv bleiben nach Browser-Neuladen erhalten; der bestehende Archiv-Watcher aktualisiert die Ansicht automatisch.

**Backtest** auf ausgewählten Archivzeilen öffnet den vorhandenen Datums-/Balance-/Börsendialog und reiht einen neuen nativen PB9-Snapshot ein. Importierte Candle-/Datasetpfade müssen unter PBGui Market Data oder dem konfigurierten PB9-Verzeichnis liegen; nutze die PBGui-Datenoption im Dialog oder bearbeite einen fremden, hier nicht verfügbaren Pfad. Gemischte Auswahlen landen jeweils in der zuständigen Queue. **Add to Run** öffnet einen deaktivierten PB9-Run-Entwurf. Umbenennen einer nativen Archivgruppe lässt Ergebnisdateien und Herkunftsnachweise unverändert. Native Optimizer-Archivkonfigurationen fehlen weiterhin; ein Rückfall auf PB7 ist gesperrt.

Archiv-Setup gibt gespeicherte Zugriffstoken nicht an den Browser zurück. Ein leeres Tokenfeld behält den gespeicherten Wert; **Clear stored token** entfernt ihn ausdrücklich. Git verwendet serverseitige Authentifizierung über HTTPS in der temporären Prozessumgebung mit übereinstimmenden Push-/Fetch-Zielen, ohne Token in Befehlsargumenten oder Repository-URLs. Git Push, Pull und History-Compaction bleiben ausdrückliche Benutzeraktionen.

### Retest & Replace und Zeitpläne

Unter **PBv9 → Backtest → Archive** öffnest du **My Archive**, markierst Ergebnisse und klickst **Retest & Replace**. Der gemeinsame PB8-Dialog bietet **Queue Now** oder **Create Schedule** mit täglicher/wöchentlicher Wiederholung, Uhrzeit und Wochentag. Wähle die ursprüngliche Dauer oder die letzten X Tage; der neue Zeitraum endet gestern. Balance, Börsen und PBGui Market Data verwenden dieselben Bedienelemente wie PB8.

Unter **Retest Schedules** kannst du Zeitpläne ausführen, pausieren/aktivieren oder löschen. Navigation und Zeitplan-Tab bleiben beim Browser-Neuladen erhalten. Einreihen startet noch keinen Backtest: Aktiviere für unbeaufsichtigte Ausführung den nativen PB9-**Autostart** unter **Settings**. API-Neustarts lassen laufende native Aufträge bestehen und nehmen den Ersetzungs-Controller wieder auf.

Nur ein erfolgreiches, eindeutig zugeordnetes natives Ergebnis mit passendem Snapshot/Schema/Commit ersetzt das Original. Liquidierte Ergebnisse werden standardmäßig übersprungen; geänderte Originale, fehlende Herkunftsnachweise und mehrdeutige Multi-Result-Suites lassen das Original bestehen. Das Original bleibt als private Wiederherstellungssicherung erhalten; nur der lokale Retest-Auftrag wird ausgeräumt. Zeitplan-Ziele folgen der Ersetzung automatisch; Git Push bleibt manuell.

## Entwurfs-Wiederherstellung und native Quellgrenzen

Browser-Neuladen stellt ungespeicherte Namen, Werte und offene Abschnitte über dieselbe geschützte Entwurfs-Speicherung wie Optimize wieder her. Native Revisionsprüfungen bleiben an die ursprüngliche Quelle gebunden. Benutzerdefinierte Credential-Felder werden nie im Browserspeicher abgelegt. Unveränderte Konfigurationen werden erneut aus ihrer aktuellen Quelle geladen.

Der Umgang mit dem Beginn externer Quellen hängt vom installierten PB9-Build ab. Der lokale Source-Window-Testfix verwendet bei ausdrücklich gewähltem externen Ordner die erste verfügbare echte Kerze je Coin und Quelle, online und offline. Das begrenzt das ausgewählte Datenfenster und behauptet keinen Marktstart. Angeforderte Datumswerte bleiben dokumentiert; frühere Kurse werden nicht erfunden und Rohkerzen nicht kopiert. Interne Lücken und ein vollständiges Offline-Ende werden weiter geprüft. Ungepatchte PB9-Builds können einen späteren externen Datenbeginn weiterhin ablehnen. Nach Änderung des nativen Quell-Commits einen frischen Auftrag einreihen; alte Queue-Snapshots behalten ihre ursprüngliche Zuordnung.
