# PB9 Backtest

Öffne **PBv9 → Backtest** oder wähle **Backtest** im PB9-Run-Editor. Dabei wird die aktuelle, auch ungespeicherte native Konfiguration als privater Entwurf mit zehn Minuten Gültigkeit übernommen. Die gemeinsame PB8-Backtest-Seite verwendet getrennte PB9-Verzeichnisse für Konfigurationen, Warteschlange und Ergebnisse.

## Konfigurieren und einreihen

Lege Name, Zeitraum, Börsen und Startkapital fest. Die Börsenauswahl stammt aus der installierten nativen Vorlage und dem aktuellen Dokument. Backtest Settings, Portfolio, Global Defaults, Long/Short Overrides und Logging verwenden das Feldregister der installierten PB9-Version. Geerbte Seitenwerte bleiben ausgelassen, bis du sie bearbeitest. Long-/Short-JSON enthält eigene Overrides; Raw JSON die vollständige ursprüngliche Konfiguration. Die Run-Instanz wird durch die Übergabe nicht geändert.

**Save** speichert die native Konfiguration. **Save & Queue** erzeugt einen unveränderlichen Snapshot des aktuellen Formulars mit Schema, Paketversion, Quell-Commit und Snapshot-Hash. Vorhandene Konfigurationen benötigen die beim Öffnen geladene Revision. Bei Konflikten erneut öffnen oder einen anderen Namen wählen. Browser-Neuladen erhält den Entwurf oder die geöffnete gespeicherte Konfiguration.

## Ausführen und prüfen

Öffne **Queue** und nutze Start in der jeweiligen Zeile. PB9 benötigt eine getrennte installierte Runtime, unveränderte Quellen und eine dazu passende Rust-Erweiterung. Geänderte Quellen oder Queue-Snapshots werden abgelehnt; reihe dann bewusst einen neuen Auftrag ein. Native Caches und weitere Arbeitsdateien bleiben im eigenen Arbeitsverzeichnis des Auftrags. Der Backtest läuft unabhängig und übersteht API-Neustarts. Beim API-Ende werden nur deren Controller beendet, nicht der native Auftrag.

Stop/Restart betreffen ausschließlich den zugehörigen verwalteten Prozess. Das gemeinsame Logfenster zeigt dessen Ausgabe. Nach Abschluss findest du unter **Results** native Kennzahlen, Fills und Balance-/Equity-Charts. Die Anzeige übersetzt PB9-NPZ-Zeitstempel nach UTC und interpretiert sie nicht als PB8-Minutenindizes. **Backtest** aus einem Ergebnis öffnet dessen ursprüngliche Konfiguration für einen bewusst neu eingerichteten Lauf, nicht stillschweigend den aktuellen gespeicherten Stand. Jedes erfolgreiche Ergebnis behält eine private Kopie seines ursprünglichen Snapshots samt Herkunftsnachweis auch nach dem Leeren der Warteschlange. Fehlende native Balance-Werte bleiben Lücken im Chart. Ergebnisfilter, Auswahl und geöffnete Chart-Ansichten bleiben beim Neuladen erhalten.

Diese Anbindung unterstützt derzeit manuell gestartete lokale PB9-Aufträge. Automatischer Queue-Start, Cloud-/VPS-Ausführung, Archive und Optimizer-Übergaben sind hier noch nicht verfügbar. PB7/PB8 bleiben getrennt. Ein Backtest startet keinen Live-Trading-Bot.

## Mit einem PB8-Backtest beginnen

Wähle unter **PBv8 → Backtest** bei einer gespeicherten Konfiguration oder einem lokalen Ergebnis **V9 — Open as PB9 Backtest**. PBGui nutzt den offiziellen Konverter deiner installierten PB9-Laufzeit und zeigt vor der Übernahme alle Best-effort-Änderungen und Warnungen. Risiko-Speicher und Metrikformeln können anders abgeleitet werden; teste das Ergebnis neu. PB8-Dateien werden nicht überschrieben und es startet kein Job automatisch.

Nach **Open PB9 draft** zeigt der native Editor den Migrationsbericht, Zeitraum, Kontostand und die migrierten Einstellungen. Der private Entwurf gehört zu deiner Anmeldung und läuft nach zehn Minuten ab; speichere ihn rechtzeitig. Ein Neuladen stellt den noch gültigen Entwurf wieder her. Unterstützt werden die PB8-Schemas `v8.0.0`, `v8.1.0` und `v8.6.0`. Lokale JSON-Overrides werden eingebettet; externe Verweise und alte Coin-Flags müssen vor der Konvertierung aufgelöst werden.

## OHLCV-Datenquelle

Der Editor verwendet dasselbe Feld **ohlcv_source_dir** und denselben Button **PBGui Data** wie PB7/PB8. Neue PB9-Backtests und Entwürfe aus Run verwenden standardmäßig das gemeinsame PBGui-OHLCV-Verzeichnis. Gespeicherte und importierte Konfigurationen behalten ihre gewählte Quelle. Leere das Feld für die eigene Candle-Verwaltung von Passivbot oder gib ein anderes Quellverzeichnis ein. Die Auswahl wird in Konfiguration und unveränderlichem Queue-Snapshot gespeichert; der Start ersetzt sie nicht stillschweigend. Backtest Settings ist zunächst geöffnet, damit die getrennte Einstellung **offline** und die nativen Einstellungen sichtbar sind.

PB9 liest bestehende PBGui-Candles direkt. Candle-Verzeichnisse und Metadatendateien werden nicht kopiert. Offline-Jobs verweisen über Dateilinks auf vorhandene öffentliche CCXT-Markt- und versionierte Listing-Metadaten. Abgeleitete Caches und Ergebnisse bleiben getrennt. PBGui Coin Data und PB9-Caches werden zuerst verwendet; kompatible Metadaten anderer konfigurierter Bot-Caches können zusätzlich gelesen werden, ohne deren Code zu importieren oder diese Installationen vorauszusetzen. PB9 prüft weiterhin Cache-Versionen und Marktkennungen. Online-Jobs erhalten keine gemeinsamen Metadatenlinks, damit native Aktualisierungen nicht in gemeinsame Dateien schreiben.

Fehlen Offline-Marktmetadaten, bleibt der Job mit einer konkreten Meldung in der Warteschlange. Aktualisiere die Börse in Coin Data, stelle gegebenenfalls native Listing-Metadaten bereit oder deaktiviere offline bewusst. Offline lädt fehlende Daten nicht stillschweigend nach. Vorhandene Quelldateien bleiben unverändert.

## Kursüberlagerung

Öffne das Balance-/Equity-Diagramm über das Auge. **Price (PBGui MarketData)** liest vorhandene PBGui-Candles mit demselben Leser wie PB7/PB8 und zeigt den Schlusskurs auf der rechten Achse. Kombinierte Ergebnisse bieten jede konfigurierte Börse einzeln zur Auswahl. Die anfängliche Auswahl bevorzugt eine Börse, die den gesamten Diagrammzeitraum abdeckt. Fehlende oder teilweise Daten werden ausdrücklich angezeigt. Beim Neuladen werden das geöffnete Diagramm und deine Börsenauswahl wiederhergestellt. Native HSL-Diagramme sind über die Plot-Bilder verfügbar; die PB8-spezifische HSL-Rekonstruktion wird nicht auf native PB9-Ergebnisse angewendet. Diese Ansicht lädt oder kopiert keine Candle-Daten.

## PB7, PB8 und PB9 vergleichen

PB9 erweitert die bestehende Ansicht **Compare**. Wähle unter **Results** PB7, PB8, PB9 oder **All**, markiere mindestens zwei Zeilen und drücke **Compare**. Jedes Ergebnis lädt seine eigene versionsgerechte Equity-CSV; das bestehende Diagramm überlagert Equity und Balance auf einer gemeinsamen UTC-Achse. Unterschiedlich definierte Metriken werden nicht in vermeintlich gleiche Scores umgerechnet. Versionsfilter, Auswahl und geöffneter Vergleich bleiben beim Neuladen erhalten.

Neue Konvertierungen aus einem lokalen PB8-**Ergebnis** behalten eine PBGui-Vergleichsreferenz über Entwurf, gespeicherte Konfiguration und Queue bis zum fertigen Ergebnis. **Compare with PB8 source** in der Ergebniszeile wählt dieses konkrete Paar und öffnet dasselbe Diagramm direkt. Die Referenz bleibt nach Entfernen des Queue-Eintrags erhalten. Sie bezeichnet das Vergleichsziel und bestätigt keine Gleichwertigkeit nachträglich geänderter PB9-Parameter. Wurde das Original entfernt oder seine Konfiguration/Analyse geändert, meldet der direkte Vergleich dies, statt ein anderes Ergebnis zu erraten. Diese GUI-Metadaten bleiben von den nativen Bot-Parametern getrennt.

Ältere PB9-Läufe und Konvertierungen aus gespeicherten Konfigurationen ohne konkretes Ergebnis lassen sich über die normale manuelle Compare-Auswahl vergleichen. Dafür ist kein neuer Backtest nötig; Ergebnis- und Candle-Daten werden nicht verändert.
