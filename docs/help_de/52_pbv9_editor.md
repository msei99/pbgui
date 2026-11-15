# PB9 Config Editor

Unter **PBv9 → Config Editor** findest du die erste Vorschau des PB9-Editors. Er bearbeitet die vollständige ursprüngliche Konfiguration als JSON. PB7/PB8-Editoren und deren Konfigurationen bleiben getrennt.

## Runtime-Pfade

Öffne **Runtime paths**, trage den absoluten Quellpfad eines separaten Passivbot-v9-Checkouts und die Python-Datei seiner eigenen virtuellen Umgebung ein und wähle **Save paths**. Der Checkout muss ein v9-Konfigurationsschema deklarieren. Die Umgebung benötigt CLI und Rust-Erweiterung sowie eine unterstützte Python-Version. Verzeichnisse und Umgebungen von PB7, PB8 oder PBGui dürfen nicht wiederverwendet werden.

Der Editor installiert oder aktualisiert Passivbot nicht und kompiliert kein Rust. **PB9 detected** bezeichnet statische Prüfungen; Vorlage laden und Validieren prüfen zusätzlich die gewählte Umgebung. Installationen und Änderungen an Bots/VPS bleiben separate Vorgänge. Ohne verfügbare Runtime bleibt das Textfeld benutzbar, native Aktionen sind jedoch gesperrt.

## Erstellen und bearbeiten

**New from template** lädt die ursprüngliche Vorlage aus der gewählten PB9-Runtime. Trage einen sichtbaren Dateinamen mit `.json` ein, bearbeite **Raw JSON** und wähle **Validate**. Die Prüfung validiert native Inline-Quellen und mischt keine dabei ergänzten Standardwerte in dein Dokument. Doppelte JSON-Schlüssel, nicht unterstützte Werte, nicht endliche Zahlen und externe Dateiverweise werden abgelehnt.

Speichern verwendet vier Leerzeichen zur Einrückung und verschiebt Root-`pbgui` ans Ende; die ursprünglichen Werte bleiben erhalten.

Nur ursprüngliche Root-`pbgui`-Metadaten bleiben als GUI-Daten erhalten und werden von der nativen Validierung getrennt. Runtime-/Schema-/Commit-Nachweise stammen aus der gewählten Umgebung, nicht aus diesen Metadaten. Es findet keine Migration statt. Eine v8-Konfiguration muss zuerst ausdrücklich außerhalb dieser Vorschau in ein separates v9-Dokument migriert werden.

## Speichern und wieder öffnen

**Save** erstellt eine neue Datei exklusiv im privaten PBGui-Verzeichnis `data/configs_v9`. Wähle den Namen in der Seitenleiste zum Öffnen. Aktualisierungen benötigen genau die geladene Revision; Änderungen anderer Schreiber werden nicht still überschrieben. Ein anderer Dateiname erzeugt eine Kopie. Die Vorschau löscht oder benennt keine Dateien um und startet keine Backtests, Optimierungen oder Live-Bots.

Wenn ein anderer Schreiber die Datei verändert, bleibt dein Text erhalten. **Open stored version** öffnet den aktuellen gespeicherten Stand nach Bestätigung des Verwerfens ungespeicherter Änderungen; alternativ kannst du unter einem neuen Namen speichern. Schlägt die Speicherbestätigung nach Veröffentlichung fehl, prüft der Editor die gespeicherte Datei und wiederholt den Schreibvorgang nicht blind. Eine fehlgeschlagene native Validierung kann das Öffnen einer gespeicherten Datei verhindern; füge korrigiertes JSON ins Textfeld ein und speichere unter einem neuen Namen.

## Automatische Aktualisierung und Navigation

Runtime-Status, Dateiliste und ausgewähltes gespeichertes Dokument aktualisieren sich automatisch bei sichtbarer Seite und bei Rückkehr. Unveränderte Dateien werden ausschließlich über eine begrenzte Revisionsabfrage ohne nativen Helper geprüft; nur extern geänderte Dateien werden neu geladen, wenn keine lokalen Änderungen vorliegen. Unveränderte Dokumente übernehmen neue gespeicherte Inhalte; ungespeicherter Text und Pfadeingaben bleiben erhalten. Wenn du während einer Prüfung weiter tippst, bezieht sich ihr Ergebnis auf die eingesendete Fassung.

Browser-Neuladen stellt Dateiauswahl und Seitensuche wieder her. Ungespeicherter Konfigurationstext wird nicht im Browser-Speicher abgelegt; vor dem Verlassen geänderter Dokumente warnt der Browser. Speichere vor dem Neuladen, um diese Änderungen zu behalten. Der gemeinsame **Guide** öffnet sich über dem Editor; **Information → Help** öffnet die separate Hilfeseite.

## Grenzen

Runtime-Pfade mit `%` werden vom INI-Slot-Schreiber derzeit nicht unterstützt und ohne Einstellungsänderung abgelehnt.

Anfrage und Dokument haben getrennte Ein-MiB-Grenzen. Die Dokumentgrenze zählt ASCII-escaped JSON; der native Transport enthält zusätzliche Runtime-Daten. Kompakte oder nicht-ASCII-Anfragen können daher schon unter einem MiB Übertragungsgröße abgelehnt werden. JSON ist auf 64 Verschachtelungsebenen, Dateinamen auf 255 UTF-8-Bytes begrenzt. Unterstützt werden nur Inline-Konfigurationen; dateibasierte Coin-Listen/Overrides und Symbol-Mapping-abhängige Quellen werden abgelehnt. Der Bibliotheksscan ist auf 1.000 Verzeichniseinträge begrenzt.

Die native Prozessverwaltung benötigt derzeit Linux. Stop/Shutdown bricht laufende native Prüfungen ab; eine bereits atomar veröffentlichte Datei bleibt gespeichert. Revisionssperren koordinieren PBGui-Schreiber; unabhängige Dateieditoren desselben Benutzers nehmen an diesem Sperrprotokoll nicht teil. Für native Tests ist eine echte v9-/Rust-Installation erforderlich; Offline-Browsertests nutzen isolierte Fixtures.
