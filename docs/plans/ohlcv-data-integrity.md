# Plan: OHLCV-Vollständigkeit und Checksummen

Status: teilweise umgesetzt  
Stand: 2026-08-03

## Ziel

Abgeschlossene OHLCV-Tage vollständig halten und mit einer einfachen
Checksum-Datei auf anderen Hosts vergleichen können.

## Bereits umgesetzt

- [x] Partielle Bybit-Downloads überschreiben keine vorhandenen Tagesdateien mehr.
- [x] Abgeschlossene gefetchte Tage werden auf 1.440 lückenlose Minuten geprüft.
- [x] Fehlerhafte Fetches werden verworfen und später erneut versucht.
- [x] Der Source-Index wird beim Overwrite passend zur Datei ersetzt.
- [x] 71 vorhandene Gaps auf 63 Coins wurden identifiziert.

Die 71 beschädigten Dateien wurden noch nicht verändert.

## Noch umzusetzen

### 1. Prüfsystem und Checksum-Datenbank fertigstellen

- [ ] Eine zentrale SQLite-Datenbank `data/ohlcv/checksums.sqlite` für alle Exchanges verwenden.
- [ ] Eine Tabelle mit `exchange`, `timeframe`, `coin`, `day`, `candles`, `missing_minutes`, `status`, `sha256` und `validated_at` anlegen.
- [ ] Pro Coin und Tag genau einen Eintrag speichern.
- [ ] Vorhandene Tagesdateien einmalig über einen Market-Data-Task-Worker-Job prüfen und den Zustand eintragen.
- [ ] Der Market-Data-Task-Worker reiht den Scan nach einem Update automatisch genau einmal ein, wenn der Abschlussmarker für die aktuelle Prüfschema-Version fehlt.
- [ ] Einen abgebrochenen Scan später automatisch erneut versuchen und erst nach vollständigem Abschluss markieren.
- [ ] Fortschritt und Stop im Market-Data-GUI anzeigen; der Scan blockiert keinen API- oder PBData-Start.
- [ ] SHA-256 über den sortierten Candle-Inhalt berechnen.

Primärschlüssel: `(exchange, timeframe, coin, day)`

### 2. Vortag einmal täglich prüfen

- [ ] Der stündliche Refresh aktualisiert nur den laufenden Tag.
- [ ] PBData prüft beim ersten Lauf nach `00:15 UTC` den Vortag einmal.
- [ ] Nur unvollständige Vortage stündlich erneut versuchen.
- [ ] Prüfergebnis und Checksumme in SQLite aktualisieren.
- [ ] Den letzten erfolgreich geprüften Tag über Neustarts hinweg speichern.

### 3. Schäden im GUI anzeigen und reparieren

- [ ] Im Market-Data-GUI beschädigte Tage mit Exchange, Coin, Datum und fehlenden Minuten anzeigen.
- [ ] Eine Aktion `Repair` anbieten, die fehlende Candles über den Market-Data-Task-Worker erneut lädt.
- [ ] Reparierte Daten erst speichern, wenn der komplette Tag danach gültig ist.
- [ ] Nach der Reparatur Status und Checksumme erneut berechnen.

### 4. Bekannte Gaps über das GUI reparieren

- [ ] Die 71 bekannten Gaps mit demselben normalen Repair-Ablauf bearbeiten.
- [ ] Nicht reparierbare Tage im GUI sichtbar lassen.

### 5. Referenz über GitHub bereitstellen

- [ ] In Market Data die Option `Publish checksum snapshot` ergänzen; Standard ist aus.
- [ ] Mit `Publish archive` ein eigenes beschreibbares GitHub-Archive auswählen.
- [ ] Mit `Reference archive` unabhängig davon ein beliebiges konfiguriertes öffentliches Archive für Vergleiche auswählen.
- [ ] Publish und Compare getrennt behandeln, damit ein Benutzer nur dein Archive als Referenz verwenden kann.
- [ ] Archive-Zugangsdaten nicht in Market Data speichern, sondern nur den Archive-Namen referenzieren.
- [ ] Mit der SQLite-Backup-API einen konsistenten Snapshot für die Veröffentlichung erzeugen.
- [ ] Das bereits konfigurierte eigene Config-/Optimize-Archive als GitHub-Repository verwenden.
- [ ] Den Snapshot nicht in den Git-Verlauf committen, sondern einmal täglich als Release-Asset `checksums.sqlite.gz` veröffentlichen.
- [ ] Einen festen Release-Tag `checksums-latest` verwenden und dessen Asset täglich ersetzen.
- [ ] Repository-URL und serverseitigen Schreibzugang aus der bestehenden My-Archive-Konfiguration verwenden.
- [ ] Für tokenlosen Download durch Optimizer muss das Archive-Repository öffentlich sein.
- [ ] Der Token bleibt serverseitig und erscheint nie in URLs oder Logs.
- [ ] Der Snapshot enthält keine Benutzer-, Host- oder Zugangsdaten.

### 6. Optimizer und andere Benutzer vergleichen

- [ ] Optimizer laden `checksums.sqlite.gz` einmal täglich direkt über die öffentliche GitHub-Release-URL.
- [ ] Dafür ist weder eine Cluster-Mitgliedschaft noch ein GitHub-Token erforderlich.
- [ ] Das im Market-Data-Feld `Reference archive` gewählte Archive als Downloadquelle verwenden.
- [ ] Heruntergeladene Referenz und lokale Checksum-Datenbank getrennt speichern und read-only vergleichen.
- [ ] Fehlende Coins, andere Candle-Anzahlen und abweichende Checksummen anzeigen.

## Prüfregeln

Ein abgeschlossener 24x7-Tag benötigt:

- 1.440 Candles,
- `00:00` bis `23:59 UTC`,
- genau eine Minute Abstand,
- keine doppelten Timestamps.

Erstnotierungstage dürfen später beginnen, müssen danach aber lückenlos sein.

## Nicht vorgesehen

- kein Merkle-Tree oder Signatursystem,
- keine Migration auf das PB8-Speicherformat,
- keine PB8-Tabellen für Symbole, Gaps oder Fetch-Logs.

## Fertig wenn

- die bekannten Gaps repariert oder dokumentiert sind,
- der Vortag einmal täglich und bei Fehlern erneut geprüft wird,
- beschädigte Tage im GUI sichtbar und über den normalen Ablauf reparierbar sind,
- ein Checksum-Snapshot täglich auf GitHub veröffentlicht und von Optimizer-Hosts verglichen werden kann.
