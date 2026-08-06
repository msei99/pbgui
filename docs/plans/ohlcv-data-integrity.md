# Plan: OHLCV-Vollständigkeit und Checksummen

Status: umgesetzt; bekannte Dateien noch nicht repariert
Stand: 2026-08-04

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

## Umsetzung

### 1. Prüfsystem und Checksum-Datenbank fertigstellen

- [x] Eine zentrale SQLite-Datenbank `data/ohlcv/checksums.sqlite` verwenden; Bybit besitzt Repair-/Finalisierungsintegration und Hyperliquid nutzt seinen vorhandenen Improve-/Fallback-Pfad für Repair.
- [x] Scans, Findings, Repair und Referenzvergleiche auf Binance USDM, OKX, Bitget und Hyperliquid Crypto erweitern.
- [x] Hyperliquid XYZ/TradFi bis zur session-aware Prüfung aus dem Crypto-Scan ausschließen.
- [x] Neue Exchange-Scans wegen rund 1,8 Millionen Tagesdateien manuell und serialisiert starten.
- [x] Eine Tabelle mit `exchange`, `timeframe`, `coin`, `day`, `candles`, `missing_minutes`, `status`, `sha256` und `validated_at` anlegen.
- [x] Pro Coin und Tag genau einen Eintrag speichern.
- [x] Vorhandene Tagesdateien einmalig über einen Market-Data-Task-Worker-Job prüfen und den Zustand eintragen.
- [x] Der Market-Data-Task-Worker reiht den Scan nach einem Update automatisch genau einmal ein, wenn der Abschlussmarker für die aktuelle Prüfschema-Version fehlt.
- [x] Einen abgebrochenen Scan später automatisch erneut versuchen und erst nach vollständigem Abschluss markieren.
- [x] Fortschritt und Stop im Market-Data-GUI anzeigen; der Scan blockiert keinen API- oder PBData-Start.
- [x] SHA-256 über den sortierten Candle-Inhalt berechnen.

Primärschlüssel: `(exchange, timeframe, coin, day)`

### 2. Vortag einmal täglich prüfen

- [x] Der stündliche Refresh aktualisiert nur den laufenden Tag.
- [x] PBData prüft beim ersten Lauf nach `00:15 UTC` den Vortag einmal.
- [x] Nur unvollständige Vortage stündlich erneut versuchen.
- [x] Prüfergebnis und Checksumme in SQLite aktualisieren.
- [x] Den letzten erfolgreich geprüften Tag über Neustarts hinweg speichern.

### 3. Schäden im GUI anzeigen und reparieren

- [x] Im Market-Data-GUI beschädigte Tage mit Exchange, Coin, Datum und fehlenden Minuten anzeigen.
- [x] Eine Aktion `Repair` anbieten, die fehlende Candles über den Market-Data-Task-Worker erneut lädt.
- [x] Reparierte Daten erst speichern, wenn der komplette Tag danach gültig ist.
- [x] Nach der Reparatur Status und Checksumme erneut berechnen.

### 4. Bekannte Gaps über das GUI reparieren

- [ ] Die 71 bekannten Gaps mit demselben normalen Repair-Ablauf bearbeiten.
- [x] Nicht reparierbare Tage im GUI sichtbar lassen.

### 5. Referenz über GitHub bereitstellen

- [x] In Market Data die Option `Publish checksum snapshot` ergänzen; Standard ist aus.
- [x] Mit `Publish archive` ein eigenes beschreibbares GitHub-Archive auswählen.
- [x] Mit `Reference archive` unabhängig davon ein beliebiges konfiguriertes öffentliches Archive für Vergleiche auswählen.
- [x] Publish und Compare getrennt behandeln, damit ein Benutzer nur dein Archive als Referenz verwenden kann.
- [x] Archive-Zugangsdaten nicht in Market Data speichern, sondern nur den Archive-Namen referenzieren.
- [x] Mit der SQLite-Backup-API einen konsistenten Snapshot für die Veröffentlichung erzeugen.
- [x] Das bereits konfigurierte eigene Config-/Optimize-Archive als GitHub-Repository verwenden.
- [x] Den Snapshot nicht in den Git-Verlauf committen, sondern einmal täglich als Release-Asset `checksums.sqlite.gz` veröffentlichen.
- [x] Einen festen Release-Tag `checksums-latest` verwenden und dessen Asset täglich ersetzen.
- [x] Repository-URL und serverseitigen Schreibzugang aus der bestehenden My-Archive-Konfiguration verwenden.
- [x] Für tokenlosen Download durch Optimizer muss das Archive-Repository öffentlich sein.
- [x] Der Market-Data-Publisher übergibt den Token nur serverseitig über `GH_TOKEN`; er erscheint nicht in Task-Payloads, URLs oder Logs.
- [x] Der Snapshot enthält keine Benutzer-, Host- oder Zugangsdaten.

### 6. Optimizer und andere Benutzer vergleichen

- [x] Optimizer laden `checksums.sqlite.gz` einmal täglich direkt über die öffentliche GitHub-Release-URL.
- [x] Dafür ist weder eine Cluster-Mitgliedschaft noch ein GitHub-Token erforderlich.
- [x] Das im Market-Data-Feld `Reference archive` gewählte Archive als Downloadquelle verwenden.
- [x] Heruntergeladene Referenz und lokale Checksum-Datenbank getrennt speichern und read-only vergleichen.
- [x] Fehlende Coins, andere Candle-Anzahlen und abweichende Checksummen anzeigen.

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
- keine pauschale Synthese fehlender Candles, wenn weder die Exchange noch ein definierter Fallback Daten liefert,
- keine 24x7-Prüfung für Hyperliquid XYZ/TradFi ohne Session-Kalender.

## Fertig wenn

- die bekannten Gaps repariert oder dokumentiert sind,
- der Vortag einmal täglich und bei Fehlern erneut geprüft wird,
- beschädigte Tage im GUI sichtbar und über den normalen Ablauf reparierbar sind,
- ein Checksum-Snapshot täglich auf GitHub veröffentlicht und von Optimizer-Hosts verglichen werden kann.
