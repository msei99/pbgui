# Market Data

Diese Seite steuert die PBGui-Market-Data-Workflows für Hyperliquid: l2Book-Archiv-Downloads, TradFi-Symbol-Mapping und Build best 1m OHLCV Jobs.

## Seitenaufbau

Die Expander erscheinen in dieser Reihenfolge:
1. Market Data status
2. Build best 1m OHLCV
3. TradFi Symbol Mappings
4. Download l2Book from AWS

## Market Data Status

Dieser Bereich dient zur Überwachung von Fetch-Loops, Inventar und Background-Jobs.

Highlights:
- Read-only Inventar für PBGui- und PB7-Cache-Daten
- Source-Code-basierte Coverage-Ansichten
- Job-Fortschritt mit Tages-/Monatskontext bei Stock-Perp-Builds

## TradFi Symbol Mappings

Dieser Bereich ist die zentrale Steuerung für XYZ-Stock-Perp-Symbolrouting.

### Tabelle

Die Mapping-Tabelle wird aus folgenden Quellen zusammengeführt:
- Hyperliquid Mapping (`mapping.json`)
- Manuelle/angereicherte Einträge (`tradfi_symbol_map.json`)

Angezeigte Spalten u. a.:
- Symbol (Hyperliquid-Link)
- HL Price / Tiingo Price
- Description / Type / Status
- Start Date / Fetch Start
- Pyth-Link
- Verification und Notes

Tabellen-Filter:
- Filter by status
- Filter by symbol (matcht XYZ-Symbol und Tiingo-Symbol/Ticker)
- Filter by type (canonical type, z. B. `equity_us`, `fx`)

Startdate-Semantik:
- Start Date: Provider-Metadatum (`tiingo_start_date`)
- Fetch Start: effektives frühestes Fetch-Datum
  - IEX Equity nutzt `max(Start Date, 2016-12-12)`
  - Leer, wenn Start Date unbekannt ist

### Action Buttons

Die Buttons sind in zwei ausgerichteten Reihen angeordnet.

Reihe 1 (Workflow pro ausgewähltem Symbol):
- Search ticker
- Edit
- Test Resolve
- Fetch start date
- Spec

Reihe 2 (globale Aktionen):
- Auto-Map
- Fetch all start dates
- Refresh metadata
- Refresh prices
- View specs

### Specs Popup

`View specs` öffnet ein Popup mit:
- Source/Fetched-Timestamp/Row-Count
- Link zur originalen XYZ-Spec-Seite
- großer Tabellenansicht (nutzt die Dialoghöhe)
- klickbaren Links:
  - Pyth Link
  - HL Link

### Hinweise

- `Fetch start date` gilt nur für Equity (Daily-Metadaten-Endpoint).
- Für FX gibt es keinen dedizierten Startdate-Metadata-Fetch-Button.
- Auto-Map sowie Metadata/Price-Refresh benötigen einen konfigurierten Tiingo API-Key.

## Download l2Book from AWS

Lädt Hyperliquid l2Book-Archivdateien (Requester Pays).

Workflow:
1. AWS-Profil und Region konfigurieren
2. Coins und Datumsbereich auswählen
3. Auto-Download-Job starten

UI-Verhalten:
- Die Download-Job-Queue wird direkt unter den Download-Controls angezeigt
- `Last download job` ist als einklappbare Zusammenfassung verfügbar
- Die Zusammenfassung zeigt Status, Coins, Range, Counts (downloaded/skipped/failed), Größen-Statistik, Fortschritt % und Laufzeit

Kostenverhalten:
- Lokale Dateien werden zuerst geprüft und übersprungen
- Übersprungene Dateien verursachen keinen zusätzlichen Transfer-/Download-Aufwand

Speicherpfad:
- `data/ohlcv/hyperliquid/l2Book/<COIN>/<YYYYMMDD>-<H>.lz4`

## Build best 1m OHLCV

Startet Background-`hl_best_1m`-Jobs für berechtigte XYZ-Symbole.

Berechtigung in der Coin-Auswahl:
- Mapping-Status muss `ok` sein
- Tiingo-Mapping muss vorhanden sein (`tiingo_ticker` oder `tiingo_fx_ticker`)

Controls:
- Build best 1m
- Start date (optional)
- Refetch TradFi data from scratch (stock-perps)

### Datenstrategie (Stock-Perps)

Tiingo ist die TradFi-Quelle.

Für FX-gemappte Stock-Perps:
- Backfill-Richtung ist neu nach alt
- Build stoppt nach aufeinanderfolgenden leeren Chunks
- Bereits vorhandene Source-Daten werden respektiert, um unnötige Fetches zu vermeiden

Für Equity-gemappte Stock-Perps:
- Nutzt Tiingo IEX 1m
- Nutzt effektive Startlogik (`tiingo_start_date` + IEX-Floor)

### Fortschritt und Wartezustände

Im Job-Panel können u. a. angezeigt werden:
- `month YYYY-MM day X/Y`
- Tiingo month request usage
- Quota/429-Wartezustände mit Sekunden und Grund

## Tiingo Settings (im Settings-Bereich)

Die Seite enthält Tiingo-Controls:
- `tiingo_api_key`
- Test Tiingo Button
- Runtime-Quota-Anzeigen (Stunde/Tag/Monatsbandbreite)
- Externe Links für API-Key-Signup und Usage-Dashboard

## Troubleshooting

Wenn ein Build-Job kurz erscheint und wieder verschwindet:
1. Neuesten Failed-Job in `data/ohlcv/_tasks/failed` prüfen
2. Sicherstellen, dass der Worker mit aktuellem Code läuft (ggf. Worker neu starten)
3. Tiingo-Key und Symbol-Mapping-Status prüfen
4. `Test Resolve` für das ausgewählte Symbol verwenden

Wenn die Build-Coin-Liste leer ist:
- Prüfen, ob Symbole gemappt sind und Status `ok` haben
- Prüfen, ob Tiingo Ticker oder FX Ticker im Mapping gesetzt ist
