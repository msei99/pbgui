# Market Data

Diese Seite steuert die PBGui-Market-Data-Workflows für Hyperliquid und Binance USDM: l2Book-Archiv-Downloads, TradFi-Symbol-Mapping, Binance 1m Auto-Refresh und Build best 1m OHLCV Jobs.

## Seitenaufbau

Die Expander erscheinen in dieser Reihenfolge:
1. Settings (Latest 1m Auto-Refresh) — Hyperliquid
2. Settings (Binance USDM Latest 1m Auto-Refresh)
3. Market Data status (Hyperliquid)
4. Market Data status (Binance USDM)
5. Build best 1m OHLCV
6. TradFi Symbol Mappings
7. Download l2Book from AWS

## Settings (Latest 1m Auto-Refresh) — Hyperliquid

Steuert den automatischen 1m-Candle-Refresh-Loop für Hyperliquid-Symbole.

- **Enabled coins** — Multiselect aus allen bekannten Hyperliquid-Symbolen
- **Select all / Clear all** — alle Coins schnell aktivieren oder deaktivieren
- **Cycle interval (s)** — wie oft alle aktivierten Coins aktualisiert werden (Standard: 1800s)
- **Pause between coins (s)** — Pause zwischen Coins um Rate-Limits zu vermeiden (Standard: 0,5s)
- **API timeout per coin (s)** — Timeout pro Coin (Standard: 30s)
- **Min / Max lookback days** — Fenster für den letzten Fetch (Standard: 2 / 4 Tage)
- Änderungen werden in `pbgui.ini` gespeichert und im nächsten Zyklus wirksam — kein Neustart nötig.

## Settings (Binance USDM Latest 1m Auto-Refresh)

Steuert den automatischen 1m-Candle-Refresh-Loop für Binance USDM Perpetuals.

- **Enabled coins** — Multiselect aus allen bekannten Binance USDM Coins
- **Select all / Clear all** — alle Coins schnell aktivieren oder deaktivieren
- **Cycle interval (s)** — wie oft alle aktivierten Coins aktualisiert werden (Standard: 3600s)
- **Pause between coins (s)** — Pause zwischen Coins (Standard: 0,5s)
- **API timeout per coin (s)** — Timeout pro Coin (Standard: 30s)
- **Min / Max lookback days** — Fenster für den letzten Fetch (Standard: 2 / 7 Tage)
- Änderungen werden in `pbgui.ini` gespeichert und im nächsten Zyklus wirksam — kein Neustart nötig.

## Market Data Status

Dieser Bereich dient zur Überwachung von Fetch-Loops, Inventar und Background-Jobs.

Der Status-Expander aktualisiert sich automatisch alle 5 Sekunden.

### Steuer-Buttons

- **⏩ Run now** — überspringt die verbleibende Wartezeit und startet den nächsten Refresh-Zyklus sofort
- **⏹ Cancel queued refresh** — erscheint statt Run now, wenn bereits ein Refresh eingereiht ist; bricht ihn vor dem Start ab
- **⏹ Stop current run** — erscheint während eines laufenden Zyklus; sendet ein Stop-Signal, sodass PBData nach dem aktuellen Coin abbricht

### Fortschrittsbalken

Während ein Zyklus läuft, zeigt ein Fortschrittsbalken `erledigte / gesamt Coins` und den aktuellen Coin.

### Status-Tabelle

Zeigt das Ergebnis des letzten abgeschlossenen Zyklus pro Coin:
- `last_fetch` — Zeitstempel des letzten Versuchs
- `result` — `ok`, `error` oder `skipped`
- `lookback_days` — abgerufene Tage
- `minutes_written` — geschriebene Candles in diesem Lauf
- `note` — `no_local_data` bedeutet: noch keine lokalen Daten vorhanden; maximales Lookback-Fenster wurde automatisch verwendet
- `next_run_in_s` — geschätzte Sekunden bis zum nächsten Zyklus

### Verhalten nach Neustart

Wenn PBData neu gestartet wird, liest es den letzten Lauf-Timestamp und wartet die verbleibende Intervallzeit ab — kein sofortiger Re-Fetch. Bei einem Absturz mitten im Zyklus wird der Lauf ab dem letzten abgeschlossenen Coin fortgesetzt.

---
- Read-only Inventar für PBGui- und PB7-Cache-Daten
- Source-Code-basierte Coverage-Ansichten
- Job-Fortschritt mit Tages-/Monatskontext bei Stock-Perp-Builds
- In der Stock-Perp-Minute-Ansicht können die Overlays `market holiday` und `expected out-of-session gap` ausgeschaltet werden, um rohe Missing-Gaps direkt zu sehen
- Die Minute-Ansicht enthält optional einen `OHLCV chart`-Expander mit interaktiven Plotly-Candles und Volume-Balken zur schnellen visuellen Prüfung
- Der Chart nutzt Lazy-Zoom: vollständig herausgezoomt werden grobe Kerzen (typisch `1d`) angezeigt, beim Reinzoomen wird automatisch auf feinere Timeframes umgerechnet — keine manuelle Timeframe-Auswahl nötig
- Der Coin-Name wird oben links im Chart als Label angezeigt
- Für Equity-Stock-Perps werden historische Aktiensplit-Daten als vertikale gestrichelte orangefarbene Linien mit Annotationen (z.B. "Split 20:1") angezeigt; OHLCV-Daten werden automatisch für Splits angepasst
- Split-Faktor-Daten werden pro Exchange in `data/coindata/hyperliquid/split_factors.json` gespeichert (via Tiingo Daily API abgerufen)

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

Startet Background-Build-Jobs für berechtigte Symbole.

### Job-Typen

**`hl_best_1m`** — Hyperliquid XYZ Stock-Perps:
- Berechtigung: Mapping-Status `ok` + Tiingo-Ticker vorhanden
- Controls: Build best 1m, Start date, End date, Refetch TradFi from scratch

**`binance_best_1m`** — Binance USDM vollständiger historischer Backfill:
- Lädt komplette 1m OHLCV-Daten von Inception bis heute aus offiziellen Binance-Archiven (data.binance.vision) — monatliche + tägliche ZIPs — mit CCXT-Lückenfüllung
- Coin-Auswahl aus allen aktivierten Binance Coins
- Controls: Start date, End date, Refetch
- Speicherpfad: `data/ohlcv/binanceusdm/1m/<COIN>/YYYY-MM-DD.npz` (gleiches Format wie PB7 Cache)

### Job-Verwaltung

Das Job-Panel zeigt drei Bereiche:
- **Pending** — eingereihte Jobs
- **Running** — aktuell laufender Job mit Live-Fortschritt
- **Failed / Done** — abgeschlossene Jobs

Aktionen:
- **Retry** — stellt einen fehlgeschlagenen Job wieder in Pending ein
- **Delete** — löscht einen einzelnen Job
- **Delete selected / Delete all** — Bulk-Löschen aus Failed- oder Done-Liste
- **Raw JSON** (🔍 Button) — zeigt den vollständigen Job-Datei-Inhalt zur Fehlersuche

### Fortschrittsanzeige

Während ein Job läuft, zeigt das Panel:
- Stage: `starting`, `running`, `done`
- Aktueller Coin
- Chunk erledigt / gesamt
- Geschriebene Minuten
- Laufzeit
- Für Binance: abgerufene Pages, abgedeckte Tage
- Für HL TradFi: Monat YYYY-MM Tag X/Y, Tiingo-Quota-Auslastung, 429-Wartezustände

### Datenstrategie (hl_best_1m)

Build best 1m läuft im gewählten Datumsfenster immer von neu → alt.

Für Crypto-Symbole (non-XYZ):
- Nutzt zuerst lokales `1m_api` und lokale `l2Book`-Konvertierung
- Füllt verbleibende Lücken über Perp-Exchange-Fallback
- `l2Book` wird nur in diesem Crypto-Pfad genutzt (nicht für XYZ-Stock-Perps)

Für FX-gemappte Stock-Perps (`tiingo_fx_ticker`):
- Nutzt Tiingo FX 1m in Wochen-Chunks (weniger Requests)
- Nutzt bestehende `other_exchange`-Historie als Anker, wenn kein Refetch aktiv ist
  - Start-Cursor = ältester vorhandener `other_exchange`-Tag minus 1 Tag
- `Refetch` startet am gewählten/End-Tag und baut rückwärts im erlaubten Bereich neu auf
- Weekend-Sessiongrenze folgt dem beobachteten Feed-Verhalten:
  - Freitag-Close = 17:00 New York Lokalzeit (DST-aware in UTC)
  - Sonntag-Reopen ≈ 22:00 UTC (fix)
- Bekannte reduzierte FX-Feiertagssessions:
  - `12-24` und `12-31`: frühes Close um ca. 22:00 UTC
  - `12-25` und `01-01`: spätes Reopen um ca. 23:00 UTC

Für Equity-gemappte Stock-Perps (`tiingo_ticker`):
- Nutzt Tiingo IEX 1m
- Nutzt bestehende `other_exchange`-Historie als Anker, wenn kein Refetch aktiv ist
  - Start-Cursor = ältester vorhandener `other_exchange`-Tag minus 1 Tag
- Untere Grenze bleibt `max(tiingo_start_date, 2016-12-12)`
- Raw-first-Write-Verhalten: alle von Tiingo gelieferten Minuten werden geschrieben (kein zusätzliches Market-Hours-Clipping im Write-Pfad)

Write-Sicherheitsregeln:
- TradFi-Write (`other_exchange`) füllt nur fehlende Minuten oder Minuten, die bereits als `other_exchange` markiert sind
- Bereits vorhandene `api` / `l2Book_mid` Minuten werden durch TradFi nicht überschrieben

Datums-Controls:
- `Start date` begrenzt den ältesten zu verarbeitenden Tag
- `End date` begrenzt den neuesten zu verarbeitenden Tag (Standard = heute)

### Fortschritt und Wartezustände (hl_best_1m)

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
