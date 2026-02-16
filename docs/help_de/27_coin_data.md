# Coin Data

Die Coin-Data-Seite verwaltet das Mapping zwischen Exchange-Symbolen und CoinMarketCap-Metadaten und zeigt filterbare Symbolqualität sowie Trading-Limits.

## Was diese Seite macht

- Exchange-spezifische Symbol-Mappings erstellen und aktualisieren
- CoinMarketCap-Daten (Rank, Market Cap, Tags, Metadaten) zusammenführen
- Live-Preise und `vol/mcap` aktualisieren
- Copy-Trading-Verfügbarkeit und Exchange-Limits anzeigen (Min Amount/Cost, Leverage)

## Refresh-Controls (Sidebar)

- `:material/sync:` Ausgewählte Exchange aktualisieren
  - Holt Markets, aktualisiert den Copy-Trading-Cache, baut Mapping neu, aktualisiert Preise
- `:material/sync_alt:` Alle Exchanges aktualisieren
  - Führt denselben Ablauf für alle V7-Exchanges mit Fortschrittsanzeige aus
- `:material/cloud_sync:` CoinMarketCap-Daten aktualisieren
  - Lädt Listings und Metadaten neu und aktualisiert danach die gewählte Exchange

## Last-Refreshed Status

Oben zeigt Coin Data Zeitstempel für:

- CMC Listings und Metadaten
- Market-Snapshot der gewählten Exchange (`ccxt_markets.json`)
- Mapping-Datei (`mapping.json`)
- Neuester beobachteter Price-Timestamp aus Mapping-Zeilen
- Copy-Trading-Cache-Datei

Damit kannst du schnell prüfen, ob die Daten frisch sind.

## Filter und Tabellen

Filter in der Kopfzeile:

- Exchange
- Minimum `market_cap`
- Maximum `vol/mcap`
- Tags

Die Seite enthält:

- **CMC unmatched** Expander: Symbole ohne aktuelle CMC-Zuordnung
- **Haupttabelle**: gematchte Nicht-HIP-3-Symbole nach Filtern
- **HIP-3 symbols** Expander (nur Hyperliquid)

Klicke eine Tabellenzeile, um den vollständigen Notice-Text unter der Tabelle zu sehen (falls vorhanden).

## Hyperliquid Hinweise

- Quote-Priorität ist standardmäßig `USDC`, danach `USDT0`
- Wenn keine HIP-3-Symbole gefunden werden, kann Coin Data das Hyperliquid-Mapping einmal automatisch neu bauen
- Wenn HIP-3 vorhanden aber ausgefiltert ist, Filter lockern (z. B. `market_cap` auf `0`, Tags leeren)

## Daten-Dateien

Coin Data liest/schreibt unter:

- `data/coindata/coindata.json`
- `data/coindata/metadata.json`
- `data/coindata/<exchange>/ccxt_markets.json`
- `data/coindata/<exchange>/mapping.json`
- `data/coindata/<exchange>/copy_trading.json`

## Troubleshooting

### Keine Zeilen sichtbar

- Ausgewählte Exchange aktualisieren
- Filter testweise lockern (`market_cap=0`, hohes `vol/mcap`, keine Tags)
- CMC- und Mapping-Zeitstempel prüfen

### Preis fehlt bei einigen Symbolen

- Ausgewählte Exchange erneut aktualisieren
- Prüfen, ob der Markets-Datei-Zeitstempel aktuell ist
- Bei Hyperliquid beachten: manche Symbole nutzen Market-Info-Fallback-Preise

### CMC unmatched ist hoch

- Erst CMC-Daten aktualisieren, dann die ausgewählte Exchange
- Prüfen, ob Symbole neu gelistet sind oder Exchange-spezifische Namensvarianten verwenden
