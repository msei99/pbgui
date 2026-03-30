# PBCoinData Service

PBCoinData ist ein Hintergrunddienst, der CoinMarketCap-Listings (CMC) und Metadaten abruft und daraus Exchange-Symbol-Mappings aufbaut. Diese Mappings bilden die Grundlage für die dynamische Coin-Filter-Logik in PBRun (`ignored_coins.json` / `approved_coins.json`).

## Was PBCoinData macht

PBCoinData führt eine Daemon-Schleife (60-Sekunden-Zyklus) aus:

- Ruft CMC-Listings (Rang, Market Cap, Volumen, Tags) nach einem konfigurierbaren Zeitplan ab
- Ruft CMC-Metadaten ab — primär das `notice`-Feld (Delisting-/Migrationswarnungen, genutzt von dynamischen Filtern)
- Erstellt je Exchange ein Symbol-Mapping (`data/coindata/{exchange}/mapping.json`) für alle V7-unterstützten Exchanges (binance, bybit, bitget, gateio, hyperliquid, okx)
- Erkennt Copy-Trading-Symbole pro Exchange (bybit: aus CCXT-Marktdaten; binance/bitget: via authentifizierte API mit automatischer User-Erkennung)
- Löst doppelte CMC-Symbole (z. B. HOT, ACT) via preisbasierte Disambiguierung mit Exchange-Ticker-Preisen auf
- Führt TradFi-Sync für Hyperliquid HIP-3 Stock-Perp-Symbole aus (nur Master für Web-Scraping, alle Nodes für Spec-Sync)
- Führt einen Self-Heal-Zyklus durch, der Exchanges mit fehlgeschlagenem Mapping automatisch erneut versucht (exponentielles Backoff)
- Schreibt Service-Logs nach `data/logs/PBCoinData.log`

## Konfiguration

Alle PBCoinData-Einstellungen werden im **Settings**-Tab des PBCoinData-Detail-Panels konfiguriert.
Klicke auf die PBCoinData-Kachel in der Services-Übersicht, dann zum **Settings**-Tab wechseln.

| Einstellung | Standard | Beschreibung |
|---|---|---|
| `CoinMarketCap API_Key` | *(leer)* | CoinMarketCap API-Key (erforderlich für CMC-Abrufe) |
| `Fetch Interval` | `24` | Wie oft CMC-Listings neu abgerufen werden (Stunden) |
| `Fetch Limit` | `5000` | Max. Symbole pro CMC-Abruf |
| `Metadata Interval` | `1` | CMC-Metadaten-Aktualisierung (Tage) |
| `Mapping Interval` | `24` | Exchange-Mapping-Rebuild-Intervall (Stunden) |

Ein CMC-API-Key ist erforderlich. Für die meisten Setups reicht ein kostenloser Basic-Plan.
Nach Eingabe eines gültigen API-Keys zeigt die Statusleiste über den Tabs den API-Credit-Status (Monatslimit, Verbrauch, verbleibende Credits).

## PBCoinData-Detail-Panel

Klicke auf die PBCoinData-Kachel in der Services-Übersicht (oder nutze die Sidebar), um das Detail-Panel zu öffnen:

- Der Control-Strip zeigt den aktuellen Status (läuft/gestoppt) und Start/Stop/Restart-Buttons
- Der **Log**-Tab zeigt einen Live-gefilterten PBCoinData-Log-Viewer
- Der **Settings**-Tab enthält das oben beschriebene Konfigurationsformular

## Self-Heal-Zyklus

Schlägt ein Mapping-Build für eine Exchange fehl (z. B. durch einen temporären Netzwerkfehler), versucht PBCoinData diese Exchange im nächsten Zyklus automatisch erneut (exponentielles Backoff). Der Log zeigt `[self-heal]`-Einträge für diese Wiederholungen.

## Datendateien

| Pfad | Beschreibung |
|---|---|
| `data/coindata/coindata.json` | CMC-Listings-Snapshot |
| `data/coindata/metadata.json` | CMC-Metadaten-Snapshot |
| `data/coindata/{exchange}/mapping.json` | Exchange-Symbol → CMC-Coin-Mapping |
| `data/coindata/{exchange}/ccxt_markets.json` | Roher CCXT-Markt-Snapshot |
| `data/logs/PBCoinData.log` | Service-Log |

## Schnelle Fehlersuche

- **Noch kein Mapping erstellt**: Prüfen ob PBCoinData läuft und ein gültiger CMC-API-Key im PBCoinData **Settings**-Tab eingetragen ist
- **Mapping veraltet**: `data/logs/PBCoinData.log` auf wiederholte `ERROR`- oder `self-heal`-Einträge prüfen
- **CMC-Rate-Limit-Fehler (429)**: PBCoinData wiederholt automatisch; bei anhaltenden Fehlern `fetch_interval` erhöhen
- **Ignored/Approved-Listen in PBRun werden nicht aktualisiert**: Mapping-Dateien unter `data/coindata/{exchange}/` prüfen und PBCoinData einmal neu starten
