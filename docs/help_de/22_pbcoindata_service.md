# PBCoinData Service

PBCoinData ist ein Hintergrunddienst, der CoinMarketCap-Listings (CMC) und Metadaten abruft und daraus Exchange-Symbol-Mappings aufbaut. Diese Mappings bilden die Grundlage für die dynamische Coin-Filter-Logik in PBRun (`ignored_coins.json` / `approved_coins.json`).

## Was PBCoinData macht

- Ruft CMC-Listings (Rang, Market Cap, Tags) nach einem konfigurierbaren Zeitplan ab
- Ruft CMC-Metadaten (Beschreibungen, Kategorien) ab
- Erstellt je Exchange ein Symbol-Mapping (`data/coindata/{exchange}/mapping.json`)
- Führt einen Self-Heal-Zyklus durch, der Exchanges mit fehlgeschlagenem Mapping automatisch erneut versucht
- Schreibt Service-Logs nach `data/logs/PBCoinData.log`

## Konfiguration

Alle PBCoinData-Einstellungen werden direkt auf der **PBCoinData-Detailseite** konfiguriert (`System → Services → PBCoinData → Show Details`).
Änderungen mit dem `:material/save:`-Button in der Sidebar speichern.

| Einstellung | Standard | Beschreibung |
|---|---|---|
| `CoinMarketCap API_Key` | *(leer)* | CoinMarketCap API-Key (erforderlich für CMC-Abrufe) |
| `Fetch Interval` | `24` | Wie oft CMC-Listings neu abgerufen werden (Stunden) |
| `Fetch Limit` | `5000` | Max. Symbole pro CMC-Abruf |
| `Metadata Interval` | `1` | CMC-Metadaten-Aktualisierung (Tage) |
| `Mapping Interval` | `24` | Exchange-Mapping-Rebuild-Intervall (Stunden) |

Ein CMC-API-Key ist erforderlich. Für die meisten Setups reicht ein kostenloser Basic-Plan.
Nach Eingabe eines gültigen API-Keys zeigt die Seite sofort den API-Credit-Status (Monatslimit, Verbrauch, verbleibende Credits).

## PBCoinData-Detailseite

Unter `System → Services → PBCoinData → Show Details` kannst du:

- Den aktuellen PBCoinData-Status prüfen (läuft/gestoppt)
- Den Service ein-/ausschalten
- Den integrierten gefilterten PBCoinData-Log-Viewer im Detailbereich nutzen

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

- **Noch kein Mapping erstellt**: Prüfen ob PBCoinData läuft und ein gültiger CMC-API-Key auf der PBCoinData-Detailseite eingetragen ist
- **Mapping veraltet**: `data/logs/PBCoinData.log` auf wiederholte `ERROR`- oder `self-heal`-Einträge prüfen
- **CMC-Rate-Limit-Fehler (429)**: PBCoinData wiederholt automatisch; bei anhaltenden Fehlern `fetch_interval` erhöhen
- **Ignored/Approved-Listen in PBRun werden nicht aktualisiert**: Mapping-Dateien unter `data/coindata/{exchange}/` prüfen und PBCoinData einmal neu starten
