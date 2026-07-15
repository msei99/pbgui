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

Klicke auf die PBCoinData-Kachel in der Services-Übersicht. Credentials werden unter **Pool** verwaltet; nicht-sensitive Zeitpläne bleiben unter **Settings**.

### CMC-Pool

Unter **Pool -> Add Key** können ein oder mehrere CMC-Keys hinzugefügt werden. Secrets liegen im owner-only Credential Vault und werden weder von Status-APIs noch durch Reveal-Controls zurückgegeben.

- **Imported / externally used** Keys sind erlaubt und nehmen an der fairen lokalen Auswahl teil.
- **Shared quota** markieren, wenn dasselbe Provider-Kontingent außerhalb dieses PBGui-Eintrags mitgenutzt wird.
- Cluster Sync verteilt versiegelte Generationen; keinen CMC-Key in `pbgui.ini` oder pro VPS einrichten.
- Leases koordinieren die Nutzung, wenn sie verfügbar sind, bleiben aber Best Effort. Ohne Lease-System nutzt jeder Node sein lokales Soft-Budget.
- Ein Provider-`429` setzt den betroffenen Key auf Cooldown und der Request kann auf den nächsten geeigneten Key wechseln. Ungültige, deaktivierte, erschöpfte, abkühlende oder konfliktbehaftete Keys werden übersprungen.
- **Rotate** ist optional und ersetzt den gewählten Key durch eine neue unveränderliche Generation. **Disable** behält die Historie; **Delete** veröffentlicht einen Tombstone.

PBCoinData kann ohne bereiten Pool weiterlaufen und Exchange-seitige Mapping-Inputs aktualisieren. CMC-Listings und Metadaten werden jedoch erst mit mindestens einem aktiven materialisierten Key abgerufen.

### Zeitplanung

| Einstellung | Standard | Beschreibung |
|---|---|---|
| `Fetch Interval` | `24` | Wie oft CMC-Listings neu abgerufen werden (Stunden) |
| `Fetch Limit` | `5000` | Max. Symbole pro CMC-Abruf |
| `Metadata Interval` | `1` | CMC-Metadaten-Aktualisierung (Tage) |
| `Mapping Interval` | `24` | Exchange-Mapping-Rebuild-Intervall (Stunden) |

Für die meisten Setups reicht ein kostenloser Basic-Plan. Statusleiste und Pool-Tab zeigen Readiness, aktive Key-Anzahl, Health, Generationen, lokale Nutzung, vom Provider gemeldete Rest-Credits, Cooldowns, Fehler und secret-freie Lease-Statistiken.

## PBCoinData-Detail-Panel

Klicke auf die PBCoinData-Kachel in der Services-Übersicht (oder nutze die Sidebar), um das Detail-Panel zu öffnen:

- Der Control-Strip zeigt den aktuellen Status (läuft/gestoppt) und Start/Stop/Restart-Buttons
- Der **Log**-Tab zeigt einen Live-gefilterten PBCoinData-Log-Viewer
- Der **Pool**-Tab verwaltet CMC-Credentials und zeigt secret-freien Pool-/Lease-Status
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

- **Noch keine CMC-Daten**: Prüfen, ob PBCoinData läuft und **Services -> PBCoinData -> Pool** mindestens einen aktiven materialisierten Key meldet
- **Mapping veraltet**: `data/logs/PBCoinData.log` auf wiederholte `ERROR`- oder `self-heal`-Einträge prüfen
- **CMC-Rate-Limit-Fehler (429)**: Der betroffene Key geht in Cooldown und der Pool versucht einen anderen geeigneten Key; wenn alle Keys limitiert bleiben, `fetch_interval` erhöhen
- **Ignored/Approved-Listen in PBRun werden nicht aktualisiert**: Mapping-Dateien unter `data/coindata/{exchange}/` prüfen und PBCoinData einmal neu starten
