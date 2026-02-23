# API-Keys (PBGui / PB7)

PBGui unterstützt sowohl Exchange-API-Credentials als auch TradFi-Provider-Credentials für Stock-Perp-Backtests.

## Wo die Daten verwendet werden

- **Setup → API-Keys** bearbeitet Exchange-User in `api-keys.json`.
- PB7 Live-Trading liest diese Exchange-User aus `api-keys.json`.
- TradFi-Provider-Konfiguration wird in `pbgui.ini` (`[tradfi_profiles]`) und in der PBGui-User-Konfiguration (`users.tradfi`) gespeichert.

## Exchange-User (`api-keys.json`)

Jeder User ist ein JSON-Objekt unter seinem Usernamen:

```json
{
	"myuser": {
		"exchange": "bybit",
		"key": "...",
		"secret": "...",
		"passphrase": "..."
	}
}
```

### Erkannte Felder

- Pflicht
	- `exchange`

- Credentials
	- `key` (Aliase beim Laden: `apiKey`, `api_key`)
	- `secret`
	- `passphrase` (Alias beim Laden: `password`)

- Hyperliquid-spezifisch
	- `wallet_address` (Aliase beim Laden: `walletAddress`, `wallet`)
	- `private_key` (Alias beim Laden: `privateKey`)
	- `is_vault` (boolean)

- Optionaler PB7/CCXT-Passthrough
	- `quote` (string)
	- `options` (JSON-Objekt)
	- `extra` (JSON-Objekt für exchange-spezifische Zusatzwerte)

Unbekannte Zusatzfelder werden erhalten, damit bestehende Setups kompatibel bleiben.

## TradFi Data Provider (Stock-Perps Backtesting)

Auf der API-Keys-Seite gibt es zusätzlich den Bereich **TradFi Data Provider**:

- **yfinance**
	- Standardquelle für die letzten 7 Tage.
	- Kein API-Key nötig.
	- Install/Uninstall und Test sind direkt in der UI möglich.

- **Extended provider** (optional, für ältere Historie)
	- Provider: `alpaca`, `polygon`, `finnhub`, `alphavantage`
	- API-Key ist erforderlich.
	- Aktionen: **Test Connection**, **Save TradFi Config**, **Clear TradFi Config**.

### TradFi-Runtime-Verhalten (Single Source of Truth)

- Diese Seite ist für **Credentials und Provider-Setup** zuständig.
- Das Runtime-Market-Data-Verhalten (HIP-3-Flow, Quellen-Priorität und Loop-Scope) steht im Market-Data-Guide:
	- `docs/help/26_market_data.md` (EN)
	- `docs/help_de/26_market_data.md` (DE)

### Free-Provider-Abdeckung (Kurzüberblick)

Die Angaben sind als praktische PBGui/PB7-Richtwerte zu verstehen; Provider-Pläne können sich ändern.

- `yfinance`
	- In PBGui kostenlos nutzbar, kein API-Key erforderlich.
	- Wird im PBGui-Workflow standardmäßig für die letzten ~7 Tage genutzt.
- `alpaca`
	- Kostenloser API-Key verfügbar.
	- In der API-Keys-UI als empfohlener Provider markiert.
	- Free-Tier kann für diesen Workflow mehrjährige 1m-Historie liefern.
- `polygon`
	- Abdeckung ist planabhängig.
	- Free-Pläne können bei 1m-Intraday-Abfragen `0 candles` liefern.
- `finnhub`
	- Free-Tier liefert für diesen Workflow keine praxistaugliche 1m-Intraday-Historie.
	- Für PBGui-Stock-Perp-Backtests nicht empfohlen.
- `alphavantage`
	- Free-Tier ist stark rate-limitiert (z. B. Tageslimits bei API-Calls).
	- Für größere historische Backfills meist zu eingeschränkt.

### Provider-Matrix (Free-Tier-orientiert)

| Provider | API-Key nötig | Praxistaugliche 1m-Tiefe für PBGui/PB7 | Free-Tier-Limits (praktisch) | Empfehlung |
|---|---:|---|---|---|
| `yfinance` | Nein | Aktuelles Fenster (PBGui-Standard-Workflow: letzte ~7 Tage) | Kein dedizierter Key, Verhalten externer Quelle kann variieren | Für aktuelle Kerzen aktiv lassen |
| `alpaca` | Ja (`key` + `secret`) | Mehrjährige 1m-Historie (praktisch gute Abdeckung) | Benötigt Account-Credentials mit Market-Data-Zugriff | **Empfohlener Extended Provider** |
| `polygon` | Ja (`key`) | Planabhängig für 1m-Intraday-Historie | Free-Plan kann für Backfills unzureichend sein | Optional, Plan prüfen |
| `finnhub` | Ja (`key`) | Für 1m-Backtest-Historie nicht praxistauglich | Free-Tier für diesen Workflow i. d. R. ungeeignet | Nicht empfohlen |
| `alphavantage` | Ja (`key`) | Für größere 1m-Zeiträume oft zu wenig/zu langsam | Starke Tageslimits im Free-Tier | Nur für kleine Ad-hoc-Checks |

### Empfehlung

- `yfinance` für das aktuelle Zeitfenster verwenden (in PBGui automatisch).
- **`alpaca`** als Extended Provider für Stock-Perp (HIP-3) 1m-Backtests konfigurieren.

### Verhalten von „Test Connection“

- Der Test lädt `AAPL` 1m-Kerzen für die letzten 7 abgeschlossenen Tage.
- Erfolg bedeutet: Kerzen wurden geliefert.
- `0 candles` kann Plan-/Tier-Limits bedeuten (nicht zwingend ein technischer Fehler).

## Beispiel (PB7/CCXT-Stil)

```json
{
	"myuser": {
		"exchange": "bybit",
		"apiKey": "...",
		"secret": "...",
		"password": "...",
		"quote": "USDT",
		"options": {"defaultType": "swap"},
		"uid": "123456"
	}
}
```

## Upstream-Referenz

- https://github.com/enarjord/passivbot
