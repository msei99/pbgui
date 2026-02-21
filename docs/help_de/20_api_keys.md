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
	- API-Secret ist nur für `alpaca` erforderlich.
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
	- Gute historische 1m-Intraday-Abdeckung (typisch mehrere Jahre, häufig 5+ Jahre).
	- **Empfohlener Standard als Extended Provider** für Backtests älter als 7 Tage.
- `polygon`
	- Free-Tier/Plan ist accountabhängig.
	- Tiefe der Intraday-Historie hängt vom Plan ab; kann wenig oder keine nutzbaren 1m-Daten liefern.
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
| `alpaca` | Ja (`key` + `secret`) | Mehrjährige 1m-Historie (häufig 5+ Jahre) | Free-Feed ist verzögert, für Backtests aber unkritisch | **Bester Standard als Extended Provider** |
| `polygon` | Ja (`key`) | **PB7-Annahme aktuell:** bis ca. 2 Jahre 1m-Historie im Free-Zugang | **PB7-Annahme aktuell:** ca. 5 Calls/Min und 50k Bars/Request; account-/planabhängig | Nur nutzen, wenn Test die Daten für deinen Plan bestätigt |
| `finnhub` | Ja (`key`) | Für 1m-Backtest-Historie nicht praxistauglich | Free-Tier für diesen Workflow i. d. R. ungeeignet | Nicht empfohlen |
| `alphavantage` | Ja (`key`) | Für größere 1m-Zeiträume oft zu wenig/zu langsam | Starke Tageslimits im Free-Tier | Nur für kleine Ad-hoc-Checks |

Polygon-Hinweis: Möglichkeiten haben sich über die Zeit geändert (Polygon → Massive). Immer mit **Test Connection** und Candle-Count für den gewünschten Zeitraum verifizieren.

### Empfehlung

- `yfinance` für das aktuelle Zeitfenster verwenden (in PBGui automatisch).
- **`alpaca`** als Extended Provider für Monate/Jahre an 1m-Backtests konfigurieren.
- `polygon` nur verwenden, wenn dein Plan die benötigte Intraday-Historie explizit enthält.

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
