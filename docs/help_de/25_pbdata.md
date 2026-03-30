# PBData (Service)

PBData ist ein Hintergrund-Service in PBGui. Es lädt kontinuierlich Account-Daten via REST und Live-Preise über öffentliche WebSockets, und schreibt alles in die PBGui-Datenbank, damit andere Seiten diese Daten schnell verwenden können.

Klicke auf die PBData-Kachel in der Services-Übersicht, um das Detail-Panel mit drei Tabs zu öffnen: **Log**, **Settings** und **Status**.

## Was PBData lädt

### Preise (öffentlicher WebSocket)

PBData öffnet einen **öffentlichen** WebSocket pro Exchange und abonniert Preis-Ticker für alle Symbole mit offenen Positionen. Preise werden im Speicher gepuffert und alle 10 Sekunden in die Datenbank geschrieben.

### Account-Daten (REST Poller)

Alle Account-Daten werden via REST abgerufen — PBData verwendet **keine** privaten WebSocket-Verbindungen. Pro ausgewähltem User (siehe **Settings**-Tab → Users):

- **Combined Poller** (einer pro Exchange, serialisiert pro User)
  - Balances (Standard ca. alle ~300 s)
  - Positions (Standard ca. alle ~300 s)
  - Orders (Standard ca. alle ~60 s)
- **History Poller** (einer pro Exchange)
  - Income / Funding History
- **Executions Poller** (einzelner Task)
  - My Trades — *Opt-in*, nur für User in der Executions-Download-Liste

### Aktuelle 1-Minuten-Candles

Separate Tasks laden die aktuellen 1-Minuten-OHLCV-Candles für Hyperliquid, Binance und Bybit (genutzt von der Marktdaten-Pipeline).

## Users vs. Executions download (Opt-in)

PBData hat zwei getrennte User-Listen:

- **Users**
  - User, die PBData aktiv via REST abfragt
- **Executions download**
  - **Opt-in Allow-List**: Nur diese User laden/speichern Executions (My Trades)
  - Default ist **keine Auswahl**
  - Änderungen wirken schnell; PBData prüft vor jedem Executions-Fetch erneut

## Timer und Performance

Im **Settings**-Tab unter **Timers** kannst du steuern, wie aggressiv PBData pollt.

- **Max private WS**
  - Globales Limit für private WebSocket-Clients, die das Live-Streaming des Dashboards (`api/live.py`) öffnen darf. Betrifft nicht PBData selbst (das nur REST nutzt), aber die Einstellung wird hier verwaltet, weil PBData den Exchange-Connection-Pool besitzt.
- **Startup delay (s)**
  - Wartezeit nach Start, bevor die Shared REST Poller beginnen
- **Combined interval (s)**
  - Intervall für den Shared Combined REST Poll (Balances + Positions + Orders Fallback/Refresh)
- **Balance interval (s)**
  - Intervall für den dedizierten Balance REST Poll
- **Positions interval (s)**
  - Intervall für den dedizierten Positions REST Poll
- **Orders interval (s)**
  - Intervall für den dedizierten Orders REST Poll
- **History interval (s)**
  - Intervall für Shared History Updates
- **Executions interval (s)**
  - Intervall für Shared Executions (My Trades)
- **Market data coin pause (s)**
  - Pause zwischen Coin-Abrufen in der 1-Minuten-Marktdaten-Pipeline

Allgemeine Hinweise:

- Zu kleine Intervalle führen oft zu **Rate Limits (HTTP 429)**.
- Wenn du häufig Backoffs siehst: Intervalle erhöhen oder Anzahl aktiver User reduzieren.

## Rate-Limit Kontrolle (REST Pause)

PBData nutzt eine kleine Pause zwischen Usern in den Shared REST Pollern.

- **REST pause/user (s)**
  - Globale Pause zwischen Usern beim Shared REST Polling

### Shared REST pause per exchange

Manche Exchanges brauchen eine größere Pause.

- Du kannst eine Pause pro Exchange setzen.
- Werte, die dem globalen Wert entsprechen, werden nicht als Override gespeichert.
- Wenn kein Override gesetzt ist, verwendet PBData die eingebauten Exchange-Defaults (z. B. Hyperliquid/Bybit).

## Log-Viewer Tipps

Der PBData **Log**-Tab nutzt den Live-Log-Viewer. Er streamt Log-Zeilen via WebSocket und unterstützt:

- **Files-Sidebar** — klicke auf den **Files**-Button (oder das Dateiname-Badge in der Toolbar), um eine Sidebar mit allen verfügbaren Log-Dateien zu öffnen. Klicke auf eine Datei, um zu ihr zu wechseln. Es wird immer nur eine Datei angezeigt.
- **Level-Filter-Buttons** — **DBG**, **INF**, **WRN**, **ERR**, **CRT** ein-/ausschalten, um Zeilen nach Schweregrad zu filtern
- **Suche** — Freitext in das Suchfeld eingeben oder ein **Preset** wählen (Errors, Warnings, Connection, Restart/Stop, Traceback). Mit der **Filter**-Checkbox zwischen Filtern (Nicht-Treffer ausblenden) und Hervorheben (alles zeigen, Treffer markieren) umschalten. Die **▲ / ▼**-Buttons springen zwischen Treffern.
- **Lines** — wähle, wie viele Zeilen im Blick bleiben (200 / 500 / 1000 / 2000 / 5000)
- **Steuerungs-Buttons**:
  - ⏸ **Pause** / ▶ **Resume** — Live-Stream anhalten oder fortsetzen
  - 🗑 **Clear** — alle Zeilen aus der Anzeige entfernen
  - ↓ **Download** — das aktuell angezeigte Log herunterladen
  - **# Lines** — Zeilennummern ein-/ausblenden

Die Einstellung **Log Level** (steuert, wie ausführlich PBData loggt) befindet sich im **Settings**-Tab, nicht im Log-Viewer.

## Status-Tab

Der **Status**-Tab zeigt den Fetch-Summary- und Poller-Metrics-Bereich.

Er zeigt eine kompakte Laufzeitübersicht für:

- Ergebnisse von Balances / Positions / Orders
- Ergebnisse von History / Executions
- Letzte Fetch-Zeitpunkte und Status pro User

Wenn noch keine Summary sichtbar ist, hat PBData meist noch keinen ersten Summary-Zyklus geschrieben.

## Wo Settings gespeichert werden

Die meisten PBData-Settings werden in `pbgui.ini` unter `[pbdata]` gespeichert, u. a.:

- `trades_users`
- Poll-Intervalle (`poll_interval_*_seconds`)
- `shared_rest_user_pause_seconds`
- Exchange-Overrides (`shared_rest_pause_by_exchange_json`)
- `ws_max`
- `log_level`

## Troubleshooting

### Viele 429 / Rate-Limit Meldungen

- **REST pause/user** erhöhen
- Poll-Intervalle erhöhen
- Anzahl aktiver **Users** reduzieren
- Per-Exchange Pausen für empfindliche Exchanges setzen

### Executions werden nicht geladen

- Prüfen, ob der User in **Executions download** ausgewählt ist
- PBData-Logs auf „skipped/filtered executions" prüfen

### UI wirkt „stale"

- Prüfen, ob PBData läuft (Start/Stop-Buttons im Control-Strip)
- Im **Status**-Tab die Fetch Summary auf aktuelle Zeitstempel prüfen
- Bei Überlast: Combined-Intervall erhöhen
