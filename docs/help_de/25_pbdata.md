# PBData (Service)

PBData ist ein Hintergrund‑Service in PBGui. Es lädt kontinuierlich Account‑Daten (über WS + REST) und schreibt sie in die PBGui‑Datenbank, damit andere Seiten diese Daten schnell verwenden können.

## Was PBData lädt

Pro ausgewähltem User (siehe **System → Services → PBData Details**):

- **WebSocket (private)**
  - Balances
  - Positions
  - Orders
- **Shared REST Poller** (serialisiert „round‑robin“)
  - Combined Poller: Balances/Positions/Orders (Fallback + periodischer Refresh)
  - History Poller
  - Executions Poller (My Trades) — *Opt‑in*

## Users vs. Executions download (Opt‑in)

PBData hat zwei getrennte User‑Listen:

- **Users**
  - User, die PBData aktiv aktualisiert (WS + REST)
- **Executions download**
  - **Opt‑in Allow‑List**: Nur diese User laden/speichern Executions (My Trades)
  - Default ist **keine Auswahl**
  - Änderungen wirken schnell; PBData prüft vor jedem Executions‑Fetch erneut

## Timer und Performance

Unter **PBData timers** kannst du steuern, wie aggressiv PBData pollt.

- **Startup delay (s)**
  - Wartezeit nach Start, bevor die Shared REST Poller beginnen
- **Combined interval (s)**
  - Intervall für den Shared Combined REST Poll (Balances/Positions/Orders)
- **History interval (s)**
  - Intervall für Shared History Updates
- **Executions interval (s)**
  - Intervall für Shared Executions (My Trades)

Allgemeine Hinweise:

- Zu kleine Intervalle führen oft zu **Rate Limits (HTTP 429)**.
- Wenn du häufig Backoffs siehst: Intervalle erhöhen oder Anzahl aktiver User reduzieren.

## Rate‑Limit Kontrolle (REST Pause)

PBData nutzt eine kleine Pause zwischen Usern in den Shared REST Pollern.

- **REST pause/user (s)**
  - Globale Pause zwischen Usern beim Shared REST Polling

### Shared REST pause per exchange

Manche Exchanges brauchen eine größere Pause.

- Du kannst eine Pause pro Exchange setzen.
- Werte, die dem globalen Wert entsprechen, werden nicht als Override gespeichert.
- Wenn kein Override gesetzt ist, verwendet PBData die eingebauten Exchange‑Defaults (z. B. Hyperliquid/Bybit).

## Private WS Global Limit (ws_max)

- **Max private WS global** begrenzt, wie viele private Websocket‑Clients PBData offen halten darf.
- Das hilft bei vielen Usern/Exchanges, WS‑Überlast zu vermeiden.

## Log‑Viewer Tipps

Die PBData‑Detailseite nutzt den gefilterten Log‑Viewer für PBData‑Logs. Er unterstützt:

- Auswahl von einem oder mehreren **Logfiles** (zusammengeführt nach Timestamp)
- Filter:
  - **Users**
  - **Tags** (aus `[tag]` Tokens)
  - **Levels (filter)**
  - **Free-text**
- **RAW** zeigt unformatierte Zeilen
- Buttons:
  - ✖ Filter zurücksetzen
  - 🔄 Refresh
  - 🗑️ Ausgewählte Logdatei(en) leeren/rotieren

Zusätzlich gibt es **PBData Log level** im Log‑Header (rechts neben Logfiles). Das steuert, wie ausführlich PBData selbst loggt.

## Fetch Summary Bereich

In den PBData Details gibt es zusätzlich einen **Fetch Summary** Bereich (aus `data/logs/fetch_summary.json`).

Er zeigt eine kompakte Laufzeitübersicht für:

- Ergebnisse von Balances / Positions / Orders
- Ergebnisse von History / Executions
- Letzte Fetch‑Zeitpunkte und Status pro User

Wenn noch keine Summary sichtbar ist, hat PBData meist noch keinen ersten Summary‑Zyklus geschrieben.

## Wo Settings gespeichert werden

Die meisten PBData‑Settings werden in `pbgui.ini` unter `[pbdata]` gespeichert, u. a.:

- `trades_users`
- Poll‑Intervalle (`poll_interval_*_seconds`)
- `shared_rest_user_pause_seconds`
- Exchange‑Overrides (`shared_rest_pause_by_exchange_json`)
- `ws_max`
- `log_level`

## Troubleshooting

### Viele 429 / Rate‑Limit Meldungen

- **REST pause/user** erhöhen
- Poll‑Intervalle erhöhen
- Anzahl aktiver **Users** reduzieren
- Per‑Exchange Pausen für empfindliche Exchanges setzen

### Executions werden nicht geladen

- Prüfen, ob der User in **Executions download** ausgewählt ist
- PBData‑Logs auf „skipped/filtered executions“ prüfen

### UI wirkt „stale“

- 🔄 im Log‑Viewer klicken
- 🔄 im Fetch Summary Bereich klicken
- Prüfen, ob PBData läuft (PBData Toggle)
- Bei Überlast: Combined‑Intervall erhöhen
