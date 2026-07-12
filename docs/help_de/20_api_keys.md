# API-Keys

Exchange-API-Credentials und TradFi-Provider-Einstellungen verwalten. Alle Credentials werden in `api-keys.json` gespeichert und von PB7 für den Live-Betrieb gelesen.

---

## Seitenaufbau

Die Seite läuft als eigenständige FastAPI-Seite mit vollständiger Topnav zur Navigation zwischen allen PBGui-Bereichen. Sie besteht aus einer **Sidebar** (links) und einem **Hauptbereich** (rechts).

### Sidebar-Buttons

| Button | Funktion |
|---|---|
| **+ Add User** | Öffnet das Formular zum Anlegen eines neuen Exchange-Users |
| **HL Expiry Check** | Prüft den Key-Ablauf aller Hyperliquid-User (Bulk) |
| **Bybit Expiry Check** | Prüft den Key-Ablauf + IP-Whitelist aller Bybit-User (Bulk) |
| **Comments** | Öffnet das Kommentar-Panel |
| **HL Warning Config** | Konfiguriert den Schwellenwert für Hyperliquid-Ablaufwarnungen via Telegram |
| **TradFi** | Öffnet das TradFi-Data-Provider-Panel |
| **🗄 Backups** | Öffnet den Backup-Browser mit Diff-Viewer |
| **📋 Logs** | Öffnet den Live-Log-Viewer (streamt `ApiKeys.log` und weitere Logs) |
| **Refresh** | Lädt die User-Liste neu von der Festplatte |
| **🟠 Restart** | Sichtbar, wenn der API-Server ausstehende Code-Änderungen hat; Klick startet neu |

---

## User-Liste

Zeigt alle Einträge aus `api-keys.json`.

- **Filterfeld** — nach Name oder Exchange suchen; Zustand wird in der URL gespeichert (`?filter=`)
- **Spaltenüberschriften** — Klick zum Sortieren; Richtung bleibt in der URL erhalten (`?sort=`, `?dir=`)
- **Tastaturnavigation** — ArrowDown aus dem Filterfeld wählt die erste Zeile; ArrowUp/ArrowDown navigiert zwischen Zeilen; Enter öffnet den gewählten User
- **In Use-Badge** — wird angezeigt, wenn der User einem laufenden Bot zugeordnet ist

Benutzernamen werden strikt als Text gerendert und Zeilenaktionen verwenden delegierte Browser-Events. Aus Backups oder Cluster Sync importierte Namen koennen daher nicht als Seiten-Markup oder JavaScript interpretiert werden; Zeilenklick, Tastaturnavigation, Edit und Delete funktionieren unveraendert.

### Ablauf-Spalten

- **HL Expiry** — zeigt verbleibende Tage / Ablaufdatum für Hyperliquid-User (aus lokalem Cache, kein API-Call); sortierbar aufsteigend (nächster Ablauf zuerst)
- **Bybit Expiry** — zeigt verbleibende Tage für Bybit-User (aus lokalem Cache)

---

## User anlegen / bearbeiten

Klick auf eine User-Zeile öffnet das Formular, oder **+ Add User** verwenden. Der URL-Hash wechselt auf `#edit/username`, sodass ein Browser-Refresh denselben User wiederherstellt.

**Escape** schließt ohne Speichern (mit Rückfrage bei ungespeicherten Änderungen).

### Felder im Bearbeitungsformular

| Feld | Beschreibung |
|---|---|
| **Username** | Schlüssel in `api-keys.json`; kann umbenannt werden — neuen Namen eingeben und speichern |
| **Exchange** | Exchange-Name (z. B. `bybit`, `binanceusdm`, `hyperliquid`) |
| **API Key** | Exchange-API-Key |
| **Secret** | API-Secret |
| **Passphrase** | Von manchen Exchanges erforderlich (z. B. OKX) |
| **Wallet Address** | Nur Hyperliquid |
| **Private Key** | Nur Hyperliquid |
| **Is Vault** | Hyperliquid-Vault-Modus |
| **Quote** | Optionaler CCXT-Passthrough (z. B. `USDT`) |
| **Options** | Optionales JSON-Objekt (z. B. `{"defaultType": "swap"}`) |
| **Extra** | Optionaler JSON-Passthrough für Exchange-spezifische Felder |

### Auge-Symbol (Credentials enthüllen)

Alle Credential-Felder (Secret, Passphrase, Private Key, TradFi-Keys) haben einen 👁-Button:

- **Klick** — ruft den echten gespeicherten Wert vom Server ab und zeigt ihn im Klartext
- **Erneuter Klick** — verbirgt und leert das Feld (Speichern mit leerem Feld lässt den gespeicherten Wert unverändert)
- Credential ersetzen: enthüllen, leeren, neuen Wert eingeben, speichern

### Validierung

- Standard-Exchanges benötigen **API Key + Secret**
- Passphrase-Exchanges zusätzlich **Passphrase**
- Hyperliquid benötigt **Wallet Address**; Private Key nur bei der Erstellung Pflicht (beim Bearbeiten leer lassen, um den bestehenden Wert zu behalten)
- Username muss eindeutig sein; Umbenennung wird abgelehnt, wenn der neue Name bereits vergeben ist oder der User von einem Bot verwendet wird

### Expiry prüfen / Verbindung testen

Beide Buttons verwenden die **aktuell eingegebenen Credentials** aus dem Formular — nicht nur die gespeicherten. So kann ein neuer Key vor dem Speichern geprüft werden.

- **Check Expiry** (HL / Bybit) — Ergebnis ist eine Vorschau; erst nach Save persistent
- **Test Connection** — testet die Verbindung live; verwendet ebenfalls ungespeicherte Credentials

Ungespeicherte Hyperliquid Private Keys fuer **Check Expiry** werden ausschliesslich im Body eines authentifizierten POST-Requests uebertragen. Sie werden nie an die Request-URL angehaengt; Pruefungen ohne ungespeicherten Override verwenden weiterhin den gespeicherten Key.

---

## Backups

Vor jedem Speichern wird automatisch ein Backup erstellt. Backups liegen in `data/api-keys/` als zeitgestempelte JSON-Dateien.

Öffnen über **🗄 Backups** in der Sidebar (URL-Hash: `#backups`).

| Eintrag | Beschreibung |
|---|---|
| **Current (live)** | Die aktive `api-keys.json` für jede installierte PB-Version (pb7/pb6); für Diff-Vergleiche auswählbar |
| Zeitgestempelte Einträge | Frühere Speicherstände; **Restore** überschreibt die aktuelle Datei (Pre-Restore-Snapshot wird vorher erstellt) |

### Diff-Viewer

Beliebige zwei Einträge nebeneinander oder unified vergleichen:
- Grün = hinzugefügt, rot = entfernt, grau = unveränderter Kontext
- „✓ Files are identical" wird angezeigt, wenn beide Versionen identisch sind

---

## Cluster Sync

Die API-Keys-Seite bearbeitet nur das lokale `api-keys.json`. Remote-Schreibvorgänge für API-Keys gehören zu **Cluster Sync**.

Beim Speichern von Credentials legt PBGui die aktualisierten API-Key-Metadaten und den Secret-Blob im Cluster-State ab. Verwende **System -> Cluster Sync**, um `api-keys.json` auf einem erreichbaren Node zu prüfen und explizit zu materialisieren.

Die Cluster-Materialisierung erstellt Ersatz-Backups nur auf Master-Nodes, wenn sich die Zieldatei unterscheidet. Diese Backups liegen bei den normalen API-Key-Backups in `data/api-keys/`. VPS-Runner ueberspringen lokale Backups, schreiben den verifizierten Secret-Blob atomar und starten keine Bots neu.

---

## HL Warning Config

Öffnen über **HL Warning Config** in der Sidebar.

- Wenn `hl_expiry.telegram_warning_days` bereits in `pbgui.ini` vorhanden ist, zeigt das Panel den Wert als **configured** an.
- Wenn der INI-Eintrag noch fehlt, zeigt das Panel jetzt **Not configured** und weist explizit darauf hin, dass PBAPIServer aktuell mit dem Default von **7 Tagen** arbeitet.
- Ein Klick auf **Save** schreibt den gewählten Schwellenwert in `pbgui.ini` und der Panel-Status wechselt auf configured.

---

## Live-Log-Viewer

Öffnen über **📋 Logs** in der Sidebar.

Streamt Logdateien in Echtzeit via WebSocket.

### Steuerelemente

| Steuerelement | Beschreibung |
|---|---|
| **Files**-Button / Sidebar | Schaltet die einklappbare linke Sidebar mit allen verfügbaren Logdateien um; Klick auf eine Datei wechselt die Ansicht |
| **DBG / INF / WRN / ERR / CRT** | Sichtbarkeit nach Log-Level steuern |
| **Lines** | Anzahl initial geladener Zeilen (200 – 5000) |
| **⏸ Pause / ▶ Stream** | Live-Streaming pausieren oder fortsetzen |
| **🗑 Clear** | Löscht die Terminal-Anzeige |
| **↓ Download** | Lädt die aktuell geladenen Zeilen als Textdatei herunter |
| **# Lines** | Zeilennummern ein-/ausblenden |
| **— Preset —** | Vorgefertigte Suchmuster (Errors, Warnings, Connection, Traceback, …) |
| **Suchfeld** | Live-Suche / Filter; Checkbox **Filter** blendet nicht passende Zeilen aus; ▲▼ navigiert zwischen Treffern |

Wichtige Logdateien:
- `ApiKeys.log` — Aktivität des API-Key-Editors
- `VPSMonitor.log` — VPS-Monitoring
- `PBGui.log` — allgemeine UI-Aktivität

---

## Kommentare

Öffnen über **Comments** in der Sidebar (URL-Hash: `#comments`).

Verwaltet `_comment_*`-Einträge auf oberster Ebene in `api-keys.json` — freie Notizen ohne Zuordnung zu einem Exchange-User.

---

## TradFi Data Provider (Stock-Perps Backtesting)

Öffnen über **TradFi** in der Sidebar (URL-Hash: `#tradfi`).

Für Hyperliquid-XYZ-Symbol-Backtests werden 1-Minuten-OHLCV-Daten traditioneller Assets (Aktien, FX) benötigt.

> 💡 **Empfohlen für vollständige Stock-Perp-Historie:** PBGuis **Market Data**-Modul mit **Tiingo** aufbauen — deutlich vollständiger als die PB7-seitigen Provider weiter unten. Tiingo konfigurieren und **Build best 1m OHLCV** starten unter _Setup → Market Data_.

### yfinance (automatischer Standard)

- Kein Einrichten nötig; automatischer Fallback für die letzten ~7 Tage
- Kostenlos, kein API-Key erforderlich
- **Install** / **Uninstall** verwalten das Python-Paket

### Extended Provider (optional, für ältere Daten)

| Anbieter | Key nötig | Free-Tier 1m-Tiefe | Hinweise |
|---|---|---|---|
| **alpaca** | key + secret | 5+ Jahre | Kostenlos (IEX-Feed, 15 Min. Verzögerung — für Backtests irrelevant). **Empfohlen.** |
| **polygon** | nur key | 2 Jahre | Bezahlpläne bieten längere Historie |
| **finnhub** | nur key | Nicht nutzbar | Free-Tier hat kein 1-Minuten-Intraday |
| **alphavantage** | nur key | Sehr limitiert | 25 API-Calls/Tag im Free-Tier |

Bei der Auswahl eines Providers wird ein Link zur Registrierungsseite angezeigt.

**Test Connection** ruft einen Test-Quote/-Kerzen für `AAPL` ab und zeigt das Ergebnis in einem Modal. Funktioniert auch mit bereits gespeicherten Credentials, wenn die Felder leer sind.

---

## `api-keys.json` Feldreferenz

```json
{
  "myuser": {
    "exchange": "bybit",
    "key": "...",
    "secret": "...",
    "passphrase": "...",
    "quote": "USDT",
    "options": {"defaultType": "swap"},
    "extra": {}
  },
  "myhl": {
    "exchange": "hyperliquid",
    "wallet_address": "0x...",
    "private_key": "0x...",
    "is_vault": false
  }
}
```

---

## Upstream-Referenz

- https://github.com/enarjord/passivbot
