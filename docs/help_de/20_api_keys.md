# API-Keys

Wenn ein Konto gleichzeitig laufende und deaktivierte PB7/PB8-Konfigurationen hat, zeigt die Statusspalte nur die Laufhosts.

Die Statusspalte zeigt die gemeldeten Laufhosts der PB7/PB8-Bots. Der Tooltip nennt die Bot-Version. Ohne laufenden Bot erscheint Disabled, Stopped, Blocked, Collecting oder Unknown; Unused bedeutet, dass keine lokale Bot-Konfiguration das Konto verwendet. Die sichtbare Liste aktualisiert sich automatisch. Auch deaktivierte Bots schützen ihre Zugangsdaten vor dem Löschen.

Exchange-API-Credentials und TradFi-Provider-Profile verwalten. Exchange-User bleiben in `api-keys.json`; TradFi-Secrets werden getrennt im owner-only Credential Vault von PBGui gespeichert.


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
| **📋 Logs** | Öffnet den Live-Log-Viewer mit `PBGui.log`, das API-Key-Aktivitaet und weitere UI-Logs enthaelt |
| **🟠 Restart** | Sichtbar, wenn die API oder ein anderer verwalteter PBGui-Dienst veralteten Code ausführt; Klick zeigt und startet die betroffenen Dienste neu |

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

### Gemeinsamen Hyperliquid-Private-Key ändern

Beim Bearbeiten eines bestehenden Hyperliquid-Kontos den neuen **Private Key** eintragen. Nutzen weitere Hyperliquid-Einträge denselben gespeicherten Key, erscheint darunter **Update all accounts using this private key (N)**. Die Option ist standardmäßig ausgeschaltet; ohne Aktivierung wird nur das ausgewählte Konto geändert.

Nach Aktivierung erscheint die vollständige Kontenliste mit Name, Wallet-/Vault-Adresse und Kennzeichnung als Hauptkonto oder Vault. Hauptkonten mit demselben Key gehören ebenfalls zur Gruppe; leere Keys und Konten anderer Börsen nicht. Gleichwertige hexadezimale Keys mit oder ohne `0x` werden als derselbe Key erkannt.

**Save** anklicken, die Liste im Bestätigungsdialog prüfen und **Update all N accounts** wählen. Der neue Private Key wird in einem atomaren Speichervorgang für die gesamte Gruppe übernommen. Andere Konten behalten ihre Adressen und individuellen Einstellungen; normale Änderungen am ausgewählten Konto werden in derselben Transaktion gespeichert. Die Vorschau legt keine bisherigen Private Keys offen.

Die Vorschau aktualisiert sich automatisch, solange der Editor sichtbar ist, und erhält ungespeicherte Eingaben. Ändern sich Gruppenzugehörigkeit oder angezeigte Kontodetails, wird die Option zur erneuten Prüfung abgewählt. Zusätzlich prüft der Server beim Speichern die bestätigten Datensätze: Eine veränderte oder abgelaufene Vorschau wird abgewiesen, ohne die angeforderten Änderungen zu speichern. Danach die aktualisierte Liste prüfen und erneut bestätigen. Beim Abbrechen bleiben die Eingaben erhalten.

Gespeicherte Ablaufdaten werden für alle betroffenen Konten gelöscht. Anschließend übernimmt der vorhandene Ablauf aus Cluster Sync und PBRun die Zustellung und Key-Übernahme der betroffenen laufenden Bots. Zustellung und Neustarts erfolgen je VPS nach verifizierter Key-Ankunft, nicht gleichzeitig auf allen Hosts; gestoppte Bots bleiben gestoppt. Bei einem Verteilungsfehler kann die vollständige Gruppe bereits lokal gespeichert sein, während die Veröffentlichung noch aussteht und von PBCluster erneut versucht wird. In Cluster Sync prüfen, ob alle VPS den neuen Key übernommen haben.


### Felder im Bearbeitungsformular

| Feld | Beschreibung |
|---|---|
| **Username** | Schlüssel in `api-keys.json`; kann umbenannt werden — neuen Namen eingeben und speichern |
| **Exchange** | Exchange-Name (z. B. `bybit`, `hyperliquid`, `bitunix` oder `weex`) |
| **API Key** | Exchange-API-Key |
| **Secret** | API-Secret |
| **Passphrase** | Von manchen Exchanges erforderlich, unter anderem OKX und WEEX |
| **Wallet Address** | Nur Hyperliquid |
| **Private Key** | Nur Hyperliquid |
| **Is Vault** | Hyperliquid-Vault-Modus |
| **Quote** | Optionaler CCXT-Passthrough (z. B. `USDT`) |
| **Options** | Optionales JSON-Objekt (z. B. `{"defaultType": "swap"}`) |
| **Extra** | Optionaler JSON-Passthrough für Exchange-spezifische Felder |

### Bitget Classic und UTA

Verwende fuer beide Modi Exchange `bitget` mit **API Key**, **Secret** und **Passphrase**. PBGui verifiziert den Account-Modus, statt bei fehlgeschlagener Erkennung Classic anzunehmen; ein unbekannter Modus blockiert modusabhaengige Operationen. Die UTA-Unterstuetzung umfasst native USDT-Perpetual-Account-Abfragen und Historie in PBGui, nicht Multi-Asset-Equity. Sie aendert keinen Bot-Code, zertifiziert keinen PB7-Live-Handel auf UTA und garantiert keine sichere Classic-zu-UTA-Migration. Die vollstaendige Live-Abnahme steht noch aus; ein erfolgreicher Verbindungstest ist keine Bot- oder Migrationszertifizierung.

UTA-Lesezugriffe benoetigen sowohl **Unified account management, read-only** (Einstellungen, Assets und Financial Records) als auch **Unified account trade, read-only** (Positionen, Orders und Fills). Wenn Trade-Abfragen funktionieren, Einstellungen/Balance/Historie aber Berechtigungsfehler `40014` liefern, pruefe Management-Leserechte am ausgewaehlten Bitget-API-Key. Schreib-, Auszahlungs- und Copy-Trading-Rechte sind fuer diese Abfragen nicht erforderlich; Bot-Setup und Trading haben eigene Anforderungen.

### Gespeicherte Credentials

Credential-Details liefern nur feste Masken und Presence-Informationen. Secret, Passphrase und Private Key sind replacement-only: leer lassen, um den gespeicherten Wert zu behalten, oder einen neuen Wert eingeben und speichern. PBGui enthuellt diese Werte niemals.

Nur der API Key besitzt ein Auge fuer gespeicherte Exchange-Credentials. Es sendet fuer den ausgewaehlten User einen authentifizierten Same-Origin-POST, verwendet `Cache-Control: no-store` und leert den Wert beim Verbergen, User-Wechsel, Authentifizierungsverlust und Verlassen der Seite.

TradFi-Vault-Secrets funktionieren anders: Gespeicherte Werte werden nie an den Browser zurückgegeben. Das Auge kann dort nur Text anzeigen, der während der aktuellen Bearbeitung eingegeben wurde. Ein leeres Feld behält den gespeicherten Wert; ein neuer Wert ersetzt ihn beim Speichern.

### Validierung

- Standard-Exchanges benötigen **API Key + Secret**
- Passphrase-Exchanges zusätzlich **Passphrase**
- Bitunix benoetigt **API Key + Secret**; WEEX zusaetzlich eine **Passphrase**
- Hyperliquid benötigt **Wallet Address**; Private Key nur bei der Erstellung Pflicht (beim Bearbeiten leer lassen, um den bestehenden Wert zu behalten)
- Username muss eindeutig sein; Umbenennung wird abgelehnt, wenn der neue Name bereits vergeben ist oder der User von einem Bot verwendet wird

### Expiry prüfen / Verbindung testen

Beide Buttons verwenden die **aktuell eingegebenen Credentials** aus dem Formular — nicht nur die gespeicherten. So kann ein neuer Key vor dem Speichern geprüft werden.

- **Check Expiry** (HL / Bybit) — Ergebnis ist eine Vorschau; erst nach Save persistent
- **Test Connection** — testet die Verbindung live; verwendet ebenfalls ungespeicherte Credentials

Beim Öffnen eines gespeicherten Hyperliquid-Users wird sein **Account Request Limit** einmalig abgefragt. Das Limit erscheint als Karte neben Futures Balance mit Used, Cap, verbleibenden Requests und Messzeit. Die Request-Zähler verwenden Apostrophe als Tausendertrennzeichen (zum Beispiel `2'209'548`). Sobald ein gespeichertes Hauptkonto sein Request-Limit ausgeschöpft hat, erscheinen in der Limit-Karte die Eingabe der Credit-Anzahl, die Kosten in Perps-USDC und eine einmalige Kaufbestätigung. Bei Vault-Einträgen wird stattdessen die Beschränkung auf Hauptkonten angezeigt. Die Abfrage verwendet die gespeicherte öffentliche Wallet-Adresse, auch wenn im Formular ungespeicherte Änderungen stehen. Ein laufender Bot ist nicht nötig und es wird keine periodische History gestartet. Die VPS-übergreifende Übersicht steht unter **Information → Hyperliquid Limits**.

Ungespeicherte Hyperliquid Private Keys fuer **Check Expiry** werden ausschliesslich im Body eines authentifizierten POST-Requests uebertragen. Sie werden nie an die Request-URL angehaengt; Pruefungen ohne ungespeicherten Override verwenden weiterhin den gespeicherten Key.

Wird der Account waehrend eines Verbindungs- oder Inline-Ablaufchecks bearbeitet, wird das veraltete Ergebnis verworfen, der Aktionsbutton aber immer wieder freigegeben. Ein abgeschlossener aelterer Request kann keinen neueren Check desselben Buttons entsperren.

---

## Backups

Vor jedem Speichern wird automatisch ein Backup erstellt. Backups liegen in `data/api-keys/` als zeitgestempelte JSON-Dateien.

Öffnen über **🗄 Backups** in der Sidebar (URL-Hash: `#backups`).

| Eintrag | Beschreibung |
|---|---|
| **Current (live)** | Die aktive PB7-`api-keys.json`, sofern vorhanden; für Diff-Vergleiche auswählbar (nur PB7) |
| Zeitgestempelte Einträge | Frühere Speicherstände; **Restore** überschreibt die aktuelle Datei (Pre-Restore-Snapshot wird vorher erstellt) |

### Diff-Viewer

Beliebige zwei Einträge nebeneinander oder unified vergleichen:
- Grün = hinzugefügt, rot = entfernt, grau = unveränderter Kontext
- „✓ Files are identical" wird angezeigt, wenn beide Versionen identisch sind

---

## Cluster Sync

Exchange-User werden in das lokale `api-keys.json` projiziert. Remote-Schreibvorgänge für Exchange-API-Keys gehören zu **Cluster Sync**.

Beim Speichern von Exchange-Credentials legt PBGui die aktualisierten API-Key-Metadaten und den eingeschränkten Secret-Blob im Cluster-State ab. Verwende **System -> Cluster Sync**, um `api-keys.json` auf einem erreichbaren Node zu prüfen und explizit zu materialisieren.

Die Cluster-Materialisierung erstellt Ersatz-Backups nur auf Master-Nodes, wenn sich die Zieldatei unterscheidet. Diese Backups liegen bei den normalen API-Key-Backups in `data/api-keys/`. VPS-Runner überspringen lokale Backups und schreiben den verifizierten Secret-Blob atomar. Nach der Prüfung startet der lokale PBRun gezielt nur laufende PB7/PB8-Instanzen mit geänderten Account-Zugangsdaten geordnet neu. Unveränderte Accounts und deaktivierte Bots bleiben unberührt; weitere Dateien werden nicht verteilt.

TradFi-Profile verwenden stattdessen Sealed Envelopes aus Credential Protocol v2. Sie sind nur an aktive Master adressiert; VPS-Nodes können den Ciphertext weiterleiten, aber TradFi-Credentials weder entschlüsseln noch projizieren.

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

Sehr lange Zeilen zeigen eine Vorschau mit 4.000 Zeichen, damit Suche, Aufklappen und Scrollen bedienbar bleiben. Gekuerzte Zeilen sind gekennzeichnet; die Suche verwendet weiterhin den vollstaendigen geladenen Text, und **Download** enthaelt ihn ohne diese Anzeigebegrenzung.

Der geladene Dateiausschnitt ist von alt nach neu sortiert. Der Viewer oeffnet mit `[ApiKeys]` im Suchfeld und aktivierter Checkbox **Filter**, sodass nur Aktivitaeten des API-Key-Editors angezeigt werden; `api` findet auch andere UI-Services und Nachrichtentexte. Filtertreffer werden mit Kontext gruppiert und anfangs eingeklappt: Ein Block zeigt seinen ersten Treffer, nicht den neuesten Eintrag. Klappe den letzten Block auf oder nutze **Expand all**, um darin enthaltene neue Aenderungen zu sehen. Ein konkreter Username grenzt die Suche weiter ein.

Bei lokalen Logs durchsuchen aktive Filter den gewaehlten Datei-Tail auf dem Server; zum Browser gelangen nur Treffer und der angeforderte Kontext. Die bisherigen Ergebnisse bleiben waehrend der entprellten Suche sichtbar, und veraltete Antworten koennen keine neuere Suche ueberschreiben. Eingeklappte Ergebnisse behalten nur einen Kopf pro Block im DOM; beim Aufklappen werden Kontext- und Trefferzeilen schrittweise in korrekter Reihenfolge erzeugt. **Download** laedt weiterhin den vollstaendigen gewaehlten Quell-Tail, und das Leeren des Filters stellt die normale ungefilterte Ansicht wieder her. Legacy- und Remote-Streams behalten das begrenzte Browsermodell; Live-Updates durchsuchen dieses Modell nicht mehr bei jeder neuen Zeile komplett.

Die lokale Log-Verbindung dieser Seite abonniert keinen VPS-Monitoring-State. Host-, Service-, Instanz- und Task-Snapshots koennen deshalb den Lines-Selektor oder die Log-Steuerung nicht mehr unterbrechen; die explizite lokale Dateiliste und der gefilterte Log-Stream laufen unabhaengig weiter.

Beim Zurueckkehren mit **Back** wird die lokale Log-Verbindung geschlossen und das geladene Log-Modell samt gerenderten Zeilen freigegeben. Ein erneutes Oeffnen von **Logs** verbindet sich mit der gewaehlten Lines-Einstellung und einem frischen Snapshot.

### Steuerelemente

| Steuerelement | Beschreibung |
|---|---|
| **Files**-Button / Sidebar | Schaltet die einklappbare linke Sidebar mit allen verfügbaren Logdateien um; Klick auf eine Datei wechselt die Ansicht |
| **DBG / INF / WRN / ERR / CRT** | Sichtbarkeit nach Log-Level steuern |
| **Lines** | Anzahl initial geladener Zeilen (200 bis 50.000) |
| **⏸ Pause / ▶ Stream** | Live-Streaming pausieren oder fortsetzen |
| **🗑 Clear** | Löscht die Terminal-Anzeige |
| **↓ Download** | Laedt den vollstaendigen gewaehlten Quell-Tail als Textdatei, einschliesslich in der Filteransicht ausgelassener Zeilen |
| **# Lines** | Zeilennummern ein-/ausblenden |
| **— Preset —** | Vorgefertigte Suchmuster (Errors, Warnings, Connection, Traceback, …) |
| **Suchfeld** | Live-Suche / Filter; Checkbox **Filter** blendet nicht passende Zeilen aus; ▲▼ navigiert zwischen Treffern |

Wichtige Logdateien:
- `PBGui.log` — mit `[ApiKeys]` markierte Eintraege des API-Key-Editors zusammen mit allgemeiner UI-Aktivitaet
- `VPSMonitor.log` — VPS-Monitoring

---

## Kommentare

Öffnen über **Comments** in der Sidebar (URL-Hash: `#comments`).

Verwaltet `_comment_*`-Einträge auf oberster Ebene in `api-keys.json` — freie Notizen ohne Zuordnung zu einem Exchange-User.

---

## TradFi Data Provider (Stock-Perps Backtesting)

Öffnen über **TradFi** in der Sidebar (URL-Hash: `#tradfi`).

Für Hyperliquid-XYZ-Symbol-Backtests werden 1-Minuten-OHLCV-Daten traditioneller Assets (Aktien, FX) benötigt.

> 💡 **Empfohlen für vollständige Stock-Perp-Historie:** Hier ein **Tiingo**-Profil anlegen und danach im **Market Data**-Modul mit **Build best 1m OHLCV** ein vollständiges lokales 1-Minuten-OHLCV-Archiv aufbauen.

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

Gespeicherte Profile zeigen nur Metadaten wie Provider, Aktivstatus und Generation. **Test Connection** verwendet bei leeren Feldern das gespeicherte Profil serverseitig oder vor dem Speichern einmalige Credentials aus dem authentifizierten Request-Body. Ein Klick auf das Auge neben **API key** zeigt ausschließlich den gespeicherten Drittanbieter-Key des ausgewählten Profils; beim Verbergen, Profilwechsel, Authentifizierungsverlust oder Verlassen der Seite wird er wieder gelöscht. Ein Tiingo-Token kann außerdem direkt unter **Market Data -> Settings -> TradFi / Tiingo** über denselben Credential Vault angezeigt, erstellt oder ersetzt werden.

PBGui projiziert aktive TradFi-Profile auf Mastern automatisch in den reservierten PB7-Teil von `api-keys.json`, inklusive atomarem Merge und Retry. PB7-TradFi-Einträge nicht manuell bearbeiten. Das Ersetzen eines Provider-Keys erzeugt eine neue Vault-Generation; Provider-Rotation ist optional und keine Voraussetzung für die Credential-Migration.

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


### Hyperliquid-Kontomodus

PBGui erkennt Standard / Manual, Unified Account und Portfolio Margin. Unified und Portfolio Margin verwenden gemeinsame Sicherheiten; getrennte Spot/Perps-Transfers und Profit Sweep werden nicht unterstützt. Für Profit Sweep in Hyperliquid den Account Type auf **Manual (Standard)** umstellen und PBGui aktualisieren. Der Kontomodus muss direkt in Hyperliquid geändert werden. Vault-Transfers werden separat geprüft; ein Unified-Leader sperrt sie nicht automatisch.

Mit **Test Connection** den erkannten Kontomodus aktualisieren. Unified-Konten zeigen das gemeinsame USDC-Guthaben statt eines irreführenden Futures-Guthabens von null.

#### In Hyperliquid auf Standard / Manual umstellen

Im Kontomodus-Hinweis **Open Hyperliquid** wählen. Auf [Hyperliquid](https://app.hyperliquid.xyz/portfolio) das betreffende Konto auswählen, **Account Type** öffnen und **Manual (Standard)** wählen. Anschließend in PBGui mit **Test Connection** den erkannten Modus und das Guthaben aktualisieren.

API Keys verbindet keine Browser-Wallets, fragt keine Signaturen an und übermittelt keine Kontomodus-Änderungen. Profit Sweep muss nach der Umstellung separat konfiguriert werden.

Hyperliquid-Konten im Modus Standard / Manual zeigen Futures- und Spot-USDC-Guthaben. Nach **Test Connection** unter **Transfer Spot ↔ Perps** Richtung und Betrag auswählen. **Max** übernimmt das verfügbare übertragbare Guthaben. Jeden Transfer prüfen und bestätigen; Änderungen an Zugangsdaten vorher speichern. Bestätigte Transfers aktualisieren die Guthaben. Unklare Ergebnisse über **Transfer history** klären, ohne erneut zu überweisen. Manuelle Transfers ändern die Profit-Sweep-Buchhaltung nicht.

Die Kontoliste aktualisiert beim Zurückwechseln in ihren Browser-Tab automatisch den Status **In Use** und den Löschbutton, wenn eine Run-Konfiguration gelöscht wurde. Instanzordner sowohl von PB7 als auch PB8 schützen die Zugangsdaten vor dem Löschen.

Transfer-Guthaben aktualisieren sich automatisch, auch solange die Börsenbestätigung aussteht. PBGui prüft die Historie bereits übermittelter manueller Transfers regelmäßig nach und sendet sie niemals erneut. Sobald der bestehende Vorgang bestätigt oder fehlgeschlagen ist, werden die Transferfelder wieder freigegeben.

## Übernahme nach einem Key-Wechsel

Wird ein Key während einer laufenden Synchronisation geändert und gespeichert, folgt direkt nach deren Abschluss automatisch ein weiterer Durchlauf, ohne auf das regelmäßige Sync-Intervall zu warten. Die Zustellung hängt weiterhin von der Erreichbarkeit des Hosts und der Dauer der Synchronisation ab.

PBCluster verteilt Keys automatisch. Die verifizierte Ankunft der Datei und ein laufender Bot mit der neuen Version sind getrennte Zustände. Unter **System → Cluster Sync** zeigt **Preview** beim jeweiligen Node den automatisch aktualisierten Bereich **Credential adoption**: Key angekommen, Neustart ausstehend/läuft, Warten auf Bot-Start, übernommen, Fehler oder veraltete PBRun-Beobachtung. Ältere Nodes zeigen den Status erst nach Aktualisierung ihres PBGui-Codes und PBRun an.

PBRun verfolgt jede Instanz und jeden konkreten Prozess einzeln, auch wenn mehrere Instanzen denselben Account verwenden. Wiederholte Synchronisation und reine Metadatenänderungen starten unveränderte Bots nicht neu. Ein manueller Neustart nach der verifizierten Key-Ankunft wird erkannt. Ausstehende Änderungen überleben PBRun-Neustarts und unterbrochene Dateischreibvorgänge. Fehlgeschlagene Stops starten keinen zweiten Prozess; fehlgeschlagene Starts bleiben mit Wartezeit erneut ausführbar. Zuerst wird SIGINT gesendet, erst nach begrenzten Wartezeiten folgen TERM und KILL. Die Übernahme bestätigt einen laufenden Ersatzprozess; sie ist keine börsenseitige Prüfung der Zugangsdaten. Geheimnisse und Credential-Fingerprints erscheinen nicht im Status.
