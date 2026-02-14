# Market Data

Diese Seite verwaltet **PBGui-Market-Data Downloads** pro Exchange mit Fokus auf **Hyperliquid l2Book** (Order Book Snapshots) und daraus abgeleitete **1-Minuten-Kerzen**.

## Übersicht

Die Market Data Seite bietet:
- Download von **Hyperliquid l2Book** Rohdaten vom AWS S3 Archive (Requester Pays)
- **"Build best possible 1m archive"** - automatische Erstellung optimaler 1m-Kerzen aus mehreren Quellen
- Anzeige vorhandener Raw-Dateien (Inventory)
- Download-Log pro Exchange

---

## Hyperliquid: l2Book Archive (AWS S3)

### Was ist l2Book?

l2Book sind stündliche **Order Book Snapshots** (Limit Order Book Level 2) von Hyperliquid als `.lz4` komprimierte Dateien. Daraus können hochpräzise 1-Minuten-Kerzen berechnet werden.

### Download-Prozess

1. **AWS-Zugangsdaten eingeben:**
   - `AWS profile name`: Name des Profils (z.B. `pbgui-hyperliquid`)
   - `aws_access_key_id` und `aws_secret_access_key`: IAM-Credentials mit S3-Zugriff
   - `AWS region`: `us-east-2` (Standard für Hyperliquid Archive)

2. **Coins auswählen:**
   - Multiselect: Einzelne Coins oder `All` für alle aktivierten Coins
   - **Kein Coin ist vorausgewählt** - explizite Auswahl erforderlich

3. **Datumsbereich festlegen:**
   - **Start date**: Erstes zu ladendes Datum (Standard: ältestes verfügbares Datum im Archiv)
   - **End date**: Letztes zu ladendes Datum (Standard: neuestes verfügbares Datum im Archiv)
   - Tooltips zeigen die Archiv-Grenzen

4. **"Auto download l2Book"** klicken:
   - Job wird in Background Queue eingereiht
   - Worker startet automatisch falls nicht aktiv
   - **Auto-Trigger Build OHLCV:** Nach Abschluss des l2Book Downloads wird "Build best 1m" automatisch für jeden Coin ausgelöst, der neue Daten erhalten hat
     - Nur Coins mit tatsächlichen Downloads triggern einen Build-Job
     - Spart den manuellen Schritt "Build best 1m" separat auszuführen

### Kostenoptimierung

**Wichtig:** Das Hyperliquid S3 Archive ist **Requester Pays** - Sie bezahlen für:
- S3 GET-Requests (~$0.0004 pro 1.000 Requests)
- Daten-Transfer (~$0.09 pro GB)

**Skipped Files = keine Kosten:**
```
planned:24 downloaded:0 skipped:24 failed:0 (13.3 MB)
```
- `skipped:24` = lokale Dateien existieren bereits, **kein S3-Request**
- `downloaded:0` = keine neuen Downloads, **keine Transfer-Kosten**
- `failed:0` = keine fehlgeschlagenen Requests

**Der Download prüft zuerst lokal** ob Dateien existieren, bevor S3 kontaktiert wird!

### Connection Pooling

Der Download nutzt eine **einzige boto3 Session** für alle parallelen Downloads:
- Teilt TCP-Verbindungen zwischen Threads
- Reduziert SSL-Handshakes
- Schnellerer Download durch Connection-Reuse

### Speicherort

Heruntergeladene l2Book-Dateien:
```
pbgui/data/ohlcv/hyperliquid/l2Book/<COIN_CCXT>/<YYYYMMDD>-<H>.lz4
```

Beispiel:
```
pbgui/data/ohlcv/hyperliquid/l2Book/KBONK_USDC:USDC/20231120-01.lz4
pbgui/data/ohlcv/hyperliquid/l2Book/BTC_USDC:USDC/20250210-15.lz4
```

**COIN_CCXT Format:**
- `BTC_USDC:USDC` - Standard Format
- `KBONK_USDC:USDC` - K-Prefix für spezielle Coins (BONK, PEPE, FLOKI, SHIB, LUNC, DOGS, NEIRO)

---

## Build Best Possible 1m Archive (Auto)

### Was macht diese Funktion?

Erstellt für jeden Coin ein **optimales 1-Minuten-Archiv** durch intelligente Kombination mehrerer Datenquellen:

### Datenquellen-Priorität

Für jede fehlende Minute wird in dieser Reihenfolge geprüft:

1. **API 1m** (falls bereits downgeloadet)
   - Verwendet vorhandene API-Downloads aus `1m_api/`
   - Nur für leere Slots (SOURCE_CODE_MISSING)

2. **l2Book → 1m Konvertierung** (höchste Qualität)
   - Konvertiert **lokale** l2Book-Dateien zu 1m-Kerzen
   - Nutzt Order Book Mid-Price für OHLCV
   - **Kein Vergleich mit S3** - nur lokale Dateien

3. **Binance USDT-Perp Gap Fill** (Fallback 1)
   - Lädt fehlende Minuten von Binance USDT-Perpetuals
   - **Smart Gap-Smoothing:**
     - `open` der ersten Gap-Minute = `close` der vorherigen l2Book-Kerze
     - `close` der letzten Gap-Minute = `open` der nächsten l2Book-Kerze
   - Glättet Übergänge zwischen verschiedenen Datenquellen

4. **Bybit USDT-Perp Gap Fill** (Fallback 2)
   - Lädt verbleibende Lücken von Bybit USDT-Perpetuals
   - Wichtig für Tokens wie HYPE, die auf Binance später listeten
   - Gleiche Smart-Smoothing-Logik wie Binance

### Workflow

```
Für jeden Tag:
  1. Prüfen ob Tag bereits vollständig (1440 Minuten)
     → Verarbeitung überspringen (Optimierung 1)
     ↓
  2. API 1m einfügen (falls vorhanden)
     ↓
  3. Lokale l2Book → 1m konvertieren (optimierter Parser)
     ↓
  4. Verbleibende Lücken mit Binance füllen (nur bei Lücken)
     ↓
  5. Noch offene Lücken mit Bybit füllen (nur bei Lücken)
     ↓
  6. Source-Codes aktualisieren
```

### Performance-Optimierungen

**1. Vollständige Tage Überspringen (Optimierung 1)**
- Tage mit 1440 Minuten (vollständig) werden komplett übersprungen
- Re-Runs werden ~100x schneller (0.01s vs 1.6s pro vollständigem Tag)
- Nur Tage mit fehlenden Daten werden verarbeitet

**2. Bedingte API-Aufrufe (Optimierung 2)**
- Binance/Bybit werden nur aufgerufen wenn tatsächlich Lücken existieren
- Keine unnötigen API-Calls für vollständige l2Book-Tage
- Reduziert Netzwerk-Overhead und Rate-Limit-Druck

**3. Schnelle l2Book-Verarbeitung (~47% schneller)**
- Optimierter JSON-Parser (orjson statt stdlib json)
- Direktes Bytes-Parsing (überspringt UTF-8 Decode)
- Effiziente Float-Arithmetik für Mid-Price-Berechnung
- **Resultat:** ~1.6s pro Tag (runter von ~2.4s)
- **Impact:** 100 Tage in ~2.7 Minuten verarbeitet (war ~4 Minuten)

### Source Code Tracking

Jede Minute erhält einen Code zur Nachverfolgbarkeit:
- `SOURCE_CODE_L2BOOK` = aus lokalem l2Book berechnet (beste Qualität)
- `SOURCE_CODE_API` = von Hyperliquid API
- `SOURCE_CODE_OTHER` = von Binance/Bybit (andere Exchange)
- `SOURCE_CODE_MISSING` = leer / keine Daten

### Ausgabeordner

**Best 1m Dateien:**
```
pbgui/data/ohlcv/hyperliquid/1m/<COIN>/YYYY-MM-DD.npz
```

**API 1m Raw-Downloads:**
```
pbgui/data/ohlcv/hyperliquid/1m_api/<COIN>/YYYY-MM-DD.npz
```

**Source Index:**
```
pbgui/data/ohlcv/hyperliquid/_source_index/<COIN>/<YYYYMMDD>.npy
```

### Typischer Log-Output

**Zusammenfassung nach Build:**
```
[hl_best_1m] BTC improve: days=180 l2book_added=12450 binance_filled=3580 bybit_filled=0
[hl_best_1m] HYPE improve: days=90 l2book_added=85000 binance_filled=2500 bybit_filled=1850
```

- `days=180` - geprüfte Tage
- `l2book_added=12450` - Minuten aus l2Book konvertiert
- `binance_filled=3580` - Minuten von Binance geladen
- `bybit_filled=1850` - Minuten von Bybit geladen (z.B. für HYPE Token)

**Detaillierte Timing-Logs (optional):**

Um detaillierte Performance-Metriken pro Tag zu aktivieren, Umgebungsvariable setzen:
```bash
export PBGUI_TIMING_LOGS=1
```

Dann enthalten Logs Timing-Breakdowns:
```
[TIMING] SOL 20241001 total=1.608s read=0.000s src_idx=0.000s api=0.000s 
         l2book=1.604s l2write=0.004s binance=0.000s bybit=0.000s 
         existing=1440 l2added=1440

[TIMING] SOL 20241006 total=2.729s read=0.000s src_idx=0.000s api=0.000s 
         l2book=1.897s l2write=0.006s binance=0.820s bybit=0.005s 
         existing=1438 l2added=1438
```

**Timing-Breakdown:**
- `total` - Komplette Verarbeitungszeit für den Tag
- `read` - Lesen der existierenden 1m NPZ Datei
- `src_idx` - Laden des Source-Index
- `api` - Verarbeiten der API 1m Daten
- `l2book` - Konvertierung l2Book → 1m (~1.6s pro Tag typisch)
- `l2write` - Schreiben der l2Book-abgeleiteten 1m auf Disk
- `binance/bybit` - Lückenfüllung (nur wenn nötig)
- `existing` - Bereits vorhandene Minuten vor Verarbeitung
- `l2added` - Minuten aus l2Book-Konvertierung hinzugefügt

---

## Empfohlener Workflow

### 1. Erstmalige Einrichtung

```
1. AWS-Credentials in Market Data eingeben
2. Coins aktivieren (über Enable/Disable Toggle)
3. l2Book für gewünschten Datumsbereich downloaden
   → "Build best 1m" wird automatisch für heruntergeladene Coins getriggert
4. Warten bis beide Jobs fertig (l2Book Download + Build OHLCV)
```

### 2. Regelmäßige Updates

```
1. l2Book download (lädt nur neue/fehlende Stunden)
   → skipped Files = kostenlos, keine erneuten Downloads
   → "Build best 1m" auto-getriggert für Coins mit neuen Daten
2. Fertig! Kein manuelles "Build best 1m" nötig
```

### 3. Lücken füllen

```
1. Inventory prüfen - fehlende Zeiträume identifizieren
2. Date Range gezielt setzen (Start/End date)
3. l2Book für Lücke downloaden
   → "Build best 1m" auto-getriggert
4. Fertig! Lücke automatisch gefüllt
```

### 4. Manueller Build (Optional)

```
"Build best 1m" manuell ausführen nur wenn:
- Rebuild ohne neue l2Book Daten nötig
- Re-Processing mit anderen Einstellungen gewünscht
- Testing oder Troubleshooting
```

---

## AWS Credentials Management

### Profile speichern

PBGui speichert AWS-Credentials als Profile:

**Credentials:** `~/.aws/credentials`
```ini
[pbgui-hyperliquid]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

**Region:** `~/.aws/config`
```ini
[profile pbgui-hyperliquid]
region = us-east-2
```

### IAM Permissions

Minimal benötigte S3-Permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject"],
    "Resource": "arn:aws:s3:::hyperliquid-archive/*"
  }]
}
```

---

## Troubleshooting

### Hohe AWS-Kosten

**Vermeiden durch:**
- Nur benötigte Coins und Datumsbereiche auswählen
- Nicht mehrfach dieselben Daten downloaden (Check `skipped` im Log)
- Größere Date Ranges in einem Job statt vieler kleiner Jobs

### l2Book vorhanden, aber 1m nicht generiert

**Prüfen:**
1. Existieren l2Book-Dateien im korrekten Verzeichnis?
   ```
   ls -lh pbgui/data/ohlcv/hyperliquid/l2Book/BTC_USDC:USDC/
   ```

2. Auf auto-getriggerten "Build best 1m" Job warten
   - Task Queue auf aktive `hl_best_1m` Jobs prüfen
   - Oder "Build best 1m" manuell ausführen

3. Log prüfen:
   ```
   [hl_best_1m] BTC improve: days=X l2book_added=0 ...
   ```
   Wenn `l2book_added=0` → l2Book-Dateien werden nicht erkannt

### Build Best 1m ist langsam

**Erwartete Performance:**
- Vollständige Tage: ~0.01s pro Tag (übersprungen via Optimierung 1)
- l2Book-Verarbeitung: ~1.6s pro Tag
- Mit Binance/Bybit-Lücken: +0.8-1.0s pro Tag

**Wenn signifikant langsamer:**
1. Timing-Logs aktivieren: `export PBGUI_TIMING_LOGS=1`
2. Prüfen welche Operation langsam ist
3. l2Book > 3s pro Tag → Disk I/O prüfen (SSD empfohlen)
4. Binance/Bybit > 2s pro Tag → Netzwerk/API-Probleme prüfen

### Build Best 1m wird nicht auto-getriggert nach l2Book Download

**Prüfen:**
1. Hat l2Book Download tatsächlich neue Dateien heruntergeladen?
   - Im Log nach `downloaded:N` mit N > 0 suchen
   - `skipped:24` = keine neuen Dateien = kein Trigger
2. Task Queue auf `hl_best_1m` Jobs prüfen
3. "Build best 1m" manuell triggern falls nötig

### Source Index Bereinigung nach Löschen

**Automatische Bereinigung:**
- Quick delete: Entfernt gesamtes 1m_src Verzeichnis für Coin
- Multiselect delete: Batch-Entfernung für alle ausgewählten Coins
- Datumsbasiertes Löschen: Inkrementelles Update (nullt gelöschte Tage)
- Clear dataset: Iteriert alle Coins und bereinigt Indexes

Keine manuelle Bereinigung der Source-Indexes nötig!

---

## Technische Details

### l2Book Format

- **Kompression:** LZ4 (schnell, moderate Kompression)
- **Granularität:** Stündlich (H = 0-23, single-digit für 0-9)
- **Dateigröße:** ~700-800 KB pro Stunde (komprimiert)
- **Inhalt:** JSON-Lines mit L2 Order Book Snapshots
- **Bucket:** `hyperliquid-archive` (us-east-2)
- **S3 Path:** `market_data/YYYYMMDD/H/l2Book/<coin>.lz4`

### l2Book Konvertierungs-Performance

**Verarbeitungs-Pipeline:**
1. LZ4 Dekompression (~15% der Zeit)
2. JSON-Parsing mit orjson (~75% der Zeit)
3. Mid-Price Berechnung aus Bid/Ask (~10% der Zeit)

**Optimierungen:**
- **orjson Parser:** 37% schneller als stdlib json
- **Direktes Bytes-Parsing:** Überspringt UTF-8 Decode (12% schneller)
- **Float-Arithmetik:** Schneller als Decimal für Zwischenberechnungen
- **Kombiniert:** ~47% Verbesserung (2.4s → 1.6s pro Tag)

**Typische Raten:**
- ~22.000 L2 Snapshots pro Stunden-Datei
- ~110.000 Mid-Preise pro Sekunde Verarbeitungsrate
- ~1.6 Sekunden pro Tag (24 Stunden-Dateien)

### Spezielle Coins (k-Prefix)

Hyperliquid nutzt K-Präfix für Meme-Coins mit vielen Nullen:
- `BONK` → `kBONKUSDC` (Symbol) → `KBONK_USDC:USDC` (Verzeichnis)
- `PEPE` → `kPEPEUSDC` → `KPEPE_USDC:USDC`

**Liste:** BONK, FLOKI, LUNC, PEPE, SHIB, DOGS, NEIRO

### Coin-Normalisierung

Die Pipeline konvertiert zwischen verschiedenen Formaten:

```
UI Input           → Symbol Lookup     → Directory         → S3 Key
─────────────────────────────────────────────────────────────────────
BONK (normalized)  → kBONKUSDC         → KBONK_USDC:USDC  → kBONK
BTC (normalized)   → BTCUSDC           → BTC_USDC:USDC    → BTC
```

### Parallelisierung

- **l2Book Download:** 8 parallele Worker (Standard)
- **Shared S3 Client:** Connection Pooling über alle Threads
- **Retry Logic:** Automatisch für transiente Fehler
- **Build OHLCV:** Sequenziell pro Tag, aber Tage unabhängig verarbeitet

### Source Index Management

**Binärformat:** 360 Bytes pro Tag (1 Byte pro Minute + Padding)
- Jedes Byte kodiert Datenquelle für diese Minute
- Schnelle inkrementelle Updates beim Löschen (kein vollständiges Rebuild)
- Memory-mapped für effizienten Zugriff

**Lösch-Strategien:**
- **Kompletter Coin:** Entferne gesamtes `1m_src/<COIN>/` Verzeichnis
- **Datumsbereich:** Nulle Bytes für gelöschte Tage (schnelles Inkrement)
- **Clear dataset:** Iteriere Coins, bereinige jeden Index

---

## Hinweise

- Diese Seite löscht **keine** alten Dateien - manuelles Aufräumen erforderlich
- l2Book-Konvertierung geschieht **on-demand** beim "Build best 1m"
- PB7 CandlestickManager nutzt die `1m/` Dateien als Quelle
- PBGui OHLCV Daten koennen in Backtest und Optimize als Datenquelle ausgewaehlt werden
- Dateien werden als `.npz` gespeichert, um Platz zu sparen (ungefaehr halber Speicherbedarf)
- Nach der l2Book -> 1m Konvertierung kann man l2Book Dateien loeschen, um Platz zu sparen; solange die `1m/` Dateien bleiben, werden l2Book Daten nicht mehr benoetigt. Zukuenftige Downloads laden weiterhin nur neue l2Books, weil `1m_src` festhaelt, was bereits vorhanden ist.
- **Abhängigkeiten:** Benötigt `orjson>=3.9.0` (automatisch mit PBGui installiert)

---

## Konfiguration

### Auto-Refresh Einstellungen (PBData Background Service)

PBData aktualisiert automatisch die neuesten 1m-Kerzen von der Hyperliquid API im Hintergrund.

**Konfiguration über GUI:**
- Market Data Seite → "Settings (Latest 1m Auto-Refresh)" Expander
- Werte ändern und "Save Settings" klicken
- Änderungen werden automatisch im nächsten Refresh-Zyklus angewendet (kein Neustart nötig)

**Oder über `pbgui.ini`:**

```ini
[pbdata]
# Auto-Refresh Intervall für Latest-1m API-Fetches (Standard: 120 Sekunden / 2 Minuten)
latest_1m_interval_seconds = 300  # Beispiel: 5 Minuten bei vielen Symbolen

# Pause zwischen einzelnen Coins um Rate Limits zu vermeiden (Standard: 0.5 Sekunden)
latest_1m_coin_pause_seconds = 1.0  # Beispiel: 1 Sekunde Pause pro Coin

# API-Request Timeout pro Coin (Standard: 30 Sekunden)
latest_1m_api_timeout_seconds = 45.0  # Beispiel: 45 Sekunden bei langsamer Verbindung

# Lookback-Fenster für API-Fetches (Standard: 2-4 Tage)
latest_1m_min_lookback_days = 2
latest_1m_max_lookback_days = 4
```

**Warum das Intervall erhöhen?**
- **Standard:** 120 Sekunden (2 Minuten) funktioniert für ~20-30 Coins
- **Viele Symbole:** Erhöhe auf 300-600 Sekunden (5-10 Minuten) wenn alle Hyperliquid-Symbole gefetcht werden
- **Rate Limits:** Hyperliquid API hat Throttling - größere Intervalle vermeiden Probleme

**Warum die Coin-Pause erhöhen?**
- **Standard:** 0.5 Sekunden zwischen Coins verhindert Burst-Requests
- **Rate Limit Probleme:** Erhöhe auf 1-2 Sekunden bei 429-Fehlern
- **Viele Symbole:** Längere Pausen = mehr Gesamtzeit pro Zyklus, passe `latest_1m_interval_seconds` entsprechend an

**Warum das API-Timeout erhöhen?**
- **Standard:** 30 Sekunden pro Coin API-Request
- **Langsame Verbindung:** Erhöhe auf 45-60 Sekunden bei häufigen Timeouts
- **Viele Candles:** Größere Lookback-Fenster benötigen längere Timeouts

**Funktionsweise:**
1. PBData durchläuft alle aktivierten Coins alle N Sekunden
2. Pro Coin: Fetche Lookback-Fenster (2-4 Tage), überschreibe existierende Minuten
3. Status sichtbar im "Market Data status" Expander (zeigt next_run_in_s pro Coin)
4. Settings werden automatisch pro Zyklus neu geladen - Änderungen wirken sofort
```
