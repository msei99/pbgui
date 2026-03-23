# PBGui – Gemeinsame TODO / Roadmap

Stand: 2026-02-19
Ziel: Ideen sauber festhalten, priorisieren und mit klaren Ergebniskriterien umsetzen.

## Prioritäten
- **P0 (kritisch):** Stabilität, Sicherheit, Betriebsfähigkeit
- **P1 (hoch):** Produktivität, Monitoring, Datenqualität
- **P2 (mittel):** Komfort, Konsistenz, kontinuierliche Verbesserung

---

## P0 – Sicherheit & Stabilität

### 1) PBRemote hardening + Codequalität
**Ziel**
- PBRemote stabil, thread-safe und ohne unsichere Patterns betreiben.

**Umfang**
- Race Conditions identifizieren und beheben (Locking, atomare Writes, saubere Worker-Lifecycle).
- Unsicheren Code entfernen (unsaubere Shell-Aufrufe, unvalidierte Inputs, stille Exception-Swallows).
- Logging und Fehlerpfade vereinheitlichen (klarer Kontext, reproduzierbare Fehlermeldungen).

**Done wenn**
- Keine bekannten Race-Condition-Hotspots mehr offen.
- Kritische Pfade haben definierte Error-Strategien.
- Relevante Tests für Nebenläufigkeit/Fehlerfälle vorhanden und grün.

### 2) CMC API Pool (Master + VPS) mit globaler Usage-Steuerung
**Ziel**
- CMC API-Limits (10k) nicht mehr reißen, Last intelligent über mehrere Keys verteilen.

**Umfang**
- API-Keys als zentral verwaltete Liste auf Master/VPS.
- Globales Usage-Tracking (nicht pro VPS isoliert), inkl. verteiltem Request-Routing.
- Routing-Logik nach Restbudget/Usage entwickeln (fair + limit-schonend).
- Warnsystem bei **80%** Gesamtauslastung über alle Keys.
- Erweiterungsfluss: neuen Key hinzufügen ohne Betriebsunterbruch.

**Done wenn**
- Request-Verteilung erfolgt automatisch über alle verfügbaren Keys.
- 80%-Warnung ist sichtbar (UI + Log) und nachvollziehbar.
- Kein einzelner VPS muss manuell “durchprobieren”; Pool-Logik arbeitet zentral.

---

## P1 – Betrieb & Observability

### 3) Live-Logstream von VPS im Master (anstatt Ansible-Pull)
**Ziel**
- Logs einzelner VPS in Echtzeit direkt im Master sehen.

**Umfang**
- Streaming-Kanal für Log-Tail (push/poll-basiert, robust bei Reconnect).
- Auswahl nach VPS/Service/Logdatei.
- Basis-Filter: Level, Zeitfenster, Textsuche.

**Done wenn**
- Logzeilen laufen live im Master ein.
- Verbindungsabbrüche werden sauber wieder aufgenommen.
- Ansible ist nicht mehr der primäre Weg für Live-Debugging.

### 4) Logging über alle Module vereinheitlichen
**Ziel**
- Einheitlicher Logging-Standard für Format, Pfade, Level und Kontext.

**Umfang**
- Alle Module auf gemeinsames Logging-Schema umstellen.
- Logs konsequent in `data/logs/` halten.
- Einheitliche Tags/Kontexte (z. B. Modul, VPS, Instanz, Request-ID).

**Done wenn**
- Neue und bestehende Module nutzen denselben Logging-Standard.
- Loganalyse ist modulübergreifend konsistent möglich.

### 5) Market-Data für alle Exchanges (analog Hyperliquid-Niveau)
**Ziel**
- Für alle unterstützten Exchanges robuste, vergleichbare Marktdaten-Funktionen anbieten.

**Umfang**
- Exchange-spezifische Quellen ergänzen (Ticker, OHLCV, Symbol-Metadaten).
- Einheitliche Datenvalidierung und Fallback-Strategien.
- Qualitätschecks (Lücken, Inkonsistenzen, Delays).

**Done wenn**
- Kernfunktionen für Marktdaten auf allen Ziel-Exchanges verfügbar.
- Datenqualität ist nachvollziehbar geprüft.

---

## P2 – Qualität & Dokumentation

### 6) Guides für alle GUI-Seiten + Auto-Update-Mechanismus
**Ziel**
- Jede GUI-Seite hat eine verständliche Guide-Doku; Änderungen im Code führen zu Doku-Updates.

**Umfang**
- Fehlende Guides ergänzen (EN + DE, konsistente Struktur).
- Pflegeprozess definieren: bei Feature-/UI-Änderung Guide mitziehen.
- Optionaler Check im Workflow (Hinweis, wenn UI geändert ohne Guide-Update).

**Done wenn**
- Alle produktiven GUI-Seiten haben einen aktuellen Guide.
- Für Codeänderungen existiert ein klarer Doku-Update-Prozess.

### 7) Codebase-Review auf Unsauberkeiten & Fehler
**Ziel**
- Technische Schulden sichtbar machen und systematisch abbauen.

**Umfang**
- Strukturierter Review (Security, Concurrency, Fehlerbehandlung, Dead Code, Konsistenz).
- Findings nach Risiko/Priorität clustern.
- Abarbeitung in kleinen, testbaren Paketen.

**Done wenn**
- Priorisierte Findings-Liste liegt vor.
- Kritische Punkte sind behoben oder mit klarem Plan terminiert.

---

## Bestehende offene Punkte (Legacy-Backlog)
- Split approved / ignored coins dynamic ignore in short/long
- Convert USDT <-> USDC
- Save sort in bt

### IniWatcher überall einbauen
**Ziel**
- Alle Daemons (PBCoinData, PBData, PBRun, PBRemote, PBMon) sollen sofort auf `pbgui.ini`-Änderungen reagieren, statt erst beim nächsten Loop-Zyklus.

**Umfang**
- `IniWatcher` (bereits als `ini_watcher.py` vorhanden) in jeden Daemon integrieren.
- Bisherige `sleep()`-Aufrufe in Main-Loops durch `watcher.changed.wait(timeout=...)` ersetzen.
- Pro Daemon: Config-Reload-Logik in eigene Methode auslagern (analog `PBMaster._apply_config_changes()`).

**Done wenn**
- Alle Daemons nutzen `IniWatcher`.
- Konfigurationsänderungen (z.B. API-Keys, Intervalle, Feature-Flags) werden sofort übernommen ohne Daemon-Neustart.
- Keine redundanten `load_ini()`-Aufrufe pro Loop mehr.

### VPS Disk-Verbrauch optimieren
**Ziel**
- Speicherverbrauch auf den VPS-Servern reduzieren (Logs, Caches, alte Daten).

**Umfang**
- Analyse: welche Dateien/Verzeichnisse den meisten Platz verbrauchen (`passivbot.log`, `caches/`, alte Backtests, `__pycache__`).
- Log-Rotation: automatisches Begrenzen/Rotieren von `passivbot.log` und Dienst-Logs.
- Cache-Bereinigung: alte/unbenutzte OHLCV-Caches, Parquet-Dateien, Lock-Dateien aufräumen.
- Optional: PBMaster-Befehl für Remote-Cleanup (z.B. „Bereinige VPS X").

**Done wenn**
- Logs werden automatisch rotiert und auf sinnvolle Maximalgrößen begrenzt.
- Alte Caches werden periodisch oder auf Befehl bereinigt.
- Disk-Verbrauch pro VPS ist nachvollziehbar im VPS Monitor sichtbar.

---

## P1 – Unified WebSocket + VPS Error Management

### Unified WebSocket `/ws/app`
**Ziel**
- Ein einziger WebSocket-Endpoint für die gesamte Anwendung (Navigation, Dashboard, VPS, Errors).

**Hintergrund**
- Aktuell existieren `/ws/dashboard` (nav + widget updates) und `/ws/vps` (VPS state, logs, commands) als getrennte Endpoints.
- Jeder Client subscribed nur die Topics die er braucht (`vps_error_summary`, `vps_state`, `nav`, etc.).
- In der Streamlit-Phase ergeben sich max. 2 Connections pro Seite (nav_bridge + error_banner als getrennte iframes). Sobald Streamlit weg ist → 1 WS pro Tab.

**Umfang**
- Neuer Endpoint `/ws/app` mit Topic-basiertem Subscribe (`{cmd: "subscribe", topics: [...]}`).
- Messages multiplexed über `type`-Feld: `nav_request`, `dashboard_action`, `dashboard_data`, `vps_state`, `vps_error_summary`, `vps_logs`, `ack_result`, etc.
- Bestehende `/ws/dashboard` und `/ws/vps` bleiben parallel (nicht brechen), werden schrittweise migriert.

**Migration**
1. `/ws/app` Endpoint mit Topic-Subscribe → ~50 Zeilen
2. VPS Error Summary Topic + Ack-Command → ~40 Zeilen
3. Error Banner HTML (`frontend/vps_error_banner.html`) → ~200 Zeilen
4. `has_vps_errors()` → Banner ersetzen → ~10 Zeilen
5. nav_bridge auf `/ws/app` migrieren (später)
6. Dashboard-Widgets auf `/ws/app` migrieren (später)
7. VPS Monitor auf `/ws/app` migrieren (später)
8. `/ws/dashboard` + `/ws/vps` deprecaten (letzter Schritt)

**Done wenn**
- `/ws/app` existiert und wird von mindestens Error-Banner + nav_bridge genutzt.
- Alte Endpoints funktionieren weiterhin (Übergang).

### VPS Error Acknowledgement
**Ziel**
- VPS-Fehler bestätigen können; bestätigte Fehler tauchen erst wieder auf wenn sich die Anzahl deutlich erhöht hat.

**Umfang**
- Ack-State in `VPSStore` (in-memory) + persistiert in `data/vps_error_acks.json`.
- WS-Command `{cmd: "ack_error", key: "server::instance"}` und `{cmd: "ack_all_errors"}` über `/ws/app`.
- Sichtbarkeitslogik: Fehler erst wieder sichtbar wenn `current_et >= acked_et + delta` (delta konfigurierbar via MonitorConfig).
- History: Array der letzten 20 Acks pro Key (Timestamp + Counts beim Ack).
- Error-Banner (`vps_error_banner.html`): collapsible (localStorage), live via WS, Ack-Buttons pro Instanz + Ack All.
- Ersetzt bestehende `has_vps_errors()` + `PBRemote.has_error()` Streamlit-Logik.

**Future: Push-basierte Fehlererfassung**
- PBRun/PBRemote auf Remote-VPS können bei Error-Detection direkt `POST /api/vps/error_event` an PBGui senden.
- VPSStore updated → WS push → Banner zeigt Fehler innerhalb 1 Sekunde (kein SSH-Polling-Delay).

**Done wenn**
- Error-Banner auf allen Seiten sichtbar (ersetzt `st.expander`).
- Ack funktioniert persistent (überlebt Server-Restart + Browser-Wechsel).
- Fehler tauchen nach Ack erst bei deutlichem Anstieg wieder auf.

---

## P2 – Längerfristig

### Hyperliquid Rate-Limit Budget Tracking (PB7)
**Ziel**
- Pro-IP Request-Budget für Hyperliquid mitzählen und proaktiv drosseln, statt reaktiv auf 429-Fehler zu warten.

**Hintergrund**
- Hyperliquid liefert (Stand März 2026) keine Rate-Limit-Header (`X-RateLimit-Remaining` etc.) in den HTTP-Antworten — nur einen blanken 429 bei Überschreitung.
- Das HL-Limit liegt bei ~1200 Requests/Minute pro IP.
- Mehrere Bots auf derselben IP teilen dieses Budget, haben aber keine Sichtbarkeit auf den aktuellen Verbrauch.

**Umfang**
- Shared Request-Counter (z.B. via Datei oder IPC) für alle PB7-Instanzen pro IP.
- Proaktives Throttling wenn Budget-Schwelle erreicht (z.B. 80%).
- Fallback bleibt der bestehende reaktive 429-Backoff.

**Done wenn**
- Bots drosseln koordiniert, bevor 429 eintritt.
- Funktioniert auch bei Server-Neustart (alle Bots gleichzeitig).
- Kein Performanceverlust im Normalbetrieb.

---

### Dashboard: Gridstack.js – Flexibles Widget-Layout
**Ziel**
- Dashboard-Editor von festem Zeile×Spalte-Raster auf ein freies Grid-Layout umstellen.
- Widgets frei platzierbar, beliebig breit (1/3, 1/2, 2/3, voll) und beliebig hoch.

**Umfang**
- Gridstack.js (MIT, Vanilla JS, ~30 KB) als Layout-Engine einbinden.
- Datenmodell von `dashboard_type_R_C` auf eine geordnete Widget-Liste mit `{type, x, y, w, h}` umstellen.
- Migration bestehender Dashboard-Configs (Zeile/Spalte → x/y/w/h Mapping).
- Drag & Drop, Resize-Handles, Auto-Packing über Gridstack.
- Bisherige manuelle Wide/Narrow-Logik entfällt komplett.

**Done wenn**
- Widgets frei verschiebbar und resizebar per Drag & Drop.
- Alte 1-Spalten- und 2-Spalten-Configs werden automatisch migriert.
- Editor- und View-Only-Modus funktionieren mit neuem Layout.

---

## Arbeitsmodus (ab jetzt)
- Neue Ideen immer als Eintrag mit **Ziel**, **Umfang**, **Done wenn** erfassen.
- Erst priorisieren (P0/P1/P2), dann umsetzen.
- Große Themen in kleine, testbare Teilaufgaben schneiden.
