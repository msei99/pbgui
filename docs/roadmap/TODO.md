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

---

## Arbeitsmodus (ab jetzt)
- Neue Ideen immer als Eintrag mit **Ziel**, **Umfang**, **Done wenn** erfassen.
- Erst priorisieren (P0/P1/P2), dann umsetzen.
- Große Themen in kleine, testbare Teilaufgaben schneiden.
