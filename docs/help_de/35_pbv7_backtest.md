# PBv7 Backtest

Die **PBv7 Backtest** Seite ermöglicht das Erstellen, Ausführen und Auswerten von Passivbot v7 Backtests.
Es handelt sich um eine eigenständige FastAPI-Seite — kein Seitenneustart nötig. Queue-Updates kommen in Echtzeit per WebSocket.
Draft-Übergaben aus den FastAPI-Seiten **Run** und **Optimize** landen jetzt ebenfalls direkt hier als FastAPI-Drafts, sodass der Wechsel zwischen diesen PBv7-Seiten keinen Legacy-Relay-Pfad mehr benötigt.
PBv8 Backtest rendert dieselbe Seitenvorlage und denselben visuellen Editor; ein kleiner Versionsadapter ändert ausschließlich generationsspezifische Config-Pfade und API-Endpunkte.

Die Seite ist in fünf Panels gegliedert, die über die linke Sidebar gewechselt werden:

| Panel | Zweck |
|-------|-------|
| **Configs** | Backtest-Konfigurationen erstellen und bearbeiten |
| **Queue** | Backtest-Runner überwachen und steuern |
| **Results** | Abgeschlossene Backtest-Ergebnisse durchsuchen und analysieren |
| **Archive** | Community- und eigene Config-Archive aufrufen |
| **Legacy** | Alte Ergebnisordner unter `pb7/backtests` außerhalb des PBGui-verwalteten `pbgui`-Pfads durchsuchen |

Die **obere Navigationsleiste** enthält:

| Schaltfläche | Aktion |
|--------|--------|
| 🔔 | Notification-Log öffnen (schwebendes Panel zeigt `PBV7UI.log`) |
| 📖 Guide | Diese Hilfeseite öffnen |
| ℹ️ About | PBGui-Versionsinformationen anzeigen |

---

## Panel: Configs

### Listenansicht

Die Tabelle zeigt alle gespeicherten Backtest-Konfigurationen mit den Spalten:
**Name**, **Exchange**, **Start Date**, **End Date**, **Created**, **Modified**, **Actions**.

**Selektion:** Zeile anklicken zum Auswählen/Abwählen. Gedrückt halten und ziehen für Mehrfachauswahl.
Über die Tabelle stehen **Select All** / **Deselect** Schaltflächen zur Verfügung.

**Sidebar-Aktionen:**

| Schaltfläche | Aktion |
|--------|--------|
| **+ New Config** | Neue Backtest-Konfiguration erstellen |
| **🗑 Delete Selected** | Ausgewählte Configs löschen (Bestätigungsdialog mit Option, auch Ergebnisse zu löschen) |

Doppelklick auf eine Zeile öffnet den Editor direkt.

### Bearbeitungsansicht

Die Bearbeitung öffnet sich inline im Hauptbereich. Felder:

| Feld | Beschreibung |
|------|-------------|
| **Name** | Config-Name (wird für Ergebnisse und Queue-Anzeige verwendet) |
| **Exchange(s)** | Eine oder mehrere Exchanges für den Backtest |
| **start_date / end_date** | Datumsbereich des Backtests |
| **starting_balance** | Startguthaben in USD |
| **hsl_signal_mode** | Aus PB7 abgeleiteter Selektor für das HSL-Verhalten auf Kontoebene: `pside` trennt Long/Short-Drawdown-Signale, `unified` verwendet ein gemeinsames Signal |
| **logging_level** | Run-ähnlicher Selektor für die Verbosität `warning`, `info`, `debug` und `trace` |
| **approved_coins / ignored_coins** | Explizite Coin-Listen; automatisch befüllbar über **Apply Filters** |
| **Coin sources** | Quelle der Coin-Listen (PBGui Coin-Datenbank, manuell, etc.) |
| **Market settings sources** | Quelle für marktspezifische Einstellungen |
| **Bot-Parameter** | Strategie-Parameter (Long/Short-Seite, TWE, etc.) |

**Aktionsschaltflächen im Editor:**

| Schaltfläche | Aktion |
|--------|--------|
| **💾 Save** | Konfiguration auf Disk speichern |
| **← Back** | Zurück zur Config-Liste ohne Speichern |
| **Add to Queue** | Speichern und einreihen → wechselt in Queue-Panel |
| **Apply Filters** | Approved/Ignored-Coin-Listen anhand der aktuellen Filter befüllen |
| **📊 View Results** | Zu den Ergebnissen dieser Config im Results-Panel springen |
| **⏩ Convert to V8** | Die aktuell gespeicherte V7-Config mit PB8s offiziellem Migrator konvertieren und in PBv8 Backtest öffnen; bis zum ersten Speichern deaktiviert |
| **💰 Balance Calculator** | Den gemeinsamen Balance Calculator unter Information mit der aktuellen Editor-Config als Draft öffnen |
| **⚡ Calc Balance** | Dieselbe Balance-Berechnung inline in einem Modal ausführen, ohne die Backtest-Seite zu verlassen |
| **🧭 OHLCV Readiness** | Öffnet ein verschiebbares und resizebares Floating-Fenster und führt darin einen PB7-basierten Read-only-Preflight für die aktuelle Config aus. Angezeigt wird, ob die aktuellen Approved Coins lokal bereit sind, aus Legacy-OHLCV-Daten importiert werden können, beim Start nachgeladen würden oder durch persistente Gaps blockiert sind; ausgewertet wird die Vereinigung aus `approved_coins_long` und `approved_coins_short`, und jeder Eintrag zeigt jetzt direkt, ob er aus `long`, `short` oder aus beiden Listen stammt. Wenn PB7 fehlende Bereiche sonst erst beim Start fetchen würde, bietet das Fenster zusätzlich **Preload OHLCV Data** zum Vorwärmen des Caches im Hintergrund an, springt beim Start dieses Preloads automatisch zum Job-Logbereich, zeigt den laufenden Download jetzt mit echten, aus den PB7-Archiv-/CCXT-Logzeilen abgeleiteten Fortschrittsbalken plus Laufzeit, PID, Log-Zählern und Zeitstempel der letzten Aktualisierung an, leitet den CCXT-Fortschritt dabei aus dem vorwärts laufenden Request-Cursor ab statt bei Börsen mit neueren Rückgabedaten ständig auf 100% zu springen, verwendet für den Preload jetzt denselben warmup-korrigierten effektiven Start wie der Readiness-Check, damit nach einem fertigen Preload nicht mehr genau diese Warmup-Tage fehlen, stuft Märkte, die erst nach dem angeforderten Fenster gestartet sind, jetzt korrekt als zu jung ein statt so zu tun, als ließen sich diese alten Kerzen noch preloaden, blendet während des laufenden Downloads zusätzlich eine **Stop Preload**-Aktion ein, bietet oben rechts außerdem einen Button zum Aufziehen des Fensters auf die Browserfläche, hält dieses Log sauber am Live-Tail statt wieder nach oben zu springen und lässt das fertige Preload-Ergebnis sichtbar, bis ein frischer Readiness-Check es ersetzt |
| **📥 Import** | Den Run-ähnlichen Paste-JSON-Dialog öffnen und die importierte Config zur Prüfung in den Editor laden; gepastete Configs laufen dabei durch dieselbe PB7-Ladepipeline wie normal geladene Backtest-Configs, sodass ergänzte Parameter sowie `neutralized`- / `review`-Markierungen erhalten bleiben |

Der **Raw JSON**-Expander, die JSON-Editoren für **Bot Configuration** `long` / `short`, JSON-basierte **Additional Parameters** und der **Import**-Dialog nutzen jetzt ein gemeinsames JSON-Validierungsmuster. Ungültiges JSON wird direkt im Editor markiert, die fehlerhafte Zeile kann per Button angesprungen werden, der Fehlerhinweis erscheint an einer gemeinsamen festen Stelle im Viewport, und Speichern/Import bleibt blockiert, bis das JSON wieder gültig ist.

Die **Coin Overrides → Config File**-Editoren für `long` / `short` nutzen dasselbe JSON-Validierungsmuster und dieselbe gemeinsame feste Position für den Fehlerhinweis wie die Haupteditor-Felder. Ungültiges JSON wird direkt im Editor markiert, und das Schließen des Coin-Override-Editors bleibt blockiert, bis diese JSON-Snippets wieder gültig sind.

### Coins & Filter

Diese Felder steuern, welche Coins über die PBGui-Coin-Datenbank einbezogen werden.
Nach dem Anpassen **Apply Filters** klicken, um die Approved/Ignored-Listen zu aktualisieren.

| Feld | Beschreibung |
|------|-------------|
| **market_cap (min M$)** | Minimale Marktkapitalisierung in Millionen USD. `0` = kein Limit. |
| **vol/mcap** | Maximales 24h-Volumen-zu-Marktkapitalisierung-Verhältnis. Sehr hohe Werte deuten oft auf manipulierte Coins hin. |
| **tags** | CoinMarketCap-Kategorietags. Nur Coins mit mindestens einem passenden Tag werden berücksichtigt. Leer = alle. |
| **only_cpt** | Nur Copy-Trading-fähige Coins einbeziehen. Erfordert aktuelle Daten (Coin-Data-Seite). |
| **notices_ignore** | Coins mit aktiven CoinMarketCap-Hinweisen ausschließen (z. B. Untersuchung, Insolvenz). |

---

## Panel: Queue

Zeigt alle ausstehenden, laufenden und abgeschlossenen Backtest-Jobs mit Echtzeit-Updates.

### Tabellenspalten

| Spalte | Beschreibung |
|--------|-------------|
| **Status** | `queued` / `running` / `backtesting` / `complete` / `error` |
| **Name** | Config-Name |
| **Exchange** | Verwendete Exchange(s) |
| **Created** | Zeitstempel der Einreihung |
| **Actions** | Kontextabhängige Aktionsschaltflächen |

**Selektion:** Klick zum Auswählen, ziehen für Mehrfachauswahl.
**Select All** / **Deselect** stehen in der Toolbar über der Tabelle bereit.

### Aktionsschaltflächen pro Zeile

| Schaltfläche | Bedingung | Aktion |
|--------|-----------|--------|
| ▶ (gelb) | `error` | Neustart — den fehlgeschlagenen Backtest sofort neu starten |
| ▶ | `queued` | Start — diesen Job sofort starten |
| ⬛ (rot) | `running` / `backtesting` | Stopp — laufenden Prozess beenden |
| 📊 (grün) | `complete` | Ergebnisse anzeigen — zu Results wechseln, gefiltert auf diese Config |
| 📜 | immer | Log — schwebendes Log-Panel für die Log-Datei dieses Jobs öffnen |
| 🗑 | immer | Entfernen — Queue-Eintrag löschen (stoppt falls läuft) |

### Sidebar-Aktionen

| Schaltfläche | Aktion |
|--------|--------|
| **📈 Compare** | Die passenden Ergebnisse für die ausgewählten abgeschlossenen Queue-Jobs laden, zu Results wechseln und den Vergleich direkt öffnen |
| **✓ Clear Finished** | Alle `complete`- und `error`-Jobs entfernen |
| **⬛ Stop All** | Alle laufenden Backtest-Prozesse beenden |
| **🗑 Delete Selected** | Ausgewählte Queue-Einträge entfernen |
| **⚙ Settings** | Einstellungs-Modal öffnen |

Wenn du mehrere abgeschlossene Queue-Zeilen auswählst und **📈 Compare** klickst, löst PBGui pro ausgewähltem Queue-Eintrag den passenden Ergebnis-Batch auf, öffnet das **Results**-Panel, markiert diese Ergebniszeilen vor und rendert den Vergleich sofort. Queue-Einträge, die noch nicht abgeschlossen sind oder kein passendes gespeichertes Ergebnis haben, werden übersprungen.

### Einstellungs-Modal

Der Queue-Settings-Dialog enthält jetzt zusätzlich `Use PBGui Market Data`. Wenn diese Option aktiv ist, setzt PBGui `backtest.ohlcv_source_dir` unmittelbar vor jedem gequeueeten oder manuell gestarteten Backtest auf das aktuelle PBGui-Market-Data-Root um, unabhängig davon, welcher Pfad im Config-Editor gespeichert ist.

| Einstellung | Beschreibung |
|-------------|-------------|
| **CPU** | Anzahl paralleler Backtest-Prozesse (max = CPU-Kernanzahl) |
| **Autostart** | Wenn aktiviert, startet der Worker `queued`-Jobs automatisch |
| **Use PBGui Market Data** | Überschreibt `backtest.ohlcv_source_dir` direkt vor dem Start, sodass Queue-Jobs immer den von PBGui verwalteten OHLCV-Datensatz verwenden |
| **HLCVS Cache Cleanup — Enabled** | Alte Verzeichnisse unter `pb7/caches/hlcvs_data` und `pb7/caches/ohlcvs/materialized` regelmäßig löschen |
| **Retention (days)** | Verzeichnisse löschen, die älter als dieser Wert sind (Standard: 7) |
| **Check interval (h)** | Prüfintervall in Stunden (Standard: 24) |
| **🧹 Clean Now** | Bereinigung sofort mit dem aktuellen Retention-Wert über beide Cache-Pfade ausführen; zeigt per Toast wie viele Verzeichnisse gelöscht und wie viel Speicher freigegeben wurden |

---

## Panel: Results

Alle abgeschlossenen Backtest-Ergebnisse durchsuchen.

### Filter & Sortierung

- **Version**-Dropdown — PBv7-Ergebnisse, PBv8-Ergebnisse oder beide anzeigen; auf dieser Seite ist PBv7 vorausgewählt
- **Config**-Dropdown — nach Config-Name filtern (exakte Übereinstimmung)
- **Suchfeld** — Freitext-Filter über alle Spalten
- Spaltenheader anklicken zum Sortieren; erneut klicken für umgekehrte Reihenfolge

Abgeschlossene Queue-Jobs invalidieren jetzt sofort den gecachten Results-Stand. Wenn du bereits im Results-Panel bist, lädt PBGui die Tabelle automatisch neu, sodass der neue Eintrag ohne Panel-Wechsel erscheint.

### Toolbar-Aktionen

| Schaltfläche | Aktion |
|--------|--------|
| **🔄 Backtest** | Ausgewählte Ergebnisse als neue Backtests neu ausführen (öffnet Datums-/Balance-/Exchange-Modal) |
| **▶ Add to Run** | Live-Run aus der ausgewählten Config erstellen |
| **📈 Compare** | Ausgewählte Ergebnisse zur Vergleichsansicht hinzufügen |
| **🧬 Optimize from Result** | Den Optimize-Editor direkt mit dem ausgewählten Ergebnis als Draft und `Starting Seeds = self` öffnen |
| **🗑 Delete Selected** | Ausgewählte Ergebnisse von Disk löschen |

### Aktionsschaltflächen pro Zeile

| Symbol | Aktion |
|--------|--------|
| 📊 | Ergebnis-Charts öffnen (Equity-Kurve, TWE, etc.) |
| **V8** | Die exakte `config.json` dieses Ergebnisses mit PB8s offiziellem Migrator konvertieren und in PBv8 Backtest öffnen |
| 🗑 | Dieses einzelne Ergebnis löschen |

Auch die Configs-Tabelle bietet **V8** für die gespeicherte V7-Backtest-Config. Beide Konvertierungen lassen die V7-Quelle unverändert. Bei der Konvertierung eines Results ermittelt PBGui vor der Migration die effektiv verwendeten Maker- und Taker-Raten aus dessen linearen `fills.csv`-Daten. Dadurch kann ein normalisierter Default aus der Result-Config nicht mehr die tatsächlich von V7 verwendeten Exchange-Gebühren ersetzen. PBGui entfernt vor der Migration ausschließlich eigene Metadaten und einen veralteten temporären Loader-Pfad; verbleiben echte nicht unterstützte oder manuell zu prüfende Felder, veröffentlicht PB8 keine lauffähige Config.

### Ergebnis-Charts

Ein Klick auf eine Zeile öffnet ein vollständiges Chart-Panel mit:
- **Equity-Kurve** (log-Skala umschaltbar)
- **PnL** über Zeit
- **TWE** (Total Wallet Exposure) Chart
- **Hedged PnL** falls vorhanden
- Vollständige **Analyse-Metriken** Tabelle
- **Config JSON** Viewer

📌 **Pin** drücken, um das Chart beim Durchsuchen anderer Ergebnisse sichtbar zu lassen.
📈 **Compare** drücken, um mehrere Ergebnisse auf einem Chart zu überlagern. Mit **Version: Both** können PBv7- und PBv8-Ergebnisse gemeinsam ausgewählt werden; PBGui lädt jede Equity-Datei vom passenden Backend und kennzeichnet die Chart-Serien mit ihrer Version.

### Re-Backtest Modal

Verfügbar über die **🔄 Backtest**-Schaltfläche in der Toolbar. Optionen:

| Option | Beschreibung |
|--------|-------------|
| **start_date / end_date** | Datumsbereich für den erneuten Lauf überschreiben |
| **starting_balance** | Startguthaben überschreiben |
| **Exchange(s)** | Zu verwendende Exchange(s) überschreiben |
| **📂 Use PBGui Market Data** | Wenn aktiviert, wird `ohlcv_source_dir` auf den PBGui-Datenpfad gesetzt |

Bei archivierten Ergebnissen verwenden diese Controls zunächst die Werte aus der archivierten `config.json`, einschließlich Enddatum und Market-Data-Auswahl. Das Deaktivieren von **Use PBGui Market Data** ist ein expliziter Override und wird beim Queue-Start nicht durch die globale Backtest-Einstellung ersetzt.

---

## Panel: Archive

Community- und persönliche Config-Archive, gespeichert als Git-Repositories. Nur das als **My Archive** ausgewählte Archiv ist für PBGui beschreibbar. Andere Archive sind inhaltlich read-only: Durchsuchen, Importieren, Vergleichen, Re-Backtests und Pulls sind möglich, PBGui fügt dort aber keine Einträge hinzu, benennt oder löscht sie nicht und führt weder Commit noch Push aus. Sobald ein Clone lokale Änderungen hat, wird Pull vor jedem Remote-Kontakt blockiert; Änderungen in **My Archive** müssen zuerst gepusht oder anderweitig aufgelöst werden, während ein Dirty-Fremdarchiv unangetastet bleibt.

### Archiv-Listenansicht

| Schaltfläche | Aktion |
|--------|--------|
| **⬇ Pull All** | Neueste Commits aus allen konfigurierten Archiven holen |
| **⬆ Git Push** | Änderungen aus **My Archive** committen und auf das Remote schieben |
| **+ Add Archive** | Neues Archiv über Name und Git-URL klonen |
| **⚙ Setup** | **My Archive**, Git-Identität, Token, Auto-Pull-Intervall und README-Text einstellen |
| **📋 Log** | Archiv-Sync-Log in schwebenden Panel öffnen |

Klick auf eine Archivzeile öffnet es und zeigt seine Ergebnisse. Die Zähler stammen aus `pbgui/archive_manifest.json`, wenn das Manifest gültig ist; andernfalls verwendet PBGui einen read-only Dateisystem-Scan.

PBGui leitet Archivziele aus der PB7-`config_version` ab; einen manuell editierbaren Archivpfad gibt es nicht. Backtest-Ergebnisse liegen unter `pbgui/configs/{config_version}/backtests/`, Optimize-Configs unter `pbgui/configs/{config_version}/optimize/`. Fehlende oder ungültige Versionen verwenden ein `unknown`-Verzeichnis plus Content-Fingerprint, um Kollisionen zu vermeiden.

Wenn **My Archive** sauber ist, migriert PBGui beim Öffnen des Panels einen begrenzten Batch alter Ergebnisse und prüft vor Add oder Push eine vollständige Migration. Ein Dirty Worktree oder ein fehlgeschlagener Git-Status blockiert die Migration, ohne bestehende Änderungen zu verwerfen. Die Statuszeile zeigt verbleibende Legacy-Einträge und lokal migrierte, noch zu pushende Änderungen.

### Archiv-Inhaltsansicht

Die Ansicht enthält die Tabs **Backtests**, **Optimize Settings** und nur für **My Archive** **Schedules**.

| Schaltfläche | Aktion |
|--------|--------|
| **🏠 Archives** | Zurück zur Archiv-Liste |
| **🔄 Backtest** | Ausgewählte Configs als neue Backtests einreihen → wechselt zu Queue |
| **▶ Add to Run** | Live-Run erstellen |
| **📈 Compare** | Zur Vergleichsansicht hinzufügen |
| **🧮 Balance** | Ausgewähltes Ergebnis im Balance Calculator öffnen |
| **🧬 Score Preview** | Archiv-Scoring ohne Schreibzugriff vorab anzeigen |
| **🗑 Delete Selected** | Ausgewählte Ergebnisse nur aus **My Archive** löschen |

Weitere Aktionen für **My Archive** sind Config-Gruppen umbenennen, **Retest & Replace**, Scores neu aufbauen, Git-Historie kompaktieren, Duplikate entfernen und **Remove Liquidated**. Liquidated Cleanup zeigt immer zuerst einen Dry Run und verlangt vor der verifizierten Löschung eine ausdrückliche Bestätigung. Geplante Retests ersetzen archivierte Ergebnisse erst nach einem erfolgreichen, nicht liquidierten Lauf.

Der Tab **Optimize Settings** kann archivierte Optimize-Configs aus jedem Archiv anzeigen oder importieren. Existiert der lokale Name bereits, stehen **Overwrite**, **Import as Copy** und **Cancel** zur Auswahl. Hinzufügen und Löschen archivierter Optimize-Configs ist auf **My Archive** beschränkt. Ein erneuter Export identischer Inhalte verwendet den bestehenden Fingerprint-Pfad und dessen Metadaten statt einer weiteren nummerierten Kopie.

---

## Panel: Legacy

Das **Legacy**-Panel ist für alte oder falsch abgelegte Ergebnisordner gedacht, die unter `pb7/backtests/*` liegen, aber nicht unter dem normal von PBGui verwalteten Baum `pb7/backtests/pbgui/*`.

Dieses Panel ist sinnvoll, wenn ein Backtest auf Disk fertig vorliegt, aber im normalen **Results**-Panel nicht erscheint, weil er in einen Legacy-Pfad wie `pb7/backtests/combined/...` geschrieben wurde.

### Toolbar-Aktionen

| Schaltfläche | Aktion |
|--------|--------|
| **↻ Refresh** | Legacy-Ergebnisordner erneut scannen |
| **🔄 Backtest** | Ausgewählte Legacy-Configs als neue Backtests erneut einreihen |
| **▶ Add to Run** | Live-Run aus der ausgewählten Legacy-Config erstellen |
| **📈 Compare** | Ausgewählte Legacy-Ergebnisse in einem gemeinsamen Vergleichschart überlagern |
| **🗑 Delete Selected** | Ausgewählte Legacy-Ergebnisordner von Disk löschen |

### Hinweise

- Die Tabelle unterstützt dasselbe Zeilen- und Drag-Selection-Verhalten wie **Results** und **Archive**.
- Ergebnisnamen werden bei Bedarf aus dem Verzeichnispfad abgeleitet, wenn der ursprüngliche Config-Name in der Legacy-Config nicht mehr sauber vorhanden ist.
- Mit **🔄 Backtest** lassen sich Legacy-Läufe wieder in den normal von PBGui verwalteten Workflow überführen.

---

## Typische Workflows

### Neuen Backtest ausführen
1. **Configs** → **+ New Config** → Config ausfüllen → **Add to Queue**
2. **Queue** → **⚙ Settings** → CPU einstellen, **Autostart** aktivieren → **Save**
3. Status-Badge beobachten: `queued` → `running` / `backtesting` → `complete`
4. 📜 in der Job-Zeile klicken, um das Live-Log in einem schwebenden Panel zu verfolgen
5. 📊 (grün) klicken nach Abschluss → springt zu Results

### Ergebnis erneut ausführen / verfeinern
1. **Results** → Ergebnis auswählen → **🔄 Backtest** → Datum/Balance anpassen → **OK**
2. **Queue** → Fortschritt überwachen

### Backtest-Ergebnis als Optimize-Draft verwenden
1. **Results** → genau ein Ergebnis auswählen → **🧬 Optimize from Result**
2. PBGui öffnet direkt den FastAPI-Optimize-Editor und nicht erst die Config-Liste
3. Der importierte Draft wird mit **Starting Seeds = self** vorbelegt, sodass der Optimize-Run mit genau dieser Config startet

### Community-Config verwenden
1. **Archive** → **⬇ Pull All** → Archiv öffnen → Configs auswählen → **🔄 Backtest**
2. **Queue** → überwachen; oder Autostart aktivieren
3. Nach Abschluss → **Results** zur Analyse

### Mehrere Ergebnisse vergleichen
1. **Results** → Ergebnisse auswählen → **📈 Compare**
2. Das Vergleichschart öffnet sich mit allen ausgewählten Equity-Kurven überlagert

### Speicherplatz freigeben (HLCVS-Cache)
1. **Queue** → **⚙ Settings**
2. **HLCVS Cache Cleanup** aktivieren, **Retention** und **Check interval** einstellen
3. **🧹 Clean Now** für sofortige Bereinigung klicken — die Toast-Meldung zeigt die freigegebenen MB
4. **Save** klicken, um den automatischen Zeitplan zu speichern
