# PBv7 Backtest

Die **PBv7 Backtest** Seite ermöglicht das Erstellen, Ausführen und Auswerten von Passivbot v7 Backtests.
Es handelt sich um eine eigenständige FastAPI-Seite — kein Seitenneustart nötig. Queue-Updates kommen in Echtzeit per WebSocket.

Die Seite ist in vier Panels gegliedert, die über die linke Sidebar gewechselt werden:

| Panel | Zweck |
|-------|-------|
| **Configs** | Backtest-Konfigurationen erstellen und bearbeiten |
| **Queue** | Backtest-Runner überwachen und steuern |
| **Results** | Abgeschlossene Backtest-Ergebnisse durchsuchen und analysieren |
| **Archive** | Community- und eigene Config-Archive aufrufen |

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
| **💰 Balance Calculator** | Die eigenständige Balance-Calculator-Seite mit der aktuellen Editor-Config als Draft öffnen |
| **⚡ Calc Balance** | Dieselbe Balance-Berechnung inline in einem Modal ausführen, ohne die Backtest-Seite zu verlassen |
| **📥 Import** | Den Run-ähnlichen Paste-JSON-Dialog öffnen und die importierte Config zur Prüfung in den Editor laden |

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
| **✓ Clear Finished** | Alle `complete`- und `error`-Jobs entfernen |
| **⬛ Stop All** | Alle laufenden Backtest-Prozesse beenden |
| **🗑 Delete Selected** | Ausgewählte Queue-Einträge entfernen |
| **⚙ Settings** | Einstellungs-Modal öffnen |

### Einstellungs-Modal

| Einstellung | Beschreibung |
|-------------|-------------|
| **CPU** | Anzahl paralleler Backtest-Prozesse (max = CPU-Kernanzahl) |
| **Autostart** | Wenn aktiviert, startet der Worker `queued`-Jobs automatisch |
| **HLCVS Cache Cleanup — Enabled** | Alte `pb7/caches/hlcvs_data`-Verzeichnisse regelmäßig löschen |
| **Retention (days)** | Verzeichnisse löschen, die älter als dieser Wert sind (Standard: 7) |
| **Check interval (h)** | Prüfintervall in Stunden (Standard: 24) |
| **🧹 Clean Now** | Bereinigung sofort mit dem aktuellen Retention-Wert ausführen; zeigt per Toast wie viele Verzeichnisse gelöscht und wie viel Speicher freigegeben wurden |

---

## Panel: Results

Alle abgeschlossenen Backtest-Ergebnisse durchsuchen.

### Filter & Sortierung

- **Config**-Dropdown — nach Config-Name filtern (exakte Übereinstimmung)
- **Suchfeld** — Freitext-Filter über alle Spalten
- Spaltenheader anklicken zum Sortieren; erneut klicken für umgekehrte Reihenfolge

### Toolbar-Aktionen

| Schaltfläche | Aktion |
|--------|--------|
| **🔄 Backtest** | Ausgewählte Ergebnisse als neue Backtests neu ausführen (öffnet Datums-/Balance-/Exchange-Modal) |
| **▶ Add to Run** | Live-Run aus der ausgewählten Config erstellen |
| **📈 Compare** | Ausgewählte Ergebnisse zur Vergleichsansicht hinzufügen |
| **🗑 Delete Selected** | Ausgewählte Ergebnisse von Disk löschen |

### Aktionsschaltflächen pro Zeile

| Symbol | Aktion |
|--------|--------|
| 📊 | Ergebnis-Charts öffnen (Equity-Kurve, TWE, etc.) |
| 🗑 | Dieses einzelne Ergebnis löschen |

### Ergebnis-Charts

Ein Klick auf eine Zeile öffnet ein vollständiges Chart-Panel mit:
- **Equity-Kurve** (log-Skala umschaltbar)
- **PnL** über Zeit
- **TWE** (Total Wallet Exposure) Chart
- **Hedged PnL** falls vorhanden
- Vollständige **Analyse-Metriken** Tabelle
- **Config JSON** Viewer

📌 **Pin** drücken, um das Chart beim Durchsuchen anderer Ergebnisse sichtbar zu lassen.
📈 **Compare** drücken, um mehrere Ergebnisse auf einem Chart zu überlagern.

### Re-Backtest Modal

Verfügbar über die **🔄 Backtest**-Schaltfläche in der Toolbar. Optionen:

| Option | Beschreibung |
|--------|-------------|
| **start_date / end_date** | Datumsbereich für den erneuten Lauf überschreiben |
| **starting_balance** | Startguthaben überschreiben |
| **Exchange(s)** | Zu verwendende Exchange(s) überschreiben |
| **📂 Use PBGui Market Data** | Wenn aktiviert, wird `ohlcv_source_dir` auf den PBGui-Datenpfad gesetzt |

---

## Panel: Archive

Community- und persönliche Config-Archive, gespeichert als Git-Repositories.

### Archiv-Listenansicht

| Schaltfläche | Aktion |
|--------|--------|
| **⬇ Pull All** | Neueste Commits aus allen konfigurierten Archiven holen |
| **⬆ Git Push** | Eigenes Archiv auf das Remote schieben |
| **+ Add Archive** | Neues Archiv konfigurieren (URL, lokaler Pfad) |
| **⚙ Setup** | Archiv-Einstellungen bearbeiten |
| **📋 Log** | Archiv-Sync-Log in schwebenden Panel öffnen |

Klick auf eine Archivzeile öffnet es und zeigt seine Ergebnisse.

### Archiv-Ergebnisansicht

| Schaltfläche | Aktion |
|--------|--------|
| **🏠 Archives** | Zurück zur Archiv-Liste |
| **🔄 Backtest** | Ausgewählte Configs als neue Backtests einreihen → wechselt zu Queue |
| **▶ Add to Run** | Live-Run erstellen |
| **📈 Compare** | Zur Vergleichsansicht hinzufügen |
| **🗑 Delete Selected** | Ausgewählte Archiv-Ergebnisse löschen |

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

