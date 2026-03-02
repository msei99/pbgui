# PBv7 Backtest

Die **PBv7 Backtest** Seite ermöglicht das Erstellen, Ausführen und Auswerten von Passivbot v7 Backtests.
Sie ist in fünf Tabs am Seitenanfang gegliedert:

| Tab | Zweck |
|-----|-------|
| **Configs** | Backtest-Konfigurationen erstellen und bearbeiten |
| **Queue** | Backtest-Runner überwachen und steuern |
| **Log** | Live-Log-Ausgabe eines laufenden Backtests streamen |
| **Results** | Abgeschlossene Backtest-Ergebnisse durchsuchen und analysieren |
| **Archive** | Community Config-Archive aufrufen |

---

## Tab: Configs

### Listenansicht
Die Sidebar bietet folgende Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| `:material/refresh:` | Config-Liste neu laden |
| `:material/add:` | Neue Backtest-Config erstellen |
| `:material/chart_data:` | Ergebnisse der gewählten Config öffnen |
| `:material/edit:` | Gewählte Config bearbeiten |
| `:material/delete:` | Gewählte Config löschen (Haken bei *Results* um auch Ergebnisse zu löschen) |

### Bearbeitungsansicht
Beim Erstellen oder Öffnen einer Config öffnet sich das Bearbeitungsformular im Configs-Tab.
Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| `:material/home:` | Zurück zur Config-Liste |
| `:material/save:` | Aktuelle Config speichern |
| **Import** | Config aus Zwischenablage / Datei importieren |
| **Results** | Direkt zu den Ergebnissen dieser Config springen (Results-Tab) |
| **Calculate Balance** | Balance Rechner für diese Config öffnen |
| **Add to Backtest Queue** | Speichern und einreihen → wechselt in den Queue-Tab |

---

## Tab: Queue

Zeigt alle ausstehenden, laufenden und abgeschlossenen Backtest-Jobs.

Spalten in der Tabelle:

| Spalte | Beschreibung |
|--------|-------------|
| **Start/Stop** | Job starten oder stoppen |
| **View Results** | Ergebnisse des Jobs öffnen (wechselt in Results-Tab) |
| **View Logfile** | Checkbox aktivieren → Log wird auf dem **Log**-Tab gestreamt |
| **Finished** | Zeigt abgeschlossene Jobs |

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| `:material/refresh:` | Queue neu laden |
| **Max CPU** | Maximale parallele Backtest-Prozesse |
| **Autostart** | Eingereihte Jobs automatisch starten |
| `:material/delete: selected` | Ausgewählte Jobs löschen |
| `:material/delete: finished` | Alle abgeschlossenen Jobs löschen |
| `:material/delete: all` | Alle Jobs löschen |

---

## Tab: Log

Streamt die Live-Log-Ausgabe eines laufenden (oder kürzlich abgeschlossenen) Backtests.

**So öffnest du das Log für einen bestimmten Job:**
1. In den **Queue**-Tab wechseln.
2. In der Zeile des gewünschten Jobs die Checkbox **View Logfile** aktivieren.
3. PBGui wechselt automatisch in den **Log**-Tab und beginnt zu streamen.

Wurde der Backtest noch nicht gestartet, wartet der Log-Viewer und beginnt automatisch zu streamen, sobald die Log-Datei auf der Disk erscheint.

Mit dem **Lines**-Selektor lässt sich einstellen, wie viele historische Zeilen beim Verbinden geladen werden.

---

## Tab: Results

Alle abgeschlossenen Backtest-Ergebnisse durchsuchen.

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| `:material/refresh:` | Ergebnisse neu laden |
| **All Results** | Zurück zur globalen Ergebnisansicht |
| **BT selected** | Ausgewähltes Ergebnis als neuen Backtest erneut ausführen |
| **Strategy Explorer** | Strategy Explorer für das gewählte Ergebnis öffnen |
| **Calculate Balance** | Balance Rechner öffnen |
| **Add to Compare** | Ergebnis zum Live-vs-Backtest Vergleich hinzufügen |
| **Add to Run** | Live-Run aus der gewählten Config erstellen |
| **Optimize from Result** | Optimierung auf Basis des Ergebnisses starten |
| **Add to Config Archive** | Config ins persönliche Archiv speichern |
| **Go to Config Archives** | In den Archive-Tab wechseln |
| `:material/delete: selected` | Ausgewählte Ergebnisse löschen |
| `:material/delete: all` | Alle Ergebnisse löschen |

Die Tabelle ist nach **Result Time** oder anderen Spalten sortierbar. Mit **Filter by Backtest Name** lassen sich grosse Ergebnis-Listen einschränken.

---

## Tab: Archive

Community- und persönliche Config-Archive.

### Archiv-Listenansicht
Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| `:material/refresh:` | Archive neu laden |
| `:material/settings:` | Archiv-Einstellungen konfigurieren |
| **Sync Github** | Neueste Community-Archive von GitHub holen |
| **Push own Archive** | Eigenes Archiv auf GitHub schieben |

### Config-Archiv Detailansicht
Ein Klick in ein Archiv zeigt seine Ergebnisse. Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| `:material/refresh:` | Ergebnisse neu laden |
| `:material/arrow_upward_alt:` | Zurück zur Archiv-Liste |
| **BT selected** | Gewählte Config als neuen Backtest einreihen → wechselt in Queue-Tab |
| **Calculate Balance** | Balance Rechner öffnen → kehrt in den Archive-Tab zurück |
| **Add to Compare** | Zum Live-vs-Backtest Vergleich hinzufügen |
| `:material/delete: selected` | Ausgewählte Ergebnisse löschen |


## Typische Workflows

### Neuen Backtest ausführen
1. **Configs** → `:material/add:` → Backtest konfigurieren → **Add to Backtest Queue**
2. **Queue** → *Max CPU* einstellen, *Autostart* aktivieren
3. *View Logfile* aktivieren → Fortschritt im **Log**-Tab verfolgen
4. Nach Abschluss → *View Results* aktivieren oder in **Results** wechseln

### Ergebnis erneut ausführen / verfeinern
1. **Results** → Ergebnis auswählen → **BT selected** → Datum/Balance anpassen → **OK**
2. **Queue** → Fortschritt überwachen

### Community-Config verwenden
1. **Archive** → **Sync Github** → Archiv öffnen → Configs auswählen → **BT selected**
2. Wechselt automatisch in **Queue**
3. Nach Abschluss → **Archive** aufrufen um Ergebnisse zu vergleichen

### Live vs. Backtest vergleichen
1. **Results** → Ergebnis auswählen → **Add to Compare**
2. Zu *Information → Live vs Backtest* navigieren
