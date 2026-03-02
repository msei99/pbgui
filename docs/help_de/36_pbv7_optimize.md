# PBv7 Optimierung

Die **PBv7 Optimize** Seite erlaubt das Erstellen, Ausführen und Auswerten von Passivbot v7 Optimierungen (genetischer Algorithmus zur Parametersuche).
Sie ist in vier Tabs am Seitenanfang gegliedert:

| Tab | Zweck |
|-----|-------|
| **Config** | Optimierungskonfigurationen erstellen und bearbeiten |
| **Queue** | Optimierungsläufe überwachen und steuern |
| **Log** | Live-Log-Ausgabe eines laufenden Optimierungslaufs streamen |
| **Results** | Abgeschlossene Ergebnisse durchsuchen und Pareto-Fronten analysieren |

---

## Tab: Config

### Listenansicht
Zeigt alle gespeicherten Optimierungskonfigurationen.

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| `:material/refresh:` | Config-Liste neu laden |
| `:material/add:` | Neue Optimierungsconfig erstellen |
| `:material/edit:` | Ausgewählte Config bearbeiten |
| `:material/delete:` | Ausgewählte Config(s) löschen |

### Bearbeitungsansicht
Beim Erstellen oder Öffnen einer Config öffnet sich das Bearbeitungsformular im Config-Tab.

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| `:material/home:` | Zurück zur Config-Liste |
| `:material/save:` | Aktuelle Config speichern |
| **Add to Queue** | Speichern und einreihen → wechselt in den Queue-Tab |
| **Preset… / Load / Save / Del** | Benannte Parameter-Presets verwalten |

Wichtige Einstellungen im Bearbeitungsformular:

| Bereich | Beschreibung |
|---------|--------------|
| **Exchange / Symbols** | Börse und Coins für die Optimierung |
| **Datumsbereich** | Start- und Enddatum der Optimierungssimulation |
| **Starting balance** | Startkapital |
| **Iterations** | Anzahl der genetischen Generationen |
| **CPU cores** | Parallele Prozesse pro Optimierungslauf |
| **Population size** | Größe der Population pro Generation |
| **Pareto max size** | Maximale Anzahl von Configs auf der Pareto-Front |
| **Scoring** | Zielfunktionen (z. B. Sharpe, Drawdown, Profit) |
| **Filter coins** | CoinMarketCap-basierte Coin-Filter anwenden |
| **Starting config** | Optimierer mit einer bestehenden Config als Startwert seeden |

---

## Tab: Queue

Zeigt alle ausstehenden, laufenden und abgeschlossenen Optimierungsjobs.

Spalten in der Tabelle:

| Spalte | Beschreibung |
|--------|-------------|
| **Start/Stop** | Job starten oder stoppen |
| **Status** | Aktueller Zustand: *not started / running / optimizing… / complete / error* |
| **Edit** | Öffnet die Quell-Config des Jobs im Bearbeitungsformular (Config-Tab) |
| **View Logfile** | Aktivieren → Log wird auf dem **Log**-Tab gestreamt |
| **Delete** | Entfernt den Job (und sein Log) aus der Queue |
| **starting_config** | Ob eine Seed-Config verwendet wurde |
| **exchange** | Für diesen Job konfigurierte Börsen |
| **finish** | Zeigt abgeschlossene Jobs |

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| `:material/refresh:` | Queue von der Disk neu laden |
| **Autostart** | Eingereihte Jobs automatisch nacheinander starten |
| `:material/delete: selected` | Ausgewählte Jobs löschen |
| `:material/delete: finished` | Alle abgeschlossenen Jobs löschen |
| `:material/delete: all` | Alle Jobs löschen (laufende werden vorher gestoppt) |

---

## Tab: Log

Streamt die Live-Log-Ausgabe eines laufenden (oder kürzlich abgeschlossenen) Optimierungslaufs.

**So öffnest du das Log für einen bestimmten Job:**
1. In den **Queue**-Tab wechseln.
2. In der Zeile des gewünschten Jobs die Checkbox **View Logfile** aktivieren.
3. PBGui wechselt automatisch in den **Log**-Tab und beginnt zu streamen.

Wurde der Optimierungslauf noch nicht gestartet, wartet der Log-Viewer und beginnt automatisch zu streamen, sobald die Log-Datei auf der Disk erscheint.

Log-Dateien werden unter `data/logs/optimizes/` gespeichert.
Mit dem **Lines**-Selektor lässt sich steuern, wie viele historische Zeilen beim Verbinden geladen werden.

---

## Tab: Results

### Ergebnisliste
Zeigt alle abgeschlossenen Optimierungsergebnisse.

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| `:material/refresh:` | Ergebnisse neu laden |
| `:material/delete: selected` | Ausgewählte Ergebnisse löschen |
| `:material/delete: all` | Alle Ergebnisse löschen |

Tabellenspalten:

| Spalte | Beschreibung |
|--------|-------------|
| **View Paretos** | Pareto-Front-Viewer für dieses Ergebnis öffnen |
| **🎯 explorer** | Vollständigen **Pareto Explorer** für detaillierte Analyse starten |
| **3d plot** | 3-D-Streudiagramm des Ergebnisses generieren (externer Viewer) |
| **Result Time** | Zeitpunkt des Optimierungsabschlusses |
| **Name** | Config-Name dieses Laufs |
| **Result** | Pfad zum Ergebnisverzeichnis |

Mit **Filter by Optimize Name** nach Name filtern.

### Pareto-Viewer (inline)
Nach Klick auf **View Paretos** lassen sich die Pareto-optimalen Configs durchsuchen:

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| `:material/refresh:` | Pareto-Daten neu laden |
| `:material/arrow_upward_alt:` | Zurück zur Ergebnisliste |
| **BT selected** | Ausgewählte Pareto-Config als Backtest einreihen → wechselt zur Backtest-Seite |
| **BT all** | Alle Pareto-Configs als Backtests einreihen |

Mit den Selektoren **Scenario**, **Statistic** und **analyses** lässt sich steuern, welcher Pareto-Schnitt angezeigt wird.

### 🎯 Pareto Explorer (Vollseite)
Öffnet die dedizierte **Pareto Explorer** Seite mit interaktiven Streudiagrammen, Korrelationsanalyse, Config-Inspektion und Ein-Klick-Backtest-Einreihung.

Über **← Back to Optimize Results** in der Sidebar gelangt man zurück zum Results-Tab.

---

## Typische Arbeitsabläufe

### Neue Optimierung starten
1. **Config** → `:material/add:` → Börse, Coins, Datumsbereich und Scoring konfigurieren → **Add to Queue**
2. **Queue** → *Autostart* aktivieren (CPU-Kerne werden in der Config festgelegt)
3. **View Logfile** aktivieren → Fortschritt des genetischen Algorithmus auf dem **Log**-Tab beobachten
4. Nach Abschluss → **Results** öffnen

### Ergebnisse auswerten
1. **Results** → **View Paretos** aktivieren, um die Pareto-Front inline zu durchsuchen
2. Oder **🎯 explorer** für den vollständigen interaktiven Pareto Explorer
3. Vielversprechende Configs auswählen → **BT selected** → Backtests werden auf der Backtest-Seite eingereiht

### Bestehende Config verfeinern
1. **Config** → Config auswählen → `:material/edit:` → Grenzen, Scoring oder Coin-Liste anpassen
2. **Starting config** aktivieren, um den neuen Lauf mit dem besten Ergebnis eines vorherigen Laufs zu seeden
3. **Add to Queue** → Fortschritt auf dem **Log**-Tab überwachen

### Presets nutzen
Häufig verwendete Parametersätze (Grenzen, Population, Scoring) als benannte Presets über **Preset… / Save** in der Bearbeitungs-Sidebar speichern.
Mit **Load** lassen sie sich auf künftigen Configs sofort laden.
