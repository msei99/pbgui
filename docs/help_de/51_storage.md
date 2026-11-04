# Storage

**System -> Storage** zeigt, wie viel Speicherplatz PBGui, PB7, PB8 und ihre Python-Umgebungen belegen, was sich aufräumen lässt, wie sich der Verbrauch entwickelt und die wichtigsten Systemwerte dieses Hosts.

## Hintergrund-Scan

Die Ordnergrößen stammen aus einem Hintergrund-Scan. Er durchläuft den PBGui-Ordner, die konfigurierten PB7- und PB8-Ordner und die PB/PBGui-Umgebungen neben PBGui. Der Scan:

- läuft in einem eigenen Prozess mit niedriger Priorität, damit API und laufende Bots nicht ausgebremst werden
- wiederholt sich automatisch im eingestellten Intervall (Standard 6 Stunden, siehe **Retention**)
- startet zusätzlich, wenn du die Seite öffnest und der letzte Scan älter als eine Stunde ist
- ist nach einem Cleanup nicht nötig: gelöschte Einträge verschwinden sofort aus dem Ergebnis

Oben steht, wann der letzte Scan fertig war. Die Plattenbelegung pro Dateisystem ist immer aktuell.

## Overview

- **Disk**: belegter und freier Platz pro Dateisystem sowie ein Balken, der den belegten Platz auf PBGui, PB7, PB8, Python-Umgebungen und den Rest der Platte aufteilt.
- **Reclaimable**: Summen pro Stufe mit den Standardfiltern. **Preview safe cleanup** und **Preview optional cleanup** nehmen alle Kategorien dieser Stufe mit deinen aktuellen Filtern und öffnen eine Vorschau. Jede Kachel listet ihre Kategorien mit Größe; ein Klick auf eine Kategorie öffnet ihre Einträge.
- **Largest folders**: aufklappbarer Baum der größten Ordner, bis zu vier Ebenen tief.
- Nach einem Cleanup steht das Ergebnis (gelöschte Einträge, freigegebener Platz, Fehler) unter den Stufen.

## Stufen

| Stufe | Bedeutung |
| --- | --- |
| **Safe** | Es gehen keine nützlichen Daten verloren. Rotierte Logs, alte Job-Logs, alte Lauf-Logs, Datenbank-Backups über die neuesten hinaus und Reste abgeschlossener Reparaturen. Kann automatisch aufgeräumt werden. |
| **Optional** | Wird bei Bedarf neu erzeugt oder hat wenig Wert, etwa die vorbereiteten Backtest-Datensätze von PB7 oder abgebrochene und fehlgeschlagene Cloud-Jobs. Wird nur ausgewählt, wenn du es willst. |
| **Review** | Eigene Ergebnisse und mögliche Altlasten: Optimize-Ergebnisse und Backtests von PB7/PB8, abgeschlossene Cloud-Jobs, PB7-Altdaten, Release-Worktrees, manuelle Kopien und unbenutzte Python-Umgebungen. Jeden Eintrag wählst du selbst aus. |
| **Protected** | Marktdaten, Datenbank, Zugangsdaten, API-Keys und Konfigurationen. Nur zur Information; Storage löscht sie nie. |

## Kategorie-Ansichten

**Logs**, **Jobs & Results**, **Caches & Market Data** und **Backups & Leftovers** listen ihre Kategorien mit Anzahl, Größe, ältestem Eintrag und den Einträgen, die zum Filter passen.

- **Older than (days)** trifft nur Einträge, deren neueste Datei älter ist als dieses Alter. Bei Ordnern zählt die neueste Datei im Ordner.
- **Keep newest** behält immer diese Anzahl der neuesten Einträge (Datenbank-Backups).
- Filter merkt sich der Browser.
- **Show entries** / **Choose entries** öffnet die Eintragsliste. Ein Klick wählt eine Zeile, Ziehen über mehrere Zeilen oder Shift-Klick wählt einen Bereich, auf der Tastatur gehen Leertaste und Enter. **Select matching** wählt alle Einträge, die zum Filter passen.
- Ausgegraute Einträge sind in Benutzung und nicht wählbar, zum Beispiel Logs laufender Jobs, Ordner mit Änderungen in den letzten 24 Stunden, Python-Umgebungen, die PBGui, PB7, PB8, ein laufender Prozess oder ein systemd-Dienst verwendet, und noch aktive Cloud-Jobs.

### Hinweise zu einzelnen Kategorien

- **PB7 legacy historical data**: PB7 hat `historical_data/` nach `caches/ohlcv/` übernommen und lässt den alten Ordner zum manuellen Löschen stehen. Die Kategorie ist erst verfügbar, wenn PB7 die Migration als abgeschlossen markiert hat (`caches/ohlcv/.migration_done`).
- **PB7 prepared backtest data** ist gesperrt, solange ein PB7-Backtest oder -Optimize läuft.
- **Cloud-Jobs** werden über das Purge der Vast-Queue gelöscht, zusammen mit ihren Wiederholungsversuchen. Abläufe mit einem abgeschlossenen Versuch stehen unter **Completed cloud jobs**.
- **Release worktrees** werden mit `git worktree remove` entfernt.
- **Marktdaten** (PBGui-Archiv, PB7-OHLCV-Cache und -Store) verwaltest du unter **Information -> Market Data**.

## Vorschau und Löschen

Jeder Cleanup beginnt mit einer Vorschau, die Einträge, Größen und Summe zeigt. Zu Beginn sind alle Einträge ausgewählt; ein Klick auf eine Zeile nimmt sie heraus, Ziehen oder Shift-Klick wählt einen Bereich, dazu gibt es **Select all** / **Select none**. **Delete** löscht nur die ausgewählten Einträge und fragt noch einmal nach. Unmittelbar vor dem Löschen prüft der Worker jeden Eintrag erneut auf der Platte mit der aktuellen Zeit und überspringt Einträge, die inzwischen benutzt werden oder nicht mehr passen, etwa einen Ergebnisordner, der während des Cleanups neue Dateien bekommen hat. Ordner werden zuerst beiseitegeschoben und dann gelöscht; stoppt PBGui dazwischen, wird das Löschen beim nächsten Lauf beendet. **Stop** im Fortschrittsbalken beendet den Cleanup nach der aktuellen Datei.

Es läuft immer nur ein Scan oder Cleanup gleichzeitig. Eine Vorschau verfällt nach 30 Minuten.

## Retention

- **Scan interval (hours)**: wie oft der Hintergrund-Scan läuft.
- **Automatic cleanup**: führt die aktivierten Regeln einmal pro Tag nach der gewählten Stunde aus. Automatisch aufräumen lassen sich nur die Kategorien in der Regel-Tabelle. Jede aktive Regel braucht ein Alter, eine Anzahl zu behaltender Einträge oder beides.
- Automatic cleanup ist standardmäßig aus. Das Ergebnis steht wie bei einem manuellen Cleanup in der Overview.

## History

Pro Tag wird ein Messpunkt gespeichert (bis etwa zwei Jahre): belegter Plattenplatz und die Größe von PBGui, PB7, PB8, den Python-Umgebungen und ihren größten Ordnern. Die Tabelle zeigt das Wachstum der letzten 7 und 30 Tage.

## System

CPU-Modell, Kerne, Auslastung und Load, Arbeitsspeicher, Swap, Uptime, Betriebssystem, die Python-Versionen von PBGui, PB7 und PB8, NVIDIA-GPUs (wenn `nvidia-smi` vorhanden ist), Plattenbelegung und die Liste der PBGui-Prozesse mit Speicher- und CPU-Verbrauch. Die Werte aktualisieren sich alle paar Sekunden, solange die Ansicht offen ist.
