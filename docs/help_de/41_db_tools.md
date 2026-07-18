# DB Tools

DB Tools bietet kontrollierte Wartungs- und Einweg-Migrationsabläufe für PBGui-Datenbanken, Dashboards und Templates zwischen dem lokalen Master und bekannten Remote-Mastern. Öffne die Seite über **System -> DB Tools**.

Vor jedem Datenbank-Schreibvorgang wird ein Backup erstellt. Destruktive Aktionen benötigen eine explizite Vorschau oder Bestätigung. Remote-Aktionen verwenden ausschließlich die bereits im VPS Manager konfigurierten Hosts.

## Ziele und Sicherheit

- **local** bezeichnet die PBGui-Installation, auf der die Seite geöffnet ist.
- Remote-Ziele sind bekannte Master-Nodes aus dem VPS Manager.
- Quelle und Ziel müssen bei Kopier- und Sync-Aktionen unterschiedlich sein.
- Aktive PBData-Schreibvorgänge können eine Aktion blockieren, wenn das Ziel nicht sicher geändert werden kann.
- Datenbank-Austausch und Restore stoppen PBData auf dem Ziel, entfernen veraltete SQLite-WAL-/SHM-Dateien, installieren die Dateien und starten PBData erneut, falls es vorher lief.
- Längere Aktionen erscheinen im Fortschrittsbereich und können zusätzlich im DB-Tools-Logviewer geprüft werden.

Schließe oder starte den API-Server nicht neu, solange eine gebundene DB-Tools-Aktion läuft. Der Restart-Button zeigt bis zum Abschluss einen Blocker an.

## Cleanup User Data

Cleanup entfernt Zeilen ausgewählter Benutzer aus `pbgui.db` und `pbgui_trades.db` auf einem Ziel.

1. Ziel-Master auswählen.
2. **Remove all data for users** oder **Remove data older than date** wählen.
3. Einen oder mehrere Benutzer auswählen.
4. **Preview** anklicken und die Zeilenzahlen pro Datenbank und Tabelle prüfen.
5. **Run Cleanup** erst starten, wenn die Vorschau der beabsichtigten Änderung entspricht.

Der Datumsmodus entfernt Zeilen vor dem UTC-Stichtag aus Tabellen mit Zeitstempelspalten. Nach Änderungen an Ziel, Modus, Datum oder Auswahl ist eine neue Vorschau erforderlich.

## Copy User Data

Copy User Data überträgt ausgewählte Benutzer zwischen zwei Mastern.

| Modus | Verhalten |
|---|---|
| **Add only missing** | Fügt auf dem Ziel fehlende Zeilen ein und behält vorhandene Zielzeilen bei. |
| **Replace user data** | Löscht zuerst die Zielzeilen der ausgewählten Benutzer und importiert danach deren Quellzeilen. |

Vor dem Kopieren immer die Vorschau prüfen. Sie kontrolliert Quellbenutzer, Zielsicherheit und die erwartete Aktion. Replace ist für die ausgewählten Benutzer auf dem Ziel absichtlich destruktiv.

## Copy Complete Database

Diese Aktion ersetzt `pbgui.db` und `pbgui_trades.db` auf dem Ziel durch die Dateien des Quell-Masters.

Nutze sie nur, wenn das Ziel eine vollständige Datenbankkopie der Quelle werden soll. Der Ablauf:

1. validiert Quelle und Ziel,
2. erstellt konsistente Quell-Snapshots,
3. sichert die Zieldatenbanken,
4. stoppt bei Bedarf PBData auf dem Ziel,
5. installiert beide Datenbanken und entfernt veraltete Sidecars,
6. startet PBData erneut, falls es vorher lief.

Nutze bevorzugt Copy User Data oder Sync Jobs, wenn nur einzelne Benutzer übertragen werden sollen.

## Sync Jobs

Sync Jobs kopieren ausgewählte Benutzer regelmäßig von einem Quell-Master auf einen oder mehrere Ziel-Master.

- Der Sync läuft ausschließlich von der konfigurierten Quelle zu allen ausgewählten Zielen.
- Jobs ergänzen nur fehlende Zeilen; vorhandene Zielzeilen werden weder gelöscht noch überschrieben.
- Jeder Lauf erstellt vor dem Schreiben Ziel-Backups.
- Das minimale Intervall beträgt 30 Sekunden.
- Ein Ziel wird übersprungen oder blockiert, wenn PBData gerade Daten eines ausgewählten Benutzers schreibt.

Empfohlene Einrichtung:

1. Einen Job mit einem eindeutigen Quelle-zu-Ziel-Namen erstellen.
2. Eine Quelle, mindestens ein Ziel und die zu replizierenden Benutzer auswählen.
3. **Check Safety** ausführen.
4. Den ersten kontrollierten Lauf mit **Run Now** starten.
5. Fortschritt und Logs prüfen.
6. Den Job erst nach einem erfolgreichen manuellen Lauf aktivieren.

Gespeicherte aktive Jobs werden nach einem API-Neustart rekonstruiert. Detached Sync-Läufe sind unabhängige Jobs und bleiben über ihren persistierten Status und ihre Logs sichtbar.

## Backup Manager

Der Backup Manager zeigt von DB Tools erstellte Backups eines Masters. Zeilen können sortiert und zum Restore oder Löschen ausgewählt werden.

Restore erstellt vor jedem Austausch ein zusätzliches Sicherheitsbackup. PBData wird gestoppt, das gewählte Backup installiert, veraltete SQLite-Sidecars werden entfernt und PBData wird bei Bedarf erneut gestartet.

Delete entfernt die ausgewählten Backup-Dateien dauerhaft. Behalte mindestens ein geprüftes aktuelles Backup, bevor ältere Kopien gelöscht werden.

## Dashboards

Dashboards kopiert Dashboard-JSON-Dateien und Dashboard-Template-JSON-Dateien zwischen Mastern.

| Modus | Verhalten |
|---|---|
| **Add only missing** | Erstellt auf dem Ziel fehlende Einträge und überspringt vorhandene Namen. |
| **Replace all selected** | Ersetzt vorhandene ausgewählte Einträge und erstellt fehlende Einträge. |

Vor dem Ersetzen werden vorhandene Dateien gesichert. Prüfe vor dem Start die ausgewählten Dashboards, Templates, Quelle, Ziel und den Modus in der Vorschau.

## Fehlerbehebung

- **Target unavailable**: Host- und SSH-Status im VPS Manager prüfen.
- **Safety check blocked**: den gemeldeten PBData-Schreibvorgang stoppen oder abwarten und danach erneut prüfen.
- **Operation already running**: auf den Abschluss der aktiven DB-Tools-Aktion warten und Fortschritt oder Log prüfen.
- **Sync job skipped a target**: im Job-Log PBData-Aktivität, Benutzerverfügbarkeit und Zielverbindung prüfen.
- **Restore or copy failed**: nicht blind wiederholen. Zuerst das Log prüfen und sicherstellen, dass das automatisch erzeugte Ziel-Backup vorhanden ist.

## Best Practices

1. Jeden manuellen Schreibvorgang vorab anzeigen und Quelle sowie Ziel kontrollieren.
2. Mit **Add only missing** beginnen, wenn vorhandene Zieldaten erhalten bleiben müssen.
3. Einen Sync Job vor dem Aktivieren des Zeitplans mit **Run Now** testen.
4. Aktuelle Backups behalten, bis das geänderte Ziel geprüft wurde.
5. Gleichzeitige Wartungsaktionen auf demselben Ziel vermeiden.
