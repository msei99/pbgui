# DB Tools

DB Tools bietet kontrollierte Wartungs- und Einweg-Migrationsabläufe für PBGui-Datenbanken, Dashboards und Templates zwischen dem lokalen Master und bekannten Remote-Mastern. Öffne die Seite über **System -> DB Tools**.

Vor jedem manuellen Datenbank-Wartungsschreibvorgang wird ein Backup erstellt. Destruktive Aktionen benötigen eine explizite Vorschau oder Bestätigung. Remote-Aktionen verwenden ausschließlich die bereits im VPS Manager konfigurierten Hosts.

## Ziele und Sicherheit

- **local** bezeichnet die PBGui-Installation, auf der die Seite geöffnet ist.
- Remote-Ziele sind bekannte Master-Nodes aus dem VPS Manager.
- Quelle und Ziel müssen bei Kopier- und Sync-Aktionen unterschiedlich sein.
- Aktive PBData-Schreibvorgänge können eine Aktion blockieren, wenn das Ziel nicht sicher geändert werden kann.
- Manuelles Cleanup, User-Copy, vollstaendige DB-Kopie und Restore reservieren das Ziel exklusiv, bevor der PBData-Stopp bestaetigt und Backups erstellt werden. Ein fehlgeschlagener Stopp oder unbekannter Dienstzustand blockiert die Aktion und gilt nicht als bereits gestoppter Writer.
- Restore aendert bestehende SQLite-Inhalte direkt und erhaelt offene Verbindungen, Dateiidentitaet und WAL-Verarbeitung. PBData startet erst nach einem konsistenten Ergebnis oder erfolgreichem Rollback erneut, und nur wenn es vorher lief.
- Remote-Wartung verwendet einen zielseitigen Worker mit einer durchgehenden Lease. Alte Remote-Checkouts oder API-Prozesse ohne Writer-Guards werden abgewiesen. Remote-PBGui muss vor einem neuen Versuch ausdruecklich aktualisiert und neu gestartet werden; DB Tools installiert keine Updates automatisch.
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

Die Zielreservierung umfasst Backup, jeden Tabellen-Commit und das Schliessen der Verbindungen. Schlaegt Cleanup oder Copy fehl, versucht die Aktion das gesamte vorherige Ziel-Bundle wiederherzustellen, bevor Writer weiterarbeiten duerfen.

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
3. reserviert das Ziel und bestaetigt den PBData-Stopp,
4. bereitet alle Quell- und Recovery-Snapshots vor der ersten Zieldatenbank-Aenderung vor,
5. stellt beide Datenbanken ueber SQLite wieder her, ohne verbundene Dateien auszutauschen,
6. startet PBData erneut, falls es vorher lief.

Nutze bevorzugt Copy User Data oder Sync Jobs, wenn nur einzelne Benutzer übertragen werden sollen.

## Sync Jobs

Sync Jobs kopieren ausgewählte Benutzer regelmäßig von einem Quell-Master auf einen oder mehrere Ziel-Master.

- Der Sync läuft ausschließlich von der konfigurierten Quelle zu allen ausgewählten Zielen.
- Jobs ergänzen nur fehlende Zeilen; vorhandene Zielzeilen werden weder gelöscht noch überschrieben.
- Geplanter Sync behaelt sein zeilenbasiertes Verhalten und erstellt keine manuellen Wartungs-Snapshots. Normale Scan-/Sync-Leases bleiben geteilt; exklusive Wartung blockiert ueberlappende Schreibvorgaenge, ohne persistente Jobs zu beenden.
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

Restore reserviert das Ziel, bestaetigt den PBData-Stopp und erstellt vor der ersten Datenbankaenderung ein zusaetzliches Sicherheitsbackup. Alle Ersatz-Snapshots werden zuerst vorbereitet. Fehler oder Abbruch vor dem konsistenten Abschluss setzen die betroffenen Datenbanken zurueck; eine erst durch die fehlgeschlagene Aktion angelegte optionale DB wird wieder entfernt. Bestehende Datenbanken werden direkt ueber SQLite wiederhergestellt; ihre aktiven WAL-/SHM-Dateien werden nicht geloescht.

Lokale Backups haben ein Zeitbudget von 30 Sekunden je Datei einschliesslich SQLite-Busy-Wiederholungen. Backup und Validierung laufen ausserhalb der API-Ereignisschleife; ein Abbruch wartet auf deren Aufraeumen, bevor Wartungssperren freigegeben werden. Snapshot-Namen sind eindeutig, bestehende Backups werden nicht ueberschrieben. Das Schema wird vor der Integritaetspruefung gegen eine Allowlist geprueft: ausfuehrbare Schemaobjekte, nicht unterstuetzte Ausdruecke und uebergrosse SQLite-Werte werden abgelehnt. Fehlgeschlagene Validierung erreicht die Installation nicht.

Remote-Datenbankbackups verwenden ebenfalls eindeutige Namen und ein SQLite-Zeitbudget von 30 Sekunden. Ein vollstaendiger WAL-faehiger Snapshot wird privat erstellt und erst danach atomar veroeffentlicht. Eine Namenskollision bricht ab, ohne vorhandene Dateien oder Symlinks zu ersetzen; ein fehlgeschlagenes Backup entfernt nur seine eigenen temporaeren Dateien. Dadurch bleiben fruehere Recovery-Snapshots auch bei zwei Anfragen auf denselben Backupnamen erhalten.

Delete entfernt die ausgewählten Backup-Dateien dauerhaft. Behalte mindestens ein geprüftes aktuelles Backup, bevor ältere Kopien gelöscht werden.

## Recovery unterbrochener Wartung

Das private Journal `data/locks/db-tools-recovery.json` erfasst vor der Installation die urspruengliche Existenz jeder DB und dauerhaft gespeicherte Undo-Snapshots. Nach unterbrochener Installation oder fehlgeschlagenem Rollback bleiben kooperative Schreibvorgaenge und der Start von PBData/Database blockiert. Auch die API-Restart-Steuerung zeigt ausstehende Recovery an. Das Journal niemals loeschen, um diesen Schutz zu umgehen.

Der authentifizierte Endpoint `POST /api/db-tools/maintenance/recover` nimmt `{"target":"local"}` oder den Namen des ausgewaehlten Remote-Masters an und liefert den normalen Fortschritts-Handle. Recovery setzt eine unvollstaendige Installation zurueck; war Konsistenz bereits vermerkt, werden nur Writer-Neustart und Aufraeumen abgeschlossen. Es wird keine neue Cleanup-/Copy-/Restore-Anfrage ausgefuehrt. Bei blockiertem API-Start kann ein Operator im betroffenen PBGui-Checkout `python -m db_maintenance recover` mit dessen PBGui-Python-Umgebung ausfuehren. Fehlgeschlagene Recovery erhaelt Journal und Snapshots fuer die Reparatur; das DB-Tools-Log beachten.

Nach verlorener Remote-Antwort behaelt der initiierende Master einen `db-tools-remote-*.json`-Beleg und blockiert Neustarts, bis explizite Ziel-Recovery das Ergebnis klaert. Ein Verbindungsabbruch bedeutet nicht, dass der Remote-Worker beendet wurde. Cancellation wartet auf Worker und Rollback; nach bereits vermerktem konsistentem Abschluss wird eine fertige Aktion nicht mehr rueckgaengig gemacht.

Der SSH-Transport sendet Wartungs- und Recovery-Kommandos auch nach Timeout oder verlorener Antwort niemals automatisch erneut; reine Vorabpruefungen behalten ihr normales Retry-Verhalten. Recovery entfernt auch erkannte private `.sqlite-backup-*`- und `.sqlite-restore-*`-Reste nach einem Absturz innerhalb eines SQLite-Helfers. Symlinks, unerwartete Dateien und Staging anderer Aktionen werden weder verfolgt noch rekursiv geloescht.

Sicherheitsbackups bleiben im Backup Manager verfuegbar. Privates Recovery-Staging wird erst nach bestaetigter Konsistenz und abgestimmtem Writer-Neustart entfernt. Dies ist wiederherstellbare Mehrdatei-Wartung, keine einzelne crash-atomare SQLite-Transaktion ueber beide Dateien. Alle PBGui-Writer muessen die aktualisierten Guards verwenden; beliebige externe SQLite-Werkzeuge oder alte Peers, die das Protokoll umgehen, liegen ausserhalb dieser Koordination.

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
