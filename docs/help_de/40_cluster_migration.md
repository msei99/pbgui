# Cluster-Mode-Migration

Diese Anleitung beschreibt die notwendigen Schritte, um ein bestehendes PBGui-Setup von PBRemote/API-Sync/V7-SSH-Sync auf Cluster Sync umzustellen.

PBRemote wird nicht mehr benötigt und beim Upgrade entfernt. Cluster Sync übernimmt den Sync von V7-Configs und API-Keys.

Cluster Sync ersetzt die alten Sync-Wege. PBRun wird nur auf Hosts benötigt, die Bots ausführen. Ein reiner Master braucht PBApiServer und PBCluster, aber keinen laufenden PBRun. Reine VPS-Runner brauchen kein `pbgui-api.service` und kein `PBApiServer.py`; sie brauchen PBCluster für Sync und PBRun nur dann, wenn sie Bots ausführen.

---

## Schritte

### 1. Primären Master aktualisieren

1. Aktualisiere PBGui auf dem Master, den du normalerweise für die UI verwendest.
2. Starte `pbgui-api.service` neu, wenn PBGui den Restart-Hinweis zeigt.
3. Der normale PBGui-Update- und Branch-Switch-Workflow synchronisiert die PBCluster-systemd-Unit und startet PBCluster neu. Wenn du per manuellem `git pull` aktualisiert hast, starte PBCluster selbst mit `systemctl --user restart pbgui-pbcluster.service` neu.
4. Wenn dieser Master keine Bots ausführt, kann `PBRun` gestoppt bleiben.

### 2. Cluster Sync bootstrappen

1. Öffne **System -> Cluster Sync**.
2. Starte **Bootstrap Preview**.
3. Wenn die Vorschau die erwarteten lokalen V7-Configs und VPS-Hosts zeigt, führe **Bootstrap Apply** aus.

### 3. Zusätzliche Master joinen

1. Füge auf jedem zusätzlichen Master den primären Master im VPS Manager hinzu, falls er dort noch nicht bekannt ist, oder trage seine SSH-Daten direkt im Join-Formular ein.
2. Öffne **System -> Cluster Sync** auf dem zusätzlichen Master.
3. Nutze **Join Existing Cluster** mit dem VPS-Monitor-Hostnamen und den SSH-Daten des primären Masters. Wenn die Cluster-SSH-Keys noch nicht installiert sind, versucht PBGui zuerst bestehenden Key-/Pool-Login und fragt das SSH-Passwort nur bei Bedarf per Prompt ab. Das Passwort wird nur für diesen Request verwendet und nicht gespeichert.
4. PBGui übernimmt die `cluster_id` des primären Masters automatisch, wenn dieser zusätzliche Master noch keine lokalen Cluster-Oplog-Einträge hat.
5. Der neue Master registriert sich standardmäßig als **Outbound Only**. Stelle ihn nur dann auf **Reachable via SSH**, wenn andere erlaubte Peers SSH zurück zu ihm initiieren sollen.
6. Wenn der Master versehentlich zuerst gebootstrappt wurde, aktiviere die Recovery-Option. PBGui archiviert den bisherigen lokalen Cluster-State unter `data/cluster/archives/` und joint danach den Cluster des primären Masters.

### 4. VPS-Runner aktualisieren

1. Aktualisiere jeden VPS-Runner über **VPS Manager -> Update PBGui**. Das synchronisiert PBCluster-Service-Dateien und startet PBCluster, PBRun und PBCoinData neu, sofern diese Services konfiguriert sind.
2. Wenn der VPS Manager eine Systemd-Migration anzeigt, führe **Systemd Migration Preview** und danach **Apply** aus.
3. Führe danach **Cleanup VPS** aus, um alte PBRemote/rclone-Reste zu entfernen.
4. Auf reinen VPS-Runnern wird `pbgui-api.service` nicht benötigt und `PBApiServer.py` sollte dort nicht laufen.
5. Wenn du einen Runner manuell per `git pull` aktualisierst, starte PBCluster danach mit `systemctl --user restart pbgui-pbcluster.service` neu.

### 5. VPS-Nodes joinen

1. Öffne die VPS in **System -> VPS Manager**. Wenn sie nach dem Setup nicht automatisch registriert wurde, klicke **Add to Cluster**. Das schreibt nur lokale Cluster-Metadaten; es verbindet nicht per SSH zur VPS und joint sie nicht.
2. Öffne **System -> Cluster Sync -> Nodes**.
3. Öffne beim VPS-Node **Edit**, stelle **Sync Mode** auf **Reachable via SSH**, prüfe SSH Host/User/Port und **Remote PBGui Dir**, und speichere.
4. Klicke **Probe Active Nodes** und warte, bis der Node erreichbar ist und **No Identity** meldet.
5. Nutze **Join**. Join schreibt die Cluster-Identität, synchronisiert Cluster-Daten, materialisiert V7-Configs/API-Keys und startet PBRun danach wieder, wenn alles passt. Bei VPS-Runnern stoppt Join PBRun währenddessen automatisch; laufende passivbot-Prozesse bleiben unangetastet.
6. Editiere den lokalen Master-Node, der mit dieser VPS synchronisieren soll, und füge die VPS zu dessen Sync Peers hinzu.
7. Nutze beim VPS-Node **Install Key** oder nach mehreren aktualisierten Nodes bzw. Peer-Listen-Änderungen **Repair All SSH**.
8. Wenn PBGui während Key-Installation oder Repair ein SSH-Passwort abfragt, gib das Passwort für den genannten Node ein. Es wird nur für diesen Request verwendet und nicht gespeichert.
9. Klicke erneut **Probe Active Nodes**. **Login Key** sollte nach einem PBCluster-Sync-Durchlauf **Installed** anzeigen. **Skipped** bedeutet, dass der Node noch nicht in der Outbound-Sync-Peer-Liste des lokalen Masters liegt; es bedeutet nicht, dass Join fehlgeschlagen ist.

### 6. Ergebnis prüfen

1. Öffne **PBv7 -> Run** und den **VPS Manager**.
2. Wenn Bots als blockiert angezeigt werden, korrigiere Zuweisung oder Config in PBGui.
3. Wenn Join meldet, dass automatische Sync/Materialisierung Aufmerksamkeit braucht, öffne beim Node **Preview** und führe den dort vorgeschlagenen Schritt aus.

---

## Fertig

- PBRemote wird nicht mehr verwendet.
- API-Keys und V7-Configs werden über Cluster Sync materialisiert.
- `data/cmd/status_v7.json` wird nicht mehr erstellt, gelesen oder beachtet.
- PBCluster läuft auf Sync-Nodes; `pbgui-api.service` läuft nur auf Mastern, die PBGui-UI/API bereitstellen.
