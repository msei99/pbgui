# Cluster-Mode-Migration

Diese Anleitung beschreibt die notwendigen Schritte, um ein bestehendes PBGui-Setup von PBRemote/API-Sync/V7-SSH-Sync auf Cluster Sync umzustellen.

PBRemote wird nicht mehr benötigt und beim Upgrade entfernt. Cluster Sync übernimmt den Sync von V7-Configs und API-Keys.

Cluster Sync ersetzt die alten Sync-Wege. PBRun wird nur auf Hosts benötigt, die Bots ausführen. Ein reiner Master braucht PBApiServer und PBCluster, aber keinen laufenden PBRun.

---

## Schritte

### 1. Primären Master aktualisieren

1. Aktualisiere PBGui auf dem Master, den du normalerweise für die UI verwendest.
2. Starte `pbgui-api.service` neu, wenn PBGui den Restart-Hinweis zeigt.
3. Wenn dieser Master keine Bots ausführt, kann `PBRun` gestoppt bleiben.

### 2. Cluster Sync bootstrappen

1. Öffne **System -> Cluster Sync**.
2. Starte **Bootstrap Preview**.
3. Wenn die Vorschau die erwarteten lokalen V7-Configs und VPS-Hosts zeigt, führe **Bootstrap Apply** aus.

### 3. Zusätzliche Master joinen

1. Füge auf jedem zusätzlichen Master den primären Master im VPS Manager hinzu, falls er dort noch nicht bekannt ist, oder trage seine SSH-Daten direkt im Join-Formular ein.
2. Öffne **System -> Cluster Sync** auf dem zusätzlichen Master.
3. Nutze **Join Existing Cluster** mit dem VPS-Monitor-Hostnamen und den SSH-Daten des primären Masters. Wenn die Cluster-SSH-Keys noch nicht installiert sind, fragt PBGui das SSH-Passwort per Prompt ab und verwendet es nur für diesen Request, ohne es zu speichern.
4. PBGui übernimmt die `cluster_id` des primären Masters automatisch, wenn dieser zusätzliche Master noch keine lokalen Cluster-Oplog-Einträge hat.
5. Wenn der Master versehentlich zuerst gebootstrappt wurde, aktiviere die Recovery-Option. PBGui archiviert den bisherigen lokalen Cluster-State unter `data/cluster/archives/` und joint danach den Cluster des primären Masters.

### 4. VPS-Runner aktualisieren

1. Aktualisiere jeden VPS-Runner über **VPS Manager -> Update PBGui**.
2. Wenn der VPS Manager eine Systemd-Migration anzeigt, führe **Systemd Migration Preview** und danach **Apply** aus.
3. Führe danach **Cleanup VPS** aus, um alte PBRemote/rclone-Reste zu entfernen.
4. Auf reinen VPS-Runnern wird `pbgui-api.service` nicht benötigt.

### 5. VPS-Nodes joinen

1. Öffne **System -> Cluster Sync**.
2. Wenn ein VPS-Node **No Identity** zeigt, nutze **Join**.
3. **Join** schreibt die Cluster-Identität, synchronisiert Cluster-Daten, materialisiert V7-Configs/API-Keys und startet PBRun danach wieder, wenn alles passt.
4. Bei VPS-Runnern stoppt **Join** PBRun währenddessen automatisch. Die laufenden passivbot-Prozesse bleiben unangetastet.

### 6. Ergebnis prüfen

1. Öffne **PBv7 -> Run** und den **VPS Manager**.
2. Wenn Bots als blockiert angezeigt werden, korrigiere Zuweisung oder Config in PBGui.
3. Wenn Join meldet, dass automatische Sync/Materialisierung Aufmerksamkeit braucht, öffne beim Node **Preview** und führe den dort vorgeschlagenen Schritt aus.

---

## Fertig

- PBRemote wird nicht mehr verwendet.
- API-Keys und V7-Configs werden über Cluster Sync materialisiert.
- `data/cmd/status_v7.json` wird nicht mehr erstellt, gelesen oder beachtet.
