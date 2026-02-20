# PBRemote Service

PBRemote ist ein Hintergrunddienst, der Ihre PBGui-Instanzen und Konfigurationen über mehrere Server hinweg mithilfe eines Cloud-Speicher-Buckets (z. B. Synology C2, AWS S3 oder ein anderer S3-kompatibler Speicher) synchronisiert.

## Was macht PBRemote?

- **Instanz-Synchronisation:** Synchronisiert Ihre konfigurierten Bot-Instanzen (deren Einstellungen, Status und API-Schlüssel) mit einem zentralen Bucket.
- **Multi-Server-Verwaltung:** Ermöglicht die Verwaltung von Bots, die auf verschiedenen VPS oder lokalen Maschinen laufen, über eine einzige PBGui-Oberfläche.
- **Befehlsweiterleitung:** Wenn Sie eine Instanz auf einem Remote-Server über Ihre lokale PBGui starten, stoppen oder bearbeiten, sendet PBRemote diese Befehle über den Bucket an den Zielserver.
- **Status-Updates:** Ruft den aktuellen Status (laufend, gestoppt, Fehler) von Remote-Instanzen ab, damit Sie diese lokal überwachen können.

## Konfiguration

Um PBRemote zu nutzen, müssen Sie einen S3-kompatiblen Bucket konfigurieren. PBGui verwendet im Hintergrund `rclone`, um die Synchronisation durchzuführen.

1. **rclone installieren:** Falls noch nicht installiert, gehen Sie zum **VPS Manager**, wählen Sie Ihr lokales System aus und installieren Sie `rclone`.
2. **Bucket hinzufügen:** Klicken Sie auf der PBRemote-Detailseite auf die Schaltfläche **Add bucket**.
3. **Bucket-Details:**
   - **Bucket name:** Der Name Ihres Buckets (z. B. `my-pbgui-sync-bucket`).
   - **Region:** Die Region Ihres Buckets (z. B. `eu-central-1`).
   - **Endpoint:** Die S3-Endpunkt-URL (z. B. `https://eu-central-1.s3.synologyc2.net`).
   - **Access Key ID:** Ihr S3-Zugangsschlüssel.
   - **Secret Access Key:** Ihr geheimer S3-Schlüssel.
4. **Verbindung testen:** Klicken Sie auf **Test Connection**, um Ihre Einstellungen zu überprüfen.
5. **Speichern:** Klicken Sie auf das Speichern-Symbol in der Seitenleiste, um die Konfiguration zu speichern.

## Nutzung

Sobald konfiguriert und gestartet, synchronisiert PBRemote die Daten automatisch im Hintergrund. Sie können den Status der Remote-Server und deren Instanzen direkt auf der PBRemote-Detailseite einsehen.

- **API-Sync-Status:** Die Seite warnt Sie, wenn API-Schlüssel nicht mit den Remote-Servern synchronisiert sind.
- **Remote-Server:** Wählen Sie einen Remote-Server aus der Seitenleiste aus, um dessen Instanzen und deren aktuellen Status anzuzeigen.
- **Logs:** Verwenden Sie den gefilterten Log-Viewer am Ende der Seite, um eventuelle Synchronisationsprobleme zu beheben.
