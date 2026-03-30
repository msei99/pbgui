# PBRemote Service

PBRemote ist ein Hintergrunddienst, der Ihre PBGui-Instanzen und Konfigurationen über mehrere Server hinweg mithilfe eines Cloud-Speicher-Buckets (z. B. Synology C2, AWS S3 oder ein anderer S3-kompatibler Speicher) synchronisiert.

## Was macht PBRemote?

- **Instanz-Synchronisation:** Synchronisiert Ihre konfigurierten Bot-Instanzen (V7, PB6 Multi, PB6 Single) und deren Konfigurationen mit einem zentralen S3-Bucket via `rclone`.
- **Multi-Server-Verwaltung:** Ermöglicht die Verwaltung von Bots auf verschiedenen VPS oder lokalen Maschinen über eine einzige PBGui-Oberfläche. Unterstützt Master/Slave-Architektur (einstellbar in `pbgui.ini`).
- **Alive-Heartbeat:** Alle 60 Sekunden veröffentlicht jeder Server eine komprimierte Heartbeat-Datei im Bucket mit System-Metriken (Speicher, Swap, Disk, CPU), Software-Versionen (PBGui, PB6, PB7) und Monitor-Daten pro Instanz. Stündlich werden auch OS-Upgrades und Reboot-Status geprüft.
- **API-Key-Sync:** Jeder Server bettet den MD5-Hash seiner `api-keys.json` in den Heartbeat ein. Unterscheiden sich die Hashes zwischen Servern, verteilt PBRemote automatisch die aktualisierten Keys (mit Zeitstempel-Backups der alten Keys).
- **Befehlsweiterleitung:** Wenn Sie eine Instanz auf einem Remote-Server starten, stoppen oder bearbeiten, sendet PBRemote diese Befehle über den Bucket an den Zielserver. PBRun nimmt die Befehle aus `data/cmd/` entgegen.
- **Status-Updates:** Lädt Peer-Status-Dateien herunter und leitet sie an PBRun weiter, der Instanzen nach Bedarf installiert, aktualisiert oder entfernt.

## Konfiguration

Um PBRemote zu nutzen, müssen Sie einen S3-kompatiblen Bucket konfigurieren. PBGui verwendet im Hintergrund `rclone`, um die Synchronisation durchzuführen.

1. **rclone installieren:** Falls noch nicht installiert, gehen Sie zum **VPS Manager**, wählen Sie Ihr lokales System aus und installieren Sie `rclone`.
2. **Bucket hinzufügen:** Öffne den PBRemote **Settings**-Tab und klicke auf **+ Add**.
3. **Bucket-Details:**
   - **Bucket name:** Der Name Ihres Buckets (z. B. `my-pbgui-sync-bucket`).
   - **Region:** Die Region Ihres Buckets (z. B. `eu-central-1`).
   - **Endpoint:** Die S3-Endpunkt-URL (z. B. `https://eu-central-1.s3.synologyc2.net`).
   - **Access Key ID:** Ihr S3-Zugangsschlüssel.
   - **Secret Access Key:** Ihr geheimer S3-Schlüssel.
4. **Verbindung testen:** Klicke auf **🔌 Test**, um die Einstellungen zu überprüfen.
5. **Speichern:** Klicke auf **💾 Save**, um die Bucket-Konfiguration zu speichern.

## Nutzung

Sobald konfiguriert und gestartet, synchronisiert PBRemote die Daten automatisch im Hintergrund. Klicke auf die PBRemote-Kachel in der Services-Übersicht, um das Detail-Panel mit drei Tabs zu öffnen:

- **Log**: Live PBRemote-Log-Viewer zur Fehlersuche bei Synchronisationsproblemen
- **Info**: Remote-Server-Status, API-Sync-Status und Instanz-Übersicht pro Server (mit Systemressourcen-Balken für Speicher, Swap, Disk, CPU)
- **Settings**: Bucket-Konfiguration und Monitor-Einstellungen (Warn-/Fehlerschwellen für Server, V7, Multi und Single-Instanzen)
