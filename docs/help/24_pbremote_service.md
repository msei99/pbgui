# PBRemote Service

PBRemote is a background service that synchronizes your PBGui instances and configurations across multiple servers using a cloud storage bucket (e.g., Synology C2, AWS S3, or any S3-compatible storage).

## What does PBRemote do?

- **Instance Synchronization:** Syncs your configured bot instances (V7, PB6 Multi, PB6 Single) and their configs to a central S3 bucket via `rclone`.
- **Multi-Server Management:** Allows you to manage bots running on different VPS or local machines from a single PBGui interface. Supports master/slave architecture (set in `pbgui.ini`).
- **Alive Heartbeat:** Every 60 seconds, each server publishes a gzipped heartbeat file to the bucket containing system metrics (memory, swap, disk, CPU), software versions (PBGui, PB6, PB7), and per-instance monitor data. Every hour, it also checks for OS upgrades and reboot status.
- **API Key Sync:** Each server embeds the MD5 hash of its `api-keys.json` in the heartbeat. If hashes differ across servers, PBRemote automatically distributes the updated keys (with timestamped backups of the old keys).
- **Command Routing:** When you start, stop, or edit an instance on a remote server, PBRemote sends these commands through the bucket to the target server. PBRun picks up the commands from `data/cmd/`.
- **Status Updates:** Downloads peer status files and dispatches them to PBRun, which installs, updates, or removes instances as needed.

## Configuration

To use PBRemote, you need to configure an S3-compatible bucket. PBGui uses `rclone` under the hood to handle the synchronization.

1. **Install rclone:** If not already installed, go to the **VPS Manager**, select your local system, and install `rclone`.
2. **Add Bucket:** Open the PBRemote **Settings** tab and click the **+ Add** button.
3. **Bucket Details:**
   - **Bucket name:** The name of your bucket (e.g., `my-pbgui-sync-bucket`).
   - **Region:** The region of your bucket (e.g., `eu-central-1`).
   - **Endpoint:** The S3 endpoint URL (e.g., `https://eu-central-1.s3.synologyc2.net`).
   - **Access Key ID:** Your S3 access key.
   - **Secret Access Key:** Your S3 secret key.
4. **Test Connection:** Click **🔌 Test** to verify your settings.
5. **Save:** Click **💾 Save** to store the bucket configuration.

## Usage

Once configured and running, PBRemote will automatically sync data in the background. Click the PBRemote card on the Services overview to open the detail panel with three tabs:

- **Log**: Live PBRemote log viewer for troubleshooting synchronization issues
- **Info**: Remote server status, API sync status, and instance overview per server (with system resource bars for memory, swap, disk, CPU)
- **Settings**: Bucket configuration and Monitor Settings (warning/error thresholds for Server, V7, Multi, and Single instances)
