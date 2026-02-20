# PBRemote Service

PBRemote is a background service that synchronizes your PBGui instances and configurations across multiple servers using a cloud storage bucket (e.g., Synology C2, AWS S3, or any S3-compatible storage).

## What does PBRemote do?

- **Instance Synchronization:** It syncs your configured bot instances (their settings, state, and API keys) to a central bucket.
- **Multi-Server Management:** Allows you to manage bots running on different VPS or local machines from a single PBGui interface.
- **Command Routing:** When you start, stop, or edit an instance on a remote server via your local PBGui, PBRemote sends these commands through the bucket to the target server.
- **Status Updates:** It fetches the current status (running, stopped, errors) of remote instances so you can monitor them locally.

## Configuration

To use PBRemote, you need to configure an S3-compatible bucket. PBGui uses `rclone` under the hood to handle the synchronization.

1. **Install rclone:** If not already installed, go to the **VPS Manager**, select your local system, and install `rclone`.
2. **Add Bucket:** Click the **Add bucket** button on the PBRemote details page.
3. **Bucket Details:**
   - **Bucket name:** The name of your bucket (e.g., `my-pbgui-sync-bucket`).
   - **Region:** The region of your bucket (e.g., `eu-central-1`).
   - **Endpoint:** The S3 endpoint URL (e.g., `https://eu-central-1.s3.synologyc2.net`).
   - **Access Key ID:** Your S3 access key.
   - **Secret Access Key:** Your S3 secret key.
4. **Test Connection:** Click **Test Connection** to verify your settings.
5. **Save:** Click the save icon in the sidebar to store the configuration.

## Usage

Once configured and running, PBRemote will automatically sync data in the background. You can view the status of remote servers and their instances directly on the PBRemote details page.

- **API Sync Status:** The page will warn you if any API keys are not in sync with the remote servers.
- **Remote Servers:** Select a remote server from the sidebar to view its instances and their current status.
- **Logs:** Use the filtered log viewer at the bottom of the page to troubleshoot any synchronization issues.
