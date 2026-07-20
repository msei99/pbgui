# Welcome & Login

The **Welcome** page is the standalone entry point for PBGui. It gives you the first login, basic setup, and the minimum runtime checks before you move into the rest of the application.

## What the page is for

Use the Welcome page to:

- log in with the current PBGui password
- change the password
- configure the local PBv7 and PBv8 paths and interpreters
- choose whether this machine acts as **Master** or **Slave**
- confirm that the API server can read the current runtime setup

## Overview section

The default **Overview** section summarizes the current local state:

- **Session**: whether you are authenticated or still a guest
- **PB7**: whether the configured PBv7 runtime looks usable
- **PB8**: whether source, V8 config schema, Python, CLI, and the compiled Rust extension are ready
- **Identity**: the current host role and configured bot name
- **Runtime Status**: detailed readiness checks from the backend
- **Login security**: active login blocks and retained brute-force lockout history

This section is intended as a quick sanity check after first startup, password changes, or path updates.

The issue list also shows a persistent security warning when PBGui listens on all interfaces while still using the known legacy default password. PBGui cannot inspect external NAT or firewall rules, so either verify that the API port is limited to VPN or trusted networks, or set an individual password. New installer runs generate an individual password automatically, and remote installs expose the PBGui port only to the configured OpenVPN network by default.

When repeated failed logins trigger a temporary block, the issue list shows a warning with the last direct client address and event time. **Acknowledge** hides that warning globally while keeping the Login security status and retained history visible. A newer lockout automatically raises the warning again.

When authentication is intentionally disabled, every standalone page shows a persistent red **NO LOGIN** indicator. PBGui cannot inspect external firewall rules: anyone who can reach the configured API address has full administrative access.

## Setup section

The **Setup** section edits the values that PBGui reads from `pbgui.ini`.

Important fields:

- **Passivbot V7 path**: root directory of the local PBv7 checkout
- **Passivbot V7 python interpreter**: full path to the Python binary inside the PBv7 virtual environment
- **Passivbot V8 path**: root directory of the local PBv8 checkout
- **Passivbot V8 python interpreter**: full path to the Python binary inside `venv_pb8`
- **Bot name**: local bot identity used by PBGui
- **Role**: choose **Master** when this host manages remote VPS systems, otherwise **Slave**

Use the **Browse** buttons to pick directories and the Python interpreter from the server filesystem.

The role is picked up by fresh operation cycles after saving. Changes to the
PB7 path, PB7 interpreter, or bot name require the PBRun service to restart;
the save confirmation reports the required apply timing.
PB8 path changes apply to the next PB8 operation and do not require an API or
PBRun restart. PB8 is expected on Masters and remains optional on unconfigured
Slaves. Welcome performs static artifact checks; the installer additionally
executes the PB8 CLI and validates the Rust import during installation.

## Password section

The **Password** action in the left sidebar opens the password form.

Use it to:

- replace the current login password
- deliberately enter No-Login mode with **Disable Authentication** and its security confirmation
- enable password authentication again by entering a new password

An empty password by itself is rejected. Every password or authentication-mode change revokes existing sessions and issues a new session to the current browser. You must be authenticated before changing this setting.

## Typical first-time workflow

1. Open the Welcome page.
2. Log in with the current PBGui password.
3. Set the **Passivbot V7 path**.
4. Set the **Passivbot V7 python interpreter**.
5. Choose the correct **Role**.
6. On a Master, set the PBv8 path and interpreter or install PB8 with the Master Installer.
7. Save the setup.
8. Re-check **Runtime Status** until the required runtimes are ready.

## Troubleshooting

- **PB7 blocked**: the configured PBv7 path or interpreter is missing or invalid
- **PB8 blocked/partial**: PBv8 source, schema, interpreter, CLI, or Rust extension is missing or invalid
- **Save Setup stays disabled**: log in first
- **Browse does not work**: check authentication and server path permissions
- **You only want to change the password**: use the **Password** sidebar action instead of editing setup fields
