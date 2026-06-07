# Unreleased

- Added a detailed architecture plan for future self-hosted multi-master cluster sync without external state services.
- Added automatic SSH key installation when saving an Existing VPS import so live monitoring can be enabled without manual `ssh-copy-id` steps.
- Made V7 config sync reconcile remote `status_v7.json` when a VPS watcher starts, pull locally missing legacy instances without per-instance timestamps, and ignore stale remote manifests for delete decisions.
