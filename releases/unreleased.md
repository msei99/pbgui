# Unreleased

- Improved VPS Manager version status cards to show the available target version/commit when a PBGui or PB7 update is available.
- Fixed VPS PBGui/PB7 update status targets so remote hosts show the available target version instead of repeating the currently installed version.
- Fixed the shared restart button so clicks show the restart-blocked explanation instead of being ignored while master/VPS tasks are still active.
- Fixed master update completion detection so finished PBGui/PB7/Linux update playbooks are recognized from the Ansible recap immediately and no longer keep API restart blocked for extra polling cycles.
