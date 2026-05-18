# Unreleased

- Removed local Linux upgrade/reboot probing from `PBRun`; the VPS Manager now reads local maintenance state directly inside `vps_manager_service.py` instead of via the bot runtime manager.
- Fixed the new PBGui/PB7 release providers and remote host-meta version probe so VPS Manager overview rows show real PBGui/PB7 versions and branch status again instead of `N/A`/`unknown`.
- Fixed VPS Manager monitor-state fallback so persisted remote `host_meta` is merged into partial live monitor snapshots instead of being dropped, which restored missing remote PBGui/PB7 overview fields on the running API server.
