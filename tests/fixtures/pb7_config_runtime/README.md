# Offline PB7 configuration fixture

Unmodified Passivbot configuration code from revision `befaa9b7aa89e00ee55704221b39621ad700ac36`.
Archive SHA-256: `edbdb1027a2b5f87920556658c268f7d1198e4a30b4ad5e78e6e9e88eacce9ff`.

Contains only tracked `src/config/`, `src/pure_funcs.py`, `src/utils.py`,
`src/custom_endpoint_overrides.py`, `src/config_utils.py`, `src/analysis_visibility.py`, the optimization bounds/adapter modules, and `LICENSE`.
The upstream license is included. No credentials, bot configurations, runtime data,
or native extensions are copied. Extracted under pytest temporary directories;
executed in a separate network-disabled Python process by the explicit
`pb7_config_runtime` fixture. No local bot installation is required.

Update this pinned snapshot deliberately when the supported schema contract
changes; preserve the license and record the revision and checksum.
