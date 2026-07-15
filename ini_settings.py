"""Value-free apply timing metadata for PBGui-editable INI settings."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable, Literal


ApplyTiming = Literal[
    "immediate",
    "next_cycle",
    "next_operation",
    "next_log_write",
    "api_restart",
    "service_restart",
]


@dataclass(frozen=True)
class SettingApplyMetadata:
    """Runtime ownership and apply behavior without configuration values."""

    section: str
    key: str
    owner: str
    timing: ApplyTiming
    restart_required: bool
    message: str


def _entries(section: str, keys: Iterable[str], owner: str, timing: ApplyTiming, message: str) -> list[SettingApplyMetadata]:
    restart_required = timing in {"api_restart", "service_restart"}
    return [SettingApplyMetadata(section, key, owner, timing, restart_required, message) for key in keys]


_REGISTRY_ENTRIES = [
    *_entries("api_server", ("host", "port", "cors_origins", "ssh_log_level"), "PBApiServer", "api_restart", "API restart required"),
    *_entries("main", ("pbname", "pb7dir", "pb7venv"), "PBRun", "service_restart", "Service restart required"),
    *_entries("vps_monitor", ("auto_restart", "enabled_hosts", "debug_logging"), "VPSMonitor", "immediate", "Applied immediately"),
    *_entries("vps_monitor_alerts", ("offline_gui", "service_gui", "system_gui", "instance_gui", "ssh_lost_telegram", "ssh_recovered_telegram", "service_down_telegram", "service_restart_started_telegram", "service_recovered_telegram", "system_problem_telegram", "system_recovered_telegram", "instance_problem_telegram", "instance_recovered_telegram"), "VPSMonitor", "immediate", "Applied immediately"),
    *_entries("main", ("telegram_token", "telegram_chat_id"), "VPSMonitor", "immediate", "Applied immediately"),
    *_entries("monitor", ("mem_warning_server", "mem_error_server", "swap_warning_server", "swap_error_server", "disk_warning_server", "disk_error_server", "cpu_warning_server", "cpu_error_server", "mem_warning_v7", "mem_error_v7", "swap_warning_v7", "swap_error_v7", "cpu_warning_v7", "cpu_error_v7", "error_warning_v7", "error_error_v7", "traceback_warning_v7", "traceback_error_v7"), "VPSMonitor", "immediate", "Applied immediately"),
    *_entries("vps_monitor_ui", ("compact",), "VPSMonitor", "immediate", "Applied immediately"),
    *_entries("pbdata", ("fetch_users", "trades_users", "log_level", "ws_max", "pollers_delay_seconds", "poll_interval_combined_seconds", "poll_interval_balance_seconds", "poll_interval_positions_seconds", "poll_interval_orders_seconds", "poll_interval_history_seconds", "poll_interval_executions_seconds", "shared_rest_user_pause_seconds", "shared_rest_pause_by_exchange_json", "latest_1m_interval_seconds", "latest_1m_coin_pause_seconds", "latest_1m_api_timeout_seconds", "latest_1m_min_lookback_days", "latest_1m_max_lookback_days", "price_watch_timeout", "rest_semaphore_acquire_timeout"), "PBData", "next_cycle", "Applies next cycle"),
    *_entries("coinmarketcap", ("fetch_limit", "fetch_interval", "metadata_interval", "mapping_interval"), "PBCoinData", "next_cycle", "Applies next cycle"),
    *_entries("binance_data", ("latest_1m_interval_seconds", "latest_1m_coin_pause_seconds", "latest_1m_api_timeout_seconds", "latest_1m_min_lookback_days", "latest_1m_max_lookback_days"), "PBData", "next_cycle", "Applies next cycle"),
    *_entries("bybit_data", ("latest_1m_interval_seconds", "latest_1m_coin_pause_seconds", "latest_1m_api_timeout_seconds", "latest_1m_min_lookback_days", "latest_1m_max_lookback_days"), "PBData", "next_cycle", "Applies next cycle"),
    *_entries("bitget_data", ("latest_1m_interval_seconds", "latest_1m_coin_pause_seconds", "latest_1m_api_timeout_seconds", "latest_1m_min_lookback_days", "latest_1m_max_lookback_days"), "PBData", "next_cycle", "Applies next cycle"),
    *_entries("okx_data", ("latest_1m_interval_seconds", "latest_1m_coin_pause_seconds", "latest_1m_api_timeout_seconds", "latest_1m_min_lookback_days", "latest_1m_max_lookback_days"), "PBData", "next_cycle", "Applies next cycle"),
    *_entries("logging", ("rotate_default_max_bytes", "rotate_default_backup_count", "rotate_max_bytes", "rotate_backup_count"), "LoggingHelpers", "next_log_write", "Applies on next log write"),
    *_entries("pareto", ("load_strategy", "max_configs"), "ParetoExplorer", "next_operation", "Applies to next operation"),
    *_entries("market_data", ("hl_aws_profile",), "TaskWorker", "next_operation", "Applies to next operation"),
    *_entries("market_data", ("hl_l2book_scan_timeout_s", "hl_l2book_scan_workers"), "TaskWorker", "next_cycle", "Applies next cycle"),
    *_entries("market_data", ("l2book_archive_enabled", "l2book_archive_dir"), "MarketData", "next_operation", "Applies to next operation"),
    *_entries("config_archive", ("my_archive", "my_archive_path", "my_archive_username", "my_archive_email", "my_archive_access_token", "auto_pull_interval"), "BacktestV7", "next_operation", "Applies to next operation"),
]

SETTING_APPLY_REGISTRY = {(item.section, item.key): item for item in _REGISTRY_ENTRIES}

APPLY_GROUPS = {
    "api_server": (("api_server", "host"), ("api_server", "port")),
    "api_server_live": tuple(key for key in SETTING_APPLY_REGISTRY if key[0] in {"vps_monitor", "vps_monitor_alerts", "monitor", "vps_monitor_ui"} or key in {("main", "telegram_token"), ("main", "telegram_chat_id")}),
    "api_server_full": tuple(key for key in SETTING_APPLY_REGISTRY if key[0] in {"api_server", "vps_monitor", "vps_monitor_alerts", "monitor", "vps_monitor_ui"} or key in {("main", "telegram_token"), ("main", "telegram_chat_id")}),
    "pbdata": tuple(key for key in SETTING_APPLY_REGISTRY if key[0] == "pbdata"),
    "pbcoindata": tuple(key for key in SETTING_APPLY_REGISTRY if key[0] == "coinmarketcap"),
    "logging_rotation": tuple(key for key in SETTING_APPLY_REGISTRY if key[0] == "logging"),
    "config_archive": tuple(key for key in SETTING_APPLY_REGISTRY if key[0] == "config_archive"),
    "market_data": tuple(
        key for key in SETTING_APPLY_REGISTRY
        if key[0].endswith("_data")
        or key[0] == "market_data"
        or (key[0] == "pbdata" and key[1].startswith("latest_1m_"))
    ),
}

_TIMING_PRIORITY = {"immediate": 0, "next_log_write": 1, "next_operation": 2, "next_cycle": 3, "service_restart": 4, "api_restart": 5}


def apply_metadata_for(keys: Iterable[tuple[str, str]]) -> dict[str, object]:
    """Return apply metadata for an exact set of registry keys."""

    keys = set(keys)
    settings = [SETTING_APPLY_REGISTRY[key] for key in sorted(keys)]
    timing = max((item.timing for item in settings), key=_TIMING_PRIORITY.__getitem__)
    messages = {
        "immediate": "Applied immediately",
        "next_cycle": "Applies next cycle",
        "next_operation": "Applies to next operation",
        "next_log_write": "Applies on next log write",
        "api_restart": "API restart required",
        "service_restart": "Service restart required",
    }
    return {
        "version": 1,
        "timing": timing,
        "restart_required": timing in {"api_restart", "service_restart"},
        "message": messages[timing],
        "owners": sorted({item.owner for item in settings}),
        "settings": [asdict(item) for item in settings],
    }


def apply_metadata(*groups: str) -> dict[str, object]:
    """Return a stable additive API payload for one or more setting groups."""

    return apply_metadata_for(key for group in groups for key in APPLY_GROUPS[group])
