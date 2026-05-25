# PB7 Integration Inventory

This developer note tracks the PB7/PBGui boundary so PB7 upgrades can be checked in one place.

## Rules

- FastAPI code should use `api.pb7_bridge` for PB7 schema/metadata imports.
- Config load/save must use `pb7_config.py` (`load_pb7_config`, `prepare_pb7_config_dict`, `save_pb7_config`).
- Do not import `Config.py` from FastAPI/pure modules because it imports Streamlit.
- New direct PB7 module imports should be wrapped in `api.pb7_bridge` unless they are process launches or isolated OHLCV tooling.

## Current PB7 Touchpoints

| Area | PBGui File | PB7 Contract |
|---|---|---|
| Config pipeline | `pb7_config.py` | `config.load.load_prepared_config`, `config_utils.strip_config_metadata` |
| Schema/template metadata | `api/pb7_bridge.py` | `config.schema.get_template_config` |
| Override metadata | `api/pb7_bridge.py` | `config.overrides.get_allowed_modifications` |
| Coerce options | `api/pb7_bridge.py` | `config.coerce.HSL_SIGNAL_MODES`, `PYMOO_ALGORITHMS`, `PYMOO_REF_DIR_METHODS` |
| Optimize metric metadata | `api/pb7_bridge.py` | `config.metrics.CURRENCY_METRICS`, `SHARED_METRICS` |
| Optimize limits/scoring metadata | `api/pb7_bridge.py` | `config.limits.SUPPORTED_LIMIT_STATS`, `config.scoring.OBJECTIVE_GOALS`, `default_objective_goal` |
| Backtest execution | `api/backtest_v7.py` | PB7 `src/backtest.py` launched with configured `pb7venv()` |
| Optimize execution | `api/optimize_v7.py` | PB7 `src/optimize.py` launched with configured `pb7venv()` |
| Pareto Dash | `api/optimize_v7.py` | PB7 `src/tools/pareto_dash.py` |
| OHLCV readiness/preload | `api/pb7_ohlcv_tools.py` | PB7 `config.access`, `hlcv_preparation`, `ohlcv_catalog`, `ohlcv_planner`, `procedures`, `utils`, `warmup_utils`, `ohlcv_download.py` |
| Rust helpers | `pbgui_purefunc.py` | `passivbot_rust` import |

## Upgrade Checklist

Run these checks after changing PB7 versions:

- `python -m pytest tests/config/test_pb7_config_run_v7.py`
- `python -m pytest tests/test_pb7_bridge_contract.py`
- `python -m pytest tests/test_optimize_v7_api.py`
- `python -m pytest tests/test_backtest_v7_api.py`
- `python -m pytest tests/ui/test_v7_optimize_backend_logic.py`

Manual smoke checks:

- Open PBv7 Backtest and Optimize pages.
- Load, save, and reload one config in each editor.
- Open Optimize Scoring, Limits, and Bounds sections and verify options render.
- Run OHLCV readiness on a small config.
- Open Pareto Explorer and create an Optimize preset from a selected config.
