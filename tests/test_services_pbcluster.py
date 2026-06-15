"""Regression tests for local PBCluster service integration."""

from pathlib import Path

import api.services as services


def test_local_services_registry_includes_pbcluster() -> None:
    """Local Services API exposes PBCluster with its systemd unit and PID file."""

    assert "pbcluster" in services._SERVICES
    assert services._SYSTEMD_SERVICE_UNITS["pbcluster"] == "pbgui-pbcluster.service"
    assert services._SERVICE_SCRIPT_NAMES["pbcluster"] == "PBCluster.py"
    assert services._SERVICE_PID_FILES["pbcluster"] == "pbcluster.pid"
    assert "pbcluster" in services._MIGRATION_DEFAULT_SERVICES
    assert "pbcluster" in services._MIGRATION_LEGACY_STOP_SERVICES


def test_local_services_ui_includes_pbcluster() -> None:
    """Services page renders a PBCluster card/panel and log viewer target."""

    source = Path("frontend/services_monitor.html").read_text(encoding="utf-8")

    assert "data-panel=\"pbcluster\"" in source
    assert "id=\"panel-pbcluster\"" in source
    assert "id=\"log-pbcluster\"" in source
    assert "id: 'pbcluster'" in source
    assert "PBCluster.log" in source
