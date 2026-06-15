"""
Comprehensive unit tests for PBRun.py runtime filtering behavior.

These tests focus on the most critical migration path:
- DynamicIgnore.watch() must use mapping-based CoinData.filter_mapping()

All tests are isolated and avoid live exchange/API dependencies.
"""

import sys
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


# Ensure project root is on path
ROOT_DIR = Path(__file__).parent.parent.resolve()
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from master.cluster_state import build_config_manifest, compute_config_manifest_hash, default_cluster_root, ensure_local_identity


# Load real PBCoinData module first, then inject into sys.modules so PBRun
# resolves the real CoinData class (tests/conftest may preload a lightweight mock).
pbcoindata_path = ROOT_DIR / "PBCoinData.py"
pbcoindata_spec = importlib.util.spec_from_file_location("PBCoinData", pbcoindata_path)
PBCoinData_mod = importlib.util.module_from_spec(pbcoindata_spec)
pbcoindata_spec.loader.exec_module(PBCoinData_mod)
sys.modules["PBCoinData"] = PBCoinData_mod

# Import PBRun from real module path
pbrun_path = ROOT_DIR / "PBRun.py"
pbrun_spec = importlib.util.spec_from_file_location("PBRun_real", pbrun_path)
PBRun_mod = importlib.util.module_from_spec(pbrun_spec)
pbrun_spec.loader.exec_module(PBRun_mod)

DynamicIgnore = PBRun_mod.DynamicIgnore


def _write_v7_config(instance_dir: Path, *, enabled_on: str = "test-vps", version: int = 3) -> None:
    """Write a minimal PB7 config with PBGui metadata."""

    instance_dir.mkdir(parents=True, exist_ok=True)
    payload = {"pbgui": {"enabled_on": enabled_on, "version": version}, "live": {}}
    (instance_dir / "config.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_cluster_desired(
    pbgdir: Path,
    instance_dir: Path,
    *,
    desired_state: str = "running",
    assigned_node_id: str | None = None,
    assigned_host: str | None = None,
    manifest_hash: str | None = None,
    version: str = "3",
) -> dict:
    """Create local Cluster Sync identity and desired state for one instance."""

    cluster_root = pbgdir / "data" / "cluster"
    identity = ensure_local_identity(cluster_root, pbname="test-vps", node_id=assigned_node_id)
    active_manifest_hash = manifest_hash or compute_config_manifest_hash(build_config_manifest(instance_dir))
    desired = {
        "schema_version": 1,
        "cluster_id": identity["cluster_id"],
        "generated_at": 1,
        "instances": {
            instance_dir.name: {
                "version": version,
                "desired_state": desired_state,
                "assigned_host": assigned_host or identity["node_id"],
                "config_manifest_hash": active_manifest_hash,
                "updated_by": identity["node_id"],
                "updated_at": 1,
                "conflicted": False,
            }
        },
        "tombstones": {},
    }
    (cluster_root / "desired_state.json").write_text(json.dumps(desired, indent=4), encoding="utf-8")
    return identity


def _make_cluster_runv7(pbgdir: Path, instance_name: str = "bot-a") -> PBRun_mod.RunV7:
    """Build a loaded RunV7 instance for Cluster Sync gate tests."""

    instance_dir = pbgdir / "data" / "run_v7" / instance_name
    _write_v7_config(instance_dir)
    rv7 = PBRun_mod.RunV7()
    rv7.path = str(instance_dir)
    rv7.user = instance_name
    rv7.name = "test-vps"
    rv7.pb7dir = str(pbgdir / "pb7")
    rv7.pb7venv = "/usr/bin/python3"
    rv7.pbgdir = str(pbgdir)
    assert rv7.load() is True
    return rv7


class _LegacyAccessError(RuntimeError):
    """Raised when legacy CoinData list attributes are accessed."""


class FakeCoinData:
    """Minimal CoinData double used to validate filter_mapping-based runtime behavior."""

    def __init__(self):
        self.exchange = "binance"
        self.market_cap = 50
        self.vol_mcap = 7.5
        self.only_cpt = True
        self.notices_ignore = True
        self.tags = []
        self.calls = []

    @property
    def approved_coins(self):
        raise _LegacyAccessError("Legacy approved_coins must not be used in DynamicIgnore.watch")

    @property
    def ignored_coins(self):
        raise _LegacyAccessError("Legacy ignored_coins must not be used in DynamicIgnore.watch")

    def list_symbols(self):
        raise _LegacyAccessError("Legacy list_symbols must not be used in DynamicIgnore.watch")

    def filter_mapping(
        self,
        exchange,
        market_cap_min_m,
        vol_mcap_max,
        only_cpt,
        notices_ignore,
        tags,
        active_only,
        quote_filter,
        use_cache,
    ):
        self.calls.append(
            {
                "exchange": exchange,
                "market_cap_min_m": market_cap_min_m,
                "vol_mcap_max": vol_mcap_max,
                "only_cpt": only_cpt,
                "notices_ignore": notices_ignore,
                "tags": tags,
                "active_only": active_only,
                "quote_filter": quote_filter,
                "use_cache": use_cache,
            }
        )

        # First call: availability set
        if market_cap_min_m == 0 and vol_mcap_max == float("inf"):
            return ["BTC", "ETH", "SOL"], []

        # Second call: actual filtered result
        return ["BTC", "SOL"], ["ETH"]


class FakeCoinDataIgnoredOnlyChange(FakeCoinData):
    """CoinData double where only ignored set changes across watch cycles."""

    def __init__(self):
        super().__init__()
        self._cycle = 0

    def filter_mapping(
        self,
        exchange,
        market_cap_min_m,
        vol_mcap_max,
        only_cpt,
        notices_ignore,
        tags,
        active_only,
        quote_filter,
        use_cache,
    ):
        self.calls.append(
            {
                "exchange": exchange,
                "market_cap_min_m": market_cap_min_m,
                "vol_mcap_max": vol_mcap_max,
                "only_cpt": only_cpt,
                "notices_ignore": notices_ignore,
                "tags": tags,
                "active_only": active_only,
                "quote_filter": quote_filter,
                "use_cache": use_cache,
            }
        )

        if market_cap_min_m == 0 and vol_mcap_max == float("inf"):
            # availability shrinks on second cycle, while filtered approved stays constant
            self._cycle += 1
            if self._cycle == 1:
                return ["BTC", "ETH", "SOL"], []
            return ["BTC", "ETH"], []

        # runtime filtered sets keep approved stable; ignored changes only due availability delta
        return ["BTC"], ["ETH"]


class TestDynamicIgnoreRuntime:
    """Tests for mapping-based dynamic ignore behavior in PBRun runtime."""

    def test_watch_uses_mapping_filters_and_updates_lists(self, monkeypatch):
        """watch() computes approved/ignored via filter_mapping and saves on approved changes.

        Manual _long/_short lists are preserved regardless of listing status
        (to avoid endless remove/re-add loops for delisted coins).
        """
        di = DynamicIgnore()
        di.coindata = FakeCoinData()
        di.path = "dummy"
        di.ignored_coins_long = ["SOL"]
        di.ignored_coins_short = ["DOGE"]  # not in available set but preserved as manual
        di.approved_coins_long = ["ETH"]
        di.approved_coins_short = ["XRP"]  # not in available set but preserved as manual

        save_calls = {"count": 0}

        def _save_stub():
            save_calls["count"] += 1

        monkeypatch.setattr(di, "save", _save_stub)

        changed = di.watch()

        assert changed is True, "First run should detect changes and persist dynamic lists"
        assert save_calls["count"] == 1, "save() should be called when approved list changes"
        # XRP preserved from manual approved but not in ignored → stays in approved
        assert di.approved_coins == ["BTC", "XRP"], \
            "Manual approved symbols preserved even outside available set; ignored-priority removes overlapping"
        # DOGE preserved from manual ignored even outside available set
        assert di.ignored_coins == ["DOGE", "ETH", "SOL"], \
            "Manual ignored symbols preserved regardless of listing status"
        assert len(di.coindata.calls) == 2, "watch() should call filter_mapping twice"

        availability_call = di.coindata.calls[0]
        runtime_call = di.coindata.calls[1]

        assert availability_call["active_only"] is True
        assert availability_call["market_cap_min_m"] == 0
        assert availability_call["vol_mcap_max"] == float("inf")

        assert runtime_call["active_only"] is True
        assert runtime_call["market_cap_min_m"] == 50
        assert runtime_call["vol_mcap_max"] == 7.5
        assert runtime_call["only_cpt"] is True
        assert runtime_call["notices_ignore"] is True

    def test_watch_no_change_returns_false(self, monkeypatch):
        """watch() returns False and does not save when lists are unchanged."""
        di = DynamicIgnore()
        di.coindata = FakeCoinData()
        di.path = "dummy"

        save_calls = {"count": 0}

        def _save_stub():
            save_calls["count"] += 1

        monkeypatch.setattr(di, "save", _save_stub)

        assert di.watch() is True, "Initial run should build lists"
        assert di.watch() is False, "Second run with identical data should be stable"
        assert save_calls["count"] == 1, "No additional save expected on unchanged state"

    def test_watch_ignored_only_change_triggers_save(self, monkeypatch):
        """If only ignored list changes, watch() must still save and return True."""
        di = DynamicIgnore()
        di.coindata = FakeCoinDataIgnoredOnlyChange()
        di.path = "dummy"

        save_calls = {"count": 0}

        def _save_stub():
            save_calls["count"] += 1

        monkeypatch.setattr(di, "save", _save_stub)

        assert di.watch() is True, "Initial run should save"
        assert di.approved_coins == ["BTC"]
        assert di.ignored_coins == ["ETH", "SOL"]

        assert di.watch() is True, "Ignored-only delta must still trigger save"
        assert di.approved_coins == ["BTC"], "Approved should remain unchanged"
        assert di.ignored_coins == ["ETH"], "Ignored should reflect latest filtered set"
        assert save_calls["count"] == 2

    def test_static_ignored_has_priority_over_dynamic_approved(self, tmp_path):
        """Coins in static ignored lists must remain ignored even if dynamically approved."""
        di = DynamicIgnore()
        di.path = str(tmp_path)

        # Dynamic runtime result (from filter_mapping/watch)
        di.approved_coins = ["BTC", "ETH"]
        di.ignored_coins = ["SOL"]

        # Static overrides from multi config
        di.ignored_coins_long = ["ETH"]
        di.ignored_coins_short = []
        di.approved_coins_long = []
        di.approved_coins_short = []

        di.save()

        ignored_file = tmp_path / "ignored_coins.json"
        approved_file = tmp_path / "approved_coins.json"

        ignored_saved = set(__import__("json").loads(ignored_file.read_text(encoding="utf-8")))
        approved_saved = set(__import__("json").loads(approved_file.read_text(encoding="utf-8")))

        assert "ETH" in ignored_saved, "Static ignored coin must be written to ignored_coins.json"
        assert "ETH" not in approved_saved, "Static ignored coin must not remain in approved_coins.json"

    def test_coin_in_static_approved_and_ignored_ignored_wins(self, tmp_path):
        """If a coin is configured in both static approved and static ignored, ignored must win."""
        di = DynamicIgnore()
        di.path = str(tmp_path)

        di.approved_coins = []
        di.ignored_coins = []
        di.approved_coins_long = ["BTC"]
        di.approved_coins_short = []
        di.ignored_coins_long = ["BTC"]
        di.ignored_coins_short = []

        di.save()

        ignored_file = tmp_path / "ignored_coins.json"
        approved_file = tmp_path / "approved_coins.json"

        ignored_saved = set(__import__("json").loads(ignored_file.read_text(encoding="utf-8")))
        approved_saved = set(__import__("json").loads(approved_file.read_text(encoding="utf-8")))

        assert "BTC" in ignored_saved, "Coin must stay in ignored list on conflict"
        assert "BTC" not in approved_saved, "Coin must be removed from approved list on conflict"

    def test_watch_conflict_static_approved_vs_dynamic_ignored_ignored_wins(self, monkeypatch):
        """During watch(), a coin marked ignored by filters must be removed from static approved result."""
        di = DynamicIgnore()
        di.coindata = FakeCoinData()
        di.path = "dummy"
        di.approved_coins_long = ["ETH"]

        save_calls = {"count": 0}

        def _save_stub():
            save_calls["count"] += 1

        monkeypatch.setattr(di, "save", _save_stub)

        assert di.watch() is True
        assert "ETH" in di.ignored_coins, "Dynamic ignored classification must be preserved"
        assert "ETH" not in di.approved_coins, "Ignored must have priority over static approved"
        assert save_calls["count"] == 1

    def test_watch_partition_invariant_all_available_are_classified(self, monkeypatch):
        """After watch(), approved and ignored must be disjoint and cover all available symbols."""

        class FakeCoinDataPartition(FakeCoinData):
            def filter_mapping(
                self,
                exchange,
                market_cap_min_m,
                vol_mcap_max,
                only_cpt,
                notices_ignore,
                tags,
                active_only,
                quote_filter,
                use_cache,
            ):
                if market_cap_min_m == 0 and vol_mcap_max == float("inf"):
                    return ["BTC", "ETH", "SOL", "XRP"], []
                # Missing ETH/SOL/XRP from filtered sets on purpose; safety net should put them into ignored.
                return ["BTC"], []

        di = DynamicIgnore()
        di.coindata = FakeCoinDataPartition()
        di.path = "dummy"

        save_calls = {"count": 0}

        def _save_stub():
            save_calls["count"] += 1

        monkeypatch.setattr(di, "save", _save_stub)

        assert di.watch() is True

        approved_set = set(di.approved_coins)
        ignored_set = set(di.ignored_coins)
        available_set = {"BTC", "ETH", "SOL", "XRP"}

        assert approved_set.isdisjoint(ignored_set), "approved and ignored must never overlap"
        assert approved_set | ignored_set == available_set, "every available symbol must be classified"
        assert save_calls["count"] == 1

    def test_watch_manual_symbols_outside_available_are_preserved(self, monkeypatch):
        """Manual _long/_short symbols outside available mapping universe are preserved.

        This prevents an endless remove/re-add loop when a coin is delisted but
        still referenced in the user's static ignored/approved lists.
        """

        class FakeCoinDataAvailableSubset(FakeCoinData):
            def filter_mapping(
                self,
                exchange,
                market_cap_min_m,
                vol_mcap_max,
                only_cpt,
                notices_ignore,
                tags,
                active_only,
                quote_filter,
                use_cache,
            ):
                if market_cap_min_m == 0 and vol_mcap_max == float("inf"):
                    return ["BTC", "ETH"], []
                return ["BTC"], ["ETH"]

        di = DynamicIgnore()
        di.coindata = FakeCoinDataAvailableSubset()
        di.path = "dummy"
        di.approved_coins_long = ["DOGE"]   # not in available but manual → preserved
        di.ignored_coins_short = ["XRP"]    # not in available but manual → preserved

        save_calls = {"count": 0}

        def _save_stub():
            save_calls["count"] += 1

        monkeypatch.setattr(di, "save", _save_stub)

        assert di.watch() is True
        assert "DOGE" in di.approved_coins, "Manual approved symbol preserved even outside available set"
        assert "XRP" in di.ignored_coins, "Manual ignored symbol preserved even outside available set"
        assert set(di.approved_coins) == {"BTC", "DOGE"}
        assert set(di.ignored_coins) == {"ETH", "XRP"}
        assert save_calls["count"] == 1

    def test_watch_filter_mapping_error_returns_false_and_keeps_state(self, monkeypatch):
        """watch() must fail safe on CoinData/filter errors (no state change, no save)."""

        class FakeCoinDataError(FakeCoinData):
            def filter_mapping(
                self,
                exchange,
                market_cap_min_m,
                vol_mcap_max,
                only_cpt,
                notices_ignore,
                tags,
                active_only,
                quote_filter,
                use_cache,
            ):
                raise RuntimeError("simulated filter failure")

        di = DynamicIgnore()
        di.coindata = FakeCoinDataError()
        di.path = "dummy"
        di.approved_coins = ["BTC"]
        di.ignored_coins = ["ETH"]

        save_calls = {"count": 0}

        def _save_stub():
            save_calls["count"] += 1

        monkeypatch.setattr(di, "save", _save_stub)

        assert di.watch() is False
        assert di.approved_coins == ["BTC"], "State must remain unchanged on error"
        assert di.ignored_coins == ["ETH"], "State must remain unchanged on error"
        assert save_calls["count"] == 0, "No save expected on error"




class TestDynamicIgnoreHardening:
    """Additional robustness tests for DynamicIgnore IO and input handling."""

    def test_atomic_write_json_cleans_temp_file_on_error(self, tmp_path, monkeypatch):
        """_atomic_write_json() should remove temp file if writing fails."""
        di = DynamicIgnore()
        target = tmp_path / "approved_coins.json"
        tmp_target = target.with_suffix(target.suffix + ".tmp")

        class _BrokenFile:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def write(self, *_args, **_kwargs):
                raise OSError("disk full")

        class _BrokenTmpPath:
            def __init__(self, real_path):
                self._real_path = real_path

            def open(self, *args, **kwargs):
                return _BrokenFile()

            def replace(self, *_args, **_kwargs):
                raise AssertionError("replace should not be called after write failure")

            def exists(self):
                return self._real_path.exists()

            def unlink(self, missing_ok=False):
                return self._real_path.unlink(missing_ok=missing_ok)

        original_with_suffix = Path.with_suffix

        def _with_suffix_stub(self, _suffix):
            if self == target:
                tmp_target.parent.mkdir(parents=True, exist_ok=True)
                tmp_target.write_text("partial", encoding="utf-8")
                return _BrokenTmpPath(tmp_target)
            return original_with_suffix(self, _suffix)

        monkeypatch.setattr(Path, "with_suffix", _with_suffix_stub)

        with pytest.raises(OSError, match="disk full"):
            di._atomic_write_json(target, ["BTC"])

        assert not tmp_target.exists(), "Temporary file must be cleaned up on write failure"

    def test_watch_normalizes_mixed_type_inputs(self, monkeypatch):
        """watch() should normalize mixed-type symbol inputs without crashing.

        Non-string values (int, None) are cast to strings via _normalize_symbol_list.
        Manual lists are preserved regardless of available set.
        """

        class FakeCoinDataMixedTypes(FakeCoinData):
            def filter_mapping(
                self,
                exchange,
                market_cap_min_m,
                vol_mcap_max,
                only_cpt,
                notices_ignore,
                tags,
                active_only,
                quote_filter,
                use_cache,
            ):
                if market_cap_min_m == 0 and vol_mcap_max == float("inf"):
                    return ["btc", " ETH ", None, 123], []
                return ["btc", "eth", ""], [None, "eth"]

        di = DynamicIgnore()
        di.coindata = FakeCoinDataMixedTypes()
        di.path = "dummy"
        di.approved_coins_long = ["  btc  ", None, 7]
        di.ignored_coins_short = ["eth", " ", None]

        save_calls = {"count": 0}

        def _save_stub():
            save_calls["count"] += 1

        monkeypatch.setattr(di, "save", _save_stub)

        assert di.watch() is True
        # "7" preserved from manual approved (cast from int); not in ignored → stays
        assert di.approved_coins == ["7", "BTC"], \
            "Manual approved preserved even when cast from non-string types"
        # "123" from available but uncovered → goes to ignored
        assert di.ignored_coins == ["123", "ETH"], \
            "Uncovered available symbols classified as ignored"
        assert save_calls["count"] == 1


class TestCommandFileGuard:
    """Tests for malformed command file guard and quarantine behavior."""

    @staticmethod
    def _make_pbrun_stub(tmp_path):
        run = PBRun_mod.PBRun.__new__(PBRun_mod.PBRun)
        run.cmd_path = str(tmp_path / "cmd")
        Path(run.cmd_path).mkdir(parents=True, exist_ok=True)
        run.failed_cmd_path = Path(run.cmd_path) / "failed"
        run.failed_cmd_path.mkdir(parents=True, exist_ok=True)
        run._bad_cmd_failures = {}
        run._bad_cmd_quarantine_after = 3

        run.v7_path = str(tmp_path / "run_v7")
        run.multi_path = str(tmp_path / "multi")
        run.single_path = str(tmp_path / "single")

        run.update_from_status = lambda *_args, **_kwargs: None
        run.update_from_status_single = lambda *_args, **_kwargs: None
        run.update_from_status_v7 = lambda *_args, **_kwargs: None

        run.update_activate_v7 = lambda *_args, **_kwargs: None
        run.update_activate = lambda *_args, **_kwargs: None
        run.update_activate_single = lambda *_args, **_kwargs: None
        run.watch_v7 = lambda *_args, **_kwargs: None
        run.watch_multi = lambda *_args, **_kwargs: None
        run.watch_single = lambda *_args, **_kwargs: None

        return run

    def test_invalid_update_status_command_quarantined_after_three_attempts(self, tmp_path):
        """Malformed update_status command file should be quarantined after 3 failed parses."""
        run = self._make_pbrun_stub(tmp_path)

        bad_file = Path(run.cmd_path) / "update_status_bad.cmd"
        bad_file.write_text("{broken json", encoding="utf-8")

        run.has_update_status()
        assert bad_file.exists(), "File should remain for retry after first failure"
        assert not list(run.failed_cmd_path.glob("update_status_bad*.cmd"))

        run.has_update_status()
        assert bad_file.exists(), "File should remain for retry after second failure"
        assert not list(run.failed_cmd_path.glob("update_status_bad*.cmd"))

        run.has_update_status()
        assert not bad_file.exists(), "File should be moved away after third failure"
        quarantined = list(run.failed_cmd_path.glob("update_status_bad.failed_*.cmd"))
        assert len(quarantined) == 1, "Expected exactly one quarantined update_status command file"
        assert str(bad_file) not in run._bad_cmd_failures, "Failure counter should be cleared after quarantine"


class TestActivateTimestampUpdates:
    """Tests for robust activate timestamp persistence and in-memory sync."""

    @staticmethod
    def _make_pbrun_stub():
        run = PBRun_mod.PBRun.__new__(PBRun_mod.PBRun)
        run.activate_v7_ts = 0
        run.instances_status_v7 = SimpleNamespace(activate_ts=0)
        return run

    @pytest.mark.parametrize(
        "method_name,key,attr,status_attr",
        [
            ("update_activate_v7", "activate_v7_ts", "activate_v7_ts", "instances_status_v7"),
        ],
    )
    def test_update_activate_persists_main_section_and_syncs_status(self, tmp_path, monkeypatch, method_name, key, attr, status_attr):
        """update_activate* should create [main] if missing and sync both object + status timestamps."""
        monkeypatch.chdir(tmp_path)
        Path("pbgui.ini").write_text("[other]\nfoo=bar\n", encoding="utf-8")

        run = self._make_pbrun_stub()
        method = getattr(run, method_name)
        method()

        cfg_text = Path("pbgui.ini").read_text(encoding="utf-8")
        assert "[main]" in cfg_text

        import configparser

        cfg = configparser.ConfigParser()
        cfg.read("pbgui.ini")
        assert cfg.has_option("main", key), f"Expected {key} to be persisted in pbgui.ini"

        ts_value = int(cfg.get("main", key))
        assert ts_value > 0
        assert getattr(run, attr) == ts_value
        assert getattr(getattr(run, status_attr), "activate_ts") == ts_value


class TestGitHardening:
    """Tests for git/subprocess hardened paths in PBRun."""

    @staticmethod
    def _completed(stdout: str = "", returncode: int = 0):
        return SimpleNamespace(stdout=stdout, returncode=returncode)

    @staticmethod
    def _make_pbrun_stub(tmp_path):
        run = PBRun_mod.PBRun.__new__(PBRun_mod.PBRun)

        pbgui_root = tmp_path / "pbgui"
        pb7_root = tmp_path / "pb7"

        (pbgui_root / ".git").mkdir(parents=True, exist_ok=True)
        (pb7_root / ".git").mkdir(parents=True, exist_ok=True)

        run.pbgdir = pbgui_root
        run.pb7dir = str(pb7_root)

        run.pbgui_branch = "main"
        run.pb7_branch = "master"
        run.pbgui_branches_data = {}
        run.pb7_branches_data = {}

        run.pbgui_commit = "N/A"
        run.pb7_commit = "N/A"
        run.pbgui_commit_origin = "N/A"
        run.pb7_commit_origin = "N/A"

        return run

    def test_parse_git_log_output_handles_multiline_commit_messages(self):
        """_parse_git_log_output() should keep multiline messages and first timestamp."""
        raw = (
            "a1|aaaa|alice|1 day ago|200|title line\nbody line\x00"
            "b2|bbbb|bob|2 days ago|150|single line\x00"
        )

        commits, latest_ts = PBRun_mod._parse_git_log_output(raw, "unit-test")

        assert len(commits) == 2
        assert latest_ts == 200
        assert commits[0]["message"] == "title line\nbody line"
        assert commits[1]["message"] == "single line"

    def test_load_git_branches_history_prefers_remote_and_sorts_by_latest_timestamp(self, tmp_path, monkeypatch):
        """load_git_branches_history() should skip duplicate local branches and sort newest first."""
        run = self._make_pbrun_stub(tmp_path)

        branch_output = "\n".join(
            [
                "* main",
                "  feature/local-only",
                "  remotes/origin/main",
                "  remotes/origin/feature/new",
            ]
        )

        log_outputs = {
            "remotes/origin/main": "m1|m111|alice|1 day ago|200|main msg\\x00",
            "remotes/origin/feature/new": "n1|n111|bob|1 hour ago|300|new msg\\x00",
            "feature/local-only": "l1|l111|eve|2 days ago|150|local msg\\x00",
        }

        calls = []

        def _run_subprocess_stub(cmd, **_kwargs):
            calls.append(cmd)
            if "branch" in cmd:
                return self._completed(branch_output)
            if "log" in cmd:
                branch_ref = cmd[4]
                return self._completed(log_outputs.get(branch_ref, ""))
            return self._completed("")

        monkeypatch.setattr(PBRun_mod, "_run_subprocess", _run_subprocess_stub)

        run.load_git_branches_history()

        assert list(run.pbgui_branches_data.keys()) == [
            "feature/new",
            "main",
            "feature/local-only",
        ], "Expected sort by latest timestamp desc with remote-preferred branch names"
        assert "main" in run.pbgui_branches_data
        assert run.pbgui_branches_data["main"][0]["short"] == "m1"
        assert any("fetch" in c for c in calls), "Expected fetch call before branch history load"

    def test_load_more_commits_uses_remote_ref_for_non_current_branch(self, tmp_path, monkeypatch):
        """load_more_commits() should query remotes/origin/<branch> when branch is not current."""
        run = self._make_pbrun_stub(tmp_path)
        run.pbgui_branch = "main"
        run.pbgui_branches_data = {"feature/x": []}

        seen_log_refs = []

        def _run_subprocess_stub(cmd, **_kwargs):
            if "log" in cmd:
                seen_log_refs.append(cmd[4])
                return self._completed("c1|cccc|dev|now|123|commit\x00")
            return self._completed("")

        monkeypatch.setattr(PBRun_mod, "_run_subprocess", _run_subprocess_stub)

        run.load_more_commits("feature/x", limit=5)

        assert seen_log_refs == ["remotes/origin/feature/x"]
        assert run.pbgui_branches_data["feature/x"][0]["short"] == "c1"

    def test_get_current_status_returns_fallbacks_when_subprocess_fails(self, tmp_path, monkeypatch):
        """get_current_*_status() should return safe fallback values on command failures."""
        run = self._make_pbrun_stub(tmp_path)

        monkeypatch.setattr(PBRun_mod, "_run_subprocess", lambda *_args, **_kwargs: None)

        pbgui_branch, pbgui_commit = run.get_current_pbgui_status()
        pb7_branch, pb7_commit = run.get_current_pb7_status()

        assert pbgui_branch == "unknown"
        assert pbgui_commit == ""
        assert pb7_branch == "unknown"
        assert pb7_commit == ""

    def test_load_git_commits_keeps_defaults_when_git_commands_fail(self, tmp_path, monkeypatch):
        """load_git_commits() should not overwrite fields on non-zero git command results."""
        run = self._make_pbrun_stub(tmp_path)

        def _run_subprocess_stub(_cmd, **_kwargs):
            return self._completed("", returncode=1)

        monkeypatch.setattr(PBRun_mod, "_run_subprocess", _run_subprocess_stub)

        run.load_git_commits()

        assert run.pbgui_commit == "N/A"
        assert run.pb7_commit == "N/A"


class TestGitVersionLoading:
    """Tests for git-based version extraction in PBRun."""

    @staticmethod
    def _completed(stdout: str = "", returncode: int = 0):
        return SimpleNamespace(stdout=stdout, returncode=returncode)

    @staticmethod
    def _make_pbrun_stub(tmp_path):
        run = PBRun_mod.PBRun.__new__(PBRun_mod.PBRun)

        pbgui_root = tmp_path / "pbgui"
        pb7_root = tmp_path / "pb7"

        (pbgui_root / ".git").mkdir(parents=True, exist_ok=True)
        (pb7_root / ".git").mkdir(parents=True, exist_ok=True)

        run.pbgdir = pbgui_root
        run.pb7dir = str(pb7_root)

        run.pbgui_version_origin = "N/A"
        run.pb7_version_origin = "N/A"

        return run

    def test_load_versions_origin_extracts_first_version_from_each_repo(self, tmp_path, monkeypatch):
        """load_versions_origin() should parse first matching version token from each README stream."""
        run = self._make_pbrun_stub(tmp_path)

        outputs = {
            "origin/main:README.md": "# title\nnotes\nv9.99\nv9.98\n",
            "origin/master:README.md": "something\n# v7.77\nnext v7.76\n",
        }

        def _run_subprocess_stub(cmd, **_kwargs):
            key = cmd[-1]
            return self._completed(outputs.get(key, ""), returncode=0)

        monkeypatch.setattr(PBRun_mod, "_run_subprocess", _run_subprocess_stub)

        run.load_versions_origin()

        assert run.pbgui_version_origin == "v9.99"
        assert run.pb7_version_origin == "v7.77"

    def test_load_versions_origin_keeps_defaults_on_show_failures(self, tmp_path, monkeypatch):
        """load_versions_origin() should preserve existing values when git show fails."""
        run = self._make_pbrun_stub(tmp_path)

        def _run_subprocess_stub(_cmd, **_kwargs):
            return self._completed("", returncode=1)

        monkeypatch.setattr(PBRun_mod, "_run_subprocess", _run_subprocess_stub)

        run.load_versions_origin()

        assert run.pbgui_version_origin == "N/A"
        assert run.pb7_version_origin == "N/A"


class TestLocalVersionLoading:
    """Tests for local README-based version extraction in PBRun."""

    @staticmethod
    def _make_pbrun_stub(tmp_path):
        run = PBRun_mod.PBRun.__new__(PBRun_mod.PBRun)

        pbgui_root = tmp_path / "pbgui"
        pb7_root = tmp_path / "pb7"

        pbgui_root.mkdir(parents=True, exist_ok=True)
        pb7_root.mkdir(parents=True, exist_ok=True)

        run.pbgdir = pbgui_root
        run.pb7dir = str(pb7_root)

        run.pbgui_version = "N/A"
        run.pb7_version = "N/A"

        return run

    def test_load_versions_extracts_first_match_within_first_20_lines(self, tmp_path):
        """load_versions() should parse first matching version token in first 20 README lines."""
        run = self._make_pbrun_stub(tmp_path)

        (Path(run.pbgdir) / "README.md").write_text(
            "\n".join(["line"] * 5 + ["# v1.23", "v1.22"]),
            encoding="utf-8",
        )
        (Path(run.pb7dir) / "README.md").write_text(
            "intro\n# v7.89\nnext v7.88\n",
            encoding="utf-8",
        )

        run.load_versions()

        assert run.pbgui_version == "v1.23"
        assert run.pb7_version == "v7.89"

    def test_load_versions_ignores_matches_after_line_20(self, tmp_path):
        """load_versions() should only inspect the first 20 lines of each README."""
        run = self._make_pbrun_stub(tmp_path)

        pbgui_lines = ["line" for _ in range(20)] + ["v9.99"]
        (Path(run.pbgdir) / "README.md").write_text("\n".join(pbgui_lines), encoding="utf-8")

        pb7_lines = ["line" for _ in range(25)]
        pb7_lines[21] = "v7.99"
        (Path(run.pb7dir) / "README.md").write_text("\n".join(pb7_lines), encoding="utf-8")

        run.load_versions()

        assert run.pbgui_version == "N/A"
        assert run.pb7_version == "N/A"


class TestDynamicIgnoreStartupGate:
    """Tests ensuring startup is delayed until dynamic ignore lists are ready."""

    def test_runv7_start_delays_when_dynamic_lists_not_ready(self, tmp_path, monkeypatch):
        """RunV7.start() must not spawn process while dynamic ignore lists are unavailable."""

        class DynamicIgnoreNotReady:
            def __init__(self):
                self.watch_calls = 0

            def lists_ready(self):
                return False

            def watch(self):
                self.watch_calls += 1
                return False

        rv7 = PBRun_mod.RunV7()
        rv7.path = str(tmp_path)
        rv7.pb7dir = str(tmp_path)
        rv7.pb7venv = "/usr/bin/python3"
        rv7.pbgdir = str(tmp_path)
        rv7.dynamic_ignore = DynamicIgnoreNotReady()

        monkeypatch.setattr(rv7, "is_running", lambda: False)
        monkeypatch.setattr(PBRun_mod, "sleep", lambda *_args, **_kwargs: None)

        popen_calls = []

        def _popen_stub(*_args, **_kwargs):
            popen_calls.append(1)
            return SimpleNamespace()

        monkeypatch.setattr(PBRun_mod.subprocess, "Popen", _popen_stub)

        rv7.start()

        assert rv7.dynamic_ignore.watch_calls == 1
        assert popen_calls == [], "V7 process must not start before dynamic ignore lists are ready"

    def test_runv7_start_delays_dynamic_ignore_without_cmc_key(self, tmp_path, monkeypatch):
        """RunV7.start() must not spawn or bootstrap CMC data without an API key."""

        class CoinDataNoKey:
            """CoinData double with no configured CoinMarketCap API key."""

            def __init__(self):
                self.api_key = ""
                self.load_config_calls = 0

            def load_config(self):
                self.load_config_calls += 1

        class DynamicIgnoreNotReady:
            """DynamicIgnore double whose lists are unavailable."""

            def __init__(self):
                self.coindata = CoinDataNoKey()
                self.watch_calls = 0

            def list_files_exist(self):
                return False

            def lists_ready(self):
                return False

            def watch(self):
                self.watch_calls += 1
                return False

        rv7 = PBRun_mod.RunV7()
        rv7.path = str(tmp_path)
        rv7.pb7dir = str(tmp_path)
        rv7.pb7venv = "/usr/bin/python3"
        rv7.pbgdir = str(tmp_path)
        rv7.dynamic_ignore = DynamicIgnoreNotReady()

        monkeypatch.setattr(rv7, "is_running", lambda: False)
        monkeypatch.setattr(PBRun_mod, "_log", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(PBRun_mod.subprocess, "Popen", lambda *_args, **_kwargs: pytest.fail("Popen must not run"))
        monkeypatch.setattr(rv7, "_bootstrap_dynamic_ignore_data", lambda: pytest.fail("CMC bootstrap must not run"))

        rv7.start()

        assert rv7.dynamic_ignore.watch_calls == 1
        assert rv7.dynamic_ignore.coindata.load_config_calls == 1
        assert (tmp_path / "running_version.txt").read_text(encoding="utf-8") == "0"

    def test_watch_v7_keeps_status_not_running_when_start_skips(self, tmp_path, monkeypatch):
        """watch_v7() must not report a bot as running when RunV7.start() skips startup."""

        instance_dir = tmp_path / "data" / "run_v7" / "bot-a"
        instance_dir.mkdir(parents=True)
        (instance_dir / "config.json").write_text("{}", encoding="utf-8")

        class FakeRunV7:
            """RunV7 double that accepts load() but never becomes running."""

            def __init__(self):
                self.path = None
                self.user = None
                self.name = None
                self.pb7dir = None
                self.pb7venv = None
                self.pbgdir = None
                self.version = 3
                self.start_calls = 0

            def load(self):
                return True

            def is_running(self):
                return False

            def start(self):
                self.start_calls += 1

            def stop(self):
                pass

        run = PBRun_mod.PBRun.__new__(PBRun_mod.PBRun)
        run.instances_status_v7 = PBRun_mod.InstancesStatus(str(tmp_path / "status_v7.json"))
        run.run_v7 = []
        run.v7_path = str(tmp_path / "data" / "run_v7")
        run.pbgdir = str(tmp_path)
        run.pb7dir = str(tmp_path / "pb7")
        run.pb7venv = "/usr/bin/python3"
        run.name = "test-vps"

        monkeypatch.setattr(PBRun_mod, "RunV7", FakeRunV7)

        run.watch_v7([str(instance_dir)])

        status = run.instances_status_v7.find_name("bot-a")
        assert status is not None
        assert status.running is False
        assert run.run_v7[0].start_calls == 1

    def test_load_versions_keeps_defaults_when_files_missing_or_no_match(self, tmp_path):
        """load_versions() should keep defaults when README is missing or contains no version token."""
        run = TestLocalVersionLoading._make_pbrun_stub(tmp_path)

        (Path(run.pbgdir) / "README.md").write_text("no semantic version here\n", encoding="utf-8")
        (Path(run.pb7dir) / "README.md").write_text("another text block\n", encoding="utf-8")

        run.load_versions()

        assert run.pbgui_version == "N/A"
        assert run.pb7_version == "N/A"


class TestClusterDesiredStateGate:
    """Tests for the PBRun Cluster Sync desired-state gate."""

    def test_runv7_start_allows_classic_non_cluster_install(self, tmp_path, monkeypatch):
        """RunV7.start() should preserve classic behavior before Cluster Sync is initialized."""

        rv7 = _make_cluster_runv7(tmp_path)
        monkeypatch.setattr(PBRun_mod, "sleep", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(PBRun_mod, "_log", lambda *_args, **_kwargs: None)

        state = {"calls": 0}

        def _is_running() -> bool:
            state["calls"] += 1
            return state["calls"] > 1

        popen_calls = []

        def _popen_stub(*args, **kwargs):
            popen_calls.append((args, kwargs))
            return SimpleNamespace(stderr=iter(()))

        monkeypatch.setattr(rv7, "is_running", _is_running)
        monkeypatch.setattr(PBRun_mod.subprocess, "Popen", _popen_stub)

        rv7.start()

        assert len(popen_calls) == 1
        assert rv7.cluster_blocked is False
        assert rv7.cluster_gate == "not_configured"

    def test_runv7_start_blocks_when_cluster_desired_state_missing(self, tmp_path, monkeypatch):
        """Initialized Cluster Sync without desired_state.json must block PBRun starts."""

        rv7 = _make_cluster_runv7(tmp_path)
        ensure_local_identity(tmp_path / "data" / "cluster", pbname="test-vps")
        monkeypatch.setattr(rv7, "is_running", lambda: False)
        monkeypatch.setattr(PBRun_mod, "_log", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(PBRun_mod.subprocess, "Popen", lambda *_args, **_kwargs: pytest.fail("Popen must not run"))

        rv7.start()

        assert rv7.cluster_blocked is True
        assert rv7.cluster_gate == "missing_desired_state"
        assert "desired_state.json is missing" in rv7.cluster_blocked_reason
        assert (Path(rv7.path) / "running_version.txt").read_text(encoding="utf-8") == "0"

    def test_runv7_start_allows_matching_cluster_desired_state(self, tmp_path, monkeypatch):
        """Matching desired state, assignment, version and manifest should allow startup."""

        rv7 = _make_cluster_runv7(tmp_path)
        _write_cluster_desired(tmp_path, Path(rv7.path))
        monkeypatch.setattr(PBRun_mod, "sleep", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(PBRun_mod, "_log", lambda *_args, **_kwargs: None)

        state = {"calls": 0}

        def _is_running() -> bool:
            state["calls"] += 1
            return state["calls"] > 1

        popen_calls = []

        def _popen_stub(*args, **kwargs):
            popen_calls.append((args, kwargs))
            return SimpleNamespace(stderr=iter(()))

        monkeypatch.setattr(rv7, "is_running", _is_running)
        monkeypatch.setattr(PBRun_mod.subprocess, "Popen", _popen_stub)

        rv7.start()

        assert len(popen_calls) == 1
        assert rv7.cluster_blocked is False
        assert rv7.cluster_gate == "allowed"

    def test_runv7_start_reloads_materialized_cluster_config(self, tmp_path, monkeypatch):
        """RunV7.start() must reload materialized config before Cluster gate checks."""

        rv7 = _make_cluster_runv7(tmp_path)
        instance_dir = Path(rv7.path)
        _write_v7_config(instance_dir, version=4)
        _write_cluster_desired(tmp_path, instance_dir, version="4")
        assert rv7.version == 3
        assert json.loads((instance_dir / "config_run.json").read_text(encoding="utf-8"))["pbgui"]["version"] == 3
        monkeypatch.setattr(PBRun_mod, "sleep", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(PBRun_mod, "_log", lambda *_args, **_kwargs: None)

        state = {"calls": 0}

        def _is_running() -> bool:
            state["calls"] += 1
            return state["calls"] > 1

        popen_calls = []

        def _popen_stub(*args, **kwargs):
            popen_calls.append((args, kwargs))
            return SimpleNamespace(stderr=iter(()))

        monkeypatch.setattr(rv7, "is_running", _is_running)
        monkeypatch.setattr(PBRun_mod.subprocess, "Popen", _popen_stub)

        rv7.start()

        assert len(popen_calls) == 1
        assert rv7.version == 4
        assert json.loads((instance_dir / "config_run.json").read_text(encoding="utf-8"))["pbgui"]["version"] == 4
        assert rv7.cluster_blocked is False
        assert rv7.cluster_gate == "allowed"

    def test_runv7_start_blocks_stopped_cluster_desired_state(self, tmp_path, monkeypatch):
        """Cluster desired_state=stopped must prevent spawning a local bot."""

        rv7 = _make_cluster_runv7(tmp_path)
        _write_cluster_desired(tmp_path, Path(rv7.path), desired_state="stopped")
        monkeypatch.setattr(rv7, "is_running", lambda: False)
        monkeypatch.setattr(PBRun_mod, "_log", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(PBRun_mod.subprocess, "Popen", lambda *_args, **_kwargs: pytest.fail("Popen must not run"))

        rv7.start()

        assert rv7.cluster_blocked is True
        assert rv7.cluster_gate == "desired_stopped"
        assert (Path(rv7.path) / "running_version.txt").read_text(encoding="utf-8") == "0"

    @pytest.mark.parametrize(
        ("desired_kwargs", "expected_gate"),
        [
            ({"assigned_host": "pbgui-node-other"}, "wrong_host"),
            ({"manifest_hash": "sha256:mismatch"}, "manifest_mismatch"),
            ({"version": "4"}, "version_mismatch"),
        ],
    )
    def test_runv7_start_blocks_cluster_desired_state_mismatches(
        self,
        tmp_path,
        monkeypatch,
        desired_kwargs,
        expected_gate,
    ):
        """Wrong assignment, manifest or version must prevent startup."""

        rv7 = _make_cluster_runv7(tmp_path)
        _write_cluster_desired(tmp_path, Path(rv7.path), **desired_kwargs)
        monkeypatch.setattr(rv7, "is_running", lambda: False)
        monkeypatch.setattr(PBRun_mod, "_log", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(PBRun_mod.subprocess, "Popen", lambda *_args, **_kwargs: pytest.fail("Popen must not run"))

        rv7.start()

        assert rv7.cluster_blocked is True
        assert rv7.cluster_gate == expected_gate
        assert (Path(rv7.path) / "running_version.txt").read_text(encoding="utf-8") == "0"

    def test_runv7_watch_stops_running_bot_when_cluster_gate_blocks(self, tmp_path, monkeypatch):
        """RunV7.watch() must stop a running bot if desired state no longer permits it."""

        rv7 = _make_cluster_runv7(tmp_path)
        _write_cluster_desired(tmp_path, Path(rv7.path), desired_state="stopped")
        fake_process = object()
        killed = []

        monkeypatch.setattr(rv7, "pid", lambda: fake_process)
        monkeypatch.setattr(PBRun_mod, "_kill_process", lambda proc, context: killed.append((proc, context)))
        monkeypatch.setattr(PBRun_mod, "_log", lambda *_args, **_kwargs: None)

        rv7.watch()

        assert killed == [(fake_process, f"v7 {rv7.path}")]
        assert rv7.cluster_blocked is True
        assert rv7.cluster_gate == "desired_stopped"
        assert (Path(rv7.path) / "running_version.txt").read_text(encoding="utf-8") == "0"

    def test_instance_status_persists_cluster_block_fields(self, tmp_path):
        """InstancesStatus should round-trip cluster block metadata."""

        status_path = tmp_path / "status_v7.json"
        statuses = PBRun_mod.InstancesStatus(str(status_path))
        item = PBRun_mod.InstanceStatus()
        item.name = "bot-a"
        item.version = 3
        item.multi = False
        item.enabled_on = "test-vps"
        item.running = False
        item.blocked = True
        item.blocked_reason = "Cluster desired state is not running"
        item.cluster_gate = "desired_stopped"
        statuses.add(item)

        statuses.save()
        loaded = PBRun_mod.InstancesStatus(str(status_path)).find_name("bot-a")

        assert loaded is not None
        assert loaded.blocked is True
        assert loaded.blocked_reason == "Cluster desired state is not running"
        assert loaded.cluster_gate == "desired_stopped"


class TestClusterBootSyncWait:
    """Tests for PBRun's non-fatal PBCluster boot sync wait."""

    def test_wait_for_cluster_boot_sync_skips_non_cluster_install(self, tmp_path):
        """PBRun should not wait when Cluster Sync is not initialized."""

        result = PBRun_mod._wait_for_cluster_boot_sync(tmp_path, timeout=0)

        assert result["status"] == "not_configured"

    def test_wait_for_cluster_boot_sync_accepts_fresh_status(self, tmp_path, monkeypatch):
        """A fresh PBCluster sync_status should let PBRun continue immediately."""

        cluster_root = default_cluster_root(tmp_path)
        ensure_local_identity(cluster_root, pbname="runner-a")
        (cluster_root / "sync_status.json").write_text(
            json.dumps({"status": "local_reconciled", "finished_at": int(PBRun_mod.time())}),
            encoding="utf-8",
        )
        monkeypatch.setattr(PBRun_mod, "sleep", lambda *_args, **_kwargs: pytest.fail("sleep should not be needed"))

        result = PBRun_mod._wait_for_cluster_boot_sync(tmp_path, timeout=0)

        assert result["status"] == "local_reconciled"

    def test_wait_for_cluster_boot_sync_timeout_is_warning_only(self, tmp_path, monkeypatch):
        """Missing PBCluster status should warn but not block PBRun startup."""

        cluster_root = default_cluster_root(tmp_path)
        ensure_local_identity(cluster_root, pbname="runner-a")
        logs = []
        monkeypatch.setattr(PBRun_mod, "_log", lambda service, message, **kwargs: logs.append((service, message, kwargs)))

        result = PBRun_mod._wait_for_cluster_boot_sync(tmp_path, timeout=0)

        assert result["status"] == "timeout"
        assert logs
        assert "continuing with local desired state" in logs[0][1]
