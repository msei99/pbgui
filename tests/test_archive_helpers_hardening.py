"""Security and integrity regression tests for archive helper boundaries."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import threading
from pathlib import Path

import pytest

from api import archive_helpers as helpers


def write_json(path: Path, payload: object) -> None:
    """Write an isolated JSON fixture."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def make_result(root: Path, name: str = "run", *, gain: float = 1.2, version: object = "v7.12.0") -> Path:
    """Create a minimal valid backtest result fixture."""
    result = root / "cfg" / "bybit" / name
    config = {
        "backtest": {"base_dir": "backtests/pbgui/cfg", "exchanges": ["bybit"], "starting_balance": 1000},
        "bot": {"long": {}, "short": {}},
    }
    if version is not None:
        config["config_version"] = version
    write_json(result / "config.json", config)
    write_json(result / "analysis.json", {"gain": gain, "liquidated": gain < 1})
    return result


def init_git(path: Path) -> None:
    """Commit all current temporary archive content into a clean repository."""
    subprocess.run(["git", "init"], cwd=path, check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=path, check=True)
    subprocess.run(["git", "add", "-A"], cwd=path, check=True)
    subprocess.run(["git", "commit", "-m", "fixtures"], cwd=path, check=True, capture_output=True, text=True)


def test_git_status_failure_is_fail_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A nonzero git status exposes its return code and blocks migration."""
    archive = tmp_path / "archive"
    (archive / ".git").mkdir(parents=True)
    monkeypatch.setattr(
        helpers.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 23, stdout="M ignored", stderr="failed"),
    )

    state = helpers.git_worktree_state(archive)
    report = helpers.migrate_archive_layout(archive)

    assert state == {"is_git": True, "dirty": True, "porcelain": "M ignored", "status_ok": False, "returncode": 23}
    assert report["reason"] == "git_status_failed"


def test_backtest_listing_requires_safe_json_objects(tmp_path: Path) -> None:
    """Malformed and symlinked required JSON files are omitted from archive scans."""
    archive = tmp_path / "archive"
    valid = make_result(archive / "valid")
    malformed = make_result(archive / "malformed", "bad")
    (malformed / "analysis.json").write_text("[]", encoding="utf-8")
    linked = make_result(archive / "linked", "link")
    target = tmp_path / "outside-analysis.json"
    write_json(target, {"gain": 9})
    (linked / "analysis.json").unlink()
    (linked / "analysis.json").symlink_to(target)

    listed = helpers.list_archive_backtest_results(archive)

    assert [Path(item["path"]) for item in listed] == [valid]
    with pytest.raises(RuntimeError):
        helpers.summarize_backtest_result(linked, archive)


def test_optimize_listing_skips_bad_configs_and_ignores_bad_optional_meta(tmp_path: Path) -> None:
    """Optimize scans require safe config objects but treat invalid metadata as absent."""
    archive = tmp_path / "archive"
    base = archive / "pbgui/configs/v7.12.0/optimize"
    write_json(base / "good.json", {"config_version": "v7.12.0", "optimize": {}})
    (base / "good.meta.json").write_text("not-json", encoding="utf-8")
    write_json(base / "array.json", [])
    outside = tmp_path / "outside.json"
    write_json(outside, {"config_version": "v7.12.0"})
    (base / "linked.json").symlink_to(outside)

    listed = helpers.list_archive_optimize_configs(archive)

    assert [item["name"] for item in listed] == ["good"]
    assert listed[0]["meta"] == {}


def test_manifest_rejects_non_dict_items_and_symlinks(tmp_path: Path) -> None:
    """Manifest trust requires object entries and a regular no-follow source file."""
    archive = tmp_path / "archive"
    manifest_path = archive / helpers.ARCHIVE_MANIFEST
    write_json(manifest_path, {"schema_version": 1, "items": [{"type": "backtest_result"}, "bad"]})
    assert helpers.load_archive_manifest(archive) is None

    outside = tmp_path / "outside-manifest.json"
    write_json(outside, {"schema_version": 1, "items": []})
    manifest_path.unlink()
    manifest_path.symlink_to(outside)
    assert helpers.load_archive_manifest(archive) is None


@pytest.mark.parametrize("kind", ["root", "file", "directory"])
def test_copy_rejects_all_source_symlinks(kind: str, tmp_path: Path) -> None:
    """Backtest copying rejects source roots, files, and nested directory symlinks."""
    source = make_result(tmp_path / "source")
    selected = source
    if kind == "root":
        selected = tmp_path / "source-link"
        selected.symlink_to(source, target_is_directory=True)
    elif kind == "file":
        target = tmp_path / "outside.txt"
        target.write_text("outside", encoding="utf-8")
        (source / "linked.txt").symlink_to(target)
    else:
        outside = tmp_path / "outside-dir"
        outside.mkdir()
        (source / "linked-dir").symlink_to(outside, target_is_directory=True)

    with pytest.raises(RuntimeError):
        helpers.copy_backtest_result_to_archive(selected, tmp_path / "archive")


def test_copy_uses_symlink_preserving_defense_and_rejects_destination_symlink(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Copytree preserves any raced symlink and destination components cannot redirect writes."""
    source = make_result(tmp_path / "source")
    archive = tmp_path / "archive"
    called = {}
    original = shutil.copytree

    def recording_copytree(*args, **kwargs):
        called.update(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(helpers.shutil, "copytree", recording_copytree)
    helpers.copy_backtest_result_to_archive(source, archive)
    assert called["symlinks"] is True

    other_archive = tmp_path / "other-archive"
    other_archive.mkdir()
    outside = tmp_path / "outside-destination"
    outside.mkdir()
    (other_archive / "pbgui").symlink_to(outside, target_is_directory=True)
    with pytest.raises(RuntimeError):
        helpers.copy_backtest_result_to_archive(source, other_archive)


@pytest.mark.parametrize("version", [" v7.12.0", "v7/12", "x" * 121, 712, ""])
def test_invalid_config_versions_use_unknown_fingerprint_paths(version: object, tmp_path: Path) -> None:
    """Only unchanged bounded safe strings qualify as version path components."""
    info = helpers.config_version_info({"config_version": version}, fingerprint="deadbeef")
    source = make_result(tmp_path / str(len(str(version))), version=version)
    relative, meta = helpers.derive_backtest_archive_relative_path(source, tmp_path / "archive")

    assert info["pb7_config_version"] == "unknown"
    assert info["has_pb7_config_version"] is False
    assert relative.parts[2] == "unknown"
    assert meta["result_name"].endswith(f"__{meta['fingerprint']}")


def test_backtest_collision_reuses_existing_fingerprint_suffix(tmp_path: Path) -> None:
    """Repeated copies reuse an identical suffix instead of creating a numbered duplicate."""
    archive = tmp_path / "archive"
    first = make_result(tmp_path / "first", gain=1.1)
    desired = make_result(tmp_path / "desired", gain=1.3)
    helpers.copy_backtest_result_to_archive(first, archive)

    copied = helpers.copy_backtest_result_to_archive(desired, archive)
    repeated = helpers.copy_backtest_result_to_archive(desired, archive)

    assert copied["skipped"] is False
    assert repeated["skipped"] is True
    assert repeated["path"] == copied["path"]
    assert not Path(copied["path"] + "_2").exists()


def test_optimize_collision_reuses_valid_suffix_but_not_malformed_candidate(tmp_path: Path) -> None:
    """Optimize collision matching ignores malformed JSON and reuses valid identical suffixes."""
    archive = tmp_path / "archive"
    config = {"config_version": "v7.12.0", "optimize": {"x": 1}}
    primary, meta = helpers.derive_optimize_archive_relative_path("cfg", config)
    primary_path = archive / primary
    write_json(primary_path, {"config_version": "v7.12.0", "optimize": {"x": 2}})
    malformed = primary_path.with_name(f"{primary_path.stem}__{meta['fingerprint']}.json")
    malformed.write_text("[]", encoding="utf-8")

    candidate, _, skipped = helpers.resolve_optimize_archive_destination(archive, "cfg", config)
    assert skipped is False
    assert candidate.name.endswith("_2.json")
    write_json(candidate, config)

    repeated, _, skipped = helpers.resolve_optimize_archive_destination(archive, "cfg", config)
    assert skipped is True
    assert repeated == candidate


def test_migration_reuses_identical_collision_suffix(tmp_path: Path) -> None:
    """Migration removes a legacy duplicate when its fingerprint suffix already exists."""
    archive = tmp_path / "archive"
    legacy = make_result(archive / "legacy", gain=1.3)
    relative, meta = helpers.derive_backtest_archive_relative_path(legacy, archive)
    primary = archive / relative
    different = make_result(tmp_path / "different", gain=1.1)
    primary.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(different, primary)
    suffix = primary.with_name(f"{primary.name}__{meta['fingerprint']}")
    shutil.copytree(legacy, suffix)
    init_git(archive)

    report = helpers.migrate_archive_layout(archive)

    assert report["removed_duplicates"] == 1
    assert report["collisions"] == 0
    assert report["items"][0]["target"] == str(suffix)
    assert not legacy.exists()
    assert helpers.archive_migration_status(archive)["status"] == "migrated_pending_push"


def test_archive_item_counts_uses_manifest_or_safe_scan(tmp_path: Path) -> None:
    """Counts prefer a valid manifest and safely scan when it cannot be trusted."""
    archive = tmp_path / "archive"
    manifest = {"schema_version": 1, "items": [{"type": "backtest_result"}, {"type": "optimize_config"}]}
    assert helpers.archive_item_counts(archive, manifest) == {
        "configs": 1,
        "results": 1,
        "optimize_configs": 1,
        "items": 2,
        "source": "manifest",
    }

    make_result(archive / "results")
    write_json(archive / "pbgui/configs/v7.12.0/optimize/cfg.json", {"config_version": "v7.12.0"})
    counts = helpers.archive_item_counts(archive, {"schema_version": 1, "items": ["bad"]})
    assert counts["source"] == "scan"
    assert counts["results"] == 1
    assert counts["optimize_configs"] == 1


def test_liquidated_removal_dedupes_and_verifies_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Deletion counts only verified removals and reports a no-op rmtree as failed."""
    archive = tmp_path / "archive"
    result = make_result(archive / "results", gain=0.5)
    monkeypatch.setattr(helpers.shutil, "rmtree", lambda path: None)

    response = helpers.remove_liquidated_results(archive, [str(result), str(result)], "selected_results", False)

    assert response["ok"] is False
    assert response["matched"] == 1
    assert response["removed"] == 0
    assert response["failed"] == 1
    assert response["items"][0]["ok"] is False


def test_liquidated_removal_rejects_symlinks_and_continues(tmp_path: Path) -> None:
    """Unsafe selections fail independently while another valid result is removed."""
    archive = tmp_path / "archive"
    valid = make_result(archive / "valid", gain=0.5)
    target = make_result(archive / "target", gain=0.5)
    linked = archive / "linked-result"
    linked.symlink_to(target, target_is_directory=True)

    response = helpers.remove_liquidated_results(archive, [str(linked), str(valid)], "selected_results", False)

    assert response["ok"] is False
    assert response["removed"] == 1
    assert response["failed"] == 1
    assert not valid.exists()
    assert linked.is_symlink()


def test_migration_report_hash_must_match_current_porcelain(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Stale reports, malformed counters, and failed git status are never trusted."""
    archive = tmp_path / "archive"
    write_json(
        archive / helpers.ARCHIVE_REPORT,
        {"migrated": "invalid", "post_migration_porcelain_sha256": hashlib.sha256(b"old").hexdigest()},
    )
    monkeypatch.setattr(
        helpers,
        "git_worktree_state",
        lambda root: {"is_git": True, "dirty": True, "porcelain": "new", "status_ok": True, "returncode": 0},
    )
    assert helpers.archive_migration_status(archive, fast=True)["status"] != "migrated_pending_push"

    monkeypatch.setattr(
        helpers,
        "git_worktree_state",
        lambda root: {"is_git": True, "dirty": True, "porcelain": "new", "status_ok": False, "returncode": 2},
    )
    assert helpers.archive_migration_status(archive)["status"] == "git_status_failed"


def test_migration_persists_exact_post_status_hash_and_max_items(tmp_path: Path) -> None:
    """Bounded migration records remaining work and hashes its resulting porcelain exactly."""
    archive = tmp_path / "archive"
    make_result(archive / "legacy-a", "a")
    make_result(archive / "legacy-b", "b")
    init_git(archive)

    report = helpers.migrate_archive_layout(archive, max_items=1)
    current = helpers.git_worktree_state(archive)

    assert report["migrated"] == 1
    assert report["truncated"] is True
    assert report["remaining_legacy"] is True
    assert len(report["items"]) == 1
    assert report["post_migration_porcelain_sha256"] == hashlib.sha256(current["porcelain"].encode()).hexdigest()
    assert helpers.archive_migration_status(archive)["status"] == "migrated_pending_push"


def test_maybe_migrate_accepts_max_items(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Automatic own-archive migration forwards its optional processing bound."""
    seen = []
    monkeypatch.setattr(helpers, "migrate_archive_layout", lambda root, max_items=None: seen.append(max_items) or {"skipped": True})
    monkeypatch.setattr(helpers, "archive_migration_status", lambda root: {"status": "current"})

    helpers.maybe_migrate_own_archive("mine", tmp_path, "mine", max_items=7)

    assert seen == [7]


def test_manifest_write_and_cleanup_reject_symlink_destinations(tmp_path: Path) -> None:
    """Generated manifest writes and cleanup traversal cannot cross symlink destinations."""
    archive = tmp_path / "archive"
    outside = tmp_path / "outside"
    outside.mkdir()
    (archive / "pbgui").mkdir(parents=True)
    manifest = archive / helpers.ARCHIVE_MANIFEST
    manifest.symlink_to(outside / "manifest.json")
    with pytest.raises(RuntimeError):
        helpers.rebuild_archive_manifest(archive)

    linked_parent = archive / "linked-parent"
    linked_parent.symlink_to(outside, target_is_directory=True)
    with pytest.raises(RuntimeError):
        helpers.cleanup_empty_parents(linked_parent / "child", archive)


def test_archive_transaction_is_reentrant_external_and_serializes_threads(tmp_path: Path) -> None:
    """The public transaction uses an external lock and serializes concurrent threads."""
    archive = tmp_path / "archive"
    archive.mkdir()
    (archive / "tracked.txt").write_text("tracked", encoding="utf-8")
    init_git(archive)

    with helpers.archive_transaction(archive):
        with helpers.archive_transaction(archive):
            pass

    lock_files = list((tmp_path / ".pbgui-archive-locks").glob("*.lock"))
    assert len(lock_files) == 1
    assert not list(archive.rglob("*.lock"))
    assert helpers.git_worktree_state(archive)["porcelain"] == ""

    first_entered = threading.Event()
    release_first = threading.Event()
    second_entered = threading.Event()

    def hold_first() -> None:
        """Hold the transaction until the assertion observes contention."""
        with helpers.archive_transaction(archive):
            first_entered.set()
            assert release_first.wait(timeout=5)

    def enter_second() -> None:
        """Record when the second transaction acquires the same lock."""
        with helpers.archive_transaction(archive):
            second_entered.set()

    first = threading.Thread(target=hold_first)
    second = threading.Thread(target=enter_second)
    first.start()
    assert first_entered.wait(timeout=5)
    second.start()
    assert not second_entered.wait(timeout=0.1)
    release_first.set()
    first.join(timeout=5)
    second.join(timeout=5)
    assert second_entered.is_set()
    assert not first.is_alive()
    assert not second.is_alive()


def test_generated_readme_and_config_writes_reject_symlinks(tmp_path: Path) -> None:
    """README configuration and content writes reject destination symlinks."""
    archive = tmp_path / "archive"
    (archive / "pbgui").mkdir(parents=True)
    outside_config = tmp_path / "outside-config.json"
    outside_config.write_text("outside", encoding="utf-8")
    config_path = helpers.archive_readme_config_path(archive)
    config_path.symlink_to(outside_config)

    with pytest.raises(RuntimeError):
        helpers.save_archive_readme_config(archive, {"title": "Unsafe"})
    assert outside_config.read_text(encoding="utf-8") == "outside"

    outside_readme = tmp_path / "outside-readme.md"
    outside_readme.write_text("outside", encoding="utf-8")
    readme_path = helpers.archive_readme_path(archive)
    readme_path.symlink_to(outside_readme)
    with pytest.raises(RuntimeError):
        helpers.update_archive_readme(archive, {"title": "Unsafe", "static_markdown": ""})
    assert outside_readme.read_text(encoding="utf-8") == "outside"


@pytest.mark.parametrize("generated_name", ["SCORES.md", "SCORES.html"])
def test_generated_score_writes_reject_symlinks(generated_name: str, tmp_path: Path) -> None:
    """Generated score pages reject symlink destinations without changing targets."""
    archive = tmp_path / "archive"
    archive.mkdir()
    outside = tmp_path / f"outside-{generated_name}"
    outside.write_text("outside", encoding="utf-8")
    (archive / generated_name).symlink_to(outside)

    with pytest.raises(RuntimeError):
        helpers.update_archive_scores_and_readme(archive)

    assert outside.read_text(encoding="utf-8") == "outside"


def test_optimize_sidecar_write_rejects_symlink_destination(tmp_path: Path) -> None:
    """Optimize metadata sidecars use archive-root-aware no-follow writes."""
    archive = tmp_path / "archive"
    sidecar = archive / "pbgui/configs/v7.12.0/optimize/cfg.meta.json"
    sidecar.parent.mkdir(parents=True)
    outside = tmp_path / "outside-meta.json"
    outside.write_text("outside", encoding="utf-8")
    sidecar.symlink_to(outside)

    with pytest.raises(RuntimeError):
        helpers.write_optimize_meta(sidecar, {"name": "cfg"}, archive_root=archive)

    assert outside.read_text(encoding="utf-8") == "outside"


def test_archive_atomic_writes_use_unique_exclusive_temp_names(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Generated writes allocate a fresh O_EXCL temporary name for every write."""
    archive = tmp_path / "archive"
    opened_temps = []
    original_open = helpers.os.open

    def recording_open(path, flags, mode=0o777, *, dir_fd=None):
        """Record exclusive temporary opens while preserving real behavior."""
        if flags & helpers.os.O_EXCL:
            opened_temps.append(str(path))
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(helpers.os, "open", recording_open)
    helpers.save_archive_readme_config(archive, {"title": "First"})
    helpers.save_archive_readme_config(archive, {"title": "Second"})

    assert len(opened_temps) == 2
    assert opened_temps[0] != opened_temps[1]
    assert all(name.startswith(".readme_config.json.") and name.endswith(".tmp") for name in opened_temps)
    assert "readme_config.json.tmp" not in opened_temps
    assert not list((archive / "pbgui").glob(".*.tmp"))


def test_archive_atomic_write_cleans_temp_after_replace_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A failed archive replacement removes its unique temporary file."""
    archive = tmp_path / "archive"

    def fail_replace(*args, **kwargs):
        """Simulate a final atomic replacement failure."""
        raise OSError("replace failed")

    monkeypatch.setattr(helpers.os, "replace", fail_replace)
    with pytest.raises(OSError, match="replace failed"):
        helpers.save_archive_readme_config(archive, {"title": "Failure"})

    assert not helpers.archive_readme_config_path(archive).exists()
    assert not list((archive / "pbgui").glob(".*.tmp"))


def test_backtest_copy_detects_source_mutation_and_cleans_staging(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A source changed after staging is rejected without a partial final result."""
    source = make_result(tmp_path / "source")
    archive = tmp_path / "archive"
    relative, _ = helpers.derive_backtest_archive_relative_path(source, archive)
    destination = archive / relative
    original_copytree = shutil.copytree

    def copy_then_mutate(*args, **kwargs):
        """Mutate required source JSON after its staged copy completes."""
        result = original_copytree(*args, **kwargs)
        write_json(source / "analysis.json", {"gain": 9.9, "liquidated": False})
        return result

    monkeypatch.setattr(helpers.shutil, "copytree", copy_then_mutate)
    with pytest.raises(RuntimeError, match="source changed"):
        helpers.copy_backtest_result_to_archive(source, archive)

    assert not destination.exists()
    assert not list(destination.parent.glob(f".{destination.name}.stage-*"))


def test_selected_migration_failure_reports_remaining_and_not_ok(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A failure-only migration stays clean and can be retried after the cause is fixed."""
    archive = tmp_path / "archive"
    make_result(archive / "legacy")
    init_git(archive)
    original_derivation = helpers.derive_backtest_archive_relative_path

    def fail_derivation(*args, **kwargs):
        """Simulate a selected source failing during migration."""
        raise RuntimeError("selected migration failed")

    monkeypatch.setattr(helpers, "derive_backtest_archive_relative_path", fail_derivation)
    report = helpers.migrate_archive_layout(archive, max_items=1)

    assert report["ok"] is False
    assert report["failed"] == 1
    assert report["remaining_legacy"] is True
    assert report["truncated"] is False
    assert report["items"][0]["outcome"] == "failed"
    assert "manifest" not in report
    assert not (archive / helpers.ARCHIVE_REPORT).exists()
    assert helpers.git_worktree_state(archive)["porcelain"] == ""

    monkeypatch.setattr(helpers, "derive_backtest_archive_relative_path", original_derivation)
    retried = helpers.migrate_archive_layout(archive, max_items=1)
    assert retried["migrated"] == 1


@pytest.mark.parametrize("max_items", [0, -1])
def test_migration_rejects_nonpositive_max_items_without_report(max_items: int, tmp_path: Path) -> None:
    """Invalid migration bounds fail before writing archive status files."""
    archive = tmp_path / "archive"
    make_result(archive / "legacy")
    init_git(archive)

    with pytest.raises(ValueError, match="greater than zero"):
        helpers.migrate_archive_layout(archive, max_items=max_items)

    assert not (archive / helpers.ARCHIVE_REPORT).exists()
    assert helpers.git_worktree_state(archive)["porcelain"] == ""


def test_write_archive_json_strips_param_status_and_rejects_legacy_tmp_symlink(tmp_path: Path) -> None:
    """Public archive config writes strip UI metadata and reject predictable temp symlinks."""
    archive = tmp_path / "archive"
    target = archive / "pbgui/configs/v7.12.0/optimize/demo.json"
    outside = tmp_path / "outside.json"
    outside.write_text("outside", encoding="utf-8")
    target.parent.mkdir(parents=True)
    target.with_suffix(".json.tmp").symlink_to(outside)

    with pytest.raises(RuntimeError, match="temporary path"):
        helpers.write_archive_json(target, {"config_version": "v7.12.0"}, archive)

    target.with_suffix(".json.tmp").unlink()
    helpers.write_archive_json(
        target,
        {"config_version": "v7.12.0", "_pbgui_param_status": {"long": {"x": "added"}}},
        archive,
    )
    assert "_pbgui_param_status" not in json.loads(target.read_text(encoding="utf-8"))
    assert outside.read_text(encoding="utf-8") == "outside"


def test_duplicate_cleanup_rejects_symlink_and_verifies_rmtree_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Duplicate cleanup rejects symlinks and counts only verified removals."""
    archive = tmp_path / "archive"
    first = make_result(archive / "first", "a")
    second = make_result(archive / "second", "b")
    linked = archive / "linked-result"
    linked.symlink_to(first, target_is_directory=True)
    monkeypatch.setattr(helpers.shutil, "rmtree", lambda path: None)

    response = helpers.remove_duplicate_results(
        archive,
        [str(linked), str(linked), str(first), str(second)],
        "selected_results",
        False,
    )

    assert response["ok"] is False
    assert response["matched"] == 1
    assert response["removed"] == 0
    assert response["failed"] == 2
    assert sum(item["outcome"] == "rejected" for item in response["items"]) == 1
    assert sum(item["outcome"] == "failed" for item in response["items"]) == 1
    assert first.exists()
    assert second.exists()
    assert linked.is_symlink()
