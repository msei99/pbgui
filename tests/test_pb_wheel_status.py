"""Verified PB7/PB8 receipt and guarded VPS Rust cleanup regression tests."""
import hashlib
import json
from pathlib import Path

import pytest
import yaml

import pb_wheel_status as status


COMMIT = "a" * 40


def make_prepared_bundle(path: Path, product: str = "pb7") -> Path:
    """Create a small hashed bundle for isolated cleanup tests."""
    path.mkdir()
    artifacts = {}
    for name in ("passivbot.whl", "passivbot_rust.whl", "Cargo.lock"):
        content = name.encode()
        (path / name).write_bytes(content)
        artifacts[name] = hashlib.sha256(content).hexdigest()
    (path / "manifest.json").write_text(json.dumps({
        "format": 1, "product": product, "commit": COMMIT, "artifacts": artifacts,
    }))
    return path


def make_runtime(home, product, monkeypatch):
    """Create an isolated checkout, venv binary and source-bound receipt."""
    repo = home / "software" / product
    repo.mkdir(parents=True)
    (repo / ".git").mkdir()
    python = home / "software" / f"venv_{product}" / "bin/python"
    python.parent.mkdir(parents=True)
    python.write_text("")
    binary = python.parent.parent / "lib/python3.12/site-packages/passivbot_rust/passivbot_rust.abi3.so"
    binary.parent.mkdir(parents=True)
    binary.write_bytes(product.encode())
    pbgdir = home / "software/pbgui"
    (pbgdir / "data/state").mkdir(parents=True, exist_ok=True)
    (pbgdir / "data/state" / f"{product}-wheel.json").write_text(json.dumps({
        "format": 1, "product": product, "commit": COMMIT,
        "extension_sha256": hashlib.sha256(product.encode()).hexdigest(),
    }))
    monkeypatch.setattr(status.subprocess, "check_output", lambda *a, **kw: COMMIT)
    return repo, python, binary


def test_receipt_requires_matching_checkout_and_binary(tmp_path, monkeypatch):
    """A source update or binary replacement makes the wheel indicator stale."""
    repo, python, binary = make_runtime(tmp_path, "pb7", monkeypatch)
    pbgdir = tmp_path / "software/pbgui"
    assert status.wheel_installed(pbgdir, "pb7", repo, python)
    binary.write_bytes(b"changed")
    assert not status.wheel_installed(pbgdir, "pb7", repo, python)
    binary.write_bytes(b"pb7")
    monkeypatch.setattr(status.subprocess, "check_output", lambda *a, **kw: "b" * 40)
    assert not status.wheel_installed(pbgdir, "pb7", repo, python)


def test_cleanup_waits_for_both_wheel_installs(tmp_path, monkeypatch):
    """A mixed source/wheel VPS keeps Rust until the second bot is converted."""
    home = tmp_path / "mani"
    pbgdir = home / "software/pbgui"
    pb7repo, pb7python, _ = make_runtime(home, "pb7", monkeypatch)
    pb8repo, pb8python, _ = make_runtime(home, "pb8", monkeypatch)
    (pbgdir / "pbgui.ini").write_text(
        f"[main]\nrole = vps\npb7dir = {pb7repo}\npb7venv = {pb7python}\n")
    monkeypatch.setattr(Path, "home", lambda: home)
    for repo in (pb7repo, pb8repo):
        (repo / "passivbot-rust/target").mkdir(parents=True)
    (home / ".cargo/bin").mkdir(parents=True)
    (home / ".cargo/bin/cargo").write_text("")
    (home / ".rustup/toolchains/1.90.0-x86_64-unknown-linux-gnu").mkdir(parents=True)
    (pbgdir / "data/state/pb8-wheel.json").unlink()
    assert status.cleanup_vps_rust(pbgdir)["cleaned"] is False
    assert (home / ".cargo").exists()
    make_runtime_receipt = {"format": 1, "product": "pb8", "commit": COMMIT,
                            "extension_sha256": hashlib.sha256(b"pb8").hexdigest()}
    (pbgdir / "data/state/pb8-wheel.json").write_text(json.dumps(make_runtime_receipt))
    (home / ".cargo/bin/other-tool").write_text("")
    assert status.cleanup_vps_rust(pbgdir)["cleaned"] is False
    assert (home / ".cargo").exists()
    (home / ".cargo/bin/other-tool").unlink()
    assert status.cleanup_vps_rust(pbgdir)["cleaned"] is True
    assert not (home / ".cargo").exists()
    assert not (home / ".rustup").exists()
    assert not any((repo / "passivbot-rust/target").exists() for repo in (pb7repo, pb8repo))


def test_cleanup_rejects_symlinked_rust_source(tmp_path, monkeypatch):
    """A checkout must not redirect cleanup into an unrelated Rust project."""
    home = tmp_path / "mani"
    pbgdir = home / "software/pbgui"
    repo, python, _ = make_runtime(home, "pb7", monkeypatch)
    (pbgdir / "pbgui.ini").write_text(
        f"[main]\nrole = vps\npb7dir = {repo}\npb7venv = {python}\n")
    monkeypatch.setattr(Path, "home", lambda: home)
    other = home / "other-rust"
    (other / "target").mkdir(parents=True)
    (repo / "passivbot-rust").symlink_to(other, target_is_directory=True)
    assert status.cleanup_vps_rust(pbgdir)["cleaned"] is False
    assert (other / "target").exists()


def test_prepared_wheel_cleanup_retires_rust_before_capacity_check(tmp_path, monkeypatch):
    """A verified PB7 bundle can retire its old build tools before installation."""
    home = tmp_path / "mani"
    pbgdir = home / "software/pbgui"
    repo, python, _ = make_runtime(home, "pb7", monkeypatch)
    (pbgdir / "pbgui.ini").write_text(
        f"[main]\nrole = vps\npb7dir = {repo}\npb7venv = {python}\n")
    (pbgdir / "data/state/pb7-wheel.json").unlink()
    (repo / "passivbot-rust/target").mkdir(parents=True)
    (home / ".rustup/toolchains/1.90.0-x86_64-unknown-linux-gnu").mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: home)
    bundle = make_prepared_bundle(tmp_path / "bundle")

    result = status.cleanup_vps_rust(
        pbgdir, prepared_product="pb7", prepared_bundle=bundle, prepared_commit=COMMIT)

    assert result["cleaned"] is True
    assert not (home / ".rustup").exists()
    assert not (repo / "passivbot-rust/target").exists()


def test_prepared_wheel_cleanup_preserves_rust_for_unconverted_pb8(tmp_path, monkeypatch):
    """A prepared PB7 wheel cannot retire tools while PB8 still needs them."""
    home = tmp_path / "mani"
    pbgdir = home / "software/pbgui"
    pb7repo, pb7python, _ = make_runtime(home, "pb7", monkeypatch)
    pb8repo, _pb8python, _ = make_runtime(home, "pb8", monkeypatch)
    (pbgdir / "pbgui.ini").write_text(
        f"[main]\nrole = vps\npb7dir = {pb7repo}\npb7venv = {pb7python}\n")
    (pbgdir / "data/state/pb7-wheel.json").unlink()
    (pbgdir / "data/state/pb8-wheel.json").unlink()
    (home / ".rustup/toolchains/1.90.0-x86_64-unknown-linux-gnu").mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: home)
    bundle = make_prepared_bundle(tmp_path / "bundle")

    result = status.cleanup_vps_rust(
        pbgdir, prepared_product="pb7", prepared_bundle=bundle, prepared_commit=COMMIT)

    assert result["cleaned"] is False
    assert "pb8 has no verified wheel install" in result["reason"]
    assert (home / ".rustup").exists()
    assert pb8repo.exists()


def test_prepared_wheel_cleanup_rejects_modified_bundle(tmp_path, monkeypatch):
    """A changed artifact cannot authorize early Rust removal."""
    home = tmp_path / "mani"
    pbgdir = home / "software/pbgui"
    repo, python, _ = make_runtime(home, "pb7", monkeypatch)
    (pbgdir / "pbgui.ini").write_text(
        f"[main]\nrole = vps\npb7dir = {repo}\npb7venv = {python}\n")
    (pbgdir / "data/state/pb7-wheel.json").unlink()
    (home / ".rustup/toolchains/1.90.0-x86_64-unknown-linux-gnu").mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: home)
    bundle = make_prepared_bundle(tmp_path / "bundle")
    (bundle / "passivbot.whl").write_bytes(b"modified")

    result = status.cleanup_vps_rust(
        pbgdir, prepared_product="pb7", prepared_bundle=bundle, prepared_commit=COMMIT)

    assert result["cleaned"] is False
    assert (home / ".rustup").exists()


@pytest.mark.parametrize("playbook", [
    "vps-update-pb7.yml", "master-update-pb.yml", "master-update-pb7.yml",
    "vps-update-pb.yml", "vps-switch-pb7-branch.yml",
    "master-switch-pb7-branch.yml",
])
def test_pb7_updates_use_controller_wheels_without_target_rust_build(playbook):
    """PB7 deploys checked binaries and never invokes target rustup or maturin."""
    content = Path(playbook).read_text()
    tasks = yaml.safe_load(content)[0]["tasks"]
    names = [task["name"] for task in tasks]
    prepare = names.index("Prepare central PB7 wheels before runtime changes")
    stop = next(i for i, name in enumerate(names) if name.startswith("Stop PBRun before"))
    assert prepare < stop
    assert "--only-binary=:all:" in content or "tasks/pb7_wheels_install.yml" in content
    assert "pb8_wheels.py runtime --product pb7" in content or "tasks/pb7_wheels_install.yml" in content
    assert "maturin" not in content
    assert "rustup update" not in content


def test_pb7_setup_imports_wheel_update():
    """Combined VPS setup installs PB7 through the same verified wheel path."""
    content = Path("vps-setup.yml").read_text()
    assert "import_playbook: vps-update-pb7.yml" in content
    assert "import_playbook: vps-update-pb8.yml" in content
    assert content.index("import_playbook: vps-update-pb7.yml") < content.index("import_playbook: vps-update-pb8.yml")


def test_linux_updates_preserve_wheel_only_vps_cleanup():
    """A later OS update must not reinstall Rust after verified wheel cleanup."""
    content = Path("vps-update.yml").read_text()
    assert "rustup" not in content
    assert "rustc" not in content
    assert "cargo" not in content


def test_python312_maintenance_reuses_verified_pb7_wheel_update():
    """The legacy venv migration recreates Python 3.12 without compiling on VPS."""
    wrapper = Path("vps-pb7-python312.yml").read_text()
    update = Path("vps-update-pb7.yml").read_text()
    assert "import_playbook: vps-update-pb7.yml" in wrapper
    assert "pb7_recreate_python312: true" in wrapper
    assert "Install and validate central PB7 wheels" in update
    assert "Remove old PB7 virtualenv for Python 3.12 migration" in Path("tasks/pb7_wheels_install.yml").read_text()
    assert "Prepare central PB7 wheels before runtime changes" in update


@pytest.mark.parametrize("playbook", ["vps-update-pb.yml", "master-update-pb.yml"])
def test_combined_update_skips_verified_pb7_runtime(playbook):
    """A PBGui-only change leaves verified PB7 bots and wheels alone."""
    spec = yaml.safe_load(Path(playbook).read_text())[0]
    tasks = {task["name"]: task for task in spec["tasks"]}
    assert "Inspect installed PB7 wheel receipt" in tasks
    decision = tasks["Decide whether PB7 requires an update"]["set_fact"]["pb7_update_required"]
    assert "checkout_required=true" in decision
    assert "pb7_wheel_status.stdout" in decision
    for name in (
        "Prepare central PB7 wheels before runtime changes",
        "Stop PBRun before changing PB7 checkout",
        "Stop PB7 processes before changing checkout",
        "Checkout verified pinned Passivbot v7",
        "Install and validate central PB7 wheels",
    ):
        assert "pb7_update_required" in tasks[name]["when"]


@pytest.mark.parametrize("playbook", [
    "vps-update-pb.yml", "vps-update-pb7.yml", "master-update-pb.yml", "master-update-pb7.yml",
])
def test_pb7_stop_schedules_recovery_even_on_later_failure(playbook):
    """Forced handlers restart PBRun after a post-stop installation failure."""
    spec = yaml.safe_load(Path(playbook).read_text())[0]
    stop = next(task for task in spec["tasks"] if task["name"].startswith("Stop PBRun before"))
    handlers = spec["handlers"]
    assert spec["force_handlers"] is True
    assert stop["changed_when"] is True
    assert stop["notify"] in {h.get("name") for h in handlers} | {h.get("listen") for h in handlers}
    assert not any(task["name"] == "Start PBRun after validated PB7 update" for task in spec["tasks"])


def test_wheel_source_is_fetched_on_controller():
    """A target host cannot supply the archive executed by the native builder."""
    for path in ("tasks/pb7_wheels_prepare.yml", "tasks/pb8_live_wheels_prepare.yml"):
        tasks = yaml.safe_load(Path(path).read_text())
        fetch = next(task for task in tasks if "source on the controller" in task["name"])
        assert fetch["delegate_to"] == "localhost"
        assert "source" in fetch["command"]["argv"]
        assert not any("fetch" in task for task in tasks)


@pytest.mark.parametrize("playbook", ["vps-switch-pb7-branch.yml", "master-switch-pb7-branch.yml"])
def test_pb7_branch_switch_confirms_controller_commit_and_recovers_pbrun(playbook):
    """A target branch ref cannot select build code or suppress PBRun recovery."""
    spec = yaml.safe_load(Path(playbook).read_text())[0]
    tasks = {task["name"]: task for task in spec["tasks"]}
    assert tasks["Pin selected PB7 wheel source commit"]["include_tasks"] == "tasks/pb7_branch_wheel_commit.yml"
    stop = tasks["Stop PBRun before changing PB7 checkout"]
    assert spec["force_handlers"] is True
    assert stop["changed_when"] is True
    assert stop["notify"] == "Recover PBRun after PB7 branch switch"
    assert any(h["name"] == stop["notify"] for h in spec["handlers"])
    assert "{{ pb7_wheel_source_commit }}" in tasks["Checkout target branch directly at verified commit"]["command"]
    assert not any(task["name"] == "Start PBRun after validated PB7 branch switch" for task in spec["tasks"])
    controller = yaml.safe_load(Path("tasks/pb7_branch_wheel_commit.yml").read_text())
    lookup = next(task for task in controller if task["name"] == "Resolve selected PB7 branch head on controller")
    assert lookup["delegate_to"] == "localhost"
    assert "resolved_commit.stdout == pb7_wheel_source_commit" in str(controller)


def test_branch_switches_require_explicit_remote_url_before_bot_stop():
    """A missing fork URL cannot silently select the official upstream source."""
    pb7 = yaml.safe_load(Path("tasks/pb7_branch_wheel_commit.yml").read_text())
    pb8 = yaml.safe_load(Path("tasks/pb8_live_wheels_prepare.yml").read_text())
    assert pb7[0]["name"] == "Require selected PB7 remote URL for branch switch"
    assert "remote_url != '' or use_pinned_ref" in pb7[0]["assert"]["that"]
    assert pb8[0]["name"] == "Require selected PB8 remote URL for branch switch"
    assert "pb8_remote_url_value != ''" in pb8[0]["assert"]["that"]
    for task in (pb7[0], pb8[0]):
        assert "before" in task["assert"]["fail_msg"]
        assert "fallback" in task["assert"]["fail_msg"]
    html = Path("frontend/vps_manager.html").read_text()
    assert "this URL is reported by that host" in html
