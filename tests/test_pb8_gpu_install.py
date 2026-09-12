"""Optional GPU installation selection without installers, hardware, or network."""

from types import SimpleNamespace

import pytest

import pb8_gpu_install as installer


@pytest.mark.parametrize(
    ("system", "machine", "nvidia", "requested", "expected"),
    [
        ("Linux", "x86_64", False, "auto", "full"),
        ("Linux", "x86_64", True, "auto", "full,gpu-cuda"),
        ("Linux", "x86_64", True, "cpu", "full"),
        ("Linux", "x86_64", False, "cuda", "full,gpu-cuda"),
        ("Darwin", "arm64", False, "auto", "full,gpu-mps"),
        ("Darwin", "x86_64", False, "auto", "full"),
        ("Windows", "AMD64", True, "auto", "full"),
    ],
)
def test_gpu_install_profile_matches_target(tmp_path, monkeypatch, system, machine, nvidia, requested, expected):
    """CPU hosts stay lightweight while compatible GPU hosts receive their own extra."""
    (tmp_path / "setup.py").write_text("setup(extras_require={'gpu-mps': [], 'gpu-cuda': []})")
    monkeypatch.setattr(installer.platform, "system", lambda: system)
    monkeypatch.setattr(installer.platform, "machine", lambda: machine)
    monkeypatch.setattr(installer, "_has_nvidia_device", lambda: nvidia)
    assert installer.pb8_gpu_extras(tmp_path, requested) == expected


def test_old_pb8_gpu_install_auto_falls_back_but_explicit_cuda_fails(tmp_path, monkeypatch):
    """Old revisions never receive an unsupported CUDA extra by accident."""
    (tmp_path / "setup.py").write_text("setup(extras_require={'gpu-mps': []})")
    monkeypatch.setattr(installer.platform, "system", lambda: "Linux")
    monkeypatch.setattr(installer, "_has_nvidia_device", lambda: True)
    assert installer.pb8_gpu_extras(tmp_path) == "full"
    with pytest.raises(ValueError, match="does not declare gpu-cuda"):
        installer.pb8_gpu_extras(tmp_path, "cuda")


@pytest.mark.parametrize("requested", ["cuda", "mps", "invalid"])
def test_gpu_install_rejects_unsupported_platform_or_profile(tmp_path, monkeypatch, requested):
    """Explicit GPU choices do not silently downgrade on unsupported platforms."""
    monkeypatch.setattr(installer.platform, "system", lambda: "Windows")
    with pytest.raises(ValueError):
        installer.pb8_gpu_extras(tmp_path, requested)


@pytest.mark.parametrize("returncode,output,expected", [(0, "NVIDIA GPU\n", True), (0, "", False), (1, "error", False)])
def test_nvidia_detection_requires_a_working_driver(monkeypatch, returncode, output, expected):
    """The presence of nvidia-smi alone does not trigger a CUDA installation."""
    monkeypatch.setattr(installer.shutil, "which", lambda _name: "/test/nvidia-smi")

    def probe(argv, **kwargs):
        """Return a driver probe result without running a process."""
        assert argv == ["/test/nvidia-smi", "--query-gpu=name", "--format=csv,noheader"]
        assert kwargs["timeout"] == 5
        return SimpleNamespace(returncode=returncode, stdout=output)

    monkeypatch.setattr(installer.subprocess, "run", probe)
    assert installer._has_nvidia_device() is expected
