"""Offline GPU capability and alias checks with isolated PB8 dependencies."""

import sys
from types import ModuleType, SimpleNamespace

import pytest

import pb8_config_helper as helper


@pytest.fixture
def gpu_runtime(monkeypatch):
    """Install fake optional dependencies; never import or probe real GPU hardware."""
    for name in ("optimization", "optimization.gpu"):
        package = ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)
    available = {"mps": False, "cuda": True, "torch": True, "cupy": True}
    torch = SimpleNamespace(
        backends=SimpleNamespace(mps=SimpleNamespace(
            is_available=lambda: available["mps"], is_built=lambda: True,
        )),
        cuda=SimpleNamespace(
            is_available=lambda: available["cuda"], current_device=lambda: 0,
            get_device_name=lambda _index: "Test NVIDIA GPU",
        ),
    )
    cupy = SimpleNamespace(cuda=SimpleNamespace(
        runtime=SimpleNamespace(getDeviceCount=lambda: 1),
        nvrtc=SimpleNamespace(getVersion=lambda: (12, 8)),
    ))
    native = ModuleType("optimization.gpu.runtime")

    def gpu_device(torch_module):
        """Match PB8's MPS-first automatic device selection."""
        if torch_module.backends.mps.is_available():
            return "mps"
        if torch_module.cuda.is_available():
            return "cuda"
        raise RuntimeError("No device")

    native.gpu_device = gpu_device
    monkeypatch.setitem(sys.modules, native.__name__, native)
    monkeypatch.setitem(sys.modules, "torch", torch)
    monkeypatch.setitem(sys.modules, "cupy", cupy)
    monkeypatch.setattr(helper.importlib.util, "find_spec", lambda name: object() if available.get(name) else None)
    monkeypatch.setattr(helper.platform, "system", lambda: "Linux")
    monkeypatch.setattr(helper.platform, "machine", lambda: "x86_64")
    return available, torch, cupy, native


@pytest.mark.parametrize("device", ["cuda", "mps", "legacy_mps"])
def test_gpu_runtime_accepts_native_cuda_and_mps(gpu_runtime, monkeypatch, device):
    """New CUDA and both old/new MPS contracts remain available."""
    available, _torch, _cupy, _native = gpu_runtime
    if device != "cuda":
        available["mps"] = True
        monkeypatch.setattr(helper.platform, "system", lambda: "Darwin")
        monkeypatch.setattr(helper.platform, "machine", lambda: "arm64")
    if device == "legacy_mps":
        monkeypatch.delitem(sys.modules, "optimization.gpu.runtime")
    runtime = helper._gpu_runtime_contract(["gpu"])
    assert runtime["device_available"] is True
    assert runtime["reason_code"] == "available"
    assert runtime["accelerator"] == ("nvidia_cuda" if device == "cuda" else "apple_mps")
    if device == "cuda":
        assert runtime["device_name"] == "Test NVIDIA GPU"


@pytest.mark.parametrize(
    ("missing", "reason"),
    [("torch", "torch_not_installed"), ("cupy", "cupy_not_installed"), ("cuda", "cuda_unavailable")],
)
def test_gpu_runtime_rejects_missing_cuda_components(gpu_runtime, missing, reason):
    """Torch alone must not make incomplete CUDA installations runnable."""
    available, *_rest = gpu_runtime
    available[missing] = False
    runtime = helper._gpu_runtime_contract(["gpu"])
    assert runtime["device_available"] is False
    assert runtime["reason_code"] == reason


def test_old_pb8_cannot_claim_cuda_support(gpu_runtime, monkeypatch):
    """A CUDA-enabled Torch cannot upgrade an MPS-only PB8 implementation."""
    monkeypatch.delitem(sys.modules, "optimization.gpu.runtime")
    runtime = helper._gpu_runtime_contract(["gpu"])
    assert runtime["device_available"] is False
    assert runtime["reason_code"] == "pb8_cuda_not_supported"


def test_cuda_compiler_failure_blocks_preflight(gpu_runtime):
    """Missing NVRTC must fail before native preparation or queue materialization."""
    _available, _torch, cupy, _native = gpu_runtime

    def fail():
        """Simulate a missing runtime compiler library without exposing its payload."""
        raise OSError("private diagnostic must not be exposed")

    cupy.cuda.nvrtc.getVersion = fail
    runtime = helper._gpu_runtime_contract(["gpu"])
    assert runtime["reason_code"] == "cuda_runtime_unavailable"
    assert "private diagnostic" not in runtime["reason"]
    with pytest.raises(RuntimeError, match="CuPy/CUDA"):
        helper._optimize_preflight({"backends": ["gpu"]}, {"optimize": {"backend": "gpu"}})


def test_cpu_preflight_and_unregistered_gpu_do_not_probe_torch(monkeypatch):
    """CPU-only entry points remain independent of optional GPU dependencies."""
    def fail(*_args):
        """Reject any optional dependency lookup."""
        raise AssertionError("Unexpected GPU dependency probe")

    monkeypatch.setattr(helper.importlib.util, "find_spec", fail)
    assert helper._gpu_runtime_contract(["pymoo"])["reason_code"] == "backend_not_registered"
    assert helper._optimize_preflight({}, {"optimize": {"backend": "pymoo"}})["valid"]


def test_gpu_metric_aliases_follow_native_validation(gpu_runtime, monkeypatch):
    """Expose accepted aliases without enabling exact-only or unsupported metrics."""
    metrics = ModuleType("optimization.gpu.metrics")
    metrics.SUPPORTED_METRICS = ("pnl_ratio_long_short",)

    def validate(names):
        """Model the installed PB8 alias policy, including rejection."""
        if names[0] not in {"pnl_ratio_long_short", "long_short_profit_ratio"}:
            raise ValueError("Unsupported GPU metric")
        return frozenset({"pnl_ratio_long_short"})

    metrics.validate_gpu_metric_names = validate
    registry = ModuleType("optimization.gpu.metric_registry")
    registry.GPU_EXACT_ONLY_METRICS = {"exact_only"}
    monkeypatch.setitem(sys.modules, metrics.__name__, metrics)
    monkeypatch.setitem(sys.modules, registry.__name__, registry)
    monkeypatch.setattr(helper, "_gpu_effective_defaults", lambda: {})
    contract = helper._optimizer_backend_contract(
        ["pymoo", "gpu"], ["long_short_profit_ratio", "exact_only", "unknown"],
    )
    assert contract["items"]["gpu"]["available"] is True
    assert contract["metric_sets"]["gpu_proxy"] == ["long_short_profit_ratio", "pnl_ratio_long_short"]


def test_cuda_preflight_delegates_preparation_and_scope(gpu_runtime, monkeypatch):
    """An available CUDA runtime reaches PB8's native config and metric validation."""
    package = ModuleType("optimization.backends")
    package.__path__ = []
    monkeypatch.setitem(sys.modules, package.__name__, package)
    backend = ModuleType("optimization.backends.gpu_backend")
    calls = []
    backend.materialize_gpu_preparation_config = lambda config: config
    backend.validate_gpu_preparation_scope = lambda config, suite: calls.append((config, suite))
    metrics = ModuleType("optimization.gpu.metrics")
    metrics.validate_gpu_metric_names = lambda names: calls.append(names)
    suite = ModuleType("suite_runner")
    suite.extract_suite_config = lambda config, _path: {"suite": True}
    for module in (backend, metrics, suite):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    config = {"optimize": {"backend": "gpu", "scoring": [{"metric": "long_short_profit_ratio"}]}}
    result = helper._optimize_preflight({"backends": ["gpu"]}, config)
    assert result["valid"] is True
    assert result["runtime"]["accelerator"] == "nvidia_cuda"
    assert calls == [["long_short_profit_ratio"], (config, {"suite": True})]
