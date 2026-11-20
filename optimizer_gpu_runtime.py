"""Shared accelerator probe executed only inside the selected bot environment."""
import importlib.util
import platform

SERVICE = "Optimize"

def gpu_runtime_contract(backends: list[str]) -> dict:
    """Probe the installed PB8 accelerator inside its isolated virtualenv."""
    registered = "gpu" in backends
    runtime = {
        "accelerator": "unknown",
        "platform": platform.system(),
        "machine": platform.machine(),
        "dependency": "torch",
        "dependency_installed": False,
        "backend_built": False,
        "device_available": False,
        "device_name": "",
        "reason_code": "backend_not_registered",
        "reason": "The installed PB8 runtime does not register the GPU backend.",
    }
    if not registered:
        return runtime
    apple = runtime["platform"] == "Darwin" and runtime["machine"] == "arm64"
    if not apple and runtime["platform"] != "Linux":
        runtime.update(
            reason_code="unsupported_platform",
            reason="PB8 GPU optimization requires Apple Silicon or NVIDIA CUDA on Linux/WSL2.",
        )
        return runtime
    if importlib.util.find_spec("torch") is None:
        runtime.update(
            reason_code="torch_not_installed",
            reason="Install PB8's optional gpu-mps (Apple) or gpu-cuda (NVIDIA) dependencies in the PB8 virtualenv.",
        )
        return runtime
    runtime["dependency_installed"] = True
    try:
        import torch

        mps = getattr(getattr(torch, "backends", None), "mps", None)
        try:
            native_runtime = importlib.import_module("optimization.gpu.runtime")
        except ModuleNotFoundError as exc:
            if exc.name not in {"optimization.gpu.runtime", "optimization.gpu", "optimization"}:
                raise
            native_runtime = None
        if native_runtime is None:
            # Older PB8 revisions only implement MPS, even if Torch supports CUDA.
            if not apple:
                runtime.update(
                    reason_code="pb8_cuda_not_supported",
                    reason="This PB8 revision only supports Apple MPS. Update PB8 for NVIDIA CUDA support.",
                )
                return runtime
            device = "mps"
        else:
            try:
                device = native_runtime.gpu_device(torch)
            except RuntimeError:
                runtime.update(
                    reason_code="mps_unavailable" if apple else "cuda_unavailable",
                    reason=("Apple MPS is unavailable in the PB8 process." if apple else
                            "NVIDIA CUDA is unavailable in the PB8 process. Check the NVIDIA driver and CUDA-enabled PyTorch in the PB8 virtualenv."),
                )
                return runtime
        if device == "mps":
            runtime["accelerator"] = "apple_mps"
            runtime["backend_built"] = bool(mps and (not hasattr(mps, "is_built") or mps.is_built()))
            runtime["device_available"] = bool(mps and mps.is_available())
            if not runtime["backend_built"]:
                runtime.update(reason_code="mps_not_built", reason="The installed PyTorch build has no Apple MPS backend.")
            elif not runtime["device_available"]:
                runtime.update(reason_code="mps_unavailable", reason="Apple MPS is unavailable in the PB8 process.")
            else:
                runtime.update(reason_code="available", reason="Apple MPS is available.")
            return runtime
        if device != "cuda" or runtime["platform"] != "Linux":
            runtime.update(reason_code="unsupported_platform", reason="This PB8 GPU runtime is not supported on this host.")
            return runtime
        runtime.update(accelerator="nvidia_cuda", dependency="torch, cupy", backend_built=True)
        if importlib.util.find_spec("cupy") is None:
            runtime.update(
                dependency_installed=False,
                reason_code="cupy_not_installed",
                reason="NVIDIA CUDA requires PB8's gpu-cuda dependencies, including CuPy, in the PB8 virtualenv.",
            )
            return runtime
        try:
            import cupy

            device_index = torch.cuda.current_device()
            if cupy.cuda.runtime.getDeviceCount() <= device_index:
                raise RuntimeError("CuPy cannot access the selected CUDA device")
            # Probe the runtime compiler too; Torch availability alone is insufficient.
            cupy.cuda.nvrtc.getVersion()
            runtime["device_name"] = str(torch.cuda.get_device_name(device_index))
        except Exception as exc:
            runtime.update(
                reason_code="cuda_runtime_unavailable",
                reason=f"PB8's CuPy/CUDA runtime could not be loaded ({type(exc).__name__}). Reinstall the gpu-cuda profile and check the NVIDIA driver.",
            )
            return runtime
        runtime.update(device_available=True, reason_code="available", reason="NVIDIA CUDA is available.")
    except Exception as exc:
        runtime.update(
            reason_code="torch_import_failed",
            reason=f"The PB8 PyTorch runtime could not be loaded: {type(exc).__name__}",
        )
        return runtime
    return runtime

