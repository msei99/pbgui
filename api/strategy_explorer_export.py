"""MP4 export helpers for the FastAPI Strategy Explorer Movie Builder."""

from __future__ import annotations

import importlib.metadata
import os
import queue
import shutil
import subprocess
import tempfile
import threading
from pathlib import Path
from typing import Any, Callable

SERVICE = "StrategyExplorerExport"
MAX_EXPORT_FRAMES = 2500

ProgressCallback = Callable[[float, str], None]
CancelCallback = Callable[[], bool]


def _parse_version(value: str | None) -> tuple[int, int, int]:
    """Parse a package version into a coarse three-part tuple."""
    try:
        parts = str(value or "").split("+")[0].split("-")[0].split(".")
        major = int(parts[0]) if len(parts) > 0 else 0
        minor = int(parts[1]) if len(parts) > 1 else 0
        patch = int(parts[2]) if len(parts) > 2 else 0
        return major, minor, patch
    except Exception:
        return 0, 0, 0


def _run_ffmpeg_probe(ffmpeg: str, args: list[str], *, timeout: int = 3) -> bool:
    """Return True when a short ffmpeg probe exits successfully."""
    try:
        result = subprocess.run(
            [ffmpeg, *args],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.returncode == 0
    except Exception:
        return False


def _ffmpeg_version_ok(ffmpeg: str) -> bool:
    """Return True when ffmpeg is executable."""
    return _run_ffmpeg_probe(ffmpeg, ["-version"], timeout=2)


def _ffmpeg_encoders(ffmpeg: str) -> str:
    """Return ffmpeg encoder listing as lower-case text."""
    try:
        result = subprocess.run(
            [ffmpeg, "-hide_banner", "-encoders"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return str(result.stdout or "").lower()
    except Exception:
        return ""


def _get_ffmpeg_exe() -> tuple[str, str]:
    """Return a usable ffmpeg executable and source label."""
    system_ffmpeg = shutil.which("ffmpeg")
    if system_ffmpeg and _ffmpeg_version_ok(system_ffmpeg):
        return str(system_ffmpeg), "system"

    try:
        import imageio_ffmpeg  # type: ignore

        bundled = str(imageio_ffmpeg.get_ffmpeg_exe())
        if bundled and _ffmpeg_version_ok(bundled):
            return bundled, "bundled"
    except Exception:
        pass

    raise RuntimeError("Could not locate ffmpeg. Install system ffmpeg or the 'imageio-ffmpeg' package.")


def _has_nvidia_runtime() -> bool:
    """Return True when an NVIDIA runtime device is visible."""
    try:
        return os.path.exists("/dev/nvidia0") or os.path.exists("/proc/driver/nvidia/version")
    except Exception:
        return False


def _qsv_works(ffmpeg: str) -> bool:
    """Return True when Intel QuickSync can initialize."""
    return _run_ffmpeg_probe(
        ffmpeg,
        [
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "nullsrc=s=128x72:d=0.1",
            "-c:v",
            "h264_qsv",
            "-frames:v",
            "1",
            "-f",
            "null",
            "-",
        ],
        timeout=3,
    )


def _vaapi_works(ffmpeg: str) -> bool:
    """Return True when VAAPI can initialize."""
    try:
        if not os.path.exists("/dev/dri/renderD128"):
            return False
    except Exception:
        return False
    return _run_ffmpeg_probe(
        ffmpeg,
        [
            "-hide_banner",
            "-loglevel",
            "error",
            "-vaapi_device",
            "/dev/dri/renderD128",
            "-f",
            "lavfi",
            "-i",
            "nullsrc=s=128x72:d=0.1",
            "-vf",
            "format=nv12,hwupload",
            "-c:v",
            "h264_vaapi",
            "-frames:v",
            "1",
            "-f",
            "null",
            "-",
        ],
        timeout=3,
    )


def detect_hw_encoder(ffmpeg: str, fallback_preset: str = "ultrafast") -> tuple[str, str]:
    """Detect the best available H.264 encoder and matching preset."""
    encoders = _ffmpeg_encoders(ffmpeg)
    if "h264_nvenc" in encoders and _has_nvidia_runtime():
        return "h264_nvenc", "p4"
    if "h264_qsv" in encoders and _qsv_works(ffmpeg):
        return "h264_qsv", "medium"
    if "h264_vaapi" in encoders and _vaapi_works(ffmpeg):
        return "h264_vaapi", "medium"
    if "h264_v4l2m2m" in encoders:
        return "h264_v4l2m2m", "medium"
    if "h264_videotoolbox" in encoders:
        return "h264_videotoolbox", "medium"
    return "libx264", str(fallback_preset or "ultrafast")


def available_video_codecs() -> list[dict[str, str]]:
    """Return available video codec choices for the frontend."""
    available: list[dict[str, str]] = []
    try:
        ffmpeg, _source = _get_ffmpeg_exe()
        encoders = _ffmpeg_encoders(ffmpeg)
        has_nvidia = _has_nvidia_runtime()
        qsv_ok = _qsv_works(ffmpeg) if "h264_qsv" in encoders else False
        vaapi_ok = _vaapi_works(ffmpeg) if "h264_vaapi" in encoders else False

        if vaapi_ok:
            if "av1_vaapi" in encoders:
                available.append({"id": "av1_vaapi", "label": "AV1 (VAAPI) - best compression (Linux GPU)"})
            if "hevc_vaapi" in encoders:
                available.append({"id": "hevc_vaapi", "label": "H.265/HEVC (VAAPI) - high quality (Linux GPU)"})
            if "h264_vaapi" in encoders:
                available.append({"id": "h264_vaapi", "label": "H.264 (VAAPI) - fast and compatible (Linux GPU)"})

        if "h264_qsv" in encoders:
            if qsv_ok:
                if "av1_qsv" in encoders:
                    available.append({"id": "av1_qsv", "label": "AV1 (Intel QuickSync) - best compression"})
                if "hevc_qsv" in encoders:
                    available.append({"id": "hevc_qsv", "label": "H.265/HEVC (Intel QuickSync) - high quality"})
                available.append({"id": "h264_qsv", "label": "H.264 (Intel QuickSync) - fast and compatible"})
            else:
                available.append({"id": "h264_qsv", "label": "H.264 (Intel QuickSync) - not working on this host"})

        if has_nvidia:
            if "h264_nvenc" in encoders:
                available.append({"id": "h264_nvenc", "label": "H.264 (NVIDIA NVENC) - fast and compatible"})
            if "hevc_nvenc" in encoders:
                available.append({"id": "hevc_nvenc", "label": "H.265/HEVC (NVIDIA NVENC) - high quality"})
            if "av1_nvenc" in encoders:
                available.append({"id": "av1_nvenc", "label": "AV1 (NVIDIA NVENC) - best compression"})

        if "h264_v4l2m2m" in encoders:
            available.append({"id": "h264_v4l2m2m", "label": "H.264 (V4L2 Hardware) - Linux"})
        if "h264_videotoolbox" in encoders:
            available.append({"id": "h264_videotoolbox", "label": "H.264 (VideoToolbox) - macOS"})
        if "hevc_videotoolbox" in encoders:
            available.append({"id": "hevc_videotoolbox", "label": "H.265/HEVC (VideoToolbox) - macOS"})
    except Exception:
        pass

    available.append({"id": "libx264", "label": "H.264 (libx264) - CPU software"})
    available.append({"id": "libx265", "label": "H.265/HEVC (libx265) - CPU software"})
    return available


def video_encoder_info() -> dict[str, str]:
    """Return current encoder detection details."""
    try:
        ffmpeg, source = _get_ffmpeg_exe()
        encoders_output = _ffmpeg_encoders(ffmpeg)
        codec, _preset = detect_hw_encoder(ffmpeg)
        if codec == "libx264":
            h264_encoders = []
            try:
                result = subprocess.run(
                    [ffmpeg, "-hide_banner", "-encoders"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                for line in str(result.stdout or "").split("\n"):
                    if "h264" in line.lower() and line.strip().startswith("V"):
                        parts = line.split()
                        if len(parts) >= 2:
                            h264_encoders.append(parts[1])
            except Exception:
                pass
            suffix = f" | available: {', '.join(h264_encoders)}" if h264_encoders else ""
            label = f"Software encoder: libx264 (CPU) | ffmpeg: {source}{suffix}"
        else:
            label = f"Hardware encoder: {codec} | ffmpeg: {source}"
        return {"ok": "true", "ffmpeg": ffmpeg, "source": source, "auto_codec": codec, "label": label, "encoders": encoders_output}
    except Exception as exc:
        return {"ok": "false", "ffmpeg": "", "source": "", "auto_codec": "libx264", "label": f"Software encoder: libx264 (CPU) | error: {exc}", "encoders": ""}


def movie_export_options() -> dict[str, Any]:
    """Return presets, defaults, and detected codec options for Movie export."""
    info = video_encoder_info()
    auto_codec = str(info.get("auto_codec") or "libx264")
    codecs = [{"id": "auto", "label": f"Auto (detect best available) - {auto_codec}"}]
    codecs.extend(available_video_codecs())
    return {
        "ok": True,
        "defaults": {"preset": "Balanced", "width": 1600, "height": 800, "scale": 1, "crf": 18, "ffmpeg_preset": "veryfast", "codec": "auto"},
        "presets": {
            "Fast": {"preset": "Fast", "width": 1280, "height": 720, "scale": 1, "crf": 23, "ffmpeg_preset": "ultrafast", "codec": "auto"},
            "Balanced": {"preset": "Balanced", "width": 1600, "height": 800, "scale": 1, "crf": 18, "ffmpeg_preset": "veryfast", "codec": "auto"},
            "Quality": {"preset": "Quality", "width": 1920, "height": 1080, "scale": 1, "crf": 16, "ffmpeg_preset": "fast", "codec": "auto"},
            "Custom": {"preset": "Custom"},
        },
        "codecs": codecs,
        "encoder": info,
    }


def _apply_plotly_frame_inplace(fig: Any, frame: Any) -> None:
    """Apply Plotly frame data/layout onto an existing figure in-place."""
    try:
        frame_data = list(getattr(frame, "data", None) or [])
    except Exception:
        frame_data = []
    try:
        frame_traces = list(getattr(frame, "traces", None) or [])
    except Exception:
        frame_traces = []

    if frame_data:
        if frame_traces:
            for trace_obj, trace_idx in zip(frame_data, frame_traces):
                try:
                    idx = int(trace_idx)
                except Exception:
                    continue
                if idx < 0:
                    continue
                try:
                    if idx < len(fig.data):
                        try:
                            payload = trace_obj.to_plotly_json()
                        except Exception:
                            payload = trace_obj
                        if isinstance(payload, dict):
                            payload.pop("type", None)
                            fig.data[idx].update(payload)
                        else:
                            fig.data[idx] = trace_obj
                    else:
                        fig.add_trace(trace_obj)
                except Exception:
                    pass
        else:
            for idx, trace_obj in enumerate(frame_data):
                try:
                    if idx < len(fig.data):
                        try:
                            payload = trace_obj.to_plotly_json()
                        except Exception:
                            payload = trace_obj
                        if isinstance(payload, dict):
                            payload.pop("type", None)
                            fig.data[idx].update(payload)
                        else:
                            fig.data[idx] = trace_obj
                    else:
                        fig.add_trace(trace_obj)
                except Exception:
                    pass

    try:
        frame_layout = getattr(frame, "layout", None)
        if frame_layout:
            fig.update_layout(frame_layout)
    except Exception:
        pass


def _strip_animation_controls_for_export(fig: Any) -> None:
    """Remove interactive animation controls before rendering frames."""
    try:
        fig.update_layout(updatemenus=[], sliders=[])
    except Exception:
        pass
    try:
        fig.layout.updatemenus = []
    except Exception:
        pass
    try:
        fig.layout.sliders = []
    except Exception:
        pass
    try:
        current = getattr(getattr(fig.layout, "margin", None), "b", None)
        current_i = int(current) if current is not None else None
        target = 70
        fig.update_layout(margin=dict(b=target if current_i is None else min(current_i, target)))
    except Exception:
        pass
    try:
        fig.update_xaxes(rangeslider=dict(visible=False))
    except Exception:
        pass


def _apply_export_theme(fig: Any) -> None:
    """Ensure exported images have a solid dark background."""
    try:
        fig.update_layout(template="plotly_dark", paper_bgcolor="#1a1d24", plot_bgcolor="#1a1d24", font=dict(color="#fafafa"))
    except Exception:
        pass
    try:
        fig.update_xaxes(showgrid=True)
        fig.update_yaxes(showgrid=True)
    except Exception:
        pass


def _export_progress(progress_cb: ProgressCallback | None, progress: float, message: str) -> None:
    """Emit bounded export progress."""
    if not callable(progress_cb):
        return
    try:
        progress_cb(max(0.0, min(1.0, float(progress))), str(message or ""))
    except Exception:
        pass


def _check_cancelled(cancel_cb: CancelCallback | None) -> None:
    """Raise RuntimeError when the caller requested cancellation."""
    if not callable(cancel_cb):
        return
    try:
        if bool(cancel_cb()):
            raise RuntimeError("Movie export stopped.")
    except RuntimeError:
        raise
    except Exception:
        pass


def _normalize_export_options(options: dict[str, Any] | None) -> dict[str, Any]:
    """Return sanitized Movie export options."""
    opts = dict(options or {})
    preset = str(opts.get("preset") or "Balanced")
    try:
        width = int(opts.get("width") or 1600)
    except Exception:
        width = 1600
    try:
        height = int(opts.get("height") or 800)
    except Exception:
        height = 800
    try:
        scale = int(opts.get("scale") or 1)
    except Exception:
        scale = 1
    try:
        crf = int(opts.get("crf") or 18)
    except Exception:
        crf = 18
    try:
        fps = int(opts.get("fps") or 15)
    except Exception:
        fps = 15
    ffmpeg_preset = str(opts.get("ffmpeg_preset") or "veryfast")
    codec = str(opts.get("codec") or "auto")
    return {
        "preset": preset,
        "width": max(640, min(3840, width)) // 2 * 2,
        "height": max(360, min(2160, height)) // 2 * 2,
        "scale": max(1, min(4, scale)),
        "crf": max(0, min(51, crf)),
        "fps": max(1, min(120, fps)),
        "ffmpeg_preset": ffmpeg_preset if ffmpeg_preset in {"ultrafast", "superfast", "veryfast", "faster", "fast", "medium", "slow"} else "veryfast",
        "codec": codec,
    }


def export_plotly_animation_to_mp4(
    figure_json: dict[str, Any],
    *,
    options: dict[str, Any] | None = None,
    progress_cb: ProgressCallback | None = None,
    cancel_cb: CancelCallback | None = None,
) -> tuple[bytes, dict[str, Any]]:
    """Render a Plotly animation figure JSON object to MP4 bytes."""
    if not isinstance(figure_json, dict):
        raise RuntimeError("Movie export requires a Plotly figure payload.")

    try:
        import plotly.graph_objects as go  # type: ignore
    except Exception as exc:
        raise RuntimeError("Plotly is required for video export.") from exc

    try:
        kaleido_version = importlib.metadata.version("kaleido")
    except Exception:
        kaleido_version = None
    if not kaleido_version:
        raise RuntimeError("Plotly image export requires the pip package 'kaleido'. Install kaleido to export MP4.")
    if _parse_version(kaleido_version) >= (1, 0, 0):
        raise RuntimeError(f"Movie export requires kaleido==0.2.1 for pip-only export. Found kaleido=={kaleido_version}.")

    opts = _normalize_export_options(options)
    fig = go.Figure(figure_json)
    frames = list(getattr(fig, "frames", None) or [])
    if not frames:
        raise RuntimeError("This figure has no animation frames to export.")
    if len(frames) > MAX_EXPORT_FRAMES:
        raise RuntimeError(f"Movie export is limited to {MAX_EXPORT_FRAMES} frames. Reduce Frames or Step Size before exporting.")

    ffmpeg, ffmpeg_source = _get_ffmpeg_exe()
    requested_codec = str(opts["codec"] or "auto")
    if requested_codec and requested_codec != "auto":
        codec = requested_codec
        preset = str(opts["ffmpeg_preset"] if requested_codec.startswith("lib") else "medium")
    else:
        codec, preset = detect_hw_encoder(ffmpeg, str(opts["ffmpeg_preset"]))

    if codec.endswith("_nvenc") and not _has_nvidia_runtime():
        _export_progress(progress_cb, 0.0, f"{codec} selected but NVIDIA runtime is not present. Falling back.")
        codec, preset = ("h264_vaapi", "medium") if _vaapi_works(ffmpeg) else ("libx264", str(opts["ffmpeg_preset"]))
    if codec in {"h264_qsv", "hevc_qsv", "av1_qsv"} and not _qsv_works(ffmpeg):
        _export_progress(progress_cb, 0.0, f"{codec} selected but QuickSync initialization failed. Falling back.")
        codec, preset = ("h264_vaapi", "medium") if _vaapi_works(ffmpeg) else ("libx264", str(opts["ffmpeg_preset"]))
    if codec in {"h264_vaapi", "hevc_vaapi", "av1_vaapi"} and not _vaapi_works(ffmpeg):
        _export_progress(progress_cb, 0.0, f"{codec} selected but VAAPI initialization failed. Falling back to libx264.")
        codec, preset = "libx264", str(opts["ffmpeg_preset"])

    _check_cancelled(cancel_cb)
    base_fig = go.Figure(fig)
    base_fig.frames = []
    _apply_export_theme(base_fig)
    _strip_animation_controls_for_export(base_fig)

    with tempfile.TemporaryDirectory(prefix="pbgui_movie_export_") as tmpdir:
        out_mp4 = Path(tmpdir) / "movie.mp4"
        cmd = [
            ffmpeg,
            "-y",
            "-hide_banner",
            "-nostats",
            "-loglevel",
            "info",
            "-progress",
            "pipe:2",
        ]
        if codec in {"h264_vaapi", "hevc_vaapi", "av1_vaapi"} and os.path.exists("/dev/dri/renderD128"):
            cmd.extend(["-vaapi_device", "/dev/dri/renderD128"])
        cmd.extend(["-f", "image2pipe", "-vcodec", "png", "-framerate", str(opts["fps"]), "-i", "pipe:0", "-c:v", codec])

        crf = int(opts["crf"])
        if codec in {"h264_nvenc", "hevc_nvenc", "av1_nvenc"}:
            cmd.extend(["-preset", preset if preset != "medium" else "p4", "-cq", str(crf), "-b:v", "0"])
        elif codec in {"h264_qsv", "hevc_qsv", "av1_qsv"}:
            cmd.extend(["-preset", preset, "-global_quality", str(crf + 5)])
        elif codec == "h264_v4l2m2m":
            cmd.extend(["-b:v", f"{max(500, int(5000 - (crf * 80)))}k"])
        elif codec in {"h264_videotoolbox", "hevc_videotoolbox"}:
            cmd.extend(["-q:v", str(int(crf * 100 / 51))])
        elif codec in {"h264_vaapi", "hevc_vaapi", "av1_vaapi"}:
            cmd.extend(["-vf", "format=nv12,hwupload", "-qp", str(crf)])
        elif codec in {"libx264", "libx265"}:
            cmd.extend(["-preset", preset, "-crf", str(crf), "-threads", "0"])
        else:
            cmd.extend(["-b:v", f"{max(500, int(5000 - (crf * 80)))}k"])

        pix_fmt = "nv12" if codec in {"h264_qsv", "hevc_qsv", "av1_qsv", "h264_vaapi", "hevc_vaapi", "av1_vaapi"} else "yuv420p"
        cmd.extend(["-pix_fmt", pix_fmt, "-movflags", "+faststart", str(out_mp4)])

        try:
            proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        except Exception as exc:
            raise RuntimeError(f"Failed to start ffmpeg: {exc}") from exc
        if proc.stdin is None:
            raise RuntimeError("Failed to open ffmpeg stdin for video export.")

        stderr_queue: queue.Queue[bytes] = queue.Queue()

        def _read_stderr() -> None:
            try:
                if proc.stderr:
                    for chunk in iter(lambda: proc.stderr.read(4096), b""):
                        if chunk:
                            stderr_queue.put(chunk)
            except Exception:
                pass

        stderr_thread = threading.Thread(target=_read_stderr, daemon=True)
        stderr_thread.start()
        stderr_lines: list[str] = []
        stderr_buf = ""

        def _drain_stderr() -> str:
            nonlocal stderr_buf
            collected: list[str] = []
            while True:
                try:
                    chunk = stderr_queue.get_nowait()
                except Exception:
                    break
                text = chunk.decode("utf-8", errors="replace")
                stderr_buf += text
                while "\n" in stderr_buf:
                    line, stderr_buf = stderr_buf.split("\n", 1)
                    stderr_lines.append(line + "\n")
                collected.append(text)
            return "".join(collected)

        def _stderr_has_fatal() -> bool:
            tail = "".join(stderr_lines[-80:]).lower()
            return any(
                marker in tail
                for marker in (
                    "error while opening encoder",
                    "error during set display handle",
                    "device failed",
                    "cannot load libcuda.so.1",
                    "nothing was written into output file",
                    "conversion failed",
                )
            )

        def _render_png(fig_obj: Any) -> bytes:
            try:
                return fig_obj.to_image(format="png", width=int(opts["width"]), height=int(opts["height"]), scale=int(opts["scale"]))
            except Exception as exc:
                msg = str(exc)
                if "kaleido" in msg.lower():
                    raise RuntimeError("Plotly image export requires the 'kaleido' package. Install it to export video.") from exc
                raise RuntimeError(f"Failed to render frame image: {exc}") from exc

        total = len(frames) + 1
        try:
            work_fig = go.Figure(base_fig)
            _export_progress(progress_cb, 0.0, f"Rendering frame 0/{total - 1} ({codec})")
            _check_cancelled(cancel_cb)
            if proc.poll() is not None:
                _drain_stderr()
                raise BrokenPipeError("ffmpeg exited before receiving frames")
            proc.stdin.write(_render_png(work_fig))
            try:
                proc.stdin.flush()
            except Exception:
                pass
            _drain_stderr()
            if _stderr_has_fatal():
                raise BrokenPipeError("ffmpeg reported fatal error")

            for idx, frame in enumerate(frames, start=1):
                _check_cancelled(cancel_cb)
                _export_progress(progress_cb, float(idx) / float(max(1, total)), f"Rendering frame {idx}/{total - 1} ({codec})")
                _apply_plotly_frame_inplace(work_fig, frame)
                if proc.poll() is not None:
                    _drain_stderr()
                    raise BrokenPipeError("ffmpeg exited early")
                proc.stdin.write(_render_png(work_fig))
                if idx % 3 == 0:
                    _drain_stderr()
                    if _stderr_has_fatal():
                        raise BrokenPipeError("ffmpeg reported fatal error")

            _export_progress(progress_cb, 0.98, f"Encoding MP4 ({codec})")
            try:
                proc.stdin.close()
            except Exception:
                pass
            return_code = proc.wait()
            stderr_output = "".join(stderr_lines) + _drain_stderr()
            if return_code != 0:
                raise RuntimeError(f"ffmpeg failed to encode MP4 (exit code {return_code}):\n{stderr_output}")
        except (BrokenPipeError, IOError) as exc:
            stderr_output = "".join(stderr_lines) + _drain_stderr()
            raise RuntimeError(f"ffmpeg closed pipe during Movie export:\n{stderr_output}") from exc
        finally:
            try:
                if proc.stderr is not None:
                    proc.stderr.close()
            except Exception:
                pass
            try:
                if proc.poll() is None:
                    proc.kill()
            except Exception:
                pass

        _export_progress(progress_cb, 1.0, "Movie export ready.")
        return out_mp4.read_bytes(), {"codec": codec, "preset": preset, "ffmpeg": ffmpeg_source, "frames": len(frames), "options": opts}
