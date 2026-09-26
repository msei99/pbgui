# Build candidate: GPU validation is performed on the rented worker.
FROM rust:1.90-bookworm AS rust
FROM python:3.12-bookworm AS build
COPY --from=rust /usr/local/cargo /usr/local/cargo
COPY --from=rust /usr/local/rustup /usr/local/rustup
ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo
ENV PATH=/usr/local/cargo/bin:$PATH
RUN apt-get update && apt-get install -y --no-install-recommends git build-essential && rm -rf /var/lib/apt/lists/*
# Official enarjord/passivbot source, pinned independently of dependency wheels.
ARG PB8_REVISION=903ed11153ce82d1b6760604eaa3a553309a752a
RUN git clone https://github.com/enarjord/passivbot.git /opt/passivbot \
    && cd /opt/passivbot && git checkout --detach "$PB8_REVISION" \
    && git rev-parse HEAD > /opt/pb8-revision \
    && python -m pip install --no-cache-dir '.[full,gpu-cuda]' \
    && python -m pip freeze > /opt/pb8-pip-freeze.txt
# pip/maturin builds the extension but does not run PB8's rustbuild.sh stamping
# step. Record the actual pinned source fingerprint using PB8's native helper.
RUN cd /opt/passivbot && PYTHONPATH=/opt/passivbot/src python -c 'import passivbot_rust; from rust_utils import source_fingerprint, stamp_compiled_extensions, verify_loaded_runtime_extension; stamp_compiled_extensions(source_fingerprint()); state=verify_loaded_runtime_extension(); assert state["runtime_compiled_source_stamp"] == state["expected_source_fingerprint"] and state["runtime_compiled_source_stamp"]'
COPY run.py /opt/benchmark/run.py
# Keep the installed package bytes/versions unchanged, but distribute large GPU
# libraries across independent runtime layers. The first Vast pull repeatedly
# retried the original 3.43 GB pip-install layer before any container code ran.
RUN python -c 'import pathlib,shutil; source=pathlib.Path("/usr/local/lib/python3.12/site-packages"); target=pathlib.Path("/opt/runtime-site"); target.mkdir(); [(shutil.copytree(p,target/p.name,symlinks=True) if p.is_dir() else shutil.copy2(p,target/p.name)) for p in source.iterdir() if p.name not in ("nvidia","torch","triton")]; shutil.copytree(source/"nvidia/cu13", "/opt/runtime-cu13", symlinks=True, ignore=shutil.ignore_patterns("libcublasLt.so.13"))'

FROM python:3.12-bookworm AS runtime
COPY --from=build /opt/runtime-site/ /usr/local/lib/python3.12/site-packages/
COPY --from=build /usr/local/lib/python3.12/site-packages/torch/ /usr/local/lib/python3.12/site-packages/torch/
COPY --from=build /usr/local/lib/python3.12/site-packages/triton/ /usr/local/lib/python3.12/site-packages/triton/
RUN apt-get update && apt-get install -y --no-install-recommends rsync \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/lib/python3.12/site-packages/nvidia/__init__.py /usr/local/lib/python3.12/site-packages/nvidia/__init__.py
COPY --from=build /opt/runtime-cu13/ /usr/local/lib/python3.12/site-packages/nvidia/cu13/
COPY --from=build /usr/local/lib/python3.12/site-packages/nvidia/cu13/lib/libcublasLt.so.13 /usr/local/lib/python3.12/site-packages/nvidia/cu13/lib/libcublasLt.so.13
COPY --from=build /usr/local/lib/python3.12/site-packages/nvidia/cudnn/ /usr/local/lib/python3.12/site-packages/nvidia/cudnn/
COPY --from=build /usr/local/lib/python3.12/site-packages/nvidia/nccl/ /usr/local/lib/python3.12/site-packages/nvidia/nccl/
COPY --from=build /usr/local/lib/python3.12/site-packages/nvidia/nvshmem/ /usr/local/lib/python3.12/site-packages/nvidia/nvshmem/
COPY --from=build /usr/local/lib/python3.12/site-packages/nvidia/cusparselt/ /usr/local/lib/python3.12/site-packages/nvidia/cusparselt/
COPY --from=build /usr/local/lib/python3.12/site-packages/nvidia/cuda_nvrtc/ /usr/local/lib/python3.12/site-packages/nvidia/cuda_nvrtc/
COPY --from=build /usr/local/lib/python3.12/site-packages/nvidia/cuda_runtime/ /usr/local/lib/python3.12/site-packages/nvidia/cuda_runtime/
COPY --from=build /usr/local/bin/ /usr/local/bin/
COPY --from=build /opt/passivbot/ /opt/passivbot/
COPY --from=build /opt/pb8-revision /opt/pb8-pip-freeze.txt /opt/
COPY --from=build /opt/benchmark/ /opt/benchmark/
WORKDIR /work
CMD ["bash"]
