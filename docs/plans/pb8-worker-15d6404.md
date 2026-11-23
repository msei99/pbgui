# Published PB8 worker update: 15d6404f

## Scope and activation

Published tag: `ghcr.io/msei99/pbgui-pb8-worker:15d6404-current-v1`.
Immutable digest:
`sha256:f5bc5f31c80b0d3e923aba2ca02cacc9d4980e20b63245c9ff24e999d7fb1d4a`.
Official source: `enarjord/passivbot`, commit
`15d6404f33bd5623d46a46e04ea694188ea7acea`, schema `v8.6.0`.
Publication and the PBGui pin change were explicitly authorized after the local
build passed. Anonymous pull was verified, and the registry config digest matches
the tested local image. Job, config-validation and calibration pins now select
this revision for new rentals. Existing rentals, installed PB8, PBRun and hosts
are unchanged. No paid GPU test or API restart was performed.

## Retained behavior

The runtime inherits the published PB8 image by immutable digest, including its
Python/CUDA dependencies and unchanged worker wrapper. The upstream dependency
declaration is unchanged. The candidate rebuilds the native Rust extension with
the retained Cargo lock and Rust 1.90. It retains the rolling CPU HSL history,
simulator prefix reuse, GPU HSL history cache and adaptive CUDA memory changes.
The simulator prefix optimization is combined with the official minute-prefix
reuse path; the reference path still uses full normalization.

The EMA span-alias and fresh-population fixes are already present in the official
source, so their old overlays are not reapplied. Source hashes, overlay checksums,
wrapper hash and the loaded native fingerprint are checked during the build.

## Reproduce

Prepare an allowlisted context with the existing Python builder:

```sh
python setup/vast_gpu_benchmark/build_worker_context.py --runtime pb8 \
  --revision 15d6404f33bd5623d46a46e04ea694188ea7acea \
  --reuse-runtime-base --output /tmp/pb8-15d6404-context
docker build -t pbgui-pb8-worker:15d6404-current-candidate /tmp/pb8-15d6404-context
```

The output directory must not exist. BuildKit is not required. Only public,
explicitly allowlisted build inputs are copied; no PBGui runtime directory,
configuration or credential store is a build context.

## Publication and preserved contracts

The publication receipt records the registry manifest digest, anonymous pull and
compressed layer sizes. Historical images retain their original source revisions
and cleanup ownership. The new revision's HSL scaling capability is explicit.
Calibration evidence stays tied to its measured revision and is not relabelled as
evidence for this revision. API serial 3846 makes the activation requirement
visible; the running API was not restarted.

The completed offline build/test evidence is recorded in
`setup/vast_gpu_benchmark/worker_build_pb8_current.json`; publication evidence is
in `setup/vast_gpu_benchmark/worker_release_15d6404.json`. Physical GPU acceptance
and performance measurements remain unverified until explicitly authorized.
