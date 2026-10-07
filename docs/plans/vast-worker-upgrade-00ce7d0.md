# PB8 worker upgrade candidate — 2026-10-06

## Publication and activation — completed

The user subsequently authorized publication, pin integration and a commit,
and explicitly declined another GPU reproduction run. The worker was published
as `ghcr.io/msei99/pbgui-pb8-worker:00ce7d0-pr1871-v1` with immutable manifest
`sha256:47032b1e42076555a29909a9b888cd24efb69dec6ccd80d06ac26b0bc653ac99`.
Anonymous tag and digest retrieval succeeded. The registry configuration digest
matches the tested local image ID, and every uncompressed layer identity matches.
The publication receipt and all 28 compressed layer sizes are versioned in
`setup/vast_gpu_benchmark/worker_release.json`.

Job, validation and calibration pins now select that digest and the new revision.
The metric contract source hashes and pull-progress layer sizes were updated.
Historical image/revision pairs and calibration evidence remain supported.
The API serial was incremented. No existing rental or remote bot host was changed.
After pin integration, 1,299 focused PBGui tests passed across worker image,
configuration validation, calibration, job ownership, provisioning, GPU tuning
and throughput parsing. No full offline suite was run.
The final image did not receive another CUDA execution test; the earlier GPU
evidence below covers the targeted upstream HSL fix on the older worker.

The following sections retain the build and pre-publication review history.

The worker candidate was built locally from official PB8 master revision
`00ce7d0ddf123071ae67db149753d6b3619f52ac`. This revision includes the HSL
terminal-balance fix `1d142ec9720f62cdce5c7b1c911efd7e45178501`.

PR #1871 was still open and unmerged when checked. Its existing runtime patch
applies without adaptation; its changed lines match PR head
`83d18f0afbd0c927367aed522067558184a34933`. No other historical PB8 overlay
is applied. The candidate-specific `MetricAggregationError` catch covers the
final scorer, uses the existing invalid-candidate helper with penalty `1e18`,
and propagates other exceptions.

## Build and provenance

- Recipes: `setup/vast_gpu_benchmark/Dockerfile` and `Dockerfile.calibration`.
- Metadata: `setup/vast_gpu_benchmark/worker_candidate.json`, copied into the image.
- Patch SHA-256: `2d86e672d3163d76e1e68f165d647582e2b643e8ad6dc84b4efef83f3db5d6d8`.
- Local final tag: `pbgui-pb8-worker:upstream-00ce7d0-pr1871-candidate`.
- Local image ID: `sha256:1afa2af31baedb4baa229958f443dcb173b9bc680cf0f15821dd130d04cab5f7`.
- Uncompressed image size: 7,538,718,410 bytes; 28 filesystem layers.
- Loaded native module SHA-256: `55e40545844f94755b1e85d7e05938cbdb909c00e8f3bede5cb9622159dc3315`.
- Compiled and expected source stamp: `dc656b28436f2eb2b3c77f70a3be7164bee80cd8b59116f9d21b0ff5b8d3afca`.

The local image ID is not a published registry manifest digest. Build dependencies
are resolved during the build; use the eventual immutable published digest for
activation rather than assuming that a rebuild produces the same image.

## Completed checks

- Both Docker stages built successfully, including the upstream Rust module.
- In the final image, with network disabled and a read-only root filesystem:
  all five original PR #1871 regressions and all three CPU valuation-error
  regressions passed (eight tests).
- Native runtime source verification passed; the final worker module imports.
- The reproduction config loads as `v8.6.0` and passes the real GPU preparation
  scope validation with device availability mocked. This is a configuration
  compatibility check, not a CUDA execution test.
- The exported supported, allowed and exact-only metric sets match the active
  PBGui metric contract exactly. Source hashes were captured separately.
- PBGui worker image and throughput/parser tests: 62 passed.

Local logs, original PR tests, exported contract, native verification and image
configuration are retained under `.local-work/pb8-worker-upgrade-00ce7d0/`.
The earlier RTX 3060 reproduction and targeted-fix comparison are documented in
`docs/plans/vast-hsl-terminal-balance-reproduction.md`. That GPU test used the
older worker plus the isolated upstream fix, not this newly built image.
