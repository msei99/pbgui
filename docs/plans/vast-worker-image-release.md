# Public PB8 GPU worker image

## Published official PB8 calibration worker (2026-09-25)

- Upstream source: official `enarjord/passivbot` at `903ed11153ce82d1b6760604eaa3a553309a752a`.
- Public tag: `ghcr.io/msei99/pbgui-pb8-worker:903ed11-upstream-calibration-v1`.
- Immutable manifest: `sha256:09cb0f9ba004db44f3ca02a7b3b03ea3211fd9e6c3c9ea33794cd4c6d24dee30`.
- Image config: `sha256:e83fd9968e52dd98a16181c95319aa9369f676468a704abbd801dc065268c1e9`.
- PBGui worker source SHA256: `707eaa4e12dd37e5f53339a29f674409166e0aab8c4b6692ee407b96f0529b8e`.
- The published manifest has 26 compressed layers; their sizes are recorded in `vast_image_layers.py`.

The base and calibration Dockerfiles build from that official commit and check
the stamped revision. The one worker image was built and
verified offline: worker import, rsync, PB8 optimizer/GPU backend imports,
Rust source stamp, and exported metric contract. The contract matched the
checked-in PBGui contract exactly. An anonymous manifest read and immutable
digest pull confirmed public access. PBGui pins new rentals and both
calibration protocols to this digest; existing rentals retain their recorded
image/revision pair. No paid rental or remote host deployment was performed.

Build order for a repeatable local rebuild:

```sh
docker build -f setup/vast_gpu_benchmark/Dockerfile -t pbgui-pb8-worker:upstream-903ed11-base setup/vast_gpu_benchmark
docker build -f setup/vast_gpu_benchmark/Dockerfile.calibration -t pbgui-pb8-worker:upstream-903ed11-calibration setup/vast_gpu_benchmark
```

## Published offline-cache calibration worker (2026-09-23)

- Public tag: `ghcr.io/msei99/pbgui-pb8-worker:69227b7-calibration-v5`.
- Manifest: `sha256:8ad62f43decae47fe670f3ac15ba7e4d7c648f0bb030fa511cd4771321ff4327`.
- Config: `sha256:30c620d3d1e515d18efc575243c38a6c3949c354fd47391571552bb368b30ca7`.
- Worker SHA256: `7c842321cede4cfcc86497faa23a307bfbf2338636f759d48466db7c5afc00ae`.
- Added compressed layer: `sha256:b7578f7cce419be7d2031ad0ef38c9807c17868b0da92b5524140c93218a3135` (11,143 bytes); 39 layers total.

Built from only `Dockerfile.calibration` and `cloud_worker.py` on the unchanged immutable heartbeat base and PB8 revision. Each isolated calibration case now receives the staged market and first-candle caches, so a complete offline input does not fall back to the exchange. A network-disabled container verified the worker hash, import, calibration entry points, rsync and PB8 revision. An anonymous manifest read and immutable-digest pull verified public access. The previous image digests remain allowlisted for existing rentals and profile evidence. No paid rental or remote host deployment was performed.

## Published full-dispatch calibration worker (2026-09-23)

- Public tag: `ghcr.io/msei99/pbgui-pb8-worker:69227b7-calibration-v4`.
- Manifest: `sha256:70366b9989a12528245d4c263e0e3dc4350271427afd76f9568dcf29cf33878f`.
- Config: `sha256:de4498560e690a44cbc7274db7447b4c86155ff57335c7a94a4eb52195f96209`.
- Worker SHA256: `e41a310167a30b5a9d8af21a95a9c73d69bf5fb29ec007e4815e0113db906dc8`.
- Added compressed layer: `sha256:26842a45b418c14437a401923fef86a9c77638ce9b854a5a232b969f9e4da7f5` (10,694 bytes); 39 layers total.

Built from only `Dockerfile.calibration` and `cloud_worker.py` on the same immutable heartbeat base and PB8 revision. A network-disabled container verified the worker hash, import, calibration entry points, rsync and PB8 revision. An anonymous manifest read and immutable-digest pull verified public access. The wrapper raises the per-case test work envelope, requires an unsplit PB8 dispatch for valid protocol-3 evidence and records measured candidate-bars for the fixed production profile limit. Earlier image digests remain allowlisted for existing rentals and profile evidence. No paid rental or remote host deployment was performed.

## Published calibration protocol v3 (2026-09-22)

- Public tag: `ghcr.io/msei99/pbgui-pb8-worker:69227b7-calibration-v3`.
- Manifest: `sha256:bee2e513d77e2c22f5b0671392063c51bb04bd547c7334e614fe918ada2d49e2`.
- Config: `sha256:60b25187d3ed32dae3fd177cc64542b62eb9f108899b25f6926c02ed43a4d1eb`.
- Worker SHA256: `32a3dcf0f0eb1dfa2ad369fa6b60a156401d00b827555a2e4a576677bf5006e1`.
- Added compressed worker layer: `sha256:b0211849434322384c507f83328187b1130b97b8e9ce137ee7ef5645d92bc2c1` (9,380 bytes).

Built from a temporary context containing only `Dockerfile.calibration` and
`cloud_worker.py`, on the unchanged published heartbeat base. A network-disabled
container verified syntax, calibration entry points, the exact PB8 revision and
the worker hash. Anonymous manifest inspection and pull by immutable digest
verified the public artifact. Existing calibration v1/v2 evidence and all prior
rental image digests remain supported. No paid rental or remote deployment was
performed.

## Published calibration protocol v2 (2026-09-22)

- Tag: `ghcr.io/msei99/pbgui-pb8-worker:69227b7-calibration-v2`.
- Manifest: `sha256:8a85444f341a447564fc23e955b34f8a68b1970afc54026334aefa204c6cbc56`.
- Config: `sha256:a9a671f7877671651d7767b4c20047e2b222671cd8a38a561608ec97f938d742`.
- Worker SHA256: `aad8faa9c2ab99823ec447152dd0b24652ee87bc47501c4036363e8974a67505`.

Built and published with user approval using a streamed context containing only
Dockerfile.calibration and cloud_worker.py. The immutable heartbeat base and PB8
revision remain unchanged. A network-disabled container verified worker import,
source hash, linear growth and positive-plateau retention. Anonymous manifest
inspection with an empty Docker configuration verified the config digest and
39 layers. The new wrapper layer is 9,202 compressed bytes. Both calibration
wrapper digests are explicitly compatible for existing profile reuse; previous
rental image pins remain recoverable. No paid rental or remote deployment was
performed.

Released 2026-09-13 with user authorization.

- Package: `ghcr.io/msei99/pbgui-pb8-worker`
- Tag: `ee2b7d4-queue-v1`
- Manifest: `sha256:0d827eb097a9d26c9088421097b4a9f0eacf660f08613871c48946ddf71a0a88`
- PB8 revision: `ee2b7d49fd53ef790a66a28e2c85f2a6c8faebe8`
- Runtime base: the benchmarked/stamped image at `sha256:4c280d1e027e037c7a17952b45c8c5f4af821ff3737bc3d469c34c35c3952a9d`.

The new package was uploaded separately from the historical private benchmark
package. Its owner changed visibility to public. The package API reports `public`,
and the pinned manifest was downloaded anonymously and checked against its SHA256.
No registry account key is sent to Vast. Future rentals verify public access
before requesting any paid instance.

## Content review

The final OCI export contained 25 filesystem layers and 112,187 entries. The
review enumerated every layer, including small metadata/runtime layers. It
inspected filenames and scanned regular files up to 2 MB for common GitHub/AWS
credential patterns and private-key headers. No user credential-store paths,
PBGui configuration files, personal home directories or uploaded market-data
paths were found. This bounded pattern scan is not a proof that arbitrary data
can never contain a secret; the narrow build context and recipe were also reviewed.

Reported matches were standard SSH/crypto marker strings, a coincidental font
payload match, and CCXT cryptography code/test fixtures. The marked Pillow,
CCXT and cryptography source files matched the same-version public PyPI wheels
byte-for-byte; downloaded wheels were checked against PyPI SHA256 metadata.
Compiled companion files belong to those same installed packages.

The public worker layer adds only `cloud_worker.py`. The build context contained
that file and the Dockerfile; no account configuration, registry login, SSH key,
optimizer config or user market data was part of it. Registry login for publishing
used an ephemeral Docker configuration and password stdin, outside the build context.

## Licenses and validation

The PB8 Unlicense and installed package license directories remain in the image,
including the NVIDIA CUDA/cuDNN/NCCL package license notices. The image does not
relabel third-party components under a single PBGui license.

A local, network-disabled test of the published image installed two independent
job archives. The second archive omitted the market-data file and successfully
reconstructed its input from the first job's verified shared cache. No optimizer
or GPU rental was started by this check.

The shared-queue integration has offline lifecycle and browser coverage. A paid
end-to-end test of automatic rental, SSH bootstrap, two optimizer jobs, import
and final deletion remains outstanding.

## GPU metric contract (2026-09-14)

`setup/vast_gpu_benchmark/gpu_metric_contract.json` is generated by
`export_metric_contract.py` inside the published worker with networking disabled.
It records `SUPPORTED_METRICS`, names accepted by `validate_gpu_metric_names`,
explicit exact-only exclusions, and SHA256 hashes of `config.metrics`,
`optimization.gpu.metrics` and `optimization.gpu.metric_registry`.
The hashes were checked against local PB8 revision
`1f8f52b69facb1df9cd61f3e4454a86ce136e942` and all matched.
This expands scoring and limits without rebuilding or changing running workers.

When upgrading PB8 or the worker, run this exporter in both target environments
and compare the contracts. Regenerate the checked-in contract from the worker
matching the desired PB8 metric support; run the cloud validator tests. Do not
restore a manually selected subset or assume local CUDA availability determines
remote metric support.

## Published deadline wrapper update (2026-09-15)

- Public tag: `ghcr.io/msei99/pbgui-pb8-worker:ee2b7d4-queue-v2`.
- Manifest: `sha256:ea52a9ea51f1945b5c3c5133246fd125ee7793faa288754b21e095b95e138743`.
- Local image ID: `sha256:688b43b7b625e274efaa9bd8d3826d4f74aad9e49b249c1fc63cb09e8a5668d0`.
- Worker source SHA256: `53e06990e15785f246c93a3826160cc0b9614381554d207ba949859bc1bdd07a`.

Published with explicit user authorization. Anonymous manifest access and its
SHA256 were verified after push. Publication used password stdin and a temporary
private Docker configuration that was removed afterwards.

The build uses queue-v1 as its immutable foundation and a fresh temporary
context containing only `Dockerfile.worker` and `cloud_worker.py`. Only the PBGui
wrapper is replaced; PB8 and dependencies remain unchanged. No runtime data or
credentials were included in the build context.

A network-disabled container passed deadline adjustment and retry idempotence
checks. Its exported GPU metric contract exactly matches the checked-in contract.
The focused deadline, worker and upload suite passed all 50 tests.

PBGui v2.04.6 pins queue-v2 for new rentals. The previous exact digest remains
allowlisted for validating and managing existing immutable rental intents;
creation payloads retain the image authorized in their intent. Existing rented
containers are not upgraded by changing the local pin. No paid GPU performance
test or deployment to an existing rental was performed for this wrapper update.

## Published rsync transport image (2026-09-15)

- Local review image: `pbgui-pb8-worker:rsync-review`
- Image ID: `sha256:5934d691673e92721e01853c7743a12160f10700068fb1314fa06ec721059b6a`
- Approved public tag: `ee2b7d4-queue-v3`.
- Manifest: `sha256:b6f61c54b546640f5f00e386c10a27380e0ed8715788bcc8c4b597eedff58dbc`.
- Debian rsync package: `3.2.7-1+deb12u6`; protocol 32.
- PB8 remains `ee2b7d49fd53ef790a66a28e2c85f2a6c8faebe8`.
- Worker script SHA256 remains `53e06990e15785f246c93a3826160cc0b9614381554d207ba949859bc1bdd07a`.

Built from a temporary context containing only Dockerfile.worker and cloud_worker.py.
The additional layer installs Debian's rsync and libpopt0 packages, retains their
license metadata and removes apt lists. No credentials, jobs, caches or user
configuration are copied into the image. A network-disabled container check
verified rsync, Python compilation and the unchanged PB8 revision. Publication
was explicitly approved by the user. Anonymous manifest access and its SHA256
were verified after publication; the manifest config digest matches the reviewed
local image ID. PBGui now pins this digest for new rentals and retains queue-v1
and queue-v2 digests for recovery and cleanup of existing rentals.

## Published HSL and optimizer-heartbeat overlay (2026-09-22)

- Public tag: `ghcr.io/msei99/pbgui-pb8-worker:69227b7-hsl-heartbeat-v1`.
- Manifest: `sha256:bc330893bef1864065dc21835faac725e551383c3455ad4c5908a6713417268e`.
- Local image ID: `sha256:0e3ed6c61e90a35f16a162ec87fe20f2b5634511f0e0bccb292310b21d115b6e`.
- Compatibility revision: `69227b75e808f8ce1f4b8949350b3311916bd377`.
- Heartbeat source commit: `9c160f05913283fa3cd4e9198d83f0ce26ec3640` (Passivbot PR #1801).

The image derives from the published disabled-HSL specialization at
`sha256:b67111ebcc0d0c55c57c0b27ad8d2017c8577061a47a4b7909936bd4215c0fc4`.
It overlays only the shared asynchronous optimizer helper, PyMOO and DEAP callers, and optimizer
entrypoint. SHA256 checks confirmed that the GPU backend and MPS kernel files remain byte-identical
to the HSL image. A network-disabled container check verified the compatibility marker, Python
syntax and five-minute heartbeat contract. Anonymous manifest access was verified after publishing.

PBGui pins the new digest only for new rentals and keeps every prior digest allowlisted for cleanup
and recovery. Existing containers are not modified or restarted. No paid Vast instance was rented
for this publication.

## Published GPU calibration runner (2026-09-22)

- Public tag: `ghcr.io/msei99/pbgui-pb8-worker:69227b7-calibration-v1`.
- Manifest: `sha256:f078b47466f53e3039b13e41905531d9504ca6caca7b57469499fae20b77ec0e`.
- Local image ID/config digest: `sha256:6fc2fa6e28583701bcba0d27b93498f78c963daf749d2b26eb70bdbcae50a12a`.
- Worker source SHA256: `12d3d1c19ad58c16f8cea41668b296a7d8eda480fb969e8cd74e5555661df518`.
- Compatibility revision: `69227b75e808f8ce1f4b8949350b3311916bd377`.

The image derives by immutable digest from the HSL/optimizer-heartbeat image and
replaces only `/opt/pbgui/worker.py`. Its runner recognizes the authenticated
calibration plan, executes bounded fresh PB8 processes for adaptive population
cases, publishes live case status and returns separately validated calibration
evidence. The build context was the repository's dedicated Vast benchmark setup;
the Dockerfile copies only `cloud_worker.py`. No credentials, configs, market
data or runtime state are copied into the image.

A network-disabled container check verified Python syntax, rsync, the exact
worker hash, calibration entry points and the PB8 compatibility marker. The
published manifest was pulled anonymously using an empty Docker configuration
and rechecked by exact digest without network access. PBGui pins this manifest
only for new rentals and retains the heartbeat image for recovery and cleanup of
existing immutable rental intents. No paid Vast instance was started as part of
the image publication itself.
