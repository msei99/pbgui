# PB8 GPU benchmark on Vast.ai

The original benchmark helpers are historical. The benchmark rental was deleted
after result collection; the measurements are recorded in the
[benchmark report](../../docs/plans/vast-ai-gpu-benchmark-results.md).

The current integration is in **PB8 Optimize**, with Cloud setup in Queue and GPU
selection in the editor. Multiple jobs share one rental and cached market data.
See the [user guide](../../docs/help/48_vast_gpu.md).

Public image: `ghcr.io/msei99/pbgui-pb8-worker:ee2b7d4-queue-v3` (includes rsync).
PBGui pins its manifest digest; no registry token is needed to download it.
The [image release review](../../docs/plans/vast-worker-image-release.md) records
content/license checks and anonymous access validation. `Dockerfile.worker`
provides a public runtime foundation for later wrapper updates; normal users do
not build or publish images. Publishing a new version is a maintainer release
operation, not part of creating a queue job.

The older `Dockerfile` and rental/template helpers below describe the original
benchmark setup. They are not the live queue's lifecycle controller.

## Current workload

The user-created `gpu_test` selects ETH, Binance/Bybit, and five 400-day training
scenarios from February 2020 to August 2025. Its original backend is `pymoo`,
budget 200,000, and seed unset. The exporter preserves its scenarios, search
bounds, constraints, and GPU validation settings. The approved GPU comparison
uses `--gpu-adg-objective` to replace `gain_strategy_eq` scoring with
`adg_strategy_eq` equally in CPU and GPU copies. Without this explicit option,
the exporter preserves scoring. Only copies change:

| Case | Backend | Exact CPU workers | Budget | Seed |
| --- | --- | ---: | ---: | ---: |
| cpu4 | pymoo | 4 | 512 | 7 |
| gpu1 | gpu | 1 | 512 | 7 |
| gpu2 | gpu | 2 | 512 | 7 |
| gpu4 | gpu | 4 | 512 | 7 |

The user subsequently requested testing greater CPU parallelism. Additional
`gpu8` and `cpu8` experiment copies use eight workers with the same budget,
seed and ADG objective. The second host exposes 64 logical CPUs, but its
container `cpu.max` is `877714 100000`: only 8.77714 CPU cores of aggregate
execution time. Offer-reported allocation was 9.142857 effective vCPUs.
Do not size workers from `os.cpu_count()` alone. CPU quota/throttling and GPU
utilization are recorded during the extended comparison.
The prepared CPU-only cases were not executed. The actual short GPU comparison
was stopped at equal 25-generation windows for a quick conclusion, rather than
the originally configured 512 exact-iteration budget. Native exit code 130
records those intentional checkpoint interruptions.

GPU auto-lean parallelism is disabled so the requested worker counts are used.
Other GPU options remain native, including automatic population/batch sizing,
bootstrap, eight validations per generation and four drift probes. Native
startup logs are authoritative for effective settings. Exact evaluation budgets
do not equal proxy screening counts. Different backends and asynchronous worker
counts need not visit identical candidates despite an identical seed.

## Prepare locally

From the PBGui root, with its Python environment:

```sh
python setup/vast_gpu_benchmark/prepare.py --config gpu_test --output .local-work/vast-gpu-test/input
```

For this explicitly approved ADG comparison, add `--gpu-adg-objective` and
choose a new output directory. The manifest records the objective adjustment.

The destination must not exist. Configs use PBGui's native PB8 configuration
loader/writer. No account credential files, live instances, or PBGui settings
are exported. Strategy configs are private files; do not publish the input
bundle with the image. The exporter supports explicit approved-coin lists,
Binance/Bybit, and date-only scenarios; source/coin overrides are rejected.

The export copies daily history for the selected symbols plus BTC reference
data only within the combined base/scenario date envelope, extended backward
by PB8's native optimizer warmup and rounded outward to complete days. Native
optimization bounds and scenario overrides contribute to this calculation.
`max_warmup_minutes=0` disables the cap, not the date filter. Failure to compute
warmup stops export. Requeue builds a fresh package; existing frozen packages
are never rewritten.

File presence and hashes do not prove candle continuity or sufficient history.
The manifest therefore marks `coverage_validated=false`. PB8 preparation must
still succeed and its selected exchange, actual tradable periods, exclusions,
and warmup must be reviewed in the worker logs before comparing results.
Bybit's available ETH history starts later than the first scenario. The first
prototype uses `ohlcv_source_dir`; this does **not** promise fully offline
execution. Native preparation may access public market metadata or other
public resources. A subsequent production export should use prepared arrays
and frozen metadata with an enforced offline validation step.

## Image and worker

### Account and template handoff

The account is not connected merely because the user is signed into Vast in
their browser. Programmatic access requires a Vast API key. The current setup
uses the private local file below and direct authenticated REST calls; no Vast
CLI installation is required. Authentication with the user-supplied key has
now been verified.

Create a dedicated key in the Vast console with the permissions needed for
template management and the subsequently agreed instance operations. Store it
locally, never in chat or as a command-line argument. From the PBGui root, the
following interactive command uses hidden input and the project's private-file
helpers (it does not contact Vast):

In the console's Standard permission view, enable User Read/Write, Instances
Read/Write, and Miscellaneous; disable both Billing/Earning permissions. User
Write covers templates and account SSH keys; Miscellaneous covers offer search.
These standard categories grant more operations than the benchmark uses. Leave
per-key/per-category 2FA requirements off for this automation key; this setting
does not disable account-login 2FA. Instance Write still permits paid rentals
even with Billing/Earning disabled; rental authorization and the agreed budget
remain separate. Category mapping:
[Vast permission reference](https://docs.vast.ai/api-reference/permissions).

```sh
python3 - <<'PY'
from getpass import getpass
from pathlib import Path
from secure_files import atomic_write_private_text, ensure_private_directory
directory = ensure_private_directory(Path('.local-work/vast-gpu-test/credentials'))
destination = directory / 'vast_api_key'
if destination.exists():
    raise SystemExit('A key file already exists; keep the existing credential.')
key = getpass('Vast.ai API key (hidden): ').strip()
if not key or any(char.isspace() for char in key):
    raise SystemExit('No valid key entered; nothing saved.')
atomic_write_private_text(destination, key + '\n')
print('Key saved locally with owner-only permissions.')
PY
```

This custom path is for subsequent server-side API use; it does not configure
the Vast CLI automatically. See [Vast API authentication](https://docs.vast.ai/guides/reference/api-keys).

For the user handoff, first save that API key locally. For later instance access,
add this machine's public SSH key to the Vast account's SSH Keys section; an
existing `~/.ssh/id_ed25519.pub` was found by an existence-only check. Display
only that `.pub` file to copy it into Vast, never the private key. There is no
instance SSH login to perform yet. Image publication and template creation come
before selecting and approving a paid rental; account setup alone starts no
benchmark.

The template can be reviewed before credentials are supplied:

| Setting | Proposed value |
| --- | --- |
| Name | `PBGui PB8 GPU Benchmark ee2b7d4` |
| Visibility | Private |
| Image | `ghcr.io/msei99/pbgui-pb8-gpu:ee2b7d4` (published privately; Vast pull access pending) |
| Launch mode | Interactive shell server, SSH |
| CPU architecture | x86_64, matching the local image |
| GPU | One NVIDIA GPU compatible with this PyTorch/CUDA build |
| CPU allocation | At least four available CPU cores |
| Initial sizing target | 24 GB VRAM, 32 GB RAM, 40 GB disk; provisional until smoke measurements |
| On-start benchmark | None; start explicitly after verifying transferred input |
| Application ports | None; use the SSH access supplied by Vast |
| Secrets/config/data in template | None |

The chosen registry is GHCR under the existing GitHub account. Publishing the
image and saving the private Vast template are separate actions from renting a
paid instance. The local image must be pushed to a registry accessible to Vast
before the template can launch successfully. A private image also needs
registry pull authentication on Vast.

Account provisioning has now authenticated successfully, registered and
verified the local public SSH key, and saved the private template defined in
[template.json](template.json). Provider IDs are stored locally in
`.local-work/vast-gpu-test/ssh-registration.json` and
`.local-work/vast-gpu-test/template-registration.json`. The SSH list endpoint
returned `public_key` as the field name, unlike the documentation's `key`.

The GitHub CLI account now has the verified `write:packages` scope after the
user completed browser authorization. The interactive command used was:

```sh
gh auth refresh --hostname github.com --scopes write:packages
```

Complete the GitHub browser authorization and then recheck registry access
before attempting publication. Do not print or pass tokens as command-line
arguments. See [GitHub CLI scope refresh](https://cli.github.com/manual/gh_auth_refresh).
The image is now uploaded and its private visibility has been verified through
the GitHub API. The registry digest is
`sha256:0fdd5fecba36b1f69e0f1cc963e5b4d37cd4319b554142391e5e55cb8940b5fe`.
Publication evidence is stored locally in
`.local-work/vast-gpu-test/image-publication.json`. The earlier template
registration record describes the state at template creation; it is not the
current image-publication status. No instance has been rented. The saved
template remains marked preparatory until private pull access and the rental
cleanup guards are ready.

### Image ownership and visibility

The intended image address is `ghcr.io/msei99/pbgui-pb8-gpu:ee2b7d4`, a
container package under the user's GitHub account. An image upload is separate
from a Git commit, release or repository push. GitHub makes a newly published
container package private by default. Keep it private for the initial test;
changing it to public requires the user's decision. A private package is
available only to authorized accounts/workflows and authenticated workers.
Vast needs suitable registry read access to pull it. Do not forward the broad
local GitHub CLI credential to Vast; arrange separate read-only pull access if
keeping the image private. A public image would be anonymously downloadable.

The image contains PB8 at the pinned revision, Python/Rust/CUDA dependencies
and the benchmark runner. The Docker build context excludes the user's
optimizer configs, OHLCV bundle and credential files. Those inputs remain
separate even if the image is made public later. The Vast template's visibility
and the GHCR package's visibility are independent settings. See
[GitHub Container registry documentation](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry).

For the private worker pull, create a separate GitHub **personal access token
(classic)** with only `read:packages` and a short expiration. The existing
local CLI token includes repository/workflow/package-write access and must not
be forwarded to Vast. Save the separate token interactively:

```sh
python3 setup/vast_gpu_benchmark/store_pull_key.py
```

The helper writes an owner-only credential below the ignored local work
directory, never to command arguments or logs. Validate its scopes and actual
pull access before passing it to Vast in the authenticated instance-creation
request body. Do not add it to the versioned template definition or image.

### Independent cleanup guard

`cleanup.py` now implements the local guard. It accepts an immutable job file
below `.local-work/vast-gpu-test` with `instance_id`, unique
`pbgui-gpu-test-<32 hex digits>` label, `created_at`, `deadline`, and
`destroy_authorized: true`. The deadline must be within 90 minutes of creation.
It locks the job, waits independently of PBGui, checks ID and label, requests
Destroy, and requires two consecutive successful listings with the instance
absent before recording `deletion_verified`. Network/429/5xx failures retry;
permission, ownership and schema failures are recorded for intervention.

Launch the guard as a user systemd service with restart-on-failure and
`RestartPreventExitStatus=2` (permanent refusal), using the PBGui Python and
absolute script/job paths. A live user systemd manager with lingering enabled
was confirmed, and a harmless transient service start/exit was verified. Arm
the actual job when its rental ID is known, and check its active state before
starting the workload. For early cleanup, stop the waiting guard and relaunch
it with `--now`; a concurrent second guard is rejected by the job lock.

The guard does not yet have a real rental job and no live deletion was tested.
Before renting, also complete the creation/ID-persistence handoff and prepare a
second worker-side deadline using Vast's per-instance credential. No account
API key belongs on the worker. A provider outage can still prevent deletion;
never report costs stopped until deletion is verified. Tests:

```sh
python -m pytest -q tests/test_vast_gpu_benchmark.py tests/test_vast_gpu_cleanup.py
```

Current result: **35 offline tests passed**, including complete cleanup-loop
tests with temporary files/mocked API calls. The client's read-only instance
listing was also verified against Vast; the account had zero instances.

PB8 is pinned to `ee2b7d49fd53ef790a66a28e2c85f2a6c8faebe8`, including the newer
CUDA CPU-wait and HSL/rounding fixes. The image builds the matching native Rust
extension and uses upstream `[full,gpu-cuda]` dependencies. The image build
context excludes everything except the Dockerfile and worker script. The base
image tags and Python transitive dependencies are not fully locked; record the
resulting image digest and `/opt/pb8-pip-freeze.txt` for each experiment.

```sh
docker build -t pbgui-pb8-gpu-benchmark:ee2b7d4 setup/vast_gpu_benchmark
```

No image is automatically pushed, no Vast instance is rented, and no data is
automatically uploaded. Before a cloud run, agree on the concrete instance,
hourly/storage/transfer charges and total budget, then approve that deployment.
The first worker needs a supported NVIDIA GPU/driver and at least four available
CPU cores. RAM/VRAM requirements must be checked in the smoke test.

The current dependency resolution selects PyTorch 2.13.0 with CUDA 13.0 as well
as upstream's CuPy/CUDA-12 packages. Select a CUDA-13-capable driver (R580 or
newer is the family-level minimum), then verify the actual kernel compilation
on that GPU. PTX/toolkit requirements can be stricter than minor compatibility;
see [NVIDIA's compatibility table](https://docs.nvidia.com/deploy/cuda-compatibility/minor-version-compatibility.html).
Do not select an older CUDA-12-only host solely because the extra is named
`gpu-cuda` or CuPy is named `cupy-cuda12x`.

On a Linux GPU host with Docker and NVIDIA Container Toolkit, the execution
contract is:

```sh
docker run --rm --gpus all --shm-size=2g \
  --mount type=bind,src=/absolute/private/input,dst=/work/input,readonly \
  --mount type=bind,src=/absolute/private/results,dst=/work/results \
  pbgui-pb8-gpu-benchmark:ee2b7d4 \
  python /opt/benchmark/run.py --case gpu1 --output /work/results/smoke --timeout 120
```

The host input/results directories must exist. Vast usually runs the workload
inside its own container: configure the built image as a custom template,
upload the reviewed bundle to `/work/input`, and run the Python command inside
that container. Select **Interactive shell server, SSH** launch mode: Vast
installs SSH and replaces the image entrypoint with its setup script. Leave
automatic benchmark execution disabled until the input transfer is verified.
Nested Docker is not required. Registry image access and transfer paths remain
to be configured for the actual instance. See
[Vast template settings](https://docs.vast.ai/guides/templates/template-settings).

After reviewing the smoke log, use `--case all --timeout 600` and a fresh output
directory. The default matrix permits up to 40 minutes of computation, plus
termination grace and setup. A smoke timeout is recorded as incomplete, not a
successful benchmark. Increase the budget only after measuring the smoke.

**The timeout stops benchmark processes, not Vast billing.** Retrieve results
and explicitly destroy the instance after the approved experiment; stopping a
container or using Docker `--rm` does not destroy a Vast rental. See the
[Vast getting-started documentation](https://docs.vast.ai/guides/get-started).

## Evidence and interpretation

- Each case has a fresh working directory and its own preparation/cache cost.
  Raw logs include `PASSIVBOT_GPU_PROFILE` and `PASSIVBOT_OPTIMIZE_PROFILE`.
- `report.json` captures wall time, exit/timeout state, package versions, CUDA
  hardware, and native structured GPU events. Keep the input manifest, full
  native logs, results, image digest and pip freeze alongside the report.
- Check for native drift halts, startup failures, excluded coins/scenarios and
  insufficient completed validations. A successful process exit alone does
  not establish an accurate or useful proxy.
- Compare GPU 1/2/4-worker throughput at equal completed exact evaluations.
  Native `exact_work` and `exact_queue_wait` accumulate across evaluations;
  neither divided by wall time is a CPU bottleneck percentage. CPU and GPU work
  overlap. Kernel times and aggregate worker times must not simply be added.
- CPU-only versus GPU optimization is an end-to-end experiment, not a claim
  that the two algorithms performed identical work or found equally good
  solutions. Keep proxy/exact counts separate; compare exact result quality.
- Repeat with reversed case order and further seeds before choosing hardware.
  Shared-host load, cold compilation and automatic dispatch sizing affect a
  single run. This kit does not yet collect continuous GPU/CPU utilization.

This first matrix identifies CPU scaling and proxy drift behavior. A controlled
candidate-by-candidate parity study and a local-CPU/remote-GPU latency benchmark
are separate next steps. The latter requires an explicit PB8 service boundary;
installing a Vast template cannot make a remote GPU appear as local CUDA.

## Offline tests

```sh
python -m pytest -q tests/test_vast_gpu_benchmark.py
```

These tests use temporary fixtures and owned child processes, not production
market data, exchange access or GPU hardware. They do not validate CUDA kernels,
the Docker build, or a Vast deployment.

## Recorded local validation (2026-09-13)

- 16 offline tests passed.
- Image build succeeded: `pbgui-pb8-gpu-benchmark:ee2b7d4`, local image ID
  `sha256:731ab21dbdade5ae17fbcc1438e8bde6fa2662219a1bff8f0b4e121e051f60e2`.
  Docker reports 8,324,222,464 bytes; this is not the compressed upload size.
- A disposable container with `--network none` passed `pip check`, imported
  PyTorch/CuPy/native Rust, and ran the optimizer CLI help successfully.
- That container loaded the exported GPU config using the pinned PB8 loader,
  computed 36,288 minutes of optimizer warmup, and verified all 9,419 OHLCV
  hashes plus four config hashes from a read-only input mount.
- The current prepared bundle is
  `.local-work/vast-gpu-test/input-v2`; the earlier `input` directory is an
  intermediate export without the final config checksum fields. Use `input-v2`.
- The original optimizer config still matches its recorded SHA-256.
- CUDA availability was false on this CPU-only host, as expected. No optimizer
  performance run, GPU parity check, Vast rental or worker deployment has been
  performed. Subsequent account/template provisioning and the verified private
  image upload are recorded in the account handoff section above.
# Experiment supervision

The approved first rental uses `rental.py` under the persistent user service
`pbgui-vast-benchmark-3090.service`. It records an immutable intent and a durable
create-attempt marker before the only creation request. Restarts recover the
instance by its unique label and never repeat the creation request. The
supervisor starts before billing and polls through two successful absent
observations after destruction. Runtime files are private and local under
`.local-work/vast-gpu-test/rental-3090*`.

To end this experiment early after retrieving results, create the empty file
`.local-work/vast-gpu-test/rental-3090.finish`. The supervisor then destroys only
the matching rental; inspect its `.status.json` for `deletion_verified` and
verify the account's instance list. Do not stop the supervisor before cleanup.
The absolute 90-minute deadline also covers image loading. The worker startup
installs a second deadline guard using `CONTAINER_API_KEY`; the account-wide
Vast credential is never sent to the worker. Verify `worker-guard-ready.json`
on the worker before starting optimizer work.

Private GHCR access has been verified using a separate `read:packages` token.
Registry login is supplied only in the authenticated Vast creation request
body, using the official CLI's `image_login` format. It is not written to a
versioned template or passed through a local process command line.

The 1 USD amount is an authorized operating budget, not a provider-enforced
spending cap. Price checks, the deadline, bounded tests and result transfers
control expected spending. Destruction still depends on provider availability;
only verified deletion establishes that the rental has ended.

## First rental diagnosis and cleanup (2026-09-13)

The first rental, instance **50866485**, used an RTX 3090 in Hong Kong at
0.1555556 USD/hour including 40 GB ephemeral storage. It remained in `loading`:
the host repeatedly retried layer `1424af6c1404` (3,429,166,935 compressed bytes).
Other layers downloaded successfully. No container-side benchmark, CUDA test,
worker-guard readiness check or user data transfer took place.

An independent authenticated download from GHCR retrieved the entire affected
layer in 137.08 seconds and matched its SHA-256. This rules out a corrupt or
missing registry blob for that test. The available Vast logs expose retries,
but not the underlying host/network error; the exact transport cause remains
unknown. Smaller layers are a mitigation to test, not a proven root-cause fix.

The supervisor received an early-finish request and verified deletion twice.
A separate account API check confirmed zero instances; systemd reported a
successful exit with no restarts. The rental lasted approximately 20.3 minutes
through the guard's final observation: estimated compute/storage charges are
0.0527 USD, excluding network transfer and billing adjustments. No persistent
volumes were created. Local evidence is under `.local-work/vast-gpu-test/` in
`startup-diagnosis.json`, `layer-download-validation.json`, and
`rental-3090-final-verification.json`.

The corrected Dockerfile keeps the existing built PB8/GPU packages and copies
large libraries into separate runtime layers. Rust build tools remain in the
build stage. Its explicit CUDA directory layout matches the validated
PyTorch 2.13.0/CUDA 13.0 dependencies; review it when upgrading those packages.
Local tag: `pbgui-pb8-gpu-benchmark:ee2b7d4-layered`; image ID:
`sha256:fb96a989de374a8f29492cd17ce905168343958dd327b6e51fd88815a1986c19`.
Docker reports 7,524,044,793 uncompressed bytes. `pip check`, Torch/CuPy/native
Rust imports, optimizer CLI startup and all 9,419 shard/four config checksums
pass in an isolated container without network access. The second RTX 3090 host
successfully pulled this image and passed Torch/CuPy CUDA calculation checks.

Verified publication target:
`ghcr.io/msei99/pbgui-pb8-gpu:ee2b7d4-layered`, in the existing private package
owned by `msei99`. Following explicit user authorization, the upload completed
and the registry digest was verified as
`sha256:31ac85b55b08be8fbcdaf9df2411b8ae42df55462671c72562fc2e843f5e1227`.
The package remains private and the separate credential was rechecked for
exactly `read:packages` access. The image has 24 layers totaling 3,774,778,881
compressed bytes; the largest is 820,822,247 bytes, compared with the original
3,429,166,935-byte layer. Evidence is in `image-publication-layered.json`.
The repeat test initially had a 60-minute deadline. The user subsequently
authorized up to twelve hours if needed for a meaningful result. Both guards
were replaced with verified four-hour guards, expiring at 2026-09-13 13:38 UTC.
The active local intent/status/service names use `rental-3090-layered-extended`
/ `pbgui-vast-benchmark-3090-extended`. Its existing create-attempt marker was
preserved, so resuming supervision cannot rent another instance. Finish early
using this active intent's `.finish` marker; never reuse an older completed job.

Native GPU preflight rejects the original `gain_strategy_eq` objective as
exact-only. With explicit user approval, benchmark copies replace only that
scoring metric with `adg_strategy_eq`; the original `gpu_test` remains unchanged.
These objectives are different, so resulting speed measurements must be labeled
as the adjusted ADG workload. The source-directory loader also expects standard
exchange directory names: export Binance shards under `ohlcv/binance`, although
PBGui stores their originals under `ohlcv/binanceusdm`. Bybit stays `bybit`.
Native preparation still fetches public market/first-candle metadata; exporting
OHLCV shards alone does not make this prototype completely offline.

The initial layered image passed imports but failed native optimizer evaluation
identity: installing through pip/maturin does not execute the source-stamp step
from PB8's `rustbuild.sh`. The Dockerfile now calls the same native
`source_fingerprint` / `stamp_compiled_extensions` helpers immediately after
building the pinned source, and verifies the loaded extension. On the test
worker, the existing binary and source fingerprints were first compared with
the locally built image before applying this missing packaging step. Neither
Rust strategy code nor Python optimizer code was changed.

Vast's default automatic tmux attachment can break noninteractive SSH commands.
The rental startup now creates `/root/.no_auto_tmux`. Strict SSH host-key checks
remain enabled. A dedicated test public key was attached to the instance after
the globally registered key was rejected by its SSH server.
