# Rental protocol v3 candidate

This is a **locally built, offline-verified, unpublished candidate**, not an active
worker release. `worker_build_rental_protocol.json` records its local image ID,
source and build-input hashes, retained base layers and completed checks.
The Docker recipe derives from the current immutable image and replaces only
PBGui's worker protocol. PB8 source, Rust binaries and dependencies are retained.
It includes multi-day independent guard deadlines and the Pareto delta protocol
from PR #509. `worker_candidate_rental_protocol.json` binds the worker source hash.

## Retained PB8 fixes

The base is the published fresh-population image, not a clean upstream PB8 image.
Its inherited layers retain the rolling HSL history cache, CPU HSL-prefix reuse,
adaptive CUDA history-memory sizing and VRAM headroom, offline wrapper and exchange
listing timestamps, and fresh GPU populations without the implicit input bot.
Explicit starting configs, checkpoint restore and the compiled Rust extension
are retained. No PB8 sources, patches, Python packages or native binaries are
copied, rebuilt or changed by this Docker recipe.

The build checks the exact PB8 revision and re-runs the base image's existing
`verify_fresh_population.py` before accepting the candidate. That verifies the
patched GPU backend SHA-256, the Rust source fingerprint, the loaded extension's
compiled source stamp, random sampling, explicit seeds and checkpoint restore.
These checks passed during the local build and in a separate network-disabled
container run. GPU hardware validation and a real optimizer run on this candidate
remain pending; no paid rental has been started.

Build and verify on a machine with a working Docker daemon:

```sh
docker build --network=none -f setup/vast_gpu_benchmark/Dockerfile.rental-protocol -t pbgui-pb8-worker:b9d6692-rental-protocol-v3-candidate setup/vast_gpu_benchmark
docker run --rm --network none --entrypoint python pbgui-pb8-worker:b9d6692-rental-protocol-v3-candidate /opt/pbgui/verify_rental_protocol.py
```

The protocol verifier exercises the exact shipped worker's delta snapshot and a 120-hour
guard deadline using temporary files and fake credentials. It performs no GPU,
Vast API, provider deletion or paid-rental operation. The local source verifier
can also run without Docker; that does **not** constitute an image build receipt.

Publication needs separate approval. After publishing, retrieve and verify the
immutable registry digest and retain a build/publication receipt including the
source hash and verification output. Only then update `vast_jobs.IMAGE`, keep the
old image/revision mapping for recovery, and add the new digest's `multi_day` and
`pareto_delta` capabilities in `vast_worker_capabilities.py` (in both PRs if they
are still separate). Re-run the focused start, guard, deadline and collection tests.
No existing image receives these capabilities based merely on its tag or source.

Until activation, requests longer than 24 hours fail before any paid rental on
old images. Existing rentals retain their original deadlines and guards. Building
or publishing the image does not deploy to or restart an existing rental.
