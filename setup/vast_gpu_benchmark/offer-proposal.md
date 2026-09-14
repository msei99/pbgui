# First Vast GPU rental proposal

Snapshots: 2026-09-13 08:31 UTC (broad search) and 08:39 UTC (RTX 3090),
authenticated Vast `/api/v0/bundles` searches.
Offers are not reserved; check availability, resources and prices again just
before creating an instance. The proposal below is historical. The approved
first rental subsequently used offer 44042303 in Hong Kong (RTX 3090,
0.1555556 USD/hour including 40 GB storage) after earlier offers disappeared.
Instance 50866485 has since been destroyed and its absence confirmed; see the
README's first-rental diagnosis. No optimizer workload started because a large
image-layer download repeatedly failed on that host. The corrected image was
subsequently published with explicit approval. A second RTX 3090 instance,
50869726 in Taiwan, successfully started at 0.1711111 USD/hour including
40 GB storage. Its benchmark and extended guard state are described in the
README; the original proposal and prerequisites below are historical.

The query requested one GPU, on-demand pricing, a verified rentable host,
CUDA >= 13.0, amd64 CPU, at least 24,000 MB reported VRAM, 32,000 MB host RAM,
four effective vCPUs, a direct port and 40 GB allocated storage. The filtered
response is saved locally in `.local-work/vast-gpu-test/offers.json`.

Following the user's preference for a cheaper first GPU test, a targeted 3090
query retained those requirements except host RAM, which was relaxed to 16,000
MB. The earlier 32 GB target was provisional, not a measured requirement.
The new recommendation reports about 28 GB host RAM; preparation/peak memory
must be checked in the smoke test. Its snapshot is saved locally in
`.local-work/vast-gpu-test/offers-3090.json`.

| Offer | Hardware | Location | Compute USD/h | 40 GB disk USD/h | Total USD/h |
| --- | --- | --- | ---: | ---: | ---: |
| **36021662** | **RTX 3090, 24 GB; Threadripper 3960X, 12 effective vCPUs, ~28 GB RAM** | **France** | **0.1467** | **0.0111** | **0.1578** |
| 36038183 | RTX 3090 Ti, 24 GB; Xeon E5-2660 v3, 20 effective vCPUs, ~64 GB RAM | Quebec | 0.2000 | 0.0111 | 0.2111 |
| 50759780 | RTX 4090, 24 GB; Threadripper PRO 5975WX, 8 effective vCPUs, ~32 GB RAM | Norway | 0.8000 | 0.0148 | 0.8148 |
| 34221914 | RTX 5090, 32 GB; EPYC 9654, 48 effective vCPUs, ~96 GB RAM | Germany | 0.9187 | 0.0111 | 0.9298 |

Recommendation: the **RTX 3090 in France**, offer 36021662, for the first
functional/worker-scaling test. A 4090 is not required to establish CUDA support
or measure GPU/exact-CPU interaction. The Threadripper 3960X also avoids the
oldest Xeon options. Reported reliability is 0.9851; this is a provider metric,
not a guarantee. Keep early results and use on-demand rather than interruptible
pricing. More expensive hardware can be compared later if measurements justify
it. These are workload judgments, not measured performance claims.
Provider-reported effective vCPUs are not dedicated physical cores.

For the recommended offer, transfer into the host costs 0.0026042 USD/GB and
outgoing traffic costs 0.0039063 USD/GB. At 90 minutes and an allowance of 10 GB
incoming plus 1 GB outgoing traffic, the estimate is:

`1.5 × 0.1577778 + 10 × 0.0026042 + 1 × 0.0039063 = 0.2666 USD`.

The transfer allowance includes image download and the 223 MB data bundle;
actual compressed layer sizes, host cache hits and output size may differ.
Propose a **1 USD experiment budget** and a **90-minute rental deadline**,
including image download, startup, preparation, test and result retrieval.
This is a proposed operating limit, not a provider-enforced spending cap or
user approval. Do not infer authorization from the account credit balance.

## Cleanup requirement before rental

The benchmark's process timeout does not stop rental billing. The intended
cleanup protocol must be implemented and checked before creating the instance:

1. Use only ephemeral instance storage; create no persistent volumes.
2. Record the exact instance ID and a unique benchmark label immediately, with
   an absolute deadline measured from rental creation rather than test start.
3. Run a separate persistent cleanup controller that survives PBGui/API
   restarts and the end of this conversation. `cleanup.py` implements this
   controller and its deadline/error paths have offline coverage. Configure
   the actual rental job and verify the service before running the workload;
   no rental guard is armed yet.
   Vast also documents self-destruction using its per-instance credential;
   validate this as a second guard on the worker, without transferring the
   account-wide API key. It complements the local guard, which must also cover
   failures before the container starts.
4. Retrieve partial results before the deadline, then call instance **Destroy**
   on success, error, or deadline. Cleanup must still happen if the optimizer
   fails; the hard deadline takes precedence over waiting indefinitely for a
   result download.
5. Confirm deletion through the Vast API, checking that the exact instance is
   gone and no test-created persistent storage remains. Retry transient API
   failures and explicitly report an unconfirmed deletion.

Stopping, process termination, rental expiry, or an empty account balance do
not establish that storage billing has ended. Vast documents continued storage
charges for stopped instances. A provider/network outage can prevent deletion;
an unverified API request must never be reported as a guaranteed cost stop.

Sources: [Vast offer fields](https://docs.vast.ai/api-reference/search/search-offers),
[Vast pricing](https://docs.vast.ai/guides/instances/pricing),
[instance management](https://docs.vast.ai/guides/instances/manage-instances),
[per-instance API access](https://docs.vast.ai/guides/instances/docker-environment),
[storage lifecycle](https://docs.vast.ai/guides/instances/storage/types).

Private GHCR publication and visibility verification are complete. The user has
approved the RTX 3090 experiment within **1 USD / 90 minutes**, including result
retrieval and destruction. Do not ask for that same authorization again.
Those initial prerequisites have since been completed. The user subsequently
authorized up to twelve hours if needed for a useful result; the current
experiment uses a twelve-hour deadline with both guards verified. Historical
90-minute limits below or in earlier setup notes do not supersede that later
authorization.
