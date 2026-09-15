# Vast upload performance investigation — 2026-09-15

Scope: read-only diagnosis and proposal. No upload implementation change, rental
restart, bulk speed test or alternate data upload was performed for this analysis.
Existing uncommitted cache-progress UI changes belong to the preceding task.

## Observations

Current SOL_DOGE_ADA_BNB upload: 464,530,361 compressed bytes; 21,855 manifest
entries. The rented offer reports Texas, US, download 864.8 Mbps, upload 662 Mbps.
The upload into the worker must be compared with its download rate.

- Persisted verified application throughput during sampling: approximately
  6.5 Mbps (811,667 bytes/s), first upload attempt, no reported error.
- Local physical interface transmitted 7.88 Mbps across a 10-second interval,
  with no additional local transmit drops. This includes other traffic and
  protocol overhead; it is not a measurement of ISP upload capacity.
- Two simultaneous direct SSH upload sockets had approximately 172/186 ms RTT,
  minimum RTT 156/163 ms. The sampled TCP records did not show retransmission
  counters. A short sample cannot exclude intermittent loss.
- Effective SSH configuration has ControlMaster disabled and no ControlPersist.
- Three empty commands through the same verified direct SSH path took 2.186,
  2.230 and 3.143 seconds. They write no worker data and do not alter the run.
- Code opens a fresh SSH/TCP connection for each 2 MiB chunk, with at most two
  concurrent chunks: approximately 222 connections for this archive. Cache
  checking uses 22 pages and two SSH commands per page, approximately 44 more
  connections before transfer. These commands are sequential across pages.
- Input is already gzip-compressed; enabling SSH compression is not the first
  optimization to pursue.

## Interpretation

Connection setup is a demonstrated avoidable cost in PBGui. Repeated TCP startup
also limits how long each connection can reach steady throughput on a high-RTT
route. The empty-command timing includes authentication, remote shell/process
startup and scheduling, not only a TCP handshake.

As an illustration, 222 commands at 2.2 seconds with two upload slots correspond
to roughly four minutes of command setup budget. This is not a measured additive
saving: setup, data transfer and concurrency overlap, and reusable channels still
have overhead. Similarly, 44 sequential cache calls can explain substantial
pre-transfer latency even for an empty cache. A controlled before/after benchmark
is required to quantify improvement.

At 6.5 Mbps, transferring 464.5 MB alone takes roughly 9.5 minutes. At the
advertised host rate, the arithmetic floor is 4.3 seconds, ignoring local uplink,
routing, protocol and disk overhead. That gap does not establish false host
advertising. The actual local uplink capacity remains unknown pending tariff or
an independent upload measurement from the PBGui machine.

## Recommended sequence

1. Keep direct SSH and reuse two owned connections for upload. Preserve the
   existing small verified chunks, retry/resume behavior, strict host keys and
   private credentials. Own and close connection masters/control sockets on
   completion/cancellation. Reuse a connection for cache queries as well.
   OpenSSH ControlMaster/ControlPersist can remove repeated authentication
   without requiring a new worker protocol; a persistent framed receiver could
   later remove per-command Python/shell startup too.
2. Measure before/after with identical synthetic data, the same host and a known
   local uplink, after the current upload completes. Compare sustained rate,
   connection setup time and retries. Test two versus four persistent connections
   only if there is remaining uplink capacity. Do not promise a speed multiplier.
3. Optimize cache checking: batch bounded responses over a persistent session;
   for a new cache, return the empty-cache case cheaply. Retain checksum checking
   for reused data and bounded responses. Avoid simply reverting the response
   size limit or uploading thousands of small files individually.
4. If needed, evaluate rsync over direct SSH as an alternative transport for the
   stable archive, keeping partial files and validating the final SHA256 before
   installation. It is still SSH, but can use a sustained transfer rather than
   establishing a connection for every block.
5. Consider private object storage/HTTPS download for frequently reused datasets
   across hosts. Upload shared data once, then let workers fetch missing objects.
   Initial local upload remains limited by the local connection; extra storage,
   transfer cost and credential ownership must be considered. Do not expose the
   home/PBGui server as a public download service by default.

## Other supported transports

Vast documents `vastai copy` (rsync), direct SCP/SFTP and Cloud Sync. Switching to
SCP alone is not evidence of a faster protocol. HTTPS/object storage provides a
different route and can help repeated runs, but does not bypass the original
local upload. A nearby host is another test variable, not a guaranteed fix.

Sources:
- https://docs.vast.ai/guides/instances/storage/data-movement
- https://docs.vast.ai/guides/instances/connect/ssh
- https://docs.vast.ai/guides/instances/storage/cloud-sync
- https://man.openbsd.org/ssh_config#ControlMaster
- https://download.samba.org/pub/rsync/rsync.1
