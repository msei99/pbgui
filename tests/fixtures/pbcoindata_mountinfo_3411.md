# Native PBCoinData mountinfo regression (#438)

`pbcoindata_mountinfo_3411.txt` contains all 55 records supplied by the authorized
isolated non-root Ubuntu/Linux 7.0.0-34-generic acceptance run on Serial 3411.
The external evidence ID is `42bc50fd60554fb0bed38e8224c44728` (FAILED).
The supplied file was `/tmp/acceptance-3411-sanitized-mountinfo.txt`.

The external investigator replaced source fields with `synthetic-source` and
arbitrary superblock option payloads with `rw`/`ro`. Mount IDs, parents, devices,
root fields, mountpoints, mount options and propagation tags remain as supplied.
No additional simplification is applied here. This is the full sanitized native
shape, not a fabricated minimal host or evidence of successful native acceptance.

Supplied sanitized bytes and this versioned copy have SHA-256:
`44838a252e318301794d172eb5c6067e6ef884d0f2f3791582ffd61a6f496da3`.
The unsanitized original is not included; no unsanitized-byte hash is asserted.

The four nsfs mount roots are kept verbatim. Tests must retain every record and
run the real parser, topology and mount selection, descriptor validation, pinned
self/PID-1 comparisons, fresh reopens and request-local authorization reuse.
Filesystem and namespace observations are controlled at the syscall boundary;
no mount, permission or production process changes are required.
