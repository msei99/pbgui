# Unreleased

## Fixed

- Repair replica-relevant config, secret, and sealed blobs even when PBCluster operation state vectors are already converged, allowing remote retention projections to recover from incomplete blob stores.
