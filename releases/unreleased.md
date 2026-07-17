# Unreleased

## Fixed

- Exchange replica-relevant config, secret, and sealed blobs even when PBCluster operation state vectors are already converged, including recovering a locally missing master blob from another master before redistribution.
