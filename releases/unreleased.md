# Unreleased

- Released the PB8 CUDA worker on official revision `00ce7d0`, including the HSL terminal-balance fix and the unchanged PR #1871 candidate-metric protection. Published `00ce7d0-pr1871-v1`, verified anonymous manifest access and image identity, and updated job, validation and calibration pins plus compressed layer metadata together. Historical image/revision pairs remain supported. Eight PB8 regressions and 1,299 focused PBGui tests pass; native source verification and metric-contract compatibility pass. The final image was not subjected to a second GPU rental at the user's request.

- Added an isolated RTX 3060 CUDA reproduction and historical targeted upstream patch for false held-position valuation errors after HSL closing losses exhaust cash. The failing optimizer configuration completes with the fix; twelve failing replay cases, six unchanged controls, and 48 boundary probes are verified. The released worker includes that fix directly from upstream.
