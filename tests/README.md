# PBGui Tests

PBGui separates deterministic default tests from tests that intentionally use
current exchange data, a separate PB7 installation, or local PBGui runtime
data.

## Setup

Install the application and test dependencies in a Python 3.12 environment:

```bash
python -m pip install -r requirements-test.txt
```

UI and Vanilla JS regression checks use a locally installed native Chromium
browser, controlled directly by Python over the DevTools protocol. Set
`PBGUI_TEST_BROWSER=/absolute/path/to/browser` when it is not discoverable as
Brave, Chrome or Chromium. No browser or driver is downloaded during a test.
Profiles, storage and process ownership are isolated from the user's browser.
All unhandled requests are blocked; fixtures explicitly fulfil synthetic API
responses. JavaScript assertions execute in the browser, never in a JavaScript
server, package-manager runtime or automation driver.

Functional checks default to software rendering. The
real-time Coin Data benchmark explicitly uses normal browser rendering and the
real clock; its existing latency budget remains unchanged.

## Default Tests

```bash
python -m pytest tests/
```

The default suite blocks external network connections and skips every test
marked `live_exchange`, `external_pb7`, or `local_runtime`. Loopback and Unix
socket connections remain available for isolated local test servers.

Default tests must not read or modify `pbgui.ini`, `data/`, a sibling PB7
installation, remote hosts, or external APIs. Writable artifacts belong under
Pytest temporary directories.

## Live Exchange Tests

```bash
python -m pytest -m live_exchange --run-live -v -s
```

These tests intentionally query current public exchange or market-data APIs.
They may copy one active CoinMarketCap credential from the local owner-only
vault into an isolated temporary store, but they keep generated mappings,
caches, and reports inside temporary directories. They do not use private
exchange credentials or read a CMC key from `pbgui.ini`.

## External PB7 Tests

```bash
PB7_ROOT=/path/to/pb7 python -m pytest -m external_pb7 --run-external-pb7 -v -s
```

`PB7_ROOT` defaults to a sibling `pb7` directory next to PBGui. These tests may
refresh PB7's public market caches because that is their explicit purpose.
The Strategy Explorer parity test accepts an existing result directory through
`PB7_PARITY_BACKTEST_DIR`.

## Local Runtime Tests

```bash
python -m pytest -m local_runtime --run-local-runtime -v
```

These tests may read existing PBGui runtime configuration, such as Run V7
instance configs. Source data is read-only; all roundtrip output is written to
temporary files.

## Combined Explicit Run

```bash
python -m pytest tests/ --run-live --run-external-pb7 --run-local-runtime -v
```

Supplying an opt-in does not make unmarked tests network-enabled. Only tests
carrying the matching marker receive the corresponding permission.

## Conventions

- Test files are named `test_*.py` and include module docstrings.
- Test classes use `Test*`; test functions use `test_*`.
- Multiple equivalent cases use `@pytest.mark.parametrize`.
- External-data tests assert stable schema and invariants rather than exact
  market counts or complete symbol lists.
- New live tests must carry the narrowest matching marker.
- Generated reports, caches, bytecode, and local runtime fixtures are not
  versioned.
