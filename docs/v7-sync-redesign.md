# V7 Sync/Activate Redesign

## Grundprinzip

- Cluster Sync `desired_state.json` ist die Quelle fuer V7-Zielzustand, Host-Zuordnung, Version, Manifest-Hash und Tombstones.
- `data/run_v7/<name>/` enthaelt lokal materialisierte Konfigurationen.
- PBCluster repliziert Operationen/Blobs und materialisiert Dateien lokal oder remote.
- PBRun bleibt der Runtime-Gatekeeper und startet/stoppt nur lokale Bots anhand lokal materialisierter Cluster-Zustaende.
- Legacy `data/cmd/status_v7.json`, `activate_*.cmd` und direkte V7-SSH-Sync-Pfade sind aus dem V7-Cluster-Mode-Runtime-Pfad entfernt. PBGui erstellt, liest oder beachtet `status_v7.json` nicht mehr.

## Materialisierte V7-Dateien

- Synchronisiert werden `config.json` und Coin-Override-Dateien wie `BTC.json`.
- Nicht synchronisiert werden Runtime-Dateien wie `config_run.json`, `running_version.txt`, `ignored_coins.json`, `approved_coins.json` und Logs.
- `pbgui.version` bleibt die lokale Config-Version.
- Der Cluster-Manifest-Hash deckt alle syncbaren JSON-Dateien ab.

## Running-State

- Ist-Zustand kommt aus echten Prozessen, PB7-Logs und `running_version.txt`.
- Soll-/Blocked-Zustand kommt aus `data/cluster/desired_state.json`.
- `running_version.txt` bleibt als schnelles Monitoring-Feedback erhalten, ist aber keine Quelle fuer gewuenschte Starts/Stops.

## Flows

### Save/Create

1. Master schreibt lokal `data/run_v7/<name>/config.json` und Overrides.
2. Master schreibt eine Cluster-Operation mit Version, Assignment, Desired State und Manifest-Hash.
3. PBCluster repliziert Operationen/Blobs an konfigurierte Peers.
4. PBCluster materialisiert zugewiesene Dateien auf dem Zielknoten.
5. PBRun erkennt die lokale Aenderung und prueft Cluster Gate vor Start/Weiterlauf.

### Delete

1. Master sichert und entfernt die lokale Instanz.
2. Master schreibt eine Tombstone/Delete-Operation.
3. PBCluster repliziert die Operation.
4. PBRun stoppt lokale Bots, sobald der lokale Desired State Tombstone/Stop signalisiert.
5. Fehlende Dateien oder fehlende Remote-Eintraege erzeugen niemals implizite Deletes.

### Move/Enable/Disable

1. Master schreibt eine explizite Cluster-Operation fuer neues Assignment oder `desired_state`.
2. PBCluster materialisiert nur auf dem zugewiesenen Node.
3. PBRun auf dem alten Node stoppt durch `wrong_host`/`desired_stopped` Gate.
4. PBRun auf dem neuen Node startet nur bei passendem Manifest, passender Version und `desired_state=running`.

## PBRun Gate

PBRun startet oder laesst einen V7-Bot nur weiterlaufen, wenn alle Checks passen:

- Cluster ist nicht initialisiert, oder `desired_state.json` ist lesbar und gehoert zum lokalen Cluster.
- Instanz existiert im Desired State.
- Instanz ist nicht tombstoned oder conflicted.
- `desired_state == "running"`.
- `assigned_host == local node_id`.
- Lokaler Manifest-Hash entspricht `config_manifest_hash`.
- Lokale Config-Version entspricht `version`.

Fehlschlaege werden als Blocked State in PBRun/Monitoring sichtbar gemacht.

## Legacy V7 SSH Sync

- `master/v7_config_sync.py` ist entfernt.
- V7-Saves, Restores, Deletes und Forced-Mode-Aenderungen schreiben Cluster-Operationen statt Remote-SFTP-Pushes.
- Remote `run_v7`-Dateien werden nur noch durch explizite Cluster Sync Materialisierung geschrieben.
- PBCluster startet oder stoppt keine Bots direkt.

## Aenderungen pro Datei

| Datei | Rolle |
|-------|-------|
| `PBRun.py` | Pollt Cluster Desired State und `data/run_v7`, fuehrt lokale Start/Stop-Entscheidungen aus |
| `api/v7_instances.py` | Schreibt lokale Configs und Cluster-Operationen, keine direkten Remote-V7-Pushes |
| `cluster_sync_command.py` | Materialisiert V7/API-Key Payloads ueber den eingeschraenkten SSH Wrapper |
| `master/async_monitor.py` | Meldet Ist-Zustand aus Prozessen/Logs und Soll-/Blocked-Zustand aus Cluster Desired State |
| `PBCluster.py` | Repliziert Operationen/Blobs und materialisiert lokale Dateien |

## Wichtige Regeln

1. Keine Delete-Inferenz aus fehlenden Dateien, fehlenden VPS-Eintraegen oder fehlendem Remote-Status.
2. Tombstones werden nur durch explizites Restore/Recreate entfernt.
3. PBCluster schreibt Dateien, startet/stoppt aber keine Bots.
4. PBRun kontaktiert keine Peers und schreibt keine Remote-Dateien.
5. Runner/VPS benoetigen PBCluster und PBRun, aber keinen laufenden PBApiServer.
