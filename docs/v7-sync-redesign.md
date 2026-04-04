# V7 Sync/Activate Redesign

## Grundprinzip

`status_v7.json` ist die **Single Source of Truth** für den Instanz-Katalog.
VPS-Verzeichnisse (`data/run_v7/*`) spiegeln diesen Katalog wider.
`delete_*.cmd` und `activate_*.cmd` (für v7) entfallen komplett.
Der separate "Activate"-Button im GUI für v7 wird entfernt — Save schreibt und synct alles.

---

## `status_v7.json` — Zentrales Manifest

- Enthält die Liste aller v7 Instanzen mit **per-Instance `activate_ts`**
- **Nur von Mastern geschrieben**
- PBRun auf VPS **liest nur** → startet/stoppt Bots entsprechend
- Andere Master überwachen sie auf VPS per inotify → **Merge-Logik: höchster `activate_ts` pro Instanz gewinnt**

## Per-Instance Config-Dateien

- Weiterhin pro Instanz unter `data/run_v7/{name}/`
- Eine Instanz kann aus **mehreren .json Dateien** bestehen:
  - `config.json` — Haupt-Config (immer vorhanden)
  - `{SYMBOL}.json` — Per-Coin Configs (z.B. `ALGOUSDT.json`, `DOGEUSDT.json`, `XRP.json`)
- **Nicht gesynct** werden runtime/lokale Dateien:
  - `ignored_coins.json`, `approved_coins.json`, `config_run.json`, `monitor.json`
- Nur von Mastern geschrieben
- VPS bekommt sie per SSH (sofort) oder PBRemote/rclone (verzögert)
- `pbgui.version` für Config-Versionierung (wie bisher)

## Running-State

- Wie bisher über `alive_*.cmd.gz`
- Kein separates Runtime-File nötig
- `running_version.txt` pro Instanz bleibt für inotify-Feedback

---

## Flows

### Save/Create

```
Master 1:
  1. config.json + Coin-Configs schreiben (data/run_v7/{name}/)
  2. status_v7.json updaten (activate_ts für diese Instanz setzen)
  3. config.json + Coin-Configs + status_v7.json per SSH auf alle VPS pushen

VPS:
  PBRun erkennt neue status_v7 (Polling alle 5s)
  → startet/stoppt Bots entsprechend

Master 2/3:
  inotify auf VPS status_v7.json
  → Merge: per-Instance activate_ts vergleichen
  → neue/geänderte Instanzen: config.json + Coin-Configs von VPS pullen
  → gelöschte Instanzen: Backup + lokal löschen
```

### Delete

```
Master 1:
  1. rm -rf lokal (data/run_v7/{name})
  2. rm -rf auf allen VPS per SSH
  3. status_v7.json updaten (Instanz entfernt, activate_ts bumpen)
  4. status_v7.json per SSH auf alle VPS pushen

VPS:
  PBRun erkennt neue status_v7 → stoppt Bot (falls laufend)

Master 2/3:
  inotify → neue status_v7 → Instanz fehlt → Backup + lokal löschen
```

### Enable/Disable (enabled_on ändern)

```
Master 1:
  1. config.json: enabled_on ändern
  2. status_v7.json: activate_ts für Instanz bumpen
  3. config.json + Coin-Configs + status_v7.json per SSH auf alle VPS

VPS:
  PBRun → startet oder stoppt Bot

Master 2/3:
  inotify → config.json + Coin-Configs pullen + status_v7 mergen
```

### Startup (Master war offline)

```
Master:
  1. status_v7.json von VPS lesen (alle verbundenen)
  2. Merge: per-Instance activate_ts vergleichen
  3. Neuere Configs von VPS pullen (config.json + Coin-Configs)
  4. Lokal vorhandene Instanzen die in keiner VPS-status_v7 mehr stehen → Backup + löschen
```

---

## Merge-Logik

- **Per-Instance `activate_ts`** (nicht per-File)
- Wenn Master 1 Instanz A ändert (ts=100) und Master 2 Instanz B ändert (ts=101):
  → Merge ergibt: Instanz A mit ts=100 + Instanz B mit ts=101
- Datei-Level-Timestamp entscheidet NICHT — nur pro Instanz
- Instanz fehlt komplett in einer status_v7 → wurde gelöscht (nur wenn activate_ts der
  Remote-Version neuer als die lokale)

---

## PBRun (VPS)

- Überwacht **nur** `status_v7.json` per Polling (alle 5s mtime check)
- Startet/stoppt Bots entsprechend bei neuer Version
- Für v7: **kein `activate_*.cmd` mehr** — nur status_v7 gilt
- v6 Multi/Single System: **komplett unverändert** (activate_*.cmd bleibt für v6)

## PBRemote

- **Master**: synct config.json + Coin-Configs pro Instanz + status_v7 + alive
- **Slave**: synct nur alive; bezieht configs + status_v7 **read-only**
- v6 Multi/Single: **komplett unverändert**

## V7ConfigSyncWorker (Master)

- Überwacht `status_v7.json` auf allen VPS per inotify — **einziger Trigger für Config-Sync**
- Überwacht `running_version.txt` pro Instanz (geschrieben von PBRun auf VPS — Running-Feedback im UI)
- `config.json` Watch: **entfällt** — jede Config-Änderung bumpt `activate_ts` in status_v7, das reicht als Trigger
- `delete_*.cmd` Watch + Verarbeitung: **komplett entfernt**
- Startup: Reconciliation gegen VPS status_v7
- Watchdog (120s): periodischer Abgleich als Fallback

---

## Änderungen pro Datei

| Datei | Änderungen |
|-------|-----------|
| **`Status.py`** | Per-Instance `activate_ts` im Schema |
| **`PBRun.py`** | `watch_v7()` → nur status_v7 auswerten; `activate_*.cmd` für v7 entfernen (v6 bleibt!); Polling auf status_v7 mtime |
| **`api/v7_instances.py`** | Delete: `delete_*.cmd` raus; Save/Create: alle .json + status_v7 per SSH auf VPS; status_v7 bei Save/Delete updaten |
| **`master/v7_config_sync.py`** | `delete_*.cmd` Code raus; status_v7 als inotify Watch-Pfad; Reconciliation-Callback (Merge + Pull config.json + Coin-Configs / Delete); Startup-Abgleich |
| **`PBRemote.py`** | Master: config.json + Coin-Configs pro Instanz + status_v7 + alive syncen; Slave: nur alive + configs/status_v7 read-only |
| **`RunV7.py`** | `save()`: auch status_v7 updaten; `activate()` für v7: entfällt komplett |
| **`navi/v7_run.py`** | Activate-Button für v7: **entfernen** — Save erledigt alles |
| **`frontend/v7_run.html`** | Activate-Button entfernen; Save-Logik ggf. anpassen |

## Wichtige Regeln

1. **config.json + Coin-Configs** ({SYMBOL}.json) werden gesynct — NICHT: ignored_coins.json, approved_coins.json, config_run.json, monitor.json
2. **v6 Multi/Single bleibt komplett unverändert** — activate_*.cmd etc. bleibt für v6
3. **PBRun Polling** (nicht inotify) zur Vermeidung neuer Komplexität
4. **Kein Activate-Button** mehr für v7 im GUI — Save triggert alles
