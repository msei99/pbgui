# PBGui Anpassungsplan für das PB8 Update

Stand: 10. Oktober 2026. Status: Lokale Umsetzung und Verifikation; Image-Veröffentlichung, Cloud-Aktivierung und bezahlte Neukalibrierung stehen aus.

Ziel ist die Kompatibilität von PBGui mit dem lokalen PB8-Update von `061e472e3` auf `b9d6692ea`. Dazu gehören die neuen HSL- und GPU-Einstellungen sowie ein Worker ohne den inzwischen upstream enthaltenen Backport. Der Plan betrifft PBGui und dessen Worker-Build; Änderungen am PB8-Repository sind nicht vorgesehen.

## Umsetzungsstand

- HSL-Steuerung, Modusvalidierung ohne Reset, kanonischer Erhalt fester HSL-Overrides, GPU-Auto/Null-Erhalt und gemeinsame Prepare-/Load-Bereinigung sind implementiert.
- Die später gemeldeten GPU-Fehler sind lokal behoben: Native Ausgaben werden innerhalb beider Helper-Protokollpfade nach stderr umgeleitet; Cloud-Exporte für ältere Worker entfernen ausschließlich das deaktivierte neue HSL-Feld aus der Kopie. Aktivierte/ungültige Werte werden abgelehnt. Ein alter eingefrorener Snapshot mit dem unbekannten Feld bleibt wartend und verlangt erneutes Vorbereiten, statt beim Dispatch verändert zu werden.
- Cloud-Vorbereitung und Dispatch prüfen die tatsächlichen Worker-Fähigkeiten. Bestehende Images und Rentals behalten ihren Pin; ein unpassender Job blockiert passende nachfolgende Jobs nicht.
- Der PR-1871-Backport und das temporäre Overlay-Dockerfile sind entfernt. Der offizielle Kandidat `b9d6692ea` wurde lokal ohne Overlay gebaut und offline geprüft. Nachweis: `setup/vast_gpu_benchmark/worker_candidate_validation.json`.
- 20 gezielte native GPU-/Checkpoint-/Migrationsregressionen, 60 native Allowance-/Session-Tests und 7 native Auto-/Richtungsprüfungen bestehen; separate Proben gegen die echte Bridge erhalten Auto-Werte und HSL-Werte beim Vorbereiten, Laden und Speichern. Die versionierte Bridge-Regression verwendet gemockte native Funktionen. Die isolierte Browser-/Guide-Prüfung ergab 59 bestandene Tests und umfasst Navigation, Guide und Sidebar-Geometrie; die neuen Editorfelder sind durch ausführbare JS-Tests geprüft, noch nicht durch einen vollständigen Editor-Rundlauf im Browser.
- Der vom Test-Gate verlangte vollständige Offline-Lauf ist registriert: **12.488 bestanden, 57 übersprungen, 2 fehlgeschlagen** (917,18 s). Beide Fehler kommen aus `tests/test_logging_policy_audit.py` und betreffen unveränderte lokale Research-Skripte unter `.local/sk_doge_research` (direkte Prints/Append-Zugriffe). Kein PB8-Anpassungstest ist fehlgeschlagen; keine Timeout-Fehler im vollständigen Lauf. Der Lauf wird gemäß Regel nicht wiederholt. Die lokalen Research-Dateien und die Audit-Regeln wurden nicht verändert.
- Alle 14 im Direction-Vertrag referenzierten nativen Symbole sowie `enforce_bounds` sind gegenüber dem aktiven Pin im AST unverändert. Der separate Kandidatenvertrag enthält neue Zeilenreferenzen und Quellen-/AST-Prüfsummen.
- Release v2.08.8: Pool-/Angebots-/Start-Kompatibilität ist implementiert; das neue Image ist anonym abrufbar veröffentlicht und Pins, Verträge sowie Layer-Größen sind gemeinsam umgestellt. Der Nutzer hat am 11.10.2026 ausdrücklich die Veröffentlichung mit neuem Worker und konservativen unkalibrierten Fallbacks freigegeben. GPU-Hardwareprüfung, echte Checkpoint-/Resume-Abnahmen und bezahlte Neukalibrierung werden nachgeholt. Alte Veröffentlichungsevidenz bleibt in `worker_release.json`, die neue steht in `worker_release_b9d6692.json`.
- Die PBGui-Resume-Regression prüft Argumente und Runner-Status einschließlich einer gemockten nativen Fehlermeldung. Sie prüft weder echte alte/neue Checkpoints noch deren Fehlerdarstellung über API und Editor. Für §6 ist bislang die Erkennung neuer Backtest-Ergebnisordner neu parametrisiert; relative Suite-Artefaktpfade, Resume im neuen Layout, Result-to-Run und Bounds-Erhalt über Laden/Strategiewechsel/Speichern bleiben offene PBGui-Integrationsabnahmen. Native Session-/Migrationsprüfungen und unveränderte Bounds-Symbole ersetzen diese Abnahmen nicht.
- Nach dem Implementierungsreview bestehen 27 fokussierte Tests aus `test_pb8_october_compatibility.py`, `test_pb8_optimize_runner.py` und `test_help_coverage.py`. Die neuen Fehlerpfadtests prüfen fehlende und beschädigte Snapshots auf aktuellem und älterem Rental: Der betroffene Job bleibt ohne Lease wartend, der nächste passende Job startet. Die vollständige Suite wurde dafür nicht erneut ausgeführt.
- Nach den GPU-Fehlerbehebungen bestehen 1.414 fokussierte Helper-/GPU-/Cloud-/Queue-/Image-Tests (2 übersprungen) und 3 Guide-Abdeckungsprüfungen. Native Proben gegen das lokale PB8 bestätigen das JSON-Protokoll von `main` und `serve`, GPU-/CPU-Preflight, Prepare/Load/Save und die originale HSL-Normalisierung der gepinnten alten Revision für beide Seiten. Nur die GPU-Verfügbarkeit wurde gemockt; kein Optimizerlauf oder Rental wurde gestartet. Quellenprüfsummen und Nachweise stehen in `worker_candidate_validation.json`. Der neue Test-Gate-Aufruf verlangt wegen des bereits registrierten vollständigen Laufs ausdrücklich keine Wiederholung der Gesamtsuite.

## 1 Ausgangslage und bestätigte Befunde

| Bereich | Befund | Konsequenz |
| --- | --- | --- |
| Lokales PB8 | Commit `b9d6692ea`, Softwareversion `8.2.0.dev0`, Config-Schema `v8.6.0` | Fähigkeiten an tatsächlicher Runtime und Revision prüfen; die Softwareversionsnummer allein genügt nicht. |
| Config-Bridge | Isolierte Operationen `status`, `default`, `optimize_metadata_static`, `coin_override_metadata` und `exchange_metadata` funktionieren. | Bestehende Isolation beibehalten. |
| Excess Allowance | `we_excess_allowance_mode: bounded` wird entfernt. `raw` wird mit Hinweis auf Prüfung und erneutes Backtesting abgelehnt. | Keine automatische Umdeutung von `raw` zu `bounded`. |
| HSL | PB8 liefert `bot.<side>.hsl.scale_budget_with_excess_allowance`, Standard `false`. | Im Optimizer fehlen dafür Formularfelder. Der Metadatenadapter erzeugt derzeit keine entsprechenden Runtime-Controls. |
| GPU Automatik | Automatische Exact-Worker-Skalierung gilt bei `gpu.exact_workers: null` und `tuning_mode != "off"`. `n_cpus` ist Fallback bzw. Startwert. PBGui ersetzt leere Exact-Worker-/Pending-Felder beim Einsammeln durch `0` und normalisiert `n_cpus` auf eine Ganzzahl. | `null`, `0` und feste Werte mit ihrer jeweiligen nativen Bedeutung erhalten. |
| Config-Normalisierung | `prepare_config()` erhält `n_cpus: null` für die Targets `canonical` und `optimize`. Erst `sanitize_prepared_config_for_dump()` ersetzt es über `clean_config()` / `_clean_with_template()` durch den Template-Wert `4`. | Gemeinsamer GPU-spezifischer Null-Erhalt für Vorbereiten und Laden; ein Frontend-Fix oder ein Fix nur in `_prepare()` reicht nicht. |
| Cloud-Worker | Aktueller Build pinnt `00ce7d0ddf123071ae67db149753d6b3619f52ac` und wendet PR #1871 als Patch an. | Worker auf eine geprüfte offizielle PB8-Revision umstellen und den Backport entfernen. |
| Cloud-HSL | Die gepinnte alte Runtime kennt den neuen HSL-Schalter nicht. Ein ansonsten gültiges Beispiel mit aktiviertem Schalter passiert die PBGui-Cloud-Validierung. | Fähigkeiten des gewählten Images prüfen; neue Einstellungen dürfen auf alten Images nicht stillschweigend akzeptiert werden. |
| Alte Rentals | Unterstützte ältere Images können weiterhin neue wartende Jobs übernehmen. Nur das aktuelle Image erhält zusätzlich `preflight_gpu_snapshots()`. | Fähigkeiten des tatsächlichen Rentals vor jeder Job-Zuweisung prüfen. |

Die bisherigen fokussierten Prüfungen ergaben 189 bestandene und 2 übersprungene Tests. Diese Zahl umfasst keine vollständige Suite: Der Backtest-WebSocket-Lauf und ein Optimizer-Autostart-Lauf wurden wegen Hängern abgebrochen; anschließend wurden begrenzte Testmengen erfolgreich ausgeführt. Die bestehenden Tests ersetzen keine Regressionen für die neuen Einstellungen.

Die anschließende Review-Prüfung bestätigte mit 17 weiteren bestandenen Tests die Weiterverwendung älterer Rentals und deren Übernahme wartender Jobs. Die Ursache des Null-Verlusts wurde zusätzlich mit isolierten nativen PB8-Proben reproduziert.

## 2 Den gemergten Worker Backport entfernen

Gemeint ist der Backport **GPU suite invalid metric candidates**, nicht die PBGui-Worker-Infrastruktur insgesamt. Das lokale PB8 enthält sowohl Fix-Commit `83d18f0af` als auch Merge-Commit `1f838bf07` für PR #1871; beide wurden als Vorfahren des aktuellen HEAD bestätigt.

Betroffene Quellen:

- `setup/vast_gpu_benchmark/Dockerfile`
- `setup/vast_gpu_benchmark/Dockerfile.calibration`
- `setup/vast_gpu_benchmark/Dockerfile.suite-metrics`
- `setup/vast_gpu_benchmark/patches/gpu-suite-invalid-metrics.patch`
- `setup/vast_gpu_benchmark/worker_candidate.json`
- `setup/vast_gpu_benchmark/.dockerignore` und `setup/vast_gpu_benchmark/README.md`
- `setup/vast_gpu_benchmark/worker_release.json` als zu bewahrender Nachweis des bisherigen veröffentlichten Images; den neuen Release-Nachweis separat ablegen
- `setup/vast_gpu_benchmark/gpu_metric_contract.json` und `setup/vast_gpu_benchmark/export_metric_contract.py`
- `vast_direction_validation.py` und `vast_image_layers.py`, einschließlich veralteter Pin-/Patch-Kommentare
- `tests/fixtures/vast_direction_contract.json`, einschließlich aller revisions- und zeilengebundenen Quellenverweise
- `tests/test_vast_worker_image_upgrade.py`
- `tests/ui/test_vast_queue_validation_race.py`
- `docs/help/48_vast_gpu.md` und `docs/help_de/48_vast_gpu.md`
- `docs/plans/vast-worker-image-release.md` und `docs/plans/vast-worker-upgrade-00ce7d0.md` als historische Referenzen

Vorgehen:

1. Als Ziel einen festen offiziellen PB8-Commit verwenden. Ausgangskandidat ist `b9d6692ea41266ecb4ab81d40bb5e5baef29b50a`; diesen vollständigen Hash verwenden und vor dem Build gegen die gewählte Quelle prüfen. Kein bewegliches `master` oder `latest` verwenden.
2. Im aktuellen Docker-Build Patch-COPY, Patch-Prüfsumme und beide `git apply`-Schritte für PR #1871 entfernen. Die offizielle Revision und die Rust-Fingerprint-Prüfung bleiben verbindlich.
3. Calibration-Basis, Build-Tags, Labels und Provenienz auf den ungepatchten Upstream-Build umstellen. Keine Overlay-Angabe für PR #1871 im neuen Image belassen.
4. Den temporären Suite-Metrics-Dockerfile und die Patch-Datei nach Prüfung aller Build-/Testreferenzen entfernen. Historische Release-Notizen bleiben erhalten; alte Planabschnitte als abgelöst kennzeichnen, statt vergangene Veröffentlichungen umzuschreiben.
5. Tests, die den Backport ausdrücklich verlangen, durch Tests für den offiziellen Pin, fehlende Overlay-Anwendung und korrekte Provenienz ersetzen. Den ursprünglichen Fehlerfall weiterhin fachlich prüfen: Ein einzelner Kandidat mit ungültigen aggregierten Metriken darf den gesamten GPU-Suite-Lauf nicht beenden.

**Abnahme:** Das neue Image benötigt den Patch nicht, meldet den tatsächlichen Upstream-Commit und besteht die Regression für ungültige Kandidatenmetriken.

### Andere lokale Fixes getrennt behandeln

`gpu_exact_progress.patch` und `Dockerfile.exact-progress-test` sind separate historische Versuchsartefakte. Deren Entfernung nur dann in denselben Cleanup aufnehmen, wenn Upstream-Ersatz und fehlende aktive Referenzen belegt sind.

Die Instrumentierung `_install_evaluation_progress_publisher()` in `pb8_optimize_runner.py` schreibt PBGui-spezifische Fortschrittsdaten. Sie ist nicht identisch mit PR #1871. Sie bleibt erhalten, bis ein nativer Ersatz für alle PBGui-Verbraucher nachgewiesen wurde. Ebenso bleiben `cloud_worker.py`, Guard, Transfers, Kostensteuerung und Job-Lifecycle bestehen.

## 3 Worker Upgrade und Kompatibilität gemeinsam umstellen

Ein lokales PB8-Update aktualisiert kein bereits veröffentlichtes Worker-Image. Der Backport-Cleanup allein behebt deshalb die Cloud-Kompatibilität noch nicht.

1. Neues Image aus der geprüften offiziellen Revision bauen und dessen tatsächlichen Inhalt prüfen: PB8-Commit, Rust-Stamp, Python-Abhängigkeiten einschließlich des neuen `packaging>=24.0`, GPU-Metrikvertrag und Worker-Protokoll.
2. Den neuen Metadaten-/Metrikvertrag aus genau dieser Runtime ableiten. Prüfen, ob die seit dem alten Pin geänderten HSL-, GPU- und Simulationsoptionen unterstützt sind; bestehende Sperren nur mit Nachweis aufheben.
3. Image-Digest und Revision konsistent in `vast_jobs.py`, `vast_config_validation.py`, `vast_calibration.py` sowie den tatsächlichen weiteren Pin-Verbrauchern aktualisieren. Zugehörige Größenangaben in `vast_image_layers.py` aus dem neuen Image gewinnen, nicht vom alten übernehmen.
4. Vorhandene Rental- und Job-Datensätze behalten ihren ursprünglichen Digest und PB8-Commit. Alte Allowlist-Einträge und Kalibrierungsevidenz nicht pauschal löschen oder auf die neue Revision umetikettieren. `vast_gpu_tuning.py` prüft bereits die Profilrevision gegen `REVISION`; alte Messungen dürfen nicht für die neue Revision verwendet werden. Neukalibrierung auf dem neuen Image ist ein gesondert freizugebender bezahlter Gate-Schritt vor der Freigabe gemessener Leistungsprofile. Bis dahin nur ausdrücklich als unkalibriert ausgewiesene Fallbacks verwenden.
5. Die neue Standardauswahl erst nach erfolgreicher Image-Validierung auf den veröffentlichten unveränderlichen Digest umstellen. Ein lokal gebauter Kandidat darf nicht als bereits verfügbares Produktionsimage gelten.
6. Zwei Stufen der Kompatibilitätsprüfung umsetzen:
   - **Sofort-Sperre vor dem Image-Upgrade:** Solange das aktuelle Cloud-Profil die Revision `00ce7d0` verwendet, lehnt `validate_cloud_config()` ein wirksames `scale_budget_with_excess_allowance: true` ab. Basisconfig, Szenarien und feste Runtime-Overrides berücksichtigen. Die Sperre ist unabhängig vom neuen Image-Release umsetzbar.
     Auch `false` ist ein unbekanntes Feld im alten Schema: Die Vorbereitung darf nur den booleschen Wert `false` akzeptieren und entfernt diesen mit `config_for_worker()` aus der Exportkopie. Original und neue kompatible Runtimes behalten das Feld. Bereits eingefrorene Snapshots werden strikt auf das tatsächliche Schema geprüft, unabhängig davon, ob das Feld aktiviert ist.
   - **Prüfung beim Dispatch:** Die Fähigkeiten aus `intent.image` und `intent.pb8_revision` des tatsächlichen Rentals mit den Anforderungen jedes wartenden Jobs vergleichen, bevor dieser Kandidat zugewiesen wird. Eine Prüfung nur gegen das aktuelle Standardimage reicht nicht. Nicht passende Jobs bleiben mit einer konkreten Wartemeldung in der Queue; passende nachfolgende Kandidaten dürfen weiter geprüft werden. Laufende Jobs bleiben unberührt. Bestehende Direction- und CPU-Prüfungen erhalten.
7. Für vor dem Pin-Wechsel vorbereitete, noch nicht gestartete Jobs eine ausdrückliche Entscheidung ausgeben: Entweder ist der unveränderte Snapshot mit dem tatsächlichen Rental nachweislich kompatibel, oder der Job bleibt mit dem Hinweis auf erneutes Vorbereiten wartend. Weder Snapshot noch Intent still auf den neuen Pin umschreiben. `clone_prepared()` prüft bereits den aktuellen Pin; diese Prüfung allein schützt aber nicht die Zuweisung an bestehende ältere Rentals.
8. **Pool-Gate vor dem Pin-Wechsel umgesetzt (Release-Vorbereitung 11.10.2026):** `worker_compatible_jobs()` verwendet dieselbe eingefrorene Config- und Image-/Revisionsprüfung für Pool-Planung, Angebotssuche, Rental-Start, Job-Profilauswahl und Dispatch. Freie alte Rentals zählen nur für passende Jobs. Die Slot-Zuordnung berücksichtigt Image-Fähigkeiten und CPU-Zuteilung unabhängig von Worker-/Job-Reihenfolge; gemischte Queues nutzen vorhandene kompatible Rentals. Unpassende Jobs bleiben mit einer Fehlermeldung wartend. 1.468 fokussierte Cloud-/Pool-/Queue-/Kalibrierungs-/Image-Tests bestehen; GPU-Hardwareprüfung, echte Resume-Abnahmen und Neukalibrierung sind dadurch nicht ersetzt.

**Abnahme:** Neue Rentals nutzen die neue Runtime. Bestehende Rentals bleiben lesbar, überwachbar und sauber beendbar und erhalten nur nachweislich kompatible weitere Jobs. Ein neues HSL-Feature wird auf einem alten Image vor Ausführung verständlich abgelehnt. Ein blockierter Kandidat verhindert nicht die Zuweisung eines späteren kompatiblen Jobs.

Image-Veröffentlichung, bezahlte GPU-Tests und Änderungen an bestehenden Remote-Hosts/Rentals sind gesonderte Ausführungsschritte. Dieser Plan führt sie nicht aus und ersetzt keine hierfür nötige konkrete Freigabe.

## 4 Den neuen HSL Schalter im Optimizer anbieten

Betroffene Dateien sind insbesondere `frontend/js/optimize_editor_adapter.js`, `frontend/v7_optimize.html` und bei Bedarf `pb8_config_helper.py`. Die HTML-Datei wird für PB7 und PB8 genutzt; Änderungen müssen versionsabhängig bleiben.

1. Pro Seite ein boolesches Feld für `bot.<side>.hsl.scale_budget_with_excess_allowance` ergänzen, wenn die installierte Runtime dieses Feld tatsächlich anbietet. Als Vorlage das vorhandene `hsl_enabled`-/Runtime-Control-Muster verwenden; die Feldverfügbarkeit wie bei `no_restart_drawdown_threshold` mit `hasOwnProperty` aus den Runtime-Defaults ableiten.
2. Standard `false` beibehalten. `true` ist ausschließlich im Coin-Modus zulässig, auch wenn die entsprechende HSL-Seite deaktiviert ist. Für Pside/Unified kein wirksames Feld anbieten und kein Unified-Control unter `bot.hsl` erzeugen. Bei Moduswechsel mit aktivem Schalter eine konkrete Validierungsmeldung zeigen und Speichern/Queueing blockieren, bis der Nutzer Modus oder Wert ausdrücklich korrigiert. Den Wert nicht automatisch zurücksetzen. Auch JSON-Eingaben und serverseitige Pfade validieren.
3. Werte beim Laden, Bearbeiten, Speichern, Queueing und bei Draft-Übergaben erhalten. Basis-HSL-Einstellung und `fixed_runtime_overrides` müssen dieselbe wirksame Einstellung darstellen, ohne widersprüchliche Doppelwerte. Der Schalter ist ein boolescher Runtime-Wert, kein numerischer Optimierungsparameter; keine Bounds dafür erzeugen.
   Die gewählte Darstellung ist die globale Basis-Policy pro Seite: Ein fester Override wird beim Speichern nach `bot.<side>.hsl` übernommen und entfernt. Damit wirkt der gespeicherte Wert auch bei einer späteren Live-Verwendung dieser Basisconfig, nicht nur im Optimizer. Der Guide beschreibt diese Wirkung ausdrücklich.
4. Auf PB7 und älteren PB8-Runtimes keine unbekannten Felder injizieren. Der Schalter ist globale Policy pro Seite und in Coin-Overrides verboten.
5. Die JSON-Editoren in Run/Backtest auf Werterhalt prüfen. Zusätzliche Formularfelder dort nur ergänzen, falls die vorhandene Bedienung diese Einstellung sonst verliert oder einen vergleichbaren Anspruch auf vollständige Formularabdeckung hat.

**Abnahme:** `true` und `false` überstehen den vollständigen Editor-/Save-/Queue-Rundlauf. Coin→Pside/Unified bei `true` wird mit einer verständlichen Meldung blockiert, ohne Eingaben zu löschen; nach ausdrücklicher Korrektur funktioniert der Rundlauf. Der native PB8-Standard bleibt unverändert. Alte Runtimes und PB7 funktionieren weiterhin.

## 5 GPU Automatik ohne Verlust von null Werten

Betroffene Stellen sind `normalizeOptimizeCpuCount()`, Rendering und Save-Logik in `frontend/v7_optimize.html`, die isolierte Config-Bridge sowie die Cloud-Export-/Validierungspfade.

1. Das Auto-Modell an `gpu.exact_workers: null` mit `tuning_mode != "off"` ausrichten. `0` bedeutet Vererbung von `n_cpus`, positive Zahlen bedeuten feste Worker-Zahlen. `n_cpus` ist Fallback bzw. Startwert; ohne verfügbaren Fallback gilt nativ ein Worker. GPU-Automatik für Exact-Workers nicht allein durch ein nullable `n_cpus`-Feld darstellen.
2. Für PB8/GPU eine klare englische Auswahl für `Auto`, Vererbung und feste Exact-Worker-Zahlen anbieten. `gpu.max_pending_exact` ebenfalls mit dem nativen Unterschied zwischen `null` und `0` behandeln. Rendering, `populateOptimizeGpuSettings()` und `collectOptimizeGpuSettings()` gemeinsam korrigieren: Der Populate-Pfad leert aktuell Null-Felder, der Collect-Pfad macht daraus wieder `0`. Den gewählten Wert durch Formular-/JSON-Wechsel, Speichern und Queueing erhalten.
3. Eine gemeinsame Sanitizing-Funktion in `pb8_config_helper.py` einführen, die sowohl `_prepare()` als auch die Operation `load` verwenden. Nach erfolgreicher nativer Vorbereitung und Sanitizing explizites `optimize.n_cpus: null` für GPU wiederherstellen. Fehlenden Key, explizites `null` und feste Zahlen unterscheiden; keine pauschale Wiederherstellung aller Null-Felder und kein Umgehen nativer Validierung.
4. Der Ladepfad benötigt dafür die Roh-Eingabe zusätzlich zur vorbereiteten Config. Er liest bereits `raw_config` aus der Datei, bevor `load_prepared_config()` aufgerufen wird; diese vorhandene Roh-Eingabe an die gemeinsame Funktion weitergeben, statt einen weiteren unabhängigen Dateiread einzuführen. Eigene Regression mit einer temporären Config-Datei: `n_cpus: null` überlebt Laden und anschließendes Speichern. Vorbereitungs-, Lade- und Queue-Rundlauf separat prüfen.
5. CPU-Backends und PB7 behalten ihren gültigen Ganzzahlvertrag; `n_cpus: null` nur für den unterstützten GPU-Pfad anbieten. Ungültige, negative und nicht-endliche Werte werden abgelehnt.
6. Die Null-Verbraucher in `api/optimize_v8.py`, `vast_config_validation.py` und `frontend/js/vast.js` berücksichtigen: `dict.get(key, default)` liefert bei vorhandenem Null-Key keinen Fallback; `Number(null)` liefert `0`. Für Cloud-Vorbereitung gültige konkrete Zahlen anhand des bestehenden Allokationsvertrags ableiten oder verständlich ablehnen. Keine unbeabsichtigte Null→0-Umdeutung.
7. Der Autostart-Override schreibt derzeit nur `n_cpus`; `exact_workers: null` bleibt automatische Skalierung. Dieses Zusammenspiel dokumentieren und testen, statt einen festen Exact-Pool zu behaupten. Das Cloud-Feld `auto_cpu_workers` steuert die Instanz-/Container-Allokation und ist von PB8-Auto zu unterscheiden. Die Cloud-Begrenzung auf konkrete Worker-Zahlen nicht ohne geprüften Runtime-/Ressourcenvertrag öffnen.
   `native_job_config()` setzt in der Cloud-Kopie sowohl `n_cpus` als auch `gpu.exact_workers` auf die konkrete Worker-Zahl (Fallback 4); bei Cloud-Auto wird diese anschließend aus der gemessenen CPU-Allokation abgeleitet. Auf dem Cloud-Worker entsteht daraus keine native automatische Exact-Pool-Skalierung. Die lokale GPU-Automatik bleibt davon getrennt.

**Abnahme:** Auto und feste Werte haben nachweisbar unterschiedliche, korrekte Wirkung; kein unbeabsichtigter Wechsel beim Speichern. Bestehende feste CPU-Einstellungen bleiben stabil.

## 6 Config Migration und neue Ergebnisverzeichnisse absichern

Hier zunächst Regressionen ergänzen, anschließend nur belegte Defekte korrigieren:

- `bounded` wird über die native PB8-Migration entfernt; `raw` bleibt ein prüfpflichtiger Fehler, auch in Overrides und Szenarien.
- Neu hydratisierte adaptive Forager-/Cooldown-Bounds bleiben feste Bereiche am konfigurierten Wert. Laden, Strategiewechsel und Speichern dürfen daraus keine geöffneten Suchräume machen. Vorhandene explizite Bounds haben Vorrang.
- Backtest-, Suite- und Optimizer-Ergebnisse mit neuem Session-Layout werden gefunden. Relative Suite-Artefaktpfade werden unter dem zulässigen Ergebnisroot aufgelöst. Alte Layouts bleiben lesbar.
- Result-to-Run, Result-to-Optimize, HSL-Berichte, Resume und Ergebnislöschung funktionieren auch mit den neuen Verzeichnisnamen und direkter Szenarioablage. `session.json` nur auswerten, wenn ein tatsächlicher Verbraucher es benötigt.
- Die konkreten `/results/*`-Verbraucher in `api/backtest_v8.py` sowie `/results/resume` und `/results/delete` in `api/optimize_v8.py` prüfen. Suite-Szenarien liegen nun direkt unter dem durch `safe_component()` erzeugten Szenarionamen; keine Pflichtannahme eines zusätzlichen Exchange-Unterverzeichnisses beibehalten.
- Die Version `8` gehört zur GPU-Checkpoint-Signatur, nicht zu einer allgemeinen GPU-Cache-Version. PB8 entscheidet über die tatsächliche Checkpoint-Kompatibilität; ein Checkpoint mit alter Signatur darf nicht still unter neuer Such-/HSL-Semantik fortgesetzt werden. Der PBGui-Runner speichert einen vom nativen Einstieg geworfenen Fehler im Status. Eine Signaturprüfung im `/results/resume`-Endpoint oder eine durchgehende verständliche UI-Anzeige ist mit dieser Änderung nicht implementiert bzw. belegt. Echte Regressionen für inkompatibles altes und kompatibles neues Resume einschließlich API-/UI-Fehlerdarstellung bleiben offen. Ein frischer Suchlauf bzw. eine zulässige Seed-Übernahme ist vom echten Resume zu unterscheiden.

## 7 Umsetzung und Verifikation

Empfohlene Reihenfolge: zuerst die Sofort-Sperre für den nicht unterstützten HSL-Schalter und die Prüfung beim Dispatch umsetzen; danach gemeinsamen Null-Erhalt und HSL-/Auto-Felder. Anschließend das offizielle Worker-Image ohne Backport validieren, den neuen Digest auswählen und mit gesonderter Freigabe neu kalibrieren. Die Sofort-Sperre wartet nicht auf Build oder Veröffentlichung. Jede Stufe bekommt ihre zugehörigen Regressionen.

Vor den Code-Edits wurde der GitNexus-Index erfolgreich aktualisiert und der Helper-/Editor-/Vast-Umfang einmal gemeinsam analysiert. Die aufgelösten Backend-Symbole ergaben LOW. Dynamische Inline-Editor-Aufrufe waren im Index nicht auflösbar (UNKNOWN) und wurden durch gezielte Quellenprüfung und ausführbare JavaScript-Regressionen ergänzt.

Fokussierte Testbereiche:

- Config/Runtime: `tests/test_pb8_config.py`, `tests/test_pb8_gpu_runtime.py`, `tests/test_pb8_optimize_runner.py`.
- Editor/Results: `tests/test_backtest_v8_api.py`, `tests/test_optimize_v8_api.py` und die vorhandenen Adapter-/Frontend-Vertragstests nach Testinventar.
- Worker/Cloud: `tests/test_vast_worker_image_upgrade.py`, `tests/test_vast_direction_preflight.py`, `tests/test_vast_queue.py`, `tests/test_vast_jobs.py` sowie passende Tests für Cloud-Validierung, Kalibrierung und Export.
- Neue gezielte Regressionen für HSL-Werterhalt und Modusvalidierung, Auto-/Null-Semantik in beiden Helper-Pfaden, Dispatch auf alte Images, Überspringen inkompatibler oder unlesbarer Kandidaten und die fachliche Wirkung des gemergten PR-1871-Fixes. Vorbereitete Jobs beim tatsächlichen Pin-Wechsel, durchgehende Session-/Resume-/Bounds-Verbraucher und echte Checkpoint-Signaturen sind gesonderte offene Integrationsabnahmen, wie im Umsetzungsstand ausgewiesen.

Tests verwenden isolierte Daten und Netzwerk-Mocks. Die bereits beobachteten Hänger separat eingrenzen und im Ergebnis ausweisen; sie dürfen nicht durch stilles Weglassen als bestanden gelten. Vor einer vollständigen Offline-Suite zwingend `check_test_suite` aufrufen und dessen Verdict befolgen; einen vollständigen Lauf anschließend registrieren.

Für UI-Änderungen `docs/design_guide.md` befolgen, Cache-Versionen erhöhen und die betroffenen Guides in `docs/help/43_pbv8_optimize.md` und `docs/help_de/43_pbv8_optimize.md` sowie gegebenenfalls die Vast-/Run-/Backtest-Guides synchronisieren. Keine manuellen Refresh-Controls einführen; Navigation, aktuelle Auswahl und ungespeicherte Eingaben erhalten.

Zum Abschluss Changelog aktualisieren, bei API-/Startup-/Runtime-Änderungen `api/serial.txt` erhöhen und `git diff --check` ausführen. Vor einem genehmigten Commit zusätzlich vollständige Graph-Änderungsanalyse durchführen. Commit und Push nur nach ausdrücklicher Zustimmung.

## 8 Prüfauftrag für Claude

Bitte diesen Plan zuerst gegen die tatsächlichen Quellen prüfen, noch nicht implementieren. Besonders prüfen:

1. Ist PR #1871 in der gewählten offiziellen Worker-Zielrevision vollständig enthalten, und sind alle aktiven Patch-/Provenienz-/Testreferenzen erfasst?
2. Werden der gemergte Worker-Fix, historische Exact-Progress-Versuche und die weiter benötigte PBGui-Fortschrittsinstrumentierung sauber unterschieden?
3. Ist die vorgesehene HSL-Anbindung für Coin-/Pside-/Unified-Modi und feste Runtime-Overrides korrekt?
4. Erhält die gemeinsame Sanitizing-Funktion explizites GPU-`n_cpus: null` sowohl beim Vorbereiten als auch beim dateibasierten Laden, ohne fehlende Werte oder ungültige CPU-Konfigurationen umzudeuten? Sind `exact_workers`, `max_pending_exact` und Autostart-/Cloud-Allokation korrekt getrennt?
5. Prüft der Dispatch die tatsächliche Rental-Revision für jeden Kandidaten, einschließlich Szenarien und fester Overrides? Bleiben inkompatible Jobs verständlich wartend und kompatible nachfolgende Jobs zuweisbar? Sind vor dem Pin-Wechsel vorbereitete Jobs sowie die Zuordnung alter Kalibrierungsevidenz abgedeckt?
6. Reichen die Regressionen für neue Suite-Artefaktpfade, Bounds-Erhalt und inkompatibles bzw. kompatibles Resume mit der Checkpoint-Signatur aus? Welche konkret belegten Risiken oder fehlenden Verbraucher sind zu ergänzen?

Erwartetes Review: konkrete Befunde mit Datei-/Symbolbezug und Schweregrad, notwendige Planänderungen sowie danach eine Einschätzung, ob der Plan umsetzungsreif ist. Keine eigenmächtige Änderung an PB8, laufenden Bots, Remote-Hosts oder Rentals.


## Internal verification and review history

- Added protocol and Cloud schema regressions, verified native helper main/serve preflight with GPU availability mocked, and checked exported HSL blocks against the pinned worker's original normalizer. No optimization, rental or image activation was performed.
- Added an offline regression using the complete, unmodified native PB8 `b9d6692ea` default template with recorded source provenance. It checks removal of only the two disabled HSL fields for older workers, preservation of the original config, and unchanged handling by the new revision.
- Clarified review coverage: mocked bridge/runner tests do not establish native checkpoint acceptance or end-to-end resume display. Added missing/malformed snapshot dispatch regressions, documented fixed Cloud worker allocation and HSL override canonicalization, and made compatibility-aware pool scheduling a required gate before activating the new worker pin.

- Reproduced the reported local helper-protocol and disabled Cloud HSL-field failures before implementing the fixes listed above. The earlier enabled-feature guard alone did not resolve the disabled-field schema mismatch.
- Updated the PB8 adaptation plan after review with preparation and dispatch compatibility checks, shared null preservation for prepare/load, Coin-only HSL mode validation without resets, the complete worker cleanup reference list, and checkpoint-resume and recalibration requirements.
- Verified the follow-up PB8 review: native sanitizing replaces GPU `n_cpus: null` with the template default, including the separate config-load path. Confirmed Coin-only HSL budget scaling and that supported older rental images can still claim queued jobs; compatibility checks remain necessary at dispatch.
- Added a review plan for the October 10 PB8 update, including removal of the PR #1871 worker backport after adopting the official merged runtime, HSL/GPU editor changes, and compatibility checks for existing worker images.
- Reviewed the local PB8 update from `061e472e3` to `b9d6692ea` (config schema `v8.6.0`). Isolated config loading and metadata requests work; obsolete bounded excess-allowance policies migrate, while raw policies require review and re-backtesting. Follow-up work is needed for the new HSL budget-scaling control in the optimizer editor, GPU automatic CPU-count handling, and compatibility validation against the older pinned Vast worker. This review does not implement those changes.
