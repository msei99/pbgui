# PBv8 Optimize

PBv8 Optimize verwaltet Passivbot-V8-Optimizer-Configs, Queue-Jobs, Ergebnisse und Pareto-Kandidaten getrennt von PBv7. Die Seite verwendet dieselbe Vorlage, dieselben Panels und denselben visuellen Editor wie PBv7 Optimize. Ein Versionsadapter uebersetzt nur PB8-API-Pfade und das verschachtelte Config-Modell; es gibt keine separate PB8-Optimizer-Oberflaeche.

## Configs

- **New Config** laedt Optimizer-Defaults, Strategien, Bounds, Scoring-Metriken, Limits, Backend-Optionen und Pymoo-Auswahl aus der installierten PB8-Runtime.
- Alle installierten PB8-Strategien werden unterstuetzt: `trailing_martingale`, `ema_anchor` und `trailing_grid_v7`.
- Ein Wechsel von `strategy_kind` aktiviert die von der Runtime gelieferten Bot-Defaults und Bounds dieser Strategie, ohne angepasste inaktive Strategy-Bloecke zu loeschen. Ungespeicherte Bounds und Bot-Werte werden beim Umschalten pro Strategie zwischengespeichert. Die aktuelle Runtime liefert 84 Controls fuer `trailing_martingale`, 58 fuer `ema_anchor` und 86 fuer `trailing_grid_v7`.
- Der visuelle Editor liest und schreibt verschachtelte PB8-Bot- und Bound-Pfade. Raw JSON bleibt synchron und erhaelt zukuenftige oder seltene Expertenfelder, einschliesslich unbekannter `fixed_runtime_overrides` und kanonischer oder kurzer `fixed_params`-Selektoren.
- Haeufig verwendete Optimizer-Controls bleiben in den vorhandenen PBv7-Editorbereichen. PB8-spezifischer RNG Seed, Fine-Tune-Selektoren, Polish Percentage und Polish Bounds Mode sind integriert, ohne einen zweiten Editor zu bauen.
- Gespeicherte Configs werden durch PB8 validiert und als recoverable Bundles unter `data/opt_v8` abgelegt.
- Die offizielle **Convert to V8**-Migration steht fuer PBv7-Optimize-Configs bereit. Die Migration stoppt, wenn PB8 ungeloeste oder manuell zu pruefende Felder meldet.
- PBv7-Pareto-Kandidaten bieten dieselbe offizielle Migration und werden nur aus verwalteten PB7-Resultverzeichnissen akzeptiert.

Der PB8-Editor zeigt alle installierten HSL-Modi und Optimizer-Overrides in getrennten Long- und Short-Karten. **HSL enabled** bestimmt, ob das Hard-Stop-Verhalten an den Optimizer-Auswertungen teilnimmt. **Restart after RED** ist eine feste Auswahl aus `always`, `threshold` und `never`; `always` ist PB8s Optimize-Default, damit Auswertungen nach dem Cooldown fortgesetzt werden, statt wegen persistentem Drawdown dauerhaft zu enden. `polish_percentage` erscheint als normaler Prozentwert, wird aber in PB8s Bruchwert fuer `--polish-pct` umgerechnet; `20` bedeutet also `0.20`. Pymoo behaelt PB8s native automatische Groessen: NSGA-II verwendet `250`, waehrend NSGA-III seine Reference Directions aus einem Budget von `500` ableitet.

PB8s Default-Optimize-Bounds sind Startbereiche fuer die Suche und keine harten Slider-Grenzen. Der Editor verwendet deshalb Parameter-Range-Metadaten fuer den Slider und erlaubt Werte unterhalb der PB8-Defaults, beispielsweise `n_positions = 1`.

Die Forager-Slider fuer Volume- und Volatility-EMA-Spans haben ein Minimum von `1`. Um diese Parameter nicht zu optimieren, bleibt ein gueltiger positiver Bot-Wert gesetzt und die jeweilige Zeile wird mit **Fixed** fixiert, statt den Span auf null zu setzen. Die Backend-Validierung akzeptiert importierte Null-Spans weiterhin nur dann, wenn die zugehoerigen Forager-Signale garantiert deaktiviert bleiben.

Mehrere ausgewaehlte Exchanges behalten PB8s kombiniertes Dataset-Verhalten. Fuer getrennte Exchange-Pruefungen muessen explizite Suite-Szenarien verwendet werden.

## Queue

Queue-Eintraege enthalten unveraenderliche PB8-Config-Snapshots. Eine spaetere Aenderung der gespeicherten Config veraendert keinen bestehenden Queue-Eintrag.

Wird der Editor dagegen ausdruecklich aus einer Queue-Zeile geoeffnet, aktualisiert **Save** sowohl die verwaltete Config als auch den Snapshot genau dieses Queue-Eintrags. Aenderungen wie `optimize.n_cpus` sind damit beim erneuten Oeffnen oder Starten der Zeile enthalten.

Der Editor merkt sich ausserdem seinen Navigationsursprung: **Home** oder **Save** fuehrt eine aus der Queue geoeffnete Config zur Queue zurueck; eine aus Configs geoeffnete Config kehrt dorthin zurueck.

- **Start** startet den ausgewaehlten Eintrag manuell.
- **Stop** beendet nur den verifizierten PB8-Optimizer-Prozess.
- **Requeue Fresh** startet einen neuen Lauf ohne bisherigen Optimizer-Zustand.
- **Continue from Pareto** verwendet verwaltete Pareto-Dateien als `--start`-Seeds.
- **Resume Checkpoint** setzt mit `--resume` den exakten verwalteten Optimizer-Zustand fort.

Checkpoint Resume akzeptiert nur lokale, von PBGui verwaltete PB8-Ergebnisse. Beliebige Checkpoint-Dateien werden abgelehnt, weil Python-Pickle-Checkpoints als vertrauenswuerdige ausfuehrbare Daten behandelt werden muessen.

PBGui bietet exaktes Resume nur an, wenn Checkpoint und `all_results.bin` lesbar sind, `write_all_results` aktiv war, eine Config wiederhergestellt werden kann und PB8 die Kompatibilitaet bestaetigt. Config und Queue-Eintrag werden danach in einer Transaktion erzeugt. Reine Checkpoint-Resultverzeichnisse benoetigen keine separate Pareto-JSON-Config.

PB7 und PB8 teilen einen automatischen Optimizer-Slot: Autostart startet nie beide Versionen gleichzeitig. Explizite manuelle Starts duerfen parallel laufen. Jeder Optimizer verwaltet seine interne Parallelitaet ueber `optimize.n_cpus`.

PB7 und PB8 verwenden eine gemeinsame Queue-**Settings**-Konfiguration. Speichern auf einer der beiden Optimize-Seiten steuert sofort beide Queues und beide Autostart-Worker. **Autostart CPU** kann jederzeit bearbeitet und gespeichert werden; **Override config CPU** entscheidet, ob dieser Wert `optimize.n_cpus` bei automatischen Starts ersetzt, waehrend manuelle Starts den Config-Wert behalten. **Use PBGui Market Data** setzt die verwaltete OHLCV-Quelle nur in einer Launch-Kopie und veraendert weder die gespeicherte Config noch den unveraenderlichen Queue-Snapshot.

Laufende PB8-Optimizer-Jobs ueberstehen einen API-Neustart. Unter Linux laeuft jeder Optimizer in einer eigenen transienten User-systemd-Unit ausserhalb der Cgroup des API-Service. PBGui speichert Prozess-ID, Prozess-Startzeit, PB8-Version und PB8-Commit, damit veraltete oder wiederverwendete Prozess-IDs nicht versehentlich gesteuert werden.

Permanente Vorbereitungsfehler setzen nur ihre Queue-Zeile auf einen handlungsfaehigen Fehlerstatus; Update- oder Runtime-Lock-Konflikte bleiben fuer einen erneuten Versuch gequeued. Beim Start gleicht PBGui Queue-Snapshots, Launch-Verzeichnisse sowie PID-, Ready- und State-Dateien ab, ohne unverifizierte Prozesse zu signalisieren. Der PB8-Controller erscheint im **Services Monitor** und bleibt nach unerwarteten Worker-Loop-Fehlern aktiv.

**OHLCV Readiness** und Preload laufen ueber PB8s eigenes Virtualenv, Planner, Cache-Pfade und den nativen Befehl `passivbot download`. Explizite Read-only-Quellen ausserhalb der freigegebenen PB8- oder PBGui-Market-Data-Roots werden abgelehnt, statt auf PB7 zurueckzufallen.

## Results Und Paretos

Ergebnisse werden nur aus `<pb8dir>/optimize_results` gelesen. Die Panels Results und Paretos bieten den gemeinsamen PB7-Workflow fuer Ergebnisansicht, Loeschen, 3D-Plots, Pareto Dash, Kandidaten-JSON, Metrik-Zusammenfassungen und Seed-Bundles.

PB8 unterscheidet drei Workflows:

- Ein Pareto-Kandidat als PB8-Backtest-Draft startet einen eigenstaendigen Backtest.
- Ein neuer PB8-Optimize-Draft verwendet einen oder mehrere Pareto-Kandidaten als Seeds.
- Checkpoint Resume setzt den bestehenden Backend-Zustand und Resultstream fort.

Der gemeinsame Pareto Explorer verwendet versionsspezifische Roots und versteht PB8s verschachtelte Bounds und Bot-Parameter, Scoring-Ziele, Limits, Suite-Metriken und inkrementelle `all_results.bin`-Eintraege.

Suite-Summaries behalten ihre konfigurierten Objective- und Szenarionamen und unterstuetzen `mean`, `min`, `max`, `std` und `median`. Listeneintraege enthalten ausserdem das kanonische Feld `gain`, das in dieser Reihenfolge aus PB8s gespeicherter Metrik `gain_usd`, `gain_strategy_eq` oder `gain` stammt. Statistiken verwenden wenn vorhanden den angeforderten Metrik-Statistikwert und sonst das gespeicherte skalare Objective. Nicht zugehoerige Suite-Statistiken werden nicht an den Browser uebertragen. Ein kompakter Dateisignatur-Cache beschleunigt wiederholte grosse Pareto-Listen; geaenderte, geloeschte, fehlerhafte oder gerade neu geschriebene Kandidaten werden einzeln behandelt.

Result-Aktionen sind nur aktiv, wenn ihre benoetigten Artefakte vorhanden sind. Ein verifizierter Optimizer blockiert das Loeschen nur fuer das exakte direkte Result-Verzeichnis, aus dem er oder einer seiner rekursiven Child-Prozesse eine Datei geoeffnet hat. Nicht zugehoerige aeltere Results bleiben loeschbar. Continue-Queue-Quellen und Pareto-Dash-Sessions bleiben exakte Loeschblocker; unsichere Ownership eines aktiven Prozesses wird konservativ behandelt. Batch-Loeschen erhaelt diese Konfliktdetails und staged die ausgewaehlten Verzeichnisse atomar. Pareto Dash laeuft ueber einen Credential-isolierten, begrenzten PBGui-Proxy mit Idle-Cleanup und verifizierter Orphan-Recovery. Das PBGui-Fenster kann am Header verschoben und an allen Kanten und Ecken vergroessert oder verkleinert werden; das Dashboard behaelt die urspruengliche native PB8-Darstellung.

## Archive

PB8-Optimize-Configs und PB8-Backtest-Ergebnisse verwenden den bestehenden Archive-Workflow. Dateien werden unter ihrer `config_version` gespeichert, damit PB7- und PB8-Inhalte einander nicht ueberschreiben. Import, Export, Ansicht, Loeschen, Restore und Handoffs verwenden immer den Parser der archivierten Config-Version.
