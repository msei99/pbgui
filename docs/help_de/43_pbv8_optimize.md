# PBv8 Optimize

PBv8 Optimize verwaltet Passivbot-V8-Optimizer-Configs, Queue-Jobs, Ergebnisse und Pareto-Kandidaten getrennt von PBv7. Die Seite verwendet dieselbe Vorlage, dieselben Panels und denselben visuellen Editor wie PBv7 Optimize. Ein Versionsadapter uebersetzt nur PB8-API-Pfade und das verschachtelte Config-Modell; es gibt keine separate PB8-Optimizer-Oberflaeche.

Wenn PB8 nach einer unvollstaendigen Installation oder Aktualisierung nicht verfuegbar ist, erscheint oberhalb der Arbeitsflaeche dauerhaft **PB8 update required** mit dem Runtime-Fehler und einem Link zum VPS Manager. Die Seite bleibt fuer die Diagnose nutzbar, statt den Hinweis nur als kurzlebige Benachrichtigung zu zeigen.

Die Configs-Liste startet parallel zu den langsameren PB8-Settings und -Metadaten. Ihre Tabelle verwendet eine leichte Summary-Anfrage ohne Optimize-Result-Inspektion; das getrennte Results-Panel laedt weiterhin die vollstaendigen Result-Metadaten.

## Configs

- **New Config** laedt Optimizer-Defaults, Strategien, Bounds, Scoring-Metriken, Limits, Backend-Optionen und Pymoo-Auswahl aus der installierten PB8-Runtime.
- Alle installierten PB8-Strategien werden unterstuetzt: `trailing_martingale`, `ema_anchor` und `trailing_grid_v7`.
- Ein Wechsel von `strategy_kind` aktiviert die von der Runtime gelieferten Bot-Defaults und Bounds dieser Strategie, ohne angepasste inaktive Strategy-Bloecke zu loeschen. Ungespeicherte Bounds und Bot-Werte werden beim Umschalten pro Strategie zwischengespeichert. Die aktuelle Runtime liefert 84 Controls fuer `trailing_martingale`, 58 fuer `ema_anchor` und 86 fuer `trailing_grid_v7`.
- Der visuelle Editor liest und schreibt verschachtelte PB8-Bot- und Bound-Pfade. Raw JSON bleibt synchron und erhaelt zukuenftige oder seltene Expertenfelder, einschliesslich unbekannter `fixed_runtime_overrides` und kanonischer oder kurzer `fixed_params`-Selektoren.
- Haeufig verwendete Optimizer-Controls bleiben in den vorhandenen PBv7-Editorbereichen. PB8-spezifischer RNG Seed, Fine-Tune-Selektoren, Polish Percentage und Polish Bounds Mode sind integriert, ohne einen zweiten Editor zu bauen.
- Gespeicherte Configs werden durch PB8 validiert und als recoverable Bundles unter `data/opt_v8` abgelegt.
- Die Configs-Tabelle zeigt die aktive PB8-Strategie und kann nach Strategy sortiert werden.
- Die offizielle **Convert to V8**-Migration steht fuer PBv7-Optimize-Configs bereit. Die vollstaendige Config wird an PB8 uebergeben und als ungespeicherte Editor-Vorschau geoeffnet; bis zum ausdruecklichen Speichern durch den Benutzer wird kein Config-Bundle erstellt oder ersetzt. Der Migrationsreport bleibt an der Vorschau und wird mit diesem manuellen Save persistiert. Blockierend sind nur Befunde aus `optimize`, `backtest` und `bot`, die eine Optimize-Auswertung beeinflussen koennen. Reine Run-`live`-Befunde blockieren diesen Kontext nicht. PBGui-Metadaten und der redundante Legacy-Default `max_pending_starting_evals_per_cpu=1` werden vor der Migration entfernt. Nach der PB8-Migration entfernt PBGui strategy-inkompatible Optimizer-Overrides, schreibt kanonische Fixed-Runtime-Pfade, fixiert bereits deaktivierte Seiten und stellt implizite positive V7-Enforcer-Schwellen wieder her. Diese deterministischen Korrekturen werden als `ok_with_adjustments` protokolliert; widerspruechliche oder nicht aufloesbare Pfade blockieren weiterhin die Vorschau. Ausschliesslich gewichtetes Scoring, ADG-/MDG-Floors, eingefuegte V8-Defaults und fixierte neue Cooldown-Bounds erzeugen Report-Warnungen, werden aber nie als Optimizer-Rezept umgeschrieben. Echte Fehler zeigen eine begrenzte Liste von Feldern und Verhaltenswarnungen statt des vollstaendigen Migrationsreports.
- PBv7-Pareto-Kandidaten bieten dieselbe offizielle Migration und werden nur aus verwalteten PB7-Resultverzeichnissen akzeptiert.

Der PB8-Editor zeigt alle installierten HSL-Modi und Optimizer-Overrides in getrennten Long- und Short-Karten. **HSL enabled** bestimmt, ob das Hard-Stop-Verhalten an den Optimizer-Auswertungen teilnimmt. **Restart after RED** ist eine feste Auswahl aus `always`, `threshold` und `never`; `always` ist PB8s Optimize-Default, damit Auswertungen nach dem Cooldown fortgesetzt werden, statt wegen persistentem Drawdown dauerhaft zu enden. `polish_percentage` erscheint als normaler Prozentwert, wird aber in PB8s Bruchwert fuer `--polish-pct` umgerechnet; `20` bedeutet also `0.20`. Pymoo behaelt PB8s native automatische Groessen: NSGA-II verwendet `250`, waehrend NSGA-III seine Reference Directions aus einem Budget von `500` ableitet.

PB8s `gpu`-Backend bedeutet experimentelles **Apple MPS**, nicht CUDA. PBGui unterscheidet ein von PB8 registriertes Backend von einem auf dem aktuellen Host verfuegbaren Backend. GPU bleibt auf nicht unterstuetzten Hosts als ausdrueckliche Editor-Vorschau auswaehlbar, damit alle Felder getestet und die portable Config gespeichert werden koennen, ohne das Backend still zu ersetzen. Queue und Start brechen weiterhin vor Snapshot- oder Prozesserzeugung mit PB8s genauem Runtime-Grund ab. PBGui-Installation und PB8-Updates fordern das optionale Profil `gpu-mps` an; dessen Plattform-Marker installiert PyTorch nur auf Apple Silicon.

Bei ausgewaehltem GPU-Backend zeigt der Editor PB8s Runtime-Defaults fuer nullable Population-, Batch- und Candidate-Bar-Groessen, M3 Lean Auto-Parallelism, Exact-Worker-, Drift- und Checkpoint-Controls sowie Successive Halving. Die Controls sind als **Automatic sizing**, **Exact validation & checkpointing**, **Drift safety** und **Successive halving** gruppiert. Sie verwenden das normale responsive Acht-Spalten-Raster des Editors: 8×1 Felder auf breiten, 4×2 auf mittleren und 2×4 auf kleinen Bildschirmen. Leere Sizing-Felder behalten PB8s automatische Defaults und zeigen den effektiven Runtime-Wert als Platzhalter `auto (…)`; eine eingetragene Zahl deaktiviert die Automatik bewusst fuer dieses Feld. **Reset GPU defaults** stellt die Defaults der installierten Runtime wieder her, ohne unbekannte zukuenftige GPU-Keys zu loeschen. Neue Scoring- und Limit-Auswahlen verwenden PB8s GPU-Proxy-Allowlist; vorhandene inkompatible Eintraege bleiben zur Reparatur sichtbar und PB8s nativer Preflight blockiert sie vor Queue oder Start.

PB8s Default-Optimize-Bounds sind Startbereiche fuer die Suche und keine harten Slider-Grenzen. Der Editor verwendet deshalb Parameter-Range-Metadaten fuer den Slider und erlaubt Werte unterhalb der PB8-Defaults, beispielsweise `n_positions = 1`.

Die Forager-Slider fuer Volume- und Volatility-EMA-Spans haben ein Minimum von `1`. Um diese Parameter nicht zu optimieren, bleibt ein gueltiger positiver Bot-Wert gesetzt und die jeweilige Zeile wird mit **Fixed** fixiert, statt den Span auf null zu setzen. Die Backend-Validierung akzeptiert importierte Null-Spans weiterhin nur dann, wenn die zugehoerigen Forager-Signale garantiert deaktiviert bleiben.

Mehrere ausgewaehlte Exchanges behalten PB8s kombiniertes Dataset-Verhalten. Fuer getrennte Exchange-Pruefungen muessen explizite Suite-Szenarien verwendet werden.

PB8.1-Scoring-Objectives koennen das globale **Objective Scenario** erben, ausdruecklich das Suite-Aggregat verwenden oder ein benanntes Suite-Szenario auswaehlen. Aggregate unterstuetzen `mean`, `min`, `max`, `std` und `median`. Limits koennen das Suite-Aggregat mit ausgelassenem Scenario verwenden, ein ausdrueckliches `scenario: null` erhalten oder ein benanntes Suite-Szenario auswaehlen; ausgelassen und explizit null haben dieselbe Laufzeitbasis, bleiben aber strukturell verschieden. PBGui liest das kanonische Reduktionsfeld aus der installierten PB8-Runtime: aktuelles PB8 verwendet `reducer`, aeltere kompatible PB8-Releases verwenden `aggregate` fuer Scoring und `stat` fuer Limits. Ein benanntes Szenario darf nicht gleichzeitig ein Reduktionsfeld verwenden. Szenarionamen muessen in der aktiven Suite vorhanden sein. PBGui erhaelt diese Unterschiede beim Synchronisieren von Visual Editor und Raw JSON.

Die PB8-Marktauswahl verwendet den offiziellen Resolver fuer das vollstaendige Exchange-Set. Eindeutige Maerkte bleiben in der Config kurz; echte Multiplikator- oder Venue-Kollisionen verwenden exakte Scoped-Identifier, waehrend der Editor kompakte Labels beibehaelt. Importierte exakte IDs bleiben in Coin-Listen, Coin Sources, Suite-Szenarien und Raw JSON unveraendert.

Nach Aenderungen an Market Cap, Volumenverhaeltnis, Tags, CPT oder Notices muss **Apply Filters** verwendet werden. Die Aktion filtert jede gewaehlte Exchange, projiziert das Ergebnis durch PB8s Marktresolver und schreibt die vereinigten Werte in beide Long-/Short-Listen fuer Approved und Ignored. Speichern ohne Apply erhaelt nur die Filtermetadaten und veraendert explizite Coin-Listen nicht.

## Queue

Queue-Eintraege enthalten unveraenderliche PB8-Config-Snapshots. Eine spaetere Aenderung der gespeicherten Config veraendert keinen bestehenden Queue-Eintrag.

Wird der Editor dagegen ausdruecklich aus einer Queue-Zeile geoeffnet, aktualisiert **Save** sowohl die verwaltete Config als auch den Snapshot genau dieses Queue-Eintrags. Aenderungen wie `optimize.n_cpus` sind damit beim erneuten Oeffnen oder Starten der Zeile enthalten.

Der Editor merkt sich ausserdem seinen Navigationsursprung: **Home** oder **Save** fuehrt eine aus der Queue geoeffnete Config zur Queue zurueck; eine aus Configs geoeffnete Config kehrt dorthin zurueck.

- **Start** startet den ausgewaehlten Eintrag manuell.
- **Stop** beendet nur den verifizierten PB8-Optimizer-Prozess.
- **Requeue Fresh** startet einen neuen Lauf ohne bisherigen Optimizer-Zustand.
- **Continue from Pareto** verwendet verwaltete Pareto-Dateien als `--start`-Seeds.
- **Resume Checkpoint** setzt mit `--resume` den exakten verwalteten Optimizer-Zustand fort.

Fuer einen exakt ausgewaehlten oder laufenden Queue-Eintrag kann PBGui AI die von der Seite angebotene Aktion `show_log` aus jedem Optimize-Panel ausfuehren. Seitenuebergreifende Aktionen navigieren zu PB8 Optimize, warten auf die Queue-Daten und rufen danach dieselbe vorhandene Log-Panel-Funktion wie die Zeilenaktion auf.

Checkpoint Resume akzeptiert nur lokale, von PBGui verwaltete PB8-Ergebnisse. Beliebige Checkpoint-Dateien werden abgelehnt, weil Python-Pickle-Checkpoints als vertrauenswuerdige ausfuehrbare Daten behandelt werden muessen.

PBGui bietet exaktes Resume nur an, wenn Checkpoint und `all_results.bin` lesbar sind, `write_all_results` aktiv war, eine Config wiederhergestellt werden kann und PB8 die Kompatibilitaet bestaetigt. Config und Queue-Eintrag werden danach in einer Transaktion erzeugt. Reine Checkpoint-Resultverzeichnisse benoetigen keine separate Pareto-JSON-Config.

PB7 und PB8 teilen einen automatischen Optimizer-Slot: Autostart startet nie beide Versionen gleichzeitig. Explizite manuelle Starts duerfen parallel laufen. Jeder Optimizer verwaltet seine interne Parallelitaet ueber `optimize.n_cpus`.

PB7 und PB8 verwenden eine gemeinsame Queue-**Settings**-Konfiguration. Speichern auf einer der beiden Optimize-Seiten steuert sofort beide Queues und beide Autostart-Worker. **Autostart CPU** kann jederzeit bearbeitet und gespeichert werden; **Override config CPU** entscheidet, ob dieser Wert `optimize.n_cpus` bei automatischen Starts ersetzt, waehrend manuelle Starts den Config-Wert behalten. **Use PBGui Market Data** setzt die verwaltete OHLCV-Quelle nur in einer Launch-Kopie und veraendert weder die gespeicherte Config noch den unveraenderlichen Queue-Snapshot.

Laufende PB8-Optimizer-Jobs ueberstehen einen API-Neustart. Unter Linux laeuft jeder Optimizer in einer eigenen transienten User-systemd-Unit ausserhalb der Cgroup des API-Service. PBGui speichert Prozess-ID, Prozess-Startzeit, PB8-Version und PB8-Commit, damit veraltete oder wiederverwendete Prozess-IDs nicht versehentlich gesteuert werden.

Permanente Vorbereitungsfehler setzen nur ihre Queue-Zeile auf einen handlungsfaehigen Fehlerstatus; Update- oder Runtime-Lock-Konflikte bleiben fuer einen erneuten Versuch gequeued. Beim Start gleicht PBGui Queue-Snapshots, Launch-Verzeichnisse sowie PID-, Ready- und State-Dateien ab, ohne unverifizierte Prozesse zu signalisieren. Der PB8-Controller erscheint im **Services Monitor** und bleibt nach unerwarteten Worker-Loop-Fehlern aktiv.

Der GPU-Logstatus trennt das Exact-Validation-Budget von der Proxy-Arbeit: Das Dashboard zeigt Exact-Auswertungen und Prozent, Generation, Proxy-Auswertungen, laufende Exact-Jobs, Dispatch-Chunks und Successive-Halving-Aktivitaet. Checkpoint Resume vergleicht GPU-Policy, Pymoo-Proposal-Settings, Reducer und Ausfuehrungswerte, aktivierte Seiten sowie Approved/Ignored Coins, bevor PB8s Checkpoint-Signatur die endgueltige Entscheidung trifft.

Strategiespezifische Optimizer-Overrides werden beim Strategiewechsel entfernt und vor Save, Queue und Launch ueber die installierte PB8-Runtime validiert.

**OHLCV Readiness** und Preload laufen ueber PB8s eigenes Virtualenv, Planner, Cache-Pfade und den nativen Befehl `passivbot download`. Explizite Read-only-Quellen ausserhalb der freigegebenen PB8- oder PBGui-Market-Data-Roots werden abgelehnt, statt auf PB7 zurueckzufallen. GPU-Suites verlangen jedes szenariospezifische Exchange-Dataset statt der besten Exchange pro Coin; fehlt eine nur im Szenario benoetigte Exchange, wird die einzelne Preload-Aktion mit einer Erklaerung deaktiviert.

## Results Und Paretos

Ergebnisse werden nur aus `<pb8dir>/optimize_results` gelesen. Die Results-Tabelle zeigt fuer jeden Lauf die konfigurierte PB8-Strategie und kann nach dieser Spalte sortiert werden. Die Panels Results und Paretos bieten den gemeinsamen PB7-Workflow fuer Ergebnisansicht, Loeschen, 3D-Plots, Pareto Dash, Kandidaten-JSON, Metrik-Zusammenfassungen und Seed-Bundles.

Ein Wechsel des Optimize-Results leert vorherige Pareto-Zeilen, Metadaten und Auswahl sofort, bevor das neue Result geladen wird. Eine spaete Antwort des vorherigen Results kann keine veralteten Zeilen wiederherstellen.

Die Results-Liste verwendet begrenzte Cold-Start-Metadaten: Jedes Pareto-Verzeichnis wird einmal aufgelistet, Verzeichnis-Zeitstempel ersetzen einzelne Stats aller Kandidaten und ohne Pareto-Config wird nur der erste MessagePack-Record gelesen. Die vollstaendige Validierung von `all_results.bin` bleibt fuer Resume/Continue verpflichtend, blockiert nach einem API-Neustart aber niemals die sichtbare Results-Liste.

PB8 unterscheidet drei Workflows:

- Ein Pareto-Kandidat als PB8-Backtest-Draft startet einen eigenstaendigen Backtest.
- Pareto-Kandidaten, die in verschiedenen benannten Suite-Szenarien ausgewaehlt wurden, behalten dieses Szenario. Der Backtest-Handoff queued jeden Kandidaten nur fuer die Exchanges seines Szenarios statt eine Kandidat-mal-Exchange-Matrix zu erzeugen.
- Ein neuer PB8-Optimize-Draft verwendet einen oder mehrere Pareto-Kandidaten als Seeds.
- Checkpoint Resume setzt den bestehenden Backend-Zustand und Resultstream fort.

Der gemeinsame Pareto Explorer verwendet versionsspezifische Roots und versteht PB8s verschachtelte Bounds und Bot-Parameter, Scoring-Ziele, Limits, Suite-Metriken und inkrementelle `all_results.bin`-Eintraege.

Im PB8 Pareto Explorer oeffnet **Strategy Explorer** den ausgewaehlten Kandidaten mit seinen Sparse Overrides. Fuer einen Vergleich wird der erste Kandidat mit **Pin Explorer Baseline** fixiert, ein anderer Kandidat desselben Results ausgewaehlt und danach Strategy Explorer geoeffnet. Fehlende referenzierte Override-Dateien blockieren Pinning oder Oeffnen, statt stillschweigend ignoriert zu werden.

Suite-Summaries behalten ihre konfigurierten Objective- und Szenarionamen und unterstuetzen `mean`, `min`, `max`, `std` und `median`. Der **Columns**-Picker steuert die sortierbaren Listenmetriken und merkt sich die PB8-Auswahl. Er bietet jede numerische Metrik aus dem Pareto-JSON an, aber die Listen-API uebertraegt Werte nur fuer Defaults und aktuell ausgewaehlte Spalten. Neu ausgewaehlte Metriken werden in einem debouncten Batch geladen und danach im begrenzten Dateisignatur-LRU-Cache gehalten; Statistikwechsel und wiederholte Ansichten lesen unveraenderte Kandidaten daher nicht erneut. Auch der Picker-DOM wird bei unveraendertem Metrikkatalog wiederverwendet. Die Defaults enthalten kanonischen Gain, konfigurierte Objectives und kanonischen Drawdown; kanonische Werte bevorzugen etablierte PB8-Aliase wie `gain_usd` vor `gain_strategy_eq`. **All (slower)** aktiviert ausdruecklich eine sehr breite Tabelle und groessere Antwort; normale Ansichten bleiben kompakt. Geaenderte, geloeschte, fehlerhafte oder aktiv neu geschriebene Kandidaten werden getrennt behandelt.

Result-Aktionen sind nur aktiv, wenn ihre benoetigten Artefakte vorhanden sind. Ein verifizierter Optimizer blockiert das Loeschen nur fuer das exakte direkte Result-Verzeichnis, aus dem er oder einer seiner rekursiven Child-Prozesse eine Datei geoeffnet hat. Nicht zugehoerige aeltere Results bleiben loeschbar. Continue-Queue-Quellen und Pareto-Dash-Sessions bleiben exakte Loeschblocker; unsichere Ownership eines aktiven Prozesses wird konservativ behandelt. Batch-Loeschen erhaelt diese Konfliktdetails und staged die ausgewaehlten Verzeichnisse atomar. Pareto Dash laeuft ueber einen Credential-isolierten, begrenzten PBGui-Proxy mit Idle-Cleanup und verifizierter Orphan-Recovery. Das PBGui-Fenster kann am Header verschoben und an allen Kanten und Ecken vergroessert oder verkleinert werden; das Dashboard behaelt die urspruengliche native PB8-Darstellung.

## Archive

PB8-Optimize-Configs und PB8-Backtest-Ergebnisse verwenden den bestehenden Archive-Workflow. Dateien werden unter ihrer `config_version` gespeichert, damit PB7- und PB8-Inhalte einander nicht ueberschreiben. Import, Export, Ansicht, Loeschen, Restore und Handoffs verwenden immer den Parser der archivierten Config-Version.
