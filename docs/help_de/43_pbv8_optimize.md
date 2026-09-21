# PBv8 Optimize

PB8-Versionen mit Offline-Simulation zeigen **offline** unter Market & Universe. Verwendet werden nur lokale Kerzen und zwischengespeicherte Metadaten; fehlende Daten führen zum Fehler statt zum Download. OHLCV Readiness berücksichtigt diesen Modus und deaktiviert den Remote-Preload. Andere PBGui-Dienste werden dadurch nicht offline geschaltet. Vorbereitete Datensätze benötigen zusätzlich den von PB8 geprüften Offline-Herkunftsnachweis.

Neuere PB8-GPU-Versionen bieten **drift_rank_halt** (leer übernimmt `drift_halt`) und **drift_objective_tolerance** (Standard `0.000001`, der Zahlenwert `0` ist gültig). Die Toleranz ist eine absolute Abweichung in festen anfänglichen Zielskaleneinheiten, kein Gewinnprozentsatz. Die Übereinstimmung der Constraints verwendet weiterhin `drift_halt`. Ein lokales PB8-Update aktualisiert das festgelegte Vast-Image nicht: Dieses prüft weiterhin den skalaren Drift, akzeptiert die neuen Standardwerte zur Kompatibilität und weist eigene Werte oder Offline-Modus vor dem Einreihen zurück.

Die lokale PB8-Config-Prüfung nutzt nach dem ersten Start den verwalteten Helper-Prozess erneut. Die Draft-Wiederherstellung erlaubt TOKEN-Coin-Overrides, lehnt darin enthaltene Zugangsdatenfelder aber weiterhin ab. Nach Änderungen an Basisdaten oder Exchanges prüft Check & Apply die erhaltenen visuellen Fenster gegen die aktuellen Formularwerte; ein erneutes Öffnen des Editors ist nicht erforderlich.

**Vast.ai → Performance History** bewahrt Vast-Durchsatz und Aufgabenmerkmale nach dem Entfernen aus der Queue auf. Wähle gleiche Aufgaben für Geschwindigkeitsvergleiche; Details im [Vast-GPU-Guide](48_vast_gpu.md#performance-history).

Die Cloud-CPU-Mindestleistung stellst du unter **Min CPU cores** in den Vast Settings ein; Cloud Auto nutzt die gemietete Zuteilung. Manuelles Rent übernimmt die Eigenschaften des ausgewählten Angebots. Später hinzugefügte Jobs können eine bestätigte Umschichtung von Mietzeit in Transferreserve innerhalb desselben Budgets auslösen. Fehler beim Abruf des Optimizer-Logs verhindern Stop und Ergebnissicherung nicht mehr.

Wenn **Run on** auf **Vast.ai** steht, sind **n_cpus** und **exact_workers** im Optimizer ausgegraut. Die gepunkteten Hilfetexte erklären: **Min CPU cores** in den Vast GPU Settings bestimmt die Mindestanforderung für die Miete; der tatsächliche Lauf nutzt die effektive CPU-Zuteilung des Containers, begrenzt auf die gemietete CPU-Quote. Beim Wechsel zurück auf **Local** lassen sich die bisherigen Werte wieder bearbeiten.

**Save & Queue** zeigt die Schritte Cloud-Kompatibilitätsprüfung, Speichern der Config und Anlegen des Jobs im Editor an. Die Speicherbuttons bleiben dabei durchgehend deaktiviert; **Saving & queueing…** bleibt bis zum Abschluss oder einem Fehler sichtbar. Sobald der Server den neuen Job bestätigt, öffnet PBGui die Queue mit **Preparing**; die vollständige Queue-Aktualisierung läuft im Hintergrund weiter.

Vast-Uploads dürfen sich bei vorübergehenden Verbindungsfehlern bis zu 15 Minuten ohne neuen Transferfortschritt erholen. Zwischen Versuchen liegen 15/30/60 Sekunden; Teildaten bleiben erhalten. Das Log zeigt den Retry-Status. Nach erfolgreicher Worker-Health-Prüfung verkürzt der vorherige Setup-Timer die Upload-Zeit nicht mehr; Mietfrist und Reserve für Ergebnissicherung gelten weiterhin.

Die Vast-Angebotssuche zeigt **Previously used**, **Working** und **Preferred** an. Mit **Prefer host** in den Angebotsdetails oder der Host-Karte einer Miete bevorzugst du den Rechner innerhalb deiner bestehenden Grenzen. Präferenzen und manuelle Working-Markierungen verwaltest du unter **Hosts**. Gesperrte Hosts bleiben ausgeschlossen.

Bei Vast-Mieten schließt **Block host** in der Host-Karte des Logs den Rechner von zukünftigen Angeboten und Mieten aus. Unter **Settings → Blocked hosts** kannst du Sperren verwalten und aufheben. Die laufende Miete bleibt dabei bestehen; **End rental** ist eine separate Aktion. Details stehen im Vast-GPU-Guide.

PBv8 Optimize verwaltet Passivbot-V8-Optimizer-Configs, Queue-Jobs, Ergebnisse und Pareto-Kandidaten getrennt von PBv7. Die Seite verwendet dieselbe Vorlage, dieselben Panels und denselben visuellen Editor wie PBv7 Optimize. Ein Versionsadapter uebersetzt nur PB8-API-Pfade und das verschachtelte Config-Modell; es gibt keine separate PB8-Optimizer-Oberflaeche.

Wenn PB8 nach einer unvollstaendigen Installation oder Aktualisierung nicht verfuegbar ist, erscheint oberhalb der Arbeitsflaeche dauerhaft **PB8 update required** mit dem Runtime-Fehler und einem Link zum VPS Manager. Die Seite bleibt fuer die Diagnose nutzbar, statt den Hinweis nur als kurzlebige Benachrichtigung zu zeigen.

Die Configs-Liste startet parallel zu den langsameren PB8-Settings und -Metadaten. Ihre Tabelle verwendet eine leichte Summary-Anfrage ohne Optimize-Result-Inspektion; das getrennte Results-Panel laedt weiterhin die vollstaendigen Result-Metadaten.

## Parameter-Tooltips

Beim Überfahren eines Parameterlabels erscheint die Originalbeschreibung aus der Dokumentation des installierten Passivbot. Der Tooltip nennt die lokale Quelldatei; längere Texte lassen sich scrollen, wenn der Mauszeiger in den Tooltip bewegt wird. Derselbe Parameter erhält in Run, Backtest und Optimize dieselbe Erklärung, auch bei verschachtelten Bounds und Optimizer-Overrides. Die Dokumentation wird lokal geladen, ohne Internetanfrage oder funktionsfähige Rust-Erweiterung. PBGui-eigene Bedienelemente behalten ihre Eingabehinweise; generische Laufzeit-Platzhalter werden ausgeblendet, wenn upstream keine passende Beschreibung vorhanden ist.


## Einstellungen für PB8-Schema 8.4

Der installierte PB8-Loader migriert ältere Konfigurationen auf sein aktuelles Schema. Ab Schema 8.4 liegen die Preis-EMA-Spannen von Trailing Martingale unter `bot.<side>.strategy.trailing_martingale.entry.ema_span_0` und `ema_span_1`. Auto-Unstuck besitzt eigene `bot.<side>.unstuck.ema_span_0` und `ema_span_1`. Die Optimizer-Bounds erscheinen unter den entsprechenden Pfaden; Coin- und Szenario-Overrides folgen den nativen PB8-Migrationsregeln.

Mit `couple_unstuck_ema_spans` in den Optimizer-Overrides werden Strategie- und Unstuck-EMA-Spannen gemeinsam optimiert. Standardmäßig bleiben sie unabhängig. Bei Änderung dieser Option oder beim Upgrade älterer GPU-Checkpoints mit anderem Parameterlayout ist eine neue Optimierung erforderlich.

`optimize.pymoo.shared.mutation_prob` steuert die Mutation pro Individuum, `mutation_prob_per_variable` pro Variable. Beide Felder stehen für CPU und GPU zur Verfügung und unterstützen **auto** oder eine explizite Wahrscheinlichkeit von 0 bis 1, einschließlich 0. Auto verwendet `1 / n_params` für Individuen und `min(0.5, 1 / n_params)` für Variablen. PB8 migriert den bisherigen Wert `mutation_prob_var` nach `mutation_prob`, ohne seine Bedeutung zu verändern.

Unterschiedliche Python-/Rust-Schemata können das Laden der Metadaten verhindern, obwohl der PBGui-Editor die Felder unterstützt. **VPS Manager → Update PB8** baut und prüft die passende PB8-Laufzeit auf dem betroffenen Host.


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

PB8s experimentelles `gpu`-Backend unterstuetzt **Apple MPS** und in neueren PB8-Versionen **NVIDIA CUDA auf Linux/WSL2**. PBGui verwendet die Geraeteauswahl des installierten PB8 und zeigt die Host-Runtime sowie den NVIDIA-Geraetenamen. CUDA benoetigt ausserdem funktionierendes CuPy und NVRTC im PB8-Venv. Natives Windows-Python wird nicht unterstuetzt; WSL2 verwendet den unter Windows installierten NVIDIA-Treiber. Ein systemweites CUDA-Toolkit ist nicht erforderlich. Aeltere PB8-Versionen mit ausschliesslich MPS bleiben unterstuetzt und melden fuer CUDA ein erforderliches PB8-Update.

GPU bleibt auf nicht verfuegbaren Hosts fuer Editor-Vorschau und Save auswaehlbar. Queue und Start brechen bei fehlender GPU-Runtime vor Snapshot- oder Prozesserzeugung ab. Vollinstallationen und PB8-Updates waehlen automatisch `gpu-mps` auf Apple Silicon oder `gpu-cuda`, wenn unter Linux/WSL2 ein funktionierender NVIDIA-Treiber erkannt wird und die PB8-Version dieses Extra deklariert. CPU-Hosts verwenden `full` ohne optionale GPU-Pakete; reine VPS-Live-Installationen bleiben live-only. Der Treiber sollte vor dem PB8-Update installiert werden. Erweiterte Ansible-Aufrufe koennen `pb8_gpu_profile` auf `auto` (Default), `cpu`, `mps` oder `cuda` setzen; explizite nicht unterstuetzte Auswahlen schlagen sichtbar fehl.

PB8 waehlt den Beschleuniger automatisch; `optimize.backend: "gpu"` bleibt bestehen. Fuer Vast.ai-Laeufe skaliert PBGui CUDA-Population, Batch-Groesse, Exact-Validierungen, Drift-Probes und Drift-Fenster gemeinsam, nachdem VRAM, Speicherbandbreite und Rechenleistung der gemieteten Karte bekannt sind. Die Profile reichen von 2.048/2.048/16/8/256 auf Karten bis 8 GB bis 24.576/24.576/192/96/3.072 auf nachgewiesen schnellen Karten ab 20 GB; bei fehlenden Leistungsdaten bleibt das konservative VRAM-Profil bestehen. Passende Apple-M3-Workloads behalten ihr separates erprobtes Tuning. Mit automatischer Groesse und gemessener Exact-Worker-Zahl beginnen und anschliessend Durchsatz und VRAM-Verbrauch pruefen. Exakte Rust-Validierung bleibt massgeblich. Resume behaelt PB8s Runtime-/Checkpoint-Kompatibilitaetspruefungen bei; bei inkompatiblen Runtime-Versionen oder Beschleunigerwechseln einen neuen Lauf starten. Die GPU-Metrikauswahl beruecksichtigt Aliase, die der installierte PB8-Validator akzeptiert, darunter `long_short_profit_ratio` als Alias fuer `pnl_ratio_long_short`; Exact-only-Metriken bleiben ausgeschlossen.

Bei ausgewaehltem GPU-Backend zeigt der Editor PB8s Runtime-Defaults fuer nullable Population-, Batch- und Candidate-Bar-Groessen, M3 Lean Auto-Parallelism, Exact-Worker-, Drift- und Checkpoint-Controls sowie Successive Halving. Die Controls sind als **Automatic sizing**, **Exact validation & checkpointing**, **Drift safety** und **Successive halving** gruppiert. Sie verwenden das normale responsive Acht-Spalten-Raster des Editors: 8×1 Felder auf breiten, 4×2 auf mittleren und 2×4 auf kleinen Bildschirmen. Leere Sizing-Felder behalten PB8s automatische Defaults und zeigen den effektiven Runtime-Wert als Platzhalter `auto (…)`; eine eingetragene Zahl deaktiviert die Automatik bewusst fuer dieses Feld. **Reset GPU defaults** stellt die Defaults der installierten Runtime wieder her, ohne unbekannte zukuenftige GPU-Keys zu loeschen. Neue Scoring- und Limit-Auswahlen verwenden PB8s GPU-Proxy-Allowlist; vorhandene inkompatible Eintraege bleiben zur Reparatur sichtbar und PB8s nativer Preflight blockiert sie vor Queue oder Start.

PB8s Default-Optimize-Bounds sind Startbereiche fuer die Suche und keine harten Slider-Grenzen. Der Editor verwendet deshalb Parameter-Range-Metadaten fuer den Slider und erlaubt Werte unterhalb der PB8-Defaults, beispielsweise `n_positions = 1`.

Die Forager-Slider fuer Volume- und Volatility-EMA-Spans haben ein Minimum von `1`. Um diese Parameter nicht zu optimieren, bleibt ein gueltiger positiver Bot-Wert gesetzt und die jeweilige Zeile wird mit **Fixed** fixiert, statt den Span auf null zu setzen. Die Backend-Validierung akzeptiert importierte Null-Spans weiterhin nur dann, wenn die zugehoerigen Forager-Signale garantiert deaktiviert bleiben.

Mehrere ausgewaehlte Exchanges behalten PB8s kombiniertes Dataset-Verhalten. Fuer getrennte Exchange-Pruefungen muessen explizite Suite-Szenarien verwendet werden.

Die zwei kompakten Buttons direkt neben **start_date** in PB8 Optimize ermitteln mit PB8 die ersten verfuegbaren Kerzen fuer die aktuell ausgewaehlten Exchanges und explizit freigegebenen Coins. **1st** verwendet die aelteste bekannte ausgewaehlte Markthistorie. **All** startet erst, wenn jeder ausgewaehlte Coin auf jeder ausgewaehlten Exchange einen bekannten OHLCV-Zeitstempel besitzt. Waehrend des Lookups zeigt ein kompakter Fortschrittsbalken die tatsaechlich abgeschlossenen Exchange/Coin-Paare und den aktuellen PB8-Schritt. **Stop** beendet nur diesen Lookup. PBGui addiert PB8s benoetigten Strategie-Warmup und rundet auf den ersten vollstaendig nutzbaren UTC-Tag auf, bevor das reine Datum `backtest.start_date` gesetzt wird. Fehlt ein Coin auf einer Exchange oder ist sein erster Zeitstempel unbekannt, meldet **All** das erste nicht aufloesbare Paar. Die dynamische Coin-Auswahl `all` ist nicht erlaubt; ein Lookup ist auf 200 Exchange/Coin-Paare begrenzt. Der ausdrueckliche Lookup darf PB8s nativen First-Timestamp-Cache fuellen, laedt aber nicht den vollstaendigen OHLCV-Bereich herunter. Beim Schliessen oder Ersetzen des Editors wird sein aktiver Lookup automatisch gestoppt.

Der **PB8 Scenario Generator** in Suite Mode zeigt deterministische Plaene fuer `rolling_windows`, `walk_forward` und `sweep_cycles` aus dem Basis-Datumsbereich des Editors. Fensterlaenge, Schrittweite, Anzahl der Trainings- und optionalen Holdout-Fenster sowie Exchange-Aufteilung werden serverseitig validiert und auf 64 erzeugte Szenarien begrenzt. Preview veraendert die Config nicht. **Check & Apply windows** ersetzt ausdruecklich die ungespeicherten Suite-Szenarien und den Reducer und setzt fuer alle drei Templates dasselbe Default-Rezept fuer Scoring und Limits; Holdout-Fenster bleiben ausserhalb von `backtest.scenarios` und werden als `pbgui.scenario_template`-Provenance gespeichert. Bei alten Preset-Plaenen entfernt eine manuelle Suite-Aenderung die Provenance. Explizite visuelle Fensterplaene behalten ihre Holdouts bei Aenderungen an Trainingsszenarien und Aggregation. Sweep Cycles bindet diesen unveraenderlichen Plan zusaetzlich an das PB8-Result und berechnet aus dem szenarioweisen Gain jedes Pareto-Kandidaten sequenzielle Sweep-/Refill-Cashflow-Metriken. PBGui AI bietet denselben Generator als Read-only-Preview-Tool an und muss fuer Save oder Queue weiterhin den bestehenden Proposal-Flow verwenden.

### Scenario Generator

Der Scenario Generator macht aus einer PB8-Optimize-Config eine reproduzierbare Gruppe historischer Tests. PB8 fuehrt weiterhin eine normale Suite-Optimierung aus. PBGui erzeugt die Datumsfenster, speichert den Experimentplan, berechnet nach PB8s Szenario-Metriken die Sweep-Cashflows und bereitet abschliessend die Holdout-Backtests vor.

#### Was Die Aktionen Tun

| Aktion | Was sich aendert | Was unveraendert bleibt |
| --- | --- | --- |
| **1st / All** neben `start_date` | Ermittelt ein OHLCV-basiertes Startdatum | Suite-Szenarien und Generator-Einstellungen |
| **Generate windows** | Zeigt exakte Train-/Holdout-Fenster und Warnungen | Config, Suite, Scoring, Bounds und Queue |
| **Check & Apply windows** | Aktiviert Suite Mode, setzt Train-Szenarien/Reducer, speichert Holdout-Provenance und wendet den Sweep-Preset an | Es wird noch nichts gespeichert oder gequeued |
| **Save / Save & Queue** | Speichert oder startet das angewendete Experiment | Holdout bleibt aus der Optimierung ausgeschlossen |
| **Paretos** | Zeigt PB8-Metriken plus PBGui-`sweep_*`-Cashflow-Metriken | Originale PB8-Kandidatenmetriken |
| **Holdout** in der Pareto-Sidebar | Baut eigenstaendige PB8-Backtest-Queue-Drafts aus unveraenderlichen Holdout-Daten | Kandidatenparameter, Coins, Exchange, Balance und Overrides |

#### Einstellungen Im Ueberblick

| Einstellung | Bedeutung |
| --- | --- |
| **Template** | Rolling-Vergleich, Walk-Forward-Validierung oder sequenzielle Sweep-Cashflow-Auswertung |
| **Window days** | Handelstage innerhalb eines Szenarios |
| **Stride days** | Abstand zwischen aufeinanderfolgenden Fensterenden; bei Sweep automatisch |
| **Training windows** | Bei Rolling/Walk-Forward editierbar; bei Sweep beim Generieren automatisch berechnet |
| **Holdout windows** | Unberuehrte Zeitraeume fuer abschliessende Out-of-Sample-Backtests |
| **Exchange mode** | Kombinierte Basis-Exchanges erben oder, sofern unterstuetzt, getrennte Exchange-Szenarien erzeugen |
| **Starting balance** | PB8-Simulationskapital und Sweep-Reset-Kapital nach Apply; verwendet immer die aktuelle allgemeine Starting balance; kein separates Generatorfeld |
| **Balance multiplier** | Sweep-Ziel: Starting balance multipliziert mit diesem Wert |
| **Refill cost** | Zusaetzliche externe Kosten beim Auffuellen eines Verlustfensters |
| **Cooldown days** | Handelsfreie Luecke zwischen Sweep-Fenstern; automatisch im Stride enthalten |

#### Empfohlener Sweep-Ablauf

1. Explizite Coins und Exchanges auswaehlen.
2. **All** fuer ein gemeinsames Startdatum aller Exchange/Coin-Paare verwenden; **1st** nur, wenn eine sich veraendernde Coin-Historie beabsichtigt ist.
3. **Sweep Cycles** waehlen und Window, Holdout, Multiplier, Refill cost sowie Cooldown setzen. PBGui berechnet Stride und Training windows.
4. Nach Datums- oder Exchange-Aenderungen **Generate windows** klicken, um neue Fenster zu berechnen und anzuzeigen.
5. **Check & Apply windows** klicken. PBGui synchronisiert Basisbalance, symmetrische Suite-Coin-Listen, Reducer, Scoring, Limits und sinnvolle Long-Bounds.
6. Optimize-Run speichern und queuen. `write_all_results=true` ist verpflichtend, damit PBGui den unveraenderlichen Sweep-Plan dem richtigen Result zuordnen kann.
7. Fertige Kandidaten nach `sweep_net_cashflow`, abgeschlossenen Zyklen, externem Kapital/Refills, Drawdown und Sortino bewerten.
8. Finalisten auswaehlen und **Holdout** klicken. Die erzeugten eigenstaendigen Backtests ohne Retuning queuen.

#### Wichtige Grenzen

- PBGui veraendert Passivbot nicht und bewegt kein echtes Geld.
- PB8 Gain ist ein End/Start-Multiplikator: `1.0` Break-even, `2.0` verdoppelt Kapital, `0.8` verliert 20%.
- Sweep-Entscheidungen erfolgen an Szenario-Fenstergrenzen, nicht bei einer unbekannten Zielueberschreitung innerhalb eines Fensters.
- Holdout-Daten beeinflussen weder Optimierung noch Pareto-Erzeugung.
- Manuelle Suite-Aenderungen nach Apply entfernen die Generator-Provenance, weil die gespeicherte Suite nicht mehr zum Preview-Experiment passt.

### Detaillierte Template-Einstellungen

1. Unter Backtest Settings die Basiswerte **exchanges**, **start_date** und **end_date** einstellen. Der Generator erzeugt seine Fenster rueckwaerts ab dem Basis-Enddatum und niemals vor dem Basis-Startdatum. Ein `end_date` mit dem Wert `now` wird fuer die Preview zum heutigen Datum aufgeloest.
2. **Suite Mode** oeffnen. Der Generator steht in PB8 Optimize auch bei noch deaktiviertem Suite Mode bereit.
3. Ein Template auswaehlen:
   - **Rolling Windows** erzeugt nur Trainingsfenster. Damit laesst sich das Verhalten ueber wiederholte historische Zeitraeume vergleichen.
   - **Walk-Forward** erzeugt chronologische Trainingsfenster mit anschliessenden getrennten Holdout-Fenstern.
   - **Sweep Cycles** erzeugt einen sequenziellen Combined-Exchange-Track und wertet die Fenster-Gains jedes Kandidaten mit Carry-, Sweep-Reset- und Refill-Reset-Regeln aus. PBGui berechnet Stride und die maximale Anzahl vollstaendiger Training-Fenster nach Reservierung der Holdouts automatisch aus dem Basis-Datumsbereich.
4. **Window days** bestimmt die Laenge jedes Szenarios. Rolling Windows und Walk-Forward erlauben einen manuellen **Stride days**-Wert. Sweep Cycles berechnet Stride automatisch als Window days plus Cooldown days.
5. Training-Anzahl fuer Rolling/Walk-Forward setzen. Sweep berechnet automatisch die passenden vollstaendigen Fenster nach Reservierung der Holdouts.
6. **Generate windows** klicken. Die erzeugten Fenster direkt im Chart pruefen und bearbeiten. Das Erzeugen der Fenster veraendert weder Suite noch Config.
7. Wenn der Plan stimmt, **Check & Apply windows** klicken. Dadurch wird Suite Mode aktiviert, die aktuelle ungespeicherte Suite ersetzt und der konfigurierte Reducer beibehalten. Holdout-Zeilen werden absichtlich nicht nach `backtest.scenarios` kopiert.
8. Nach dem Ersetzen einer bestehenden Suite die benannten Objective-Scenario-, Scoring- und Limit-Referenzen pruefen. Deren Szenarionamen muessen weiterhin in der neu erzeugten Trainings-Suite existieren.
9. Erst nach Kontrolle der angewendeten Suite den normalen **Save**- oder Queue-Workflow verwenden. Save speichert Generatorparameter und Holdout-Zeilen zur Nachvollziehbarkeit unter `pbgui.scenario_template`.

Wenn Basis-Daten oder Exchanges nach der Preview geaendert wurden, muss vor Apply erneut **Generate windows** ausgefuehrt werden. PBGui blockiert das Anwenden einer veralteten Preview. Manuelles Bearbeiten, Hinzufuegen, Entfernen, Verschieben oder Ersetzen von Suite-Szenarien nach Apply entfernt die Generator-Provenance, weil die gespeicherte Suite nicht mehr exakt dem erzeugten Plan entspricht.

**Generate windows** berechnet die Fenster aus den aktuellen Basisdaten und Generatoreingaben und zeigt sie direkt an. **Check & Apply windows** prueft und uebernimmt die Vorschau. Ein separater Neuberechnungsschritt entfaellt.

Beispiel: Fuer drei nicht ueberlappende Trainingsquartale und ein unberuehrtes Quartal **Walk-Forward** mit `Window days = 90`, `Stride days = 90`, `Training windows = 3` und `Holdout windows = 1` waehlen. Fuer sechs ueberlappende Dreimonats-Trainingsfenster im Monatsabstand **Rolling Windows** mit `Window days = 90`, `Stride days = 30` und `Training windows = 6` waehlen.

**Sweep-Cycles-Beispiel:** Wiederholte Kontowachstumszyklen von `1.000` auf `2.000` USD auswerten. **Sweep Cycles** auswaehlen und nur `Window days = 180`, `Cooldown days = 7` sowie `Holdout windows = 1` setzen. PBGui berechnet `Stride days = 187` und die maximale vollstaendige Training-Anzahl automatisch aus den Basis-Daten; unvollstaendige Resttage am Anfang werden angezeigt, statt manuelle Rechnungen zu verlangen. In den Haupteinstellungen **Starting balance = 1000**, **Balance multiplier = 2** und **Refill cost = 25** einstellen. Preview zeigt alle vollstaendigen 180-Tage-Trainingsfenster mit jeweils sieben handelsfreien Tagen dazwischen plus das reservierte unberuehrte Holdout-Fenster. PBGui verarbeitet die Fenster jedes Pareto-Kandidaten chronologisch. Positive Gains unterhalb von 2.000 USD werden ins naechste Fenster uebernommen. Ab 2.000 USD wird alles oberhalb von 1.000 USD als Sweep-Cashflow verbucht und das Arbeitskapital auf 1.000 USD zurueckgesetzt. Unter 1.000 USD verbucht PBGui die fehlende Differenz plus 25 USD externe Refill-Kosten und setzt ebenfalls auf 1.000 USD zurueck. Die Pareto-Spalten enthalten danach `sweep_net_cashflow`, `sweep_total_swept`, `sweep_external_capital`, `sweep_cycles_completed`, `sweep_refill_count`, `sweep_final_balance` und `sweep_target_hit_rate`. Der Holdout bleibt offen, bis der gewaehlte Kandidat getrennt ueber diesen Zeitraum ausgefuehrt wird. Dies ist eine deterministische Auswertung an Fenstergrenzen; sie bewegt kein echtes Geld und behauptet keine Zielueberschreitung innerhalb eines Fensters.

PB8-Gain-Werte sind Endmultiplikatoren und keine additiven Renditen: `1.0` ist Break-even, `2.0` verdoppelt die Startbalance und `0.8` bedeutet 20% Verlust. Die Sweep-Auswertung berechnet deshalb jedes Fenster als `ending_balance = opening_balance × gain_strategy_eq`.

Fuer die Validierung ohne manuelle Bearbeitung einen oder mehrere Kandidaten in der Pareto-Tabelle auswaehlen, **Holdout only**, **Full timerange only**, **Holdout + Full timerange** oder **Training + Holdout + Full timerange** waehlen und **Validate** klicken. Full timerange ist auch fuer normale PB8-Pareto-Results ohne Sweep-Plan verfuegbar. PBGui liest unveraenderliche Holdout-Daten, sofern vorhanden, erstellt pro Kandidat und Holdout einen eigenstaendigen PB8-Backtest-Eintrag und fuegt optional einen durchgaengigen Backtest vom originalen Basis-`start_date` bis `end_date` hinzu. Der All-period-Modus erstellt zusaetzlich fuer jedes konfigurierte Suite-Trainingsfenster einen eigenstaendigen Backtest, damit Training, Holdout und kontinuierlicher Full-Lauf direkt in Backtest Compare ausgewaehlt werden koennen. Combined Mode ohne Holdout-Daten queued weiterhin die verfuegbaren Training- und/oder Full-timerange-Jobs und meldet den uebersprungenen Holdout. Jeder erzeugte Validierungs-Draft deaktiviert Suite Mode, behaelt seinen eigenen exakten Datumsbereich sowie die konfigurierte Exchange-Gruppe und traegt eine kandidatenspezifische Validierungsgruppe bis in Backtest Results. Fertige Mitglieder dieser Gruppe bleiben dort gemeinsam hinter einem aufklappbaren **Optimize validation**-Kopf. Ein Multi-Exchange-Optimizer-Szenario bleibt dadurch pro Zeitraum ein vergleichbarer Combined-Backtest, statt in kuenstliche Einzel-Exchange-Jobs aufgeteilt zu werden, die in einem fruehen Fenster keine gueltigen Coins besitzen koennen. Der durchgaengige Lauf enthaelt Trainingsdaten und ist eine Diagnose fuer Pfadabhaengigkeit/Compounding, kein Ersatz fuer eine unberuehrte Out-of-Sample-Holdout-Validierung.

Beim Generieren oder Anwenden von Sweep-Fenstern wird immer die aktuelle allgemeine `backtest.starting_balance` gelesen. Nach einer spaeteren Aenderung muss vor Save oder Queue erneut generiert bzw. angewendet werden.

Apply ersetzt ausserdem das Optimizer-Rezept durch den Sweep-Preset: `gain_strategy_eq` max, `sortino_ratio_strategy_eq` max und `drawdown_worst_strategy_eq` min; alle erben Suite Aggregate. Der Suite-Reducer verwendet standardmaessig `median`, fuer den schlimmsten Drawdown `max` und fuer Backtest Completion Ratio `min`, damit ein unvollstaendiges Szenario nicht von den anderen verdeckt wird. Die Limits werden Drawdown groesser als `0.80` und Backtest Completion Ratio kleiner als `0.99`. Das 80%-Limit erlaubt bewusst High-Risk-Kandidaten fuer Profit Sweeping. Drawdown bleibt trotzdem ein minimierendes Pareto-Ziel, damit bei vergleichbarem Gain der risikoaermere Kandidat bevorzugt wird.

Bei einer expliziten Long-Coin-Auswahl setzt Apply zusaetzlich Long `n_positions` auf `1..Coin-Anzahl`; bei einem Coin wird daraus `1..1` und fixed. Long `total_wallet_exposure_limit` erhaelt den High-Risk-Sweep-Bereich `6..10`, der aktuelle Long-Bot-Wert wird auf `6` gesetzt. Die uebrigen Long-Bounds werden nach ihrer Wirkung normalisiert: echte Trailing-Martingale-, Filter-, Risk- und Unstuck-Bereiche mit Spannweite bleiben aktiv; Nullbreiten- und deaktivierte HSL-Bereiche werden fixed; Forager-Ranking-Gewichte werden bei nur einem Coin fixed, weil keine Rangfolge moeglich ist. Bei mehreren expliziten Long-Coins bleiben diese Gewichte aktiv. Short-Bounds und deren Fixed-Status bleiben unveraendert.

PB8 Suite Mode verlangt auch bei deaktivierter Seite identische Approved-Coin-Listen fuer Long und Short. Sweep Apply spiegelt deshalb die Long-Approved-Liste nach Short und entfernt diese Coins aus Short Ignored. Short-Trading wird dadurch nicht aktiviert: Solange Short-TWE `0` ist, bleibt Short deaktiviert. Der Preset speichert Fixed-Selektoren mit den tatsaechlichen `long.*`-Optimize-Bound-Keys und vermeidet dadurch nicht passende `bot.long.*`-Selektoren.

PB8.1-Scoring-Objectives koennen das globale **Objective Scenario** erben, ausdruecklich das Suite-Aggregat verwenden oder ein benanntes Suite-Szenario auswaehlen. Aggregate unterstuetzen `mean`, `min`, `max`, `std` und `median`. Limits koennen das Suite-Aggregat mit ausgelassenem Scenario verwenden, ein ausdrueckliches `scenario: null` erhalten oder ein benanntes Suite-Szenario auswaehlen; ausgelassen und explizit null haben dieselbe Laufzeitbasis, bleiben aber strukturell verschieden. PBGui liest das kanonische Reduktionsfeld aus der installierten PB8-Runtime: aktuelles PB8 verwendet `reducer`, aeltere kompatible PB8-Releases verwenden `aggregate` fuer Scoring und `stat` fuer Limits. Ein benanntes Szenario darf nicht gleichzeitig ein Reduktionsfeld verwenden. Szenarionamen muessen in der aktiven Suite vorhanden sein. PBGui erhaelt diese Unterschiede beim Synchronisieren von Visual Editor und Raw JSON. Die Spalte **Stat** in der Limits-Tabelle zeigt die gespeicherte Reduktion (zum Beispiel `max`) auch nach dem Verlassen der Inline-Bearbeitung an.

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

Bei einer laufenden CPU-/Pymoo-Optimierung liest das Dashboard die Evaluationsanzahl aus der dauerhaften `all_results.bin`, die vom verifizierten Queue-Prozess geoeffnet ist. Dadurch bleibt der Fortschritt aktuell, wenn PB8 wiederholte Kandidaten erst nach der Evaluation verwirft und deshalb keinen neuen Pareto-Update-Zaehler ausgibt. Ist das Schreiben aller Ergebnisse deaktiviert oder kann die Result-Datei nicht sicher zugeordnet werden, verwendet das Dashboard weiterhin den letzten strukturierten Evaluationswert aus dem Optimizer-Log.

Strategiespezifische Optimizer-Overrides werden beim Strategiewechsel entfernt und vor Save, Queue und Launch ueber die installierte PB8-Runtime validiert.

**OHLCV Readiness** und Preload laufen ueber PB8s eigenes Virtualenv, Planner, Cache-Pfade und den nativen Befehl `passivbot download`. Explizite Read-only-Quellen ausserhalb der freigegebenen PB8- oder PBGui-Market-Data-Roots werden abgelehnt, statt auf PB7 zurueckzufallen. GPU-Suites verlangen jedes szenariospezifische Exchange-Dataset statt der besten Exchange pro Coin; fehlt eine nur im Szenario benoetigte Exchange, wird die einzelne Preload-Aktion mit einer Erklaerung deaktiviert.

## Results Und Paretos

Ergebnisse werden nur aus `<pb8dir>/optimize_results` gelesen. Die Results-Tabelle zeigt fuer jeden Lauf die konfigurierte PB8-Strategie und kann nach dieser Spalte sortiert werden. Die Panels Results und Paretos bieten den gemeinsamen PB7-Workflow fuer Ergebnisansicht, Loeschen, 3D-Plots, Pareto Dash, Kandidaten-JSON, Metrik-Zusammenfassungen und Seed-Bundles.

Beim Oeffnen von Results waehrend eines kalten Metadaten-Scans wird ein eindeutiger Ladezustand angezeigt. Ein Hintergrund-Refresh laesst die zuletzt bestaetigten Zeilen sichtbar, und ein Panelwechsel waehrend des Requests verwirft die fertige Antwort nicht mehr.

Beim Oeffnen von Paretos speichert PBGui nur die versionierte Result-Verzeichnis-ID im Session-State des Tabs. Ein Reload auf `#paretos` oder die Rueckkehr ueber die Sidebar wartet zuerst auf die aktuelle Results-Liste, validiert die ID erneut und laedt danach die Kandidaten genau einmal. Fruehe Navigation loescht keine noch gueltige Auswahl. Fehlende oder geloeschte Results entfernen die gespeicherte Auswahl; absolute Result-Pfade werden nicht gespeichert.

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

Loeschen akzeptiert nur vollstaendige Run-Verzeichnisse direkt unter dem verwalteten Result-Root. Verschachtelte Artefakte wie `run/pareto`, versteckte Staging-Verzeichnisse sowie Parent-/Root-Selektoren werden vor jedem Verschieben oder Entfernen abgelehnt, auch bei Batch-Anfragen. Verschachtelte Pfade bleiben zum Lesen von Kandidaten und zur Seed-Auswahl verfuegbar.

## Archive

PB8-Optimize-Configs und PB8-Backtest-Ergebnisse verwenden den bestehenden Archive-Workflow. Dateien werden unter ihrer `config_version` gespeichert, damit PB7- und PB8-Inhalte einander nicht ueberschreiben. Import, Export, Ansicht, Loeschen, Restore und Handoffs verwenden immer den Parser der archivierten Config-Version.

Wenn eine OHLCV-Startdatumssuche Stop nicht innerhalb von 10 Sekunden bestaetigt, gibt Optimize die Bedienelemente frei und meldet einen Timeout. Das Backend kann noch stoppen; ein verspaetetes Ergebnis wird nicht angewendet.

## Vast.ai Cloud-Ausführung

Im Editor **Execution → Vast.ai GPU** wählen und **Save & Queue** verwenden.
GPU-Typ und Grenzen unter **Queue → Settings → GPU requirements** speichern. PBGui wählt erst beim Queue-Start ein aktuelles passendes Angebot.
Das Konto unter **Queue → Settings → Cloud setup** einrichten und dort **Rent GPU & start
queue** bestätigen. Mehrere Cloud-Jobs teilen sich einen Worker und dessen
Kursdaten-Cache. Details unter [Vast.ai GPU-Queue](48_vast_gpu.md).

Der Pareto Explorer lässt sich auch für importierte Vast-Ergebnisse öffnen. Geprüfte finale Vast-Importe enthalten all_results.bin; regelmäßige Zwischensicherungen enthalten die aktuellen Pareto-Dateien. Die vollständige Auswertungshistorie ist nach der finalen Sicherung verfügbar.

Queue Backtest und Queue Validation fügen die Kandidaten direkt hinzu; Diagramm, Filter und Auswahl bleiben erhalten. Erst Open Queue wechselt die Seite. Der Zähler umfasst wartende und laufende Jobs. Daneben steht, ob Autostart aktiv ist und Jobs automatisch starten kann. Hinzufügen sendet keinen Startbefehl. Zeiträume, Börsen, Startkapital und Overrides werden aus der Konfiguration übernommen; Änderungen sind bei Backtests möglich. Während frühere Aufträge im Hintergrund übertragen werden, kannst du weitere Kandidaten auswählen und hinzufügen. Jeder Klick übernimmt seine Konfiguration und Validierungsart; die Übertragung erfolgt nacheinander und der Status zeigt wartende Aufträge. Bereits bestätigte Jobs werden bei Wiederholungen in dieser Seitensitzung übersprungen. Lass die Seite bis zum Ende des Hinzufügens geöffnet. Solange noch Aufträge ausstehen, öffnet Open Queue einen weiteren Tab, damit das Hinzufügen weiterlaufen kann. Bei einem teilweise fehlgeschlagenen PB8-Aufruf setzt ein erneuter Klick mit denselben Vorgangs-IDs fort.

Das ausdrückliche Anwenden von **Sweep Cycles** setzt für EMA und Trailing genau drei Scoring-Ziele: Gain (ADG bei Vast GPU), Sortino und maximaler Drawdown. Alle unterstützten Metriken bleiben für anschließende manuelle Änderungen auswählbar.

### Visueller Scenario Editor

Oeffne **Suite Mode → Visual windows**. Oben erscheinen lokale OHLCV-Archive als Tageskerzen oder Kurslinie. Exchange und Coin betreffen nur diesen Referenzchart. Fehlende Tage werden orange markiert, unvollstaendige Tage gezaehlt. Es startet kein Exchange-Download. Beim Reinzoomen wechselt die Kurslinie automatisch zu Kerzen, sobald genug Platz vorhanden ist. Als Referenz stehen nur konfigurierte Optimizer-Coins zur Auswahl. Leere Holdout-Bahnen bleiben als Drop-Ziel sichtbar; beim Ziehen bleiben die Bahnpositionen stabil. Die Datenaufloesung der Optimierung bleibt unveraendert.

- Ziehe die Fenstermitte zum Verschieben oder einen Rand zum Vergroessern/Verkleinern. Datumswerte rasten auf UTC-Tage ein.
- Training/Holdout auf freier Chartflaeche zeichnen, mit dem Papierkorb loeschen und Undo/Redo bearbeiten den Entwurf.
- Holdouts duerfen zwischen Trainingsperioden liegen. Trainingsfenster werden nach Abschluss der Bearbeitung automatisch um Holdouts gekuerzt oder geteilt. Training/Holdout-Ueberschneidungen blockieren Apply. Ueberlappende Trainingsfenster erscheinen in separaten Spuren. Sweep verlangt zusaetzlich chronologische, nicht ueberlappende Fenster mit dem eingestellten Cooldown.
- **Check & Apply windows** prueft die Daten und uebernimmt nur Training in die Suite. Scoring, Limits und Aggregation bleiben erhalten. **Save** oder **Save & Queue** speichert anschliessend die angewendete Konfiguration. Erlaubt sind bis zu 48 Trainings- und 16 Holdout-Fenster.

Rolling Windows und Windows + Holdouts bleiben Vorlagen fuer gleichmaessige Fenster. Preview erzeugt deren Entwurf neu; danach lassen sich Fenster einzeln anpassen. Bestehende Vorlagen bleiben kompatibel. Ein verteilter Holdout ist ein ausgeschlossener Zeitraum, aber nicht automatisch ein chronologischer Forward-Test: echtes Walk-Forward wuerde vor jedem Test separat nur mit davorliegenden Daten optimieren.

Lokale und von Vast importierte Resultate behalten die expliziten Holdout-Daten. **Validate** in Results und Pareto Explorer verwendet diese Zeitraeume auch bei verteilten Holdouts. Aggregationsaenderungen entfernen sie nicht. Passt ein Plan nicht mehr zur Trainingskonfiguration, muss er vor dem Start erneut angewendet werden.

Visuelle Fensterplaene benoetigen `optimize.write_all_results=true`, damit die lokalen Metadaten dem Resultat zugeordnet werden koennen.

Neue Fenster verwenden **Window days** und **Stride days**. Training folgt dem letzten Trainingsstart plus Schrittweite; der erste Holdout folgt den vorhandenen Fenstern. Passt das ganze Fenster nicht mehr, erweitere den Bereich oder zeichne es explizit. PBGui verkuerzt es nicht automatisch.

Als Referenz wird ein konfigurierter Coin anhand des lokalen Market-Mappings vorausgewaehlt. Gibt es keinen Treffer, waehle ihn explizit aus. Eine manuelle Referenzauswahl bleibt beim Neuzeichnen erhalten. Datenluecken erscheinen als schmaler orangefarbener Streifen. **Full range** zeigt den gesamten konfigurierten Zeitraum, ohne Fensterdaten zu veraendern.

Tageszusammenfassungen werden bis zu fuenf Minuten im API-Speicher zwischengespeichert und beim erneuten Laden wiederverwendet. Geaenderte Quelldateien verwerfen den Cache des betroffenen Tages sofort. Beim ersten Laden werden weiterhin die lokalen Minutenarchive gelesen; es startet kein Exchange-Download.

Ein ausgewaehltes Fenster mit dem Papierkorb oben loeschen oder seinen Balken auf den Papierkorb ziehen. Undo stellt es wieder her. Die Markierungen im Kurschart zeigen ueber alle Fenster hinweg direkte Anschluesse gruen, Luecken orange und Ueberlappungen rot. Der Tooltip nennt Datum und Tagesanzahl. Ueberlappende Trainingsfenster koennen beabsichtigt sein.

Duenn gezeichnete gruene Anschluesse liegen im Kurschart. Beim Ziehen zum Papierkorb folgt eine schwebende Beschriftung der Maus.

Das Mausrad zoomt im Chart um den Mauszeiger. Shift-Ziehen im Kursbereich verschiebt den sichtbaren Zeitraum; Doppelklick auf eine freie Chartstelle zeigt den gesamten Zeitraum. Die Fensterbalken verschieben und aendern weiterhin ihre Datumsgrenzen.

Neue Fenster direkt durch Ziehen auf freier Chartflaeche erstellen. Auf der Holdout-Bahn entstehen Holdouts, sonst Trainingsfenster. Ein einfacher Klick erstellt kein Fenster.

Browser-Refresh oeffnet die gespeicherte Config erneut, auch bei Aufruf aus der Queue. Ungespeicherte Aenderungen an einer bestehenden gespeicherten Config werden nicht wiederhergestellt; neue und kopierte Entwuerfe haben eine eigene temporaere Wiederherstellung im Tab. Home/Schliessen entfernt die Editor-Adresse.

Die vier Chart-Icons sind Undo, Redo, Papierkorb und Gesamtbereich, jeweils mit Tooltip. Ein Fenster auf die andere Training/Holdout-Bahn ziehen, um seine Rolle zu wechseln. Neue Fenster direkt zeichnen; separate Hinzufuegen- oder Kursbereich-Knoepfe entfallen.

Beim Ziehen ist nur das Fenster unter dem Mauszeiger sichtbar; die urspruengliche Kopie wird bis zum Loslassen ausgeblendet. Refresh stellt den Editor ohne kurzzeitige Config-Auswahl wieder her.

Das gezogene Fenster zeigt bereits vor dem Ablegen die Zielrolle Training/Holdout mit passender Beschriftung und Farbe.

Das Magnet-Icon schaltet Einrasten ein/aus (anfangs aktiv). Beim Verschieben oder Skalieren rastet eine Grenze innerhalb von acht Bildschirmpixeln an anderen Fenstergrenzen ein. Verschieben erhaelt die Dauer. Balken zeigen Start/Ende und Dauer; bei schmalen Fenstern zeigt der Tooltip den ganzen Text.

Check & Apply zeigt Prueffortschritt und Fehler direkt neben dem Knopf. Nach erfolgreicher Uebernahme wird die Szenarioliste unten aktualisiert; die Config wird dadurch noch nicht auf Disk gespeichert.

Holdouts schliessen ihre Datumsbereiche nach Bearbeiten oder Ablegen automatisch vom Training aus. Ueberlappende Trainingsfenster werden gekuerzt, geteilt oder entfernt. Undo stellt die gesamte vorherige Aenderung inklusive betroffenem Training wieder her. Das Backend weist verbleibende Training/Holdout-Ueberlappungen weiterhin zurueck.

Direkt angrenzende Fenster erhalten bei eindeutiger Zuordnung einen gemeinsamen Grenzgriff. Ziehen verschiebt linkes Ende und rechten Start gemeinsam, ohne die aeusseren Grenzen zu aendern. Beide Fenster bleiben mindestens einen Tag lang. Undo stellt beide Fenster wieder her.

An einer gemeinsamen Grenze aendert der linke Griff nur das linke Fensterende, der mittlere beide Fenster und der rechte nur den rechten Fensterstart. Zum Trennen einen seitlichen Griff wegziehen; der Magnet greift weiterhin innerhalb seiner normalen Distanz und kann abgeschaltet werden.

Seitliche Griffe sind kleine Markierungen am unteren Balkenrand. Ihre groesseren unsichtbaren Mausflaechen bleiben leicht greifbar, ohne die Datumsangaben zu verdecken.

Unter dem Chart bleiben nur **Check & Apply windows** und Pruefrueckmeldungen. Fenster direkt im Chart auswaehlen und bearbeiten.

Der Chart ist die Szenariovorschau. **Generate windows** erzeugt den grafischen Entwurf aus den Template-Einstellungen. **Check & Apply windows** prueft ihn und ersetzt direkt die Suite-Szenarioliste; eine separate Preview-Tabelle und ein zweiter Apply-Schritt entfallen. Bei fehlgeschlagener Pruefung bleiben bestehende Szenarien erhalten. Save speichert die Config.

Die kompakte Referenzzeile zeigt Exchange, Coin, Tage und Complete. Fehlende oder unvollstaendige Tage erscheinen nur, wenn vorhanden. Der Tooltip erklaert Quellaufloesung und Pruefumfang; dies betrifft nur den Referenzchart.

Vast-Uploads verwenden wiederaufnehmbare 2-MiB-Bloecke und stabile komprimierte Archive. Der Fortschrittsbalken zaehlt per Pruefsumme bestaetigte Bloecke; empfangene Bytes der laufenden Bloecke werden separat angezeigt. Nach Verbindungsabbruch werden nur unbestaetigte Bloecke erneut gesendet. Die Geschwindigkeit misst bestaetigte Bytes im aktuellen Versuch. Solange der Empfaenger Fortschritt meldet, darf ein Upload laenger als zehn Minuten dauern. 120 Sekunden ohne Empfaengerfortschritt loesen einen Retry aus; die Mietfrist begrenzt weiterhin den Transfer.

In den Optimizer Settings ein kompatibles Angebot auswaehlen und **Rent** klicken: Genau diese GPU wird sofort gemietet. Abrechnung und Mietfrist beginnen sofort; Queue-Jobs bleiben pausiert. Ist das Angebot nicht mehr verfuegbar oder teurer geworden, muss neu ausgewaehlt werden; es wird kein Ersatz automatisch gemietet. Die Reservierung bleibt bis zum ersten Job, **End rental** oder der Mietfrist bestehen. **Start queue** verwendet diese GPU; nach dem ersten Job gilt wieder die normale Leerlauf-Regel. Queue und Settings zeigen die aktive Miete und **End rental**. Bei einem laufenden Job verwendet das Beenden den bestehenden Stop-and-collect-Ablauf.

Bei Sweep Cycles stellt das grafische **Check & Apply windows** fuer EMA und Trailing die drei Scoring-Defaults wieder her: ADG (Vast) beziehungsweise Gain (lokal), Sortino und groesster Drawdown. Bearbeitete Strategieparameter und Limits bleiben erhalten. Apply gleicht ausserdem die Short-Coin-Liste an Long an (ohne Short-Trading einzuschalten) und uebernimmt das Sweep-Startkapital in den Backtest.

Save und Save & Queue zeigen den laufenden Vorgang und melden Pruef- oder API-Fehler sichtbar, ohne den Editor zu schliessen. Den gemeldeten Fehler beheben und erneut speichern; wiederholte Klicks waehrend des Speicherns erzeugen keine doppelten Anfragen.

Neue und kopierte Optimize-Entwuerfe werden nach Refresh im selben Browser-Tab wiederhergestellt. Das ist eine temporaere Wiederherstellung, keine gespeicherte Config und kein Queue-Eintrag. Schliessen des Editors entfernt sie. Configs mit Zugangsdatenfeldern werden nicht zur Wiederherstellung abgelegt.

Rent prueft das ausgewaehlte Angebot direkt anhand seiner Vertrags-ID; die allgemeine Marktsuche kann ein anderes repraesentatives Angebot anzeigen. Fehler stehen direkt bei Rent. Es wird keine Ersatz-GPU automatisch gemietet.

GPU-Zeile und Details zeigen die von Vast gemeldeten TFLOPS. Der Wert vergleicht Rechenleistung, nicht den gemessenen Optimizer-Durchsatz; CPU-Pruefungen und Speicher beeinflussen die Laufzeit ebenfalls. Fehlende Werte erscheinen als „TFLOPS unknown“.

Nur im alten Übertragungsweg ohne rsync wird der Cache in Seiten mit jeweils 1.024 Dateien geprüft; auch grosse Multi-Coin-Datensaetze benoetigen keine uebergrosse SSH-Antwort. Bis zu zwei 2-MiB-Bloecke werden gleichzeitig hochgeladen. Der Fortschritt fasst beide Verbindungen zusammen; nur per Pruefsumme bestaetigte Bloecke gelten als abgeschlossen. Bei endgueltigem Fehler wird die andere Verbindung beendet und abgewartet; bestaetigte Bloecke bleiben wiederverwendbar. Das bestehende Worker-Image unterstuetzt dieses Verfahren.

Die automatische CPU-Auswahl verwendet den kleineren Wert aus gemessener Container-Kapazitaet und gemieteter Zuteilung, abgerundet auf ganze Worker (mindestens einen). Meldet der Host 256 CPUs bei einer Miete von 21,3 Kernen, werden 21 Exact-Worker verwendet. Ungueltige Zuteilungsdaten verhindern den Start.

Bei manuell reservierten GPUs wird das Provider-Startlog alle 60 Sekunden abgerufen, auch vor dem Queue-Start. Vorbereitete/wartende Cloud-Jobs zeigen dieses Miet-Log, bis ihr eigenes Provider- oder Optimizer-Log vorliegt. Das Provider-Log beschreibt den Container-Start, nicht den Optimizer-Fortschritt.

Die Mietanzeige in der Queue bietet ebenfalls **Start queue**. Nach dem Start wartet eine reservierte GPU, bis ein Eingabepaket fertig vorbereitet ist; der Queue-Start überspringt die Vorbereitung nicht.

Wird die Eingabevorbereitung durch das Beenden des Prozesses unterbrochen, wird der Job bei der Erkennung als fehlgeschlagen markiert. Mit **Requeue** wird er erneut vorbereitet; eine vorhandene Miete kann weiterverwendet werden.

Die direkte rsync-Synchronisierung unterscheidet Dateivorbereitung, Cache-Vergleich und Installation. Während sie läuft, zeigt der Fortschritt logisch verglichene Bytes und bezeichnet sie nicht als Netzwerkverkehr. Der Abschlussstatus trennt auf der GPU wiederverwendete Daten von tatsächlich gesendeten Bytes. Der alte Übertragungsweg ohne rsync behält Cache-Prüfung, Archivvorbereitung und geprüfte Datenblöcke. Erneutes Öffnen zeigt die gespeicherte Phase und das verfügbare Start-/Optimizer-Log; solange noch kein Log existiert, erscheint ein zur Phase passender Wartegrund.


Uploadrate und Host-Netzwerkangabe werden beide in **Mbps** angezeigt; Datenmengen bleiben in **MB** (1 Byte = 8 Bit). Während der Übertragung erscheinen gemessene Rate, angegebene Downloadrate des Hosts, erreichter Prozentanteil und beide Restzeitschätzungen gleichzeitig. Die Restzeit laut Hostrate ist theoretisch: Auch eigener Upload, Netzwerkstrecke, SSH-Overhead und Wiederholungen begrenzen den Durchsatz. Ein niedriger Anteil beweist daher keine falsche Hostangabe. Bei Archivbau, Wiederverbindungen und Verifikation erscheinen keine Übertragungsschätzungen.

Die Knöpfe **− / +** neben **Budget / deadline** fordern Änderungen um jeweils 30 Minuten für die aktive Miete an. Das bisherige Budget bleibt erhalten; maximal sind 24 Stunden seit Mietannahme erlaubt. Für Einsammeln und Beenden müssen mindestens zehn Minuten verbleiben. Während **Awaiting worker confirmation** bleibt die bestätigte Deadline sichtbar. Wiederholungen verwenden dieselbe Anfrage und verlängern nicht nochmals um 30 Minuten. Bei alten Worker-Images mit unveränderlicher Abschaltzeit sind die Knöpfe deaktiviert. Neue Mieten ab PBGui v2.04.6 verwenden den veröffentlichten queue-v2-Worker mit diesem Protokoll. Bestehende Mieten behalten ihren ursprünglichen Worker und bleiben bedienbar; für die Deadline-Anpassung ist eine neue Miete nötig.

Im GPU-Log kann `chunks=2/2 candidates=1024/1024` mehrfach erscheinen: Im Suite-Modus wird derselbe Kandidatenblock für jedes Szenario separat geprüft. `eta=0` gilt nur für diesen Dispatch, nicht für die gesamte Optimierung. Exact-/Pareto-Ergebnisse erscheinen erst nach den CPU-Prüfungen der ausgewählten Kandidaten. Daher kann die erste Suite-Runde bereits GPU-Aktivität zeigen, obwohl noch keine exakten Ergebnisse vorliegen.

Die GPU-Laufzeit mit mehreren Coins muss nicht linear mit der Coin-Anzahl steigen. Für einen Hostvergleich sollten aufgewärmte Proxy-Profile mit denselben Coins, Szenarien und Kandidatenzahlen verwendet werden: `kernel_execution` trennt die GPU-Berechnung von Kompilierung und Datentransfer. Eine ausgelastete GPU allein belegt keine normale Hostleistung.

Nur der alte Übertragungsweg ohne rsync führt noch eine Cache-Prüfung aus. Sie zeigt die vom Worker bestätigte Anzahl geprüfter Dateien und einen eigenen Prozentwert vor Beginn der Datenübertragung. Der Zähler bleibt beim erneuten Öffnen des Logs erhalten und ist vom Uploadfortschritt getrennt.

Bei Queue-Bundles ohne öffentliche Markt-Snapshots lädt PBGui unmittelbar vor dem Remote-Start frische Binance-/Bybit-Marktmetadaten lokal und überträgt sie zum Worker. Diese beschreiben Instrumente, nicht zusätzliche OHLCV-Kerzen. Schlägt Abruf oder Installation fehl, startet der Optimizer nicht; veraltete Snapshots werden nicht ersatzweise verwendet.

Vast-Uploads synchronisieren einzelne Dateien direkt mit rsync, wenn es auf PBGui und dem Worker installiert ist. Die vorgelagerte Cache-Abfrage, erneutes Lesen des gesamten Datenbestands und das große Upload-Archiv entfallen. Unveränderliche Datendateien tragen ihren Inhaltshash als Namen. Rsync erkennt identische Daten anhand von Name und Größe auch zwischen Jobs mit unterschiedlichen lokalen Zeitstempeln. Geänderte Übertragungen werden durch rsync geprüft und atomar bereitgestellt; abgebrochene Dateien bleiben separat und werden beim Wiederholen weiterverwendet. Job-Konfiguration und Manifest werden separat übertragen. Fertige Daten werden ohne erneutes vollständiges Lesen oder Kopieren in die Job-Eingabe verlinkt. Die anfängliche Gesamtgröße enthält wiederverwendbare Dateien; Restzeitschätzungen können deshalb zu hoch ausfallen. Nach der Synchronisation verwendet die Transferkostenschätzung die von rsync gemeldeten gesendeten Bytes statt der Größe wiederverwendeter Daten; maßgeblich bleibt die Abrechnung des Anbieters. Ein von PBGui über SSH ausgeführter Helfer unterstützt das veröffentlichte queue-v3-Image, ohne es ersetzen zu müssen. Ältere Worker ohne rsync behalten die Übertragung geprüfter Datenblöcke. Auf bestehenden PBGui-Rechnern lässt sich rsync über die Paketverwaltung installieren; neue Installer enthalten es. Die beworbene Host-Geschwindigkeit bleibt durch den eigenen Upload und die Netzwerkstrecke begrenzt.

Während der Image-Vorbereitung zeigt der animierte Balken einen Vorgang mit unbekanntem Restumfang, keinen gemessenen Byte-Fortschritt. Angezeigt werden die bisherige Dauer, abgeschlossene Layer und die Host-Logquelle (Vast Extra Debug Logs). Fetched bezeichnet den Abrufzeitpunkt des Log-Snapshots durch PBGui, nicht die Entstehung seiner letzten Zeile. Vast Instance Logs können No such container melden, solange das Image noch geladen und der Container noch nicht erstellt wurde.

Requeue zeigt sofort Preparing und sperrt weitere Klicks, während das lokale Eingabepaket neu erstellt wird. Der Ersatzjob wird eingereiht, ohne eine GPU zu mieten.

Vor dem Optimizer-Start beschafft PBGui außerdem die erste Tageskerze für jeden exportierten Coin (einschließlich BTC) auf jeder ausgewählten Börse. Übertragen wird der vollständige PB8-Ersthandelszeitpunkt-Cache mit börsenspezifischen Zeitstempeln, aufgelösten Symbolen und Resolver-Version. Damit muss ein regional gesperrter Worker diese Daten nicht selbst nachladen. Das konfigurierte Mindestalter bleibt erhalten; fehlende oder inkompatible Metadaten stoppen den Start mit einer Fehlermeldung. Dies unterstützt auch ältere Queue-Bundles und benötigt kein neues Worker-Image.

## Optimizer Settings

**Queue Settings** steuert lokalen Autostart, CPU-Overrides und PBGui-Marktdaten. Vast.ai besitzt fünf getrennte Sidebar-Bereiche: GPU & Offers, Rental & Automation, Hosts, Performance History und Account. Siehe [Vast.ai-Anleitung](48_vast_gpu.md#optimizer-settings).

Die Queue-Spalte **Est. coin candles / candidate** schätzt die Datenmenge einer vollständigen Kandidatenbewertung aus der eingefrorenen Config schon vor dem Start. Warm-up und Datenverfügbarkeit sind nicht enthalten. Berechnung und History-Vergleich beschreibt [Vast.ai workload comparison](48_vast_gpu.md#config-workload-comparison).

Datumsfelder im Optimizer überschreiben beim Tippen innerhalb eines vollständigen YYYY-MM-DD-Werts die vorhandenen Ziffern. Markierter Text und eingefügte Datumswerte lassen sich weiterhin normal ersetzen. Beim Übernehmen grafischer Fenster werden automatisch erzeugte Szenarionamen an die aktuelle Training-/Holdout-Rolle und die Daten angepasst. Nur Trainingsfenster erscheinen in den Optimizer-Szenarien; beide verteilten Holdouts bleiben im Validierungsplan erhalten.

Parameterhilfe erscheint beim Überfahren des gepunktet markierten Feldtitels. Klicken, Fokussieren oder Bearbeiten eines Eingabefelds öffnet keine Parameterhilfe.

Nach Änderungen an den visuellen Fenstern vor Save oder Save & Queue zuerst **Check & Apply windows** verwenden. PBGui weist nicht übernommene Fensterdaten/-rollen und aktive Fenster außerhalb der aktuellen Basisdaten zurück. Die Aufwandsschätzung der Queue beschreibt die übernommenen Trainingsszenarien, nicht den Generatorentwurf; Holdouts zählen nicht mit.

Gruene Linien markieren auch den Beginn des ersten und das Ende des letzten Fensters.

Bei Sweep Cycles uebernimmt **Generate windows** auch die aktuelle allgemeine **Starting balance** in den Generator und die erzeugte Vorschau.

Der festgelegte Vast-GPU-Worker unterstuetzt HSL fuer ema_anchor und trailing_martingale. HSL-Einstellungen bleiben beim Cloud-Export erhalten; die nativen GPU-Konfigurationspruefungen gelten weiterhin.

Mit **Max concurrent GPUs** und **Auto rent & start** unter **Rental & Automation** laufen vorbereitete Queue-Jobs parallel auf getrennten Vast-GPUs (Standard 1). Nach der ausdrücklichen Bestätigung beim Speichern reicht **Save & Queue**; pro Job ist kein Start nötig. Budget und Deadline gelten pro Miete. Verhalten bei Pause, Bereinigung und Ersatzmieten steht im [GPU-Pool-Guide](48_vast_gpu.md#gpu-pool-und-gemeinsame-mieten).
