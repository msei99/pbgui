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

Die zwei kompakten Buttons direkt neben **start_date** in PB8 Optimize ermitteln mit PB8 die ersten verfuegbaren Kerzen fuer die aktuell ausgewaehlten Exchanges und explizit freigegebenen Coins. **1st** verwendet die aelteste bekannte ausgewaehlte Markthistorie. **All** startet erst, wenn jeder ausgewaehlte Coin auf jeder ausgewaehlten Exchange einen bekannten OHLCV-Zeitstempel besitzt. Waehrend des Lookups zeigt ein kompakter Fortschrittsbalken die tatsaechlich abgeschlossenen Exchange/Coin-Paare und den aktuellen PB8-Schritt. **Stop** beendet nur diesen Lookup. PBGui addiert PB8s benoetigten Strategie-Warmup und rundet auf den ersten vollstaendig nutzbaren UTC-Tag auf, bevor das reine Datum `backtest.start_date` gesetzt wird. Fehlt ein Coin auf einer Exchange oder ist sein erster Zeitstempel unbekannt, meldet **All** das erste nicht aufloesbare Paar. Die dynamische Coin-Auswahl `all` ist nicht erlaubt; ein Lookup ist auf 200 Exchange/Coin-Paare begrenzt. Der ausdrueckliche Lookup darf PB8s nativen First-Timestamp-Cache fuellen, laedt aber nicht den vollstaendigen OHLCV-Bereich herunter. Beim Schliessen oder Ersetzen des Editors wird sein aktiver Lookup automatisch gestoppt.

Der **PB8 Scenario Generator** in Suite Mode zeigt deterministische Plaene fuer `rolling_windows`, `walk_forward` und `sweep_cycles` aus dem Basis-Datumsbereich des Editors. Fensterlaenge, Schrittweite, Anzahl der Trainings- und optionalen Holdout-Fenster sowie Exchange-Aufteilung werden serverseitig validiert und auf 64 erzeugte Szenarien begrenzt. Preview veraendert die Config nicht. **Apply Training Scenarios** ersetzt ausdruecklich die ungespeicherten Suite-Szenarien und den Reducer; Holdout-Fenster bleiben ausserhalb von `backtest.scenarios` und werden als `pbgui.scenario_template`-Provenance gespeichert. Jede spaetere manuelle Suite-Aenderung entfernt diese Provenance. Sweep Cycles bindet diesen unveraenderlichen Plan zusaetzlich an das PB8-Result und berechnet aus dem szenarioweisen Gain jedes Pareto-Kandidaten sequenzielle Sweep-/Refill-Cashflow-Metriken. PBGui AI bietet denselben Generator als Read-only-Preview-Tool an und muss fuer Save oder Queue weiterhin den bestehenden Proposal-Flow verwenden.

### Scenario Generator

Der Scenario Generator macht aus einer PB8-Optimize-Config eine reproduzierbare Gruppe historischer Tests. PB8 fuehrt weiterhin eine normale Suite-Optimierung aus. PBGui erzeugt die Datumsfenster, speichert den Experimentplan, berechnet nach PB8s Szenario-Metriken die Sweep-Cashflows und bereitet abschliessend die Holdout-Backtests vor.

#### Was Die Aktionen Tun

| Aktion | Was sich aendert | Was unveraendert bleibt |
| --- | --- | --- |
| **1st / All** neben `start_date` | Ermittelt ein OHLCV-basiertes Startdatum | Suite-Szenarien und Generator-Einstellungen |
| **Recalculate** | Liest aktuelle Daten/Exchanges neu und berechnet automatische Sweep-Werte | Gespeicherte Config und bereits angewendete Suite |
| **Preview** | Zeigt exakte Train-/Holdout-Fenster und Warnungen | Config, Suite, Scoring, Bounds und Queue |
| **Apply Training Scenarios** | Aktiviert Suite Mode, setzt Train-Szenarien/Reducer, speichert Holdout-Provenance und wendet den Sweep-Preset an | Es wird noch nichts gespeichert oder gequeued |
| **Save / Save & Queue** | Speichert oder startet das angewendete Experiment | Holdout bleibt aus der Optimierung ausgeschlossen |
| **Paretos** | Zeigt PB8-Metriken plus PBGui-`sweep_*`-Cashflow-Metriken | Originale PB8-Kandidatenmetriken |
| **Holdout** in der Pareto-Sidebar | Baut eigenstaendige PB8-Backtest-Queue-Drafts aus unveraenderlichen Holdout-Daten | Kandidatenparameter, Coins, Exchange, Balance und Overrides |

#### Einstellungen Im Ueberblick

| Einstellung | Bedeutung |
| --- | --- |
| **Template** | Rolling-Vergleich, Walk-Forward-Validierung oder sequenzielle Sweep-Cashflow-Auswertung |
| **Window days** | Handelstage innerhalb eines Szenarios |
| **Stride days** | Abstand zwischen aufeinanderfolgenden Fensterenden; bei Sweep automatisch |
| **Training windows** | Szenarien, die PB8 waehrend der Optimierung auswertet; bei Sweep automatisch |
| **Holdout windows** | Unberuehrte Zeitraeume fuer abschliessende Out-of-Sample-Backtests |
| **Exchange mode** | Kombinierte Basis-Exchanges erben oder, sofern unterstuetzt, getrennte Exchange-Szenarien erzeugen |
| **Starting balance** | PB8-Simulationskapital und Sweep-Reset-Kapital nach Apply |
| **Balance multiplier** | Sweep-Ziel: Starting balance multipliziert mit diesem Wert |
| **Refill cost** | Zusaetzliche externe Kosten beim Auffuellen eines Verlustfensters |
| **Cooldown days** | Handelsfreie Luecke zwischen Sweep-Fenstern; automatisch im Stride enthalten |

#### Empfohlener Sweep-Ablauf

1. Explizite Coins und Exchanges auswaehlen.
2. **All** fuer ein gemeinsames Startdatum aller Exchange/Coin-Paare verwenden; **1st** nur, wenn eine sich veraendernde Coin-Historie beabsichtigt ist.
3. **Sweep Cycles** waehlen und Window, Holdout, Starting balance, Multiplier, Refill cost sowie Cooldown setzen. PBGui berechnet Stride und Training windows.
4. Nach jeder OHLCV-/Datums-/Exchange-Aenderung **Recalculate**, danach **Preview** klicken.
5. **Apply Training Scenarios** klicken. PBGui synchronisiert Basisbalance, symmetrische Suite-Coin-Listen, Reducer, Scoring, Limits und sinnvolle Long-Bounds.
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
5. **Training windows** bei Rolling Windows oder Walk-Forward manuell setzen. Sweep Cycles berechnet nach Reservierung der gewaehlten **Holdout windows** automatisch die maximale vollstaendige Training-Anzahl zwischen `start_date` und `end_date`. Mit **Exchange mode = Inherit base** verwendet jedes Fenster die kombinierte Basis-Exchange-Auswahl.
6. **Preview** klicken. Labels, exakte Datumsbereiche, Train-/Holdout-Zuordnung, Szenarioanzahl und Warnungen pruefen. Preview allein veraendert weder Suite noch Config.
7. Wenn der Plan stimmt, **Apply Training Scenarios** klicken. Dadurch wird Suite Mode aktiviert, die aktuelle ungespeicherte Suite ersetzt und der vorgeschlagene Reducer angewendet. Holdout-Zeilen werden absichtlich nicht nach `backtest.scenarios` kopiert.
8. Nach dem Ersetzen einer bestehenden Suite die benannten Objective-Scenario-, Scoring- und Limit-Referenzen pruefen. Deren Szenarionamen muessen weiterhin in der neu erzeugten Trainings-Suite existieren.
9. Erst nach Kontrolle der angewendeten Suite den normalen **Save**- oder Queue-Workflow verwenden. Save speichert Generatorparameter und Holdout-Zeilen zur Nachvollziehbarkeit unter `pbgui.scenario_template`.

Wenn Basis-Daten oder Exchanges nach der Preview geaendert wurden, muss vor Apply erneut **Preview** ausgefuehrt werden. PBGui blockiert das Anwenden einer veralteten Preview. Manuelles Bearbeiten, Hinzufuegen, Entfernen, Verschieben oder Ersetzen von Suite-Szenarien nach Apply entfernt die Generator-Provenance, weil die gespeicherte Suite nicht mehr exakt dem erzeugten Plan entspricht.

Nach einer Aenderung der Approved Coins und einem neuen `start_date` ueber **1st** oder **All** muss **Recalculate** neben **Guide** geklickt werden. Die Aktion liest die aktuellen Basis-Daten und Exchanges neu ein, berechnet Sweep Stride und Training windows automatisch und verwirft eine veraltete Preview, bevor eine neue angewendet werden kann.

Beispiel: Fuer drei nicht ueberlappende Trainingsquartale und ein unberuehrtes Quartal **Walk-Forward** mit `Window days = 90`, `Stride days = 90`, `Training windows = 3` und `Holdout windows = 1` waehlen. Fuer sechs ueberlappende Dreimonats-Trainingsfenster im Monatsabstand **Rolling Windows** mit `Window days = 90`, `Stride days = 30` und `Training windows = 6` waehlen.

**Sweep-Cycles-Beispiel:** Wiederholte Kontowachstumszyklen von `1.000` auf `2.000` USD auswerten. **Sweep Cycles** auswaehlen und nur `Window days = 180`, `Cooldown days = 7` sowie `Holdout windows = 1` setzen. PBGui berechnet `Stride days = 187` und die maximale vollstaendige Training-Anzahl automatisch aus den Basis-Daten; unvollstaendige Resttage am Anfang werden angezeigt, statt manuelle Rechnungen zu verlangen. **Starting balance = 1000**, **Balance multiplier = 2** und **Refill cost = 25** einstellen. Preview zeigt alle vollstaendigen 180-Tage-Trainingsfenster mit jeweils sieben handelsfreien Tagen dazwischen plus das reservierte unberuehrte Holdout-Fenster. PBGui verarbeitet die Fenster jedes Pareto-Kandidaten chronologisch. Positive Gains unterhalb von 2.000 USD werden ins naechste Fenster uebernommen. Ab 2.000 USD wird alles oberhalb von 1.000 USD als Sweep-Cashflow verbucht und das Arbeitskapital auf 1.000 USD zurueckgesetzt. Unter 1.000 USD verbucht PBGui die fehlende Differenz plus 25 USD externe Refill-Kosten und setzt ebenfalls auf 1.000 USD zurueck. Die Pareto-Spalten enthalten danach `sweep_net_cashflow`, `sweep_total_swept`, `sweep_external_capital`, `sweep_cycles_completed`, `sweep_refill_count`, `sweep_final_balance` und `sweep_target_hit_rate`. Der Holdout bleibt offen, bis der gewaehlte Kandidat getrennt ueber diesen Zeitraum ausgefuehrt wird. Dies ist eine deterministische Auswertung an Fenstergrenzen; sie bewegt kein echtes Geld und behauptet keine Zielueberschreitung innerhalb eines Fensters.

PB8-Gain-Werte sind Endmultiplikatoren und keine additiven Renditen: `1.0` ist Break-even, `2.0` verdoppelt die Startbalance und `0.8` bedeutet 20% Verlust. Die Sweep-Auswertung berechnet deshalb jedes Fenster als `ending_balance = opening_balance × gain_strategy_eq`.

Fuer die Validierung ohne manuelle Bearbeitung einen oder mehrere Kandidaten in der Pareto-Tabelle auswaehlen und in der Sidebar **Holdout** klicken. PBGui liest die unveraenderlichen Holdout-Daten aus dem Result-Sidecar, erstellt pro Kandidat und Holdout einen eigenstaendigen PB8-Backtest-Eintrag, deaktiviert Suite Mode in diesen Drafts, behaelt Kandidateneinstellungen, Coins, Exchanges, Balance und Overrides bei und oeffnet den fertigen Backtest-Queue-Draft.

Beim Anwenden einer Sweep-Cycles-Preview setzt PBGui auch das obere PB8-Feld `backtest.starting_balance` auf die **Starting balance** des Generators. Save und Queue lehnen eine spaetere Abweichung ab, weil PB8 die Gains mit derselben Kapitalgroesse berechnen muss, die das Cashflow-Modell verwendet.

Apply ersetzt ausserdem das Optimizer-Rezept durch den Sweep-Preset: `gain_strategy_eq` max, `sortino_ratio_strategy_eq` max und `drawdown_worst_strategy_eq` min; alle erben Suite Aggregate. Der Suite-Reducer verwendet standardmaessig `median`, fuer den schlimmsten Drawdown `max` und fuer Backtest Completion Ratio `min`, damit ein unvollstaendiges Szenario nicht von den anderen verdeckt wird. Die Limits werden Drawdown groesser als `0.80` und Backtest Completion Ratio kleiner als `0.99`. Das 80%-Limit erlaubt bewusst High-Risk-Kandidaten fuer Profit Sweeping. Drawdown bleibt trotzdem ein minimierendes Pareto-Ziel, damit bei vergleichbarem Gain der risikoaermere Kandidat bevorzugt wird.

Bei einer expliziten Long-Coin-Auswahl setzt Apply zusaetzlich Long `n_positions` auf `1..Coin-Anzahl`; bei einem Coin wird daraus `1..1` und fixed. Long `total_wallet_exposure_limit` erhaelt den High-Risk-Sweep-Bereich `6..10`, der aktuelle Long-Bot-Wert wird auf `6` gesetzt. Die uebrigen Long-Bounds werden nach ihrer Wirkung normalisiert: echte Trailing-Martingale-, Filter-, Risk- und Unstuck-Bereiche mit Spannweite bleiben aktiv; Nullbreiten- und deaktivierte HSL-Bereiche werden fixed; Forager-Ranking-Gewichte werden bei nur einem Coin fixed, weil keine Rangfolge moeglich ist. Bei mehreren expliziten Long-Coins bleiben diese Gewichte aktiv. Short-Bounds und deren Fixed-Status bleiben unveraendert.

PB8 Suite Mode verlangt auch bei deaktivierter Seite identische Approved-Coin-Listen fuer Long und Short. Sweep Apply spiegelt deshalb die Long-Approved-Liste nach Short und entfernt diese Coins aus Short Ignored. Short-Trading wird dadurch nicht aktiviert: Solange Short-TWE `0` ist, bleibt Short deaktiviert. Der Preset speichert Fixed-Selektoren mit den tatsaechlichen `long.*`-Optimize-Bound-Keys und vermeidet dadurch nicht passende `bot.long.*`-Selektoren.

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

Bei einer laufenden CPU-/Pymoo-Optimierung liest das Dashboard die Evaluationsanzahl aus der dauerhaften `all_results.bin`, die vom verifizierten Queue-Prozess geoeffnet ist. Dadurch bleibt der Fortschritt aktuell, wenn PB8 wiederholte Kandidaten erst nach der Evaluation verwirft und deshalb keinen neuen Pareto-Update-Zaehler ausgibt. Ist das Schreiben aller Ergebnisse deaktiviert oder kann die Result-Datei nicht sicher zugeordnet werden, verwendet das Dashboard weiterhin den letzten strukturierten Evaluationswert aus dem Optimizer-Log.

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
