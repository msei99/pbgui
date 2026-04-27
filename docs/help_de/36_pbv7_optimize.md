# PBv7 Optimierung

Die **PBv7 Optimize** Seite wird jetzt als eigenständige **FastAPI + Vanilla JS** Seite geöffnet.
Damit lassen sich Passivbot-v7-Optimierungen erstellen, einreihen und auswerten, ohne die alte Streamlit-Worker-Logik zu verwenden.
Die obere **PBv7**-Navigation wechselt jetzt auch zwischen den FastAPI-Seiten **Run**, **Backtest** und **Optimize** direkt, statt für Optimize noch einmal über Streamlit zu gehen.
Die Seite ist in vier Sidebar-Bereiche aufgeteilt:

| Bereich | Zweck |
|--------|-------|
| **Configs** | Gespeicherte Optimize-Configs suchen, mehrfach auswählen, bearbeiten, duplizieren, löschen und queueen |
| **Queue** | Eingereihte und laufende Jobs überwachen, Queue-Settings öffnen und Logs prüfen |
| **Results** | Abgeschlossene Optimize-Ergebnisse durchsuchen |
| **Paretos** | Die Pareto-Dateien eines ausgewählten Ergebnis-Sets prüfen |

---

## Configs-Bereich

Zeigt alle gespeicherten Optimierungskonfigurationen aus `data/opt_v7/`.

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| **New Config** | Neue Config im strukturierten Editor öffnen |
| **Edit Selected** | Genau eine ausgewählte Config im Editor öffnen |
| **Duplicate** | Die ausgewählte Config unter neuem Namen kopieren |
| **Queue Selected** | Alle ausgewählten Configs zur Queue hinzufügen |
| **Delete Selected** | Alle ausgewählten Configs löschen |

Wenn du eine bestehende Optimize-Config öffnest und vor dem Speichern `config_name` änderst, speichert PBGui diesen Editorzustand jetzt als neue Config-Datei unter dem neuen Namen. Die ursprünglich geöffnete Config bleibt unverändert; das ist also ein Save-as-new und kein Rename der alten Datei.

Mit dem Suchfeld lässt sich nach Config-Namen filtern. Die Config-Zeilen unterstützen jetzt dieselbe Click-and-Drag-Mehrfachauswahl wie Backtest, und rechts werden kompakte Icon-Aktionen statt Textbuttons verwendet.

### Strukturierter Editor

Beim Erstellen oder Bearbeiten ersetzt ein eigener Editor die Config-Liste und blendet eine Editor-Sidebar ein, analog zur Backtest-Seite.
Der Editor verwendet dasselbe responsive 8-Spalten-Feldraster, denselben Popup-Datumsauswähler und dieselben mausgesteuerten Multiselect-Widgets wie die Backtest- und Run-Editoren.
Über gepunktet unterstrichene Feldnamen lässt sich die jeweilige Inline-Hilfe per Hover einblenden.

| Schaltfläche | Aktion |
|--------|--------|
| **Home** | Zurück zur Config-Liste |
| **Save** | Config nach `data/opt_v7/{name}.json` speichern |
| **Save and Queue** | Config speichern und direkt einreihen |
| **🧭 OHLCV Readiness** | Öffnet ein verschiebbares und resizebares Floating-Fenster und führt darin einen PB7-basierten Read-only-Preflight für die aktuelle Optimize-Config aus. Angezeigt wird, ob der aktuelle Approved-Coin-Satz lokal bereit ist, aus Legacy-OHLCV-Daten importiert werden kann, erst beim Start nachgeladen würde oder durch persistente Gaps blockiert ist; ausgewertet wird die Vereinigung aus `approved_coins_long` und `approved_coins_short`, und jeder Eintrag zeigt jetzt direkt, ob er aus `long`, `short` oder aus beiden Listen stammt. Wenn PB7 sonst fehlende Bereiche beim Start holen würde, bietet das Fenster zusätzlich **Preload OHLCV Data** zum Vorwärmen des Caches im Hintergrund an, springt beim Start dieses Preloads automatisch zum Job-Logbereich, zeigt den laufenden Download jetzt mit echten, aus den PB7-Archiv-/CCXT-Logzeilen abgeleiteten Fortschrittsbalken plus Laufzeit, PID, Log-Zählern und Zeitstempel der letzten Aktualisierung an, leitet den CCXT-Fortschritt dabei aus dem vorwärts laufenden Request-Cursor ab statt bei Börsen mit neueren Rückgabedaten ständig auf 100% zu springen, verwendet für den Preload jetzt denselben warmup-korrigierten effektiven Start wie der Readiness-Check, damit nach einem fertigen Preload nicht mehr genau diese Warmup-Tage fehlen, stuft Märkte, die erst nach dem angeforderten Fenster gestartet sind, jetzt korrekt als zu jung ein statt so zu tun, als ließen sich diese alten Kerzen noch preloaden, blendet während des laufenden Downloads zusätzlich eine **Stop Preload**-Aktion ein, bietet oben rechts außerdem einen Button zum Aufziehen des Fensters auf die Browserfläche, hält dieses Log sauber am Live-Tail statt wieder nach oben zu springen und lässt das fertige Preload-Ergebnis sichtbar, bis ein frischer Readiness-Check es ersetzt |

Der Bereich **Raw Config JSON** verhält sich jetzt wie in Backtest und Run: Änderungen im Raw-JSON werden automatisch in die strukturierten Felder übernommen, strukturierte Änderungen bauen das Raw-JSON automatisch neu auf, und ungültiges JSON wird live markiert und zeilenbasiert hervorgehoben.

Ältere Optimize-Dateien, die nur minimale `backtest`- und `optimize`-Blöcke speichern, lassen sich jetzt ebenfalls wieder im Editor öffnen. PBGui ergänzt die fehlenden Basis-Abschnitte aus dem aktuellen Optimize-Template, bevor die Config an die PB7-Prepare-Pipeline übergeben wird, sodass Legacy-Queue-Kandidaten nicht mehr allein wegen eines abgespeckten Stubs mit einem Formatfehler scheitern.

`btc_collateral_ltv_cap` verhält sich jetzt ebenfalls wie im Backtest: das Formular zeigt `0`, wenn PB7 intern `null` speichert, und beim Speichern wird `0` wieder auf den zugrunde liegenden Unlimited-Debt-Wert `null` zurückgesetzt.

Hauptbereiche im Editor:

| Bereich | Beschreibung |
|---------|--------------|
| **Kopfzeile** | Name, Börsen und Datumsbereich der Optimize-Config |
| **Market & Universe** | Starting Balance, Candle-Intervall, OHLCV-Quelle mit `PBGui Data`-Quickfill-Knopf, BTC-Collateral-Caps, `hsl_signal_mode`, Marktfilter, Approved/Ignored Coins und `coin_sources` |
| **Optimization** | Gemeinsamer Suite-Editor plus `Scoring`, `Limits`, `Bounds & Overrides` und die backend-spezifischen Optimizer-Controls |
| **Run Settings** | Starting Seeds, Iterations, CPUs, Pareto-Aufbewahrung, Logging, Memory Snapshots, Output-Throttles, Rundung signifikanter Stellen und Ergebnis-Schalter |
| **Additional Parameters** | Immer sichtbarer Expander für unbekannte `optimize.*`-Einstellungen; wenn es keine Extras gibt, erscheint dort ein Leer-Hinweis, während kanonische Backend-Felder wie `optimize.backend`, `optimize.pymoo.*`, `compress_results_file` und `write_all_results` in ihren eigenen Abschnitten bleiben |
| **Raw Config JSON** | Vollständiges Basis-JSON der Config; dient beim Speichern als Grundlage, damit unberührte Abschnitte erhalten bleiben, inklusive automatischer Zwei-Wege-Synchronisierung und Live-Validierung |

Der Editor zeigt jetzt bewusst nur noch drei sichtbare Hauptüberschriften: **Market & Universe**, **Optimization** und **Run Settings**. Ziel ist weniger Label-Rauschen bei weiterhin klarer Reihenfolge: erst Datenbasis und Coin-Universum, dann der Suchraum des Optimizers, danach die Laufzeit-Einstellungen. Das entspricht zugleich den technischen Abhängigkeiten der Seite: Pymoo `auto` bestimmt den effektiven Algorithmus aus der aktuellen Anzahl der Scoring-Ziele, und Pymoo Mutation `auto` leitet sich aus der Anzahl aktiver Bounds ab.

Die Eingabe `n_cpus` in **Run Settings** ist jetzt auf die CPU-Anzahl des Hosts begrenzt, auf dem PBGui/Optimize läuft. Der strukturierte Editor kann damit keine höhere Worker-Zahl mehr anfordern, als die Maschine tatsächlich hat.

In der ersten Kopfzeile steht `config_name` jetzt vor `exchanges`, weil das Benennen der Optimize-Config in der Praxis meist die erste sinnvolle Eingabe ist, bevor Märkte und Testzeitraum gewählt werden.

Das Feld `end_date` erhält jetzt auch den literalen Wert `now`, wenn eine Config die gleitende „bis heute“-Semantik nutzt, statt ihn schon durch bloßes Öffnen und Speichern des Editors in das feste Tagesdatum umzuwandeln.

Wenn **Starting Seeds** auf `self` steht, zeigt der Bereich die Seed-Config direkt mit Schnellzugriff auf `total_wallet_exposure_limit` und `n_positions` sowie den vollständigen JSON-Editoren für `bot.long` und `bot.short`. Bei `path` bleibt `seed_path` direkt neben `seed_mode` und hat keine zusätzliche Hilfszeile darunter, damit beide Felder bündig bleiben. Der Config-Bereich bleibt bei `none` und `path` ausgeblendet, weil diese Modi nicht von der aktuellen Config seeden.

Die JSON-Editoren für `bot.long` und `bot.short` zeigen jetzt ebenfalls dieselbe Neutralisierungs-Rückmeldung wie Backtest/Run: vom Passivbot-Vorbereitungspfad eingefügte oder normierte Felder werden mit derselben gelb/roten Zeilenhervorhebung und Legende markiert, sodass sich Long/Short-Anpassungen vor dem Speichern leichter prüfen lassen.

Der **Scoring**-Expander bildet jetzt das kanonische PB7-Zielformat direkt ab: bestehende Ziele erscheinen als explizite Metric-/Goal-Zeilen und lassen sich per Klick auf die Zeile direkt inline bearbeiten, während neue Ziele zunächst verborgen bleiben, bis der Benutzer auf **Add** klickt. Die Erstellung erscheint dann in derselben Inline-Tabellenstruktur, wobei Type / Metric / Currency in einer einzigen Zeile nebeneinander stehen statt vertikal gestapelt, weil der Scoring-Bereich dafür genug Breite hat. Wo Passivbot einen Standard kennt, wird dessen `min`/`max`-Richtung vorausgewählt; bei Metriken ohne PB7-Default bleibt das Goal trotzdem explizit in der gespeicherten Config.

Der **Limits**-Expander orientiert sich jetzt deutlich stärker am alten Streamlit-Workflow und zugleich am aktuellen PB7-Schema: bestehende Limits erscheinen in einer kompakten Tabelle mit separaten Spalten für Metric / Penalize If / Stat / Value / Enabled und lassen sich per Klick auf die Zeile direkt inline bearbeiten. Neue Limits bleiben verborgen, bis der Benutzer auf **Add** klickt; erst dann erscheint dieselbe Inline-Tabellenzeile zur Erstellung, ohne separaten „Add New Limit“-Block oder Hilfetext darunter. Der gestapelte Metric-Bereich bleibt links als Type / Metric / Currency zusammen, und `Enabled` sitzt am Ende direkt neben den Zeilenaktionen statt mitten in der Tabelle. Der Editor zeigt jetzt die komplette kanonische Operatorliste (`>`, `>=`, `<`, `<=`, `==`, `!=`, `outside_range`, `inside_range`, `auto`), unterstützt `median` als Aggregations-Statistik und bietet die neueren PB7-Metrikfamilien aus `docs`, `schema` und `src` an, darunter Strategy-PnL-Rebased, HSL-/Hard-Stop-Metriken, Trade-Loss-Metriken, Win-Rate, Paper-Loss-/Exposure-Ratios und `backtest_completion_ratio`.

Der **Bounds & Overrides**-Expander ersetzt jetzt das alte rohe `optimize.bounds`-Textarea durch ein Streamlit-artiges Layout: **Bounds long** und **Bounds short** stehen nebeneinander, und jede Bounds-Zeile verwendet einen Range-Slider mit den aktuellen Min-/Max-Werten darüber, ein kompaktes `step`-Feld rechts und eine eigene **Fixed**-Checkbox, die den jeweiligen Bound-Key in `optimize.fixed_params` speichert. Der Bound-Name sowie die Labels `step` und `fixed` tragen ihre Hover-Hilfe jetzt direkt über den gepunktet unterstrichenen Text, ohne zusätzliche Fragezeichen-Buttons in dieser Zeile. Wenn beide Slider-Griffe sehr nah zusammengeschoben werden, trennt die Oberfläche die Min-/Max-Labels automatisch, damit die Zahlen lesbar bleiben. Wenn beide Griffe exakt auf demselben Wert liegen, entscheidet weiter die erste Ziehrichtung, welchen Griff PBGui bewegt: nach links den unteren Bound, nach rechts den oberen. Wenn die Griffe aber nur sehr nah beieinander liegen, etwa bei `0 | 1`, bevorzugt PBGui jetzt wieder zuerst die angeklickte Seite statt pauschal die Ziehrichtung zu erzwingen, sodass sich der linke Griff auch nach rechts und der rechte Griff auch nach links natürlich bewegen lässt. Die Bounds-Praezision richtet sich jetzt zuerst nach dem jeweiligen `step`; wenn kein bound-spezifischer Step gesetzt ist, verwendet PBGui wieder die eingebauten parameter-spezifischen Slider-Defaults, und nur unbekannte Bounds fallen noch auf `round_to_n_significant_digits` mit `5` als letztem Fallback zurück. Sobald ein Bound-Step gesetzt ist, übernehmen die live sichtbaren Range-Inputs diesen Step sofort ebenfalls, sodass das Ziehen direkt dem eingegebenen Inkrement folgt statt erst nach einem vollständigen Editor-Rebuild. Direkte Min-/Max-Eingaben in den Chips folgen diesem expliziten Step jetzt ebenfalls: zusätzliche Nachkommastellen werden schon beim Tippen abgeschnitten, und der gespeicherte Wert rastet anschließend auf demselben Step-Raster ein, das auch der Slider verwendet. Die sichtbaren Min-/Max-Chips verwenden diese Präzision nur noch als Obergrenze und trimmen nachgestellte Nullen, sodass Werte wie `0`, `10` und ein Step von `0.1` auch genau als `0`, `10` und `0.1` angezeigt werden statt als gepolsterte Formen wie `0.00000`, `10.0` oder `0.10000`. Im selben Bereich bleiben jetzt auch die TP-Grid-Richtung und `lossless_close_trailing` als Suchrestriktionen direkt bei den Runtime-Overrides, sodass alle Suchraum-Beschränkungen vor den backend-spezifischen Settings zusammenliegen. Zusätzlich gibt es dort zwei dedizierte, nur für Optimize geltende `fixed_runtime_overrides`-Felder für `bot.long.hsl_no_restart_drawdown_threshold` und `bot.short.hsl_no_restart_drawdown_threshold`, passend zur PB7-Prepare-Pipeline, die aktuell nur diese beiden Runtime-Override-Keys erhält. Das GUI hält diesen Teil bewusst kompakt und wiederholt die rohen Dotted-Keys nicht unter den Eingaben; die Details stehen hier im Guide: Die Felder zeigen Dezimalwerte immer mit Punkt wie `0.1` an, akzeptieren nur den dokumentierten Bereich `0.0` bis `1.0`, und PB7 zieht Werte unterhalb des passenden `hsl_red_threshold` zur Laufzeit nach oben.

Der Optimizer-Teil des Editors ist jetzt backend-aware statt alle Optimizer-Keys als einen flachen Block zu behandeln. Die allgemeine Kopfzeile enthält nur noch die Identität der Config, während `optimize.backend` jetzt erst nach **Scoring**, **Limits** und **Bounds & Overrides** folgt, weil genau diese Bereiche die backend-spezifischen Auto-Anzeigen direkt beeinflussen. Die Auswahl dieses Backend-Schalters schaltet zwischen den kanonischen verschachtelten Pymoo-Controls und den nur für DEAP relevanten Legacy-Controls um. Wenn eine ältere Optimize-Config noch kein explizites `optimize.backend` hat, aber weiterhin die alten DEAP-Felder mitbringt, öffnet der Editor sie jetzt wieder als `deap` statt still auf `pymoo` zu fallen. Das Umschalten zwischen beiden Backends führt im Editor jetzt außerdem eine explizite Migration aus: gemeinsame Felder wie Population Size und Eta-Werte werden übernommen, DEAP-only Felder ohne direkte Pymoo-Entsprechung werden auf klare PB7-Defaults gesetzt, und beim Speichern werden stale Felder des jeweils inaktiven Backends wieder entfernt, damit die Config nicht in einen gemischten DEAP/Pymoo-Zustand driftet. Für Pymoo bearbeitet der eigene Abschnitt jetzt `optimize.pymoo.algorithm`, `optimize.pymoo.shared.*` und die NSGA-III-`ref_dirs`-Keys direkt; zusätzlich zeigt der Editor den effektiven Algorithmus an, den PB7 anhand der aktuellen Objective-Anzahl tatsächlich verwenden wird, und hält Mutation-`auto` an die aktuelle Anzahl aktiver Bounds gekoppelt. So bleiben kanonische PB7-Felder aus **Additional Parameters** heraus, und PBGui muss keine zweite statische Optimizer-Optionsliste mehr pflegen.

Die Raw-JSON-Synchronisierung berechnet diese Legacy-Backend-Erkennung jetzt ebenfalls direkt aus der neu geparsten Config neu, sodass kein veralteter `deap`-Hinweis mehr hängen bleibt, nachdem alte DEAP-only-Keys entfernt oder ein explizites `optimize.backend` im Raw-Editor gesetzt wurden.

Der Expander **coin_sources** nutzt jetzt denselben chip-basierten Bedienablauf wie im Backtest statt des bisherigen PBGui-JSON-Blocks: zuerst wird eine Exchange gewählt, dann ein Coin aus der geladenen Symbolliste hinzugefügt, und der Override wird sauber unter `backtest.coin_sources` gespeichert. Alte `pbgui.coin_sources`-Werte werden beim Laden in den strukturierten Editor übernommen, und das veraltete PBGui-Feld `market_settings_sources` wird dort nicht mehr angezeigt.

Der Bereich **Suite Mode** verwendet jetzt denselben gemeinsamen Editor wie der FastAPI-Backtest, statt Suite-Configs nur in der alten Streamlit-Optimize-Seite bearbeitbar zu lassen. Dadurch lassen sich `backtest.suite_enabled`, `backtest.scenarios` und `backtest.aggregate` direkt im FastAPI-Optimize-Editor bearbeiten und speichern, einschließlich der eingebauten Templates, Szenario-Overrides, per-Szenario-`coin_sources` und Aggregationsregeln.

Der Bereich **Additional Parameters** ist jetzt gezielt für Optimizer-Einstellungen reserviert und bleibt am Ende des Editors immer sichtbar. Enthält `optimize.*` Schlüssel, für die es noch keine eigenen Formularfelder gibt, erscheinen sie dort als typisierte Eingaben oder JSON-Editoren statt nur im Raw-JSON; wenn keine solchen Schlüssel vorhanden sind, zeigt der Expander einen kleinen Leer-Hinweis, damit trotzdem klar bleibt, wo zusätzliche Optimize-Keys auftauchen würden. Bekannte Felder mit eigenen Controls wie `round_to_n_significant_digits`, `compress_results_file` und `write_all_results` bleiben in ihrem normalen Abschnitt und werden dort nicht zusätzlich dupliziert. Änderungen daraus werden beim Speichern genauso übernommen wie die restlichen strukturierten Editorfelder.

Wichtige Konfigurationspunkte:

| Bereich | Beschreibung |
|---------|--------------|
| **Exchange / Symbols** | Börse und Coins für die Optimierung |
| **Date range** | Start- und Enddatum der Optimierungssimulation |
| **Iterations** | Anzahl der Optimizer-Generationen |
| **CPU cores** | Parallele Worker innerhalb eines einzelnen Optimierungslaufs |
| **Market & Universe** | Bündelt Starting Balance, Candle-Intervall, OHLCV-Quelle, BTC-Collateral-Caps und die vollständigen Coin-Universum-Controls vor den optimizer-spezifischen Einstellungen |
| **Run Settings** | Enthält Starting Seeds, Iterations, CPUs, Pareto-Aufbewahrung, Logging, Throttles, Rundung und Ergebnis-Schalter erst nachdem der Suchraum definiert ist |
| **Logging level** | Nutzt denselben Selektor wie Run mit den Labels `warning`, `info`, `debug` und `trace`, speichert darunter aber weiter die numerischen PB7-Loglevel `0`-`3` |
| **hsl_signal_mode** | Aus PB7 abgeleiteter Selektor für das HSL-Verhalten auf Kontoebene während Optimize/Backtest: `pside` trennt Long/Short-Signale, `unified` verwendet ein gemeinsames Signal |
| **Backend** | Wählt `optimize.backend` aus den von PB7 unterstützten Optimizer-Backends erst nachdem Ziele, Limits und Bounds feststehen, damit das backend-spezifische Auto-Verhalten den aktuellen Suchraum widerspiegelt |
| **Pymoo algorithm** | Verwendet das kanonische Feld `optimize.pymoo.algorithm` mit den PB7-Werten `auto`, `nsga2` und `nsga3` und zeigt zusätzlich den effektiven Algorithmus aus der aktuellen Objective-Anzahl an |
| **Population size** | Für pymoo/NSGA-III ist `auto` jetzt eine Readonly-Anzeige mit der tatsächlich effektiven numerischen Population Size aus den aktiven Reference Directions. Wenn NSGA-III mit einer expliziten Population Size kleiner als das aktuelle Reference-Direction-Minimum konfiguriert wird, hebt der Editor den Wert auf die echte von PB7 verwendete Runtime-Größe an, statt einen irreführend kleineren Wert stehen zu lassen. Für pymoo/NSGA-II erzwingt der Editor weiterhin eine explizite Population Size. Bei Legacy-DEAP-Configs mit `population_size = null` öffnet der Editor das Feld als `500`, weil genau das die von PB7 tatsächlich verwendete DEAP-Runtime-Fallback-Größe ist |
| **Pymoo shared params** | `optimize.pymoo.shared.crossover_eta`, `crossover_prob_var`, `mutation_eta`, `mutation_prob_var` und `eliminate_duplicates` haben jetzt eigene Controls statt unter Additional Parameters zu landen; wenn `mutation_prob_var_mode` auf `auto` steht, zeigt der Editor den von PB7 abgeleiteten Wert `1 / n_params` als Readonly-Anzeige statt eines leeren deaktivierten Feldes |
| **NSGA-III ref_dirs** | `optimize.pymoo.algorithms.nsga3.ref_dirs.method` und `n_partitions` werden nun strukturiert angezeigt, sobald NSGA-III aktiv ist; wenn PB7 nur genau eine unterstützte `ref_dirs_method` liefert, zeigt der Editor diesen Wert fest als Readonly-Anzeige statt eines sinnlosen Dropdowns, und bei `ref_dirs_n_partitions_mode = auto` zeigt er den von PB7 abgeleiteten Partitionswert statt eines leeren deaktivierten Feldes |
| **Backend switching** | Der Wechsel zwischen `pymoo` und `deap` übernimmt gemeinsame Felder, inklusive der DEAP-`crossover_probability` → pymoo-`crossover_prob_var`-Abbildung, setzt PB7-DEAP-Defaults dort, wo keine direkte Pymoo-Abbildung existiert, und entfernt stale Felder des jeweils inaktiven Backends beim Speichern |
| **crossover_probability + mutation_probability** | Der DEAP-Editor hält die kombinierte Wahrscheinlichkeit bei höchstens `1.0` und entspricht damit wieder dem alten Streamlit-Optimize-Verhalten |
| **Bounds & Overrides** | Optimizer-Min/Max/Step-Bounds strukturiert bearbeiten, einzelne Bounds als `fixed_params` markieren, TP-Grid-/Lossless-Close-Suchrestriktionen dort gebündelt halten und die zwei von PB7 erhaltenen HSL-`fixed_runtime_overrides` verwalten |
| **round_to_n_significant_digits** | Rundet optimierte Parameterwerte auf die konfigurierte Anzahl signifikanter Stellen, bevor sie in gespeicherte Configs und Artefakte geschrieben werden |
| **Pareto max size** | Maximale Anzahl von Configs auf der Pareto-Front |
| **Suite Mode** | Aktiviert PB7-Multi-Szenario-Optimierung direkt im FastAPI-Editor und speichert die Suite-Config unter `backtest.suite_enabled`, `backtest.scenarios` und `backtest.aggregate` |
| **Scoring** | Zielfunktionen. PB7 speichert sie als explizite Metric/Goal-Paare, und der FastAPI-Editor zeigt diese Paare jetzt direkt über eigene Metric-/Goal-Controls statt über JSON-Editing |
| **Starting Seeds** | `none` deaktiviert Seeding, `self` seedet mit der gerade gequeueten Config und blendet die Seed-Config direkt ein, und `path` übergibt eine explizite Datei oder ein Verzeichnis an Passivbot `--start` |

---

## Queue-Bereich

Zeigt alle ausstehenden, laufenden und abgeschlossenen Optimierungsjobs aus `data/opt_v7_queue/`.

Tabellenspalten:

| Spalte | Beschreibung |
|--------|-------------|
| **Name** | Name der Quell-Config |
| **Exchange** | Für diesen Job konfigurierte Börsen |
| **Status** | Aktueller Zustand: `queued`, `running`, `optimizing`, `complete`, `error` |
| **Created** | Zeitpunkt des Queue-Eintrags |
| **Actions** | Kompakte Icon-Aktionen für Start, Stop, Restart, Log, Config erneut öffnen und Löschen |

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| **Delete Selected** | Entfernt die ausgewählten Queue-Einträge, inklusive fertiger Jobs, passend zum Config-Listenmuster |
| **Settings** | Öffnet den Queue-Settings-Dialog für `Autostart` und den CPU-Wert, den Autostart vor dem Start auf gequeueete Configs anwenden soll |

Über den schmalen Greifstreifen ganz links am Anfang jeder Queue-Zeile lässt sich die Queue jetzt per Drag-and-Drop umsortieren. Genau dort erscheint bei ausgewählten Zeilen auch der blaue Marker; bei nicht ausgewählten Zeilen bleibt dieser Streifen unsichtbar, bis du direkt über die linke Greifkante hoverst, damit die Queue nicht dauerhaft wie selektiert aussieht. PBGui speichert diese Reihenfolge dauerhaft in den Queue-Einträgen, und Autostart respektiert dieselbe Reihenfolge von oben nach unten auch beim Start des nächsten gequeueeten Optimize-Jobs.
Wenn mehrere Queue-Zeilen ausgewählt sind und du eine dieser ausgewählten Zeilen ziehst, verschiebt PBGui jetzt den gesamten ausgewählten Block gemeinsam. Die gegriffenen Einträge bleiben dabei über eine Drag-Vorschau aus Klonen der echten Queue-Zeilen am Mauszeiger erkennbar, sodass beim Ziehen dieselbe Zeilenoptik sichtbar bleibt statt nur eines generischen Browser-Drag-Ghosts.
Das Ziehen über Zeilen zum Auswählen oder Deselektieren orientiert sich jetzt ebenfalls nur noch an den Zeilen, die der Mauszeiger tatsächlich überfährt. Wenn der Zeiger beim Deselektieren eines mittleren Blocks kurz zwischen den Zeilen oder leicht daneben liegt, behält PBGui den letzten gültigen Zeilenanker bei, statt die Auswahl plötzlich auf einen viel größeren Bereich auszudehnen.
PBGui berechnet die sichtbare Queue-Auswahl bei jedem Drag-Update außerdem neu aus dem beim Mouse-Down erfassten Ausgangszustand. Wenn du beim Selektieren oder Deselektieren kurz eine Zeile zu weit ziehst und den Bereich danach wieder verkleinerst, werden Zeilen außerhalb des finalen Bereichs korrekt wiederhergestellt, statt versehentlich verloren zu bleiben.
Live-Websocket-Updates der Queue rendern die Tabelle dabei jetzt auch nicht mehr mitten in dieser Mausaktion neu. PBGui wartet mit dem Nachziehen des neuesten Queue-Refreshs bis zum Ende der Auswahl, sodass sich das Selektieren wieder stabil anfühlt wie in der Results-Tabelle.

### Log-Viewer

Jede Queue-Zeile hat eine **Log**-Aktion.
Sie öffnet den gemeinsamen schwebenden Log-Viewer und streamt die lokale Datei aus `data/logs/optimizes/`.
Die **Edit**-Aktion öffnet die tatsächliche Config-Datei, auf die diese Queue-Zeile zeigt, auch wenn die Zeilenbezeichnung nicht dem gespeicherten Config-Dateinamen entspricht.
Wenn ein älterer Queue-Eintrag noch auf einen gelöschten Config-Pfad zeigt, PBGui aber passende Configs findet, kann das Matching-Config-Modal diesen bestehenden Queue-Eintrag jetzt direkt reparieren. Nach Auswahl des richtigen Kandidaten aktualisiert PBGui die Queue-Zeile auf den gewählten Config-Pfad samt frischem eingebettetem Snapshot, sodass der Eintrag nicht mehr manuell gelöscht und neu gequeued werden muss.
Neuere Queue-Einträge behalten zusätzlich einen eingebetteten Config-Snapshot. Wenn die ursprüngliche Config-Datei später gelöscht wird, kann PBGui den Queue-Job weiterhin aus diesem Snapshot öffnen oder starten, statt am veralteten Pfad zu scheitern. Das Speichern einer bearbeiteten Config unter anderem `config_name` erzeugt jetzt eine neue Config und lässt bestehende Queue-Zeilen der alten Config unverändert.
Wenn ein älterer Queue-Eintrag noch aus der Zeit vor diesen Snapshots stammt und sein ursprünglicher Config-Pfad fehlt, während mehrere passende Configs existieren, öffnet PBGui jetzt direkt ein Auswahl-Modal mit **Open**-Buttons für diese Kandidaten statt nur kurz einen Toast-Fehler anzuzeigen.
PBGui lehnt **Requeue** jetzt außerdem für Queue-Zeilen ab, deren Config weiterhin nicht startbar ist. Solche Zeilen behalten ihren aktuellen `error`-Status und das vorhandene Optimize-Log, bis die Config wirklich korrigiert wurde, statt in einen irreführenden `queued`-Status ohne startbaren Job zurückgesetzt zu werden.

Die Queue braucht damit keinen manuellen Refresh-Knopf mehr in der Sidebar. Sie aktualisiert sich laufend über den Websocket-Feed, und der **Settings**-Dialog übernimmt jetzt die Autostart-Steuerung statt einer permanent sichtbaren Sidebar-Checkbox. Wenn Autostart aktiv ist, schreibt PBGui vor jedem automatischen Start `optimize.n_cpus` der gequeueeten Config auf den im Dialog gesetzten Queue-CPU-Wert um.
Die Log-Dashboard-Zusammenfassung nutzt das Feld **CPU** jetzt für die konfigurierten Optimizer-Kerne. Wenn du über diesen CPU-Wert hoverst, öffnet sich eine htop-ähnliche Per-Core-Ansicht mit Speicher-, Swap- und Load-Average-Details, die sich während des offenen Hovers live weiter aktualisiert.
Wenn der ursprüngliche Launcher-PID eines Optimizers veraltet ist, der eigentliche `optimize.py`-Prozess aber noch läuft, hängt PBGui die Queue-Zeile jetzt wieder an den Live-Prozess an. Dadurch bleibt der Eintrag als laufend sichtbar und **Stop** beendet weiterhin den echten Job.
Wenn mehrere Queue-Zeilen dieselbe Config referenzieren, bindet PBGui den Live-Prozess jetzt nur noch an die Zeile, deren eigenes Optimize-Log wirklich zu diesem Prozess gehört. Andere Zeilen übernehmen diesen `running`-Status nicht mehr nur wegen der gemeinsamen Config-Datei.
Die Toast-Meldungen der Optimize-Seite werden jetzt zusätzlich in das globale PBGui-Notification-Log geschrieben. Kurz eingeblendete Fehler- oder Erfolgsmeldungen lassen sich damit später auch über die Glocke oben rechts erneut ansehen.

---

## Results-Bereich

Zeigt abgeschlossene Optimierungsergebnisse aus `pb7/optimize_results/`.

Toolbar- und Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| **Delete Selected** | Alle ausgewählten Ergebnisordner über die Sidebar löschen, analog zum Backtest-Result-Workflow |
| **Search** | Nach Optimize-Name oder Ergebnisordner filtern |

Solange ein Optimize-Job läuft, aktualisiert sich dieses Panel automatisch alle paar Sekunden, damit neu geschriebene Pareto-Dateien ohne manuelles Neuladen im **Paretos**-Zähler erscheinen.

Durch Klick auf die sortierbaren Tabellenüberschriften lässt sich nach **Name**, **Result Directory**, **Paretos**, **Mode** oder **Modified** sortieren.

Tabellenspalten:

| Spalte | Beschreibung |
|--------|-------------|
| **Name** | Aus dem Ergebnis erkannter Config-Name |
| **Result Directory** | Name des Ergebnisordners |
| **Paretos** | Anzahl gefundener Pareto-JSON-Dateien |
| **Mode** | Zeigt, ob das Ergebnis ein normaler Single-Result-Optimize-Lauf oder ein Suite-Result ist, inklusive Anzahl der Suite-Szenarien |
| **Modified** | Zeitstempel des Ergebnis-Sets |
| **Actions** | Kompakte Icon-Aktionen für die Pareto-Dateiliste, den vollständigen **Pareto Explorer**, das originale PB7-**Pareto Dash**, den Legacy-PB7-**3D plot**, Continue Optimize aus dem `pareto/`-Verzeichnis des Ergebnisses und den ersten Config-Draft |

Die kompakte **Pareto-Liste** verwendet jetzt ein eigenes ordnerartiges Icon, während **🎯 Pareto Explorer** wieder als separate eigene Aktion vorhanden ist. Damit entspricht die Results-Tabelle wieder der alten Streamlit-Aufteilung, statt das Explorer-Icon für die reine Dateiliste zu verwenden. Beim Klick auf **🎯 Pareto Explorer** navigiert PBGui jetzt im aktuellen Tab weiter und reicht den ausgewaehlten Result-Pfad automatisch durch den Streamlit-Relay weiter. Wenn du anschliessend zu FastAPI Optimize zurueckkehrst, aktualisiert die Seite Configs, Queue und Results automatisch, damit die Results-Ansicht nicht auf einem gecachten leeren Snapshot stehen bleibt.

Wenn der Pareto Explorer von der schnellen `pareto/*.json`-Ansicht auf **Load all_results.bin** umschaltet, bleibt die persistierte PB7-Pareto-Menge jetzt auch in diesem groesseren Sample erhalten. PBGui hasht die kompletten PB7-Result-Entries jetzt genauso wie PB7 die Dateien `pareto/<hash>.json` benennt, mischt diese bekannten Pareto-Mitglieder immer in das gesampelte Config-Fenster ein und behaelt diese offiziellen Pareto-Flags bei, statt nur noch eine kleinere Subset-Front neu zu berechnen.

Die Aktion **PB7 3D plot** rendert die Legacy-PB7-Ansicht des interaktiven 3D-Plotly-Plots jetzt direkt in einem großen Modal innerhalb des aktuellen FastAPI-Tabs, sofern das Ergebnis genau 3 Objectives liefert. Damit bleibt diese originale PB7-3D-Perspektive weiterhin bewusst vom umfangreicheren PBGui-Pareto-Explorer getrennt, aber es ist kein separates Browser-Tab mehr noetig. Falls ein Ergebnis keine gueltigen 3D-Pareto-Punkte liefert, faellt die Seite stattdessen auf ein Detail-Modal mit dem PB7-Grund zurueck, statt nur einen generischen Start-Toast anzuzeigen.

Die Aktion **PD** öffnet jetzt auch Passivbots originales `tools/pareto_dash.py` direkt in demselben großen Modal-Fenster. PBGui startet dafür die Dash-App im PB7-Environment, stellt nur das ausgewählte Ergebnis in einen kleinen Staging-Root und liefert das Dashboard über dieselbe FastAPI-Origin zurück, damit das native PB7-Pareto-Dashboard im aktuellen Tab nutzbar bleibt.

Mit **Continue Optimize** lässt sich ein neuer Optimize-Lauf starten, der mit den Pareto-Seeds eines früheren Ergebnisses weiterarbeitet. Dabei wird `seed_path` automatisch auf das `pareto/`-Verzeichnis des Ergebnisses gesetzt.

Mit **Open Config** lässt sich die erste Pareto-Config dieses Ergebnisses als Draft im Editor öffnen, ohne zusätzliche Seed-Metadaten zu setzen.

---

## Paretos-Bereich

Wird über **Results → Paretos** geöffnet.
Hier werden die Pareto-JSON-Dateien eines ausgewählten Ergebnis-Sets angezeigt.

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| **Seed Selected** | Öffnet einen neuen Optimize-Draft, der die ausgewählte Pareto-Datei oder ein Bundle aus den ausgewählten Pareto-Zeilen als `seed_path` verwendet |
| **Seed Whole Result** | Öffnet einen neuen Optimize-Draft mit dem kompletten `pareto/`-Verzeichnis des aktuellen Ergebnisses als `seed_path` |

Während ein Optimize-Job noch weitere Paretos schreibt, wird die geöffnete Liste automatisch alle paar Sekunden aktualisiert.

Toolbar-Steuerungen:

| Steuerung | Beschreibung |
|--------|-------------|
| **Mode-Chip** | Zeigt, ob das ausgewählte Ergebnis ein normales Result, ein Suite-Result oder ein Legacy-Pareto-Format ist |
| **Scenario** | Schaltet bei Suite-Results die Summary zwischen **Aggregated** und einem konkreten Szenario um |
| **Statistic** | Schaltet für aggregierte Suite-Ansicht und normale Single-Result-Paretos zwischen `mean`, `min`, `max` und `std` um |

Tabellenspalten:

| Spalte | Beschreibung |
|--------|-------------|
| **Name** | Dateiname der Pareto-Datei |
| **Summary** | Kurze Metric-Badges, soweit aus der Datei ableitbar |
| **Modified** | Änderungszeitpunkt der Datei |
| **Actions** | Kompakte Icon-Aktionen für Roh-JSON oder die direkte Verwendung einer Pareto-Datei als Start-Seed für einen neuen Optimize-Draft |

Bei Suite-Results folgen die **Summary**-Badges jetzt der Toolbar-Auswahl, statt immer nur eine feste Aggregatansicht zu zeigen. Damit lassen sich direkt in der FastAPI-Seite entweder die aggregierte Suite-Statistik oder einzelne Szenarien prüfen, ohne zurück in die alte Streamlit-Optimize-Seite zu wechseln.

**Use as Seed** öffnet einen neuen Optimize-Draft auf Basis der aktuellen Result-Config und setzt `seed_path` auf genau diese Pareto-Datei.

Wenn mehrere Pareto-Zeilen markiert sind, erstellt **Seed Selected** ein kleines Seed-Verzeichnis nur aus diesen Dateien und verwendet dieses Verzeichnis als `seed_path`.

---

## Typische Arbeitsabläufe

### Neue Optimierung starten
1. **Configs** öffnen und **New Config** klicken.
2. Strukturierte Felder ausfüllen, bei Bedarf die Advanced-JSON-Bereiche anpassen und danach **Save and Queue** verwenden.
3. **Queue** öffnen, bei Bedarf über **Settings** **Autostart** oder den Queue-CPU-Wert setzen und mit **Log** den Fortschritt beobachten.
4. Nach Abschluss zu **Results** wechseln.

### Ergebnisse auswerten
1. **Results** öffnen und zum gewünschten Lauf filtern.
2. **Paretos** klicken, um die erzeugten Pareto-Dateien zu prüfen.
3. **View JSON** für die Rohansicht verwenden, auf das Seed-Icon für einen einzelnen Pareto klicken oder mehrere Zeilen markieren und **Seed Selected** nutzen.

### Bestehende Config anpassen
1. In **Configs** nach der Config suchen und sie auswählen.
2. **Edit Selected** klicken.
3. Strukturierte Felder oder die Advanced-JSON-Bereiche anpassen und danach **Save** oder **Save and Queue** verwenden.

### Mit einem früheren Optimize-Lauf weiterarbeiten
1. **Results** öffnen und das gewünschte Ergebnis auswählen.
2. **Continue Optimize** klicken, um das ganze `pareto/`-Verzeichnis zu verwenden, oder im **Paretos**-Bereich **Seed Whole Result** bzw. **Seed Selected** für feinere Auswahl nutzen.
3. Den Draft bei Bedarf anpassen und danach **Save** oder **Save and Queue** verwenden.
4. Der neue Lauf startet mit den gespeicherten Pareto-Seeds, ist aber trotzdem ein frischer Optimize-Run und kein exaktes Checkpoint-Resume.
