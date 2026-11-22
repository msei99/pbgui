# PBv9 Optimize

Die Exchange-Auswahl zeigt alle von den installierten PB9-Konnektoren und CCXT unterstützten Börsen für historische Daten. Das Template legt nur die Vorauswahl fest. Suite Mode und Coin Sources verwenden denselben Katalog.

Coin-Auswahl und Apply Filters verwenden dieselbe Katalog- und Filterlogik wie
PB8, mit dem nativen PB9-Resolver. Normale Coins erscheinen einmal unter ihrem
Namen. Exakte Marktkennungen bleiben für mehrdeutige Märkte und bereits exakt
gespeicherte Auswahlen erhalten.

Die gemeinsame Coin-Suche berücksichtigt Namen und native Kontrakte, jedoch
keine Börsenpräfixe oder Anzeigezusätze: `hype` findet HYPE/HYPER statt STX auf
Hyperliquid. Vollständige Kennungen wie `hyperliquid::19` bleiben suchbar und
werden bei der Auswahl unverändert erhalten.

Öffne **PBv9 → Optimize**. Configs, Queue, Results und Paretos verwenden dieselbe
Seite, Bedienung und Engine wie PBv8, mit eigener nativer PB9-Laufzeit und
getrennten Datenverzeichnissen.

Die Config-Liste lädt vor nativen Metadaten, Queue- und Ergebnisprüfungen. Bis zur
ersten Antwort erscheint eine Ladeanzeige statt „0 configs“. Backtest-Zähler
werden separat aktualisiert und verzögern die Config-Zeilen nicht.

## Editor und native Regeln

Beschriftungen und Checkboxen folgen dem PB8-Aufbau. Gepunktet unterstrichene
Beschriftungen zeigen bei Mauszeiger oder Tastaturfokus die Feldhilfe mit Quelle
aus der installierten PB9-Dokumentation und vollständigem Konfigurationspfad.
Seitenspezifische Overrides nennen ihren gemeinsamen Standard aus `bot.global`;
die Seitenaktivierung bleibt ausdrücklich gesetzt. Fehlt eine Beschreibung in
der installierten Dokumentation, sagt die Hilfe das, statt den Text eines anderen
Parameters zu verwenden.

Erstelle eine neue Konfiguration oder importiere eine native PB9-Optimizer-Config.
Der gemeinsame Editor enthält Market & Universe, Scoring, Limits, Bounds &
Overrides, Backend-Einstellungen und Raw JSON. PB9 besitzt zusätzlich gemeinsame
**Global**- und **Portfolio**-Bounds. Regeln verwenden native Feldnamen.
Geerbte Seitenwerte bleiben geerbt, bis sie geändert werden. Das reine Öffnen
schreibt angezeigte Standardwerte nicht als feste Overrides zurück.

Globale Bounds gelten für Seiten, die den jeweiligen Wert erben. Die Listen
für Long und Short zeigen ausdrücklich eingetragene Seitenbereiche und können
daher unterschiedlich sein. Unter **Policy settings** findest du die Regeln.
**Global bounds** und **Portfolio bounds** öffnen die jeweiligen Suchbereiche.
Mit **Search global bounds** findest du einen Regler anhand seines Parameternamens.
Die Suche ändert nur die Anzeige. **Portfolio policy (account-wide)** und
**Shared defaults (Long and Short)** stehen über den beiden Seitenkarten.
Wähle unter Global oder Portfolio **Add bound** oder unter Long oder Short
**Add override**. Die Auswahl enthält auch Parameter, für die die Vorlage
keinen Bereich vorgibt. Ein Seitenbereich beginnt mit dem globalen Bereich,
falls einer gesetzt ist. **Remove** entfernt den gewählten Bereich.
Der HSL-Modus **portfolio** schaltet die vier zugehörigen Portfolio-Bounds frei.
Entferne beim Wechsel vorhandene globale oder seitenspezifische HSL-Bounds.
Ein fest eingetragener Seitenwert ohne eigenen
Suchbereich bleibt fix. Beim Strategiewechsel können gemeinsame Bereiche für die
neue Strategie erscheinen, wenn bereits eine gemeinsame Strategiesuche
konfiguriert ist. Bestehende Seitenbereiche behalten ihre Seite und Strategie.

**Long overrides** und **Short overrides** oben in den Seitenkarten listen die
Werte, die eine Seite selbst setzt. **Reset** entfernt diesen Seitenwert; die
Seite erbt dann wieder den gemeinsamen Standard (bei reinen Seitenwerten wie
`enabled` den nativen Standard). Seiten-JSON und Regelfelder bleiben synchron;
die jeweils letzte Änderung eines Werts gilt.

Ein **fixed** markierter Bound behält seinen aktuellen Config-Wert, nicht einen
Wert aus dem Regler-Bereich. Der verwendete Wert steht unter der Checkbox als
`uses: …`. Für einen anderen festen Wert änderst du den Config-Wert oder setzt
Minimum und Maximum des Bounds auf diesen Wert. Numerische Regelwerte, die nicht
optimiert werden können, etwa `forager.score_hysteresis_margin` sowie die
Portfolio-Einstellungen für Warmup und Market Orders, sind normale Felder in
den Regelkarten.

PB9-Metriken behalten ihre nativen Namen und Einheiten. CAGR ist nicht PB8-ADG.
Prüfe Ziele und Grenzwerte vor Versionsvergleichen. Szenario-Vorlagen verwenden
native PB9-Ziele und Limits. Wie bei PB8 listet **Additional Parameters**
Einträge aus dem Abschnitt `optimize` auf, für die es noch kein eigenes
Bedienelement gibt. Dazu gehören neue Felder aus der installierten
Passivbot-Vorlage. Bearbeite die Werte direkt in der Liste und speichere mit
**Save**.

Scoring- und Limit-Einträge mit `scenario: null` aus dem nativen Template sind
auch ohne Suite Mode gültig und bleiben beim Speichern unverändert. Benannte
Szenarien benötigen Suite Mode und ein passendes Szenario-Label.

In Suite-Szenarien können `coins` und `ignored_coins` eine Coin-Liste, ein
Textwert (zum Beispiel eine Coin-Listendatei) oder getrennte Long/Short-Auswahlen
sein. Der Editor behält die gespeicherte Form bei. Mit der Auswahl neben dem Feld
wechselst du die Form. **Inherit (base config)** entfernt die Übersteuerung. Eine
leere Liste, ein leerer Text oder eine leere Seite wird als ausdrücklich leere
Übersteuerung gespeichert und ist damit etwas anderes als Inherit. Bei Long/Short
lässt du eine Seite mit **Not set** weg.

## Metrikreferenz

Diese Referenz erklärt die aktuellen **27 nativen Selektoren**: 26 Größen mit
getrennten Long-/Short-Selektoren für die Positionsauslastung. Die Auswahl liest
den Katalog der installierten PB9-Version dynamisch. Grundlage der Erklärungen
sind deren `docs/metrics.md` und Rust-Metrikkatalog. Ältere Ergebnisse können
eine andere Definitionsversion verwenden.

### Einheiten, Ziele und Limits

- **Strategy Equity** umfasst Startkapital, realisierte Handelsgewinne/-verluste,
  Gebühren/Rückvergütungen und unrealisierte Handelsgewinne/-verluste. Direkte
  Wertsteigerungen der Sicherheiten sind ausgeschlossen. Bei reinen
  USD-Sicherheiten entspricht sie der ausgewiesenen USD-Equity.
- **Anteil** verwendet die Skala eins: `0.05` bedeutet 5 %, `1` bedeutet 100 %.
  **Verhältnis** ist einheitenlos. Zeitangaben stehen je nach Name in Stunden
  oder Tagen. Die Hochrechnung auf ein Jahr verwendet 365,25 Kalendertage.
- **Maximieren/minimieren** empfiehlt eine Zielrichtung; **Ober-/Untergrenze**
  empfiehlt ein Limit. PB9 erlaubt ausdrückliche Zielrichtungen und beide
  Limitrichtungen für jede numerische Metrik. Ein zusätzliches Ziel verändert
  Optimierung und Pareto-Front. Reine Risikoziele können Inaktivität begünstigen:
  Kombiniere sie mit passenden Wachstums- oder Aktivitätsanforderungen.
- **Goal** und **Reducer** unterscheiden sich. Bei Ulcer minimiert `goal: min`
  mit `reducer: max` den schlechtesten Quellen-/Szenarienwert. Mean, min, max,
  std und median sind Aggregationen, keine zusätzlichen Metriknamen. Ein
  benanntes Szenario wählt dieses statt des Suite-Aggregats.

### Wachstum und risikobereinigte Performance

| Metrik | Bedeutung | Einheit | Typische Verwendung |
| --- | --- | --- | --- |
| `cagr_fraction_strategy_equity` | Annualisiertes, zusammengesetztes Equity-Wachstum im tatsächlich ausgewerteten Zeitraum: `(Ende / Start)^(365.25 / Tage) - 1`. | Anteil (annualisiert) | Wachstum maximieren. |
| `cagr_fraction_strategy_equity_ew` | Annualisiertes logarithmisches Wachstum mit exponentieller Zeitgewichtung; jüngere Intervalle zählen stärker. | Anteil (annualisiert) | Stärker auf jüngste Entwicklung gewichtetes Wachstum maximieren. |
| `drawdown_max_fraction_strategy_equity` | Größter Rückgang gegenüber dem bis dahin erreichten Equity-Höchststand. | Anteil | Obergrenze für den maximalen Drawdown. |
| `ulcer_index_fraction_strategy_equity` | Quadratischer Mittelwert (RMS) der Drawdowns; berücksichtigt Tiefe und Zeit unter dem Höchststand. | Anteil | Anhaltende/tiefe Drawdowns minimieren. |
| `pain_index_fraction_strategy_equity` | Mittlerer Drawdown aller Beobachtungen, einschließlich flacher Phasen und Drawdowns von null. | Anteil | Durchschnittliche Drawdown-Belastung minimieren. |
| `volatility_annual_fraction_strategy_equity` | Stichproben-Standardabweichung der täglichen Equity-Renditen, auf ein Jahr hochgerechnet. | Anteil (annualisiert) | Obergrenze für Renditeschwankungen. |
| `sharpe_ratio_strategy_equity` | Mittlere Tagesrendite geteilt durch ihre Stichproben-Standardabweichung, mit `sqrt(365.25)` skaliert. | Verhältnis | Rendite relativ zur gesamten Schwankung maximieren. |
| `sortino_ratio_strategy_equity` | Mittlere Tagesrendite relativ zur Abweichung negativer Renditen, mit `sqrt(365.25)` skaliert. | Verhältnis | Rendite relativ zu negativen Schwankungen maximieren. |
| `omega_ratio_strategy_equity` | Summe positiver Tagesrenditen geteilt durch den Betrag negativer Tagesrenditen; nicht annualisiert. | Verhältnis | Positive gegenüber negativen Tagesrenditen maximieren. |

Tagesrendite-Metriken verwenden vollständige UTC-Tages-Schlusskurse. PB9 zieht
keinen risikofreien Zins ab; Sortino und Omega verwenden null als Renditeschwelle.
Beim Sortino-Mittelwert zählen alle Tagesrenditen; nichtnegative tragen null bei.
CAGR ist nicht PB8-ADG. Ohne `backtest.ew_half_life_days` gelten für CAGR EW
360 Tage; ausdrückliches `null` nutzt den nativen Rückfallwert von 180 Tagen.
Es gibt keine EW-Versionen von Sharpe/Sortino/Omega und keine automatischen
USD-/BTC-Kopien dieser Metriken.

### Abdeckung, Aktivität und Haltedauer

| Metrik | Bedeutung | Einheit | Typische Verwendung |
| --- | --- | --- | --- |
| `backtest_completion_fraction` | Tatsächlich abgedeckte Auswertungsdauer / vorgesehene Dauer, auf `[0, 1]` begrenzt. | Anteil | Untergrenze, häufig `1`, für vollständige Abdeckung. Diese schließt eine Liquidation am Ende nicht aus. |
| `fills_gap_max_days` | Längste Ausführungslücke im gesamten Portfolio, einschließlich der Auswertungsgrenzen. | Tage | Obergrenze für lange inaktive Phasen. |
| `fills_gap_time_weighted_mean_hours` | Dauergewichtete Ausführungslücke: `sum(Lückenstunden²) / sum(Lückenstunden)`. Lange Lücken zählen stärker. | Stunden | Längere Inaktivität minimieren. |
| `peak_recovery_max_days_strategy_equity` | Längste Phase unter einem Equity-Höchststand bis zu dessen Wiedererreichen; eine offene Erholung endet am Auswertungsende. | Tage | Erholungsdauer nach oben begrenzen oder als Ziel minimieren. |
| `position_held_max_days` | Längste Episode vom Öffnen bis zum vollständigen Schließen je Coin und Positionsseite, einschließlich noch offener Positionen am Ende. | Tage | Obergrenze für die maximale Haltedauer. |
| `position_held_time_weighted_mean_hours` | Dauergewichtete Haltedauer: `sum(Haltestunden²) / sum(Haltestunden)`. Aufstockungen und Teilschließungen starten die Uhr nicht neu. | Stunden | Lange Halteepisoden minimieren. |
| `gross_turnover_ratio_per_day` | Ein- und Ausstiegsvolumen relativ zum Kontostand unmittelbar vor jeder Ausführung, geteilt durch verstrichene Tage. | Pro Tag | Nur bei gewünschter Aktivität maximieren oder einen Aktivitätsgrenzwert setzen. |

Ohne Ausführungen entsprechen beide Lückenmetriken der tatsächlichen
Auswertungsdauer in ihrer Einheit. Ohne Positionsepisode mit positiver Dauer
sind beide Haltemetriken null. Eine offene Erholung belegt kein späteres
Wiedererreichen des Höchststands. Umsatz zählt Ein- und Ausstiege sowie inaktive
Tage mit: `0.5` bedeutet bei unverändertem Kontostand ein halbes Kontoguthaben
an gehandeltem Bruttovolumen pro Tag. Mehr Umsatz ist nicht grundsätzlich besser.

### Exposure und Gewinnzusammensetzung

| Metrik | Bedeutung | Einheit | Typische Verwendung |
| --- | --- | --- | --- |
| `gross_exposure_rms_ratio_strategy_equity` | RMS des gesamten absoluten Positionswerts zu aktuellen Marktpreisen / Strategy Equity. Long und Short werden addiert. | Verhältnis | Anhaltendes Brutto-Exposure minimieren. |
| `gross_exposure_max_ratio_strategy_equity` | Maximum dieses Brutto-Exposure-zu-Equity-Verhältnisses. | Verhältnis | Obergrenze für Exposure-Spitzen. |
| `position_exposure_utilization_rms_ratio_long` | RMS der Long-Auslastung relativ zu nominalen Positions-/Seitenbudgets (WEL/TWEL), ohne Excess Allowance. | Verhältnis | Long-Budgetauslastung minimieren oder begrenzen. |
| `position_exposure_utilization_rms_ratio_short` | Dieselbe budgetbezogene Auslastung für Short-Positionen. | Verhältnis | Short-Budgetauslastung minimieren oder begrenzen. |
| `loss_profit_ratio` | Betrag negativer realisierter Netto-Ausführungszahlungen / positive realisierte Netto-Ausführungszahlungen, einschließlich Gebühren/Rückvergütungen; ohne unrealisierte Gewinne/Verluste. | Verhältnis | Realisierte Verluste relativ zum Gewinn minimieren. Über `1` bedeutet realisierten Nettoverlust. |
| `short_net_profit_contribution_fraction` | Netto-Short-Handelsgewinn / positiver gesamter Long-/Short-Handelsgewinn, einschließlich Änderungen unrealisierter Gewinne/Verluste und Handelskosten. | Anteil | Unter-/Obergrenzen für die gewünschte Gewinnverteilung auf beide Seiten. |

Positionsauslastung misst Budgetnutzung, kein absolutes Brutto-Exposure. Der
Short-Gewinnanteil benötigt positiven gesamten Nettogewinn. Er kann negativ oder
größer als `1` sein, wenn eine Seite verliert; er wird nicht auf 0–100 % begrenzt.
Grenzen von `0.1`–`0.9` verlangen, dass jede Seite mindestens 10 % des positiven
gesamten Nettogewinns beiträgt.

### HSL-Verhalten und Verluste

| Metrik | Bedeutung | Einheit | Typische Verwendung |
| --- | --- | --- | --- |
| `hsl_panic_episode_loss_max_fraction` | Größter Verlust einer ausgeführten HSL-Episode / Kontostand vor ihrer ersten HSL-Ausführung. | Anteil | Obergrenze für den Verlust einer einzelnen HSL-Episode. |
| `hsl_panic_episode_loss_mean_fraction` | Mittelwert dieser Episodenverlust-Anteile, einschließlich ausgeführter Episoden ohne Verlust. | Anteil | Obergrenze für den mittleren HSL-Episodenverlust. |
| `hsl_execution_episode_count` | HSL-Episoden mit mindestens einer Ausführung einer dieser Episode zugeordneten Order; mehrere Ausführungen derselben Episode zählen einmal. | Anzahl | Untergrenze wie `1`, wenn ein Zeitraum eine HSL-Ausführung enthalten muss. |
| `hsl_triggers_per_year` | Beobachtete HSL-Auslösungen, auf 365,25 Tage hochgerechnet. | Pro Jahr | Häufigkeit der HSL-Auslösungen begrenzen. |
| `hsl_red_time_fraction` | Vereinigte beobachtete RED-Zeit aktiver Controller / beobachtete HSL-Zeit; mit erzwungener Neustart-Abkühlzeit, ohne Warmup. | Anteil | Obergrenze für Zeit unter HSL-RED-Bedingungen. |

Eine Auslösung ohne ausgeführten HSL-Fill ist keine Ausführungsepisode.
Episodenverluste summieren negative Netto-Ausführungszahlungen; profitable
Ausführungen gleichen diese nicht aus. Offene ausgeführte Episoden zählen bis
zum Auswertungsende. Bei Coin-/Positionsseiten-HSL bedeutet RED-Zeit nicht,
dass der gesamte Bot inaktiv war. Core-Definitionsversion 2 zählt auch
Ausführungen zugeordneter HSL-Orders nach Rückkehr des Controllers zu GREEN;
ältere Ergebnisse können andere Regeln verwenden. Mehr Ausführungen oder
Auslösungen sind nicht grundsätzlich besser.

### Verfügbarkeit und gespeicherte Optimizer-Ergebnisse

Der Optimizer berechnet ausgewählte Ziele, aktivierte Limits und notwendige
Gültigkeitswerte. Sein Standardprofil speichert deshalb deutlich weniger als
27 Metriken. Ein eigenständiger Backtest erstellt den Core-Bericht. Zusätzliche
Ziele oder aktive Limits verändern die Optimierung; sie sind keine reine
Anzeigeoption.

Fehlende Datenbasis, ungültige Eingaben und undefinierte Verhältnisse sind nicht
null. PB9 speichert Verfügbarkeits-/Statusmetadaten. Ein ausgewähltes Ziel oder
aktiviertes Limit mit nicht verfügbarem Wert kann einen Kandidaten unzulässig
machen. Manche Metriken benötigen Minutenbeobachtungen oder ausreichend
vollständige Tagesrenditen; prüfe das beim Wechsel des Kerzenintervalls.
Historische Ergebnisse behalten ihre ursprüngliche Definition und ihren
ausgewerteten Zeitraum.

## Configuration prefilter

Unter **Configuration prefilter** bearbeitest du die nativen Einstellungen
von `optimize.prefilter`. Mit **Add check** wählst du eine Metrik, aktivierst
die Regel, wählst Long und/oder Short und setzt optionale Unter- und Obergrenzen.
Eine aktive Regel benötigt mindestens eine Grenze und die Strategie
trailing_martingale. **Remove check** entfernt eine Regel. Ohne aktive Regeln
ist der Prefilter ausgeschaltet.

Die Annahmen für die hypothetischen Prüfungen haben eigene Zahlenfelder.
Mit **Add exposure fraction** und **Remove fraction** bearbeitest du die
Exposure-Liste. Erlaubt sind 1–64 Werte von null bis ausschließlich 0,999.
Speichere mit dem normalen **Save**. Unveränderte geerbte Standardwerte bleiben
geerbt. Prozentwerte werden als Bruch eingegeben, beispielsweise `0.10` für 10 %.

Unter **Run Settings** findest du auch die Optimizer-Overrides der
installierten Version, einschließlich `mirror_short_from_long` und
`couple_unstuck_ema_spans`. `fine_tune_params` verwendet PB9-Selektoren wie
`global.risk`, `long.forager`, `*.strategy.close` oder einen Parameternamen wie
`we_excess_allowance_fraction`.

Das GPU-Backend verwendet dieselben Einstellungen für `population_size` und
Pymoo/NSGA wie das pymoo-Backend; beide Bereiche bleiben daher sichtbar.
**Screening scenarios** listet die Labels der Suite-Szenarien. Hake die Szenarien
für die Screening-Stufe der GPU an; ohne Auswahl ist das Screening aus. Ein
gespeichertes Label, das in der Suite nicht mehr existiert, erscheint als
`(not in Suite)` und muss vor dem Speichern abgewählt werden.

## Kursdaten und Ausführung

Die Suite-Datenvorbereitung berücksichtigt Coin-Auswahl, Zeitraum und Börsen jedes Szenarios. PBGui erneuert nach diesen Overrides die native Optimierungsbindung; die gespeicherte Config und ihre Suchbereiche bleiben unverändert.

PBGui Market Data ist die voreingestellte Kursdatenquelle; neue Entwürfe zeigen den zugehörigen Pfad. Wird diese
Queue-Einstellung geändert, gilt die Quelle der Config. Kerzendateien werden
direkt referenziert. Backtest und Optimize verwenden wie PB8 gemeinsam den dauerhaften nativen
Cache unter `caches/` der konfigurierten PB9-Installation. Downloads,
Abdeckungsinformationen und vorbereitete Daten bleiben zwischen Läufen
wiederverwendbar. PBGui-Kerzen bleiben an ihrem bisherigen Ort. OHLCV Readiness
und 1st/All-Datumssuche verwenden die nativen PB9-Werkzeuge. Sie kopieren oder
laden keine Kerzen vor; fehlende Historie holst du über **Market Data**.
Offline-Modus benötigt vollständige lokale Kerzen und öffentliche Marktmetadaten.

1st/All funktioniert direkt mit ungespeicherten Entwürfen und verwendet die
aktuellen gültigen Editorwerte. Coin-Auswahl und Seiten-JSON bleiben synchron.
Änderst du die Konfiguration während einer Suche, wird deren älteres Ergebnis
nicht übernommen.

Save, Save & Queue, Autostart, CPU-Overrides, Pareto-Seeds und Checkpoint-Resume
verwenden denselben Ablauf. PB9 prüft Quell-Commit, Schema und unveränderliche
Eingaben vor dem Start. Einreihen, Starten und Fortsetzen erfordern einen
PB9-Checkout ohne lokale Änderungen; gespeicherte Ergebnisse bleiben auch mit
lokalen Änderungen lesbar. Unter **Services** erscheint die Steuerung
**PB9 Optimize Queue**, die du dort wie die PB8-Queue stoppen und starten kannst. Ein API-Neustart stellt die Steuerung wieder her, während
unabhängige Optimizer weiterlaufen. Stop betrifft nur einen verifizierten Prozess.

Die geschätzten Coin-Kerzen pro Kandidat verwenden den aufgelösten
PB9-Trainingsdatenplan und die gemeinsame Kerzenzählung. Trainingsfenster zählen
einmal; Holdouts, Warmup und Iterations-/Populationsfaktoren zählen nicht mit.
Dynamische Coin-Auswahl oder unaufgelöste Datumswerte erscheinen als **—**.

## Native Startoptionen

Diese Felder stehen in den gemeinsamen Ausführungseinstellungen zur Verfügung;
Raw JSON muss dafür nicht bearbeitet werden:

- **Run scenarios** wählt Suite-Szenarien aus (`--scenarios`). Ohne Auswahl
  werden alle konfigurierten Szenarien ausgeführt. Die Liste folgt Änderungen
  an der Suite; entfernte Labels müssen vor Speichern oder Einreihen korrigiert werden.
- **Filter starting configs** prüft gespeicherte Seed-Metriken gegen die am Ende
  aktiven Limits (`--filter-starting-configs`). Metriken werden nicht neu berechnet.
- **Maximum starting configs** begrenzt die Anzahl der Seeds
  (`--compress-starting-configs N`). Leer bedeutet keine Komprimierung.
  Beide Seed-Optionen benötigen **seed_mode self oder path**, keinen frischen
  Lauf und kein Checkpoint-Resume. Für den Metrikfilter eignen sich gespeicherte
  Pareto-Ergebnisse.
- **Preparation manifest** importiert den abgeschlossenen JSON-Bericht von
  `passivbot download --for-optimize` mit höchstens 4 MiB. Ein Plan ist noch
  kein abgeschlossener Bericht. PBGui speichert den Bericht als privaten Anhang unter
  `data/opt_v9_manifests`; Config und Browser-Entwurf enthalten nur die Referenz.
  Jeder eingereihte Job friert den Inhalt unabhängig ein und übergibt ihn über `--preparation-manifest`. PB9 vergleicht seine tatsächlich
  vorbereiteten Eingaben vor der Auswertung mit diesem Bericht. Der Bericht
  ist optional; er ersetzt keine Kursdatenquelle und kopiert keine Kerzen.
  **Remove manifest** entfernt diese zusätzliche Anforderung.

Akzeptiert werden nur Optionen, die der installierte PB9-CLI anbietet. Die Queue
speichert die Auswahl. Checkpoint Resume übernimmt Szenarien und Manifest aus
den Ergebnis-Metadaten; Seed-Filter und Komprimierung gelten dabei nicht.
Alte Jobs ohne diese Optionen behalten ihr bisheriges Verhalten.

## Ergebnisse und Archive

Results/Paretos verwenden die gemeinsamen Metrik-, Kandidaten-, Seed-, 3D-,
Pareto-Dash- und Pareto-Explorer-Ansichten sowie native PB9-Backtest-Entwürfe.
Die Spalte **Strategy** zeigt das native `bot.portfolio.strategy_kind`. Der
Pareto Explorer ist auch über **PBv9 → Pareto Explorer** erreichbar; seine
Optimize-Presets verwenden native PB9-Metriken und behalten die Limits des Laufs. Archivexport und -import
nutzen vorhandene Transaktionen und Kollisionsdialoge mit dem PB9-Slot.
Native Regeln und unbekannte gespeicherte Felder bleiben erhalten.

**Queue Backtest** und **Queue Validation** reihen PB9-Kandidaten in die
PB9-Backtest-Queue ein. **Open Queue** öffnet dieselbe PB9-Queue; Status und
Autostart-Anzeige gehören zu PB9. Beim Einreihen bleiben die aktuelle
Optimize-Ansicht und die Auswahl erhalten.

Gespeicherte Pareto-Dateien enthalten neben der Config auch Auswertungsdaten.
PBGui verwendet den Ergebnisleser der installierten PB9-Version, um vor dem
Öffnen oder Einreihen eine gültige Config zu entnehmen. Laufzeitfelder wie
`backtest.coins` werden nicht als Config-Parameter weitergegeben. Das gespeicherte
Ergebnis, die ausgewählten Regeln und die Datumswerte bleiben unverändert.

### Sweep Cycles

PB9 hat keine Gain-Metrik. Sweep Cycles berechnet das Wachstum jedes Fensters
aus der fensterbezogenen `cagr_fraction_strategy_equity` und dem tatsächlich
ausgewerteten Zeitraum, den PB9 für dieses Fenster speichert
(`suite_metrics.sources`), nicht aus den geplanten Szenario-Daten. Ein vorzeitig
beendeter Lauf zählt daher mit seiner echten Dauer. Das Sweep-Preset setzt diese
Metrik als erstes Ziel. Änderst du die Ziele, behalte
`cagr_fraction_strategy_equity` als Ziel oder Limit, damit PB9 sie für jedes
Fenster aufzeichnet.

Sweep wird als nicht verfügbar angezeigt, wenn sich der Gewinn eines Fensters
nicht genau wiederherstellen lässt: Der ausgewertete Zeitraum fehlt, das Fenster
mittelt mehrere Börsen, die CAGR beträgt -100 % oder liegt so nah daran, dass das
Ergebnis ungenau wäre, oder die Werte überschreiten den Zahlenbereich.

### PB8-Migration

Auf der Seite **PBv8 → Optimize** öffnet **V9** neben einer gespeicherten Config
oder Pareto-Config den offiziellen PB8→PB9-Konverter. Prüfe die vorgeschlagenen
Änderungen und Warnungen und öffne das Ergebnis als ungespeicherten
PB9-Optimize-Entwurf. Die PB8-Quelle bleibt unverändert. PBGui-Laufeinstellungen
(Starting Configs, Fine-Tune, Polish und Ausführung) werden nicht übernommen.
Der Konverter unterstützt die PB8-Schemata v8.0.0, v8.1.0 und v8.6.0.

## Optimizer Settings

Queue Settings steuert Autostart, CPU-Zuteilung und die voreingestellten PBGui-Kursdaten. Run Settings enthält Iterationen, Backend, Seed, Fine-Tuning, Polishing und optionale Stagnationserkennung. Geerbte Werte bleiben ohne Änderung ungepinnt.

## Vast.ai

Konto, Marktplatz und Bedienung werden mit PB8 geteilt. PB9 benötigt ein eigenes,
fest gepinntes Worker-Image mit nativem GPU-Vertrag. Das veröffentlichte PB9-Image
wird unabhängig von PB8 über seinen unveränderlichen Digest ausgewählt. Vor der
Miete prüft PBGui, dass genau dieser Digest ohne GitHub-Zugangsdaten öffentlich
herunterladbar ist. Ein fehlendes oder ungeprüftes Image sperrt die Miete;
es gibt keinen Rückfall auf ein PB8-Image. Ein lokaler Image-Build veröffentlicht
nichts, mietet keine GPU und installiert nichts auf einem Host. Ein Test auf einer
echten GPU ist nötig, bevor der Cloud-Ablauf als verifiziert gilt.

GPU-Optimierung wurde mit synthetischen Daten auf einer RTX 3090 einschließlich
Upload und Ergebnisimport geprüft. PB9-Performance-Tuning verwendet über die
gemeinsamen Bedienelemente eine eigene Messung abgeschlossener GPU-Gruppen
(Protokoll 5). Jede Populations-/Batch-Probe läuft in einem neuen Prozess,
verwirft dessen erste Gruppe mit Kompilierung und verlangt mindestens fünf
Minuten vollständig abgeschlossener GPU-Messungen. Fehler, Vorfilter und reine
Cache-Treffer begründen keine Empfehlung. Die Rate umfasst auch Vorbereitung
und Ergebnisverarbeitung auf der CPU; sie ist keine reine CUDA-Kernel-Rate.
Angezeigt werden GPU-Zeilen, angeforderte Batch-Breite und abgeschlossene
Gruppen. Eine gemessene Dispatch-Arbeitsgrenze wird daraus nicht abgeleitet.
PB8 behält sein eigenes Messprotokoll.

Der veröffentlichte Kalibrierungs-Worker unterstützt Protokoll 5 und ist über
seinen geprüften öffentlichen Digest festgelegt. Small vergleicht Populationen
128 und 256. Native Referenztests verwenden 2 Coins / 3 Tage (Small), 3 Coins /
7 Tage (Medium) oder 3 Coins / 14 Tage (Large); PB8-Referenztests bleiben gleich.
Ein Test aus einem Job-Snapshot misst dessen eigenen Workload. Erst eine
abgeschlossene echte GPU-Messung ermöglicht die Profilannahme. Angenommene
Eine synthetische RTX-3090-Kalibrierung bestätigte Population 128 mit zehn
abgeschlossenen Gruppen über 316 Sekunden. Die 256er-Probe erreichte vor ihrem
vollen Messfenster die Sammelreserve und wurde verworfen; daraus folgt kein
Optimum für andere Workloads. PB9-Profile bleiben von PB8 getrennt. Automatische Anwendung erfordert denselben Workload, dieselbe Anzahl
zugeteilter CPU-Worker, Quellstand und
Worker sowie die gemessene GPU-Identität. Population und Batch müssen auf Auto
stehen und Tuning muss aktiviert sein. Ausdrückliche Werte bleiben maßgeblich;
die Dispatch-Arbeitsgrenze wird durch das Profil nicht ersetzt.

Native Cloud-Pakete enthalten dieselben Startoptionen und verwalteten Seeds.
Lokale Pfade werden durch geprüfte Paketpfade ersetzt. Exportiert werden die
Trainingseingaben der ausgewählten Szenarien. Lokale Checkpoints verwenden
lokales Resume; Cloud Requeue startet einen neuen Job. Der Worker ist auf den
öffentlichen Stand `bf7329b03` gepinnt. Vor der Vorbereitung muss die lokale PB9-Quelle
dazu passen; die bestehende Installation wird nicht automatisch aktualisiert.
Ein lokaler Vorbereitungsbericht kann Cloud-Eingaben mit abweichender
Datensatzidentität ablehnen. Die native Prüfung wird nicht umgangen.

Das separate AI-Loops-System ist für PB9 noch nicht aktiviert. Strategy Explorer
bleibt ein eigener Integrationsschritt.

Die native Performance History aktualisiert sich automatisch und bleibt nach dem Aufräumen der Cloud-Jobs lesbar. Konto, Marktplatz, Kalibrierung und Mietautomatik verwenden die gemeinsamen Vast-Komponenten.

Der PB9-Cloud-Worker startet über die offizielle native CLI und verwendet bereits
vorbereitete Konfigurationen wieder. Der Suchmodus wird nicht automatisch geändert:
Max kann die CPU-Vorbereitung beschleunigen, verändert aber den Suchablauf.
PB8-GPU-Vorbewertungen sind nicht direkt mit vollständigen PB9-GPU-Auswertungen
vergleichbar.

Der aktuelle Worker ist auf den offiziellen v9-Stand `bf7329b03` (einschließlich PR #2305) gepinnt und enthält die
Altersauflösung numerischer Hyperliquid-Markt-IDs. Offline-Alterstests und native
CLI/Cohort-Prüfungen sind bestanden. Für diese Revision gibt es keine neue
Leistungsmessung auf einer echten GPU.
