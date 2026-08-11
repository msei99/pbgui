# Plan: Passivbot v8.1 in PBGui integrieren

**Status: geplant.** Dieser Plan beschreibt ausschliesslich die Anpassungen von
PBGui an Passivbot v8.1.0. Die allgemeine PB8-Integration ist bereits umgesetzt
und bleibt in `docs/plans/passivbot-v8-integration.md` dokumentiert.

## Ziel

PBGui soll PB8.1.0-Configs ohne Informationsverlust laden, bearbeiten,
validieren und ausfuehren. Neue Exchanges und Marktidentitaeten muessen in allen
PB8-Workflows korrekt behandelt werden, ohne die PB7-Vertraege zu veraendern.

## Grundsaetze

1. Die installierte PB8-Runtime bleibt die Quelle fuer Schema, Templates,
   Defaults, Strategien, Bounds und kanonische Validierung.
2. PBGui hinterlegt keine Kopie von PB8.1-Defaults oder Optimize-Bounds.
3. Unbekannte und neue PB8-Felder muessen verlustfrei erhalten bleiben.
4. Exakte Marktidentitaeten werden nicht zu PB7-kompatiblen Kurznamen
   normalisiert.
5. Bitunix und WEEX werden nur fuer PB8 freigeschaltet und nicht in PB7s
   Exchange-Vertrag aufgenommen. Diese beiden Exchanges sind eine separate
   Lieferung mit niedriger Prioritaet und werden erst nach allen Config- und
   Editor-Anpassungen umgesetzt.
6. Bestehende PB8.0-Configs werden beim Laden durch den offiziellen PB8-Loader
   vollstaendig in eine kanonische v8.1.0-Arbeitskopie normalisiert. PBGui zeigt
   und bearbeitet ausschliesslich diese saubere v8.1.0-Form. Reines Oeffnen
   schreibt die Quelldatei nicht um; beim naechsten expliziten Save wird die
   kanonische Config im v8.1.0-Format gespeichert.

## PB8.1-Config-Vertrag

Das kanonische Schema ist `v8.1.0`. PB8 akzeptiert vorhandene `v8.0.0`-Configs
und normalisiert sie beim Laden in-memory vollstaendig auf v8.1.0. Diese
normalisierte Arbeitskopie ist die einzige Config-Form, die der Visual Editor
und Raw JSON erhalten. Erst ein explizites Save ersetzt die Quelldatei atomar
durch die erneut vorbereitete und validierte v8.1.0-Config. Neue kanonische
Felder sind:

```text
live.enable_forager_ws_candles = true
live.exchange_symbol_unavailable_cooldown_hours = 6.0
live.forager_ws_candle_rest_audit_minutes = 30
live.order_replacement_churn_gate_activation_count = 10
live.order_replacement_churn_gate_market_dist_pct = 0.005
live.order_replacement_churn_gate_stability_minutes = 2.0
live.order_replacement_churn_gate_window_minutes = 10.0
live.startup_phase_budgets = {}
optimize.objective_scenario = null
optimize.scoring[].scenario
optimize.scoring[].aggregate
optimize.limits[].scenario
```

`live.initial_entry_exec_max_market_dist_pct` wurde entfernt. PB8 migriert den
alten Wert auf die neuen `live.order_replacement_churn_gate_*`-Felder. Positive
Werte aktivieren den neuen Churn Gate; ein deaktivierter alter Wert wird als
`order_replacement_churn_gate_activation_count = 0` erhalten.

Zahlreiche Long-Strategie-Defaults und Optimize-Bounds wurden geaendert. Fuer
PBGui entsteht daraus keine statische Feldliste: New Config, Prepare, Save und
Queue verwenden weiterhin die Metadaten und Normalisierung der installierten
PB8-Runtime.

## Lieferung A: Verlustfreier Optimize-Editor

### Problem

Der gemeinsame Optimize-Editor rekonstruiert `optimize.scoring` und
`optimize.limits`. Dabei verwirft er aktuell:

```text
optimize.scoring[].scenario
optimize.scoring[].aggregate
optimize.limits[].scenario
```

Eine gueltige PB8.1-Config kann dadurch bereits beim Oeffnen und Speichern ihre
Optimierungssemantik aendern.

### Umsetzung

1. `normalizeScoringEntry()` erhaelt `scenario` und `aggregate`.
2. `normalizeLimitEntry()` erhaelt `scenario`.
3. Der Scoring-Editor bietet pro Objective drei Scenario-Zustaende:
   - Feld fehlt: `optimize.objective_scenario` erben,
   - `null`: Suite-Aggregation,
   - String: benanntes Suite-Szenario.
4. `aggregate` bietet `mean`, `min`, `max`, `std` und `median`.
5. Bei einem benannten Scoring-Szenario ist `aggregate` nicht erlaubt.
6. Bei einem benannten Limit-Szenario ist `stat` nicht erlaubt.
7. Mehrere Limits fuer dieselbe Metrik bleiben erlaubt, wenn sie sich durch
   Scenario oder Aggregationsbasis unterscheiden.
8. `optimize.objective_scenario` wird strukturiert angeboten; der bestehende
   Additional-Parameters-Roundtrip bleibt als Sicherheitsnetz erhalten.

### Betroffene Dateien

```text
frontend/v7_optimize.html
frontend/js/optimize_editor_adapter.js
api/optimize_v8.py
tests/ui/test_optimize_v8_frontend_logic.py
tests/test_optimize_v8_api.py
```

### Exit-Kriterien

- Eine PB8.1-Config mit allen Scenario-Kombinationen bleibt nach Load/Save
  semantisch und strukturell unveraendert.
- Ungueltige Kombinationen werden vor Save erklaert und von PB8 validiert.
- PB7 Optimize bleibt unveraendert.

## Lieferung B: Verlustfreie Marktidentitaeten

### Upstream-Status

Das gemeldete CATUSDT-Problem ist in PB8.1.0 behoben. Unser PR #1459 wurde nicht
direkt gemergt, sondern durch den allgemeineren PR #1460 ersetzt. PR #1460 wurde
am 2026-08-09 gemergt und ist in Tag `v8.1.0` enthalten. Er uebernimmt die
relevanten Regressionstests und behandelt Bitget und Binance fail-closed:

- exakte CCXT-Symbole und native IDs haben Vorrang,
- `CAT` und `1000CAT` bleiben getrennte Marktidentitaeten,
- mehrdeutige Aliase werden mit ihren Kandidaten abgelehnt,
- `exchange::<native-id>` bietet eine explizit exchange-spezifische Identitaet.

Die gemergte Loesung unterscheidet sich bewusst von unserem fokussierten PR.
Unser PR gab bei gleichzeitig vorhandenem `CAT` und `1000CAT` dem nackten
`CAT`-Alias den unskalierten Markt und `1000CAT` den skalierten Markt. PB8.1
behandelt das nackte `CAT` stattdessen als mehrdeutigen Convenience-Alias und
bricht fail-closed ab. Der Benutzer muss einen exakten Identifier wie
`CATUSDT`, `1000CATUSDT`, das vollstaendige CCXT-Symbol oder beispielsweise
`bitget::1000CATUSDT` verwenden. Existiert auf einer Exchange nur der
Multiplikatormarkt, bleibt der historische Kurzalias aus Kompatibilitaetsgruenden
gueltig.

PR #1460 ist damit strenger und allgemeiner als unser PR: Er trennt exakte und
bequeme Aliase fuer alle Exchanges, ersetzt die order-abhaengige Auswahl des
ersten Kandidaten durch explizite Ambiguity-Fehler, persistiert pro Exchange
Ambiguity-Tombstones gegen unsichere partielle Cache-Refreshes und propagiert
die Identitaet durch Live-Coin-Listen, Overrides, Fills, Backtests, Suites,
HLCV-Caches und Inception-Daten. Unser PR loeste den konkreten
Multiplikator-Konflikt und sortierte Kandidaten deterministisch, bot aber diesen
durchgaengigen Fail-Closed-Vertrag nicht.

### PBGui-Problem

PBGui soll seine einfache Kurznamenlogik als Hauptfall behalten. Eindeutige
Coins bleiben daher in GUI und gespeicherter Config weiterhin `BTC`, `SHIB`,
`CAT` usw. Nur wenn das aktuelle Exchange-Mapping fuer einen Kurznamen
tatsaechlich mehrere wirtschaftlich unterschiedliche aktive Maerkte enthaelt,
ist der Kurzname fuer PB8 nicht mehr ausreichend.

PBGui erkennt Multiplikator-Kollisionen bereits mit
`disambiguate_multiplier_market_coins()`. Diese vorhandene Information soll
genutzt werden; PB8s allgemeiner Resolver wird nicht in PBGui nachgebaut.

### Umsetzung

1. PBGui verwendet weiterhin den vorhandenen kurzen `coin`-Wert als sichtbare
   Bezeichnung und als gespeicherten Config-Wert, solange er im relevanten
   Exchange-Mapping eindeutig ist.
2. Nur bei einer erkannten Multiplikator-Kollision bekommt der Mapping-Eintrag
   zusaetzlich einen exakten PB8-Config-Wert. Beispiel:

   ```text
   GUI CAT      -> Config CATUSDT
   GUI 1000CAT  -> Config 1000CATUSDT
   GUI SHIB     -> Config SHIB, solange kein separater SHIB-Markt kollidiert
   ```

3. Bei einer einzelnen Live-Exchange reicht die native ID. Wenn eine
   Multi-Exchange-Config eine Venue-Bindung benoetigt, wird nur fuer diesen
   Kollisionsfall `exchange::<native-id>` gespeichert.
4. Visual Editor und Picker zeigen weiterhin den Kurznamen. Der zugrunde
   liegende Wert ist nur im Kollisionsfall die exakte ID; Raw JSON zeigt den
   tatsaechlich gespeicherten PB8-Wert.
5. Beim Laden wird eine bekannte exakte Kollisions-ID fuer die visuelle Anzeige
   wieder ihrem Kurznamen zugeordnet. Der gespeicherte Wert bleibt exakt und
   wird beim erneuten Save nicht auf den mehrdeutigen Kurznamen reduziert.
6. Die Abbildung wird zentral aus den vorhandenen PBCoinData-Mapping-Zeilen
   erzeugt und von Coin-Listen, Coin-Sources, Market-Settings-Sources,
   Suite-Szenarien und Coin Overrides wiederverwendet.
7. Unbekannte manuell eingetragene exakte IDs werden nicht umgeschrieben. Die
   abschliessende Marktvalidierung bleibt Aufgabe von PB8.
8. PB7 und die allgemeine PBCoinData-Normalisierung bleiben unveraendert.

### Exit-Kriterien

- Eindeutige Coins bleiben in GUI und Config als Kurzname gespeichert.
- `CAT` und `1000CAT` werden in der GUI kurz angezeigt, bei einer echten
  Binance- oder Bitget-Kollision aber als getrennte exakte IDs gespeichert und
  ausgefuehrt.
- Entfernt die Exchange einen der kollidierenden Maerkte, bleibt fuer neue
  Auswahlen wieder die normale Kurznamenlogik aktiv.
- Bekannte exakte Kollisions-IDs bestehen einen Load/Save-Roundtrip und werden
  im Visual Editor mit ihrem Kurznamen beschriftet.
- Manuell eingetragene exakte IDs werden nicht beschaedigt.
- PB7s bestehende Multiplikator-Normalisierung bleibt unveraendert.

## Lieferung C: Typisierte Coin Overrides

### Neuer Vertrag

PB8.1 behandelt Coin Overrides als explizite, typisierte Sparse Patches. Datei-
Overrides werden zuerst angewendet, Inline-Werte danach. Explizite Werte wie
`false`, `0` oder ein Wert gleich dem globalen Default duerfen nicht als
"nicht gesetzt" verloren gehen.

Neu pro Seite erlaubt sind:

```text
bot.<side>.risk.entry_cooldown_minutes
bot.<side>.unstuck.ema_gating_enabled
```

Wenn global `live.hsl_signal_mode = "coin"` gesetzt ist, ist zusaetzlich die
vollstaendige HSL-Gruppe pro Seite erlaubt. Ausserhalb dieses Modus duerfen
Inline-HSL-Patches nicht angeboten werden. Nicht mehr pro Coin erlaubt ist:

```text
bot.<side>.risk.we_excess_allowance_mode
```

### Umsetzung

1. `pb8_config_helper.py` laedt PB8s offizielle Allowed-Modifications-Metadaten.
2. Die Override-Metadatenroute bekommt den effektiven globalen HSL-Modus.
3. Der Editor rendert nur die von PB8 gemeldeten Felder und Typen.
4. Sparse Semantik wird erhalten; es findet keine Hydrierung zu einer
   vollstaendigen Side-Config mit anschliessendem unsicherem Diff statt.
5. Fehlende, unlesbare, falsche oder strategy-inkompatible Override-Dateien
   werden als PB8-Validierungsfehler angezeigt.
6. Ein Wechsel des globalen HSL-Modus aktualisiert die erlaubten Override-Felder
   ohne vorhandene ungueltige Daten stillschweigend zu loeschen.

### Betroffene Dateien

```text
pb8_config_helper.py
pb8_config.py
api/v8_instances.py
api/backtest_v8.py
api/optimize_v8.py
frontend/js/coin_overrides_editor.js
tests/test_pb8_config.py
tests/test_backtest_v8_api.py
tests/test_v8_instances_api.py
```

### Exit-Kriterien

- Die sichtbaren Felder entsprechen fuer jeden HSL-Modus exakt PB8s Allowlist.
- Explizite `false`-, Null- und Default-Werte bleiben erhalten.
- Unsupported Fields werden vor dem Speichern angezeigt und von PB8
  fail-closed validiert.

## Lieferung D: Neue Live-Felder und Migration

Die neuen skalaren Live-Felder werden bereits ueber Runtime-Template,
Metadaten, Raw JSON und kanonisches Prepare transportiert. Fuer den normalen
Run-Workflow werden sie passend gruppiert:

- Forager WebSocket Candles:
  `enable_forager_ws_candles`, `forager_ws_candle_rest_audit_minutes`.
- Exchange Cooldown:
  `exchange_symbol_unavailable_cooldown_hours`.
- Order Replacement Churn Gate: die vier
  `order_replacement_churn_gate_*`-Felder.
- Diagnostic Startup Budgets: strukturierte Phase-Werte unter
  `startup_phase_budgets`.

`startup_phase_budgets` wird als Expert-/Diagnosefeld gekennzeichnet; die Werte
steuern Reporting und sind kein Trading Gate. PBGui zeigt nach der Normalisierung
einer v8.0-Config sofort eine vollstaendige v8.1.0-GUI, zeigt den entfernten
Initial-Entry-Wert nicht mehr an und uebernimmt die von PB8 erzeugten Churn-Gate-
Felder. Beim Save wird genau diese kanonische v8.1.0-Struktur gespeichert.

### Exit-Kriterien

- Alle neuen Felder koennen im Visual Editor oder klar gekennzeichnet unter
  Additional Parameters editiert werden.
- Alte v8.0-Configs werden ohne PBGui-eigene Migration korrekt vorbereitet und
  unmittelbar als saubere v8.1.0-GUI dargestellt.
- Oeffnen veraendert die Quelldatei nicht.
- Save validiert die Arbeitskopie erneut mit PB8 und speichert sie atomar als
  kanonische v8.1.0-Config.

## Lieferung E: Bitunix und WEEX

**Prioritaet: niedrig, Umsetzung zuletzt.** Diese Lieferung blockiert die
Config- und Editor-Kompatibilitaet mit PB8.1.0 nicht. Wenn sie umgesetzt wird,
umfasst sie neben Credentials und PB8 Run auch die vollstaendige Einbindung in
das PBGui Dashboard.

### Umfang

PB8.1 fuegt Live-USDT-Perpetual-Support hinzu fuer:

```text
bitunix: key + secret
weex: key + secret + passphrase
```

### Umsetzung

1. `Exchanges` erhaelt Bitunix und WEEX als allgemeine Credential-Exchanges.
2. `Passphrase` erhaelt WEEX.
3. `V7` bleibt unveraendert.
4. API-Key-Editor, Validierung, Reveal-Vertrag und Cluster Credential Sync
   unterstuetzen die neuen PB8-Accounts ohne Secrets in Listen, URLs oder Logs.
5. PB8 Run zeigt passende User beider Exchanges.
6. PB8 Backtest, Optimize, Suite Editor, Coin Picker und Market-Status beziehen
   ihre unterstuetzten Exchanges aus PB8-Metadaten statt aus statischen
   PB7-Listen.
7. Das Dashboard listet Bitunix- und WEEX-PB8-Instanzen und unterstuetzt deren
   Top-, Balance-, Income-, PnL-, ADG-, PPL-, Positions- und Orders-Widgets.
8. Dashboard-Live-Daten verwenden den verifizierten WebSocket- oder Polling-
   Vertrag der jeweiligen Exchange und geben Tasks, Clients und Streams beim
   letzten Consumer deterministisch frei.
9. Dashboard-Aktionen wie Position Close werden erst freigeschaltet, wenn
   Position-Side-, Hedge-Mode-, Reduce-Only-, Amount- und Order-Parameter fuer
   die jeweilige Exchange getestet sind. Bis dahin bleibt die Aktion sichtbar
   erklaert deaktiviert statt generische CCXT-Parameter zu raten.
10. Dashboard-Symbole und Orders erhalten PB8s exakte Marktidentitaeten; auch
   dort duerfen `CAT`, `1000CAT` und native IDs nicht kollidieren.
11. PBGui Market Data bleibt auf seinen tatsaechlich implementierten Exchange-
   Quellen begrenzt; eine PB8-Live-Exchange-Freigabe behauptet nicht automatisch
   PBGui-eigene historische Datenunterstuetzung.

### Betroffene Dateien

```text
Exchange.py
api/api_keys.py
api/v8_instances.py
api/editor_market_data.py
api/dashboard.py
frontend/api_keys_editor.html
frontend/v7_backtest.html
frontend/v7_optimize.html
frontend/js/suite_editor.js
frontend/dashboard_top.html
frontend/dashboard_balance.html
frontend/dashboard_income.html
frontend/dashboard_pnl.html
frontend/dashboard_adg.html
frontend/dashboard_ppl.html
frontend/dashboard_positions.html
frontend/dashboard_orders.html
cluster_sync_command.py
tests/test_api_keys_api.py
tests/test_v8_instances_api.py
tests/test_dashboard_short_logic.py
tests/ui/test_api_keys_frontend_logic.py
```

### Exit-Kriterien

- Bitunix- und WEEX-Credentials koennen sicher angelegt und PB8-Usern
  zugeordnet werden.
- WEEX erzwingt eine Passphrase; Bitunix nicht.
- PB7 bietet beide Exchanges nicht an.
- Alle PB8-Exchange-Auswahlen sind konsistent und runtimebasiert.
- Dashboard-Daten fuer Balance, Income, PnL, Positionen und Orders werden korrekt
  aktualisiert und beenden alle zugehoerigen Ressourcen deterministisch.
- Dashboard-Close-Aktionen sind exchange-spezifisch getestet oder sicher
  deaktiviert.

## Bereits kompatibel

- `pb8_config_helper.py` laedt Templates, Defaults und kanonische Configs aus
  der installierten Runtime.
- `backtest.volume_normalization` wird im Backtest-Editor strukturiert
  unterstuetzt.
- Optimize erhaelt `backtest.volume_normalization` bereits ueber den Raw-/
  Roundtrip-Pfad.
- Die drei Strategien `trailing_martingale`, `ema_anchor` und
  `trailing_grid_v7` bleiben bestehen; es gibt keine neuen Strategiepfade.

## Tests

### Config und Roundtrip

- v8.0 wird beim Laden durch PB8 in-memory vollstaendig nach v8.1 normalisiert,
  ohne die Quelldatei zu veraendern.
- Der Editor erhaelt nach dem Laden ausschliesslich die kanonische
  v8.1.0-Arbeitskopie.
- Der erste explizite Save schreibt die Config atomar im v8.1.0-Format.
- Alle neuen Live-Felder bleiben nach Load, Prepare und Save erhalten.
- Alte und neue Churn-Gate-Felder werden nicht widerspruechlich gespeichert.
- Runtime-Defaults und Bounds werden nicht durch PBGui-Werte ueberschrieben.

### Optimize

- Scoring Scenario fehlt, ist `null` oder benennt ein Szenario.
- Scoring Aggregate unterstuetzt alle fuenf Werte.
- Limit Scenario und `stat` sind gegenseitig exklusiv.
- Load/Save verwirft keine unbekannten Entry-Felder.

### Marktidentitaeten

- Eindeutige Coins bleiben in GUI und gespeicherter Config kurz.
- Binance und Bitget zeigen `CAT` und `1000CAT` kurz an, speichern bei
  gleichzeitiger Listung aber die jeweils exakte native ID.
- Beim Laden exakter Kollisions-IDs bleibt der Config-Wert erhalten, waehrend
  der Visual Editor wieder den Kurznamen zeigt.
- Manuelle exakte CCXT-Symbole, native IDs und Exchange-Scopes bleiben erhalten.
- Coin Overrides und Suite-Szenarien fuehren kollidierende Maerkte nicht
  zusammen.

### Exchanges

- Bitunix verwendet Key und Secret.
- WEEX verwendet Key, Secret und Passphrase.
- Beide sind nur in PB8-Workflows sichtbar.
- Credential-Antworten und Logs enthalten keine Secrets.
- Dashboard-Widgets zeigen Instanzen, Balance, Income, PnL, Positionen und
  Orders fuer beide Exchanges mit aktuellen Daten.
- Dashboard-WebSocket- und Polling-Fallbacks werden ohne verwaiste Tasks oder
  Clients gestartet, ersetzt und beendet.
- Position-Close ist pro Exchange mit Hedge- und One-Way-Mode getestet oder bis
  zur vollstaendigen Verifikation sicher deaktiviert.

### Nichtregression

- PB7 Run, Backtest, Optimize, Coin Picker und API Keys bleiben unveraendert.
- PB8.0-Configs bleiben lesbar.
- Archive, Drafts, Queue-Snapshots und Cluster Sync erhalten Config-Version und
  Marktidentitaeten.
- Vollstaendige Offline-Test-Suite ist gruen.

## Guides

Folgende Guides werden in Englisch und Deutsch aktualisiert:

```text
docs/help/20_api_keys.md
docs/help_de/20_api_keys.md
docs/help/42_pbv8_backtest.md
docs/help_de/42_pbv8_backtest.md
docs/help/43_pbv8_optimize.md
docs/help_de/43_pbv8_optimize.md
docs/help/44_pbv8_run.md
docs/help_de/44_pbv8_run.md
docs/help/33_dashboard.md
docs/help_de/33_dashboard.md
```

Die Guides erklaeren insbesondere die neue CAT/1000CAT-Semantik, exakte
Marktidentitaeten, scenario-spezifische Optimize-Felder und den Unterschied
zwischen PB8-Exchange-Support und PBGui-eigenen Market-Data-Quellen.

## Umsetzungsreihenfolge

1. Optimize-Roundtrip korrigieren, damit PB8.1-Configs nicht beschaedigt werden.
2. Die bestehende Kurznamenlogik nur bei echten PB8-Marktkollisionen durch einen
   exakten gespeicherten Identifier ergaenzen.
3. Typisierte Coin-Override-Metadaten anbinden.
4. Neue Live-Felder gruppieren und Migration UX pruefen.
5. Guides, fokussierte Regressionstests und komplette Offline-Suite fuer die
   v8.1-Config- und Editor-Kompatibilitaet abschliessen.
6. Bitunix und WEEX zuletzt als separate, niedrig priorisierte Lieferung
   einschliesslich Dashboard-Unterstuetzung integrieren und erneut die
   betroffenen Guides sowie die komplette Offline-Suite pruefen.

## Gesamte Exit-Kriterien

- PBGui kann jede kanonische PB8.1-Config ohne Informationsverlust bearbeiten.
- Kein Visual Editor verwirft PB8.1-Felder beim Speichern.
- Kurznamen bleiben der Standard; nur echte CAT/1000CAT-artige Kollisionen
  werden in allen PB8-Flows mit getrennten exakten Config-Werten gespeichert.
- Coin Overrides entsprechen PB8s typisiertem und HSL-abhaengigem Vertrag.
- Wenn die niedrig priorisierte Exchange-Lieferung umgesetzt wird, sind Bitunix
  und WEEX vollstaendig in PB8 einschliesslich Dashboard, aber nicht in PB7
  integriert.
- PB8.0-Configs werden weiterhin akzeptiert und durch PB8 normalisiert.
- EN-/DE-Guides und Tests sind vollstaendig; die Offline-Suite ist gruen.

## Referenzen

- Passivbot v8.1.0 Release:
  `https://github.com/enarjord/passivbot/releases/tag/v8.1.0`
- Passivbot v8.1.0 Changelog:
  `https://github.com/enarjord/passivbot/blob/v8.1.0/CHANGELOG.md`
- Gemeldetes CATUSDT-Problem:
  `https://github.com/enarjord/passivbot/issues/1456`
- Unser abgeloester PR:
  `https://github.com/enarjord/passivbot/pull/1459`
- Gemergter allgemeiner Market-Identifier-Fix:
  `https://github.com/enarjord/passivbot/pull/1460`
- Allgemeiner PB8-Integrationsplan:
  `docs/plans/passivbot-v8-integration.md`
