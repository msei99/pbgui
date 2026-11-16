# Passivbot v9: gemeinsamer Entwicklungsplan

Stand: 16. November 2026. Native PB9 Run- und Backtest-Editoren verwenden
inzwischen die gemeinsamen PB8-Seiten und Bedienelemente. Lokaler Run,
Backtest-Queue mit optionalem Autostart, PB8-Migration, Ergebnisse und versionsübergreifendes
Compare sind angebunden. Additional Parameters ergänzt unbekannte Felder;
die Balance-Werkzeuge verwenden denselben Rechner wie PB8.

Ein begrenztes Read-only-Review mit Fable 5.2 / medium hat Filterverlust,
Stop-Status, Ergebnis-Löschoption, Restart-Provenienz, doppelte Feldautoren,
Doppel-Enqueue und nicht entfernte Workspaces identifiziert. Diese Befunde
werden mit isolierten API-/Browserprüfungen korrigiert. Der gemeldete fehlende
Runner war ein Fehler des Review-Abzugs; die Datei ist vorhanden und versioniert.

OHLCV Readiness und 1st/All verwenden nun die gemeinsame Oberfläche und
den Lookup-Controller mit einem nativen PB9-Adapter. Prüfungen laufen mit
privaten Metadaten-Workspaces, direkter Candle-Quelle und PB9-Transport.

Bewusst noch offen: native PB9-Anbindung für Strategy Explorer, außerdem Optimizer- und Remote-Abläufe. Automatische Archiv-Retests sind jetzt angebunden.
Diese Funktionen dürfen nicht durch die PB7/PB8-Ausführungsengine als PB9
angeboten werden. Die restliche historische Schrittfolge unten dokumentiert
die bisherige Einführung und ist keine Aussage über den aktuellen UI-Umfang.

## Vorgehen

Wir entwickeln in kleinen, abgeschlossenen Schritten direkt auf `main`.
Codex und Claude wechseln sich bei Umsetzung und unabhängigem Review ab.
Claude-Reviews können direkt über die lokale CLI mit Fable 5.2 und Effort
medium erfolgen. Claudes Rückmeldung wird im Chat angezeigt, zusätzlich zur
Zusammenfassung von Codex; manuelles Kopieren ist nicht erforderlich.
Pro Aufgabe bearbeitet nur einer dieselben Dateien. Normale PBGui-Arbeit
und Veröffentlichungen stabiler Zwischenstände bleiben möglich.
Jede Aufgabe erhält vorab einen begrenzten Umfang und ein Abnahmekriterium.
Unfertige v9-Funktionen bleiben ausgeblendet oder gesperrt.
Commits und Pushes benötigen weiterhin die ausdrückliche Zustimmung des Nutzers.
Nach einem freigegebenen Commit geht Codex direkt zum nächsten geplanten Schritt
über, ohne eine weitere Aufforderung abzuwarten. Am 15. November 2026 hat der Nutzer die weiteren lokalen Schritte einschließlich
Claude-Reviews und Commits bis zu einem testbaren v9-Editor ausdrücklich freigegeben.
Danach brauchen weitere Commits erneut Freigabe; Pushes, Installationen und
Remote-Aktionen bleiben separat freigabepflichtig.

## Verbindliche Grenzen

- PB7/PB8 bleiben funktionsfähig; gemeinsame Änderungen werden für die
  betroffenen Versionen geprüft.
- V9 bekommt einen eigenen Checkout und eine eigene Python-Umgebung sowie
  getrennte Konfigurationen, Jobs und Ergebnisse. Gemeinsame Komponenten
  werden weiterverwendet, Versionsunterschiede über Adapter behandelt.
- Die Versionserkennung unterscheidet Runtime, Git-Commit und Config-Schema.
  Ein Branchname oder eine Paketversion allein beweist keine Kompatibilität.
- Bestehende Konfigurationen werden nur ausdrücklich in neue Dateien migriert.
  Alte Optimizer-Fitnesswerte und Checkpoints gelten nicht als v9-Nachweise.
- Installationen, Bot-Codeänderungen, Live- und Remote-Aktionen sind durch
  diesen Plan nicht freigegeben. Die Umsetzung folgt den Repository-Regeln.

## Erste Befunde

Untersuchte Basis: PBGui `e582dd9f`; lokal vorhandener Passivbot-Ref
`origin/v9` bei `ad9d6ede120589c7ea213bfd93ba0726bd840c69`
(Commit-Datum 9. November 2026). Dieser Ref wurde nicht aktualisiert;
er ist kein Nachweis des aktuellen GitHub-Branchstands.

- `pb8_config.py` und `pb8_config_helper.py` bieten bereits eine getrennte
  Konfigurationsanbindung und Metadaten aus der ausgewählten Laufzeit.
- `_load_pb8_modules()` in `pb8_config_helper.py` importiert
  `config.migrations.trailing_grid_v7`. Dieses Modul fehlt im untersuchten
  v9-Baum. Ein einfacher Wechsel des PB8-Checkouts auf v9 reicht nicht.
- `frontend/js/pb8_parameter_help.js` verwendet noch
  `live.hsl_signal_mode`; v9 dokumentiert `bot.portfolio.hsl.scope`.
- Die v9-Migrationsanleitung verlangt eine explizite Konvertierung und
  Entscheidungen zu geänderten Risikoparametern. V9 ist mehr als eine
  Umbenennung von Feldern.

Upstream-Grundlagen am untersuchten Commit:
[Migration](https://github.com/enarjord/passivbot/blob/ad9d6ede120589c7ea213bfd93ba0726bd840c69/docs/v8_to_v9_migration.md),
[Schema](https://github.com/enarjord/passivbot/blob/ad9d6ede120589c7ea213bfd93ba0726bd840c69/src/config/schema.py).

## Schritte und Abnahme

### Ergänzungen aus Claudes Bestandsaufnahme

- PB8 und v9 melden beide `8.2.0.dev0`; die Generation muss aus dem
  Config-Schema folgen. `verify_passivbot_major()` prüft bisher nur die
  Paketversion, `config_version_info()` behandelt Nicht-v8 als PB7.
- Die vorhandene Konfigurationsanbindung ist ein Singleton. Helper-Prozess
  und Caches brauchen getrennte Besitzer pro Runtime. Ein Prozess darf keine
  Python-Module unterschiedlicher Bot-Checkouts mischen; die geerbte
  Python-Umgebung und Suchpfade müssen bei der Anbindung geprüft werden.
- Neben der v7-Migration fehlen am v9-Ref importierte Metrik-Konstanten,
  der bisherige HSL-Migrationspfad und GPU-Schnittstellen. Alte HSL-Pfade
  und Werte betreffen Backend, Frontend und Optimizer.
- Weitere Schema-Prüfungen, INI-Schlüssel und Datenwurzeln sind PB8-spezifisch.
  Vor dem Verdrahten muss die Reihenfolge der Installer-Prüfung gegen den
  Checkout geprüft werden, damit ein unpassender Branch den PB8-Slot nicht
  bereits vor der Ablehnung verändert.

### Vereinbarte Reihenfolge

1. **Bestandsaufnahme unabhängig prüfen.** Claude prüft die Befunde und
   ergänzt die betroffenen Schnittstellen für Installation, Konfiguration,
   Editor und Backtests. Ergebnis: priorisierte Lückenliste und gemeinsam
   geprüfter Umfang der ersten Codeänderung.
2. **Laufzeiterkennung und Adapter vorbereiten.** Zunächst nur lokale
   Erkennung und Auswahl einer getrennten Runtime entwerfen und umsetzen.
   Abnahme: Versionen werden zuverlässig unterschieden; fehlende oder
   inkompatible Laufzeiten erzeugen eine klare Meldung; PB8 bleibt intakt.
3. **Konfiguration und Editor anbinden.** Erst neue v9-Configs, danach
   ausdrückliche Migration als Kopie. Abnahme: Erstellen, Validieren,
   Speichern und erneutes Laden ohne unbemerkten Parameterverlust.
4. **Lokale Backtests integrieren.** Queue, Ausführung und Ergebnisse
   zusammenführen. Abnahme: vollständiger Ablauf mit isolierten Testdaten
   und nachvollziehbarer Runtime-/Schema-/Commit-Zuordnung.
   Das ist der erste angestrebte nutzbare v9-Release-Meilenstein.
5. **Optimierung ergänzen.** Zuerst lokal, anschließend GPU/Vast und AI Loops
   separat prüfen. Abnahme jeweils mit passenden v9-Komponenten und ohne
   Wiederverwendung inkompatibler alter Optimizer-Zustände.
6. **Live und Remote ergänzen.** Prozesssteuerung und Überwachung prüfen;
   parallelen Betrieb derselben Börsenkonten unter PB8/PB9 verhindern.
   Praktische Live-/Remote-Abnahmen benötigen gesonderte Freigaben.

Für jeden Schritt: Umsetzung, unabhängiges Review, Korrekturen, passende
Tests, Changelog und bei Bedarf Serial-Bump sowie EN/DE-Guides.
Die Regeln zur Auswahl und Aufzeichnung vollständiger Testsuiten gelten weiter.

## Umgesetzte Zwischenstände

1. **Quellklassifikation, Commit `32f00ed5`.** `classify_passivbot_source()`
   unterscheidet Schema-Majors, liest AST statt Bot-Code auszuführen und meldet
   fehlende Metadaten unabhängig von der Generation. Absolute Pfade sind Pflicht.
   Git liest ohne geerbte Repository-/Config-Overrides; nur passende Metadaten
   erhalten einen Commit, mit ausschließlich CRLF/LF-neutralem Vergleich.
   Von Claude unabhängig geprüft; damalige fokussierte Prüfung: 128 Tests grün.
2. **Guard-Schutz für den PB8-Slot, Commit `0b980aad`.** Nur für
   `expected_major == 8` prüft `verify_passivbot_major()` zusätzlich das Schema
   aus dem Arbeitsbaum oder gewählten Git-Ref. V9/ungültige Schemas scheitern
   mit `RuntimeError` vor Stop/Checkout. PB7 verlangt weiterhin kein Schema.
   Rückgabewerte und CLI-Marker bleiben gleich; Zielmajor 9 bleibt gesperrt.
   Gemeinsame reine Helfer lesen Literale mit 256-KiB-Limit und UTF-8-BOM und
   ordnen Schema-Majors zu. Der volle Klassifikator wird nicht im Guard benutzt.
3. **Getrennte PB9-Erkennung, Commit `27dc7d55`.** `pb9_runtime.py` prüft
   Quellschema, Trennung von PB7/PB8-Quellbäumen, eigenes Env-Verzeichnis,
   Interpreter-/CLI-Dateien und Rust-Präsenz. Python 3.12/3.14 wird ausschließlich
   aus `pyvenv.cfg` erkannt. Interpreter-Symlinkziele werden nicht zum Vergleich
   von Env-Verzeichnissen verwendet; PBGui selbst zählt als reserviertes Env.
   Das Sammelflag heißt `detected`, nicht `ready`. ABI, CLI-Verhalten und
   Rust-/Quellübereinstimmung werden nicht durch Dateipräsenz bewiesen.

   Ein fehlender Commit wird bei gültigen Quelldeklarationen nur gewarnt und
   separat gemeldet; Schema-/Lesefehler blockieren die Erkennung. Der optionale
   Rust-Stempel wird nur als `rust_stamp_present` gemeldet. Eine verbindliche
   Stempelprüfung gehört zum späteren Installationsschritt.

   `pb9_runtime_status()` lädt erst bei Aufruf den bestehenden INI-Leser und
   fragt in einem Snapshot nur die sechs Pfadschlüssel ab. `pb9dir`/`pb9venv`
   werden noch nicht registriert oder geschrieben. Es gibt keine Aufrufer,
   UI-/Installer-Anbindung oder Änderungen an PB8-Funktionen.
4. **Sichere Pfadspeicherung, Commit `e5a84ea0`.** `save_pb9_runtime_paths()`
   prüft die neue Konfiguration gegen die PB7/PB8-Werte desselben gesperrten
   INI-Parsers. Nur ein erkanntes Layout wird als Paar gespeichert; Fehler
   brechen vor der atomaren Veröffentlichung mit stabilen Fehlercodes ab.
   Zwei explizit leere Werte entfernen beide Schlüssel. Fremde Einstellungen
   bleiben erhalten. Der Interpreter-Symlink wird nicht auf sein Ziel reduziert.

   Die Registry ordnet beide Schlüssel `PB9Runtime` und `next_operation` zu;
   `welcome_setup`, API-Routen und UI bleiben unverändert. Die Registry wird
   beim API-Start importiert, daher Serial-Bump von 3698 auf 3699.
   Die umgekehrte Prüfung bei späteren PB7/PB8-Pfadänderungen ist noch offen.
   Ein zukünftiger async-Aufrufer muss die synchrone Prüfung auslagern.

   Validierung: `test_pb9_runtime`, `test_ini_apply_metadata`,
   `test_ini_transactions`, `test_ini_writer_policy`, `test_welcome_pb8` und
   `test_pb8_installation`: **165 passed in 22.53s**. Der bestehende
   Restart-Status-Test ist jetzt gegen echte Runtime-/Auth-Abfragen isoliert
   und verwendet ein Inline-Doppel für den Thread-Offload. Zwei vorherige
   fokussierte Läufe wurden wegen eines Hängers in diesem Test abgebrochen.
   Claude Fable 5.2 / medium hat den Stand rein lesend als commit-reif geprüft;
   keine Tests durch Claude wiederholt. Keine produktive INI geändert.
5. **Gegenprüfung im Welcome-Speicherweg, Commit `1855c67e`.** Der optionale
   `save_ini_section`-Hook erhält nur tatsächlich geänderte Schlüssel des
   Kandidaten unter demselben INI-Lock. `check_slot_change()` prüft geänderte
   PB7/PB8-Pfade gegen die gespeicherte PB9-Reservierung, ohne Bot-/Git-/Artefakt-
   Inspektion. Ohne PB9-Konfiguration entfällt die Prüfung.

   `save_setup` bildet Konflikte auf HTTP 409 mit verständlichem Text und
   `X-PBGUI-Error-Codes` ab. Die ursprünglichen Interpreter-Anfragen werden
   zusätzlich geprüft, damit die bestehende PB8-Symlink-Auflösung keinen
   Env-Konflikt verdeckt. Unveränderte Pfade und reine Identitätsänderungen
   bleiben möglich. Andere Aufrufer verwenden keinen Hook.

   Fokussierte Offline-Prüfung: `test_pb9_slot_change`, `test_pb9_runtime`,
   `test_ini_apply_metadata`, `test_ini_transactions`, `test_welcome_pb8` und
   `test_pb8_installation`: **178 passed in 6.16s**. Die API- und gemeinsamen
   Laufzeitänderungen erhöhen die Serial von 3699 auf 3700. EN/DE-Welcome-
   Guides wurden ergänzt; HTML, Navigation und Asset-Versionen bleiben gleich.
   Zusätzlich: `test_help_coverage`, **3 passed in 0.30s**.
   Claude Fable 5.2 / medium: keine blockierenden Befunde, commit-reif; rein
   lesend am aktuellen Quellstand, ohne eigenen Testlauf und ohne Git-Diff.
6. **Native Vorlagenanbindung, Commit `b7cbb702`.** `pb9_config.py` bietet
   `load_pb9_template()` nach statischer Erkennung der getrennten Runtime.
   Ein eigener Einmal-Prozess mit `-I` lädt nur `config.schema.get_template_config()`
   und Paketmetadaten aus dem ausgewählten Checkout. Schema und Modulpfade
   werden mit der statischen Identität abgeglichen. Sparse Seiten und Bounds
   bleiben erhalten; native Vorbereitung oder Validierung wird nicht behauptet.

   Der Client besitzt eine eigene Linux-Prozessgruppe und nichtblockierende
   Pipes unter einer gemeinsamen Deadline. Der Leader bleibt bis zur
   Gruppenbereinigung unreaped, damit die PGID nicht wiederverwendet werden
   kann. Auch Gruppenmitglieder mit geerbtem stdout werden beendet; keine Reader-Threads
   oder Caches bleiben zurück. Ausgabe maximal 2 MiB, Anfrage maximal 16 KiB.
   Native-Fehlerdaten werden nicht geloggt. Kleine Umgebungs-Allowlist ohne
   Python-/Provider-/LD-/DYLD-Variablen; PATH besteht aus dem gewählten Env und
   Systemstandard. Nur JSON-reine Vorlagen werden akzeptiert, ohne stille
   Umwandlung von Tupeln oder Nicht-String-Schlüsseln.

   Keine API-/UI-Anbindung, tatsächliche v9-Installation oder Ausführung echter
   Bot-Envs. Ein späterer API-Aufrufer muss die Arbeit auslagern und laufende
   Aufrufe in die Shutdown-Verantwortung aufnehmen. Die neue Log-Gruppe
   `PB9Config` in `logging_helpers.py` wird beim Start importiert; deshalb
   Serial-Bump von 3700 auf 3701 trotz noch unverdrahtetem Adapter.

   Fokussiert geprüft: `test_pb9_config`, `test_pb9_runtime`,
   `test_passivbot_runtime`, `test_logging_helpers`: **150 passed in 6.26s**,
   dazu 12 Fork-DeprecationWarnings aus den vorhandenen Logging-Tests.
   Alle nativen Import-/Timeout-Fälle verwenden minimale Fixture-Module und
   eine temporäre Python-Umgebung; keine echte v9/Rust-Integration ist damit
   nachgewiesen. Untersuchte native Schnittstelle: Ref `ad9d6ede`,
   `src/config/schema.py:get_template_config`.

   Das erste Claude-Review fand eine unbeschränkte Thread-Wartezeit bei
   geerbtem stdout. Der Transport wurde daraufhin durch die Prozessgruppen-/
   Selector-Lösung ersetzt. Timeout, endloser Produzent, Pipe-Fehler und ein
   Enkel mit geerbtem stdout sind jetzt abgedeckt. Der erste Adapter wird
   explizit nur unter Linux unterstützt; Windows und andere Plattformen
   erhalten `unsupported_platform` vor einem Prozessstart.
   Im erneuten Review bestätigt Claude Fable 5.2 / medium die Behebung und
   bewertet den Stand als commit-reif. Die Prozess-Invariante ist im Code
   festgehalten: SIGCHLD darf nicht ignoriert werden, und ein fremder Reaper
   darf den Leader nicht vor der Gruppenbereinigung einsammeln.
7. **Native Inline-Validierung, Commit `394324ed`.** `validate_pb9_config()`
   liefert nur `valid` und Quellzuordnung. Der isolierte Helper ruft
   `config.load.prepare_config(config, base_config_path="", verbose=False,
   target="canonical", runtime=None)` auf und verwirft das aufbereitete
   Ergebnis. Die ursprüngliche Sparse-Quelle bleibt unverändert beim Aufrufer;
   normalisierte Exporte und Speichern folgen separat.

   Parent und Helper verwenden dieselbe reine Eingangsprüfung aus dem
   Standardbibliothek-Teil des Helpers, ohne dabei native Module zu importieren.
   Root-Schema muss genau zum gewählten Runtime-Schema passen. GUI-Metadaten
   (`pbgui`) und interne `_`-Schlüssel, auch in gepunkteten Pfaden, sind kein
   Teil der unterstützten nativen Eingabe und werden abgelehnt, nicht entfernt.
   Native Value/Type/KeyError-Werte werden zu `native_config_rejected`, andere
   native Fehler zu `native_validation_failed`, jeweils ohne rohe Fehlerdaten.

   I/O-Audit an Ref `ad9d6ede`: `config.normalize` bindet Universe-Quellen;
   `config.universe.resolve_universe_sources` liest explizite Dateiquellen,
   tokenisiert Inline-Listen aber mit `resolve_files=False`. `None` und der
   explizite `approved_coins: "all"`-Zweig sind dateifrei. Skalare sonstige
   Quellen, Pfadpräfixe/-suffixe, Coin-Overrides und Override-Dateipfade werden
   vor nativen Imports abgelehnt, einschließlich Szenarien/gepunkteter Patches.
   `config.hsl.require_current_hsl_schema` kann Override-Dateien laden;
   `config.hydrate.normalize_backtest_coin_sources` kann über
   `utils.symbol_to_coin` eine Symbolmap lesen. Deshalb bleiben nichtleere
   Coin-Overrides und `coin_sources` sowie Dataset-Dateipfade ausgeschlossen.
   Die Datei-Lader `load_input_config`/`load_raw_config` werden nicht aufgerufen.

   Validierungsanfragen sind auf 1 MiB begrenzt; das 16-KiB-Template-Limit
   bleibt bestehen. Der gemeinsame Linux-Transport mit Gruppenbereinigung,
   Deadline und 2-MiB-Ausgabelimit wird weiterverwendet. Parent-Antworten
   lehnen jetzt ebenfalls NaN/Infinity ab. Der Helper startet zusätzlich mit
   `-B`, damit native Imports keine Bytecode-Caches im Quellbaum schreiben.

   Fokussiert geprüft: `test_pb9_config`, `test_pb9_runtime`,
   `test_passivbot_runtime`: **204 passed in 10.37s**. Test-Marker belegen,
   dass abgelehnte Eingaben weder das native Paket noch `config.load`
   importieren. Erfolgsfälle prüfen die kwargs, Unverändertheit, Source-Bindung,
   Größenlimits, Fehlertext-Isolation und Prozessbereinigung bei Timeout.
   Nach dem ersten Claude-Review prüft das Gate zusätzlich komplette
   Listenelemente vor dem Aufteilen an Kommas (`a,.txt`/`sub/x, .json`);
   Importfehler bleiben Runtime-Fehler. Ein erfolgreicher 350-KiB-Request
   prüft Teilschreibvorgänge über der Pipe-Puffergröße und vollständigen Empfang.
   Weitere Regressionen decken Dotfile-Endungen und native Pfadnormalisierung
   ab (`x.json/`, `x.txt/.`, innere `..`-Segmente); die Prüfung entfernt für
   den Endungsvergleich abschließende Slash-/Punktzeichen und sperrt Traversal.
   Auch unerwartete Schema-Importfehler erhalten den Validierungs-Fehlercode.
   Claude Fable 5.2 / medium bestätigte im abschließenden Quellabgleich:
   kein verbleibender konkreter Datei-Read-Bypass unter den dokumentierten
   Grenzen. Der optionale isolierte Test der `..`-Regel ohne Dateiendung
   bleibt offen; der vorhandene Traversal-Fall trifft auch die Endungsregel.
   Alle nativen Fälle verwenden Fake-Module in temporären Envs; echte
   v9-/Rust-Integration bleibt offen. Kein API-Startimport geändert, daher
   kein zusätzlicher Serial-Bump (weiterhin 3701).

8. **In-Memory-Dokumentgrenze, Commit `67245d68`.** `validate_pb9_document()`
   erstellt einen strikten JSON-Snapshot (maximal 1 MiB), bevor Runtime-Arbeit
   beginnt. Nur Root-`pbgui` wird abgetrennt und muss ein JSON-Objekt sein;
   ein fehlender Schlüssel bleibt als `None` erkennbar. Der native Anteil
   durchläuft unverändert `validate_pb9_config()`. Die Rückgabe trennt
   autorisierte Sparse-Quelle (`config`), GUI-Metadaten (`pbgui`) und ermittelte
   Herkunft (`source`). Es wird nichts bereinigt, ergänzt oder in die Quelle
   zurückgeschrieben; keine Dateilader, API-/UI-Anbindung oder Migration.
   Snapshot und Rückgabe teilen keine veränderlichen Objekte mit dem Aufrufer.
   Das geht bewusst über Claudes vorgeschlagene flache Kopie hinaus, damit
   spätere Aufruferänderungen nicht die geprüfte Quelle oder Metadaten verändern.
   Native Regeln gelten weiter für verschachtelte GUI-/interne Schlüssel und
   Dateiverweise. Ungültige Dokumente ergeben vor Runtime-Inspektion den Code
   `invalid_document`; native Fehler werden ohne Teilergebnis weitergereicht.
   PB7/PB8-Dateilader bleiben unverändert; die spätere Dateianbindung muss deren
   Versionsgrenze ausdrücklich lösen, statt v9 in den PB7-Pfad zu geben.
   Fokussiert geprüft: `test_pb9_config`, `test_pb9_runtime`,
   `test_passivbot_runtime`: **225 passed in 13.14s**. Fake-Helper-Tests prüfen
   die Metadaten-Trennung, unabhängige Snapshots, unveränderte Sparse-Quelle,
   fehlende/leere Metadaten, ungültiges JSON und Größenlimit vor Inspektion,
   fortgeltende native Ablehnungsregeln und Fehler ohne Teilergebnis.
   Nach dem ersten Claude-Review wird der Metadaten-Typ am fertigen Snapshot
   geprüft. Ein Regressionstest ändert den Aufruferwert zwischen strikter
   JSON-Prüfung und Snapshot-Erzeugung. Dokumentlimit und Helper-Anfragelimit
   bleiben getrennt: beide messen ASCII-escapte JSON-Bytes, die Anfrage enthält
   zusätzlich Runtime-Daten. Ein Dokument knapp unter/am Limit kann deshalb
   nach Runtime-Inspektion mit `invalid_request` scheitern, noch vor Helper-Start;
   das ist separat getestet und im Funktionsvertrag/Changelog beschrieben.
   Claude Fable 5.2 / medium bestätigte nach den Korrekturen: commit-reif,
   keine verbleibenden konkreten Fehler. Rein statischer Review, kein eigener
   Testlauf; echte native v9-/Rust-Integration bleibt offen.
   Kein API-Startimport geändert, daher kein weiterer Serial-Bump (3701).

9. **Expliziter, nur lesender Dateipfad, Commit `f21f1acb`.**
   `load_pb7_config(..., pb9_context=PB9FileContext(...))` wählt ausdrücklich
   den neuen `pb9_config_storage`-Lader, bevor PB7-Code ausgeführt wird.
   Ohne Kontext bleibt der bestehende Ablauf unverändert; PB7-spezifische
   Optionen sind im v9-Modus nicht erlaubt. `path` ist hier ein sichtbarer
   JSON-Dateiname direkt im ausdrücklich gewählten absoluten `approved_root`.
   Es gibt keine automatische Wahl über Dokument-Metadaten und keine neue
   Produktionsdatenwurzel, API-/UI-Anbindung oder Schreibfunktion.

   Der Lader öffnet alle Root-Komponenten und die Datei über No-Follow-
   Deskriptoren. Nichtblockierendes Öffnen verhindert Warten auf FIFOs;
   `fstat` verlangt eine reguläre Datei. Dateigröße und tatsächliches Lesen
   sind auf 1 MiB begrenzt, auch wenn die Datei nach `fstat` wächst. Striktes
   UTF-8/JSON verwirft doppelte Schlüssel auf jeder Ebene, Nicht-Objekte,
   NaN/Infinity und Float-Überlauf. Die separate Dokument-/Anfrageprüfung
   bleibt anschließend bestehen. Es gibt keine Sperren oder Schreibvorgänge.

   Die vorhandene Dokumentgrenze validiert die Sparse-Quelle in ihrer v9-
   Umgebung; nur ursprünglich vorhandene Root-GUI-Metadaten werden wieder
   angefügt. Ermittelte Provenienz wird nicht in die Konfiguration geschrieben.
   Der öffentliche Rückgabewert bleibt ein Config-Dict. Speichern folgt als
   eigener Schritt nach diesem Ladepfad. Wegen `pb7_config` am API-Start ist
   `api/serial.txt` auf **3702** erhöht; produktive Aufrufer sind nicht umgestellt.
   Fokussiert geprüft: v9-Storage/Config/Runtime/Quellklassifikation **281 passed
   in 15.64s**; PB7-Bridge, PB8-Config und PB7-Pin **233 passed, 2 skipped in
   5.71s**. Der Bridge-Lauf in der Sandbox hing an `asyncio.run`/Executor-
   Abschluss; ein Minimalfall reproduzierte dies nur in der Sandbox. Die
   fokussierte Wiederholung außerhalb der Sandbox lief offline durch, ohne
   Test- oder Produktänderung als Umgehung. Keine echte v9-/Rust-Integration.
   Nach dem ersten Claude-Review: Roots werden nicht aus `HOME` expandiert;
   neue öffentliche Datei-/Parserfehler entstehen außerhalb ihrer Handler,
   ohne rohe Exception-Ursache oder -Kontext. Tests prüfen auch formatierte
   Tracebacks und leere `__context__`/`__cause__`. Die Bereinigung versucht
   alle Deskriptoren zu schließen, auch nach einem Close-Fehler; Procfs-Zählung
   prüft ausbleibende FD-Leaks. Root-`pbgui` steht in der Rückgabe aktuell am
   Dict-Ende; Werte bleiben identisch, die spätere Schreibformatierung ist offen.
   Claude Fable 5.2 / medium bestätigte beim abschließenden Quellabgleich:
   commitfähig, kein verbleibender P1–P3-Befund. Nichtblockierend bleiben ein
   vorübergehender Traceback-Zyklus beim internen Close-Fehler bis zum GC und
   die nicht separat geprüfte `__cause__` dieses Fehlerzweigs; öffentliche
   Datei-/Parserfehler enthalten keine rohe Ursachenkette.

10. **Neue Dateien sicher speichern, Commit `663a3ed1`.**
    `save_pb7_config(..., pb9_context=...)` verzweigt vor jedem Legacy-Schritt
    zum v9-Adapter und behält den Rückgabewert `None`. Ohne Kontext bleibt der
    bisherige Body unverändert. Native Validierung und ein unabhängiger Snapshot
    gehen jeder Bibliotheks-Schreiboperation voraus. Gespeichert werden nur
    authored Sparse-Werte und ursprünglich vorhandene Root-GUI-Metadaten;
    native Aufbereitung und ermittelte Provenienz werden nicht eingemischt.
    Die JSON-Ausgabe ist `indent=4`, UTF-8 plus Zeilenumbruch. Ein begrenzter
    Encoder stoppt bereits beim Formatieren bei 1 MiB, auch für tief verschachtelte
    Metadaten; es wird kein beliebig großer Pretty-String vorab aufgebaut.

    Die ausdrücklich gewählte Root muss bereits existieren, dem effektiven
    Benutzer gehören und Modus 0700 besitzen. Es gibt weder `mkdir` noch
    stilles `chmod`. Lesen und Neuerstellen nutzen dieselbe No-Follow-
    Root-Deskriptorverwaltung. Der neue additive Helfer
    `secure_files.create_private_file_at()` schreibt eine 0600-Tempdatei und
    synchronisiert deren Inhalt. Ein exklusiver Hardlink veröffentlicht die
    vollständige Datei ohne Ersetzung; anschließend werden Tempname entfernt
    und das Verzeichnis synchronisiert. Alle Operationen bleiben am Root-FD
    gebunden, auch wenn der Pfad währenddessen umbenannt wird.

    Vorhandene Dateien, Symlinks, FIFOs und Verzeichnisse ergeben
    `config_file_exists`; konkurrierende Ersteller haben genau einen Gewinner.
    Es gibt keine Read/Modify/Write-Transaktion und keinen neuen Lockcache:
    die atomare No-Replace-Operation selbst entscheidet den Konflikt. Die Update-
    Schnittstelle mit Revision wird in Schritt 11 ergänzt.
    `config_file_created_io_failed` bedeutet, dass bereits
    veröffentlicht wurde, aber Bereinigung/Synchronisationsbestätigung scheiterte;
    dann weder blind erneut schreiben noch die veröffentlichte Datei entfernen.
    Kein Installer, keine Migration, keine UI-/API-Anbindung oder Remote-Aktion.
    Der erforderliche Serial-Bump nach Abschluss der API-Startcode-Änderungen
    ist erfolgt: **3703**.
    Fokussiert geprüft: private Neuerstellung, v9-Storage/Config/Runtime und
    Quellklassifikation **325 passed in 12.98s**; PB7-Bridge/PB8-Config/PB7-Pin
    und bestehende Datei-Locks **239 passed, 2 skipped in 4.67s**. Neue Fälle
    prüfen zwei konkurrierende Prozesse, No-Follow-Rootbindung bei Umbenennung,
    Fehler vor/nach Veröffentlichung, unveränderte Legacy-Ausgabe und den
    Save/Load-Roundtrip mit isoliertem Fake-Native-Helper. Keine echte v9-/Rust-
    Integration und keine Produktionsdaten wurden gelesen oder geschrieben.
    Nach dem ersten Claude-Review ist der Logtext nach Veröffentlichung eindeutig
    als gespeichert mit fehlender I/O-Bestätigung formuliert. Zusätzliche Tests
    prüfen fehlgeschlagenes Temp-Unlink und Root-Close nach dem Commitpunkt.
    Der Helfer schreibt direkt auf seinen eigenen FD und behandelt Teil- sowie
    Nullfortschritt, ohne mehrdeutige `fdopen`-Übernahme bei Konstruktorfehlern.
    Der Prozess-Barriere-Timeout ist auf 10 Sekunden angehoben (Join: 15 s).
    Bei SIGKILL/Crash kann eine private `.pbgui-create-*.tmp` zurückbleiben;
    der Lader lehnt solche Punktnamen ab. Ein automatischer Aufräumer ist nicht
    Teil dieses Schritts und dürfte aktive Ersteller nicht stören.
    Claude Fable 5.2 / medium bestätigte im abschließenden Quellabgleich:
    keine verbleibenden Befunde; Commit nach dem inzwischen erfolgten Serial-
    Bump empfohlen. Kein eigener Testlauf durch Claude.

11. **Revisionsgeschützte Updates, Commit `3897f242`.**
    `load_pb7_config(..., pb9_with_revision=True, pb9_context=...)` liefert
    `{config, revision}`; die SHA-256-Revision gehört exakt zu den einmal für
    die Validierung gelesenen Dateibytes, nicht zur reserialisierten Rückgabe.
    Der Default bleibt ein Config-Dict. Mit `pb9_expected_revision=<64 lowerhex>`
    kann `save_pb7_config()` diesen Stand aktualisieren. Ohne Revision bleibt
    das bisherige Create-only-Verhalten erhalten. Revisionsflags ohne expliziten
    Kontext fallen nie in PB7 zurück. Revisionen/Provenienz werden nicht in
    native Konfigurationen oder GUI-Metadaten eingemischt.

    Updates prüfen die Revision vor nativer Arbeit und erneut unter einer
    Verzeichnis-Inode-Sperre. Native Validierung und Formatierung liegen außerhalb
    der Transaktion. Ein Source-FD bleibt beim geschützten Lesen/Ersetzen offen,
    und die finale Datei-Inode wird vor und nach Temp-Vorbereitung abgeglichen.
    Fehlende/veraltete Ziele ergeben `config_revision_conflict`, nie implizite
    Neuerstellung. `config_file_updated_io_failed` unterscheidet Fehler nach
    bereits erfolgtem Replace; das Log benennt den aktualisierten Zustand.

    Der additive `advisory_directory_lock()` öffnet eine eigene Dateibeschreibung
    relativ zum Root-FD. Dadurch blockieren sich auch Threads mit demselben
    Eingabe-FD. Nur aktive Inode-Besitzer bleiben threadlokal für Reentranz/PID-
    Trennung erhalten; kein Lockfile und kein persistenter Lockcache. Existierende
    pfadbasierte Locks bleiben unverändert. Ein gemeinsamer privater Publikations-
    Helfer trägt Create und Replace; die vorhandene No-Replace-Semantik bleibt.
    Die Sperre schützt kooperierende PBGui-Updates. Nichtkooperierende same-UID-
    Editoren können zwischen Prüfen und Replace weiterhin schreiben; Inode-
    Checks verengen das Fenster, bieten aber kein Betriebssystem-Byte-CAS.

    Fokussiert geprüft: Revisionen/private Dateihelfer/v9-Storage/Config/Runtime/
    Quellklassifikation und Locks **373 passed in 15.95s**; PB7-Bridge/PB8-Config/
    PB7-Pin **233 passed, 2 skipped in 5.28s**. Alle Daten liegen in temporären
    Testbäumen; nativer Roundtrip verwendet den isolierten Fake-Helper. Echte
    v9-/Rust-Integration und UI-/API-Anbindung bleiben offen. Der Serial-Bump
    nach Abschluss der API-Startcode-Änderungen ist erfolgt: **3704**.
    Nach dem ersten Claude-Review werden nur fehlende Zieldateien als
    Revisionskonflikt behandelt; fehlende Root-/Ancestor-Verzeichnisse ergeben
    `config_file_unavailable`. Storage-Tests decken Symlink/FIFO-Ziele, nichtprivate
    Roots und fehlende Bibliotheken ab. Der Save-Vertrag weist ausdrücklich auf
    potenziell blockierendes flock/fsync und künftiges API-Offloading mit klarer
    Inflight-/Shutdown-Ownership hin. Allgemeine Publikationsfehlertexte gelten
    gleichermaßen für Create und Replace.
    Claude Fable 5.2 / medium bestätigte beim abschließenden Quellabgleich:
    keine P1/P2-Restfehler, Commit nach dem inzwischen erfolgten Serial-Bump
    empfohlen. Optional bleibt die Fehlerklassifikation bei einer extern
    entfernten Tempdatei (`ENOENT` des Replace kann als Revisionskonflikt
    erscheinen); dies verändert oder überschreibt keine Nutzerdaten.
    Das gemeinsame Temp-Präfix heißt auch im Replace-Pfad `.pbgui-create-`.
    Der Review war rein lesend ohne eigenen Testlauf.

12. **Read-only-API, Commit `aa532517`.** Das neue `api/pb9.py` registriert
    authentifizierte `GET /api/pb9/runtime` und `POST /api/pb9/template` im Server.
    Ein Request liest genau einen INI-Snapshot und daraus ausschließlich die
    sechs erlaubten Runtime-Pfadschlüssel. Status und Vorlagen werden anhand
    dieser serverseitigen Auswahl erzeugt, nie anhand von Request-Pfaden.
    Query-Parameter und nichtleere Parameterobjekte werden abgelehnt; der
    Template-Body ist auf 1024 Bytes begrenzt. Geteilter Cookie-/Bearer-Auth und
    `BrowserOriginMiddleware` bleiben zuständig. Erfolgsantworten und erwartete
    Fehler sind `no-store`; Status ist ausdrücklich nur statische Erkennung.

    Ein App-eigener `PB9RequestOwner` lässt höchstens zwei Jobs gleichzeitig zu,
    ohne Warteschlange oder persistenten Cache. Jeder Thread wird vor seinem
    Start registriert; ein unabhängiges Concurrent-Future meldet erst nach
    Abschluss der synchronen Arbeit inklusive Helper-Bereinigung fertig.
    Request-Cancel und Shutdown drainieren und joinen den Worker, statt einen
    laufenden Thread als beendet zu behandeln. Startfehler vor oder nach
    tatsächlichem Start, begrenzte Admission, erneuter Startup und wiederholter
    Shutdown sind getestet. Auf dem Event-Loop laufen keine Git-/INI-/nativen
    Operationen. Nur bereits abgeschlossene Arbeitsläufe werden synchron gejoint.

    Der Server bindet Startup/Shutdown im vorhandenen geschützten Lifespan ein.
    Aktive Jobs blockieren den koordinierten API-Restart; dessen Lease schließt
    neue Admission vor der awaiteten Blocker-Prüfung. Fehler geben nur feste
    Codes aus, erwartete HTTP-Statuswerte bleiben erhalten. Datei-CRUD, Runtime-
    Pfadänderungen, Installer, Jobs und UI sind nicht angebunden.

    Fokussiert geprüft: API, tatsächliche Server-Routen/Restart-Integration und
    v9-Config, Quellklassifikation/Runtime sowie lokale Signal-/Stop-Fälle
    **334 passed in 28.35s**.
    Weitere Runtime-/Datei-/PB7/PB8-/Welcome-Regressionen: **556 passed,
    2 skipped in 13.79s**. Der Mount-Test berücksichtigt FastAPIs
    verzögerte Router-Einbindung über echte HTTP-Dispatch-Prüfung, nicht nur
    flaches `app.routes`. Tiny-App- und echte Fixture-Helper-Tests verwenden
    ausschließlich temporäre Pfade bzw. in-memory INI/Auth-Doubles. Reale
    v9-/Rust-Abnahme bleibt offen. Serial nach finalen Startup-Edits und Review-Korrektur: **3706**.

    Das erste Claude-Review fand einen Stop-Blocker: direktes Stoppen erzwingt
    nach 5 s einen Kill, systemd nach 30 s, während der native Helper maximal
    60 s laufen darf. Abbruch erst im Lifespan wäre außerdem zu spät, weil
    Uvicorn zunächst aktive HTTP-Tasks drainiert. Deshalb installiert der
    offizielle Server im Lifespan frühe SIGTERM-/SIGINT-Hooks, die den bereits
    vorhandenen Handler weiter aufrufen und ausschließlich ein lockfreies
    Poll-Flag setzen. Dafür ist kein freier Event-Loop nötig. Der Native-
    Transport prüft dieses Flag vor dem Start sowie in seinen beiden
    Poll-Schleifen und beendet/reapt bei Abbruch seine gesamte Prozessgruppe.
    Shutdown setzt dasselbe Flag, drainiert danach die Threads und gibt die
    Signal-Hooks wieder frei; Teil-Startup wird rückgängig gemacht. Normale
    Request-Cancellation wartet weiterhin auf das Ergebnis, ohne automatisch
    die Arbeit zu beenden. Nur expliziter Stop/Shutdown bricht native Arbeit ab.

    Ein echter lokaler Uvicorn-Test über einen temporären Unix-Socket hält die
    Event-Schleife 2,5 s an und führt gleichzeitig einen Fixture-Helper mit
    Kindprozess aus. SIGTERM beendet die Gruppe noch vor der Rückkehr der
    Event-Schleife, und der Server stoppt innerhalb der bestehenden Stop-Frist.
    Weder Stop-Methode noch systemd-Timeouts wurden verändert. Erzwungenes
    SIGKILL ohne vorherigen behandelbaren Stop ist kein Graceful-Shutdown-Pfad.
    Weitere Review-Hinweise sind korrigiert: native Worker reichen keine freien
    HTTP-Details/Header durch; Start-BaseExceptions werden bereinigt und nach
    ggf. nötigem Drain erneut ausgelöst; Startfehler-Logging liegt außerhalb
    der Owner-Sperre. Bestehende Admission-HTTP-Fehler bleiben erhalten.

    Auch der statische Git-Vorlauf beachtet das Stop-Flag vor jedem weiteren
    Prozessstart. API-eigene Prüfungen begrenzen jeden bereits laufenden Git-
    Aufruf auf eine Sekunde; `subprocess.run` beendet und reapt ihn bei Timeout.
    Bestehende synchrone Aufrufer behalten ohne neue Optionen ihr bisheriges
    Fünf-Sekunden-Limit. Nach dem Vorlauf verhindert eine erneute Abbruchprüfung
    einen späten nativen Start. Tests prüfen Stop vor und zwischen Git-Läufen,
    Timeout ohne Folgeaufruf sowie die Weitergabe des Owner-Flags.
    Claude Fable 5.2 / medium bestätigt den Stand als commitfähig ohne P1/P2.
    Sein P3-Hinweis auf `math.isfinite` bei extrem großen Integer-Zeitlimits
    ist durch vorgezogene Bereichsprüfung korrigiert und parametrisiert getestet.

13. **Inline-Validierungs-API, Commit `0d47545c`.** `POST /api/pb9/validate`
    akzeptiert ausschließlich `{"config": <Dokument>}`. Query-Overrides werden
    abgelehnt; der gestreamte Body ist auf 1 MiB begrenzt. Striktes UTF-8/JSON
    verwirft doppelte Schlüssel auf jeder Ebene, NaN/Infinity, Exponentenüberlauf
    und Nicht-Objekte sowie einzelne Unicode-Surrogate. Die Verschachtelung
    ist auf 64 Ebenen begrenzt; auch Antwort-Renderfehler ergeben feste Fehler.
    Dekodierung läuft im registrierten Worker vor dem einzigen
    INI-Snapshot. Native Validierung erhält authored Sparse-Werte ohne Root-GUI-
    Metadaten; die API liefert `valid`, `config`, `pbgui` und unabhängige `source`.
    Ungültige Konfigurationen ergeben feste 422-Fehler, Runtime-/Transportfehler
    feste 503-Fehler. Auth, Origin-Schutz, No-Store, Kapazität und Restart-Gate
    werden unverändert geteilt. Es wird weder gespeichert noch migriert.

    Optionale Stop-Flags werden durch beide öffentlichen Validierungshelfer
    bis zu Git-Vorlauf und Native-Prozessgruppe weitergereicht. Alte Datei-
    Aufrufer ohne Flag behalten ihre vorherige Aufrufform. Der echte lokale
    Uvicorn-Stoptest prüft nun Template und Validierung mit Kindprozess bei
    blockiertem Event-Loop. Fokussiert: API/Auth/Restart/Signale, Native-Bridge,
    Datei-Laden/Speichern und Revisionen **361 passed in 31.98s**. Ausschließlich
    temporäre Daten und Fixture-Module; echte v9-/Rust-Abnahme bleibt offen.
    Claudes P2-Hinweis auf Unicode-Surrogate ist korrigiert; erweiterte
    Regressionen sind grün. Der abschließende lesende Recheck bestätigt:
    P2 behoben, keine verbleibenden Blocker, commitfähig.
    Der separate Dokument-/Transport-Check zählt weiterhin
    ASCII-escaped JSON und kann unterhalb des rohen Body-Limits 422 ablehnen.
    Serial nach den finalen API-Änderungen: **3707**.

14. **Isolierte Datei-API, Commit `f8ee013f`.** Die authentifizierten Routen
    `GET /api/pb9/configs`, `POST /configs/load` und `POST /configs/save` nutzen
    den vorhandenen Owner und genau einen Einstellungen-Snapshot. Die Ablage
    ist ausschließlich `PBGDIR/data/configs_v9`. Nur gültige UTF-8-JSON-Basenamen
    mit höchstens 255 Bytes sind erlaubt; Anfragen können keine Root wählen.
    Liste und Load erzeugen keine Verzeichnisse. Save verlangt ausdrücklich
    `revision: null` für exklusive Neuerstellung oder eine SHA-256-Revision für
    Updates. Ein fehlendes Update legt keine Ablage an und bleibt ein Konflikt.
    Laden liefert authored Config inklusive ursprünglichem Root-`pbgui` und die
    Revision der gelesenen Bytes. Save bestätigt nur `saved` und `name`; der
    Editor lädt danach die Revision erneut, ohne neuere Eingaben zu verlieren.

    `secure_files.private_directory_at` erzeugt ausschließlich neue 0700-
    Verzeichnisse FD-relativ und öffnet vorhandene ohne Symlink-Verfolgung.
    Vorhandene Rechte werden nicht verändert; das bestehende Datenverzeichnis
    darf seinen Modus behalten. Die Liste scannt höchstens 1000 Einträge und
    berücksichtigt nur sichtbare reguläre JSON-Dateien. Dateioperationen bleiben
    am gemeinsamen PB7/PB9-Dispatcher mit explizitem `PB9FileContext` angebunden.
    Native Validierung, Private-File-Veröffentlichung und Wartezeit auf die
    Verzeichnissperre erhalten optional das Owner-Stop-Flag. API-Updates warten
    höchstens zwei Sekunden auf die Sperre. Alte Aufrufer ohne Flag bleiben
    unverändert. Stop vor Veröffentlichung entfernt vorbereitete Tempdateien;
    nach `link`/`replace` wird ein gespeichertes Ergebnis nicht zurückgerollt.
    I/O-Bestätigungsfehler nach Veröffentlichung behalten eigene Fehlercodes.

    Claude Fable 5.2 / medium fand keine P1/P2-Befunde und empfiehlt den Commit.
    Seine P3-Hinweise zu impliziter Ablage bei Updates, Rechten/Fehlercodes und
    UTF-8-Namenslänge sind korrigiert. Echte temporäre Native-Helper prüfen den
    API-Create/Load/Update-Zyklus; ein lokaler Uvicorn-Test prüft nun auch Stop
    während Save bei blockierter Event-Schleife, inklusive Kindprozess und
    ausbleibender Zielveröffentlichung. Alle Daten bleiben in Pytest-Tempwurzeln.
    Fokus-Regressionen: Datei-API, Storage/Revisionen, private Dateifunktionen,
    Locks und API/Auth/Restart **305 passed in 12.87s**; Native-Bridge und
    Signal-/Stop-Integration **147 passed in 24.99s**. Auch HTTP-Save direkt nach
    Veröffentlichung bestätigt Erfolg trotz Stop; nachträglicher Abbruch rollt
    die Datei nicht zurück. Serial nach finalen API-/Startcode-Edits: **3708**.

15. **Erster testbarer JSON-Editor, geprüft und commitfähig.** Unter **PBv9 → Config
    Editor** führt das reale gemeinsame Menü zur authentifizierten, montierten
    `GET /api/pb9/main_page`. Die neue Seite nutzt den gemeinsamen Sidebar-Shell,
    Keyboard-/Pointer-Resize und Guide. `POST /runtime/paths` speichert nur das
    eigene PB9-Paar über den vorhandenen atomaren INI-Schreiber mit Slot-Trennung.
    Native Erkennung/Git erhält das Owner-Stop-Flag; ein Stop während dieser
    Prüfung verhindert die INI-Veröffentlichung. INI-Eingabefehler sind feste
    422-Antworten; Prozentzeichen in Runtime-Pfaden bleiben vorerst unsupported.

    Der Editor lädt native authored Vorlagen oder gespeicherte Konfigurationen,
    bearbeitet vollständiges Raw JSON, validiert ausdrücklich und erstellt oder
    aktualisiert Dateien über die bestehende Revisionsgrenze. Python-generierter
    `config_text` erhält auch Integer über dem JavaScript-Sicherheitsbereich im
    Textfeld. JSON-Anfragen enthalten den ursprünglichen Raw-Text, damit doppelte
    Schlüssel nicht im Browser verschwinden. Rückgaben ergänzter nativer Defaults
    werden nicht in das Dokument übernommen. Speicherung verwendet vier Leerzeichen
    und ursprüngliche Root-GUI-Metadaten am Ende. Config-Inhalte liegen ausschließlich
    im aktuellen Seitenspeicher, nie in Browser-Storage oder Navigationsparametern.

    Dateiauswahl, Suche und geöffnetes Runtime-Panel werden als nicht geheime
    Navigation in der URL gehalten. Automatische Prüfungen bei Sichtbarkeit,
    alle zehn Sekunden und nach Rückkehr erhalten Entwürfe, Fokus, Auswahl und
    Scrollposition. Unveränderte Dateien verwenden `POST /configs/revision`:
    maximal 1 MiB No-Follow-Lesen plus SHA-256, ohne Native/Git. Nur Änderungen
    sauberer Dateien lösen einen erneuten nativen Load aus. Derselbe abgelehnte
    Stand wird nicht in jedem Intervall erneut nativ validiert. Generationen und
    Pagehide-/Pageshow-Bereinigung schützen vor verspäteten Antworten und Timern.

    Claudes erste vier P2-Befunde zu falschem Konfliktziel, Restart-409, fehlender
    Bestätigung nach Save und nativer Hintergrundlast sind korrigiert. Auch sein
    weiterer P2 zum endlosen Abgleich nicht angekommener Saves ist korrigiert:
    unbekannte Ergebnisse werden ausschließlich lesend geprüft; Abweichung oder
    fehlender Zielname beendet den Abgleich und lässt einen bewussten, weiterhin
    revisionsgeschützten Versuch zu. 401/403 sind keine unklaren Schreibausgänge.
    Ein reiner Backend-Abgleich unter `POST /configs/reconcile` vergleicht die
    ursprünglichen Werte mit Python-JSON (sortierte Schlüssel, exakte Integer
    und Typen), niemals über gerundete JavaScript-Zahlen. Er liest einmal bis
    1 MiB ohne Native/Git/Schreibzugriff und bindet Ergebnis, SHA-256 und bei
    Übereinstimmung `config_text` an dieselben Bytes. Damit startet auch die
    Save-Bestätigung keinen zusätzlichen nativen Helper.
    Neuere Texte bleiben erhalten. Publizierte Saves mit I/O-Bestätigungsfehler
    behalten eine Warnung; es gibt keine automatischen Save-Wiederholungen.

    Isolierte Browsertests nutzen echte Menü-/Guide-/Help-Wege sowie alle lokalen
    Assets, ohne Screenshots, Runtime-Installation oder Produktionsdaten. Geprüft
    sind Root/Mount, Create/Load/Update/Reload, Konflikte, neuere Eingaben, Native-
    freie unveränderte Revisionen, ausgefallene Save-Antworten und Dateinamen-/Layout-
    Grenzen. Der nächste Schritt nach dieser Vorschau braucht eine neue Aufgabe:
    strukturierte v9-Felder und Migration bzw. lokale Backtests. Die Dauerfreigabe
    endet mit dem abgeschlossenen Editor-Commit; Push/Installationen/Remote-Aktionen
    wurden nicht ausgeführt. Abschließender Claude-Review (Fable 5.2 / medium):
    „commit-ready, no blockers found“, rein lesend ohne eigene Tests. Seine
    nichtblockierenden Hinweise zum überflüssigen JS-Fallback, selten dauerhaft
    nicht lesbaren Bestätigungsdateien und normalisierter Textdarstellung sind
    bekannt; die Oberfläche behauptet keine native Validierung durch Abgleich.

    Fokussiert offline geprüft, ohne vollständige Testsuite:
    - Editor/Navigation, API/Auth/Restart, PB9-Runtime und Guide-Abdeckung:
      **258 passed in 47.69s**, eine DeprecationWarning im ASGI-Mount-Test.
    - Native Fixture-Bridge/Stop-Signale, Storage/Revisionen, Locks und private
      Dateifunktionen: **299 passed in 26.53s**.
    - PB7/PB8/Welcome-Verträge: **319 passed, 2 skipped in 4.22s**.
    - Help-/Notifications-Menü und Cache-Version: **41 passed in 42.50s**.
    - Gemeinsame Sidebar, Editorbreiten und Pointer-/Tastaturbedienung:
      **4 passed, 58 deselected in 1.68s**.
    Insgesamt **921 bestanden, 2 übersprungen** in disjunkten Fokusgruppen.
    Die Tests verwenden temporäre Daten, abgefangene Browser-Anfragen und
    Fixture-Module. Echte v9-/Rust-Abnahme bleibt Voraussetzung einer separat
    eingerichteten Installation. API-Serial nach finalen Startcode-Änderungen:
    **3709**. Kein produktiver Server oder Bot wurde neu gestartet, kein Push ausgeführt.

Die bestehenden Guard-/Installer-Wege prüfen bereits vor dem Checkout.
Playbooks rufen den Guard aus dem PBGui-Checkout auf; eine einzelne kopierte
Guard-Datei wurde bei der Quellsuche nicht gefunden. Die neu importierten
Parser-Helfer verwenden Standardbibliothek-APIs von Python 3.8; ein tatsächlicher
Python-3.8-Testlauf wurde nicht durchgeführt.

Aktuelle fokussierte Offline-Prüfung: `test_pb7_pin`, `test_passivbot_runtime`,
`test_pb8_schema_guard`, `test_pb8_installation`, `test_master_installer_maintenance`
und `test_welcome_pb8`: **209 passed in 9.44s**. Geprüft wurden insbesondere die
Ablehnung vor Stop/Checkout im CLI und Installer sowie der eigenständige
Skriptaufruf außerhalb des PBGui-Arbeitsverzeichnisses. Keine vollständige Suite,
keine Bot-/Remote-Ausführung und kein Windows-Testlauf.

`pb7_guard` wird beim API-Start importiert; `api/serial.txt` wurde für die
Guard-Änderung von 3697 auf 3698 erhöht.

Für die neue PB9-Erkennung wurden `test_pb9_runtime`, `test_passivbot_runtime`,
`test_welcome_pb8` und `test_pb8_installation` geprüft: **167 passed in 6.41s**.
Fixtures liegen ausschließlich unter `tmp_path`; Marker-Code, Interpreter und
CLI bleiben unausgeführt. Ein PB8-Regressionstest vergleicht den Status mit und
ohne v9-INI-Schlüssel. Kein Windows-Testlauf. Da das neue Modul nicht beim
API-Start importiert wird, bleibt die Serial für diesen Schritt unverändert.

## Offen für spätere Schritte

- Installer-/VPS-Schreibwege benötigen vor ihrer PB9-Anbindung ebenfalls
  Konfliktprüfungen; die aktuelle Gegenprüfung schützt nur Welcome `save_setup`.
- Beim Welcome-Hook gilt die Erkennung unveränderter Werte für kanonisch
  gespeicherte Werte. Alte manuelle Trailing-Slashes oder PB8-Interpreter-
  Symlinks können durch die vorhandene Normalisierung als Änderung gelten und
  einen bestehenden Konflikt erneut melden. Frei angegebene Interpreter
  außerhalb von bin/Scripts ergeben bei aktiver PB9-Reservierung den Code
  `other_slot_path_invalid`, statt eine Env-Trennung zu behaupten.
- Prozentzeichen in Runtime-Pfaden bleiben vom INI-Schreiber ausgeschlossen.
  Die Editor-API bildet dessen Eingabefehler jetzt auf `runtime_paths_invalid` ab;
  die INI bleibt unverändert. Unterstützung benötigt eine konsistente Behandlung
  der vorhandenen Raw-Pfad-/Konfliktprüfungen.
- Git-Umgebungsbereinigung im Guard; der bisherige Ablauf bleibt hier erhalten.
- Bei breiterer Verwendung des Klassifikators: stabile Fehlercodes und klare
  Ablehnungsregel. Generation/Commit allein sind bei Fehlern kein Erfolgsnachweis.
  Der Guard benutzt Ausnahmen statt der Fehlerliste des Klassifikators.
- Vollständiger Dirty-Status für Backtest-Provenienz; der Klassifikator belegt
  nur die beiden Metadatendateien. Dynamische Neubindung wird nicht vollständig
  durch die AST-Deklarationsprüfung ausgeschlossen.
- Bei weiteren v9-Refs die native Vorbereitung und Datei-/Symbolquellen erneut
  prüfen; die aktuelle Sparse-/Metadatengrenze wurde am dokumentierten Ref geprüft.
- Der API-Owner samt Stop-/Shutdown-Bereinigung ist umgesetzt; die normale
  SIGCHLD-Invariante bleibt Voraussetzung. Entkommene eigene Sessions werden nicht
  beendet, beenden den Aufruf aber spätestens an der Deadline. Rohes Schreiben
  auf fd 1 kann das Protokoll verwerfen; echte Rust-Integration bleibt zu prüfen.
  Striktes NaN/Infinity-Parsing im Parent und Abbildung seltener
  SubprocessError-Werte sind mit der Inline-Validierung umgesetzt.
- Neue asynchrone Aufrufer dürfen synchrone Git-Lesevorgänge nicht direkt im
  Event-Loop ausführen. Der bisherige synchrone Guard-/Installer-Pfad bleibt so.
- Breite Status-/Archiv-Funktionen, v9-Installation und Laufzeit-Adapter folgen
  in getrennten Änderungen. Keine bestehende PB8-Installation wurde umgestellt.

## Abgeschlossener Claude-Review

Aktueller Review des Revisionsschutzes mit `claude-fable-5-2`, Effort `medium`:
keine P1/P2-Restfehler; Commit empfohlen nach Serial-Bump auf 3704, der erfolgt
ist. Root-/Zieldatei-Fehler sind differenziert, Locked-Compare/Replace und
Legacy-Dispatch bestätigt. Die verbleibenden optionalen Hinweise stehen bei
Schritt 11. Kein eigener Testlauf durch Claude; echte v9-/Rust-Integration offen.

Aktueller Review der sicheren Neuerstellung mit `claude-fable-5-2`, Effort
`medium`: keine verbleibenden Befunde nach den Korrekturen. FD-Schließung,
atomarer Commitpunkt, Teil-/Null-Writes, Logzustände und die neuen Fehlerfall-
Tests sind bestätigt. Serial ist auf 3703 erhöht; echte v9-/Rust-Integration
bleibt offen. Der Review war rein lesend ohne eigenen Testlauf.

Aktueller Review des Dateiladepfads mit `claude-fable-5-2`, Effort `medium`:
commitfähig, kein verbleibender P1–P3-Befund. Literal absolute Roots,
bereinigte Fehlerketten und vollständige Close-Versuche sind bestätigt.
Der Recheck las den konkreten Quellstand samt Dispatch; keine eigene Ausführung.
Die verbleibenden Hinweise zur späteren Key-Reihenfolge und zu einem internen
kurzlebigen Traceback-Zyklus sind oben dokumentiert.

Aktueller Review der Dokumentgrenze mit `claude-fable-5-2`, Effort `medium`:
commit-reif, keine verbleibenden konkreten Fehler. Beide nichtblockierenden
Hinweise des ersten Reviews sind korrigiert: Metadaten-Typ am Snapshot und
explizit getrennte Größenlimits. Der Recheck las den konkreten Quellstand;
Tests wurden ausschließlich von Codex ausgeführt.

Aktueller Review der Inline-Validierung mit `claude-fable-5-2`, Effort `medium`:
Nach Korrektur der Dateiverweis-Prüfung commit-reif unter den bestehenden
Grenzen. Der letzte Recheck verglich den übergebenen Helper und Testdiff mit
den nativen Quellbelegen; keine Ausführung und kein eigener Testlauf.
Ein früherer Review konnte Kopien unter `/tmp` nicht lesen; die folgenden
Prüfungen erhielten die öffentlichen Belege im erlaubten Arbeitsverzeichnis
beziehungsweise direkt im Prompt. Temporäre Repo-Kopien wurden entfernt.

Aktueller Review des Vorlagenadapters mit `claude-fable-5-2`, Effort `medium`:
Nach Ersatz des Thread-Transports durch die Linux-Prozessgruppen-/Selector-Lösung
commit-reif, keine verbleibenden Blocker. Geprüft wurde der vollständige konkrete
Diff einschließlich untracked Dateien, ohne eigenen Testlauf.

Aktueller Review der Gegenprüfung mit `claude-fable-5-2`, Effort `medium`:
commit-reif, keine blockierenden Befunde. Die Randfälle alter nichtkanonischer
Werte und ungeeigneter Interpreterpfade sind oben dokumentiert. Der aktuelle
Quellstand wurde gelesen; ein Diff war Claude bei diesem Aufruf nicht verfügbar.

Aktueller Review der Pfadspeicherung mit `claude-fable-5-2`, Effort `medium`:
commit-reif, keine blockierenden Fehler. Hinweise zu INI-Interpolation,
unnötigem Neuschreiben beim bereits leeren Zustand und der Breite des
Asyncio-Test-Patches bleiben nichtblockierend dokumentiert.

Direkter CLI-Aufruf mit `claude-fable-5-2`, Effort `medium`, ausschließlich
Lesewerkzeugen und ohne Testwiederholung. Ergebnis: commit-reif, keine
blockierenden Fehler. Die 167 Tests sind Codex-Evidenz, kein Claude-Testlauf.

> Prüfe pb9_runtime.py und tests/test_pb9_runtime.py auf getrennte Quellbäume
> und Envs, statische Erkennung ohne Codeausführung und unverändertes PB8.
> Fehlender Commit und Rust-Stempel sind vorerst kein Erkennungshindernis.
> Nur lesen, nichts ändern; melde Fehler oder Commit-Reife.

## PB9 Run: gemeinsamer Editor und lokale Laufzeit

Der JSON-Bibliothekseditor aus `136cd0b7` erfüllt die Run-Anforderung nicht.
Ziel ist jetzt dieselbe Run-Liste und derselbe strukturierte Editor wie PB8,
mit explizitem v9-Adapter und nativen Metadaten statt eines neuen Seitenbaus.
Commit `07bfc915` teilt Backup-/Publikationshelfer und ergänzt den isolierten
Metadaten-/Live-Validierungsaufruf. Fokussierte Evidenz: 159 Bridge-Tests,
316 PB8-API-/Backup-/Cluster-Tests sowie 15 abschließende Bridge-Tests.

Die separat freigegebene offizielle lokale v9-Installation liegt unter
`data/passivbot_v9`, ihre Umgebung unter `data/venv_pb9`. Rust wurde gebaut;
440 native Felder, zwei Strategien und eine native Live-Strukturvalidierung
wurden erfolgreich gelesen/geprüft. Keine Trading-Prozesse oder Remote-Aktionen.
Die Integration verwendet native `config.json` plus private `pbgui.json`;
Remote-Run, Backtest und Strategy Explorer dürfen keinen PB8-Fallback erhalten.
API, gemeinsamer Frontend-Adapter, PBRun und Kontosperre sind integriert.
Die echten Menüpfade Run → Add, der Editor mit 275 Live-Feldern, beide
Strategien, der getrennte Guide/Help-Pfad und die Rückkehr zum Editor wurden
im angemeldeten Browser geprüft; kein horizontaler Überlauf bei 240 px Sidebar.
Native Rust-/Quell-Fingerprint und Live-Strukturvalidierung sind mit der
separaten Installation erfolgreich geprüft. Kein echter Trading-Start.

Fokussierte Offline-Evidenz: 937 Funktionstests im breiten betroffenen Scope;
die vier dort zunächst fehlgeschlagenen Logging-Audits sind nach Entfernen der
temporären Quellbelege und Ergänzung der Service-Liste separat grün. Weitere
abschließende Gruppen: 66 Editor-/API-/Supervisor-Tests, 31 Kern-Tests,
32 Mount-Pfad-Tests, drei Override-Tests, 100 Log-Tests. Das Suite-Gate verlangte
`focused`; keine vollständige Suite gestartet. Filter-/Draft-/Versionsgrenzen,
Native-Importisolation und Eingaben während Save sind mit Fixtures geprüft.

Die drei laufenden Claude-Implementierungsaufträge wurden nach dem Kostenhinweis
gestoppt. Die Teilstände wurden von Codex vervollständigt und geprüft; es gab
keinen abschließenden Claude-Review. Weitere Claude-Aufrufe erfolgten nicht.
Der Browser bleibt beim testbaren PB9-Editor. Nach der abschließenden Serial-
Änderung müssen API und PBRun den neuen Code laden; weitere Dienste oder
Browseraktionen werden nach dem Nutzerhinweis nicht mehr ausgelöst.

Auf ausdrücklichen Wunsch wurde anschließend ein vollständiger manueller
Browserlauf mit einem temporären lokalen QA-Server und echter nativer PB9-
Validierung durchgeführt. Fiktive Konten und Instanzen lagen ausschließlich
unter `/tmp`; kein PBRun-Supervisor oder echter Trading-Prozess lief dort.
Geprüft: zwei native Saves mit erneutem Öffnen, erhaltene Filter und sparse
Seitenwerte, Inline-Coin-Override, Kopie auf ein zweites Testkonto, beide
Strategien, Backup-Entwurf und Rollback als neue deaktivierte Version,
abgelehnte PB8/PB9-Kontokollision sowie automatische WebSocket-Aktualisierung.
Sidebar bei 160 und 420 px ohne horizontalen Überlauf; danach 240 px.

Der Lauf fand und korrigierte drei Probleme: TWE einer deaktivierten Seite,
verbleibende Draft-ID nach Backup-Save und konkurrierende UI-Reads mit dem
ursprünglichen Zwei-Worker-Limit. Native Arbeit bleibt auf zwei Operationen
begrenzt; Run-/Market-Abfragen haben zusätzlich acht begrenzte Plätze.
Legacy-Abfragen behalten ihre bisherige Zweiergrenze. Neue Evidenz:
60 fokussierte Workflow-Regressionen und 140 API-/Owner-/Browser-Tests grün.
Testtab und temporärer QA-Server wurden anschließend geschlossen. Der
produktive API-/PBRun-Dienst wurde bei diesem QA-Lauf nicht neu gestartet.

### PB9 queue policy

PB9 now reuses the Backtest Settings dialog and PB8 controller lifecycle with a separate `[backtest_v9]` policy (autostart disabled, one slot by default). Automatic dispatch is oldest-first, rechecks policy at final admission and uses the existing cross-version slots. Shutdown drains owned preparation without terminating published native jobs. Strategy Explorer remains last; native optimizer/archive integration is still pending.


### Shared archive adapter

Native9 exports, summaries, read-only browsing, native charts, Add to Run and manual requeues now reuse the shared archive infrastructure. Native result/provenance bytes survive archive copy and config-group rename. Replacement/scheduled retests now reuse the shared engine with native queue/provenance, exact-target verification and recoverable replacement; optimizer archive settings remain blocked for9. Archive credentials stay server-side; native browsing does not run migration. Strategy Explorer remains the final integration.

### Native archive retests and schedules

PB9 now uses the shared Retest & Replace dialog and daily/weekly schedule engine.
Native queue/provenance and exact completion markers gate replacement; source
changes and ambiguous suites fail closed. Original results remain as private
recovery backups. Shared state transactions, per-target archive locks, durable
cycle IDs and joined cancellation cover API restarts. Fable 5.2 medium reviewed
a bounded sanitized excerpt once; finalization retries, scan isolation, partial
batches, cleanup ordering, pre-move rollback and unchanged schedule cycles were
verified and corrected with isolated regression tests. Different archive/backup
filesystems are rejected before copying rather than risking partial retirement.
