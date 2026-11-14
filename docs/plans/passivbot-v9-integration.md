# Passivbot v9: gemeinsamer Entwicklungsplan

Stand: 14. November 2026. Status: Quellklassifikation und Guard-Schutz committed;
getrennte PB9-Laufzeiterkennung unabhängig geprüft und zum Commit freigegeben.

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
3. **Getrennte PB9-Erkennung, Review bestanden.** `pb9_runtime.py` prüft
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

- Git-Umgebungsbereinigung im Guard; der bisherige Ablauf bleibt hier erhalten.
- Bei breiterer Verwendung des Klassifikators: stabile Fehlercodes und klare
  Ablehnungsregel. Generation/Commit allein sind bei Fehlern kein Erfolgsnachweis.
  Der Guard benutzt Ausnahmen statt der Fehlerliste des Klassifikators.
- Vollständiger Dirty-Status für Backtest-Provenienz; der Klassifikator belegt
  nur die beiden Metadatendateien. Dynamische Neubindung wird nicht vollständig
  durch die AST-Deklarationsprüfung ausgeschlossen.
- Neue asynchrone Aufrufer dürfen synchrone Git-Lesevorgänge nicht direkt im
  Event-Loop ausführen. Der bisherige synchrone Guard-/Installer-Pfad bleibt so.
- Breite Status-/Archiv-Funktionen, v9-Installation und Laufzeit-Adapter folgen
  in getrennten Änderungen. Keine bestehende PB8-Installation wurde umgestellt.

## Abgeschlossener Claude-Review

Direkter CLI-Aufruf mit `claude-fable-5-2`, Effort `medium`, ausschließlich
Lesewerkzeugen und ohne Testwiederholung. Ergebnis: commit-reif, keine
blockierenden Fehler. Die 167 Tests sind Codex-Evidenz, kein Claude-Testlauf.

> Prüfe pb9_runtime.py und tests/test_pb9_runtime.py auf getrennte Quellbäume
> und Envs, statische Erkennung ohne Codeausführung und unverändertes PB8.
> Fehlender Commit und Rust-Stempel sind vorerst kein Erkennungshindernis.
> Nur lesen, nichts ändern; melde Fehler oder Commit-Reife.
