# Passivbot v9: gemeinsamer Entwicklungsplan

Stand: 14. November 2026. Status: Bestandsaufnahme und erste Codeänderung
von Claude geprüft; Restkorrekturen umgesetzt, weiterhin nicht verdrahtet.

## Vorgehen

Wir entwickeln in kleinen, abgeschlossenen Schritten direkt auf `main`.
Codex und Claude wechseln sich bei Umsetzung und unabhängigem Review ab.
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

## Nächster Auftrag an Claude

Claude hat die drei Korrekturen aus dem ersten Code-Review bestätigt und das
unverdrahtete Modul als commit-reif bewertet. Der aktuelle Stand umfasst:

- Git liest mit bereinigter Umgebung und ohne globale/System-Konfiguration;
  geerbte Repository- und Config-Overrides können den Commit nicht umleiten.
- Die eingelesenen Metadatenbytes werden mit dem ermittelten Commit nach
  ausschließlicher CRLF/LF-Normalisierung verglichen, ohne Git-Filter auszuführen.
  Bei anderer Abweichung bleibt `commit` leer und eine Diagnose wird gesetzt;
  die Generation beschreibt weiterhin das gelesene Schema.
- `None`, leere und reine Whitespace-Pfade werden vor jedem Dateizugriff
  abgelehnt. Nach `expanduser()` sind absolute Pfade zwingend; auch `Path("")`,
  `.` und `src/..` werden abgelehnt. Git-Testfixtures sind isoliert.

Aktuelle Codex-Validierung: `tests/test_passivbot_runtime.py`,
`tests/test_welcome_pb8.py` und `tests/test_pb8_installation.py` zusammen:
**128 passed in 8.01s**. Enthalten sind Tests gegen Git-Umleitungsvariablen,
staged/unstaged Metadaten, Änderungen während des Einlesens, nicht gesetzte
und relative Pfade, verlinkte Worktrees und einen echten autocrlf-Checkout,
einschließlich Ablehnung einer anschließenden Schemaänderung. Das ist kein
Nachweis eines Testlaufs unter Windows. Ausschließlich fokussierte Offline-Tests;
keine vollständige Suite und keine Bot-/Remote-Ausführung.
Das neue Modul ist noch nicht beim API-Start importiert; kein Serial-Bump.

Vor der Verdrahtung bleiben aus dem Review zu behandeln: stabile Fehlercodes
und eine klare Ablehnungsregel für Aufrufer, gesonderte PB7-Behandlung,
Dateigrößenlimit/BOM-Verarbeitung sowie Ausführung außerhalb des Event-Loops.
Der Kommentar zur AST-Prüfung ist eingegrenzt: dynamische Neubindung wird
nicht vollständig ausgeschlossen. Eine Git-Ref-Prüfung vor Checkout fehlt
weiterhin und gehört zur nächsten Phase.
Der Commit belegt nur die geprüften Metadatendateien. Für Backtest-Provenienz
in Schritt 4 ist zusätzlich der Dirty-Status des gesamten Baums nötig.
Ein vorhandener Commit trotz fehlender Metadatendatei ist kein Erfolgsnachweis;
Aufrufer müssen die Fehlerliste auswerten.

> Lies AGENTS.md, docs/architecture.md und diesen Plan. Prüfe ausschließlich
> lesend passivbot_runtime.py und tests/test_passivbot_runtime.py sowie die
> begleitenden Plan-/Changelog-Änderungen. Die neuen Python-Dateien sind
> möglicherweise noch untracked; lies sie direkt. Prüfe die Klassifikation
> ausschließlich über Schema-Major, unbekannte und nicht literale Schemas,
> fehlende Metadaten, Git-Zuordnung und den Nachweis ohne Codeausführung.
> Die Klassifikation ist keine Bereitschaftsprüfung; fehlende Paket-/Git-Daten
> werden unabhängig von der Generation als Fehler gemeldet. Bestehende
> Aufrufer bleiben absichtlich unverändert. Die drei mittleren Befunde sind
> bereits bestätigt; prüfe gezielt die nachfolgenden Restkorrekturen für
> absolute Pfade und CRLF/LF-Vergleich. Keine Installation, Bot-/Runtime-
> Änderung, Remote-Aktion oder vollständige Testsuite. Gib Befunde im Chat aus;
> ändere keine Dateien und starte keine nächste Implementierungsphase.

Nach bestandenem Review und Nutzerfreigabe für den Commit folgt die Planung
der Verdrahtung in `verify_passivbot_major()`, einschließlich seiner Git-Ref-
Prüfung vor einem Checkout. Die breite Funktion `pb8_runtime_status()` bleibt
in der ersten Änderung unberührt. Spätere Aufgaben können die Rollen tauschen.
