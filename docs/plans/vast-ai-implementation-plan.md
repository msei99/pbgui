# Umsetzungsplan: PB8 GPU-Optimierung über Vast.ai

Stand: 13. September 2026. Der gemeinsame Queue-Ablauf ist implementiert:
Cloud-Setup in der Queue, Auswahl im Editor, gespeichertes Ausführungsziel,
FIFO-Cloud-Jobs auf einer gemeinsamen Miete, Daten-Cache nach Prüfsummen,
getrennte Jobverzeichnisse, Ergebnisimport und gemeinsame Löschfrist.
Das System-Menü enthält keine eigene Vast-Seite mehr.

Öffentliches Image: `ghcr.io/msei99/pbgui-pb8-worker:ee2b7d4-queue-v1`,
Digest `sha256:0d827eb097a9d26c9088421097b4a9f0eacf660f08613871c48946ddf71a0a88`.
Paket-Sichtbarkeit public und anonymer Manifest-Zugriff bestätigt. Kein
Registry-Key im normalen Ablauf. Details zur Prüfung stehen in
[vast-worker-image-release.md](vast-worker-image-release.md).

Grenzen: Fresh-Runs ohne Pareto-Seeds, ein Coin, keine Fristverlängerung,
keine automatischen Ersatzmieten und keine installierten Boot-Dienste.
Nach Host-Reboot kann die bestehende Überwachung wiederaufgenommen werden.
Die bezahlte End-to-End-Abnahme des neuen gemeinsamen Ablaufs steht aus.

Die verbindliche aktuelle Bedienung dokumentieren die
[EN-Anleitung](../help/48_vast_gpu.md) und [DE-Anleitung](../help_de/48_vast_gpu.md).
Die folgenden ursprünglichen Planabschnitte sind Hintergrund und Ausbauideen;
abweichende Angaben zur separaten System-Seite, privaten Images oder einer
Miete pro Job sind durch den obigen Stand ersetzt.

## 1. Entscheidung und Zielumfang

PBGui steuert einen vollständigen PB8-Optimizer auf einer Vast-Instanz. PB8,
Python, CUDA und Rust liegen fertig gebaut im Worker-Image. Der Nutzer wählt
seine bestehende Optimize-V8-Konfiguration, das Ausführungsziel `Vast.ai GPU`
und ein konkretes Mietangebot. PBGui erledigt Vorbereitung, Anmietung, Upload,
Start, Überwachung, Ergebnissicherung, Import und bestätigte Instanzlöschung.

Die erste produktive Version umfasst:

- Ein Vast-Konto pro PBGui-Installation; ein Cloud-Job gleichzeitig. Lokale
  Optimizer behalten ihre eigene Ressourcensteuerung.
- Eine NVIDIA-GPU pro On-demand-Instanz, zunächst Fresh-Runs und Pareto-Seeds.
- Den getesteten Trailing-Martingale-/ETH-/Suite-Pfad einschliesslich Sweep
  als verpflichtenden Abnahmefall. Weitere Konfigurationen werden anhand der
  Fähigkeiten des ausgewählten Images geprüft, nicht pauschal freigegeben.
- Binance/Bybit und explizite Coin-Listen als erste Datenexport-Abdeckung.
  Multi-Coin, andere Strategien oder Overrides nur, wenn Image-Preflight,
  Datenpaket und Integrationstests diese Kombination abdecken; andernfalls
  verständliche Ablehnung vor Anmietung. Kein stilles Weglassen von Overrides.
- Vollständige Importierbarkeit in Results, Paretos und Pareto Explorer.
- Automatische Löschung nach Sicherung sowie unabhängige Fristüberwachung.

Nicht Teil der ersten Version: lokale CPU mit GPU-RPC, mehrere GPUs pro Lauf,
Spot/Interruptible, Serverless, automatische Ersatzmieten, persistente Volumes,
automatisches Resume auf anderer Hardware. Wiederverwendung für mehrere
Queue-Jobs ist durch die obige Korrektur ausdrücklich Teil des Zielumfangs. Native Checkpoints werden trotzdem gesichert.

Für diesen Ablauf sind nach heutigem Stand keine Änderungen am PB8-Quellcode
nötig. Änderungen betreffen PBGui und den eigenen Worker-Wrapper. Bei einer
später erkannten nativen Lücke wird diese separat beschrieben.

## 2. Einrichtung: wann reicht ein Vast-API-Key?

Empfohlener Standard für alle Nutzer: ein öffentlich lesbares, versioniertes
GHCR-Worker-Image. Damit entfallen GitHub-Login und Registry-Leseschlüssel im
normalen Queue-Setup. Jeder Nutzer verwendet seinen eigenen Vast-API-Key und
sein eigenes Guthaben. Konfigurationen und Kursdaten werden nur an seine
gemietete Instanz übertragen, nicht im öffentlichen Image veröffentlicht.
Das derzeit private Paket wurde nicht öffentlich gemacht; vor Veröffentlichung
sind das konkrete Image einschließlich aller Layer und die enthaltenen
Lizenzhinweise zu prüfen.


Im Zielzustand genügt dem Nutzer ein Vast-Konto mit Guthaben und ein geeigneter
API-Key. PBGui erzeugt den eigenen SSH-Zugang automatisch und registriert den
öffentlichen Schlüssel. Manuelle SSH-Kommandos, Docker-Builds oder persönliche
Vast-Templates sind nicht Bestandteil des normalen Ablaufs.

Das bisher getestete GHCR-Image ist privat. Dafür ist zusätzlich ein separater
Registry-Leseschlüssel erforderlich. Für die einfache allgemeine Nutzung ist
ein öffentlich lesbares, versioniertes Worker-Image empfohlen. Es enthält nur
Software, keine Nutzerkonfigurationen, Marktdaten oder Zugangsdaten. Das
bestehende private Paket wird nicht durch diesen Plan öffentlich gemacht.
Veröffentlichung eines bereinigten öffentlichen Images ist ein eigener
Release-Schritt mit Freigabe. Private Images bleiben optional unterstützt.

PBGui verwaltet den Image-Katalog inklusive Digest, PB8-Commit, Worker-Protokoll,
Build-Identität, Konfigurationsschema und unterstützten GPU-Funktionen. Ein
Lauf verwendet einen festen Digest, niemals ein veränderliches `latest`.
Das bestehende Image dient als getestete Basis; der produktive Wrapper und
die Kostenwächter benötigen einen neuen geprüften Image-Build.

Die Einrichtung unter `System → Vast.ai` enthält:

| Feld/Aktion | Verhalten |
| --- | --- |
| `API key` | Maskierte Eingabe; Speichern serverseitig mit privaten Dateirechten |
| `Test connection` | Konto-/Instanzabfrage und Angebotssuche ohne Anmietung |
| `Connection status` | Letzte erfolgreiche Prüfung und konkrete fehlende Berechtigung |
| `Registry access` | Nur bei privatem Image: separater Read-only-Zugang |
| `Defaults` | Stundenpreislimit, maximale Mietdauer, Jobbudget |
| `Active rentals` | PBGui-eigene Instanzen und offene Löschaktionen |

Benötigte Fähigkeiten: Angebote lesen, eigene Instanzen lesen/erstellen/löschen,
öffentlichen SSH-Schlüssel anbringen; optional Guthaben/Kosten lesen. Billing-
Schreibrechte für Zahlungsverwaltung oder automatische Aufladung sind nicht
Teil dieses Ablaufs. Die genaue Zuordnung zu Vast-Berechtigungsgruppen wird
mit einem eingeschränkten Test-Key geprüft; lesender Verbindungstest beweist
nicht automatisch die Erlaubnis zum späteren Erstellen und Löschen.

Cloud-Secrets bleiben lokal. Vorgesehen ist ein kleiner `VastCredentialStore`
auf Basis von `secure_files.py` und bestehenden Sperrmustern, getrennt vom
clusterweit verteilten CMC-/TradFi-Katalog. Keine automatische Veröffentlichung
oder Verteilung dieses Schlüssels an Bot-Server. Metadaten und Geheimnisse
werden getrennt gespeichert. Eine Speicherung mit 0600 ist Zugriffsschutz,
keine Behauptung einer bereits vorhandenen Verschlüsselung.

Schlüsselwechsel bei laufenden Jobs behält die alte Generation für deren
Cleanup verfügbar. Löschen der letzten benötigten Zugangsdaten wird während
offener Mieten/Löschaktionen blockiert. Browserantworten enthalten nur Status
und Referenz-IDs, niemals Schlüssel in URLs, HTML, Logs oder Local Storage.

## 3. Bedienung in Optimize V8

Die bestehende Seite bekommt `Execution: Local / Vast.ai GPU`. Das Ziel wird
als PBGui-Jobmetadatum gespeichert und ist vom nativen `optimize.backend`
getrennt. Die Originalkonfiguration wird beim Start nicht verändert.

Nach Auswahl von Vast zeigt `Choose GPU` eine anklickbare Angebotstabelle:

| Spalte | Bedeutung |
| --- | --- |
| GPU / VRAM | Modell, Anzahl und verfügbarer Speicher |
| CPU / RAM | Zugewiesene Ressourcen, nicht nur Gesamtkerne des Hosts |
| Price/hour | GPU/CPU-Miete plus berechneter Speicheranteil |
| Transfer | Eingangs-/Ausgangspreis pro GB |
| Network / Location | Bandbreite und Standort |
| Reliability / Verified | Provider-Angaben als Auswahlhilfe |
| Compatibility | Kompatibel oder konkreter Ausschlussgrund |

Filter: Modell, Mindest-VRAM/RAM/CPU, Höchstpreis, Zuverlässigkeit und Standort.
Standard: eine GPU, On-demand, passende CUDA-Hardware, günstige kompatible
Angebote zuerst. RTX 3090 mit 24 GB ist eine sinnvolle Vorauswahl für unsere
Konfiguration, aber keine fest eingebaute Modellbeschränkung. Kein erfundener
Geschwindigkeitsrang aus VRAM oder GPU-Modell allein.

Angebote werden mit kurzem Cache (z. B. 30 Sekunden), Zeitstempel und manueller
Aktualisierung geladen. Bei Auswahl und unmittelbar vor Erstellung werden
Verfügbarkeit und Preis erneut geprüft. Ein verschwundenes oder teureres
Angebot wird nicht eigenmächtig durch eine andere Maschine ersetzt.

Die Startübersicht zeigt Angebot, Datenmenge, Preisbestandteile, `Max rental
duration`, `Job budget` und die automatische Löschung. `Rent & start` bestätigt
diesen konkreten Auftrag einschliesslich Upload, Ausführung und Cleanup.
Danach sind keine weiteren Bestätigungen für die üblichen Schritte nötig.
In der ersten Version startet eine Cloud-Miete nur über diesen ausdrücklichen
Start; ein vorhandenes lokales Queue-Autostart darf nicht versehentlich mieten.

CPU-Worker: `Auto` startet bei unserem unterstützten Testprofil mit bis zu vier
Exact-Workern, begrenzt durch tatsächliche CPU-Quote und RAM. Fortgeschrittene
können die Zahl ändern. Der Worker überprüft nach Start cgroup-Quote, Affinity,
RAM und GPU; ein Host mit 64 sichtbaren Threads ist nicht automatisch eine
64-Core-Zuteilung. Vier ist ein gemessener Ausgangswert, kein universelles Optimum.

Queue/Status zeigen getrennt:

- Exakte Optimizer-Bewertungen und eingestelltes Iterationsziel.
- GPU-Vorbewertungen, Generationen und GPU-Auslastung/VRAM.
- Vorbereitungs-, Upload-, Rechen- und Downloadzeit.
- Mietstatus, Kostenschätzung, verbleibende Mietzeit und letzte Sicherung.
- `Stop & collect`: kontrollierter Optimizer-Abbruch, Sicherung, Import, Löschung.
- `Extend limits`: nur bei ausdrücklicher neuer Kostenfreigabe; beide Wächter
  müssen die neue Frist bestätigen, bevor die UI sie als wirksam anzeigt.

Logs verwenden `LogViewerPanel`. Die Angebotstabelle, Dialoge und Sidebar
verwenden bestehende Komponenten; UI-Texte Englisch, Anleitung EN/DE.

## 4. Vollständiger automatischer Jobablauf

1. **Snapshot und Vorprüfung, noch ohne Miete.** Originalkonfiguration,
   Overrides, Seeds und PBGui-Metadaten unveränderlich erfassen. Image-Version
   festlegen und statische GPU-Unterstützung prüfen. Ungeeignete Metriken wie
   ein vom Image nicht unterstütztes `gain_strategy_eq` konkret melden. Eine
   Änderung zu ADG nur als sichtbare, bestätigte Jobkopie anbieten, niemals
   automatisch das Ziel austauschen. Lokale fehlende NVIDIA/Torch-Installation
   darf einen entfernten Job nicht verhindern. Remote-Konfiguration nicht
   still durch eine inkompatible lokal installierte PB8-Version migrieren.

2. **Marktdaten vorbereiten.** Benötigte Coins, Exchanges, Szenariozeiträume,
   Warmup aus den gesamten optimierten Parametergrenzen und BTC-Referenzen
   bestimmen. Bestehende Market-Data-Dienste verwenden; fehlende öffentliche
   Kursdaten vor Anmietung lokal vervollständigen. Bei unbekannter Warmup-Grenze
   konservativ mehr vorhandene Historie exportieren und die Datenmenge zeigen.
   Snapshot darf während Uploads nicht durch einen laufenden Datensync wechseln.

3. **Paket fertigstellen.** Hash-Manifest, native Jobkonfiguration,
   notwendige Override-/Seed-Dateien und ausgewählte OHLCV-Shards zusammenstellen.
   Binance-Pfade nativ normalisieren. `.pbgui_sweep_cycles.json`, Szenariolabels,
   Holdout-Metadaten und Original-/Laufkonfigurationshash getrennt erhalten.
   Holdout-Kerzen nur übertragen, wenn der Auftrag tatsächlich Holdout ausführt.
   Kein pauschales Kopieren von `data/`, Nutzerverzeichnissen oder API-Dateien.

4. **Mietabsicht dauerhaft speichern.** Job-UUID, PBGui-Installations-ID,
   ausgewähltes Angebot, Image-Digest, Preisstand und Frist vor dem Create-Aufruf
   festschreiben. Kosten beginnen für unsere konservative Überwachung spätestens
   mit Erstellung, nicht erst beim Optimizerstart. Ein Create-Timeout führt
   zum Abgleich der eindeutigen Kennzeichnung, nicht zu einer zweiten Miete.

5. **Provisionieren und verbinden.** Image direkt über den Create-Aufruf
   konfigurieren; ein manuell angelegtes Vast-Template ist nicht erforderlich.
   SSH-Schlüssel automatisch registrieren, direkten Zugang bevorzugen. Host-Key
   an genau diese Instanz binden. Ein sicherer automatischer Erstkontakt ist
   ein Pflichtpunkt von Etappe 1: authentisierte Provider-Ausgabe mit Instanz-
   Bezug prüfen, andernfalls einmalige explizite Fingerprint-Freigabe. Ein
   unbestätigter `ssh-keyscan` allein ist kein Herkunftsnachweis. Kein globales
   Abschalten der Host-Key-Prüfung und kein gemeinsamer Host-Privatschlüssel im
   öffentlichen Image. Der reguläre Ablauf soll diesen Schritt automatisieren;
   die belastbare Bootstrap-Methode muss vor dem Produktstart nachgewiesen sein.

6. **Wächter und Worker prüfen.** Lokaler unabhängiger Supervisor ist bereits
   aktiv; Worker bestätigt seinen eigenen Fristwächter mit instanzbegrenztem
   Credential, Protokoll-/PB8-/Rust-Identität, GPU und Ressourcen. Ohne bestätigte
   Wächter kein Optimizerstart. GPU-Dry-Run prüft CUDA und nativen Evaluator.
   Kein accountweiter Vast-Key, Registry-Schreibtoken oder Exchange-Key auf
   dem Worker. Die Rust-Stempel- und Image-Layer-Fixes aus dem Test übernehmen.

7. **Übertragen und starten.** SFTP mit begrenzten Chunks, Wiederaufnahme und
   Hash-Prüfung. Erst bei vollständigem Manifest atomar freigeben. Exakt einen
   detached Optimizer pro Job starten; wiederholte Start-Anfragen liefern den
   vorhandenen Jobstatus. Image-eigener Wrapper führt native GPU-/Config-Prüfung
   aus. PB8 kann weiterhin öffentliche Marktmetadaten abfragen; Version 1
   verspricht keinen vollständig offline arbeitenden Worker.

8. **Überwachen und zwischensichern.** Kontrollierte Log- und Fortschrittsdaten
   abholen. Unveränderliche Pareto-Dateien und vollständige Ergebnisdatensätze
   regelmässig lokal sichern (Zielintervall 60 Sekunden). Native Checkpoints
   nur an nachgewiesen konsistenter Dateigrenze übertragen. UI meldet den
   tatsächlichen Sicherungszeitpunkt. Unvollständige Binär-Enden nicht importieren.

9. **Abschluss und Import.** Bei regulärem Ende oder Stop konsistentes finales
   Ergebnismanifest erzeugen. `all_results.bin`, Paretos, Checkpoint, Logs,
   Sweep-Plan und Herkunftsmetadaten herunterladen. Hashes, Pfade, Dateitypen,
   Grössenlimits und nativen Result-Parser prüfen. Atomarer Import in den
   konfigurierten PB8-Ergebnisordner; Job-ID macht Wiederholungen idempotent.
   Sweep-Auswahl und Pareto Explorer müssen ohne manuelle Reparatur funktionieren.
   Ein importierter GPU-Checkpoint wird nicht als CPU-kompatibles Resume beworben.

10. **Löschen und bestätigen.** Nach erfolgreicher Sicherung Instanz vollständig
    löschen. Abwesenheit wiederholt über die Provider-API bestätigen. Erst dann
    `Rental ended` setzen. Ein Fehler beim UI-Import darf die Miete nicht halten,
    wenn vollständige überprüfte Rohdateien bereits lokal liegen: Import lokal
    erneut versuchen. Diagnose und letzte gesicherte Ergebnisse bleiben erhalten.

## 5. Persistenz, Neustarts und Kostenkontrolle

Jobausführung und Miete sind zwei getrennte Zustände. Ein fertiger Optimizer
mit fehlgeschlagener Löschung muss als `Finished / Cleanup pending` sichtbar
bleiben. Vorschlag für die Ausführungszustände:

```text
queued → preparing → ready → provisioning → uploading → running
       → collecting → importing → completed
```

Jede Phase kann `failed` oder `cancelled` enden; Verbindungsprobleme ergänzen
`reconnecting`/`recovery_required`. Mietzustand separat:
`none → creation_pending → active → destroy_pending → deletion_verified`.
Jeder Übergang besitzt Zeitstempel, Generation, Owner und begrenzte Fehlerdaten.

Ein unabhängiger lokaler Supervisor je aktivem Cloud-Job überlebt den Neustart
der PBGui-API. Die API besitzt nur ihre Controller, Verbindungen und UI-Streams.
Jobzustand mit Cross-Process-Lock und atomarem Dateiaustausch; nur ein Supervisor
darf denselben Job übernehmen. Beim Start werden vorhandene Jobs und offene
Mieten rekonstruiert. API-Shutdown schliesst eigene Tasks sauber, beendet aber
nicht den persistenten Cloud-Job. Automatischer Neustart des Supervisors erfolgt
über die vorhandene unterstützte Dienstverwaltung; fehlende zuverlässige
Supervision ist ein Preflight-Fehler, keine stille Hintergrund-Abschwächung.

| Fall | Festgelegtes Verhalten |
| --- | --- |
| Doppelklick auf Start | Eine Job-ID und eine Mietabsicht; keine zweite Instanz |
| Create-Antwort fehlt | API-Bestand anhand Kennzeichnung/ID abgleichen; keine blinde Wiederholung |
| Angebot verschwunden | Vor kostenpflichtigem Ersatz neue Auswahl verlangen |
| Image lädt nicht / SSH nicht erreichbar | Eigene Startfrist, kurze Diagnose sichern, Cleanup auslösen |
| SSH unterbrochen | Job läuft weiter; begrenzte Wiederverbindung, kein Neustart des Optimizers |
| PBGui-API neu gestartet | Supervisor und Remote-Job laufen weiter; UI rekonstruiert Status |
| Gesamter PBGui-Rechner offline | Worker-Fristwächter beendet die Miete nach vereinbarter Frist; später letzter lokaler Stand verfügbar |
| Daten-/GPU-Preflight fehlschlägt | Kein Optimizerstart; Diagnose holen und Instanz löschen |
| Download oder lokale Platte fehlerhaft | Bis zur vereinbarten Sammelfrist erneut versuchen, Zustand/letzte Sicherung sichtbar halten |
| Budget/Frist erreicht | Früh genug Optimizer stoppen und Daten sammeln; danach Löschung gemäss Startvereinbarung |
| Instanz gelöscht/Host verloren | Lokale Sicherung erhalten, Job als unvollständig kennzeichnen; keine automatische neue Miete |
| Destroy schlägt fehl | Cleanup persistent weiter versuchen, sichtbare Warnung; keine zweite Cloud-Miete zulassen |

Für Version 1 gilt die vor Start angezeigte Kostenpriorität: Nach Ablauf der
maximalen Mietfrist wird auch bei fehlgeschlagenem finalem Download gelöscht.
Dadurch können seit der letzten lokalen Sicherung entstandene Daten verloren
gehen. Niemals unbegrenzt weiter mieten, um einen Download zu retten. Eine
Verlängerung muss vor Fristablauf ausdrücklich erfolgen. Kein Versprechen,
dass zugleich vollständiger Datenerhalt und ein hartes Rechnungslimit bei
beliebigem Provider-/Netzausfall garantiert werden können.

Kostenmodell: GPU-/CPU-Preis, Speicher, geschätzter/erfasster Transfer und
Laufzeit getrennt speichern. UI unterscheidet Schätzung und vom Provider
gemeldete Kosten. Frist konservativ aus Stundenpreis, Budget, Transferreserve
und maximaler Mietdauer ableiten; Vorbereitung/Download und Sicherheitsreserve
einplanen. Bei kleinem Budget keine Miete ohne ausreichende Reserve. Ohne
Provider-Erreichbarkeit ist dies kein garantiertes hartes Abrechnungslimit.
`Stop` allein beendet Speichergebühren nicht; Standardabschluss ist `Destroy`.

## 6. Geplante Komponenten und Schnittstellen

Die Namen neuer Dateien/Routen sind Entwurf; Funktionen bleiben fachlich getrennt.

| Komponente | Aufgabe / bestehender Anknüpfungspunkt |
| --- | --- |
| `api/vast.py` | Authentisierte Account-, Angebots- und Mietstatus-Routen |
| `vast_provider.py` | Feste Vast-Endpunkte, Bearer-Header, Pagination, Timeouts, Backoff, normalisierte Preise und Fehler |
| `vast_credentials.py` | Lokal begrenzte Secrets mit Referenzen/Generationen und privaten Dateirechten |
| `vast_jobs.py` | Jobvertrag, Zustandsübergänge, Sperren, Besitz-/Fristprüfung |
| `vast_job_runner.py` | Unabhängiger Supervisor, Abgleich, Transfer, Cleanup; nutzt keine FastAPI-Imports |
| `vast_data_bundle.py` | Datenplanung, Snapshot und Hash-Manifest; fachliche Helfer aus Benchmark-Exporter übernehmen |
| `vast_results.py` | Sichere Ergebnissicherung, atomarer idempotenter Import und Sweep-Metadaten |
| `setup/vast_worker/` | Produktiver Image-/Runner-/Fristwächter-Build mit reproduzierbarer Version |
| `api/optimize_v8.py` | Ausführungsziel und Cloud-Job-ID an bestehende Queue anbinden; Remote-Status statt lokaler PID-Heuristik |
| `pb8_config_helper.py` | Zielabhängige Preflight-Grenze: lokale CPU-only-Vorprüfung und Image-Capabilities |
| `frontend/vast.html`, `frontend/js/vast.js` | Account-Einrichtung und eigene Mieten im vorhandenen UI-Stil |
| `frontend/v7_optimize.html` | Bestehende für V8 verwendete Seite: Angebotspicker, Startübersicht und Cloud-Status |
| `PBApiServer.py`, Navigation, Guides | Router-/Controller-Lifecycle, Seitenregistrierung, Hilfe EN/DE |

Eigene HTTP-Clients und SSH-Verbindungen besitzen TTL, Grenzen und klare Owner.
Bestehende AsyncSSH-Muster verwenden, Vast aber nicht als normalen Bot-VPS
registrieren. Providerantworten und Dateiinhalte bleiben untrusted: keine
freien Remote-Shell-Kommandos aus dem Browser, keine Symlink-/Pfad-Escapes,
keine Secret-Ausgabe und keine ungeprüfte Pickle-Deserialisierung beim Import.

Vorgeschlagene API, alle via `require_auth`, browserseitig gleiche Origin:

| Route | Zweck |
| --- | --- |
| `GET /api/vast/settings` | Ausschliesslich öffentliche Einstellungen/Secret-Status |
| `POST /api/vast/credentials` | Schlüssel speichern/ersetzen; keine Secret-Rückgabe |
| `POST /api/vast/connection-test` | Lesender Kontotest |
| `GET /api/vast/offers` | Begrenzte, validierte Angebotssuche |
| `POST /api/vast/quotes` | Kurzlebige Preis-/Ressourcenübersicht für konkreten Job |
| `GET /api/vast/rentals` | Eigene Mietzustände und offene Cleanup-Fälle |
| Bestehende Optimize-V8-Queue-Routen | Snapshot, Start und Stop um Ausführungsziel erweitern |
| `POST /api/vast/jobs/{id}/extend` | Explizite Budget-/Fristverlängerung |
| `POST /api/vast/jobs/{id}/collect` | Erneute lokale Sicherung ohne weitere Miete |
| `POST /api/vast/jobs/{id}/cleanup` | Bereits autorisierte Löschung erneut anstossen |

Start bindet Job-Snapshot-Hash, Quote-ID und Request-ID aneinander. Lange Arbeit
antwortet mit Job-ID/HTTP 202. Browsersitzungs- und Provider-Credentials gelangen
nicht in Query-Parameter. UI-Polling verwendet Request-Generationen; Logs nutzen
die gemeinsame Anzeige und registrierte Streams. Fremde Vast-Instanzen dürfen
allenfalls lesend erkennbar sein, aber nie automatisch verändert werden.

## 7. Umsetzungsetappen und Abnahme

| Etappe | Konkretes Ergebnis | Abnahme |
| --- | --- | --- |
| 1: Verträge und Image | Job-/Manifest-/Capability-Schema, produktiver Wrapper, Registry- und SSH-Bootstrap-Konzept | CPU-only-PBGui kann einen Job statisch prüfen; Image enthält keine Nutzerdaten; Rust-Identität und Wächterstart geprüft |
| 2: Konto und Angebote | Sichere Einrichtung, Verbindungstest, Angebotsliste, Preisberechnung | Mit API-Key erscheinen kompatible Angebote; keine Anmietung beim Test/Refresh; private Registry optional |
| 3: Daten und Preflight | Unveränderliches Paket inklusive Sweep/Overrides/Seeds und vollständiger Datenplanung | Fehlende Daten vor Miete behandeln; Unsupported-Konfiguration verständlich blockiert; Quellkonfiguration unverändert |
| 4: Job und Cleanup | Genau eine Miete, Supervisor, Transfer, Start/Stop, Wiederverbindung und Fristen | Create-Timeout erzeugt keine Dublette; API-Neustart überlebt; Stop und Timeout führen zur bestätigten Löschung |
| 5: Ergebnisse und UI | Laufende Sicherungen, Result-Import und komplette Optimize-V8-Bedienung | Results/Paretos/Explorer inklusive `sweep_total_swept` verfügbar; exakte Iterationen und Screening getrennt; Kosten sichtbar |
| 6: Produktabnahme | Dokumentierte Gesamttests und ein begrenzter echter GPU-Lauf | Vollständiger Ablauf allein aus PBGui; Erfolg/Abbruch/Neustart und Cleanup nachvollziehbar; keine Mietreste |

Die Etappen bauen aufeinander auf. Die UI darf keinen bezahlten Start freigeben,
bevor Supervision und Cleanup aus Etappe 4 integriert sind. Ein separater
CPU-GPU-Geschwindigkeitsvergleich ist keine Voraussetzung für die Umsetzung.

Offline-Tests mit isolierten Verzeichnissen und gemockter Vast-/SSH-Anbindung:

- Credential-Zugriff, Redaktion, Rechte, Rotation bei aktiver Miete.
- Angebotsvalidierung, Preiskomponenten, abgelaufene Quotes und Änderungen.
- Daten-/Warmup-Abdeckung, Binance-Pfade, fehlende Quellen, Hash-/Snapshot-Stabilität.
- Sweep-Plan, Holdout-Abgrenzung, Seeds/Overrides und unveränderte Originaldateien.
- Doppelte Starts, unklarer Create-Ausgang, Crash nach Erstellung vor Persistenz.
- Restart/Reconciliation, PID-Reuse-Schutz, abgelaufene Fristen, Jobbesitz.
- GPU-OOM/Preflightfehler, SSH-Abbruch, partieller Upload/Download, volle Platte.
- Datei-Traversal, Symlinks, übergrosse Archive und unvollständige Ergebnisrecords.
- Import-Wiederholung, bereits gesicherte Rohdaten bei Importfehler, Sweep-Auswahl.
- Native GPU-Checkpoint-Kompatibilität nicht mit normalem CPU-Resume verwechseln.
- Löschfehler, Kontoschlüsselverlust, Worker-/lokaler Wächter und veraltete UI-Antworten.
- Regression bestehender lokaler PB7/PB8-Queues, API-Lifecycle und EN/DE-Guide-Parität.

Echte Cloud-Abnahme erst nach Implementierung mit konkretem kleinen Mietbudget:
ein regulärer GPU-Lauf, ein kontrollierter Stop und ein API-Neustart während
eines laufenden Jobs. Provider-Ausfall/Dublettenszenarien offline simulieren,
nicht durch riskante Versuche auf fremden oder produktiven Instanzen.

## 8. Repository-Auswirkungen und Fertigstellung

GitNexus ist an `pbgui` gebunden, Index-Commit `a1bbc44`. Die abgefragten
bestehenden Laufzeitdateien sind indexiert; der Index meldet zwischenzeitlich
geänderte/neue Planungs- und Benchmark-Dateien. Vor Implementierungsedits
Index aktualisieren und Impact auf die konkret geänderten Symbole wiederholen.

Die aktuelle Vorprüfung ergibt:

- `OptimizeV8Worker`: MEDIUM, zwölf betroffene Symbole/Dateien, direkte
  Abhängigkeiten unter anderem API-Start, Services, Backtest und Pareto Explorer.
- `_optimize_preflight`: LOW, vier betroffene Symbole über den Helper-Aufrufpfad.
- `CredentialStore`: HIGH, 110 betroffene Symbole/Dateien; direkte Abhängigkeiten
  unter anderem API-Keys, Cluster, Market Data und CMC. Deshalb in Version 1
  kein Umbau des gemeinsamen Katalogs für die Vast-Zugangsdaten.

Die Impact-Abfragen lieferten keine benannten betroffenen Prozesse; dies ist
keine Garantie für unbetroffene dynamische Aufrufpfade. Quelltextprüfung und
gezielte Regressionstests bleiben erforderlich.

Bei Implementierung: `api/serial.txt` für Änderungen an API/Startup erhöhen,
JS/CSS-Cache-Versionen anpassen, produktive Seite in `FASTAPI_PAGES` und
`GUIDE_TOPICS` aufnehmen und EN/DE-Hilfe ergänzen. Changelog pflegen, passende
Offline-Suite ausführen, vor Commit GitNexus-Änderungsanalyse abschliessen.
Commit, Push und Image-Veröffentlichung benötigen jeweils die entsprechende
Freigabe. Dieser Planungsschritt ändert keinen Laufzeitcode und mietet nichts.

## Quellen

- [Vast-Angebotssuche](https://docs.vast.ai/api-reference/search/search-offers):
  Angebote bilden die Grundlage der Auswahl.
- [Instanz erstellen](https://docs.vast.ai/api-reference/instances/create-instance):
  Image/Startparameter direkt über API; Registry-Anmeldung für private Images.
- [API-Keys](https://docs.vast.ai/guides/reference/api-keys): Kontozugang verwalten.
- [SSH](https://docs.vast.ai/guides/instances/connect/ssh): Schlüssel, direkter
  und Proxy-Zugang sowie Dateitransfer.
- [Instanzverwaltung](https://docs.vast.ai/guides/instances/manage-instances):
  Stop erhält Speichergebühren; Destroy entfernt Instanzdaten.
- [Preise](https://docs.vast.ai/guides/instances/pricing): Preisbestandteile.
- [Eigener Messbericht](vast-ai-gpu-benchmark-results.md): geprüfte Image-Basis,
  Daten-/Result-Transfer, Worker-Skalierung und bereits bestätigte Löschung.
