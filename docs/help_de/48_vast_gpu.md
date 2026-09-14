# Vast.ai GPU-Queue

Die Cloud-Optimierung ist direkt in **PB8 Optimize** integriert. Eine separate
Vast-Seite unter System gibt es nicht mehr. Jeder Nutzer verwendet sein eigenes
Vast-Konto und Mietguthaben.

## Optimizer Settings

**Settings** öffnet lokale Ausführung, Cloud-Konto und GPU-Anforderungen. **Guide** springt hier direkt zu diesem Abschnitt. **Back to Queue** führt zur Jobliste zurück.

### Lokale Ausführung

CPU und **Override config CPU** steuern lokale Optimizer-Worker. **Use PBGui Market Data** verwendet vorbereitete PBGui-Daten; **Autostart** steuert die lokale Queue. Diese Optionen sind unabhängig von den Cloud-GPU-Anforderungen darunter. Lokale Änderungen mit **Save**, Cloud-Anforderungen mit **Save settings** speichern.

### Vast API key

1. Auf der [Keys-Seite der Vast-Konsole](https://console.vast.ai/manage-keys/) anmelden und unter **API Keys** auf **+New** klicken.
2. Den Key `PBGui` nennen. Benutzerdefinierte/eingeschränkte Rechte wählen und die folgenden Kategorien aktivieren.
3. **Create** klicken und den einmalig angezeigten Key kopieren.
4. In PBGui **PB8 Optimize → Settings → Cloud setup** öffnen. Unter **Vast API key** einfügen, **Save key** und danach **Test connection & refresh balance** klicken.

| Berechtigung | Verwendung in PBGui |
| --- | --- |
| `user_read` | Konto und verfügbares Guthaben |
| `misc` | GPU-Angebote suchen |
| `instance_read` | Instanzstatus und Logs |
| `instance_write` | Mieten, starten, stoppen, löschen und Worker-SSH-Key zuordnen |
| `billing_read` | Tatsächliche Instanzkosten abfragen |

`billing_write`, `user_write`, Machine- und Team-Rechte werden hierfür nicht benötigt. Ein reiner Read-only-Key kann weder mieten noch Instanzen aufräumen. Billing **Read** erlaubt keine Guthabenüberweisungen.

Der Key bleibt mit Besitzerrechten im lokalen PBGui-Credential-Store; er wird nicht auf GPU-Worker oder Bot-Server kopiert. Weder ein manuell hinterlegter SSH-Key noch GitHub-Zugangsdaten sind nötig. PBGui verwaltet die Worker-SSH-Identität selbst. Guthaben separat bei Vast aufladen; PBGui lädt nichts nach.

Ein erfolgreicher Guthabentest bestätigt den Kontozugriff, nicht sämtliche Mietrechte. Bei Berechtigungsfehlern in Angebotssuche, Logs oder Start die passende Kategorie prüfen. Ohne `billing_read` schlägt die Kostenabfrage fehl; mit Berechtigung kann bis zur Buchung durch Vast weiterhin **Pending** stehen. Einen Ersatz-Key zuerst speichern und testen, danach den alten Key bei Vast widerrufen.

Quellen: [Vast-Key-Einrichtung](https://docs.vast.ai/guides/reference/api-keys) und [Berechtigungsreferenz](https://docs.vast.ai/api-reference/permissions).

**Create Vast.ai account** verwendet deinen PBGui-Empfehlungslink. Erst ein Klick
öffnet die externe Seite. Das öffentliche, versionierte Image
`ghcr.io/msei99/pbgui-pb8-worker` benötigt weder GitHub-Konto noch Registry-Key.
PBGui prüft vor einer bezahlten Miete den anonymen Zugriff auf den festen Digest.
Das Image enthält Software, keine Nutzerkonfigurationen, Kursdaten oder Keys.

## GPU-Anforderungen in den Settings

Im Editor nur **Execution → Vast.ai GPU** oder **Local** wählen.
Unter **Queue → Settings → GPU requirements** GPU-Typ, maximalen Stundenpreis,
Mindestwerte für VRAM/RAM/CPU, Disk-Größe und verifizierte Hosts festlegen.
**Preview available GPUs** dient zur Vorschau: Ein Klick auf eine Zeile übernimmt
nur den Typ, keine Angebots-ID. **Save settings** speichert die Vorgaben.
Ein leerer Typ erlaubt jede GPU, welche die übrigen Anforderungen erfüllt.
Die Suche akzeptiert Teilbegriffe unabhängig von Groß-/Kleinschreibung: `3090`
findet `RTX 3090`; Leerzeichen, Unterstriche und Bindestriche sind austauschbar.
Existiert der vollständige Modellname in Vasts Modellliste, hat er Vorrang.
Gespeicherte Teilbegriffe funktionieren auch beim Queue-Start.

Die Vorschau zeigt CPU-Modell/Zuteilung, GPU-Speicherbandbreite, maximale Mietdauer
und Kompatibilität zusätzlich zu Preis, Transferkosten, Netzwerk, Standort und
Zuverlässigkeit. **Details** klappt PCIe-Generation/Lanes/Bandbreite sowie
Datenträgermodell und Lesegeschwindigkeit auf, ohne einen GPU-Typ auszuwählen.
Fehlende Angaben erscheinen als **Unknown**. CUDA unter 13 oder unbekannte CUDA
sowie bekannte zu kurze Verfügbarkeit werden standardmäßig ausgeblendet.
**Show incompatible hosts** zeigt sie mit Begründung; geänderte Filter mit
**Preview available GPUs** anwenden. Kompatibilität ist eine Prüfung der
Marktplatzangaben, kein Benchmark oder Leistungsversprechen. Die Verfügbarkeit
gilt zum Suchzeitpunkt und wird beim Start erneut geprüft.
Die Einheiten folgen der [Vast-Angebots-API](https://docs.vast.ai/api-reference/search/search-offers).

Beim Klick auf **Start** in der Jobzeile sucht PBGui aktuelle Angebote und wählt den
niedrigsten Stundenpreis unter passenden Single-GPU-Hosts mit CUDA 13.
Die CPU-Mindestzuteilung wird bei Bedarf für die wartenden Jobs angehoben.
Typ und Grenzen werden nicht stillschweigend gelockert. Ohne passendes Angebot
startet keine Miete; später erneut versuchen oder die Anforderungen ändern.
Verschwindet ein Angebot zwischen Suche und Bereitstellung, kann der Start
weiterhin scheitern. PBGui erzeugt keine nicht freigegebene Ersatzmiete.
Der Stundenpreis enthält Disk; Transfers kosten zusätzlich. Der Worker prüft die
tatsächliche CPU-Quote. Gespeicherte Vorgaben gelten für die nächste Miete.

**Save & Queue** speichert die aktuelle Konfiguration und erstellt einen festen
Eingabestand. **Queue Selected** beachtet ebenfalls das gespeicherte Ziel. Lokale
Konfigurationen bleiben im bisherigen lokalen Ablauf. Cloud-Jobs erscheinen als
gemeinsamen Tabelle unter **Queue**, erkennbar an der Spalte **Execution**.

Aktuell unterstützt: frische Multi-Coin-Läufe mit `ema_anchor` oder
`trailing_martingale`, Binance/Bybit und reine Datumsszenarien. HSL, Overrides,
BTC-Collateral und Successive Halving werden vor der Miete abgewiesen.
Pareto-Seeds werden nicht übernommen. `gain_strategy_eq` wird vom festen
GPU-Backend nicht unterstützt. Gegebenenfalls bewusst ein unterstütztes Ziel
wie `adg_strategy_eq` im Editor auswählen; PBGui ändert das Ziel nicht heimlich.
Die vorhandene Historie des Coins und der BTC-Referenz wird inklusive Warmup
übernommen. Fehlende Daten zuvor über Market Data laden.

## Prüfung der GPU-Konfiguration

**Execution → Vast.ai GPU** startet automatisch die Prüfung gegen das feste
Worker-Image, unabhängig vom lokalen GPU-Backend. Änderungen lösen eine neue
Prüfung aus. Fehlerhafte Scoring-/Limit-Zeilen und zugeordnete Felder werden rot
markiert; das Prüffeld nennt Feldpfade und Gründe. Neue Metrik-Auswahlen sind auf
das Cloud-Profil beschränkt. Bestehende Werte werden niemals automatisch ersetzt.

**Save & Queue** ist während der Prüfung und bei Fehlern gesperrt. Ansonsten
gültige Konfigurationsentwürfe können weiterhin normal gespeichert werden.
Vor dem Einreihen wird die aktuell erfasste Konfiguration nochmals geprüft.
Serverseitig gelten dieselben Regeln vor jedem Datenexport, auch bei Queue Selected
oder externen API-Aufrufen. Geprüft werden Profilgrenzen, Metriken, Ziele,
Penalty-Werte/Bereiche, Szenario-Bezüge und Reducer sowie GPU-Größen und Bounds.
Die Regeln sind an Image-Digest und PB8-Revision gebunden; für unbekannte Images
werden die alten Regeln nicht übernommen. Datenverfügbarkeit/Export und native
Runtime-/Geräteprüfungen folgen zusätzlich. Eine erfolgreiche Konfigurationsprüfung
garantiert deshalb noch keinen erfolgreichen GPU-Lauf.

## Eine Miete für mehrere Jobs

Unter **Queue → Settings** maximale Stunden, Budgetziel und Leerlaufregel zusammen mit den GPU-Anforderungen speichern. **Start** in der Jobzeile mietet direkt mit diesen gespeicherten Vorgaben, ohne weitere Bestätigung, und sucht ein aktuelles passendes Angebot.
PBGui mietet einmal, richtet verifiziertes SSH ein und arbeitet die Jobs
nacheinander auf derselben GPU ab. Ergebnisse werden jeweils vor dem nächsten
Start lokal gesichert und importiert. Die CPU-Parallelität pro Lauf kommt aus
der jeweiligen Optimizer-Konfiguration.

Auch später hinzugefügte passende Jobs verwenden denselben Worker. Kursdaten
werden anhand ihrer Prüfsummen zwischengespeichert; nur fehlende oder geänderte
Dateien werden erneut übertragen. Jeder Job besitzt eigene Eingabe- und
Ergebnisverzeichnisse. Jobs mit zu hohem CPU-Bedarf oder unzureichender
Transferreserve bleiben mit Begründung wartend.

Mietfrist und Budget gelten für die gesamte Miete und werden durch Folgeläufe
nicht zurückgesetzt. Es gibt keine stillen Ersatzmieten. Budget oberhalb der
maximalen Mietkosten steht für weitere Job-Transfers zur Verfügung.

## Anzeige und Steuerung

Der Worker zeigt Mietstatus, Stundenpreis, Frist und Leerlauf-Löschzeit. Die
Jobanzeige unterscheidet exakte Auswertungen und GPU-Vorselektionen. Paretos und
Logs werden ungefähr jede Minute gesichert. **Open last downloaded log** öffnet
den gemeinsamen Logviewer. Fertige Ergebnisse inklusive Sweep-Metadaten werden
in PB8 Results/Paretos importiert; der genaue lokale Pfad wird angezeigt.

- **Stop & collect** beendet den gewählten Job und sichert Ergebnisse. Bei aktiver
  Queue kann danach der nächste Job dieselbe GPU verwenden.
- **Pause queue** verhindert Folgestarts; der aktuelle Lauf geht weiter.
- **Start queue** setzt die Abarbeitung auf dem vorhandenen Worker fort.
- **End rental** pausiert die Cloud-Queue, beendet/sichert den aktuellen Lauf und
  löscht den Worker. Noch wartende Jobs bleiben erhalten.
- **Resume supervision** stellt die Überwachung derselben Miete nach einem
  Controller-/Host-Ausfall wieder her, ohne eine Ersatzinstanz zu mieten.

Ohne passende Arbeit wird die Instanz sofort oder nach den gewählten fünf
Minuten Leerlauf gelöscht. Neue passende Jobs heben den Countdown auf. Auch eine
pausierte Queue unterliegt dieser Leerlaufregel. Erst **deletion_verified**
bestätigt die Löschung; blosses Stoppen kann weitere Speicherkosten verursachen.

## Grenzen und Wiederaufnahme

OpenSSH und ein funktionierender systemd-User-Manager sind erforderlich.
Controller und Mietwächter überleben Browser-/API-Neustarts. Ein entfernter
Wächter versucht die Löschung unabhängig zur festen Frist. Nach einem Neustart
des PBGui-Rechners mit **Resume supervision** die temporären Dienste wiederherstellen.

Das Budget ist keine harte Vast-Ausgabensperre. Zur Frist hat die Löschung Vorrang
vor unvollständiger Sicherung; Provider-Ausfälle können sie verzögern. Grenzen
pro Job: 10 GiB Eingabedaten, zwei Uploadversuche, 2 GiB Ergebnistransfers.
Rohbackups bleiben unter `data/vast/jobs/<job-id>/`. Checkpoints bleiben dort und
werden nicht für automatisches lokales Resume importiert. Zwischensicherungen
enthalten Paretos und Logs, keine vollständige Optimizer-Binärdatei.

Der neue gemeinsame Queue-Ablauf ist offline und im Browser getestet. Ein
bezahlter End-to-End-Abnahmelauf dieses Ablaufs steht noch aus.

Die Prüfung akzeptiert feste PB8-Bounds und `[Minimum, Maximum, Schrittweite]`, einschließlich `0` oder `null` für kontinuierliche Werte. Bei echten Fehlern werden Alternativen samt Auswirkungen gezeigt, etwa tägliches Wachstum über ADG oder Beibehalten des bisherigen Ziels mit Local. Vorschläge verändern keine Konfiguration. Ab dem siebten Fehler ist die weitere Liste aufklappbar.

Das Prüffeld verwendet das Editor-Design. **Use ADG** ersetzt nur die betroffene Scoring-Metrik im aktuellen Entwurf; Zielrichtung und Szenario bleiben erhalten. **Use Local** wechselt das Ausführungsziel. Beide aktualisieren die Prüfung, ohne automatisch zu speichern oder einzureihen. Eine offene Inline-Scoring-Bearbeitung vor der ADG-Übernahme abschließen.

Hilfen zur Ausführung, Worker-Version und Erklärungen der Alternativen stehen im Hover. Sichtbar bleiben nur echte Fehler und ihre Aktionsknöpfe; erfolgreiche und laufende Prüfungen erzeugen keinen Infoblock. Während der erneuten Prüfung bleibt der bisherige Fehlerinhalt stehen. Unveränderte Ergebnisse erhalten DOM, Fokus und aufgeklappte Details.

Bei Vast-Ausführung behält **Apply Training Scenarios** vorhandene Scoring-Einträge bei, einschließlich einer bewusst gewählten ADG-Metrik. Nur eine leere Scoring-Liste erhält GPU-kompatible Standardziele. Das lokale Preset bleibt unverändert.

Beim Speichern einer PB8-Optimizer-Konfiguration werden auch Vorlage und Eingaben des Scenario Generators gespeichert, bereits vor Preview oder Apply. Beim erneuten Öffnen erscheinen diese Werte wieder. Bei älteren Konfigurationen werden sie aus den Metadaten der angewendeten Vorlage übernommen, sofern vorhanden. Änderungen der Generator-Eingaben allein ersetzen die angewendeten Szenarien nicht.

Lokale und Cloud-Jobs stehen in derselben **Queue**-Tabelle. **Execution** zeigt **Local** oder **Vast.ai**. Der Cloud-Status **ready** erscheint als **queued**. **Open log** in der Jobzeile öffnet Details und verfügbare Logs; **Close** blendet sie aus. Mietlimits, Konto und GPU-Anforderungen werden in Settings gespeichert. **Start** lässt die Queue sichtbar. **Open log** öffnet dasselbe schwebende Optimize-log-Fenster wie bei lokalen Jobs, mit Cloud-Fortschritt, Status und den Aktionen Pause, Stop, Mietende und Wiederaufnahme. Während der Bereitstellung wird dort bereits der Status angezeigt, bevor ein heruntergeladenes Log vorliegt. Erklärungen stehen in der Hover-Hilfe.

Die GPU-Prüfung bietet **Replace metric** mit unterstützten Scoring-Metriken, **Edit limit** oder **Remove limit** für nicht unterstützte Grenzen und **Choose coins** für ungültige Coin-Listen. Änderungen betreffen nur den Entwurf. Ein Metrikwechsel ändert die Bedeutung; das Entfernen eines Limits lockert die Einschränkungen. Erklärungen stehen an gepunktet unterstrichenen Hover-Bezeichnungen neben den Aktionen, nicht auf den Knöpfen. **Use Local** erscheint einmal als Alternative.

Der feste Worker-Stand akzeptiert 1–64 unterschiedliche freigegebene Coins über beide Seiten. Die frühere Ein-Coin-Sperre stammte aus PBGuis erstem Benchmark-Profil und ist entfernt. Ein Multi-Coin-Lauf auf Vast wurde hier noch nicht getestet.

**Open log** erscheint bei vorhandener heruntergeladener Logdatei oder während Bereitstellung/Ausführung eines Cloud-Jobs. Wartende Cloud-Jobs haben einen Papierkorb statt Stop. Das Löschen eines inaktiven Queue-Eintrags entfernt ihn aus Planung und Anzeige; gespeicherte Dateien und Resultate bleiben erhalten. Aktive Jobs müssen vor dem Entfernen vollständig beendet und gesichert sein.

Die Spalte **Exchange** zeigt die Börsen des eingefrorenen Cloud-Jobs. Ältere Einträge verwenden ihr Exportmanifest; spätere Änderungen der Quellkonfiguration ändern diese Anzeige nicht.

Instanzabfragen verwenden einen kurzen gemeinsamen Cache. Bei HTTP 429 pausieren Provider-Abfragen automatisch entsprechend Retry-After. Die Löschprüfung verwendet frische Abfragen. Bis Vast eine Instanz bestätigt, zeigt das Log provisioning (instance not confirmed). Mietfehler behalten HTTP-Status und ursprünglichen Erstellungsfehler; eine unklare Erstellung wird nicht automatisch wiederholt.

Fehlgeschlagene Cloud-Jobs behalten **Open log** auch ohne heruntergeladene Logdatei, damit der gespeicherte Fehler zugänglich bleibt.

Ohne heruntergeladenes Optimizer-Log öffnet der Viewer **VastRunner.log**, das gemeinsame lokale Überwachungslog mit Bereitstellungs- und Rate-Limit-Fehlern. Es kann Meldungen anderer Cloud-Jobs enthalten. Sobald verfügbar, wechselt der Viewer zum Optimizer-Log des Jobs.

**Requeue** bereitet einen inaktiven fehlgeschlagenen/abgebrochenen Cloud-Job mit der gespeicherten Konfiguration und den bisherigen Evaluations-/Worker-Einstellungen erneut vor. Der alte Eintrag wird erst nach erfolgreicher Vorbereitung entfernt; seine Dateien bleiben erhalten. Requeue mietet keine GPU; Start steuert die Ausführung.

Start meldet einen Konflikt, solange die vorherige Miete beendet oder bereinigt wird. Wartende Jobs behalten Open log, solange eine gemeinsame Miete ungeklärt ist; dort bleiben Überwachung und Bereinigungsaktionen erreichbar.

Bei unklarer Erstellung wartet die Bereinigung bis zwei Minuten nach dem Versuch und verlangt danach zwei frische erfolgreiche Abfragen ohne passende Instanz. So kann Start vor dem Mietende freigegeben werden. Provider-Fehler und Rate-Limits zählen nicht als Abwesenheitsbestätigung.

Nach Klick auf Start zeigt die Queue-Zeile sofort **Starting…**, solange die Anfrage läuft. Cloud-Startknöpfe sind währenddessen gegen doppelte Aufrufe gesperrt.

Das Job-Logfenster bietet **Requeue** für inaktive fehlgeschlagene/abgebrochene Jobs und **Start** für vorbereitete Jobs. Requeue lässt das Fenster beim Ersatzjob geöffnet und startet keine Miete.

**Stop & collect** ist vor dem Start und nach Abschluss deaktiviert. Die Aktion ist während Bereitstellung, Upload, Ausführung und Ergebnissicherung verfügbar.

Während Requeue zeigt die Queue nur den vorbereiteten Ersatz als preparing. Schlägt die Vorbereitung fehl, bleibt der ursprüngliche Job für einen erneuten Versuch verfügbar.

Während der Bereitstellung ruft PBGui das Vast-Image-Downloadlog höchstens einmal pro Minute ab und zeigt es im vorhandenen Viewer. Sobald verfügbar, hat das Optimizer-Log Vorrang. Die Dateiliste hat eine begrenzte Breite; Aktivitäts- und Fehlertexte werden umgebrochen.

Der Image-Download bleibt durch die Mietfrist begrenzt. Das separate Zeitlimit von 15 Minuten für die Worker-Einrichtung beginnt erst, sobald Vast den Container als laufend meldet. Der SSH-Hostschlüssel wird anhand eines öffentlichen Schlüsseleintrags im Container-Log dieser Miete geprüft. Einrichtung und Wiederholungen verlängern die ursprüngliche Mietfrist nicht.

Nach einem Jobfehler pausiert die Queue; die eingestellte automatische Bereinigung bei Leerlauf bleibt aktiv. Sobald Vast die Löschung bestätigt, lässt sich diese Instanz nicht mehr fortsetzen. Requeue bereitet einen weiteren Versuch vor. Start benötigt eine neue Miete, wenn die vorherige bereits gelöscht wurde.

Wenn das zusätzliche Bereitstellungslog noch nicht verfügbar ist, versucht PBGui das Container-Log. Leere Antworten oder Meldungen über fehlende Dateien ersetzen keine vorhandene Logausgabe. Ohne nutzbares Log zeigt der Viewer an, dass er auf Provider-Logs wartet.

Das Optimize-Log zeigt GPU, VRAM, CPU/RAM, Speicher-/PCIe-/Disk-/Netzwerkbandbreite, Stunden- und Transferpreise, Host-Zuverlässigkeit sowie Budget/Mietfrist des gemieteten Angebots. Das sind gespeicherte Angebotswerte, keine Live-Geschwindigkeiten oder abgerechneten Gesamtkosten. Gepunktete Beschriftungen erklären die Werte beim Überfahren.

Während der Image-Bereitstellung zählt der Fortschrittsbalken erkannte Image-Schichten: heruntergeladen und bereit (entpackt oder bereits vorhanden). Da Schichten unterschiedlich groß sind, ist dies kein Prozentwert nach Datenmenge und keine Zeitschätzung. Danach folgt die Worker-Einrichtung; mit Beginn der Optimierung zeigt der Balken wieder die Auswertungen.

Uploads zeigen an SSH übergebene Bytes, Prozent und mittlere Geschwindigkeit, danach die Eingabeprüfung. Die Kostenkachel verwendet die von Vast gemeldeten Instanzkosten statt einer berechneten Job-Schätzung.

Optimizer-Logs werden während des Starts alle 15 Sekunden aktualisiert, auch vor
der ersten Auswertung. Vorbereitete Offline-Pakete können öffentliche Markt-Caches
enthalten; deren ursprüngliches Alter bleibt erhalten. Abgelaufene Caches blockieren den Start.

**Vast instance charges** zeigt den Betrag der Billing-API für die gesamte
Mietinstanz, einschließlich anderer Jobs auf dieser Instanz. Die Hover-Hilfe
zeigt GPU-, Speicher- und Transferbeträge sowie die Abrufzeit. Der API-Schlüssel
benötigt Billing-Leserechte. Die Abfrage wird fünf Minuten zwischengespeichert und
blockiert die Statusabfrage nicht. Pending bedeutet, dass noch kein Kostenposten
gemeldet wurde, nicht dass die Miete kostenlos ist.

Während der Optimierung werden geprüfte Ergebnissnapshots nach der ersten exakten Auswertung etwa jede Minute innerhalb des Transferlimits veröffentlicht. Results und Pareto Explorer im Joblog öffnen diesen Zwischenstand. Die abschließende Übertragung aktualisiert denselben Eintrag.

### Stopp bei ausbleibender Verbesserung

Neue PB8-Konfigurationen verwenden beim ersten Wechsel auf Vast **20.000 exakte Auswertungen**, sofern das Iterationsfeld noch nicht bearbeitet wurde. Gespeicherte Konfigurationen und ausdrücklich eingestellte Werte bleiben erhalten. Dies ist eine Obergrenze; die aktivierte Sättigungserkennung kann früher stoppen, garantiert dies aber nicht.

Setup bietet einen optionalen vorzeitigen Stopp (standardmäßig aus). Vorgaben: mindestens 512 exakte Auswertungen, danach 512 weitere Auswertungen Geduld und 0,1 % Verbesserungstoleranz. Jeder Job übernimmt die Einstellungen beim Start; Änderungen beeinflussen laufende Jobs nicht.

Nach dem Minimum bewertet PBGui die zulässige exakte Pareto-Front anhand des normalisierten Hypervolumens. Skala und Referenz werden mit der ersten Front festgelegt und bleiben unverändert. Unterstützt werden ein bis drei bereits zur Minimierung vorzeichenkorrigierte Ziele, einschließlich PB8-Suite-Auswertungen. Relative Verbesserungen über der Toleranz setzen das Geduldsfenster zurück; kleinere Verbesserungen summieren sich gegenüber dem zuletzt akzeptierten Stand. Proxy-Zahlen und verstrichene Minuten verbrauchen keine Geduld. Fehlende, ungültige, nicht unterstützte oder ausschließlich unzulässige Snapshots unterbrechen die Erkennung und starten das Beobachtungsfenster neu.

Das Joblog zeigt Phase, Auswertungen seit der letzten relevanten Verbesserung und verbrauchte Geduld. Die Prüfung folgt den verifizierten periodischen Ergebnissnapshots; der Stopp kann deshalb um ein Snapshot-Intervall verzögert erfolgen. Bei Stillstand fordert PBGui einen geordneten Stopp an, übernimmt die Endergebnisse und kennzeichnet einen erfolgreichen Stopp als abgeschlossen mit Konvergenzgrund. Anschließend folgt der nächste Queue-Job oder die eingestellte Leerlaufbereinigung. Das Verfahren ist eine Heuristik, kein Optimalitätsbeweis; Mietfrist und Iterationslimit gelten weiterhin.

Die geöffnete Results-/Paretos-Liste aktualisiert sich, sobald die nächste Jobabfrage (alle zehn Sekunden) einen neu veröffentlichten Snapshot erkennt: normalerweise etwa 60 Sekunden plus bis zu zehn Sekunden und Übertragungszeit. Elapsed misst die Optimizer-Laufzeit ohne Bereitstellung und Upload und bleibt nach Abschluss auf der gemeldeten Laufzeit stehen.

Die Live-Auslastung wird etwa alle 15 Sekunden über die bestehende SSH-Verbindung gemessen. CPU-Auslastung bezieht sich auf die effektive Container-Kapazität und benötigt zwei Messungen. RAM enthält den Container-Cache; VRAM und GPU-Aktivität stammen von nvidia-smi. Werte über 45 Sekunden sowie nicht verfügbare Messungen erscheinen als Strich, nicht als Null.

Neue Cloud-Jobs verwenden automatisch die gemessene CPU-Quote, auf ganze Worker abgerundet (mindestens einer), für n_cpus und gpu.exact_workers. Eine Quote von 9,6 ergibt neun Worker, auch wenn das Angebot zehn Kerne nennt. Die Quell-Config bleibt erhalten; eine getrennte geprüfte Ausführungskopie wird übertragen. Laufende Pools werden nicht umgestellt. Lokale Backtests aus Cloud-Ergebnissen entfernen den nur im Container gültigen HLCV-Pfad und verwenden lokale Daten, auch für Holdout-Zeiträume.

Stop & collect zeigt sofort **Stopping…**, danach **Stop requested**, bis die Überwachung **Collecting results…** meldet. Nach der Sicherung startet der nächste geeignete Queue-Job.

Uploads behalten per Prüfsumme bestätigte 8-MiB-Teilstücke bei SSH-Abbrüchen. Nur fehlende Teile werden erneut übertragen; vor dem Einrichten wird das gesamte Archiv geprüft. Wiederverbindungen erscheinen im Upload-Status.

Das Live-Log folgt der neuesten Zeile auch nach dem Umbruch langer GPU-Meldungen. Bewusstes Hochscrollen pausiert das Folgen; am unteren Ende wird es wieder aufgenommen.

CPU checks outstanding zählt eingereichte exakte Prüfungen, die PB8 noch nicht übernommen hat: wartende, laufende und bereits fertige Ergebnisse, die hinter früher eingereichten Prüfungen warten. PB8 meldet diese Gruppen nicht getrennt. Der Wert stammt aus Generationsprofilen; sein Alter wird angezeigt, ältere Werte sind als letzte Messung markiert. Der obere Exact-Zähler zählt übernommene Ergebnisse.

Beim abschließenden Einsammeln wird auch der finale Pareto-Snapshot geprüft, mit dem verifizierten Auswertungszähler des Imports. Die Anzeige kennzeichnet dies als finale Snapshot-Prüfung, nicht als Wiederholung jeder einzelnen Auswertung. Der ursprüngliche Stoppgrund (Sättigung, angeforderter Stopp oder Mietzeitlimit) bleibt erhalten; ein erst nachträglich erkanntes Erreichen der Schwelle ändert ihn nicht. Auch ein erneuter Ergebnisimport führt diese Prüfung aus. Ist die finale Front nicht auswertbar, bleiben die gesicherten Ergebnisse verfügbar und die Prüfung wird als nicht verfügbar angezeigt.

Die gepunkteten Info-Beschriftungen im Job-Log zeigen beim Darüberfahren Hilfetexte. Stagnation 0 / 512 bedeutet, dass der letzte geprüfte Snapshot eine Ausgangsbasis oder ausreichende Verbesserung festgehalten hat. Nach 512 weiteren exakten Prüfungen ohne ausreichende Verbesserung wird gestoppt und gesammelt. Since last improvement verwendet den aktuellen Exact-Zähler; der Stagnationsbalken den letzten geprüften Snapshot und kann deshalb zurückliegen. Der Tooltip zeigt Mindestanzahl, Geduld und Prozentschwelle des Jobs.

Zusätzlich zeigt das Log den zuletzt geprüften Exact-Stand, die letzte ausreichende Verbesserung und noch ungeprüfte neue Ergebnisse. Wiederholt null kann laufende Verbesserungen bedeuten, nicht eine stehengebliebene Prüfung.

Vast verwendet für Scoring und Limits den vollständigen PB8-GPU-Metrikvertrag einschließlich gültiger Aliasnamen. Aktueller Worker und lokales PB8 besitzen identische Quell-Hashes für Metrikschema, Registry und Berechnung: 157 unterstützte Metriknamen und 460 gültige Schreibweisen einschließlich Aliasnamen. Das sind nicht 460 verschiedene Kennzahlen. CPU-exklusive Metriken bleiben ausgeschlossen; andere GPU-Strategie- und Ausführungsgrenzen bleiben bestehen.

Ein neuer Run zeigt einen Wartehinweis, bis sein eigenes Provider- oder Optimizer-Log verfügbar ist. Frühere Starts aus dem globalen Runner-Log werden nicht als Ausgabe des neuen Runs angezeigt.

### Fehlgeschlagene UI-Anfragen wiederholen

- Worker-Aktionen zeigen einen Wartezustand und bleiben bis zum Ende der Anfrage gesperrt. Requeue zeigt **Preparing…** in der Queue; das Löschen eines Queue-Eintrags fragt nach einer Bestätigung.
- Kann die GPU-Validierung nicht abgeschlossen werden, hilft **Retry validation**. Normales **Save** bleibt verfügbar; **Save and Queue** benötigt eine erfolgreiche Validierung.
- Ein abgelehnter Vast-Key oder fehlende Provider-Rechte erscheinen als Provider-Fehler. Key/Rechte korrigieren und erneut versuchen. **PBGui session expired** bedeutet dagegen: erneut anmelden und die Seite neu laden.
- **Vast instance charges** zeigt nach einer fehlgeschlagenen Abrechnungsabfrage **Unavailable**. Ein bereits geladener Betrag bleibt als **last retrieved** sichtbar; der Hinweis am Feldtitel nennt Fehler und Abrufdetails.
- Fehlt die lokale Mietüberwachung, erklären Settings und Job-Log die systemd/OpenSSH-Voraussetzung. Jobs werden über **Open log** in der gemeinsamen Queue ausgewählt; dieses Fenster enthält Fortschritt, Fehler, Mietdetails und das zuletzt heruntergeladene Log.
- **Show incompatible hosts** aktualisiert die Angebotsvorschau. Die Auswahl eines Angebots übernimmt dessen GPU-Typ in die Anforderungen; diese vor dem Start speichern. Das Angebot wird dadurch nicht reserviert.
