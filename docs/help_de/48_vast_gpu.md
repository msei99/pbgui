# Vast.ai GPU-Queue

Ist eine Mietsteuerung noch nicht verfügbar, erklärt ein Klick den Grund. Die Karten zeigen dafür keinen dauerhaften Starthinweis.

Ungespeicherte Eingaben für Budget, Transferreserve und Minuten bleiben bei automatischen Aktualisierungen des Log-Dashboards erhalten. Beim Wechsel zu einer anderen Miete werden sie verworfen. Änderungen gelten erst nach dem Speichern und der Bestätigung des Workers.

Beim Image-Download gewichtet der Fortschrittsbalken die komprimierten Layer-Größen aus dem Manifest des festgelegten Worker-Images. „At least … GB available“ zählt vollständig geladene und bereits gecachte Layer; laufende Downloads werden nicht geschätzt. Der Ready-Zähler zeigt das Entpacken separat. Bei unbekannten Image-Versionen bleiben Layer-Zähler sichtbar. Beim Veröffentlichen eines neuen Worker-Images muss `vast_image_layers.py` aktualisiert werden.

Die Cloud-Optimierung ist direkt in **PB8 Optimize** integriert. Eine separate
Vast-Seite unter System gibt es nicht mehr. Jeder Nutzer verwendet sein eigenes
Vast-Konto und Mietguthaben.

Das Log-Dashboard zeigt **Proxy / min**, **Exact / min**, **Proxy / exact** und **Exact / USD (estimate)**. Die Raten verwenden Zählerdifferenzen über ungefähr die letzten 60 Sekunden der nativen GPU-Logs mit Zeitstempeln (mindestens 10 Sekunden sind erforderlich). Messintervall und Alter werden angezeigt; nach 90 Sekunden ohne neue Probe verschwinden die Live-Raten. Beendete Jobs zeigen das letzte gemessene Intervall, keinen Gesamtdurchschnitt. Proxy/Exact beschreibt den kumulierten Auswahlaufwand, keine Ergebnisqualität oder Annahmequote. Exact/USD verwendet den Mietpreis pro Stunde und berücksichtigt weder Transfers noch Start- und Leerlaufzeiten. Fehlende Zähler bleiben unbekannt; die Populationsgröße dient nicht als Ersatz. Eine begrenzte Messhistorie wird bei den Job-Abfragen gespeichert und bleibt beim Ersetzen des Log-Ausschnitts sowie beim Neuladen der Seite oder API erhalten. Ein Update des entfernten Workers ist nicht erforderlich.

## Performance History

Die History-Aktionen (Aktualisieren, Öffnen, Vergleichen und Filtern) stehen in der Werkzeugleiste über der History-Tabelle. Die Sidebar enthält die fünf festen Vast.ai-Navigationspunkte.

Öffne **Vast.ai → Performance History** in der vorhandenen Sidebar. Die API speichert ungefähr alle 60 Sekunden lokale Job-Snapshots, auch ohne geöffneten Browser. Die private SQLite-Datenbank liegt unter `data/vast/performance.sqlite3`; beim Entfernen eines Queue-Jobs bleibt seine Historie erhalten. Bei gestoppter API pausiert die Aufzeichnung. Nach dem Neustart können noch vorhandene Log-Zähler übernommen werden; fehlende frühere Messungen lassen sich nicht rekonstruieren.

Markiere bis zu vier Zeilen per Klick, Tastatur (Leertaste/Enter) oder Ziehen. **View selected run** öffnet einen einzelnen Lauf, auch ältere unvollständige Einträge. **Same workload only** filtert die gesamte Historie nach der Aufgabe des ersten ausgewählten Laufs. **Compare hardware** ist nur bei identischen verifizierten Fingerabdrücken möglich; die API prüft dies ebenfalls. **Filter this page** durchsucht die geladene Seite nach Config, GPU, Machine ID, Coin oder Exchange. Previous/Next blättern durch ältere Einträge; **Refresh history** lädt neue Messwerte.

Der Fingerabdruck umfasst vorbereitete Config, Daten-Prüfsummen und PB8-Version/Worker-Image. Jobspezifische Pfade, lokaler Benutzername und zugeteilte CPU-Worker werden ausgeschlossen. Seed, Population, Objectives, Bounds, Szenarien und Datenzeiträume gehören weiterhin zur Aufgabenidentität. CPU-Zuteilung, GPU, Speicher und Mietpreis sind getrennte Vergleichsmerkmale. Fehlende oder inkonsistente Eingaben gelten als unverifiziert und werden nicht automatisch zusammengefasst. Gleiche Fingerabdrücke garantieren keinen identischen Cache-Zustand, Ablauf oder Zufallsverlauf.

Die Komplexitätsdaten unterscheiden konfigurierte Coins von exportierten Symbol-Datensätzen einschließlich BTC-Referenzen. Exportierte Kerzen werden anhand der NumPy-Header gezählt, ohne die Arrays zu laden; die tatsächlich vom Optimizer verwendete Kerzenmenge bleibt unbekannt. Nicht lesbare Header führen zu unbekannten Werten statt Schätzungen. Szenarienzahl, Zeitraum, Zeitauflösung, Population, Parameterzahl und Seed bleiben je Lauf erhalten. Vollständige Configs, Zugangsdaten und SSH-Details werden nicht in der Historie gespeichert.

Die Diagramme zeigen Proxy/min und Exact/min relativ zur ersten aufgezeichneten Zählermessung. Durchschnittswerte teilen die gemessene Gesamtarbeit durch die erfasste Zeit, statt Einzelraten ungewichtet zu mitteln. Sinkende Zähler unterbrechen das Intervall. Doppelte oder alte Beobachtungen zählen keine zusätzliche Arbeit; je Minute bleibt die neueste native Zählermessung erhalten. Auslastung behält ihren ursprünglichen Messzeitpunkt; veraltete Werte bleiben unbekannt. Phasen und echte Auslastungsmessungen werden unabhängig von neuen Zählern gespeichert.

Die Anlaufzeit bezeichnet den **ersten beobachteten** Zähler seit Container-Setup, ersatzweise seit Job-Erstellung. Spät beginnende Aufzeichnung kann diese Zeit überschätzen. Exact/USD verwendet Optimizer-Durchsatz und Stundenpreis. Die Kostenschätzung weist Container-Zeit und beobachtete Transfers separat aus; sie ist keine Provider-Rechnung und verteilt keine gemeinsamen Leerlaufkosten zwischen Jobs. Die Zahl vergleichbarer Host-Läufe berücksichtigt gleichen Fingerabdruck, gleiche Machine ID und ein messbares Zählerintervall. Es gibt keinen pauschalen Komplexitätsscore und keine automatisch angemieteten Benchmark-Läufe.


### Config workload comparison

PB8-Queue und Performance History zeigen **Est. coin candles / candidate**. Die Schätzung summiert inklusive Kalendertage × 1.440 ÷ Kerzenintervall × eindeutig gewählte Coins über aktive Trainingsszenarien. Eigene Coin-Auswahl und Ausschlüsse je Szenario werden berücksichtigt; derselbe Coin auf Long und Short zählt einmal. Ohne Suite Mode gilt der Basiszeitraum. Mehrere Börsen, Iterationen und Populationsgröße sind keine zusätzlichen Multiplikatoren. Holdout-Validierung, Warm-up, Datenlücken und tatsächlich verarbeitete Kerzen sind nicht enthalten. Unbekannte oder dynamische Eingaben zeigen **—**. Dies schätzt die Datenmenge einer vollständigen Kandidatenbewertung, weder Laufzeit noch Gesamtberechnungen des Runs. Die Queue verwendet den eingefrorenen Snapshot. Für ältere History-Einträge lässt sich die Schätzung nur bei noch vorhandenem Snapshot nachholen.

Zwei bis vier Runs markieren und mit **Compare configs** unterschiedliche Aufgaben vergleichen. **Compare hardware** verlangt weiterhin identische verifizierte Workload-Fingerprints. Proxy/min und Exact/min bleiben getrennt. **Minutes / 1k exact** = 1.000 ÷ gemessene Exact/min; **USD / 1k exact** = 1.000 ÷ gemessene Exact/USD. Dies sind Hochrechnungen aus dem gemessenen Durchsatz ohne Startzeit, Leerlauf und Transfers, keine Prognose der gesamten Mietkosten. Bei null oder unbekanntem Durchsatz erscheint **—**. GPU, CPU-Zuteilung, Worker-Version, Strategie und Sucheinstellungen beeinflussen ebenfalls den Durchsatz; der Config-Vergleich isoliert keine Hardwareleistung.

## Optimizer Settings

**Queue Settings** enthält nur den lokalen Autostart, die CPU-Anzahl, CPU-Override und die Auswahl der PBGui-Marktdaten.

Die Queue-Sidebar bietet fünf getrennte Vast.ai-Buttons:

- **Account**: API-Key, Verbindungstest und Guthaben. Zugangsdaten mit **Save key** speichern.
- **GPU & Offers**: Hardware-Anforderungen, maximaler Stundenpreis, Angebotsvorschau und manuelles Rent. Speichern mit **Save GPU requirements**.
- **Hosts**: bekannte/bevorzugte/funktionierende Hosts und gesperrte Maschinen, mit Statusfilter All/Preferred/Working/Blocked. Host-Aktionen werden direkt gespeichert.
- **Rental & Automation**: maximale Mietdauer, Budgetziel, Aufräumen bei leerer Queue und Stop on stagnation. Speichern mit **Save rental & automation**. Dies sind Vorgaben für zukünftige Mieten/Jobs.
- **Performance History**: aufgezeichnete Runs und Vergleiche gleicher Workloads.

Beim ersten Aufruf von GPU & Offers öffnet sich Account, falls noch kein Key eingerichtet ist. Jedes Formular speichert nur seine eigenen Felder. Ungespeicherte Eingaben anderer Bereiche und Änderungen während des Speicherns bleiben erhalten. Manuelles Rent verwendet die gespeicherten Mietregeln. Laufende Mieten steuerst du über Queue und Job-Log. **Guide** folgt dem aktuellen Bereich; **Queue** in der linken Sidebar führt zur Jobliste zurück.

### Vast API key

1. Auf der [Keys-Seite der Vast-Konsole](https://console.vast.ai/manage-keys/) anmelden und unter **API Keys** auf **+New** klicken.
2. Den Key `PBGui` nennen. Benutzerdefinierte/eingeschränkte Rechte wählen und die folgenden Kategorien aktivieren.
3. **Create** klicken und den einmalig angezeigten Key kopieren.
4. In PBGui **PB8 Optimize → Account** öffnen. Unter **Vast API key** einfügen, **Save key** und danach **Test connection & refresh balance** klicken.

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

**Create Vast.ai account** steht direkt unter der Account-Überschrift und verwendet deinen PBGui-Empfehlungslink. Erst ein Klick
öffnet die externe Seite. Das öffentliche, versionierte Image
`ghcr.io/msei99/pbgui-pb8-worker` benötigt weder GitHub-Konto noch Registry-Key.
PBGui prüft vor einer bezahlten Miete den anonymen Zugriff auf den festen Digest.
Das Image enthält Software, keine Nutzerkonfigurationen, Kursdaten oder Keys.

## GPU & Offers

Im Cloud-CPU-Modus **Auto** bestimmt **Min CPU cores** die Mindestleistung der Miete. Trage hier 16 oder 32 ein, wenn diese CPU-Zahl benötigt wird. Die CPU-Zahl einer lokalen Optimizer-Config überschreibt diesen Wert nicht; die Cloud-Ausführung verwendet die gemessene Zuteilung, begrenzt auf die gemietete CPU-Quote. Ältere Jobs mit einer explizit festen CPU-Anforderung behalten diese Anforderung.

PBGui bestimmt das vollstaendige abgestimmte CUDA-Profil, nachdem der gemietete Worker seine tatsaechliche Hardware gemeldet hat. Mit aktiviertem `auto_lean_parallelism` und leeren Groessenfeldern waehlt PBGui 2.048 Kandidaten bis 8 GB VRAM, 4.096 unter 11,5 GB und 8.192 fuer allgemeine Karten ab 11,5 GB. Eine Karte ab 15 GB mit mindestens 500 GB/s Speicherbandbreite und 25 TFLOPS verwendet 16.384; eine Karte ab 20 GB mit mindestens 600 GB/s und 25 TFLOPS verwendet 24.576. Fehlende Leistungsdaten fuehren nie allein wegen des VRAM zu einem groesseren Profil. Exact-Validierungen, Drift-Probes und Drift-Fenster wachsen proportional von 16/8/256 bei 2.048 Kandidaten bis 192/96/3.072 bei 24.576. PBGui belaesst die vollstaendige ausgewaehlte Population in einem Kandidaten-Dispatch und leitet dessen Candidate-Bar-Grenze aus dem groessten aktiven Szenario ab. PB8 startet einen CUDA-Thread pro Kandidat. Live-Messungen auf einer RTX 3060 bestimmten das 8.192-Profil fuer 12-GB-Karten; ein echter 41-Coin-Vergleich auf einer RTX 3090 steigerte den Gesamtdurchsatz einer Generation von 40,71 Kandidaten/s bei 12.288 auf 58,09 Kandidaten/s bei 24.576 und verwendete dabei 2,62 GB VRAM. Native PB8-Meldungen zu Dispatch und zeitlichem Replay liefern Zwischenfortschritt, sofern er unterstuetzt wird. PBGui schreibt alle aufgeloesten Werte vor dem Upload in die gepruefte Ausfuehrungskopie. Damit bleibt das Standardverhaeltnis von 128 geprueften Proxy-Kandidaten pro Exact-Validierung erhalten. Der Worker behaelt nur sichere Fallbacks fuer einen eigenstaendigen Start; bei einem PBGui-Vast-Job waehlt er das Profil nicht aus. Deaktiviere die automatische Parallelisierung oder trage Groessenwerte ein, um manuelles Tuning beizubehalten. Quellkonfiguration und vorbereiteter Snapshot bleiben unveraendert. Die GPU-Karte in Queue zeigt die angewandten automatischen Werte. Bei einer Ersatz-GPU wird vor dem Upload ein neues Hardwareprofil erzeugt, damit keine Einstellung der vorherigen Karte weiterverwendet wird.

Für einen neuen Vast-GPU-Optimizer-Job lässt du `optimize.gpu.population_size`, `batch_size` und `max_dispatch_candidate_bars` mit aktiviertem `auto_lean_parallelism` leer. PBGui wählt dann nach der Miete anhand der tatsächlich gemessenen GPU ein abgestimmtes Profil. Diese Felder sind weder für Save noch für Save & Queue Pflichtfelder. Für manuelles Tuning trägst du eigene Werte ein. **Apply measured suggestion** setzt alle drei Werte nur bei einem passenden ausgewählten Angebot und ändert keinen Job ungefragt; der Angebotsvergleich bleibt bis zur Laufzeitprüfung vorläufig. Die Dispatch-Vorschau erscheint bei expliziten Werten. Ein echter Optimizer-Lauf führt keinen vorgeschalteten Tuning-Test aus.

Öffne die PB8-Config und wähle **Run on → Vast.ai GPU**. Unter **GPU optimization → Population & dispatch** lässt du die drei Grössenfelder für die automatische Wahl nach der Miete leer oder trägst vor **Save & Queue** manuelle Werte ein. Bei diesen drei Feldern führt **−** von 1 zu Auto zurück und **+** von Auto zu 1. Explizite Config-Werte bleiben für diesen Queue-Job fest.

Nach Auswahl eines Angebots zeigt **GPU & Offers** jeden vorbereiteten Queue-Job mit Population, Batchgröße und Arbeitslimit, die Auto für diesen Job und diese Karte berechnet. Das Arbeitslimit wird in Milliarden Candidate-Bars angezeigt und eingegeben; PBGui speichert die exakte Ganzzahl. Für jeden Auto-Job kannst du Auto, eine mitgelieferte Messung für exakt diese Arbeitslast und GPU/VRAM-Variante oder eigene Werte wählen. Messungen mit anderem Power-Limit werden als vorläufig gekennzeichnet. Angebot und beworbenes Power-Limit sind noch keine verifizierten Werte der gemieteten Karte. Explizite Größenwerte in der gespeicherten Job-Config bleiben maßgebend. Manuelles Rent speichert die Wahl pro Job; bis zum Start des ersten Jobs ist sie unter **Rental & Automation** oder über **GPU profile** auf der Queue-Karte änderbar. Später ergänzte Jobs verwenden Auto. Es findet kein Optimizer-Vortest statt.

Manuelles **Rent** verwendet GPU-Typ, Hardware, Disk-Größe, Verifizierungsstatus und angezeigten Preis des ausgewählten Angebots. Nachträgliche Änderungen der Suchfilter ersetzen dessen Eigenschaften nicht. Mietdauer und Budget stammen weiterhin aus den Miet-Einstellungen; Verfügbarkeit und Grenzen werden vor der Miete erneut geprüft.
Die GPU-Spalte zeigt vor der Miete auch das von Vast angegebene Power-Limit oder **not reported**, falls die Angabe fehlt. Dieser Angebotswert ist keine Live-Messung; das tatsächlich gesetzte Limit wird während des Betriebs an der gemieteten GPU überprüft.
Abgeschlossene Performance-Testprofile speichern auch das gemessene konfigurierte Power-Limit. Bei einer echten Miete bevorzugt PBGui ein exaktes Profil und nimmt andernfalls das Profil mit dem nächstliegenden Power-Limit für dieselbe Laufzeit-GPU und exakt dieselbe VRAM-Variante. Mod-Karten mit abweichendem VRAM werden nicht vermischt.

Die Auswahl eines Angebots zeigt neben **Rent** auch die Abdeckung durch GPU-Leistungsprofile. PBGui unterscheidet **Local test**, eine versionierte, mit PBGui ausgelieferte **PBGui reference** und den bisherigen konservativen Standard. Die Zuordnung aus den Marktplatzdaten ist vorläufig. Nach der Miete prüft der Worker GPU-Name, exakten VRAM in MiB, Compute Capability und Gerätekennung direkt zur Laufzeit; gleich benannte Karten mit abweichendem VRAM bleiben getrennte Varianten.
Abgeschlossene Messwerte und **Use completed test profile** erscheinen hier nur, wenn das ausgewählte Angebot denselben beworbenen GPU-Namen und VRAM wie die getestete Karte hat. Wähle ein passendes Angebot, um dessen Ergebnis zu sehen; in Queue bleiben alle früheren Ergebnisse unabhängig von der Angebotsauswahl erhalten.

**Run performance test** nutzt PBGui's versionierten EMA-Anchor-Vergleichs-Workload: BTC, ETH und SOL auf Binance im Jahr 2024, mit festem Seed und Objective. Normale Optimizer-Jobs werden nie automatisch getestet. Die getrennte, ausdrücklich bestätigte Tuning-Aktion am Queue-Job nutzt genau dessen eingefrorene vorbereitete Eingabe, ohne den ursprünglichen Job zu ändern oder zu starten. Erst nach der Bestätigung bereitet PBGui lokale Marktdaten auf und friert sie ein; vor der Miete wird das gewählte Angebot nochmals geprüft. Das neue Protokoll beginnt bei 4.096 und 8.192 Kandidaten und testet danach 12.288, 16.384 und weitere 4.096er-Schritte, solange die Skalierung lohnt. Mindestens 10 % Gewinn führt zur nächsten Stufe; ein kleinerer positiver Gewinn behält die neue Population, eine Verschlechterung die vorherige. Die prognostizierte VRAM-Reserve von 15 %, die Mietfrist und die Sicherheitsgrenze von 131.072 gelten weiter. Nach dem Test wird die isolierte Miete gelöscht.
Das Ergebnis zeigt Kandidatenrate, Messfenster abgeschlossener Generationen, GPU-Proxy-Zeit und – sofern PB8 sie liefert – wirksame Dispatch-Größe und -Anzahl. Protokoll 3 akzeptiert eine Population nur, wenn PB8 den vollständigen angeforderten Batch als einen Dispatch bestätigt. Ein begrenzter Fall kann keine wiederverwendbare Empfehlung werden. Die gesamte Fall-Laufzeit ist nicht zwingend das Messfenster der Rate. Ältere Trailing-Martingale-Ergebnisse aus Protokoll 2 bleiben sichtbar und nutzbar, sind aber als Legacy-Evidenz markiert; ihre Rate aus dem seltenen Optimizer-Log kann nur eine protokollierte Generation abbilden. EMA- und Trailing-Martingale-Werte sind nicht direkt vergleichbar. **Use completed test profile** übernimmt ein Ergebnis nur für die exakt zur Laufzeit verifizierte GPU-Variante und das gemessene Power-Limit; mitgelieferte Referenzen bleiben unverändert. Wiederholungen erhalten ältere Ergebnisse. Der veröffentlichte Kalibrierungs-Worker ist für neue Tests per unveränderlichem Digest festgelegt.

**Reference workload** bietet Small (3 Coins/Binance/ein Szenario 2024), Medium (10 Coins/zwei Börsen/zwei Szenarien) und Large (41 Coins/zwei Börsen/vier Szenarien 2024). Mit **Adjust test search** lassen sich Startpopulation, Schrittweite, Obergrenze, Mindestgewinn und Timeout pro Fall vor der Bestätigung ändern. Das unveränderte Small-Preset verwendet weiterhin den veröffentlichten Protokoll-3-Test. Der veröffentlichte und gepinnte Protokoll-4-Worker ermöglicht auch die anpassbaren Tests; bei einem Versionskonflikt mietet PBGui keine GPU. Wartende Tests nutzen derzeit nur die feste Small-Referenz. Protokoll 4 misst pro Population mindestens eine vollständige GPU-Generation und verwirft Fälle, bei denen ein Szenario die angeforderte Population auf mehrere Dispatches aufteilen musste. Die Empfehlung ist der beste validierte Schritt unter der gewählten Stoppregel, kein universelles Optimum. Population und Batch sind gleich; das gespeicherte Arbeitslimit entspricht den größten gemessenen Candidate-Bars des gewählten Schritts und teilt größere echte Workloads auf. Der exakte Workload-Fingerprint verhindert, dass Messraten auf fremde Konfigurationen übertragen werden.

Referenz-Workload und Testsuche bleiben bei einer normalen Miete verborgen. Erst **Performance test…** öffnet diese Optionen; danach bestätigst du **Run performance test** oder aktivierst einen wartenden Test.

Bei einem bereits vorbereiteten, inaktiven PB8-GPU-Queue-Job erstellt **Tune GPU with this frozen input** erst nach Auswahl eines Angebots und ausdrücklicher Mietbestätigung eine getrennte Testkopie. Sie verwendet die unveränderlichen vorbereiteten Dateien, bereitet die Daten nicht erneut auf und startet oder verändert den ursprünglichen Optimizer-Job nicht. Das Messergebnis steht beim Test-Queue-Job. Seine Übernahme speichert ein lokales Profil für genau diesen Workload; der ursprüngliche Queue-Snapshot wird nicht stillschweigend umgeschrieben. Normale Queue-Jobs führen nie automatisch einen Tuning-Test aus.

Die Eingabeaufbereitung beginnt erst nach dieser Bestätigung und erscheint in Queue als vorbereitende Kalibrierung mit Kopier- und Komprimierungsfortschritt. Eine kostenpflichtige Miete startet erst nach erfolgreicher Vorbereitung und erneuter Prüfung des ausgewählten Angebots. Eine deaktivierte Aktion nennt ihren Grund deutlich direkt bei den Knöpfen. Eine aktivierte automatische GPU-Planung wird als Teil des bestätigten Tests pausiert, statt eine ansonsten freie Miete zu blockieren; eine aktive Miete, eine andere Kalibrierung oder fehlende Mietüberwachung sperren die Aktion weiterhin.

Die GPU-Angebotssuche bietet zusätzlich ein Mindest-Power-Limit laut Vast (W) und eine Mindest-Zuverlässigkeit des Hosts (%). **Test when matching GPU is available** nutzt die gespeicherten Filter, die gewählte Mietdauer und das Budget nach einer gesonderten ausdrücklichen Freigabe. Der Auftrag wartet bis **Cancel waiting test** – bei Bedarf unbegrenzt; das Warten allein mietet nichts und kostet nichts. Bei einem Treffer prüft PBGui unmittelbar vor einem Mietversuch nochmals Angebotskennung, Preis und alle eingefrorenen Grenzen. Lehnt Vast ein veraltetes Angebot ab und bestätigt unabhängig, dass keine Instanz entstanden ist, sucht PBGui weiter. Solange der Ausgang unklar ist, wird kein weiterer Mietversuch gestartet. Die erste bestätigte Miete beendet die Wartesuche. Unter GPU & Offers zeigt eine kompakte Statuskarte die eingefrorenen Suchgrenzen und die aktuelle Phase, auch wenn eine passende GPU gefunden wurde und der Test startet. Gespeicherte Mietlimits und GPU-Anforderungen sind darunter einklappbar und bei Bedarf weiterhin sichtbar. Das tatsächliche GPU-Power-Limit wird erst nach der Miete gemessen; liegt es unter dem freigegebenen Mindestwert, stoppt der Test und die Miete wird aufgeräumt, statt still eine andere Karte zu testen. Ein automatischer GPU-Scheduler oder eine aktive Miete kann nicht parallel zu dieser Wartesuche laufen.

Jede Kalibrierungs-Population bekommt die von PBGui bereitgestellten Markt- und Erstkerzen-Caches in ihrem eigenen Arbeitsverzeichnis. Fehlen Metadaten oder sind sie abgelaufen, stoppt der Test statt live eine Börse abzufragen. Sofortige und wartende Tests benötigen den inzwischen veröffentlichten und per unveränderlichem Digest festgelegten Kalibrierungs-Worker für Protokoll 3. PBGui prüft diesen Pin vor dem Provider-Zugriff, statt einen gewöhnlichen Optimizer-Lauf als Kalibrierung zu werten.

Im Editor nur **Execution → Vast.ai GPU** oder **Local** wählen.
Unter **Queue → GPU & Offers** GPU-Typ, maximalen Stundenpreis,
Mindestwerte für VRAM/RAM/CPU, Disk-Größe und verifizierte Hosts festlegen.
**Preview available GPUs** dient zur Vorschau: Ein Klick auf eine Zeile übernimmt
nur den Typ, keine Angebots-ID. **Save GPU requirements** speichert die Vorgaben.
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
Mit **Min TFLOPS** wird die vom Provider angegebene Mindest-GPU-Rechenleistung
gefordert; `0` lässt die Rechenleistung offen. Die Vorgabe gilt sowohl für die
Vorschau als auch für die endgültige Angebotsauswahl vor einer Miete.

**Save & Queue** speichert die aktuelle Konfiguration und erstellt sofort eine
Cloud-Queue-Zeile mit dem Status **Preparing input**. Der feste Eingabestand wird
anschließend im Hintergrund kopiert und komprimiert; vor dem Status **queued**
kann keine GPU gemietet werden. Die Zeile zeigt kopierte Eingabe-MB, die Anzahl
fertiger Dateien und einen Fortschrittsbalken; die abschließende Komprimierung
erscheint als unbestimmte Phase. **Queue Selected** beachtet ebenfalls das gespeicherte Ziel. Lokale
Konfigurationen bleiben im bisherigen lokalen Ablauf. Cloud-Jobs erscheinen als
gemeinsamen Tabelle unter **Queue**, erkennbar an der Spalte **Execution**.

Aktuell unterstützt: frische Multi-Coin-Läufe mit `ema_anchor` oder
`trailing_martingale`, Binance, Bybit, Bitget, OKX, Hyperliquid, KuCoin und Szenarien mit eigenen Coins, Börsen, Zeiträumen und unterstützten GPU-Parameter-Overrides. HSL, separate Coin-Override-Pakete,
BTC-Collateral und Successive Halving werden vor der Miete abgewiesen.
Pareto-Seeds werden nicht übernommen. `gain_strategy_eq` wird vom festen
GPU-Backend nicht unterstützt. Gegebenenfalls bewusst ein unterstütztes Ziel
wie `adg_strategy_eq` im Editor auswählen; PBGui ändert das Ziel nicht heimlich.
Die vorhandene Historie des Coins und der BTC-Referenz wird inklusive Warmup
übernommen. Fehlende Daten zuvor über Market Data laden.

### Bekannte, funktionierende und bevorzugte Hosts

Die Angebotssuche kennzeichnet anhand der lokalen Miethistorie tatsächlich gemietete Rechner als **Previously used**. **Working** bedeutet, dass mindestens ein exaktes Optimierungsergebnis auf diesem Rechner erfasst wurde oder du ihn mit **Mark working** manuell markiert hast. Eine beim Image-Download hängende Miete wird nicht automatisch als Working markiert. Die Kennzeichnung beschreibt frühere Erfahrungen und garantiert nicht den Erfolg der nächsten Miete.

Mit **Prefer host** in den Angebotsdetails, in der Host-Karte einer Miete oder unter **Hosts** bevorzugst du einen Rechner. Bevorzugte Maschinen erscheinen zuerst und werden innerhalb dieser Gruppe nach Preis ausgewählt. PBGui sucht diese Maschinen zusätzlich gezielt, auch wenn sie nicht auf der ersten Seite der günstigsten Angebote stehen. Hardware-Anforderungen, Preisgrenze, Laufzeit und Budgetprüfung gelten weiterhin. Qualifiziert sich kein bevorzugter Rechner, bleiben andere passende Angebote verfügbar. Manuelles **Rent** mietet weiterhin exakt das ausgewählte Angebot. Eine Sperre hat immer Vorrang.

Präferenzen und manuelle Working-Markierungen bleiben nach Neuladen und API-Neustart erhalten. **Remove preference** und **Clear working mark** heben deine Markierungen auf; nachgewiesene historische Ergebnisse bleiben sichtbar. Alte Mieten ohne gespeicherte Machine ID können nicht anhand des GPU-Namens zugeordnet werden. Bei einer noch existierenden Miete ermittelt eine Host-Aktion die ID über Vast; andernfalls kannst du die bekannte Machine ID manuell eintragen. Die Mietüberwachung speichert künftig die Rechner-ID automatisch. Die Markierungen ändern oder beenden keine laufende Miete.

### Unzuverlässige Hosts sperren

Mit **Block host** neben einem Angebot, unter Hosts oder in der **Host**-Karte des Optimizer-Logs sperrst du einen Rechner. PBGui speichert dessen dauerhafte Vast **Machine ID**. Auch neue Angebote desselben Rechners werden damit aus der Vorschau sowie aus zukünftigen manuellen und automatischen Mieten ausgeschlossen. Eine laufende Miete bleibt bestehen; beende sie bei Bedarf separat mit **End rental**.

Unter **Hosts** mit dem Filter **Blocked** kannst du die IDs ansehen, eine bekannte Vast Machine ID manuell sperren und mit **Unblock host** wieder freigeben. Die Sperren bleiben nach Neuladen und API-Neustart erhalten. Bei älteren Mieten ermittelt PBGui die Machine ID beim Klick auf Block host anhand der zugehörigen Vast-Instanz. Ist diese nicht mehr verfügbar, kannst du die ID manuell eintragen. Sobald Sperren vorhanden sind, werden auch Angebote ohne gültige Machine ID ausgeschlossen, weil sich deren Freigabe nicht zuverlässig prüfen lässt.

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

## GPU-Pool und gemeinsame Mieten

Unter **Rental & Automation** lässt sich **Max concurrent GPUs** einstellen (Standard **1**, maximal **16**). Damit nicht jeder Queue-Job von Hand gestartet werden muss, **Auto rent & start** auf **On** stellen und speichern. Die Bestätigung genehmigt ausdrücklich die Budgetziele pro Miete und für alle gleichzeitigen Mieten. Danach reicht **Save & Queue**: Sobald die Eingabevorbereitung fertig ist, mietet PBGui bis zum gespeicherten Limit und startet pro GPU einen Optimizer. Passende freie GPUs werden zuerst wiederverwendet. Aufbauende und noch nicht nachweislich gelöschte Mieten zählen zum Limit. **Auto rent & start** funktioniert auch mit Limit 1.

Stunden, Budget und Leerlaufregel gelten **pro GPU-Miete**. Die Einstellungen zeigen die Summe der gleichzeitig geltenden Budgetziele. Dies ist kein Gesamtbudget über die Laufzeit des Pools: Solange der Pool aktiv ist und vorbereitete Jobs warten, kann er nach Ende einer Miete Ersatz-GPUs mieten. Neue Angebote müssen die genehmigten Anforderungen und Host-Sperren erfüllen. Ohne passendes Angebot zeigt die Queue den Grund und versucht es automatisch erneut.

**Pause queue** verhindert neue Zuteilungen und Mieten; laufende Jobs und Leerlaufbereinigung bleiben aktiv. Ein pausierter Pool wird oberhalb der Queue deutlich angezeigt; dort setzt **Resume auto start** den autorisierten Pool für wartende Jobs fort. Werden automatische Einstellungen während der Pause gespeichert, ändern sich die Limits, ohne die Queue fortzusetzen. Jede aktive GPU erscheint in der Queue als kompakte Karte mit Instanz, Job, Phase und laufendem Upload- oder Optimizer-Fortschritt. Beim Upload werden die gemessene Rate und, sofern verfügbar, deren Sättigung gegenüber der vom Provider angegebenen Host-Download-Bandbreite gezeigt. Die Mietaktionen gelten jeweils nur für diese Karte. **Replace GPU** beendet nur diese Miete und lässt die automatische Planung aktiv; PBGui wartet auf die bestätigte Löschung beim Provider, bevor die Pool-Kapazität ersetzt wird. Eingaben ohne Optimizer-Ergebnis werden automatisch wieder in die Queue gestellt. Bereits vorhandene Ergebnisse bleiben erhalten und müssen ausdrücklich mit Requeue neu gestartet werden. Wird die Automatik über die Einstellung ausgeschaltet, pausiert die Zuteilung und es entstehen keine neuen Mieten; eine laufende Miete wird dabei nicht beendet. Ein niedriger gespeichertes Limit beendet ebenfalls keine bestehende Miete. Der Scheduler wird nach einem API-Neustart wiederhergestellt. Logs, Deadline und Budget eines Jobs gehören immer zu seiner eigenen GPU.

Benötigt ein später hinzugefügter Job mehr Transferreserve, kann ein Worker mit aktuellem Budget-Control-Guard ungenutzte Mietzeit innerhalb desselben genehmigten Budgets in Transferreserve umschichten. PBGui startet den Job erst nach Bestätigung der verkürzten Deadline durch den Worker. Während einer ausstehenden Anpassung greift keine Leerlaufbereinigung. Bei älteren Guards oder unzureichendem Restbudget zeigt PBGui einen Hinweis zur manuellen Anpassung von **Transfer reserve/Budget**. Unbestätigte Reserven werden nicht ausgegeben und das Budget wird nicht automatisch erhöht.

Unter **Rental & Automation** maximale Stunden, Budgetziel und Leerlaufregel zusammen mit den GPU-Anforderungen speichern. **Start** in der Jobzeile mietet direkt mit diesen gespeicherten Vorgaben, und sucht ein aktuelles passendes Angebot.
Im Einzelmietmodus mietet PBGui einmal, richtet verifiziertes SSH ein und arbeitet die Jobs
nacheinander auf derselben GPU ab. Ergebnisse werden jeweils vor dem nächsten
Start lokal gesichert und importiert. Die CPU-Parallelität pro Lauf kommt aus
der jeweiligen Optimizer-Konfiguration.

Auch später hinzugefügte passende Jobs verwenden denselben Worker. Kursdaten
werden anhand ihrer Prüfsummen zwischengespeichert; nur fehlende oder geänderte
Dateien werden erneut übertragen. Jeder Job besitzt eigene Eingabe- und
Ergebnisverzeichnisse. Jobs mit zu hohem CPU-Bedarf oder unzureichender
Transferreserve bleiben mit Begründung wartend.

Mietfrist und Budget gelten für die gesamte Miete und werden durch Folgeläufe
nicht zurückgesetzt. Ersatzmieten erfolgen nur mit ausdrücklich gestartetem automatischem Pool. Budget oberhalb der
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
- **Start queue** auf der Karte einer reservierten GPU gibt nur diese manuelle Reservierung für die Abarbeitung frei.
- **End rental** auf einer manuellen GPU-Karte beendet/sichert deren aktiven Job und löscht diesen Worker.
- **Replace GPU** auf einer automatisch verwalteten Karte beendet nur diese Miete. Die Pool-Planung bleibt aktiv und stellt die Kapazität nach bestätigter Bereinigung wieder her.
- **Resume supervision** stellt die Überwachung derselben Miete nach einem
  Controller-/Host-Ausfall wieder her, ohne eine Ersatzinstanz zu mieten.

Ohne passende Arbeit bleibt die Instanz standardmässig bis zum Ende der Mietfrist
erhalten, damit hochgeladene Daten und der Worker-Cache für Folgeläufe verfügbar
bleiben. Sofortige Bereinigung sowie 5 Minuten, 30 Minuten oder 1 Stunde Leerlauf
bleiben auswählbar. Die GPU-Karte zeigt die aktive Regel und verbleibende Zeit.
Neue passende Jobs heben einen laufenden Countdown auf. Auch eine
pausierte Queue unterliegt dieser Leerlaufregel. Erst **deletion_verified**
bestätigt die Löschung; blosses Stoppen kann weitere Speicherkosten verursachen.

## Grenzen und Wiederaufnahme

Vorübergehende Upload-/SSH-Fehler erhalten ein Wiederherstellungsfenster von 15 Minuten statt einer Grenze von zwei Versuchen. PBGui wartet zunächst 15, dann 30 und anschließend höchstens 60 Sekunden zwischen Versuchen und verwendet Teildateien bzw. geprüfte Blöcke weiter. Neuer beobachteter Transferfortschritt erneuert das Fenster; wiederholte Versuche ohne Fortschritt nicht. Das Log zeigt den nächsten Versuch und den ursprünglichen Verbindungsfehler. Der separate 15-Minuten-Timer für die Worker-Einrichtung gilt nach erfolgreicher Health-Prüfung nicht mehr für den Upload. Die Mietfrist abzüglich drei Minuten für Ergebnissicherung begrenzt weiterhin die Übertragung. Stop reagiert auch während der Wartezeit. Identitäts-, Integritäts- und Speicherplatzfehler führen weiterhin zum sofortigen Abbruch; Mietfrist und Budget werden nicht automatisch verlängert.

OpenSSH und ein funktionierender systemd-User-Manager sind erforderlich.
Controller und Mietwächter überleben Browser-/API-Neustarts. Ein entfernter
Wächter versucht die Löschung unabhängig zur festen Frist. Nach einem Neustart
des PBGui-Rechners mit **Resume supervision** die temporären Dienste wiederherstellen.

**Budget**, **Deadline** und **Transfer reserve** stehen in eigenen Karten. Reserve-Eingaben zeigen vier Nachkommastellen. Das Eingabefeld für die Deadline-Anpassung startet immer mit 60 Minuten. Anpassungen sind in ganzen Minuten von 1 bis 1.440 möglich, innerhalb von Budget, 24-Stunden-Mietlimit und Aufräumreserve. Bei einer ausdrücklich angeforderten Deadline-Änderung aktualisiert PBGui einen älteren Mietwächter, während der Optimizer weiterläuft. Der neue Wächter muss zuerst die unveränderte Deadline bestätigen; erst danach wird die gewünschte Änderung gesendet. Bei fehlgeschlagener Übergabe wird der alte Wächter wieder aktiviert. Unbestätigte Anpassungen bleiben ausstehend und verlängern die lokale Deadline nicht. Während der Übergabe ist ein API-Neustart blockiert.

In der Karte der aktiven Miete kann das **Budgetziel** geändert werden. PBGui
berechnet daraus die Löschfrist neu und übergibt die Änderung zur Bestätigung an
den Worker-Guard; bis zur Bestätigung bleibt sie ausstehend. Dieselbe Karte zeigt
die für Ein- und Ausgaben reservierte Transferreserve. Bei der Meldung einer zu
kleinen Transferreserve kann sie dort erhöht werden; PBGui verkürzt dafür die
Frist innerhalb des gewählten Budgets. Das Budget ist keine harte Vast-Ausgabensperre. Zur Frist hat die Löschung Vorrang
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

Lokale und Cloud-Jobs stehen in derselben **Queue**-Tabelle. **Execution** zeigt **Local** oder **Vast.ai**. Der Cloud-Status **ready** erscheint als **queued**. **Open log** in der Jobzeile öffnet Details und verfügbare Logs; **Close** blendet sie aus. Mietlimits, Konto und GPU-Anforderungen werden getrennt unter Rental & Automation, Account und GPU & Offers gespeichert. **Start** lässt die Queue sichtbar. **Open log** öffnet dasselbe schwebende Optimize-log-Fenster wie bei lokalen Jobs, mit Cloud-Fortschritt, Status und den Aktionen Pause, Stop, Mietende und Wiederaufnahme. Während der Bereitstellung wird dort bereits der Status angezeigt, bevor ein heruntergeladenes Log vorliegt. Erklärungen stehen in der Hover-Hilfe.

Die GPU-Prüfung bietet **Replace metric** mit unterstützten Scoring-Metriken, **Edit limit** oder **Remove limit** für nicht unterstützte Grenzen und **Choose coins** für ungültige Coin-Listen. Änderungen betreffen nur den Entwurf. Ein Metrikwechsel ändert die Bedeutung; das Entfernen eines Limits lockert die Einschränkungen. Erklärungen stehen an gepunktet unterstrichenen Hover-Bezeichnungen neben den Aktionen, nicht auf den Knöpfen. **Use Local** erscheint einmal als Alternative.

Der feste Worker-Stand akzeptiert 1–64 unterschiedliche freigegebene Coins über beide Seiten. Die frühere Ein-Coin-Sperre stammte aus PBGuis erstem Benchmark-Profil und ist entfernt. Ein Multi-Coin-Lauf auf Vast wurde hier noch nicht getestet.

**Open log** erscheint bei vorhandener heruntergeladener Logdatei oder während Bereitstellung/Ausführung eines Cloud-Jobs. Wartende Cloud-Jobs haben einen Papierkorb statt Stop. Das Löschen eines inaktiven Queue-Eintrags entfernt dessen Wiederholungsversuche, lokale Job-Snapshots, Logs und Performance-Messungen; bereits importierte Optimizer-Ergebnisse bleiben unter Results erhalten. Alte inaktive Einträge lassen sich auch löschen, während ein anderer Job **preparing** anzeigt. Die Einträge verschwinden automatisch, bevor grosse Snapshot-Verzeichnisse fertig gelöscht sind; diese Dateibereinigung hält die Queue-Sperre nicht fest und unterbricht die Aktualisierung der Queue nicht. Wird PBGui dabei neu gestartet, bereinigt es markierte Löschreste nach dem Start selbstständig im Hintergrund. Aktive Jobs müssen vor dem Entfernen vollständig beendet und gesichert sein. Auch durch den früheren Löschfehler bereits ausgeblendete Einträge bereinigt PBGui beim API-Start automatisch. Ein ausgeblendeter Job mit sichtbarem oder aktivem Wiederholungsversuch bleibt erhalten, bis die gesamte Retry-Kette sicher entfernt werden kann.

Die Spalte **Exchange** zeigt die Börsen des eingefrorenen Cloud-Jobs. Ältere Einträge verwenden ihr Exportmanifest; spätere Änderungen der Quellkonfiguration ändern diese Anzeige nicht.

Instanzabfragen verwenden einen kurzen gemeinsamen Cache. Bei HTTP 429 pausieren Provider-Abfragen automatisch entsprechend Retry-After. Die Löschprüfung verwendet frische Abfragen. Bis Vast eine Instanz bestätigt, zeigt das Log provisioning (instance not confirmed). Mietfehler behalten HTTP-Status und ursprünglichen Erstellungsfehler; eine unklare Erstellung wird nicht automatisch wiederholt.

Fehlgeschlagene Cloud-Jobs behalten **Open log** auch ohne heruntergeladene Logdatei, damit der gespeicherte Fehler zugänglich bleibt.

Ohne heruntergeladenes Optimizer-Log öffnet der Viewer **VastRunner.log**, das gemeinsame lokale Überwachungslog mit Bereitstellungs- und Rate-Limit-Fehlern. Es kann Meldungen anderer Cloud-Jobs enthalten. Sobald verfügbar, wechselt der Viewer zum Optimizer-Log des Jobs.

**Requeue** bereitet einen inaktiven fehlgeschlagenen/abgebrochenen Cloud-Job mit der gespeicherten Konfiguration und den bisherigen Evaluations-/Worker-Einstellungen erneut vor. Der alte Eintrag wird erst nach erfolgreicher Vorbereitung entfernt; seine Dateien bleiben erhalten. Die Eingabevorbereitung zeigt getrennten Kopier- und Archiv-Komprimierungsfortschritt; beim Komprimieren erscheinen verarbeitete MB, Prozent, gemessene Rate und geschätzte Restzeit. Wurde ein autorisierter **Auto rent & start**-Pool durch den fehlgeschlagenen Versuch pausiert, setzt dieses ausdrückliche Requeue ihn fort und der Ersatz startet automatisch. Ohne automatische Autorisierung steuert Start die Ausführung.

Start meldet einen Konflikt, solange die vorherige Miete beendet oder bereinigt wird. Wartende Jobs behalten Open log, solange eine gemeinsame Miete ungeklärt ist; dort bleiben Überwachung und Bereinigungsaktionen erreichbar.

Die lokale Mietüberwachung erkennt auch direkt bei Vast.ai gelöschte Instanzen. Zwei frische erfolgreiche Abwesenheitsprüfungen im Abstand von mindestens zehn Sekunden lösen die Miet-Sperre, beenden unfertige Jobs dieser Miete und pausieren deren Queue. Wartende Jobs und lokale Ergebnissicherungen bleiben erhalten. Provider-Fehler setzen die Bestätigung zurück. Gestoppte/Offline-Instanzen, die Vast noch auflistet, gelten nicht als gelöscht. Bei geändertem Eigentumslabel ist eine Prüfung nötig; PBGui nimmt keine Löschung an. Neue oder unklar erstellte Instanzen erhalten zwei Minuten Wartezeit für die Sichtbarkeit. Diese Prüfung mietet keine Ersatzinstanz. Die Überwachung muss laufen; Resume supervision kann sie nach einem Ausfall erneut starten.

Bei unklarer Erstellung wartet die Bereinigung bis zwei Minuten nach dem Versuch und verlangt danach zwei frische erfolgreiche Abfragen ohne passende Instanz. So kann Start vor dem Mietende freigegeben werden. Provider-Fehler und Rate-Limits zählen nicht als Abwesenheitsbestätigung.

Nach Klick auf Start zeigt die Queue-Zeile sofort **Starting…**, solange die Anfrage läuft. Cloud-Startknöpfe sind währenddessen gegen doppelte Aufrufe gesperrt.

Das Job-Logfenster bietet **Requeue** für inaktive fehlgeschlagene/abgebrochene Jobs und **Start** für vorbereitete Jobs. Requeue lässt das Fenster beim Ersatzjob geöffnet und setzt einen bereits autorisierten **Auto rent & start**-Pool fort.

**Stop & collect** ist vor dem Start und nach Abschluss deaktiviert. Die Aktion ist während Bereitstellung, Upload, Ausführung und Ergebnissicherung verfügbar.

Während Requeue zeigt die Queue nur den vorbereiteten Ersatz als preparing. Schlägt die Vorbereitung fehl, bleibt der ursprüngliche Job für einen erneuten Versuch verfügbar.

Während der Bereitstellung ruft PBGui das Vast-Image-Downloadlog höchstens einmal pro Minute ab und zeigt es im vorhandenen Viewer. Hat Vast anfangs noch kein Log, ruft PBGui den wartenden Platzhalter erneut ab, sobald die Worker-Verbindung bestätigt ist. Sobald verfügbar, hat das Optimizer-Log Vorrang. Die Dateiliste hat eine begrenzte Breite; Aktivitäts- und Fehlertexte werden umgebrochen.

Der Image-Download bleibt durch die Mietfrist begrenzt. Das separate Zeitlimit von 15 Minuten für die Worker-Einrichtung beginnt erst, sobald Vast den Container als laufend meldet. Der SSH-Hostschlüssel wird anhand eines öffentlichen Schlüsseleintrags im Container-Log dieser Miete geprüft. Einrichtung und Wiederholungen verlängern die ursprüngliche Mietfrist nicht.

Nach einem Jobfehler pausiert die Queue; die eingestellte automatische Bereinigung bei Leerlauf bleibt aktiv. Sobald Vast die Löschung bestätigt, lässt sich diese Instanz nicht mehr fortsetzen. Requeue bereitet einen weiteren Versuch vor und setzt einen autorisierten automatischen Pool fort. Ohne **Auto rent & start** ist Start für eine neue Miete erforderlich.

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

Während der Optimierung werden geprüfte Ergebnissnapshots nach der ersten exakten Auswertung etwa jede Minute innerhalb des Transferlimits veröffentlicht. Results und Pareto Explorer im Joblog öffnen diesen Zwischenstand. Sobald die kumulierten Zwischenarchive 1 GiB erreichen, lädt PBGui keine weiteren vollständigen Zwischenarchive herunter, führt eine aktivierte Stagnationsprüfung aber mit einem begrenzten reinen Pareto-Metrik-Snapshot über die verifizierte Worker-Verbindung fort. Die abschließende Übertragung aktualisiert denselben Eintrag.

### Stopp bei ausbleibender Verbesserung

Neue PB8-Konfigurationen verwenden beim ersten Wechsel auf Vast **20.000 exakte Auswertungen**, sofern das Iterationsfeld noch nicht bearbeitet wurde. Gespeicherte Konfigurationen und ausdrücklich eingestellte Werte bleiben erhalten. Dies ist eine Obergrenze; die aktivierte Sättigungserkennung kann früher stoppen, garantiert dies aber nicht.

Setup bietet einen optionalen vorzeitigen Stopp (standardmäßig aus). Vorgaben: mindestens 512 exakte Auswertungen, danach 512 weitere Auswertungen Geduld und 0,25 % Verbesserungstoleranz. Jeder Job übernimmt die Einstellungen beim Start; Änderungen beeinflussen laufende Jobs nicht.

Nach dem Minimum bewertet PBGui die zulässige exakte Pareto-Front anhand des normalisierten Hypervolumens. Skala und Referenz werden mit der ersten Front festgelegt und bleiben unverändert. Unterstützt werden ein bis drei bereits zur Minimierung vorzeichenkorrigierte Ziele, einschließlich PB8-Suite-Auswertungen. Relative Verbesserungen über der Toleranz setzen das Geduldsfenster zurück; kleinere Verbesserungen summieren sich gegenüber dem zuletzt akzeptierten Stand. Proxy-Zahlen und verstrichene Minuten verbrauchen keine Geduld. Fehlende, ungültige, nicht unterstützte oder ausschließlich unzulässige Snapshots unterbrechen die Erkennung und starten das Beobachtungsfenster neu.

Das Joblog zeigt Phase, Auswertungen seit der letzten relevanten Verbesserung und verbrauchte Geduld. Die Prüfung folgt den verifizierten periodischen Ergebnissnapshots; der Stopp kann deshalb um ein Snapshot-Intervall verzögert erfolgen. Bei Stillstand fordert PBGui einen geordneten Stopp an, übernimmt die Endergebnisse und kennzeichnet einen erfolgreichen Stopp als abgeschlossen mit Konvergenzgrund. Anschließend folgt der nächste Queue-Job oder die eingestellte Leerlaufbereinigung. Das Verfahren ist eine Heuristik, kein Optimalitätsbeweis; Mietfrist und Iterationslimit gelten weiterhin.

Die geöffnete Results-/Paretos-Liste aktualisiert sich, sobald die nächste Jobabfrage (alle zehn Sekunden) einen neu veröffentlichten Snapshot erkennt: normalerweise etwa 60 Sekunden plus bis zu zehn Sekunden und Übertragungszeit. Elapsed misst die Optimizer-Laufzeit ohne Bereitstellung und Upload und bleibt nach Abschluss auf der gemeldeten Laufzeit stehen.

PBGui misst nvidia-smi einmal direkt nach der Bereitschaft des Workers, bevor automatische Ausführungseinstellungen festgelegt werden, und danach etwa alle 15 Sekunden über die bestehende SSH-Verbindung. Die CPU-Auslastung bezieht sich auf die effektive Container-Kapazität und benötigt zwei Messungen. RAM enthält den Container-Cache; VRAM, GPU-Aktivität, aktuelle Kartenleistung und das konfigurierte Leistungslimit stammen von nvidia-smi. Aktuelle und maximale Watt erscheinen unter der GPU-Auslastung. Werte über 45 Sekunden sowie nicht verfügbare Messungen erscheinen als Strich, nicht als Null.

Neue Cloud-Jobs verwenden automatisch die gemessene CPU-Quote, auf ganze Worker abgerundet (mindestens einer), für n_cpus und gpu.exact_workers. Eine Quote von 9,6 ergibt neun Worker, auch wenn das Angebot zehn Kerne nennt. Die Quell-Config bleibt erhalten; eine getrennte geprüfte Ausführungskopie wird übertragen. Laufende Pools werden nicht umgestellt. Lokale Backtests aus Cloud-Ergebnissen entfernen den nur im Container gültigen HLCV-Pfad und verwenden lokale Daten, auch für Holdout-Zeiträume.

Stop & collect verlangt zuerst eine Bestätigung für den gewählten Optimizer. Nach der Bestätigung erscheint sofort **Stopping…**, danach **Stop requested**, bis die Überwachung **Collecting results…** meldet. Nach der Sicherung startet der nächste geeignete Queue-Job.

Jede GPU-Karte bietet **Log** für die detaillierte Live-Ansicht des zugeordneten Jobs und **Results** für dessen verknüpftes Optimizer-Ergebnis. Results bleibt mit einer Hover-Erklärung deaktiviert, bis ein verifizierter Ergebnissnapshot vorliegt. **Replace GPU** verlangt eine Bestätigung, da diese Aktion den zugeordneten Optimizer stoppt, seine aktuellen Ergebnisse einsammelt, die Miete beendet und Auto rent & start nach bestätigter Bereinigung die Pool-Kapazität wiederherstellen lässt.

Wurde ein Optimizer direkt in einem behaltenen PBGui-Worker gestartet, kennzeichnet die Karte ihn als **Observed run**. **Log** öffnet dafür eine schreibgeschützte Detailansicht mit aktueller Aktivität, GPU-Auslastung, Leistung und dem gespiegelten Optimizer-Log. Die Erkennung umfasst auch den normalen PB8-Aufruf, der `optimize.json` relativ zum Arbeitsverzeichnis des Optimizer-Prozesses übergibt. Queue-Aktionen bleiben deaktiviert, weil PBGui diesen Prozess nur beobachtet und nicht dessen Lebenszyklus übernimmt. Eine Beobachtung, die älter als 45 Sekunden ist, wird auch bei vorübergehend ausgefallener Fernabfrage nicht mehr als laufend angezeigt.

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

Der Cloud-Export plant alle aktiven Szenarien gemeinsam: `coins`, `ignored_coins`, `exchanges`, Datumsfelder und `coin_sources` werden berücksichtigt. Er exportiert den gemeinsamen Markt-Datenbestand inklusive BTC-Referenz und entfernt doppelte Dateien. Tagesdateien werden auf den gemeinsamen Basis-/Szenariozeitraum plus PB8s berechneten Optimizer-Warmup begrenzt und nach außen auf ganze Tage gerundet. Optimierungsgrenzen, Szenario-Overrides und native Warmup-Obergrenzen werden berücksichtigt; eine Obergrenze von null bedeutet unbeschränkten berechneten Warmup, nicht die gesamte vorhandene Historie. Kann der Warmup nicht bestimmt werden, stoppt die Vorbereitung. Requeue erstellt mit diesen Grenzen ein neues Paket aus der gespeicherten Konfiguration; bestehende Pakete werden nicht verändert. Zusätzliche Szenario-Börsen werden auch bei Markt- und Erstkerzen-Metadaten berücksichtigt. Die ursprüngliche Konfiguration bleibt unverändert; verschachtelte Parameter-Overrides werden nur in der eingefrorenen Worker-Kopie in Punktpfade umgewandelt.

Fehlende Mappings oder lokale OHLCV-Dateien nennen Szenario, Coin und Börse; die benötigte Historie vor dem Einreihen unter Market Data herunterladen. Eine Coin muss nicht auf jeder Börse gelistet sein. `coin_sources` muss eine Coin über die Suite hinweg derselben Börse zuordnen. Hyperliquid verwendet USDC, die anderen unterstützten Börsen USDT.

Leere oder fehlende Szenario-`exchanges` übernehmen die Basisbörsen. Fehlende/null `coins` übernehmen die Basisauswahl; `coins: []` wählt dagegen ausdrücklich keine Coins und wird mit dieser Begründung abgelehnt, entsprechend PB8. Leere `overrides` und `coin_sources` sind erlaubt. Pro aktivem Szenario gelten 1–64 ausgewählte Coins; die gesamte Suite darf mehr enthalten. Parameter-Overrides müssen vorhandene kanonische PB8-Pfade verwenden, die der festgelegte GPU-Worker unterstützt. Separate Coin-Override-Pakete, Datenpfad-Overrides und HSL bleiben außerhalb dieses Cloud-Profils; Fehler nennen Szenario und konkretes Feld.

Der Restart-Knopf in der Navigation erkennt auch veraltete lokale Vast-Run- und Guard-Supervisoren, einschließlich älterer Prozesse ohne Startversion. Restart ersetzt diese lokalen Steuerprozesse und setzt ihre gespeicherten Jobs auf der vorhandenen Miete fort; die Miete wird nicht beendet und keine neue GPU angefordert.

Beim Upload zeigt **Transferring job metadata** die komprimierten Konfigurations-/Manifest-Bytes an, die an SSH übergeben wurden. Das bestätigt noch keinen Empfang beim Worker. Die Synchronisation der Kursdateien beginnt nach dessen Bestätigung der Metadatenvorbereitung.

**Stop & collect** bricht auch einen Upload ab, der auf die Metadatenbestätigung wartet. Die Transfer-Steuerung prüft Stop-Anforderungen weiter, während SSH oder rsync keine Ausgabe liefert – auch nach dem Schließen der Ausgabekanäle bis zum tatsächlichen Prozessende. Vor dem Optimierungsstart gibt es keine Optimierungsergebnisse abzuholen; der Job wechselt auf **Cancelled**. Für eine pausierte Miete gelten weiterhin die bestehende Leerlaufbereinigung und Deadline.

Gueltige Pareto-Fronten mit mehr als 1.000 Ergebnissen werden weiterhin auf Stagnation geprueft; alle zulaessigen Punkte gehen in die exakte Hypervolumenberechnung ein.
