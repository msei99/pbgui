# PBGUI Services Übersicht

Ein gestoppter Dienst oder deaktivierter Autostart bedeutet nicht, dass die systemd-Migration aussteht. Dafür sind die vorhandenen Start/Stop- und Enable/Disable-Aktionen zuständig. Fehlende erforderliche Units oder alte Startdateien erfordern weiterhin eine Migration.

Schlägt das Laden der PBData-Einstellungen fehl, zeigt das Panel einen Fehler und einen Retry-Button.

Die Services-Seite zeigt und steuert alle PBGui-Hintergrunddienste an einem Ort.

## Service-Übersicht

Die Seite öffnet mit einem Kachel-Raster, das alle Services auf einen Blick zeigt. Während der Dienststatus lädt, zeigen die Kacheln „Loading status…“; Aktionsschaltflächen erscheinen nach Eingang des Status. Jede Kachel zeigt:

- Den Service-Namen
- Einen Status-Indikator (grüner Punkt = läuft, roter Punkt = gestoppt)
- Aktions-Buttons: **Start** wenn gestoppt, **Stop + Restart** wenn aktiv

Klicke auf eine Kachel, um das Detail-Panel des Service zu öffnen.

Während der Status lädt, zeigen Übersicht und Detail-Panels **Loading status…**, mit neutralen Punkten in Sidebar und Statusanzeige und ohne Service-Aktionsschaltflächen. Schlägt eine Statusabfrage fehl, erscheint **Status unavailable**, statt die Dienste als gestoppt anzuzeigen. Service-Aktionen bleiben bis zur nächsten erfolgreichen Statusabfrage ausgeblendet. Die Seite fragt den Status bei sichtbarem Tab automatisch alle fünf Sekunden erneut ab und setzt die Abfragen beim Zurückkehren fort. Nach der Erholung erscheinen aktueller Status und Aktionen wieder, ohne Panel, Tab oder ungespeicherte Einstellungen zurückzusetzen. Worker- und Migrationsstatus sind von diesen Fehlern der Service-Statusabfrage unabhängig.

Die Übersicht enthält jetzt auch eine eigene **Workers**-Kachel. Sie öffnet den nur für Administration gedachten Worker-Bereich für Queue-Worker, Sync-/Watcher-Worker und interne Helper-Tasks.

Die Übersicht liest den aktuellen Worker-Status direkt; detaillierte Warteschlangenstatistiken werden beim Öffnen von Workers geladen. Statusänderungen werden automatisch aktualisiert.

| Service | Funktion |
|---|---|
| **PBCluster** | Repliziert Cluster-Sync-Status und materialisiert freigegebene V7/API-Key-Änderungen auf verbundenen Nodes |
| **PBRun** | Startet/stoppt lokale Passivbot-Prozesse und verwaltet dynamische Coin-Filter |
| **PBData** | Lädt Account-Daten (Balances, Positionen, Orders, History, Executions) via REST und Live-Preise über öffentliche WebSockets |
| **PBCoinData** | Nutzt den gemeinsamen CMC-Credential-Pool, ruft CoinMarketCap-Daten ab und erstellt Exchange-Symbol-Mappings für dynamische Filter |
| **PBAPIServer** | Betreibt das FastAPI-Backend (REST + WebSocket), das Dashboard, VPS Monitor, Job Queue, Live-Alarmierung und alle Echtzeit-Features versorgt |

## Services starten und stoppen

Die **Start**-, **Stop**- oder **Restart**-Buttons auf den Kacheln oder im Control-Strip oben im Detail-Panel verwenden. Änderungen wirken sofort.

Der angeklickte API-Server-Restart-Button zeigt bis zum Abschluss des Requests den Fortschritt an, auch in der kompakten Kartenansicht. Ein temporaerer Fehler beim Laden der **Settings** eines Dienstes sperrt den Tab nicht; erneutes Oeffnen startet einen neuen Versuch.

## Service-Detail-Panels

Unter **System → PBGUI Services** erscheint beim Speichern der **Settings** von **PBCoinData**, **PBData** oder **PBAPIServer** eine Rückmeldung oberhalb der Dienst-Panels, auch wenn du vor Abschluss der Anfrage das Panel wechselst. Bei Erfolg enthält sie die Anwendungsmeldung des Servers, sofern vorhanden. Jede Rückmeldung hat einen eigenen **Close**-Button; Fehler bleiben bis zum ausdrücklichen Schließen sichtbar und werden nicht durch den Erfolg eines anderen Dienstes ersetzt. Bestehende Inline-Meldungen bleiben erhalten.

Speicheranfragen einschließlich des Lesens der Antwort werden auf 15 Sekunden begrenzt. **Save unconfirmed** bedeutet, dass ein Timeout oder Verbindungsabbruch die Bestätigung verhindert hat; der Server könnte bereits gespeichert haben. Es gibt keine automatische Wiederholung. Prüfe die gespeicherten Einstellungen, bevor du erneut speicherst. Ein Panelwechsel erhält deine Eingaben und bricht das Speichern nicht ab. Beim Verlassen der Seite werden laufende Client-Anfragen abgebrochen; bei Rückkehr aus dem Browsercache erscheint für jeden unterbrochenen Save eine dauerhafte dienstbezogene **Save unconfirmed**-Meldung, ohne alte Anfragen, Timer oder Sperren wiederzubeleben. Bereits abgeschlossene Ergebnisse werden nicht umgedeutet; neuere Eingaben, Dialoge und Meldungen bleiben erhalten. Ein Browserreload stellt Panel und Tab wieder her und lädt gespeicherte Einstellungen; ungespeicherte Entwürfe bleiben dabei nicht erhalten.

Klicke auf eine Service-Kachel (oder den Sidebar-Eintrag), um ein dediziertes Detail-Panel zu öffnen mit:

- Einem Control-Strip mit Service-Status und Aktions-Buttons
- Tabs für verschiedene Ansichten (wo verfügbar):
  - **Log**: Live gefilterter Log-Viewer
  - **Pool**: CMC-Key-Lifecycle, Readiness, Nutzung, Cooldown und Lease-Status (nur PBCoinData)
  - **Settings**: Service-spezifische Konfiguration
  - **Status**: Laufzeit-Status (nur PBData)

Über die Sidebar links zwischen Services wechseln oder zur Übersicht zurückkehren.

## Workers-Panel

Über **System → PBGUI Services → Workers → Market Data Queue** öffnest du den eingebetteten Jobs Monitor. Der authentifizierte eigenständige Einstieg ist `/api/jobs/main_page` unterhalb des PBGui-Anwendungsprefixes. Dieser Einstieg und die statischen Monitor-Einbettungen behalten für API-, WebSocket-, Benachrichtigungs- und Stylesheet-Aufrufe denselben Ursprung und den Anwendungsprefix bei. Die bisherigen URL-Parameter `embed`, `exchange` und `job_type` funktionieren auch beim eigenständigen Einstieg.

Der Sidebar-Eintrag **Workers** öffnet ein eigenes Admin-Panel innerhalb der Services-Seite. Es ist für Betrieb und Fehlersuche gedacht, nicht für die tägliche Bot-Bedienung.

Das Panel gruppiert Worker in:

- **Queue Workers**: gemeinsamer Market-Data-Queue-Worker sowie getrennte PB7-/PB8-Controller fuer Backtest- und Optimize-Queues und der Controller **PB9 Optimize Queue**. Das Stoppen eines PB8- oder PB9-Controllers pausiert nur das automatische Scheduling; detached Optimizer-Jobs laufen weiter.
- **Sync / Watchers**: API-Key-Dateisync, V7-Config-Sync
- **Internal Helpers**: Archive-Sync und HLCVS-Cleanup-Hintergrundtasks

Pro Worker lassen sich ansehen:

- Laufend/gestoppt-Status
- Kleine Laufzeitstatistiken wie Queue-Größe, aktive Jobs, verbundene Hosts oder Watchdog-Status
- Start/Stop/Restart-Aktionen, sofern unterstützt
- Ein lokaler Log-Viewer, wenn der Worker in eine eigene Logdatei schreibt

Stop- und Restart-Aktionen im Workers-Panel fragen vor dem Senden des Befehls zusätzlich nach einer Bestätigung.

Einige Worker nutzen statt eines dedizierten lokalen Logs einen passenden Monitor. Der gemeinsame Market-Data-Queue-Worker verwendet zum Beispiel den Job Monitor, weil die Logs dort pro Queue-Job geführt werden. In diesen Fällen wird der Monitor beim Auswählen des Workers direkt im rechten Log-Bereich eingebettet, bleibt auch bei Worker-Refreshes stabil offen und man bleibt innerhalb der Services-Seite. Der eingebettete Job Monitor bietet jetzt außerdem `View` für die vollständigen Job-Details und `Run` auf Pending-Zeilen; `Run` fordert einen zusätzlichen manuellen Parallel-Slot für denselben Job-Typ an, sodass genau ein ausgewählter Pending-Job neben dem bereits laufenden Job dieses Typs starten kann. Run- und Cancel-Aktionen zeigen ihren laufenden Zustand an und verwerfen wiederholte Klicks, bis ein frischer Jobstatus eintrifft. Aktive Zeilen bleiben jetzt außerdem in einer stabilen Queue-/Start-Reihenfolge, sodass zwei laufende Jobs bei jedem Progress-Update nicht mehr ihre Plätze tauschen. `View`- und `Log`-Dialoge werden jetzt außerdem auf den sichtbaren Browser-Viewport begrenzt und berücksichtigen zusätzlich sowohl den Scroll-Offset der äußeren Seite als auch clippende Eltern-Panels, sodass ihr Close-Button auch dann erreichbar bleibt, wenn der Monitor in einem höheren eingebetteten Bereich sitzt und dessen Kopf bereits über den sichtbaren Browserbereich hinaus gescrollt wurde.

## Typische Startreihenfolge

Ein stabiles Setup startet die Dienste üblicherweise in dieser Reihenfolge:

1. **PBCoinData** — erstellt Symbol-Mappings; CMC-basierte dynamische Filter benötigen einen aktiven materialisierten Pool-Key
2. **PBRun** — startet Bot-Prozesse (nutzt Mappings von PBCoinData)
3. **PBData** — liefert Live-Marktdaten für das Dashboard
4. **PBCluster** — übernimmt Cluster Sync für verbundene Nodes, wenn Cluster Mode aktiv ist
5. **PBAPIServer** — aktiviert Dashboard, VPS Monitor, Job Queue und Echtzeit-Features
6. **PBAPIServer VPS Monitoring Alerts** — konfiguriert Telegram-Routing und die GUI-Sichtbarkeit der Alarme direkt in den API-Server-Einstellungen, wenn genutzt

Gespeicherte Telegram Bot Tokens und Chat IDs sind nur schreibbar. Die Einstellungsseite zeigt lediglich, ob beide Werte konfiguriert sind. Leere Felder behalten die gespeicherten Werte bei; ein neuer Wert ersetzt den bisherigen, und die explizite Clear-Option entfernt beide Credentials.

## Schnelle Fehlersuche

## Wann Einstellungen wirksam werden

PBData- und PBCoinData-Einstellungen werden validiert und vom jeweiligen Owner
im nächsten Planungszyklus übernommen. VPS-Monitor-Hosts, Schwellwerte,
Telegram-Routing und UI-Einstellungen werden live überwacht. API-Bind-Host,
Port, CORS und SSH-Log-Wiring sind Startup-Invarianten und benötigen einen
API-Neustart; PBRun-Identität und PB7-Pfade einen Neustart des zuständigen
Dienstes. PBCluster, Monitor Agent, Remote-INIs sowie request- und joblokale
Werte besitzen bewusst keinen globalen INI-Watcher.

- Ein Dienst zeigt einen roten Punkt, sollte aber laufen: das zugehörige Log im Log-Tab des Dienstes auf Fehler prüfen
- **PBRun**-Listen wirken veraltet: zuerst prüfen, ob **PBCoinData** seine Mappings erfolgreich erstellt hat
- Nach Konfigurationsänderung: betroffenen Dienst über den Restart-Button neu starten

Während des Speicherns sind die Einstellungen gegen doppelte Anfragen gesperrt. Validierungsfehler nennen das betroffene Feld.

Die Restart-Aktion in der Navigation zeigt direkt nach der Bestätigung den Fortschritt und verhindert doppelte Anfragen. PBGui verbindet sich automatisch neu und prüft vor dem Neuladen der aktuellen Seite, ob eine neue API-Instanz läuft. Bei verlorener Antwort wird der Status geprüft, ohne den Neustart automatisch erneut anzufordern.

Restart nutzt wieder den bewährten direkten Ablauf: PBGui prüft geschützte Arbeit, plant die veralteten lokalen Dienste über systemd ein und bestätigt erst danach den Erfolg. Anschließend verbindet sich der Browser neu und prüft die Ersatz-API. Während geschützte Arbeit wie eine PB9-Prüfung läuft, bleibt der Neustart vorgemerkt und zeigt den Wartegrund. Sobald die Arbeit endet, geht er automatisch weiter; erneutes Klicken ist nicht nötig. Die Wartezeit zählt nicht zum Wiederverbindungs-Timeout. Ein Browser-Reload stellt die Fortschrittsanzeige wieder her. Ohne geschützte Arbeit gibt es keine zusätzliche Wartepause. Unabhängige Bot-, Backtest- und Optimize-Prozesse behalten ihre eigenen Laufzeiten.

Die PBCoinData-Prozessprüfung akzeptiert schlüssige NAS-Automounts außerhalb ihrer Nachweispfade. Mehrdeutige Überlagerungen geschützter Pfade führen weiterhin zu Unknown. Verdeckte Tabs geben ihren Status-Stream frei und verbinden ihn beim Anzeigen automatisch neu.

Mit Escape schließt sich der aktive Hilfe-, Preis-, Ergebnis-, CMC-Schlüssel- oder CMC-Authority-Dialog im Services Monitor. Während einer laufenden CMC-Änderung bleibt der entsprechende Dialog geöffnet.

Der Preisdialog aktualisiert Preise automatisch alle fünf Sekunden und Altersangaben jede Sekunde, ohne Textauswahl und Scrollposition zurückzusetzen. Im ausgeblendeten Browser-Tab pausieren die Aktualisierungen und starten bei der Rückkehr sofort wieder. Gleichzeitige Nutzer teilen einen Server-Snapshot für bis zu drei Sekunden.

Ändert sich die Code-Version während eines Neustarts, führt PBGui nach Verfügbarkeit der neuen API einmalig einen weiteren Neustart durch. Die Fortschrittsanzeige nennt Dienste, die noch mit älterem Code laufen. Dauerhafte Fehler bleiben sichtbar, statt Dienste wiederholt neu zu starten.

Remote-Updates von PBGui und Branch-Wechsel starten PBData neu, wenn der Dienst bereits lief. Seine bestehende Autostart-Einstellung bleibt erhalten. Inaktives PBData wird durch das Update nicht gestartet.

Jede Statusabfrage zur Wiederverbindung hat ein Zeitlimit von fünf Sekunden. Eine unbeantwortete Abfrage wird automatisch wiederholt, statt die Fortschrittsanzeige festzuhalten.

Vor dem Neustart beendet PBGui reine GET-Ereignisstreams, damit deren Keepalive-Verbindungen das Herunterfahren nicht verzögern. Credential-Prüfungen bleiben aktiv; unveränderte private Dateien behalten ihre Metadaten, damit gültige Scan-Ergebnisse wiederverwendet werden. KI-Aktionen werden weiterhin vor der Bereitschaft wiederhergestellt; nur das Aufräumen alter abgeschlossener Einträge läuft danach.

Beim API-Neustart dürfen bestehende Runtime-Lesezugriffe weiterlaufen; echte Installationen und Quellcode-Updates bleiben durch ihre exklusive Sperre ausgeschlossen. Unterbrochene Analysen werden vor konkurrierenden Controller-Warmups wiederhergestellt. Die Wiederherstellung genehmigter Aktionen bleibt Voraussetzung für die Bereitschaft.

Beide Restart-Einstiege verwenden dieselbe laufende API-Instanz. Beim Herunterfahren erhalten neue GET-Abfragen vorübergehend 503 mit Retry-After; der Browser verbindet sich automatisch neu.

Sowohl Restart in der Navigation als auch Restart beim API Server erneuern einen
veralteten lokalen VPSMonitor vor der API. Dafür wird seine Start-Serial geprüft,
unabhängig von älteren RPC-Fähigkeiten. Ein erreichbarer älterer Monitor ohne
Start-Serial wird über dieselbe Aktion einmal erneuert; ein aktueller Monitor
läuft weiter. Dienste auf entfernten VPS und Trading-Bots werden nicht neu gestartet.

Die Statusanzeige prüft die laufenden Operationen. Die Restart-Aktion prüft zusätzlich die gespeicherten Wiederherstellungs- und Deployment-Sperren, bevor sie den Neustart annimmt.

Browser-Tabs teilen sich die passiven Restart-Statusprüfungen. Abgeschlossene Prüfungen bleiben drei Sekunden ab ihrem Ende verfügbar, damit langsame Prüfungen nicht vor dem Lesen ihres Ergebnisses ablaufen. Ändern sich Serial oder Laufzeitgründe für einen Neustart, wird der Anzeigecache sofort verworfen; die Restart-Aktion prüft ihre vollständigen Voraussetzungen weiterhin neu. Langsame Anzeigeprüfungen antworten vorübergehend mit einer Wiederholungsaufforderung, statt alle Tabs warten zu lassen. Die Abhängigkeiten für Pareto-Auswertungen und Exchange-WebSockets werden erst bei ihrer ersten Nutzung geladen.

Beim Start oder Neustart des VPSMonitor haben seine anfänglichen SSH-Verbindungen bis zu 60 Sekunden Zeit zur Wiederverbindung, bevor ein neuer Offline-Alarm entsteht. Das läuft im Hintergrund und verzögert den API-Neustart nicht; der Verbindungsstatus bleibt sichtbar. Die erste erfolgreiche Verbindung beendet diese Frist für den jeweiligen Host. Ein späterer Abbruch wird daher normal gemeldet. Authentifizierungsfehler werden sofort gemeldet. Ist ein Host nach der Frist weiterhin nicht erreichbar, entsteht der normale Alarm. Bereits aktive Alarme bleiben bis zur tatsächlichen Behebung bestehen; eine erfolgreiche Wiederverbindung ohne vorherigen Ausfallalarm erzeugt keine Entwarnung.
