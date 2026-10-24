# VPS Monitor


Die globale Alarmanzeige in der Navigation aktualisiert sich automatisch, solange der Browsertab sichtbar ist. Alarmabfragen pausieren in Hintergrundtabs und starten beim Zurückkehren sofort wieder, ohne überlappende Anfragen.

Ausstehende Restart- und Kill-Aktionen bleiben auch bei Monitor-Aktualisierungen deaktiviert. Die Bedienelemente werden bei einer Antwort, einem Timeout oder Verbindungsabbruch wieder freigegeben.

Der VPS Monitor ist das Echtzeit-Operations-Dashboard für alle konfigurierten VPS-Hosts. PBAPIServer sendet den Status über einen Cookie-authentifizierten WebSocket. Dadurch aktualisiert sich die Seite ohne Polling und ohne die Browser-Session in HTML, JavaScript, URLs oder WebSocket-Parametern offenzulegen.

## Aussagekräftige Grenzwertalarme

Öffne **VPSMonitor alerts** in der oberen Navigation. Instanzalarme nennen jede ausgelöste Metrik sowie den gespeicherten Messwert, den strikten `>`-Vergleich und das konfigurierte Fehlerlimit: Speicher/Swap in MB, bestätigter CPU-Mittelwert über 60 Sekunden in % und die heutigen Fehler-/Traceback-Anzahlen. Auslösevergleiche behalten die numerische Genauigkeit auch bei Messwerten knapp über einem Limit und bei Dezimalgrenzwerten. Die Übersicht der aktuellen Messwerte bleibt erhalten. CPU benötigt aktuelle Metriken und ein vollständiges Mittelwertfenster.

**Adjust Memory threshold**, **Adjust CPU threshold** und die entsprechenden Metriklinks öffnen **System → PBGUI Services → PBAPIServer → Settings → VPS Monitoring** direkt am Fehlergrenzwert unter **V7 Monitor** (Instanzen) beziehungsweise **Server Monitor** (Systeme). Das funktioniert auch auf der Services-Seite selbst. Erst nach erfolgreichem Laden wird das Feld fokussiert; ein Browser-Neuladen stellt das Feldziel wieder her. Ungespeicherte Eingaben bleiben bei Statusaktualisierungen, beim Wechsel zwischen Grenzwertfeldern und beim erneuten Laden nach Fehlern erhalten. Auch während der Fehlermeldung geänderte Eingaben überstehen Retry. Bei der Rückkehr zu einer im Browser-Seitencache erhaltenen Seite bleiben Entwurf, Fokus und Grenzwertziel erhalten; unterbrochenes Laden wird nur für den aktiven Settings-Tab fortgesetzt. **Save** übernimmt bewusste Änderungen; das Öffnen eines Links ändert keine Konfiguration.

**Ack** und **Ack all** funktionieren unverändert. Aktive Alarme aktualisieren Messwerte und Limits, ohne die Quittierung aufzuheben. Die Historie behält die zum Alarm gespeicherten Messwerte und Limits; ältere Einträge ohne Auslöserdaten bieten **Threshold settings**, ohne historische Limits zu erfinden. Ein nicht verfügbares Feld belässt dich mit kurzem Hinweis in Settings. Nach Ladefehler oder Timeout bleibt Speichern bis zu einem erfolgreichen erneuten Laden gesperrt.

## Was du überwachen kannst

- SSH-Verbindungsstatus jedes Hosts
- Live-Metriken für CPU, RAM, Disk, Swap und Bot-Prozesse
- Service-Status für PBCluster, PBRun, PBData, PBCoinData und PBMonitorAgent
- Bot-Instanzen und Synchronisierungsdetails über alle Hosts
- Live-Logs für Services und Bots
- Monitor-Agent-Heartbeat, Cache-Dateistatus und Collector-Fehler

## Monitor-Agent-Datenquelle

Auf jedem VPS läuft `pbgui-monitor-agent.service`. Der Service misst den lokalen Host einmal und schreibt Cache-Dateien für alle verbundenen PBGui-Master. Der VPS Monitor verwendet ausschließlich diesen Monitor-Agent-Cache. Bei fehlenden, veralteten oder ungültigen Cache-Daten gibt es keinen direkten Collector-Fallback.

Der kanonische Live-Stream ist:

```text
data/monitor_agent/live_metrics.ndjson
```

Der vollständige Cache besteht aus:

```text
data/monitor_agent/live_metrics.ndjson
data/monitor_agent/live_metrics.latest.json
data/monitor_agent/instance_snapshot.json
data/monitor_agent/host_meta.json
data/monitor_agent/service_status.json
data/monitor_agent/package_status.json
data/monitor_agent/collector_status.json
```

Snapshot-JSON-Dateien werden atomar ersetzt. Der NDJSON-Stream nutzt die von PBGui verwaltete, Byte-basierte Aufbewahrung und Rotation. Dadurch bleibt die Plattenbelegung begrenzt, während Leser dem Stream weiter folgen können.

Solange die Live-Telemetrie frisch ist, behält ein Instanz-Snapshot die aktuellen CPU-, RAM- und Swap-Werte desselben Botlaufs bei. So bleiben Grenzwert-Alarme zwischen Live-Samples stabil. Ohne eigene `[monitor]`-Einstellungen liegen die PB7-Bot-Grenzwerte für CPU-Warnung und -Fehler bei 15 % und 20 %; der RAM-Fehlergrenzwert bleibt bei 500 MB.

## Agent-Gesundheit

Jede Host-Karte zeigt **Monitor Agent: OK, Stale, Missing, Error oder Unknown** und nennt als Quelle immer `monitor-agent cache`.

- Live-Telemetrie gilt bis **15 Sekunden** nach dem letzten Sample als gesund.
- Der Collector-Heartbeat gilt bis **30 Sekunden** als gesund.
- **Stale** bedeutet, dass ein effektives Alter einen dieser Grenzwerte überschritten hat.
- **Missing** bedeutet, dass eine erforderliche Cache-Datei als fehlend gemeldet wurde.
- **Error** bedeutet, dass Agent, erforderliche Datei oder Collector-Schleife einen Fehler gemeldet hat.
- **Unknown** bedeutet, dass noch keine nutzbare Monitor-Agent-Diagnose vorliegt.

Der Detailbereich zeigt Heartbeat- und effektive Alter, den Status jeder erforderlichen Datei und begrenzte Collector-Fehler. Die SSH-Verbindung ist davon unabhängig: Ein Host kann über SSH weiterhin **connected** sein, während seine Monitor-Agent-Telemetrie stale ist.

## Tabs und Workflow

- **Dashboard**: Host-, SSH-, Telemetrie- und Monitor-Agent-Gesundheit
- **Instances**: laufende und bereitgestellte PB7-/PB8-Bot-Instanzen mit expliziter Runtime-Identitaet, Drift zwischen Config-/Running-Version sowie Stopped-/Disabled-Status. Gleiche PB7-/PB8-Namen bleiben getrennte Zeilen; Log- und Kill-Aktionen behalten die Runtime bei und treffen nicht die falsche Instanz. Eine gestoppte PB8-Zeile erscheint nur aus einer frischen, ausdruecklichen Monitor-Agent-Beobachtung; stale oder nicht gestempelte Inventardaten erfinden keinen gestoppten Bot.
- **Services**: Status und Neustartaktionen für PBCluster, PBRun, PBData, PBCoinData und PBMonitorAgent
- **Live Logs**: Echtzeit-Streams für Service- und Bot-Logs

Auf ausdrücklich als Slave konfigurierten Nodes mit aktivierter automatischer Dienstheilung zeigt **Services → PBCoinData** `idle`, sobald ein eigener Prozess bestätigt beendet ist und kein laufender PB7-Verbraucher mit Dynamic Ignore ihn benötigt. Ein laufender Verbraucher und ein bereiter CMC-Pool erlauben den automatischen Wiederanlauf. Der lokale Agent steuert systemd-Units im regulären 60-Sekunden-Zyklus; die Unit bleibt installiert. Im Legacy-Betrieb hält ein erfolgreich initialisierter aktueller PBRun einen exklusiven Controller-Lease und gleicht den Bedarf im vorhandenen Fünf-Sekunden-Wartungszyklus ab. Der Agent kann einen freien Lease für einen Fallback-Zyklus übernehmen. Master behalten das berechtigungsabhängige Verhalten; PB8-Live-Bots benötigen diesen DynamicIgnore-Daemon nicht.

Prozesszustand und Bedarfsbegründung werden getrennt angezeigt: Ein unbekannter Verbraucherstatus löst weder automatisches Stoppen noch Starten aus. Recovery-Metadaten benennen den verifizierten lokalen Controller gesondert. Fehlgeschlagene oder unbestätigte Stops behalten den tatsächlichen Zustand und die Diagnose. Legacy-Aktionen verlangen die exakte Zuordnung über UID, Python-Skript, Checkout, Arbeitsverzeichnis, PID und Startzeit; unbekannte oder fremde Prozesse werden nie signalisiert. **Restart** bleibt eine explizite Aktion und wird nicht durch PBRuns Controller-Lease blockiert. Statusänderungen kommen automatisch über den vorhandenen Monitor-Stream.

Bei aktivem Bedarf begrenzt der Slave-Daemon automatische Mapping-Arbeit auf die Exchanges aller bekannten laufenden PB7-Dynamic-Ignore-Verbraucher. Unvollständige Account- oder Prozesserkennung behält alle Exchanges bei. Master und direkte Coin-Data-UI-Aufträge behalten vollständigen Zugriff. Legacy-Entladen benötigt verifizierte Recovery-Befugnis; eine PBRun-PID allein genügt nicht, und ein allein gestarteter Daemon ohne diese Befugnis behält seine wartende Idle-Schleife. Eine installierte systemd-Unit oder ein unbekannter Manager blockiert automatische Legacy-Starts. Kehrt Bedarf während eines laufenden systemd-Übergangs zurück, beobachtet der Agent diesen, statt einen konkurrierenden Neustart auszulösen. Der PBCoinData-Service-Guide beschreibt Exchange-Auswahl, begrenzte Prozessaktionen und Bootstrap.

Ein manueller **Restart** auf einem unbeschäftigten Slave hält den Daemon nicht dauerhaft im Speicher: Bei aktivierter automatischer Heilung entlädt der gewählte lokale Controller ihn im nächsten bestätigten IDLE-Zyklus erneut. PBRuns Legacy-Zyklus beträgt fünf Sekunden, der Agent-Zyklus 60 Sekunden; Dienstübergänge können länger dauern, und kurze Zwischenzustände können zwischen zwei GUI-Aktualisierungen liegen.

Bei fehlendem User-Bus oder systemctl benötigt Legacy-Recovery einen neuen vollständigen Offline-Nachweis der Unit- und Benutzermanager-Abwesenheit; ein Busfehler allein genügt nicht. Unlesbare Suchpfade, lebende Manager oder unklare Namespace-/Prozesssicht bleiben unbekannt; die Begründung erscheint über vorhandene Statusfelder. Tatsächliche Startfehler teilen sich ein persistiertes Backoff von 60–1800 Sekunden über PBRun-/Agent-Wechsel hinweg. Manueller **Restart** darf diese Frist umgehen; unbekannte Eigentümerschaft blockiert weiterhin Aktionen. Der PBCoinData-Service-Guide beschreibt Evidenz-, Uhr- und Persistierungsgrenzen.

Manueller Restart verwendet das gemeinsame Guard-Zuständigkeitsprotokoll ohne Klassifikation gewöhnlicher fremder Dienste. Fehlende Übergangsevidenz allein blockiert explizite manuelle Aktionen nicht; die automatische Legacy-Entladevorprüfung verlangt einen unabhängig bestätigten Übergang. Systemd verwendet stattdessen frische konkrete Unit-/MainPID-/ControlGroup-/Checkout-/Guard-Evidenz ohne Legacy-Attestierung. Ein Service-Eltern-Cgroup allein sperrt manuelle Aktionen nicht; die separate Legacy-Herkunft muss zusätzlich belegbar sein. Ohne Übergang bleibt ein laufender Legacy-Idle-Daemon resident; fehlender Übergang allein sperrt eine sonst zulässige ACTIVE-Recovery nicht. Strukturelle Sperren und gültige Cooldowns erhalten den Idle-Prozess mit Begründung; reparierbare Legacy-Retry-Inhalte verhindern IDLE nicht. Credentials werden bei ACTIVE erneut geprüft. Der PBCoinData-Service-Guide erklärt den getrennten Betreiberbefehl, die Grenze für kopierte/ungeschützte Altbestände und die sichere offene Prüffolge. GUI-, echte Ubuntu-VM-, Live-systemd- und RAM-Abnahmen werden nicht behauptet.

## Live-Log-Features

- Echte Datei-Zeilennummern
- Blockweises Ein- und Ausklappen von Logblöcken
- Volltextsuche mit Hervorhebung
- Auto-Scroll und Compact Mode
- Host- und Service-Auswahl mit Stream-Steuerung
- Restart aktiviert den ausgewaehlten Log-Stream vorab, damit fruehe Startzeilen ohne manuellen Fetch sichtbar bleiben

## Voraussetzungen

- PBAPIServer muss laufen.
- Ziel-VPS-Hosts müssen in den PBAPIServer-Einstellungen aktiviert sein (`System → Services → PBAPIServer → Settings`).
- `pbgui-monitor-agent.service` muss auf jedem VPS installiert sein und laufen.
- Der PBAPIServer-WebSocket-Endpunkt muss für den Browser erreichbar sein.

## Fehlersuche

Prüfe den Monitor Agent auf dem betroffenen VPS:

```bash
systemctl --user status pbgui-monitor-agent.service
journalctl --user -u pbgui-monitor-agent.service
```

Starte ihn bei Bedarf neu:

```bash
systemctl --user restart pbgui-monitor-agent.service
```

- **Keine Daten sichtbar**: PBAPIServer unter `System → Services` starten.
- **Ein Host fehlt**: prüfen, ob der Host in den PBAPIServer-Einstellungen aktiviert ist.
- **Agent Missing**: VPS-Installation aktualisieren oder migrieren, damit `pbgui-monitor-agent.service` und alle Cache-Dateien installiert sind.
- **Agent Stale**: Service-Status und Journal prüfen. SSH kann verbunden bleiben, während die Telemetrie stale ist.
- **Agent Error**: die begrenzten Collector-Fehler in den Host-Details prüfen und danach den vollständigen lokalen Fehler im Service-Journal lesen.
- **Verbindungsverlust während einer Aktion oder History-Abfrage**: Das wartende Control wird automatisch wieder freigegeben. Auf die erneute WebSocket-Verbindung im Banner warten und die Aktion danach wiederholen.
- **Logs streamen nicht**: `PBApiServer.log` auf Verbindungsfehler prüfen.
