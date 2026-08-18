# VPS Manager

Die **VPS Manager** Seite erlaubt das Hinzufügen, Konfigurieren und Warten von Remote-VPS-Servern, auf denen Passivbot-Instanzen laufen.
Jeder VPS wird über Ansible-Playbooks verwaltet, die vom Master (lokal) ausgeführt werden.

Der Standard-Menüeintrag **System -> VPS Manager** öffnet die eigenständige **FastAPI**-Seite.

---

## Übersichtstabelle

Die Hauptansicht zeigt eine Tabelle mit allen Servern (Master + VPS) und ihrem aktuellen Status.

| Spalte | Beschreibung |
|--------|-------------|
| **Name** | Server-Hostname (Master wird als lokal angezeigt) |
| **Role** | 🧠 Master / 💻 VPS |
| **Online** | ✅ erreichbar / ❌ offline |
| **Bots** | Anzahl eindeutig laufender Bots, die aktuell für diesen VPS per Telemetrie gemeldet werden |
| **Started** | Letzter Boot-Zeitpunkt |
| **Updates** | Ausstehende Linux-Paketupdates; ein Klick auf eine Anzahl größer null zeigt Paketnamen, installierte/neue Version, Quelle und die Einordnung Security/Kernel/Routine |
| **PBGui / PBGui Branch / PBGui GitHub** | Installierte Version, Branch und ob sie mit dem GitHub-Origin übereinstimmt |
| **PB7 / PB7 Branch / PB7 GitHub** | PB7-Version, Branch und ob sie mit dem GitHub-Origin übereinstimmt |
| **PB8 / PB8 Branch / PB8 GitHub** | PB8-Version, Branch und ob sie mit der aktuellen Upstream-PB8-Revision uebereinstimmt |

Interaktionen in der Übersicht:

- Klick auf einen Spaltenkopf sortiert nach dieser Spalte; ein zweiter Klick dreht die Sortierrichtung um.
- Jeder sichtbare Spaltenkopf hat ein kleines Ausblend-Icon, mit dem sich genau diese Spalte direkt aus der Tabelle entfernen lässt.
- Ganz rechts im Tabellenkopf setzt ein einzelnes kleines Reset-Icon die Standardspalten und die Standard-Sortierung wieder her.
- Spaltensichtbarkeit und Sortierung werden lokal im Browser gespeichert.
- Durch Klicken und Ziehen über VPS-Zeilen lassen sich mehrere Deploy-Ziele direkt in der Übersicht markieren.

Linke Sidebar:

| Schaltfläche | Aktion |
|--------|--------|
| **Add VPS** | Formular zum Hinzufügen / Initialisieren öffnen |
| **Refresh** | Optional alle VPS-Status- und Versionsdaten sofort über das Refresh-Icon neu laden |
| **Overview / Settings / History** | Zwischen Live-Übersicht, gemeinsamen Deploy-Einstellungen und letzter Deploy-Historie wechseln |
| **Import by Hostname** | Den manuellen Hostname-Import aus dem Sidebar-Bereich **Import Host** öffnen; der Hostname muss bereits lokal über `/etc/hosts` auflösbar sein |
| **Import Cluster Nodes** | Sichere SSH-Metadaten aus Cluster-Sync-Nodes als lokale VPS-Manager-Hosts vorab anzeigen und importieren; Secrets werden nicht importiert |

Die Übersicht nutzt jetzt die normale gemeinsame PBGui-FastAPI-Shell. Beim Wechsel zu **Master** oder zu einem konkreten **VPS** wird die linke Sidebar zu einer kontextabhängigen Aktionsliste umgeschaltet. Der Hauptbereich der Übersicht bleibt dabei auf die Tabelle fokussiert, während der Host-Import als manuelle Hostname-Aktion oder nach einem Cluster-Sync-Join als **Import Cluster Nodes** in der Sidebar verfügbar bleibt.

Die Detailkoepfe fuer Master und VPS wiederholen die Versionskarten fuer PBGui, PB7 und PB8 samt Branch/Commit und Update-Status. Die Werte stammen aus demselben Monitor-Agent-Snapshot wie die Overview-Zeile.

Die normale Aktion **Update PB8** setzt PB8 absichtlich auf einen Detached Checkout des verifizierten Upstream-`master`-Commits zurueck. Wenn dieser Detached Commit exakt der verifizierten Upstream-Revision entspricht, zeigt VPS Manager `master` statt des technischen Git-Zustands `unknown` an. Die explizite Ansicht **PB8 Branch** kann stattdessen einen validierten v8-Branch oder Commit aus dem konfigurierten Upstream oder einer Custom-Remote/einem Fork verfolgen.

**Import Cluster Nodes** liest den lokal materialisierten `cluster_nodes`-State und importiert nicht-lokale Nodes mit SSH-Metadaten, unabhängig vom Cluster-Sync-Modus. Deaktivierte Cluster-Sync-Nodes können trotzdem in den VPS Manager importiert werden; disabled bedeutet nur, dass PBCluster nicht über diesen Node replizieren soll. Importiert werden nur sichere lokale VPS-Manager-Metadaten wie Hostname, SSH-Host, SSH-User, SSH-Port und Remote PBGui Dir; Passwörter und Private Keys werden nicht importiert. CMC-Secrets sind keine VPS-Manager-Felder: Cluster Sync materialisiert versiegelte Pool-Generationen getrennt. Wenn lokale `/etc/hosts`-Einträge fehlen oder auf eine andere IP zeigen, zeigt die Import-Vorschau die nötigen Host-Eintragsänderungen und fragt beim Anwenden nach dem lokalen sudo-Passwort, bevor sie geschrieben werden. Das Modal fragt das VPS-User-Passwort pro importiertem Host ab; Zeilen ohne Passwort werden übersprungen, während eingegebene Passwörter einmalig genutzt werden, um Remote-Settings zu lesen, den Monitoring-SSH-Key zu installieren und das Passwort nur in der aktuellen Browser/API-Session für spätere SSH-Aktionen zu halten.

Die Seite hält eine Live-WebSocket-Verbindung für Übersicht, Fortschritt und Branch-Status offen. PBGui prüft den Head des verfolgten PBGui-Branches automatisch alle 60 Sekunden und lädt bei einer Änderung die vollständigen Release-Metadaten neu; die Update-Farben benötigen daher keine manuelle Refresh-Aktion. Die Browser-Authentifizierung verwendet ausschließlich das Cookie; PBGui rendert den Session-Token weder in diese Seite noch sendet es einen Browser-Bearer-Header.

Die Live-Updates schließen die **VPS**-Auswahl in der Sidebar beim Umschalten zwischen Hosts nicht mehr.

Live-Aktualisierungen erneuern jetzt nur noch die tatsächlich geänderten Statusbereiche. Beim Tippen in Add-/Edit-Formularen bleibt der Cursor daher im Feld, und geöffnete Passwort-Augen bleiben offen, während neue Monitor- oder Fortschrittsdaten eintreffen.

---

## Master-Verwaltung

Über **Master** in der linken Steuerleiste wird die Verwaltung des lokalen Servers geöffnet.

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| **Overview** | Zur Hauptübersicht des VPS Managers zurückkehren |
| **Back to Master Overview** | Von Branch-/Log-Unteransichten zurück zur normalen Master-Detailansicht |
| **Task Logs** | Den dedizierten Shared-Log-Viewer für gespeicherte Master-Playbook-Logs öffnen |
| **Host Logs** | Den dedizierten Shared-Log-Viewer für lokale Service-Logs und dateibasierte Ziele öffnen |
| **PBGui Branch** | Die PBGui-Branch-Verwaltung öffnen |
| **PB7 Branch** | Die PB7-Branch-Verwaltung öffnen |
| **PB8 Branch** | Bei installiertem PB8 die PB8-Branch-Verwaltung öffnen |
| **Update PBGui and PB7 / Update PBGui and PB8** | PBGui zusammen mit der gewählten Runtime aktualisieren; kombinierte PB7+PB8-Hosts bieten beide Aktionen an |
| **Update PBGui** | Nur PBGui aktualisieren |
| **Install PB7 / Update PB7** | Eine fehlende PB7-Runtime installieren oder den vorhandenen PB7-Checkout und dessen Virtualenv aktualisieren |
| **Install PB8 / Update PB8** | PB8 aus Upstream `master` installieren oder den getrennten PB8-Checkout und dessen Virtualenv aktualisieren |
| **Update Linux** | Linux-Paketupdates ausführen (optionale Reboot-Checkbox) |
| **Reboot Master** | Den lokalen Server neu starten |
| **Install or Update rustup** | Rust-Toolchain installieren oder aktualisieren |

Der **Master**-Inhaltsbereich enthält zusätzlich:
- ein Live-Statusraster für CoinData / letzten Command
- **PBGui Branch Management** für Branch- oder Commit-Wechsel
- **PB7 Branch Management** mit optionaler Custom-Remote- / Fork-URL
- **PB8 Branch Management** fuer installierte PB8-Checkouts mit unabhaengiger Branch-/Commit-Auswahl und optionaler Custom-Remote- / Fork-URL
- einen **Monitor**-Bereich mit Server-Metriken plus runtime-gekennzeichneter PB7-/PB8-Aktivitaet aus laufenden Prozessen und Cluster-Sync-Zielzustand
- einen **Progress**-Bereich mit getrennten Status-Buckets; sobald eine Sidebar-Aktion einen Master-Ansible-Task startet, schaltet die Hauptfläche auf den gemeinsamen **Command Log Viewer** um, und **Home** bringt zurück zur normalen Master-Ansicht

Im Cluster-Modus synchronisieren **Update PBGui** und PBGui-Branch-Wechsel die lokale PBCluster-systemd-User-Unit und starten PBCluster neu. PBCluster ist außerdem in lokaler Service-Überwachung und Service-Control sichtbar. Ein manueller `git pull` startet PBCluster nicht neu; nutze danach `systemctl --user restart pbgui-pbcluster.service`.

PB8 verwendet `<install_dir>/pb8` und `<install_dir>/venv_pb8`, validiert PB8-CLI, Rust-Erweiterung und V8-Config-Schema und speichert danach `pb8dir` und `pb8venv` in `pbgui.ini`. Lokale und entfernte Master erhalten das vollstaendige PB8-Profil fuer Backtest und Optimize. Verwaltete VPS-Runner erhalten nur das minimale Live-Profil, verwenden pip ohne Download-Cache, entfernen nach der Validierung das temporaere Rust-Buildverzeichnis und aktivieren den gemeinsamen PBRun-Controller fuer PB8-Live-Supervision. Eine erste Remote-PB8-Installation verlangt mindestens 3 GiB freien Speicher. Eine bereits validierte PB8-Runtime bleibt auch unterhalb dieser Erstinstallationsreserve aktualisierbar. Das Playbook protokolliert vor und nach beiden Operationen freien Speicher sowie die Groessen von Checkout und Venv. Die RAM-Groesse ist keine Installationssperre.

Auch eine erste Remote-PB7-Installation verlangt mindestens 3 GiB freien Speicher. Der VPS Manager deaktiviert **Install PB7**, wenn frische Telemetrie weniger Platz meldet; das Playbook wiederholt die Pruefung vor rustup- oder Git-Downloads. Bereits validierte PB7-Installationen bleiben unterhalb der Erstinstallationsreserve updatefaehig.

Bulk-Updates in der Overview beruecksichtigen das Runtime-Profil jedes Hosts. **Update Runtime by Profile** und **Update PBGui and Runtime by Profile** senden PB7-only-Hosts an PB7-Playbooks, PB8-only-Hosts an PB8-Playbooks und kombinierte Hosts an geordnete PB7+PB8-Playbooks. Gemischte Selektionen behalten damit einen gemeinsamen parallelen oder sequenziellen Rollout, ohne PB7-Tasks auf PB8-only-Hosts anzuwenden.
Kombinierte lokale Master-PBGui-/Runtime-Updates verschieben den API-Neustart, bis alle importierten Runtime-Updates und Validierungsschritte abgeschlossen sind. Das Command Log bleibt waehrenddessen offen und erhaelt seinen Endstatus, bevor die API neu startet.
Wenn PB8 nicht installiert ist, wird **Install PB8** als gefüllte blaue Aktion hervorgehoben und bleibt dadurch klar von routinemäßigen Update-Schaltflächen unterscheidbar.

---

## VPS-Verwaltung

Klick auf eine VPS-Karte in der linken Leiste öffnet die Detailansicht des jeweiligen VPS.

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| **Overview** | Zur Hauptübersicht des VPS Managers zurückkehren |
| **Hostname selector** | Direkt zwischen gespeicherten VPS-Hosts wechseln, ohne den VPS-Kontext zu verlassen |
| **Back** | Von Branch-/Log-/Setup-Unteransichten zurück zur normalen VPS-Detailansicht |
| **Task Logs** | Den dedizierten Shared-Log-Viewer für alle gespeicherten VPS-Playbook-Logs inklusive Historie öffnen |
| **Host Logs** | Den dedizierten Shared-Log-Viewer für VPS-Service-Logs und dateibasierte Ziele öffnen |
| **Change VPS** | Die VPS-Konfigurationsansicht für gespeicherte Host-Einstellungen öffnen |
| **PBGui Branch** | Die PBGui-Branch-Verwaltung öffnen |
| **PB7 Branch** | Die PB7-Branch-Verwaltung öffnen |
| **PB8 Branch** | Bei installiertem PB8 die PB8-Branch-Verwaltung öffnen |
| **Initialize** | Ersteinrichtungs-Assistent starten |
| **Delete VPS** | Diesen VPS aus PBGui entfernen |
| **Update PBGui** | PBGui auf diesem VPS aktualisieren |
| **Install PB7 / Update PB7** | PB7 auf einem PB8-only-Host installieren oder eine vorhandene PB7-Runtime aktualisieren |
| **Update PBGui and PB7 / Update PBGui and PB8** | PBGui zusammen mit der gewählten Runtime aktualisieren; kombinierte PB7+PB8-Hosts bieten beide Aktionen an |
| **Install PB8 / Update PB8** | Die Installation ist bei frischer Telemetrie, unterstuetzter Rolle und mindestens 3 GiB freiem Speicher verfuegbar; eine validierte vorhandene PB8-Runtime benoetigt fuer Updates nicht die Erstinstallationsreserve |
| **Update Linux** | `apt upgrade` ausführen (optionale Reboot-Checkbox) |
| **Reboot VPS** | VPS neu starten |
| **Cleanup VPS** | Alte Pakete und Logs entfernen |

Der **VPS**-Inhaltsbereich enthält zusätzlich:
- ein Setup-/Konfigurationsraster für Passwort, Swap und Firewall-Felder; **Apply VPS Changes** speichert Änderungen lokal und wendet geänderte Swap- und Firewall-Einstellungen auf der VPS an
- **PBGui Branch Management**, **PB7 Branch Management** und bei installierter Runtime **PB8 Branch Management** mit demselben Switch-/Update-Workflow wie beim Master
- einen **Remote Monitor** mit Server-Metriken plus runtime-gekennzeichneter PB7-/PB8-Aktivitaet aus detaillierten Prozessmetriken und Cluster-Sync-Fallbacks
- einen **Progress**-Bereich mit getrennten Status-Buckets für Init-, Setup- und Update-Läufe; für die vollständige Ansible-Ausgabe werden die Sidebar-Aktionsknöpfe auf den gemeinsamen **Command Log Viewer** umgeschaltet

Im Cluster-Modus synchronisieren **Update PBGui** und PBGui-Branch-Wechsel auf einer VPS die PBCluster-Service-Dateien und starten PBCluster, PBRun und PBCoinData neu, sofern diese Services konfiguriert sind. VPS-systemd-Migrationsprüfungen schließen PBCluster ein, und die Remote-Service-/Host-Log-Ansichten zeigen `PBCluster.log`. Reine VPS-Runner brauchen weiterhin kein `pbgui-api.service` und kein `PBApiServer.py`.

Die PB7- und PB8-Branch-Verwaltung fuehren getrennten Browser-State und getrennte Remote-Caches. In beiden Runtime-Ansichten kann eine bekannte Remote gewaehlt oder eine Fork-URL eingegeben werden. Danach lassen sich Remote-Branches und Commits laden, der lokale Ziel-Branch waehlen und die passend beschriftete Switch-/Update-Aktion starten. Fehlt der gewaehlte Source-Branch auf einer geladenen Remote, bleibt die Aktion deaktiviert. Live-Statusupdates bauen das Branch-Panel nicht neu auf, solange einer seiner Selektoren geoeffnet ist. **Update PB8** ohne Branch-Management-Auswahl stellt weiterhin den verifizierten Upstream-`master` wieder her; **PB8 Branch** ist nur fuer das bewusste Tracking eines anderen validierten v8-Branches oder Commits gedacht.

Die Sidebar trennt die Log-Workflows jetzt bewusst von der normalen Host-Ansicht:
- Utility-Aktionen wie **Task Logs**, **Host Logs**, **Change VPS**, **Initialize** oder **Delete VPS** bleiben oberhalb eines Trenners, während die ausführbaren Ansible-Playbook-Knöpfe darunter gruppiert sind
- **Task Logs** öffnet einen eigenen gefilterten Viewer für alle gespeicherten Playbook-Logs des ausgewählten VPS inklusive rotierter Historie
- Aktionen wie **Initialize**, **Setup VPS**, **Update PBGui**, **Update PBGui and PB7**, **Update Linux** oder **Cleanup VPS** schalten die Hauptfläche automatisch auf den gemeinsamen **Command Log Viewer** um
- **Host Logs** öffnet einen eigenen **Host Log Viewer** für Service-Logs, laufende Bot-Logs und dateibasierte Ziele wie `PBCluster.log`
- **Back** bringt von Branch-, Setup- oder Log-Screens zurück in die normale VPS-Detailansicht, ohne den gewählten Host-Kontext zu verlieren
- jeder aufrufbare VPS-Manager-Task bekommt jetzt sein eigenes aktuelles Log plus rotierte Historie im gemeinsamen Viewer; standardmäßig bleiben 10 Historien-Dateien erhalten, konfigurierbar über `[vps_manager] task_log_history` in `pbgui.ini`
- wenn die Ansible-Ausgabe bereits Terminal-ANSI-Farben enthält, übernimmt der gemeinsame Viewer diese Farben jetzt auch im Browser, statt nur über Textmuster zu raten
- Ansible-Task-Logs mit verklebten Ergebnis-Markern oder escaped Payload-Steuerzeichen wie `\n` / `\r` werden jetzt im gemeinsamen Viewer in lesbare getrennte Anzeigezeilen aufgelöst
- strukturierte Ansible-Ergebnis-Payloads mit JSON-Inhalt werden jetzt als mehrzeilige Blöcke hübsch formatiert, damit verschachtelte Metadaten wie `stat` direkt im gemeinsamen Viewer lesbar sind
- lokale **Update PB8**-Wiederholungen bereinigen automatisch einen verwaisten Writer-Lock eines abgestuerzten Updates, wenn der Lock leer, mindestens fuenf Minuten alt und kein Ansible-/PB8-Buildprozess mehr aktiv ist; die Bereinigung erscheint im Task-Log, waehrend neue, nicht leere oder noch aktive Locks blockiert bleiben

Die Status-Kacheln oberhalb des Setup-Rasters sind jetzt direkte Operator-Hinweise:
- Der Linux-Paketstatus ist unabhängig vom VPS-Session-Passwort. Normale Anzeige-Updates lesen ausschließlich den Monitor-Agent-Cache. Ein erfolgreiches **Update Linux** führt nach einem gegebenenfalls angeforderten Reboot einmalig einen abschließenden Paket-Probe aus, aktualisiert diesen Cache atomar und lässt den Master ihn sofort einlesen.
- Ein Klick auf einen von null verschiedenen **Updates**-Wert in der Overview oder im Host-Header öffnet die gecachte apt-Paketliste einschließlich neu installierter Abhängigkeiten und geplanter Entfernungen. Security-Updates werden zur zeitnahen Installation markiert, Entfernungen verlangen eine Prüfung der Abhängigkeiten und Dienste, Kernel-Updates empfehlen ein Wartungsfenster und gegebenenfalls einen Reboot, Routine-Updates können in der normalen Wartung installiert werden. Unvollständige Listen bleiben unklassifiziert, statt die Dringlichkeit zu niedrig anzugeben. Ältere Agent-Caches bleiben lesbar, zeigen aber nur die Anzahl, bis PBGui den Agent-Payload aktualisiert.
- **Credential Capability** und **Credential Protocol** melden secret-freie CMC-Pool-Readiness, aktive Key-Anzahl sowie Katalog-/Materialisierungs-Generationen, wenn verfügbar.
- **Monitor Agent Cache** zeigt immer **Source: agent cache** und einen eindeutigen Zustand **OK**, **Stale**, **Missing** oder **Error**. Ein nicht-OK Cache bedeutet nicht, dass SSH offline ist; SSH-Verbindung und Telemetrie-/Cache-Zustand werden getrennt angezeigt.
- Das Panel listet `live_metrics.ndjson`, `instance_snapshot.json`, `host_meta.json`, `service_status.json`, `package_status.json` und `collector_status.json` mit dem effektiven Alter jeder Datei. Live-Daten werden nach 15 Sekunden stale, der Collector-Status nach 30 Sekunden. Collector-Loops und deren letzte Fehler werden separat angezeigt.
- Ausstehende Linux-Updates und Reboot-Hinweise stammen ausschließlich aus dem validierten Agent-Payload `package_status.json`. Ein positiver Reboot-Hinweis, der vor dem aktuellen Boot des Hosts erfasst wurde, wird verworfen, weil dieser Reboot bereits erfolgt ist. Andere stale Payloads behalten ihre letzten bekannten Werte und kennzeichnen sie klar. Fehlende, ungültige oder fehlerhafte Payloads bleiben **N/A** und werden nie als null Updates oder als aktuelles System angezeigt.
- PB8 **PNL Tdy** verwendet den Exchange-Fill-Zeitpunkt aus jedem einzelnen PB8-Fill-Logeintrag. Undatierte Startup-Batch-Summaries beschreiben die historische Synchronisierung und werden niemals dem aktuellen Tag zugeordnet.
- Ein Klick auf CPU, Speicher oder Swap eines Bots öffnet dessen runtime-spezifische 24-Stunden-Historie. PB7- und PB8-Instanzen mit gleichem Namen bleiben getrennt, und ein gültiger Swap-Wert von null wird gespeichert.
- PB8 **ERR 4W** und **TB 4W** stammen aus dem begrenzten UTC-Stunden-Scan des lokalen Monitor-Agents über native und stderr-Logs. **PNL Hist** verwendet die neueste verlässliche PB8-Fill-Batch-Gesamtsumme und bleibt von einer gleichnamigen alten PB7-Historie getrennt. Da PB8 keine verlässliche tägliche Netto-PnL-Aufteilung bereitstellt, zeigt PBGui die verlässliche Gesamtsumme, ohne eine Tageskurve zu erfinden.
- Die Detailseite enthält außerdem wieder eine einzeilige Zusammenfassungstabelle plus einen Remote-Server-Ressourcenblock ähnlich zur früheren Serveransicht.

Bei einem nicht-OK Agent nutze **Update PBGui** in der Inline-Fehlerbehebung. Diese Aktion installiert oder aktualisiert den Agent-Service, startet ihn neu und die UI wartet anschließend den nächsten 30-Sekunden-Collector-Zyklus ab. Zur manuellen Prüfung oder Wiederherstellung auf dem betroffenen Host dienen exakt diese Befehle:

```bash
systemctl --user status pbgui-monitor-agent.service
systemctl --user restart pbgui-monitor-agent.service
journalctl --user -u pbgui-monitor-agent.service
```

`Cleanup VPS` installiert oder aktualisiert jetzt zusätzlich zwei kleine tägliche Cleanup-Cronjobs auf der VPS: einen User-Job für Pip- und Rustup-Caches sowie einen Root-Job für `journalctl --vacuum-time=1d`. Die periodischen Jobs laufen still und behalten keine eigene Log-Historie.

Sensible Login-Felder wie **VPS User Password** haben einen Auge-Button, damit der für die aktuelle Session eingegebene Wert kurz eingeblendet werden kann. Der VPS Manager besitzt kein rohes CoinMarketCap-Key-Feld und keine entsprechende Reveal-Aktion.

Der Sichtbarkeitszustand bleibt auch bei Live-Updates erhalten, sodass ein geöffnetes Auge nicht sofort wieder auf versteckt zurückspringt, wenn neue WebSocket-Daten ankommen.

---

## Neuen VPS hinzufügen

1. **Add VPS** in der linken Sidebar klicken oder **Import by Hostname** aus dem Bereich **Import Host** verwenden, um das Add-Formular aus einem bereits lokal in `/etc/hosts` eingetragenen Hostnamen vorzubelegen.
2. Die Schritt-Karten oben auf der Seite durchgehen:
   - Ubuntu-VPS vorbereiten
   - Hostname in die lokale `/etc/hosts` eintragen
   - VPS-Eintrag zuerst speichern
   - **Initialize & Setup VPS** in der Add-Ansicht ausführen oder den Host später öffnen und die Ersteinrichtung auf der **Change VPS**-Seite abschließen
3. Das Formular **Step 4: Initialize & Setup your VPS** und die **Save VPS Entry**-Vorgaben ausfüllen.
4. **PB7** fuer den aktuellen Runner-Stack, **PB8 Live only (no PB7)** fuer einen sauberen minimalen PB8-Runner oder **PB7 + PB8** fuer beide Runtimes waehlen. Das Kombiprofil installiert zuerst PB7 mit PBRun und danach das PB8-Live-Profil. PB8-only ueberspringt PB7-Checkout und PB7-Virtualenv, aktiviert aber den gemeinsam genutzten PBRun-Controller, und jedes Profil mit PB8 benoetigt mindestens 3 GiB freien Speicher fuer den PB8-Build.
5. Mit **Save VPS** den Eintrag anlegen oder aktualisieren.
6. Mit **Initialize & Setup VPS** den Bootstrap-Lauf direkt aus der Add-Ansicht starten. Die UI bleibt waehrend Initialisierung und Setup im Task-Log des aktuellen Hosts. Bei PB8-only und beim Kombiprofil laufen PB8-Clone, Virtualenv-Installation und Validierung als zweiter Play desselben Setup-Jobs und erscheinen im selben Setup-Log.
7. Nach erfolgreichem Setup registriert PBGui den Host lokal als Cluster-Node-Kandidat. Klicke auf der VPS-Detailseite **Add to Cluster**, um das Onboarding abzuschliessen. PBGui oeffnet ein Fortschrittsfenster fuer Registrierung, SSH-Repair, Identity-Pruefung, Pull-/Push-Synchronisation, Materialisierung und PBRun-Start. Das Fenster kann waehrend des aktiven Onboardings nicht geschlossen werden; der API-eigene Job laeuft bei Reload oder Navigation weiter und wird beim erneuten Oeffnen der VPS-Detailseite wieder angezeigt.
8. Dieselbe Aktion **Add to Cluster** funktioniert auch fuer eine VPS, die schon vor der automatischen Kandidaten-Registrierung eingerichtet wurde. Die manuellen Schritte **Edit**, **Repair SSH**, **Probe Active Nodes** und **Join & Sync** sind fuer normales VPS-Onboarding nicht erforderlich.
9. Nach erfolgreicher Initialisierung fuer normale gespeicherte Einstellungsaenderungen **Change VPS** und **Apply VPS Changes** verwenden.

Nach einer VPS-Neuinstallation aendert sich der SSH-Host-Key. Der Preflight auf der Add-Seite blockiert die Initialisierung und zeigt **Review changed key**. Den angezeigten SHA-256-Fingerprint ueber einen vertrauenswuerdigen Kanal pruefen und den Austausch danach explizit bestaetigen. PBGui ersetzt atomar nur die kollidierenden Benutzer-`known_hosts`-Eintraege fuer diesen Hostnamen und diese IP und wiederholt anschliessend den Verbindungstest.

**Import Existing VPS** verwendet denselben Schutz. Ein geaenderter Key wird mit **Replace stored key and probe again** angezeigt, statt den Import dauerhaft zu blockieren. Nach Pruefung und Bestaetigung des exakt angezeigten Fingerprints liest PBGui den Key erneut, ersetzt atomar nur diesen Hostnamen und diese IP und wiederholt den Import-Probe. Ein Fingerprint, der sich waehrend des geoeffneten Dialogs geaendert hat, wird abgelehnt. Beim Speichern verbindet PBGui den VPS Monitor auf demselben Master neu und fordert sofort einen PBCluster-Retry an, sodass der bestaetigte Key weder im VPS Manager noch in Cluster Sync ein zweites Mal geprueft werden muss.

---

## Typische Arbeitsabläufe

### Alle Server aktualisieren
1. **Master (local)** klicken → **Update PBGui and PB7** → auf *successful* im Log warten
2. Für jeden VPS: Hostname klicken → **Update PBGui and PB7**

Der PBGui-Update-Workflow startet PBCluster für Cluster-Mode-Hosts neu und installiert/startet `pbgui-monitor-agent.service` auf VPS-Hosts. Agent-basierter Paket- und Collector-Status kann bis zum nächsten 30-Sekunden-Collector-Zyklus stale bleiben. Wenn du einen Host manuell per `git pull` aktualisierst, starte PBCluster und den Monitor-Agent dort danach mit `systemctl --user restart pbgui-pbcluster.service pbgui-monitor-agent.service` neu.

### Auf einen Feature-Branch wechseln
1. Master oder VPS-Detailansicht öffnen
2. **Branch Management** aufklappen → Zielbranch auswählen → **Switch Branch** klicken

PBGui-Branch-Wechsel nutzen dieselbe PBCluster-Service-Synchronisierung und denselben Restart wie PBGui-Updates.

### API-Keys materialisieren
- Verwende **System -> Cluster Sync**, um `api-keys.json` auf erreichbaren Nodes zu prüfen und zu materialisieren.
- CMC-Pool-Credentials sind getrennte versiegelte Generationen. Sie werden unter **Services -> PBCoinData -> Pool** verwaltet und durch Cluster Sync materialisiert; es gibt keine CMC-Keys pro VPS.
