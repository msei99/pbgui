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
| **Updates** | Ausstehende Linux-Paket-Updates |
| **PBGui / PBGui Branch / PBGui GitHub** | Installierte Version, Branch und ob sie mit dem GitHub-Origin übereinstimmt |
| **PB7 / PB7 Branch / PB7 GitHub** | PB7-Version, Branch und ob sie mit dem GitHub-Origin übereinstimmt |

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
| **Refresh** | Alle VPS-Status- und Versionsdaten über das Refresh-Icon neu laden |
| **Overview / Settings / History** | Zwischen Live-Übersicht, gemeinsamen Deploy-Einstellungen und letzter Deploy-Historie wechseln |
| **Import by Hostname** | Den manuellen Hostname-Import aus dem Sidebar-Bereich **Import Host** öffnen; der Hostname muss bereits lokal über `/etc/hosts` auflösbar sein |
| **Import Cluster Nodes** | Sichere SSH-Metadaten aus Cluster-Sync-Nodes als lokale VPS-Manager-Hosts vorab anzeigen und importieren; Secrets werden nicht importiert |

Die Übersicht nutzt jetzt die normale gemeinsame PBGui-FastAPI-Shell. Beim Wechsel zu **Master** oder zu einem konkreten **VPS** wird die linke Sidebar zu einer kontextabhängigen Aktionsliste umgeschaltet. Der Hauptbereich der Übersicht bleibt dabei auf die Tabelle fokussiert, während der Host-Import als manuelle Hostname-Aktion oder nach einem Cluster-Sync-Join als **Import Cluster Nodes** in der Sidebar verfügbar bleibt.

**Import Cluster Nodes** liest den lokal materialisierten `cluster_nodes`-State und importiert nicht-lokale Nodes mit SSH-Metadaten, unabhängig vom Cluster-Sync-Modus. Deaktivierte Cluster-Sync-Nodes können trotzdem in den VPS Manager importiert werden; disabled bedeutet nur, dass PBCluster nicht über diesen Node replizieren soll. Importiert werden nur sichere lokale VPS-Manager-Metadaten wie Hostname, SSH-Host, SSH-User, SSH-Port und Remote PBGui Dir; VPS-Passwörter, sudo-Passwörter, CoinMarketCap-Keys und Private Keys bleiben lokal und werden nicht aus Cluster Sync kopiert. Wenn lokale `/etc/hosts`-Einträge fehlen oder auf eine andere IP zeigen, zeigt die Import-Vorschau die nötigen Host-Eintragsänderungen und fragt beim Anwenden nach dem lokalen sudo-Passwort, bevor sie geschrieben werden. Das Modal fragt das VPS-User-Passwort pro importiertem Host ab; Zeilen ohne Passwort werden übersprungen, während eingegebene Passwörter einmalig genutzt werden, um Remote-Settings zu lesen, den Monitoring-SSH-Key zu installieren und das Passwort nur in der aktuellen Browser/API-Session für spätere SSH-Aktionen zu halten.

Die Seite hält eine Live-WebSocket-Verbindung für Übersicht, Fortschritt und Branch-Status offen.

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
| **Update PBGui and PB7** | Alle Komponenten aktualisieren |
| **Update PBGui** | Nur PBGui aktualisieren |
| **Update PB7** | Nur PB7 aktualisieren |
| **Update Linux** | Linux-Paketupdates ausführen (optionale Reboot-Checkbox) |
| **Reboot Master** | Den lokalen Server neu starten |
| **Install or Update rustup** | Rust-Toolchain installieren oder aktualisieren |

Der **Master**-Inhaltsbereich enthält zusätzlich:
- ein Live-Statusraster für CoinData / letzten Command
- **PBGui Branch Management** für Branch- oder Commit-Wechsel
- **PB7 Branch Management** mit optionaler Custom-Remote- / Fork-URL
- einen **Monitor**-Bereich mit Server-Metriken plus PB7-Aktivität aus laufenden Prozessen, PB7-Logs und Cluster-Sync-Zielzustand
- einen **Progress**-Bereich mit getrennten Status-Buckets; sobald eine Sidebar-Aktion einen Master-Ansible-Task startet, schaltet die Hauptfläche auf den gemeinsamen **Command Log Viewer** um, und **Home** bringt zurück zur normalen Master-Ansicht

Im Cluster-Modus synchronisieren **Update PBGui** und PBGui-Branch-Wechsel die lokale PBCluster-systemd-User-Unit und starten PBCluster neu. PBCluster ist außerdem in lokaler Service-Überwachung und Service-Control sichtbar. Ein manueller `git pull` startet PBCluster nicht neu; nutze danach `systemctl --user restart pbgui-pbcluster.service`.

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
| **Initialize** | Ersteinrichtungs-Assistent starten |
| **Delete VPS** | Diesen VPS aus PBGui entfernen |
| **Update PBGui** | PBGui auf diesem VPS aktualisieren |
| **Update PBGui and PB7** | Alle Komponenten aktualisieren |
| **Update Linux** | `apt upgrade` ausführen (optionale Reboot-Checkbox) |
| **Reboot VPS** | VPS neu starten |
| **Cleanup VPS** | Alte Pakete und Logs entfernen |
| **Update CoinData API** | Aktualisierten CoinMarketCap-API-Key übertragen |

Der **VPS**-Inhaltsbereich enthält zusätzlich:
- ein Setup-/Konfigurationsraster für Passwort, Swap, CoinMarketCap-Key und Firewall-Felder; **Apply VPS Changes** speichert Änderungen lokal und wendet geänderte Swap-, Firewall- und CoinMarketCap-Einstellungen auf der VPS an
- **PBGui Branch Management** und **PB7 Branch Management** mit demselben Switch-/Update-Workflow wie beim Master
- einen **Remote Monitor** mit Server-Metriken plus PB7-Aktivität aus laufenden Prozessen, PB7-Logs und Cluster-Sync-Zielzustand
- einen **Progress**-Bereich mit getrennten Status-Buckets für Init-, Setup- und Update-Läufe; für die vollständige Ansible-Ausgabe werden die Sidebar-Aktionsknöpfe auf den gemeinsamen **Command Log Viewer** umgeschaltet

Im Cluster-Modus synchronisieren **Update PBGui** und PBGui-Branch-Wechsel auf einer VPS die PBCluster-Service-Dateien und starten PBCluster, PBRun und PBCoinData neu, sofern diese Services konfiguriert sind. VPS-systemd-Migrationsprüfungen schließen PBCluster ein, und die Remote-Service-/Host-Log-Ansichten zeigen `PBCluster.log`. Reine VPS-Runner brauchen weiterhin kein `pbgui-api.service` und kein `PBApiServer.py`.

Die Sidebar trennt die Log-Workflows jetzt bewusst von der normalen Host-Ansicht:
- Utility-Aktionen wie **Task Logs**, **Host Logs**, **Change VPS**, **Initialize** oder **Delete VPS** bleiben oberhalb eines Trenners, während die ausführbaren Ansible-Playbook-Knöpfe darunter gruppiert sind
- **Task Logs** öffnet einen eigenen gefilterten Viewer für alle gespeicherten Playbook-Logs des ausgewählten VPS inklusive rotierter Historie
- Aktionen wie **Initialize**, **Setup VPS**, **Update PBGui**, **Update PBGui and PB7**, **Update Linux**, **Cleanup VPS** oder **Update CoinData API** schalten die Hauptfläche automatisch auf den gemeinsamen **Command Log Viewer** um
- **Host Logs** öffnet einen eigenen **Host Log Viewer** für Service-Logs, laufende Bot-Logs und dateibasierte Ziele wie `PBCluster.log`
- **Back** bringt von Branch-, Setup- oder Log-Screens zurück in die normale VPS-Detailansicht, ohne den gewählten Host-Kontext zu verlieren
- jeder aufrufbare VPS-Manager-Task bekommt jetzt sein eigenes aktuelles Log plus rotierte Historie im gemeinsamen Viewer; standardmäßig bleiben 10 Historien-Dateien erhalten, konfigurierbar über `[vps_manager] task_log_history` in `pbgui.ini`
- wenn die Ansible-Ausgabe bereits Terminal-ANSI-Farben enthält, übernimmt der gemeinsame Viewer diese Farben jetzt auch im Browser, statt nur über Textmuster zu raten
- Ansible-Task-Logs mit verklebten Ergebnis-Markern oder escaped Payload-Steuerzeichen wie `\n` / `\r` werden jetzt im gemeinsamen Viewer in lesbare getrennte Anzeigezeilen aufgelöst
- strukturierte Ansible-Ergebnis-Payloads mit JSON-Inhalt werden jetzt als mehrzeilige Blöcke hübsch formatiert, damit verschachtelte Metadaten wie `stat` direkt im gemeinsamen Viewer lesbar sind

Die Status-Kacheln oberhalb des Setup-Rasters sind jetzt direkte Operator-Hinweise:
- **Update Ready** wird sofort grün, sobald lokal ein VPS-User-Passwort eingetragen ist, und zeigt gleichzeitig die Anzahl ausstehender Linux-Updates.
- **CoinData Ready** zeigt die verbleibenden CoinMarketCap-Credits, sobald der Monitor diesen Wert meldet.
- Ausstehende Linux-Updates und Reboot-Hinweise werden zusätzlich über eine Live-SSH-Paketstatus-Abfrage aktualisiert.
- Die Detailseite enthält außerdem wieder eine einzeilige Zusammenfassungstabelle plus einen Remote-Server-Ressourcenblock ähnlich zur früheren Serveransicht.

`Cleanup VPS` installiert oder aktualisiert jetzt zusätzlich zwei kleine tägliche Cleanup-Cronjobs auf der VPS: einen User-Job für Pip- und Rustup-Caches sowie einen Root-Job für `journalctl --vacuum-time=1d`. Die periodischen Jobs laufen still und behalten keine eigene Log-Historie.

Sensible Felder wie **VPS User Password** und **CoinMarketCap API Key** haben einen Auge-Button, damit der gespeicherte Wert beim Bearbeiten kurz eingeblendet werden kann.

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
4. Mit **Save VPS** den Eintrag anlegen oder aktualisieren.
5. Mit **Initialize & Setup VPS** den Bootstrap-Lauf direkt aus der Add-Ansicht starten.
6. Nach erfolgreicher Initialisierung für normale gespeicherte Einstellungsänderungen **Change VPS** und **Apply VPS Changes** verwenden.

---

## Typische Arbeitsabläufe

### Alle Server aktualisieren
1. **Master (local)** klicken → **Update PBGui and PB7** → auf *successful* im Log warten
2. Für jeden VPS: Hostname klicken → **Update PBGui and PB7**

Der PBGui-Update-Workflow startet PBCluster für Cluster-Mode-Hosts neu. Wenn du einen Host manuell per `git pull` aktualisierst, starte PBCluster dort danach mit `systemctl --user restart pbgui-pbcluster.service` neu.

### Auf einen Feature-Branch wechseln
1. Master oder VPS-Detailansicht öffnen
2. **Branch Management** aufklappen → Zielbranch auswählen → **Switch Branch** klicken

PBGui-Branch-Wechsel nutzen dieselbe PBCluster-Service-Synchronisierung und denselben Restart wie PBGui-Updates.

### API-Keys materialisieren
- Verwende **System -> Cluster Sync**, um `api-keys.json` auf erreichbaren Nodes zu prüfen und zu materialisieren.
- Pro VPS: VPS-Detailansicht öffnen → **Update CoinData API** aktualisiert nur den CoinMarketCap-Key für Market-Data-Filter.
