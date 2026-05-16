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
| **Online** | ✅ erreichbar / ❌ offline |
| **Role** | 🧠 Master / 💻 VPS |
| **Start** | Letzter Boot-Zeitpunkt |
| **Reboot** | ✅ kein Reboot nötig / ❌ Reboot erforderlich |
| **Updates** | Ausstehende Linux-Paket-Updates |
| **PBGui / PBGui Branch / PBGui github** | Installierte Version, Branch und ob sie mit dem GitHub-Origin übereinstimmt |
| **PB7 / PB7 Branch / PB7 github** | PB7-Version, Branch und ob sie mit dem GitHub-Origin übereinstimmt |
| **API Sync** | ✅ API-Keys synchron mit Master / ❌ nicht synchron |

Linke Sidebar:

| Schaltfläche | Aktion |
|--------|--------|
| **Add VPS** | Formular zum Hinzufügen / Initialisieren öffnen |
| **Master** | Lokale Master-Verwaltung öffnen |
| **Refresh** | Alle VPS-Status- und Versionsdaten über das Refresh-Icon neu laden |
| **Managed VPS** Karten | Per-VPS-Verwaltungsansicht öffnen; jede Karte zeigt nur Hostnamen und aktuellen Online-/Offline-Status |
| **API Sync** | Zeigt den gemeinsamen API-Sync-Status (`API Sync`, `API X/Y out of sync`, `API all in sync`) an und pusht API-Zugangsdaten bei Klick an verbundene VPS |
| **Import by Hostname** | Den manuellen Hostname-Import aus dem Sidebar-Bereich **Import Host** öffnen; der Hostname muss bereits lokal über `/etc/hosts` auflösbar sein |

Die Übersicht nutzt jetzt die normale gemeinsame PBGui-FastAPI-Shell. Beim Wechsel zu **Master** oder zu einem konkreten **VPS** wird die linke Sidebar wie auf der alten Seite zu einer kontextabhängigen Aktionsliste umgeschaltet. Der Hauptbereich der Übersicht bleibt dabei auf die Tabelle fokussiert, während der Host-Import als manuelle Hostname-Aktion in der Sidebar verfügbar bleibt.

Die Seite hält eine Live-WebSocket-Verbindung für Übersicht, Fortschritt, Branch-Status und API-Sync-Fortschritt offen.

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
| **Install or Update rclone** | rclone installieren oder aktualisieren |

Der **Master**-Inhaltsbereich enthält zusätzlich:
- ein Live-Statusraster für PBRemote / CoinData / letzten Command
- **PBGui Branch Management** für Branch- oder Commit-Wechsel
- **PB7 Branch Management** mit optionaler Custom-Remote- / Fork-URL
- einen **Monitor**-Bereich mit Server-Metriken plus PB7-Aktivität; falls Live-Monitor-Zeilen fehlen, listet die Seite die laufenden PB7-Botnamen weiterhin aus `status_v7.json`
- einen **Progress**-Bereich mit getrennten Status-Buckets; sobald eine Sidebar-Aktion einen Master-Ansible-Task startet, schaltet die Hauptfläche auf den gemeinsamen **Command Log Viewer** um, und **Home** bringt zurück zur normalen Master-Ansicht

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
| **Setup VPS** | Die VPS-Setup-/Konfigurationsansicht öffnen |
| **PBGui Branch** | Die PBGui-Branch-Verwaltung öffnen |
| **PB7 Branch** | Die PB7-Branch-Verwaltung öffnen |
| **Initialize** | Ersteinrichtungs-Assistent starten |
| **Delete VPS** | Diesen VPS aus PBGui entfernen |
| **Update PBGui** | PBGui auf diesem VPS aktualisieren |
| **Update PBGui and PB7** | Alle Komponenten aktualisieren |
| **Update Linux** | `apt upgrade` ausführen (optionale Reboot-Checkbox) |
| **Reboot VPS** | VPS neu starten |
| **Cleanup VPS** | Alte Pakete und Logs entfernen |
| **Resize Swap** | Swap-Datei auf konfigurierte Größe anpassen |
| **Update Firewall Settings** | ufw-Firewall-Regeln anwenden |
| **Update CoinData API** | Aktualisierten CoinMarketCap-API-Key übertragen |

Der **VPS**-Inhaltsbereich enthält zusätzlich:
- ein Setup-/Konfigurationsraster für Passwort, Swap, Bucket, CoinMarketCap-Key und Firewall-Felder
- **PBGui Branch Management** und **PB7 Branch Management** mit demselben Switch-/Update-Workflow wie beim Master
- einen **Remote Monitor** mit Server-Metriken plus PB7-Aktivität; falls Live-Monitor-Zeilen fehlen, listet die Seite die laufenden PB7-Botnamen weiterhin aus `status_v7.json`
- einen **Progress**-Bereich mit getrennten Status-Buckets für Init-, Setup- und Update-Läufe; für die vollständige Ansible-Ausgabe werden die Sidebar-Aktionsknöpfe auf den gemeinsamen **Command Log Viewer** umgeschaltet

Die Sidebar trennt die Log-Workflows jetzt bewusst von der normalen Host-Ansicht:
- Utility-Aktionen wie **Task Logs**, **Host Logs**, **Setup VPS**, **Initialize** oder **Delete VPS** bleiben oberhalb eines Trenners, während die ausführbaren Ansible-Playbook-Knöpfe darunter gruppiert sind
- **Task Logs** öffnet einen eigenen gefilterten Viewer für alle gespeicherten Playbook-Logs des ausgewählten VPS inklusive rotierter Historie
- Aktionen wie **Initialize**, **Setup VPS**, **Update PBGui**, **Update PBGui and PB7**, **Update Linux**, **Cleanup VPS** oder **Update CoinData API** schalten die Hauptfläche automatisch auf den gemeinsamen **Command Log Viewer** um
- **Host Logs** öffnet einen eigenen **Host Log Viewer** für Service-Logs, laufende Bot-Logs und dateibasierte Ziele wie `sync.log`
- **Back** bringt von Branch-, Setup- oder Log-Screens zurück in die normale VPS-Detailansicht, ohne den gewählten Host-Kontext zu verlieren
- jeder aufrufbare VPS-Manager-Task bekommt jetzt sein eigenes aktuelles Log plus rotierte Historie im gemeinsamen Viewer; standardmäßig bleiben 10 Historien-Dateien erhalten, konfigurierbar über `[vps_manager] task_log_history` in `pbgui.ini`
- wenn die Ansible-Ausgabe bereits Terminal-ANSI-Farben enthält, übernimmt der gemeinsame Viewer diese Farben jetzt auch im Browser, statt nur über Textmuster zu raten
- Ansible-Task-Logs mit verklebten Ergebnis-Markern oder escaped Payload-Steuerzeichen wie `\n` / `\r` werden jetzt im gemeinsamen Viewer in lesbare getrennte Anzeigezeilen aufgelöst
- strukturierte Ansible-Ergebnis-Payloads mit JSON-Inhalt werden jetzt als mehrzeilige Blöcke hübsch formatiert, damit verschachtelte Metadaten wie `stat` direkt im gemeinsamen Viewer lesbar sind

Die Status-Kacheln oberhalb des Setup-Rasters sind jetzt direkte Operator-Hinweise:
- **Update Ready** wird sofort grün, sobald lokal ein VPS-User-Passwort eingetragen ist, und zeigt gleichzeitig die Anzahl ausstehender Linux-Updates.
- **CoinData Ready** zeigt die verbleibenden CoinMarketCap-Credits, sobald dieser Wert über PBRemote verfügbar ist.
- Ausstehende Linux-Updates und Reboot-Hinweise werden zusätzlich über eine Live-SSH-Paketstatus-Abfrage aktualisiert, sodass die Karten nicht mehr auf den langsameren stündlichen `PBRemote`-Alive-Refresh warten müssen.
- Die Detailseite enthält außerdem wieder eine einzeilige Zusammenfassungstabelle plus einen Remote-Server-Ressourcenblock ähnlich zur alten Streamlit-Ansicht.

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
   - **Initialize & Setup VPS** in der Add-Ansicht ausführen oder den Host später öffnen und **Setup VPS** in der Detailseite abschließen
3. Das Formular **Step 4: Initialize & Setup your VPS** und die **Save VPS Entry**-Vorgaben ausfüllen.
4. Mit **Save VPS** den Eintrag anlegen oder aktualisieren.
5. Mit **Initialize & Setup VPS** den Bootstrap-Lauf direkt aus der Add-Ansicht starten.
6. Nach erfolgreicher Initialisierung die VPS-Detailseite öffnen und **Setup VPS** klicken, wenn das gespeicherte Setup erneut ausgeführt werden soll.

---

## Typische Arbeitsabläufe

### Alle Server aktualisieren
1. **Master (local)** klicken → **Update PBGui and PB7** → auf *successful* im Log warten
2. Für jeden VPS: Hostname klicken → **Update PBGui and PB7**

### Auf einen Feature-Branch wechseln
1. Master oder VPS-Detailansicht öffnen
2. **Branch Management** aufklappen → Zielbranch auswählen → **Switch Branch** klicken

### API-Key-Synchronisation prüfen
- Spalte **API Sync** in der Übersicht: ❌ bedeutet, dass VPS-Keys veraltet sind
- Am unteren Ende der Sidebar zeigt die gemeinsame **API Sync** Schaltfläche je nach verbundenen VPS **API X/Y out of sync** oder **API all in sync** an - ein Klick pusht die aktualisierten Keys über den neuen SSH-API-Sync-Flow
- Pro VPS: VPS-Detailansicht öffnen → **Update CoinData API**
