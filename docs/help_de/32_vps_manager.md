# VPS Manager

Die **VPS Manager** Seite erlaubt das Hinzufügen, Konfigurieren und Warten von Remote-VPS-Servern, auf denen Passivbot-Instanzen laufen.
Jeder VPS wird über Ansible-Playbooks verwaltet, die vom Master (lokal) ausgeführt werden.

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
| **PB6 / PB6 github** | PB6-Version vs. GitHub-Origin |
| **PB7 / PB7 Branch / PB7 github** | PB7-Version, Branch und ob sie mit dem GitHub-Origin übereinstimmt |
| **API Sync** | ✅ API-Keys synchron mit Master / ❌ nicht synchron |

Sidebar:

| Schaltfläche | Aktion |
|--------|--------|
| `:material/refresh:` | Alle VPS-Status und Versionsdaten neu laden |
| `:material/add_box:` | Neuen VPS hinzufügen |
| **Master (local)** Schaltfläche | Master-Verwaltungsansicht öffnen |
| **VPS-Hostname** Schaltflächen | Per-VPS-Verwaltungsansicht öffnen |

Am Ende der Sidebar zeigt eine **API-Sync-Status-Schaltfläche** den aktuellen Stand:
- **🟢 API in sync** (grün, deaktiviert) — alle Online-VPS-Server haben die aktuellen API-Keys
- **🔴 API not in sync** (rot, klickbar) — ein oder mehrere Server sind veraltet; klicken, um Keys auf alle zu übertragen; ein Live-Zähler zeigt verbleibende Server an (Timeout: 180 s)

---

## Master-Verwaltung

Klick auf den farbigen **Master (local)** Button in der Sidebar öffnet die Verwaltung des lokalen Servers.

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| `:material/refresh:` | Status neu laden |
| `:material/home:` | Zurück zur Übersicht |
| **Update PBGui, PB6 and PB7** | Alle Komponenten aktualisieren |
| **Update PBGui** | Nur PBGui aktualisieren |
| **Update pb6 and pb7** | Nur PB6/PB7 aktualisieren |
| **Install rustup** | Rust-Toolchain installieren (benötigt sudo-Passwort) |
| **Install rclone** | rclone installieren (benötigt sudo-Passwort) |
| **Update PB7 venv** | PB7 Python-3.12-Venv neu erstellen (benötigt sudo-Passwort) |
| **Install PBGui venv** | PBGui Python-3.12-Venv neu erstellen (benötigt sudo-Passwort) |

Die **Branch-Management**-Expander erlauben das Wechseln von PBGui oder PB7 auf einen anderen Branch oder Commit direkt aus der Benutzeroberfläche. Fork-/Custom-Remote-Unterstützung ist über den optionalen *Custom remote* Sub-Expander verfügbar.

---

## VPS-Verwaltung

Klick auf den Hostname-Button in der Sidebar öffnet die Detailansicht des jeweiligen VPS.

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| `:material/refresh:` | VPS-Status neu laden |
| `:material/home:` | Zurück zur Übersicht |
| `:material/delete:` | Diesen VPS aus PBGui entfernen |
| **Read settings from VPS** | Aktuelle Konfiguration per SSH vom VPS abrufen |
| **Initialize** | Ersteinrichtungs-Assistent starten |
| **Update PBGui** | PBGui auf diesem VPS aktualisieren |
| **Update PBGui, PB6 and PB7** | Alle Komponenten aktualisieren |
| **Update PB7 venv** | PB7 Python-3.12-Venv neu erstellen |
| **Update PBGui venv** | PBGui Python-3.12-Venv neu erstellen |
| **Update Linux** | `apt upgrade` ausführen (optionale Reboot-Checkbox) |
| **Reboot VPS** | VPS neu starten |
| **Cleanup VPS** | Alte Pakete und Logs entfernen |
| **Resize Swap** | Swap-Datei auf konfigurierte Größe anpassen |
| **Update Firewall Settings** | ufw-Firewall-Regeln anwenden |
| **Update CoinData API** | Aktualisierten CoinMarketCap-API-Key übertragen |

Der Expander **VPS Setup Settings** enthält die Verbindungsparameter (Passwort, Swap, rclone-Bucket, CoinMarketCap-Key, Firewall).
Nach dem Ausfüllen aller Parameter **Setup VPS** ausführen, um die Ersteinrichtung abzuschließen.

Die **Branch-Management**-Expander (PBGui und PB7) funktionieren wie beim Master — Branch oder Commit per Ansible wechseln ohne manuelles SSH.

Der **Log-Viewer** am Seitenende erlaubt das Abrufen und Anzeigen beliebiger Log-Dateien vom VPS (PBRun, PBRemote, PBCoinData usw.).

---

## Neuen VPS hinzufügen

1. `:material/add_box:` in der Sidebar klicken.
2. Dem 4-Schritte-Assistenten folgen:
   - **Schritt 1** – VPS beschaffen (Hosting-Empfehlungen)
   - **Schritt 2** – Ubuntu 24.04 auf dem VPS installieren
   - **Schritt 3** – VPS-IP und Hostname in die lokale `/etc/hosts` eintragen
   - **Schritt 4** – Zugangsdaten eingeben und **Init VPS** klicken
3. Nach erfolgreicher Initialisierung die VPS-Detailansicht öffnen und **Setup VPS** klicken.

---

## Typische Arbeitsabläufe

### Alle Server aktualisieren
1. **Master (local)** klicken → **Update PBGui, PB6 and PB7** → auf *successful* im Log warten
2. Für jeden VPS: Hostname klicken → **Update PBGui, PB6 and PB7**

### Auf einen Feature-Branch wechseln
1. Master oder VPS-Detailansicht öffnen
2. **Branch Management** aufklappen → Zielbranch auswählen → **Switch Branch** klicken

### API-Key-Synchronisation prüfen
- Spalte **API Sync** in der Übersicht: ❌ bedeutet, dass VPS-Keys veraltet sind
- Sidebar-Ende: erscheint eine **🔴 API not in sync** Schaltfläche, klicken um Keys mit Live-Fortschrittszähler zu synchronisieren
- Pro VPS: VPS-Detailansicht öffnen → **Update CoinData API**
