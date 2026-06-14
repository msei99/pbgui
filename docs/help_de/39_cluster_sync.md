# Cluster Sync

Cluster Sync hält mehrere PBGui-Master und VPS-Runner auf demselben gewünschten V7- und API-Key-Stand, ohne einen externen Storage-Dienst zu verwenden.

Nutze Cluster Sync, wenn du mehr als einen Master betreibst, Bots auf mehrere VPS verteilst oder VPS-Nodes auch dann sauber neu starten sollen, wenn gerade kein Master online ist.

---

## Was ein Cluster ist

Ein Cluster ist eine Gruppe von PBGui-Installationen, die einen replizierten Cluster-Zustand teilen.

| Begriff | Bedeutung |
|---|---|
| **Cluster** | Die gesamte PBGui-Sync-Gruppe. Sie hat eine stabile `cluster_id`. |
| **Node** | Eine PBGui-Installation. Ein Node kann Master oder VPS-Runner sein. |
| **Master** | PBGui-Server zum Verwalten von Configs, API-Keys, VPS-Nodes und Sync-Zustand. |
| **VPS-Runner** | Server, der PB7-Bots ausführen und eine lokale Kopie des Cluster-Zustands speichern kann. |
| **Desired State** | Die Cluster-Entscheidung, welcher Bot existieren, wo er laufen und ob er laufen soll. |
| **Operation Log** | Append-only Historie aller Cluster-Änderungen. PBGui baut daraus den Desired State neu auf. |

Jeder Node hat eine stabile `node_id`. Diese ID ändert sich nicht, wenn Hostname, IP-Adresse, SSH-Port, VPS-Manager-Name oder `pbname` geändert werden.

---

## Was Cluster Sync abdeckt

Cluster Sync deckt ab:

- V7-Bot-Configs inklusive Coin-Override-JSON-Dateien.
- V7 Desired State: Start, Stop, Move, Delete und Tombstone.
- Explizite V7-Forced-Mode-Config-Änderungen wie Panic, Graceful Stop und Take Profit Only.
- API-Key-Verteilung für `api-keys.json`.
- Lokale State-Replikas auf Mastern und VPS-Nodes.
- Eingeschränkte Cluster-Sync-SSH-Keys für Node-zu-Node-Replikation.

Cluster Sync deckt nicht ab:

- DB-Tools-Row-Sync oder Datenbank-Dateikopien.
- Dashboard- und Template-Sync.
- Automatische Panic-Entscheidungen, automatisches Forced Selling oder Duplicate-Bot-Liquidation.
- Automatische Failover-Moves von selbst.

---

## Wie Änderungen durch den Cluster laufen

PBGui behandelt fehlende Dateien nicht als Delete. Jede wichtige Änderung wird als explizite Operation geschrieben.

Beispiele:

- Speichern oder Aktivieren einer V7-Config schreibt eine Upsert-Operation.
- Setzen von Panic, Graceful Stop oder Take Profit Only über Dashboard oder Run Config schreibt und synchronisiert eine Config-Operation.
- Verschieben eines Bots schreibt eine Move-Operation.
- Stoppen eines Bots schreibt eine Stop-Operation.
- Löschen eines Bots schreibt eine Delete- oder Tombstone-Operation.
- Aktualisieren von `api-keys.json` schreibt eine API-Key-Operation.

Jeder Node vergleicht seine bekannten Operation-Counter mit einem anderen Node. Fehlende Operationen und benötigte Blobs werden übertragen, danach baut der empfangende Node seinen lokalen Desired State neu auf.

Dadurch ist Sync wiederholbar und sicher erneut ausführbar.

---

## V7-Bots und Desired State

Für jede V7-Instanz speichert der Desired State:

- die aktuelle Config-Version
- ob der Bot laufen oder gestoppt sein soll
- den zugewiesenen Node, der den Bot ausführen darf
- einen Manifest-Hash aller syncbaren Config-JSON-Dateien
- Conflict-Status
- Tombstone-Status für gelöschte Bots

PBRun prüft den Desired State vor jedem Bot-Start.

PBRun startet einen Bot nur, wenn:

- der Bot im Desired State existiert
- er nicht tombstoned ist
- er nicht conflicted ist
- der Desired State `running` sagt
- der zugewiesene Node dem lokalen Node entspricht
- lokale Config-Version und Manifest-Hash zum Desired State passen

Wenn eine Prüfung fehlschlägt, startet PBRun den Bot nicht und PBGui zeigt den Blockiergrund an.

Panic, Graceful Stop und Take Profit Only sind explizite PB7-Config-Änderungen. Cluster Sync verteilt sie wie jedes andere V7-Config-Update. Sie sind keine direkten Exchange-Orders und keine automatischen Panic-Entscheidungen von Cluster Sync.

---

## Bot-Moves und Deletes

Moves sind explizit. Wenn ein Bot von einer VPS auf eine andere verschoben wird, darf die alte VPS ihn nicht mehr starten, nachdem sie die Move-Operation gelernt hat.

Deletes sind explizit. PBGui löscht eine lokale V7-Instanz nie nur deshalb, weil sie in einer Remote-Datei oder Remote-Statusliste fehlt.

Tombstones verhindern, dass alte Configs von stale Nodes wieder eingebracht werden.

---

## Offline-Nodes und Reboots

Eine VPS kann neu starten, auch wenn kein Master online ist.

Boot-Verhalten:

1. Die VPS startet PBGui/PBRun.
2. Cluster Sync versucht kurz, bekannte Peer-Nodes zu erreichen.
3. Wenn Peers erreichbar sind, zieht die VPS fehlende Operationen und baut Desired State neu auf.
4. Wenn kein Peer erreichbar ist, nutzt die VPS ihren lokalen Desired State.
5. PBRun startet nur Bots, die dieser VPS zugewiesen sind.

Stale lokaler State ist nur eine Warnung. PBRun blockiert den Start nicht nur deshalb, weil der lokale Cluster-Zustand alt ist.

Das ist Absicht: Ein Host kann nachts für ein paar Stunden offline sein. Solange es kein automatisches Failover gibt, soll PBGui normale Reboot-Recovery nicht nur wegen stale State verhindern.

---

## Conflicts

Ein Conflict kann entstehen, wenn zwei Master denselben Bot aus derselben Parent-Version ändern, bevor sie miteinander synchronisiert wurden.

Wenn PBGui einen Conflict erkennt:

- wird die Instanz als conflicted markiert
- PBRun darf sie nicht automatisch starten
- die Cluster-Seite zeigt die konkurrierenden Operationen
- der Benutzer muss die gewinnende Version auswählen oder erstellen
- die Auflösung schreibt eine neue Operation

PBGui nutzt für V7-Instance-Conflicts kein blindes Last-Write-Wins.

---

## API-Keys

Cluster Sync verfolgt auch Änderungen an `api-keys.json`.

Der Desired State speichert nur Metadaten wie Serial und Payload-Hash. Der API-Key-Inhalt wird als eingeschränkte Secret-Daten gespeichert und darf nicht in Logs oder im normalen Desired-State-JSON auftauchen.

Die Installation von API-Keys auf einem Node nutzt dieselben Sicherheitsstufen wie API Sync:

- Backup erstellen
- neue Datei schreiben
- Payload verifizieren
- nur Bots neu starten, deren Credentials sich geändert haben

---

## Cluster-Sync-SSH-Keys

Cluster Sync nutzt dedizierte eingeschränkte SSH-Keys statt normaler Admin-SSH-Keys für reguläre Replikation.

Admin-SSH-Zugangsdaten werden nur für Bootstrap, Key-Installation und Recovery verwendet.

Cluster-Sync-Keys sind so eingeschränkt, dass sie keine normale Shell öffnen und keine beliebigen Kommandos ausführen können. Sie werden mit einem OpenSSH-Forced-Command installiert, der nur Cluster-Sync-Aktionen erlaubt, zum Beispiel State Vector lesen, Operationen senden, Blobs senden und Desired State neu bauen.

Das begrenzt den Schaden, falls ein Cluster-Sync-Key geleakt wird: Der Key soll keinen interaktiven Login, kein Port Forwarding, kein Agent Forwarding und kein uneingeschränktes SFTP erlauben.

---

## VPS-zu-VPS-Firewall-Regeln

Cluster Sync verwaltet VPS-zu-VPS-SSH-Firewall-Regeln für Peer-Sync automatisch.

PBGui fügt Allow-Regeln für aktivierte Peer-VPS-Nodes hinzu, die Cluster-State austauschen müssen. Alte von PBGui verwaltete Peer-Regeln werden erst entfernt, wenn die neue Erreichbarkeit bestätigt wurde.

PBGui darf keine SSH-Firewall-Regeln entfernen, die nicht für Cluster Sync erstellt wurden.

---

## Cluster-Seite

Die dedizierte **Cluster Sync**-Seite ist der zentrale Ort zur Überwachung von Cluster Sync.

Die erste local-only Version zeigt:

- Cluster-Identität und lokale Node-Identität
- alle materialisierten Nodes und ihre Rollen
- V7 Desired State
- Conflict- und Tombstone-Status
- API-Key-Metadaten, falls vorhanden
- aktuelle lokale Operation-Log-Einträge
- eine Bootstrap-Preview/Apply-Aktion für bekannte VPS-Nodes und bestehende lokale V7-Configs
- read-only Remote-Hello-Probe-Status für bekannte Cluster-Nodes
- eine explizite Join-Aktion für Nodes, die erreichbar sind, aber noch keine Cluster-Identität haben
- eine read-only Preview-Aktion für gejointe Nodes, die Remote-State vor Replikation vergleicht

Bootstrap schreibt explizite lokale `ADD_NODE`-Operationen für bekannte VPS-Manager-Hosts und `UPSERT_CONFIG`-Operationen für lokale Configs. Wenn VPS-Monitor-Metadaten verfügbar sind, übernimmt Bootstrap, ob ein bekannter Host Master oder VPS-Runner ist. Fehlende Dateien oder fehlende VPS-Einträge werden nie als Delete interpretiert und Tombstones werden dadurch nicht entfernt. Die Probe-Spalte führt, wenn verfügbar, nur ein read-only restricted `hello` aus; sie installiert keine Keys, schreibt keine Remote-Dateien, startet oder stoppt keine Bots und deployed nichts.

Wenn ein Node **No Identity** zeigt, schreibt die **Join**-Aktion nur `cluster_id`, `node_id` und `node_identity.json` unter dem remote PBGui-Verzeichnis `data/cluster`. Eine abweichende bestehende Identität wird nicht überschrieben. Join synchronisiert keine Configs, installiert keine restricted Keys, startet oder stoppt keine Bots, deployed keine Dateien und verändert keinen lokalen Desired State. Last-Seen-Status, Node-zu-Node-Sync-Status und Conflict-Resolution-Aktionen folgen in späteren Phasen.

Wenn ein gejointer Node **OK** zeigt, liest die **Preview**-Aktion den Remote-State-Vector und Desired State. Sie vergleicht Actor-Sequenzen, V7-Instance-Metadaten, Tombstones und API-Key-Metadaten mit lokalem State. Zusätzlich berechnet sie, welche lokalen Operationen remote fehlen, welche Remote-Operationsbereiche lokal fehlen und welche Hash-Referenzen eine spätere Write-Phase brauchen würde. Preview ist read-only; es kopiert keine Operationen, Blobs oder Configs.

Im Preview-Fenster ist **Push Missing Ops + Rebuild** eine explizite Remote-Write-Aktion. Sie ist nur verfügbar, wenn dem lokalen Node keine Remote-Operationen fehlen. Die Aktion startet einen Backend-Push-Job, sendet aktuelle V7-Config-Blobs, sendet die lokalen Oplog-Einträge, die dem Remote-State-Vector fehlen, gesammelt in einem Bulk-Upload, meldet lokalen Fortschritt während der Job läuft und führt danach remote `rebuild` aus. Die Fortschrittsanzeige teilt oder verlangsamt den Remote-Sync nicht. Wenn der Remote-Wrapper noch zu alt für Bulk ist, fällt PBGui auf langsamere Einzel-Uploads zurück. Sie kopiert noch keine API-Key-Payloads oder Secret-Blobs, deployed keine Dateien und startet oder stoppt keine Bots.

---

## Was tun, wenn etwas nicht stimmt

Wenn ein Node offline ist:

- SSH-Erreichbarkeit über Cluster- oder VPS-Manager-Seite prüfen.
- Prüfen, ob der Node für Sync aktiviert ist.
- Host, Port, User und Host-Key-Metadaten prüfen.

Wenn ein Bot nicht startet:

- Cluster-Seite öffnen und Blocked-Start-Details prüfen.
- Prüfen, ob der Bot diesem Node zugewiesen ist.
- Auf Conflict- oder Tombstone-State prüfen.
- Prüfen, ob die lokale Config-Version zum Desired State passt.

Wenn ein Conflict erscheint:

- Dateien nicht manuell zwischen Nodes kopieren.
- Konkurrierende Operationen auf der Cluster-Seite prüfen.
- Die korrekte gewinnende Config auswählen oder erstellen.
- PBGui die Resolution-Operation schreiben lassen.

Wenn eine Foreign-Cluster-Warnung erscheint:

- Sync nicht erzwingen.
- Prüfen, ob der Node wirklich zu diesem PBGui-Cluster gehört.
- Remote-Cluster-Identität nur joinen oder zurücksetzen, wenn sicher ist, dass es der richtige Node ist.

---

## Sicherheitsregeln

- Lokale Bot-Verzeichnisse nicht löschen, um einen Delete auszulösen. PBGui verwenden, damit eine Delete-Operation geschrieben wird.
- Kopierte `data/cluster/node_id` Dateien nicht auf einer anderen Installation wiederverwenden.
- `desired_state.json` nicht manuell bearbeiten; die Datei wird aus dem Operation Log generiert.
- Admin-SSH-Zugang für Recovery behalten, auch wenn Cluster-Replikation eingeschränkte Keys verwendet.
