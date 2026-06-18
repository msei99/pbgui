# Cluster Sync

Cluster Sync hält mehrere PBGui-Master und VPS-Runner auf demselben gewünschten V7- und API-Key-Stand, ohne einen externen Storage-Dienst zu verwenden.

Nutze Cluster Sync, wenn du mehr als einen Master betreibst, Bots auf mehrere VPS verteilst oder VPS-Nodes auch dann sauber neu starten sollen, wenn gerade kein Master online ist.

Wenn du ein bestehendes PBRemote/API-Sync/V7-SSH-Sync-Setup aktualisierst, lies vor dem Join produktiver VPS-Nodes die [Cluster-Mode-Migration](40_cluster_migration.md).

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

Die Installation von API-Keys auf einem Node nutzt die Sicherheitsstufen der Cluster-Sync-Materialisierung:

- Backup auf Master-Nodes erstellen, wenn eine bestehende Datei abweicht
- lokale Backups auf VPS-Runnern ueberspringen
- neue Datei schreiben
- Payload verifizieren
- keine Bots neu starten und keine weiteren Dateien deployen

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
- eine explizite Join-Existing-Cluster-Aktion für einen zweiten Master, der einen bestehenden Master outbound erreichen kann
- eine Bootstrap-Preview/Apply-Aktion für bekannte VPS-Nodes und bestehende lokale V7-Configs
- read-only Remote-Hello-Probe-Status für bekannte Cluster-Nodes
- eine explizite Join-&-Sync-Aktion für erreichbare Nodes ohne Cluster-Identität
- eine read-only Preview-Aktion für gejointe Nodes, die Remote-State für Diagnose oder Retry vergleicht

Bootstrap schreibt explizite lokale `ADD_NODE`-Operationen für bekannte VPS-Manager-Hosts und `UPSERT_CONFIG`-Operationen für lokale Configs. Wenn VPS-Monitor-Metadaten verfügbar sind, übernimmt Bootstrap, ob ein bekannter Host Master oder VPS-Runner ist. Fehlende Dateien oder fehlende VPS-Einträge werden nie als Delete interpretiert und Tombstones werden dadurch nicht entfernt. Die Probe-Spalte führt, wenn verfügbar, nur ein read-only restricted `hello` aus; sie installiert keine Keys, schreibt keine Remote-Dateien, startet oder stoppt keine Bots und deployed nichts.

Wenn ein Node **No Identity** zeigt, schreibt **Join & Sync** die Remote-Cluster-Identität und überschreibt keine abweichende bestehende Identität. Danach pusht PBGui fehlende lokale Operationen, baut den Remote-Cluster-State neu, materialisiert zugewiesene V7-Configs und API-Keys und startet PBRun wieder, wenn der Remote-State aktuell ist. Auf VPS-Runnern stoppt Join vorher PBRun, damit laufende Bots während der Übergangsphase nicht bewertet werden; passivbot-Prozesse bleiben unangetastet. Auf Master-Nodes wird PBRun nicht gestoppt oder gestartet.

Nutze **Join Existing Cluster** auf einem zweiten Master, der vom primären Master nicht inbound erreichbar ist, aber selbst per SSH zum primären Master verbinden kann. Tue das vor Bootstrap, wenn der Master einem bestehenden Cluster beitreten soll. Die Aktion nutzt den VPS-Monitor-SSH-Pool, liest den Upstream-Master, übernimmt dessen `cluster_id` automatisch bei leerem lokalen Oplog, zieht Upstream-Operationen und Blobs, registriert den lokalen Master als `outbound_only`, installiert den lokalen Cluster-SSH-Key auf dem Upstream-Master und pusht die Registrierungsoperationen zurück. Wenn diese lokale Installation bereits Cluster-Operationen für einen anderen Cluster hat, verweigert Self-Join das Überschreiben, außer die Recovery-Option ist aktiviert. Recovery archiviert den bisherigen lokalen Cluster-State unter `data/cluster/archives/`, bevor er durch den Upstream-Cluster-State ersetzt wird.

PBCluster-SSH-Zugriff ist technischer Setup-Status. Beim normalen PBGui-Setup/Update auf einem VPS erzeugt PBGui jetzt automatisch einen dedizierten lokalen PBCluster-SSH-Key und installiert den Public Key des Masters auf dem VPS mit einem Forced Command, das nur `cluster_sync_command.py` ausführen kann. PBCluster nutzt diesen dedizierten Key mit `IdentitiesOnly=yes`; User müssen keine SSH-Keys manuell erzeugen oder kopieren.

VPS-Nodes starten standardmäßig keinen SSH-Fanout zu anderen Peers. Ein Runner-VPS kontaktiert nur explizite `sync_peers`; dadurch entsteht kein versehentliches VPS-zu-VPS-Mesh. Master können weiterhin zu erreichbaren VPS-Nodes pushen, solange ihre Outbound-Peer-Liste nicht explizit eingeschränkt wurde.

Nutze **Edit** an einem Node, um erlaubte Outbound-Peers zu konfigurieren. Nutze **Repair SSH** nach Änderungen an Peer-Listen oder nach einem Node-Update: die Aktion liest den remote PBCluster Public Key, speichert den Fingerprint in den Cluster-Metadaten und installiert die nötigen restricted Keys für den Master und konfigurierte Peer-Quellen.

Wenn ein gejointer Node **OK** zeigt, liest die **Preview**-Aktion den Remote-State-Vector und Desired State. Sie vergleicht Actor-Sequenzen, V7-Instance-Metadaten, Tombstones und API-Key-Metadaten mit lokalem State. Zusätzlich berechnet sie, welche lokalen Operationen remote fehlen, welche Remote-Operationsbereiche lokal fehlen und welche Hash-Referenzen eine spätere Write-Phase brauchen würde. Preview ist read-only; es kopiert keine Operationen, Blobs oder Configs.

Im Preview-Fenster ist **Push Missing Ops + Rebuild** eine explizite Retry-/Diagnose-Remote-Write-Aktion. Sie ist nur verfügbar, wenn dem lokalen Node keine Remote-Operationen fehlen. Die Aktion startet einen Backend-Push-Job, sendet aktuelle V7-Config-Blobs, API-Key-Payload-Blobs und API-Key-Secret-Blobs, sendet die lokalen Oplog-Einträge, die dem Remote-State-Vector fehlen, gesammelt in einem Bulk-Upload, meldet lokalen Fortschritt während der Job läuft und führt danach remote `rebuild` aus. Die Fortschrittsanzeige teilt oder verlangsamt den Remote-Sync nicht. Wenn der Remote-Wrapper noch zu alt für Bulk ist, fällt PBGui, wo möglich, auf langsamere Einzel-Uploads zurück.

Wenn Operationen und Config-Blobs synchron sind, zeigt das Preview-Fenster zusätzlich **V7 Config Materialization Preview**. **Materialize V7 Configs** ist die manuelle Retry-Aktion, die zugewiesene, konfliktfreie V7-JSON-Configs aus geprüften Config-Blobs in das remote `data/run_v7`-Verzeichnis schreibt. Sie verweigert den Lauf, wenn Remote-State-Vector oder Desired State vom lokalen State abweichen oder benötigte Blobs fehlen beziehungsweise ungültig sind. Configs für andere Nodes, Conflicts und Tombstones werden übersprungen.

Das Preview-Fenster zeigt außerdem **API-key Materialization Preview**. **Materialize API Keys** ist die manuelle Retry-Aktion, die `api-keys.json` aus dem replizierten Secret-Blob installiert. Master-Nodes erstellen zuerst ein normales `data/api-keys/`-Backup, wenn eine bestehende Datei abweicht; VPS-Runner ueberspringen lokale Backups. Danach schreibt PBGui atomisch und prueft den finalen Hash.

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
