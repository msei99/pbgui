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
- CMC-Pool-Credentials für alle aktiven State-Replikas und TradFi-Vault-Profile für Master.
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
- Hinzufügen, Rotieren, Deaktivieren oder Löschen eines CMC-/TradFi-Vault-Eintrags schreibt signierte Credential-Protocol-v2-Operationen und versiegelte Blobs.

Jeder Node vergleicht seine bekannten Operation-Counter mit einem anderen Node. Fehlende Operationen und benötigte Blobs werden übertragen, danach baut der empfangende Node seinen lokalen Desired State neu auf.

Dadurch ist Sync wiederholbar und sicher erneut ausführbar.

Credential-Protocol-Upgrades sind Zero-Order. Jeder aktualisierte Prozess haelt
seine eigenen Legacy-CMC-/TradFi-Credentials zuerst ueber einen lokalen
owner-only Shadow-Record verfuegbar, ohne die Legacy-Quelle zu aendern oder zu
publizieren. Gemischte v1/v2-Cluster bleiben passiv in **waiting for upgrade**.
Freeze, Inventur oder Loeschung beginnen erst, wenn jede aktive State-Replica v2
meldet; deaktivierte und entfernte Nodes werden ignoriert. Der letzte v2-Sync
startet den Cutover automatisch, ohne festgelegten letzten Node- oder
Service-Restart.

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

### Checkpoints und begrenzte Historie

PBCluster kann alte Operation-Historie durch einen verifizierten Checkpoint plus
einen aktuellen Operation-Tail ersetzen. Cleanup ist standardmäßig deaktiviert.
Unter **Cluster Sync -> Retention** siehst du die clusterweite Policy und kannst
einen reinen Lesebericht starten.

Die Werte in **Mode** und **History Days** sind Entwürfe, bis du **Save Signed
Policy** anklickst. **Run Report** verwendet immer die effektive signierte Policy
aus der blauen Zusammenfassung und nicht ungespeicherte Feldwerte. Um ein
anderes Zeitfenster ohne Löschfreigabe zu vergleichen, lässt du **Report only**
aktiv, trägst die neue Anzahl Tage ein, speicherst die signierte Policy, wartest
bis die blaue Zusammenfassung den neuen Wert zeigt und startest dann den Report
erneut. Erlaubt sind 1 bis 3650 Tage; Standard sind sieben Tage.

Verfügbare Modi:

- **Report only**: Standard; erstellt und prüft Checkpoints, löscht aber nie Historie.
- **Prune oplog history**: behält die konfigurierte Anzahl Tage sowie aktuellen und vorherigen Checkpoint.
- **Prune oplog and unreachable blobs**: entfernt zusätzlich Blobs, die in zwei identischen Reports mindestens 24 Stunden unerreichbar bleiben.

Die Report-Spalten bedeuten:

- **Status**: `dry_run` bedeutet, dass der Node nur Kandidaten berechnet und nichts gelöscht hat.
- **Checkpoint**: deterministische Shadow-Checkpoint-ID aus dem aktuell validierten Zustand und der effektiven Policy dieses Nodes.
- **Eligible Ops**: Operation-Dateien, die für das effektive Zeitfenster alt genug sind und höchstens auf der Checkpoint-Baseline liegen.
- **Eligible Size**: gemeinsame Dateigröße dieser löschbaren Operationen; Configs, Credentials, Checkpoints und Blobs sind darin nicht enthalten.
- **Retained Ops**: Operation-Dateien, die im aktuellen Tail erhalten bleiben.
- **Blob Candidates** und **Blob Size**: projizierte unerreichbare Blobs und deren gemeinsame Größe vor dem Cleanup oder die Werte der letzten passenden automatischen GC-Auswertung nach dem Cleanup-Start.
- **Blob GC Status**: `projected` für die reine Lese-Simulation vor dem Cleanup oder der automatische Blob-GC-Zustand `blocked`, `ready` beziehungsweise `complete`. Der Status nennt außerdem Blocker wie einen fehlenden committed Checkpoint oder das 24-Stunden-Stabilitätsfenster und nach Abschluss die bereits gelöschte Blob-Anzahl und Größe.
- **Migration Seal / Error**: lokales Seal-Ergebnis oder Node-Fehler. `not reported` bedeutet, dass der Remote-Preview sein Seal-Ergebnis nicht liefert; das Commit-Protokoll prüft den Seal jeder Replica trotzdem unabhängig.

Die Werte gelten pro Node. Gleiche Zeilen beschreiben normalerweise denselben
replizierten Operation-Satz auf jedem Node und dürfen nicht als unterschiedliche
Cluster-Operationen addiert werden. Eine Operation ist nur eligible, wenn sowohl
ihr signierter Zeitstempel als auch das lokale dauerhafte Dateialter älter als
der Cutoff sind.
Blob-Kandidaten beschreiben die lokalen Content-Addressed Stores jedes Nodes und
können sich berechtigt unterscheiden, wenn ein Node zusätzliche verwaiste
Kopien besitzt. Checkpoint-ID, Eligible Ops und Retained Ops müssen trotzdem auf
allen Replicas konvergieren.

Vor der Cleanup-Aktivierung projiziert **Run Report** die Blob-Kandidaten aus dem
Shadow-Checkpoint, dem Schutz des aktuellen/vorherigen Checkpoints, einem
simulierten Oplog-Prune mit dem effektiven Zeitfenster, dem verbleibenden
Operation-Tail und aktiven Mailbox-Referenzen. Werte mit Status `projected` sind
daher eine Vorschau; Blocker wie `checkpoint_missing` verhindern weiterhin jede
Löschung. Sobald eine passende automatische GC-Auswertung existiert, zeigt die
Tabelle deren tatsächliche Kandidaten und Stabilitätsstatus. **Run Report**
speichert niemals Kandidaten, verschiebt das 24-Stunden-Stabilitätsfenster nicht
und löscht keine Daten.

PBCluster prüft nach dem normalen Operation-Sync zusätzlich die Abdeckung der
für die Replica relevanten Blobs. Aktive Master vergleichen validierte
Referenzen aus dem passenden Shadow- oder aktiven/vorherigen Checkpoint, dem
verbliebenen Oplog sowie aktiven Mailboxes und senden nur die auf einem fähigen
Peer fehlenden Blobs. Ein Master mit unvollständigem lokalem Store holt einen
fehlenden Blob außerdem verifiziert von einem anderen Master, bevor er ihn
weiterverteilt. Dadurch werden konvergierte Nodes trotz bereits passender
Operation-Counter repariert. Die Coverage-Abfrage materialisiert keine Configs
und gibt keine Secret-Inhalte preis.

Direkt nach dem Speichern einer Policy sind gemischte Zeilen kurzzeitig normal,
während PBCluster die neue signierte Operation repliziert. Unterschiedliche
Checkpoint-IDs oder unterschiedliche Eligible-/Retained-Zahlen bedeuten, dass
die Replicas noch nicht konvergiert sind. Aktiviere in diesem Zustand kein
Cleanup. Warte einen abgeschlossenen PBCluster-Sync-Zyklus ab und starte den
Report erneut. Vor der Cleanup-Aktivierung müssen alle aktiven Replicas
erreichbar sein, dieselbe Checkpoint-ID und dieselben Oplog-Kandidatenzahlen
zeigen und dürfen keinen Fehler melden; Blob-Kandidaten dürfen aus dem oben
genannten lokalen Grund abweichen. Der lokale Migration Seal muss `sealed` sein
und die Cluster-Seite darf keine Conflicts zeigen.

Sicherer Ablauf für die erste Bereinigung:

1. Lasse **Report only** aktiv, speichere das gewünschte Zeitfenster und wiederhole Reports, bis alle aktiven Replicas übereinstimmen.
2. Wähle **Prune oplog history**, wenn nur die Operation-Historie bereinigt werden soll, oder direkt **Prune oplog and unreachable blobs**, wenn beides bereinigt werden soll. Speichere die signierte Policy.
3. Warte, bis PBCluster den Checkpoint vorgeschlagen und passende signierte Bestätigungen von jeder aktiven State-Replica gesammelt hat.
4. Prüfe, dass die blaue Zusammenfassung einen aktiven committed Checkpoint zeigt. `no committed checkpoint yet` bedeutet, dass Löschen weiterhin blockiert ist.
5. Warte mindestens 24 Stunden, nachdem die löschende Policy aktiv wurde. Cleanup wird bei Policy-Änderungen, am Soft-Trigger von 5.000 Operationen oder 10 MiB und mindestens täglich ausgewertet, damit auch reine Altersübergänge verarbeitet werden.
6. Starte den Report erneut und prüfe nach dem Oplog-Cleanup Cluster-State, V7-Zuweisungen, Credentials, CMC und TradFi.
7. Prüfe im kombinierten Modus nach der ersten automatischen GC-Auswertung **Blob Candidates**, **Blob Size** und **Blob GC Status**. Blob-Löschung bleibt unabhängig blockiert, bis zwei identische Kandidaten-Reports mindestens 24 Stunden auseinanderliegen.

Eine Policy-Änderung schreibt eine signierte Cluster-Operation. Ein löschender
Modus umgeht keine Sicherheitsprüfung: Jede aktive State-Replica muss denselben
Checkpoint unabhängig bestätigen, die Credential-Protocol-v2-Migration muss
versiegelt sein, der Checkpoint-Reducer muss dem Full Replay entsprechen und
die löschende Policy muss seit 24 Stunden aktiv sein. Ein Policy-Konflikt fällt
automatisch auf Report-only zurück.

Operationen nach einem Checkpoint sind signiert und an dessen Checkpoint-ID
gebunden. Ein Node hinter gelöschter Historie installiert zuerst den bestätigten
Checkpoint und die benötigten Blobs. Einen divergierenden alten Tail lehnt
PBGui ab, statt ihn zu mergen. Checkpoint-aware Join und Join Existing Cluster
benötigen keine alten Genesis-Oplog-Dateien.

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

## Credentials

Cluster Sync verfolgt sowohl Änderungen am Exchange-`api-keys.json` als auch Generationen im Credential Vault.

Der Desired State speichert nur Metadaten wie Serial und Payload-Hash. Der API-Key-Inhalt wird als eingeschränkte Secret-Daten gespeichert und darf nicht in Logs oder im normalen Desired-State-JSON auftauchen.

Die Installation von API-Keys auf einem Node nutzt die Sicherheitsstufen der Cluster-Sync-Materialisierung:

- Backup auf Master-Nodes erstellen, wenn eine bestehende Datei abweicht
- lokale Backups auf VPS-Runnern ueberspringen
- neue Datei schreiben
- Payload verifizieren
- keine Bots neu starten und keine weiteren Dateien deployen

CMC- und TradFi-Vault-Einträge verwenden nicht den Exchange-API-Key-Blob. Credential Protocol v2 signiert jede Operation und versiegelt jede Secret-Generation für ihre zulässigen Empfänger. CMC nutzt die Audience `cluster` (aktive Master und VPS-Replikas), TradFi die Audience `masters`. Eine VPS kann ein undurchsichtiges TradFi-Envelope validieren, speichern und weiterleiten, ist aber kein Empfänger und kann es nicht entschlüsseln.

Protocol-v1-Peers erhalten keine v2-Credential-Operationen. Credential-Migration und neue Credential-Veröffentlichung warten, bis jede aktive State-Replica Protocol-v2-Crypto-Capability meldet.

CMC-Leases sind Best-Effort-Koordinationsmetadaten und keine Voraussetzung für die Nutzung eines materialisierten Keys. Wenn Authority oder Relay nicht verfügbar sind, läuft die lokale Auswahl mit Soft-Budget weiter. Provider-`429` setzt den betroffenen Key auf Cooldown und wechselt nach Möglichkeit auf einen anderen geeigneten Pool-Key. Importierte, extern verwendete und Shared-Quota-Keys sind zulässige Pool-Mitglieder; Provider-Rotation ist optional.

Wenn sich die aktive Membership ändert, rewrappt PBGui bestehende Secret-Generationen für die neue Empfängermenge. Ein neuer Node darf Credential Capability erst nach Rewrap, Materialisierung der exakten Generation und ACK als aktiv melden. TradFi bleibt dabei master-only.

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

Die Seite ist in Overview, Setup, Nodes, Credentials, V7 State, Tombstones, Retention und Oplog aufgeteilt. Sie aktualisiert lokalen Status, Nodes, Desired State und aktuelle Oplog-Einträge im Hintergrund und ersetzt nur geänderte Karten und Node-Tabellenfelder, statt die ganze Seite neu zu laden.

Die Seite zeigt:

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
- editierbaren Node-Sync-Modus, SSH-Endpunkt, Remote PBGui Dir und Outbound-Peer-Allowlist
- Disabled-Node-Removal für stale Nodes, die keine V7-Configs mehr besitzen
- signierte History-Retention-Policy und begrenzte read-only Cleanup-Reports pro Node

Bootstrap schreibt explizite lokale `ADD_NODE`-Operationen für bekannte VPS-Manager-Hosts und `UPSERT_CONFIG`-Operationen für lokale Configs. Wenn VPS-Monitor-Metadaten verfügbar sind, übernimmt Bootstrap, ob ein bekannter Host Master oder VPS-Runner ist. Fehlende Dateien oder fehlende VPS-Einträge werden nie als Delete interpretiert und Tombstones werden dadurch nicht entfernt. Die Probe-Spalte führt, wenn verfügbar, nur ein read-only restricted `hello` aus; sie installiert keine Keys, schreibt keine Remote-Dateien, startet oder stoppt keine Bots und deployed nichts.

Der Node-Sync-Modus steuert, welche Nodes PBCluster kontaktieren darf:

- **Disabled** behält den Node in der Cluster-Historie, schließt ihn aber vom Sync aus.
- **Outbound Only** bedeutet, dass der Node kein inbound SSH braucht; er kann trotzdem selbst zu erlaubten Peers synchronisieren.
- **Reachable via SSH** erlaubt zugelassenen Peers, den Node über SSH-Host, SSH-User, SSH-Port und Remote PBGui Dir zu kontaktieren.

Der lokale Master erkennt sein eigenes Remote PBGui Dir aus dem laufenden Checkout und speichert es, wenn möglich, relativ zum Home-Verzeichnis. Fehlende lokale SSH-Host-/User-Metadaten werden außerdem aus lokalem Netzwerk und Login-User ergänzt. Prüfe Remote-Node-Metadaten nach Join oder Import trotzdem, besonders wenn eine private VPN-Adresse statt einer öffentlichen Adresse verwendet werden soll.

VPS-Nodes sind nach Bootstrap oder Import häufig zunächst **Disabled**. Nutze dann zuerst **Edit**, stelle den Node auf **Reachable via SSH**, prüfe SSH Host/User/Port und Remote PBGui Dir, speichere und führe danach **Repair SSH** aus. Klicke anschließend **Probe Active Nodes**. Der **Join & Sync**-Button erscheint erst, wenn PBGui einen aktuellen Probe-Status hat und der erreichbare Node **No Identity** meldet.

Wenn ein Node **No Identity** zeigt, schreibt **Join & Sync** die Remote-Cluster-Identität und überschreibt keine abweichende bestehende Identität. Danach pusht PBGui fehlende lokale Operationen, baut den Remote-Cluster-State neu, materialisiert zugewiesene V7-Configs und API-Keys und startet PBRun wieder, wenn der Remote-State aktuell ist. Auf VPS-Runnern stoppt Join vorher PBRun, damit laufende Bots während der Übergangsphase nicht bewertet werden; passivbot-Prozesse bleiben unangetastet. Auf Master-Nodes wird PBRun nicht gestoppt oder gestartet.

Nach dem Eintreffen von Protocol-v2-Operationen materialisiert PBCluster zusätzlich zulässige versiegelte Credentials in den owner-only Vault. Die Materialisierung von Exchange-`api-keys.json` und versiegelten Credentials sind getrennte Schritte.

Um einen neu eingerichteten VPS-Runner einem bestehenden Cluster hinzuzufügen, nutze diese Reihenfolge:

1. VPS in **System -> VPS Manager** anlegen und einrichten. Nach erfolgreichem Setup registriert PBGui die VPS automatisch lokal als Cluster-Node-Kandidat. Wenn der Host vor dieser Automatik eingerichtet wurde, öffne die VPS im VPS Manager und klicke **Add to Cluster**.
2. **System -> Cluster Sync -> Nodes** öffnen und die neue VPS-Zeile suchen.
3. In der neuen VPS-Zeile **Edit** nutzen, **Sync Mode** auf **Reachable via SSH** stellen, SSH Host/User/Port und Remote PBGui Dir prüfen und speichern.
4. **Probe Active Nodes** klicken. Der Node sollte erreichbar werden und vor dem Join **No Identity** melden.
5. In der VPS-Zeile **Join** / **Join & Sync** klicken. Dadurch schreibt PBGui die Remote-Cluster-Identität und synchronisiert/materialisiert den State auf der VPS.
6. Den lokalen Master-Node editieren, der aktiv mit dieser VPS synchronisieren soll, und die neue VPS in dessen erlaubte Sync Peers aufnehmen. Ohne diesen Schritt kann die VPS bereits gejoint sein, aber die Spalte **Login Key** zeigt beim Master weiter **Skipped**, weil PBCluster von diesem Master aus keinen Login zu diesem Node versucht.
7. In der neuen VPS-Zeile **Install Key** ausführen oder nach mehreren neuen Nodes bzw. Peer-Listen-Änderungen **Repair All SSH** nutzen.
8. Danach erneut **Probe Active Nodes** klicken und warten, bis PBCluster einen Sync-Durchlauf gemacht hat. **Login Key** sollte von **Skipped** oder **Checking** auf **Installed** wechseln, sobald der lokale Master sich erfolgreich mit dem dedizierten Cluster-Sync-Key eingeloggt hat.

Die Spalte **Login Key** beschreibt den regulären PBCluster-Sync-Login, nicht das Ergebnis von Join. **Skipped** bedeutet, dass der Node aktuell nicht in der lokalen Outbound-Sync-Topologie liegt, zum Beispiel weil die Sync-Peer-Liste des lokalen Masters diese VPS noch nicht enthält. Es bedeutet nicht, dass Join fehlgeschlagen ist.

Nutze **Join Existing Cluster** auf einem zweiten Master, der vom primären Master nicht inbound erreichbar ist, aber selbst per SSH zum primären Master verbinden kann. Tue das vor Bootstrap, wenn der Master einem bestehenden Cluster beitreten soll. Die Aktion nutzt den VPS-Monitor-SSH-Pool, wenn Key-Login bereits funktioniert, oder fragt per Prompt ein einmaliges SSH-Passwort ab, solange noch keine Keys installiert sind. Sie sucht das Upstream-PBGui-Verzeichnis in derselben Reihenfolge wie der VPS Manager (`remote_pbgui_dir`, `~/software/pbgui`, `~/pbgui`), liest den Upstream-Master, übernimmt dessen `cluster_id` automatisch bei leerem lokalen Oplog, zieht Upstream-Operationen und Blobs, registriert den lokalen Master als `outbound_only` mit erkannten lokalen Pfad/IP/User-Metadaten, installiert den lokalen Cluster-SSH-Key auf dem Upstream-Master und pusht die Registrierungsoperationen zurück. Das SSH-Passwort wird nur für diesen Request verwendet und nicht gespeichert. Wenn diese lokale Installation bereits Cluster-Operationen für einen anderen Cluster hat, verweigert Self-Join das Überschreiben, außer die Recovery-Option ist aktiviert. Recovery archiviert den bisherigen lokalen Cluster-State unter `data/cluster/archives/`, bevor er durch den Upstream-Cluster-State ersetzt wird. Stelle diesen Master nach dem Join nur dann auf **Reachable via SSH**, wenn andere erlaubte Peers SSH zurück zu ihm initiieren sollen.

PBCluster-SSH-Zugriff ist technischer Setup-Status. Beim normalen PBGui-Setup/Update auf einem VPS erzeugt PBGui jetzt automatisch einen dedizierten lokalen PBCluster-SSH-Key und installiert den Public Key des Masters auf dem VPS mit einem Forced Command, das nur `cluster_sync_command.py` ausführen kann. PBCluster nutzt diesen dedizierten Key mit `IdentitiesOnly=yes`; User müssen keine SSH-Keys manuell erzeugen oder kopieren.

VPS-Nodes starten standardmäßig keinen SSH-Fanout zu anderen Peers. Ein Runner-VPS kontaktiert nur explizite `sync_peers`; dadurch entsteht kein versehentliches VPS-zu-VPS-Mesh. Master können weiterhin zu erreichbaren VPS-Nodes pushen, solange ihre Outbound-Peer-Liste nicht explizit eingeschränkt wurde.

Nutze **Edit** an einem Node, um Sync-Modus, SSH-Host/User/Port, Remote PBGui Dir und erlaubte Outbound-Peers zu konfigurieren. Nutze **Repair SSH** für einen einzelnen Node nach Änderungen an dessen Peer-Liste, SSH-Metadaten oder nach einem Node-Update: die Aktion liest den remote PBCluster Public Key, speichert den Fingerprint in den Cluster-Metadaten und installiert die nötigen restricted Keys für den Master und konfigurierte Peer-Quellen. Nutze **Repair All SSH** nach einem größeren Update oder einer Topologieänderung, wenn mehrere aktive erreichbare Nodes einen Key-Refresh brauchen können. Die Aktion führt denselben Repair-Flow für alle aktiven Nodes aus, meldet fehlgeschlagene Nodes, Outbound-Install-Fehler und fehlende Source-Keys, und lässt disabled Nodes sowie inbound-Ziele von outbound-only Nodes unangetastet. Wenn normaler SSH-Key-Login noch nicht verfügbar ist, fragt PBGui für den betroffenen Node das SSH-Passwort ab, wiederholt den Repair nur für diesen Request mit diesem Passwort und speichert es nicht. Nutze **Remove** nur für deaktivierte nicht-lokale Nodes, die keine V7-Configs mehr besitzen; die Aktion schreibt eine `REMOVE_NODE`-Operation und entfernt den Node aus der materialisierten Membership, während die Oplog-Historie erhalten bleibt.

Wenn ein gejointer Node **OK** zeigt, liest die **Preview**-Aktion den Remote-State-Vector und Desired State. Sie vergleicht Actor-Sequenzen, V7-Instance-Metadaten, Tombstones und API-Key-Metadaten mit lokalem State. Zusätzlich berechnet sie, welche lokalen Operationen remote fehlen, welche Remote-Operationsbereiche lokal fehlen und welche Hash-Referenzen eine spätere Write-Phase brauchen würde. Preview ist read-only; es kopiert keine Operationen, Blobs oder Configs.

Im Preview-Fenster ist **Push Missing Ops + Rebuild** eine explizite Retry-/Diagnose-Remote-Write-Aktion. Sie ist nur verfügbar, wenn dem lokalen Node keine Remote-Operationen fehlen. Die Aktion startet einen Backend-Push-Job, sendet aktuelle V7-Config-Blobs, API-Key-Payload-Blobs und API-Key-Secret-Blobs, sendet die lokalen Oplog-Einträge, die dem Remote-State-Vector fehlen, gesammelt in einem Bulk-Upload, meldet lokalen Fortschritt während der Job läuft und führt danach remote `rebuild` aus. Die Fortschrittsanzeige teilt oder verlangsamt den Remote-Sync nicht. Wenn der Remote-Wrapper noch zu alt für Bulk ist, fällt PBGui, wo möglich, auf langsamere Einzel-Uploads zurück.

Wenn Operationen und Config-Blobs synchron sind, zeigt das Preview-Fenster zusätzlich **V7 Config Materialization Preview**. **Materialize V7 Configs** ist die manuelle Retry-Aktion, die zugewiesene, konfliktfreie V7-JSON-Configs aus geprüften Config-Blobs in das remote `data/run_v7`-Verzeichnis schreibt. Sie verweigert den Lauf, wenn Remote-State-Vector oder Desired State vom lokalen State abweichen oder benötigte Blobs fehlen beziehungsweise ungültig sind. Configs für andere Nodes, Conflicts und Tombstones werden übersprungen.

Das Preview-Fenster zeigt außerdem **API-key Materialization Preview**. **Materialize API Keys** ist die manuelle Retry-Aktion, die `api-keys.json` aus dem replizierten Secret-Blob installiert. Master-Nodes erstellen zuerst ein normales `data/api-keys/`-Backup, wenn eine bestehende Datei abweicht; VPS-Runner ueberspringen lokale Backups. Danach schreibt PBGui atomisch und prueft den finalen Hash.

Die CMC-/TradFi-Vault-Migration ist fortsetzbar und automatisch. Sie inventarisiert alte CMC-/TradFi-Felder, setzt einen signierten Writer Freeze über alle aktiven v2-Replikas, importiert und versiegelt unveränderliche Generationen, wartet auf ACKs der exakten Materialisierung und entfernt danach unveränderte Legacy-Felder mit Backup. CMC-Secrets nicht wieder in `pbgui.ini`, VPS-Inventar oder Automation eintragen und PB7-TradFi-Einträge nicht manuell bearbeiten. Rotation ist für den Abschluss des Cleanups nicht erforderlich.

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

Wenn **Repair All SSH** Outbound-Fehler meldet:

- Bei `SSH authentication failed` das angefragte SSH-Passwort für den genannten Node eingeben und aus dem Modal erneut versuchen. Das Passwort ist temporär und wird nicht gespeichert.
- Bei `Remote host is unreachable` die **Reachable via SSH**-Metadaten des Nodes sowie Netzwerk-/Firewall-Zugriff prüfen und danach **Probe Active Nodes** ausführen.
- Bei fehlenden Source-Keys zuerst **Repair SSH** auf dem Source-Node ausführen oder **Repair All SSH** wiederholen, nachdem der Source-Node einen gespeicherten Cluster-SSH-Public-Key hat.
- Nach dem Repair erneut **Probe Active Nodes** ausführen, bevor **Join & Sync** oder Remote-Preview-Aktionen genutzt werden.

---

## Sicherheitsregeln

- Lokale Bot-Verzeichnisse nicht löschen, um einen Delete auszulösen. PBGui verwenden, damit eine Delete-Operation geschrieben wird.
- Kopierte `data/cluster/node_id` Dateien nicht auf einer anderen Installation wiederverwenden.
- `desired_state.json` nicht manuell bearbeiten; die Datei wird aus dem Operation Log generiert.
- Admin-SSH-Zugang für Recovery behalten, auch wenn Cluster-Replikation eingeschränkte Keys verwendet.
