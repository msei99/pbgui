# Plan: Clusterweiter CMC-Pool und konsolidierte Credentials

**Status: umgesetzt.**

Die Credential-Migration ist ein Zero-Order Rolling Upgrade. API, lokaler
PBCluster, weitere Master, VPS, PBCoinData und TradFi-Worker duerfen in beliebiger
Reihenfolge aktualisiert und auch erst Tage spaeter neu gestartet werden. Jeder
aktualisierte Prozess legt vor dem globalen Freeze ausschliesslich aus seinen
lokalen INI-/PB7-Quellen aktive `legacy_shadow`-Vault-Records an; alte Prozesse
lesen ihre unveraenderten Legacy-Quellen weiter. Dieser lokale Bootstrap braucht
weder Cluster-Identitaet noch Verbindung und veroeffentlicht keine Operation.

## Korrigierte Zielentscheidungen

- Secrets werden nicht mehr in `pbgui.ini` gespeichert. Das gilt auch für
  CoinMarketCap und TradFi.
- Jeder aktive Master und VPS erhält den vollständigen CMC-Key-Pool. Jeder
  Knoten kann lokal einen geeigneten, noch nutzbaren Key auswählen.
- CMC-Secrets werden lokal unter `data/credentials/cmc/` materialisiert und
  direkt von PBCoinData beziehungsweise einem gemeinsamen CMC-Client gelesen.
- `pbgui.ini` behält nur nicht-sensitive Betriebsparameter wie Fetch-Intervalle
  und Limits.
- Cluster Sync verteilt CMC-Credentials transitiv durch die vorhandene
  Peer-Topologie. Ein VPS kann dabei Relay zwischen nicht direkt verbundenen
  Mastern sein.
- Relay-Knoten transportieren signierte verschlüsselte Operationen und Blobs
  unabhängig davon, ob sie deren Inhalt entschlüsseln dürfen.
- CMC-Secrets haben die Audience `cluster`: Alle aktiven Master und VPS dürfen
  sie entschlüsseln.
- TradFi-Secrets haben die Audience `masters`: VPS-Knoten speichern und leiten
  nur Ciphertext weiter, erhalten aber keinen Schlüssel zum Entschlüsseln.
- Die Konsolidierung und der globale CMC-Pool werden gemeinsam umgesetzt. Es
  gibt keinen vorübergehenden neuen per-Host-CMC-Store als Zielarchitektur.

## Risikomodell

- Der CMC-Pool verwendet kostenlose Keys. Ziel sind gute Verteilung,
  automatische Auswahl und schnelles Failover, nicht eine mathematische
  Garantie, dass unter jeder Partition niemals ein Provider-Limit erreicht
  wird.
- CMC bleibt die endgültige Limit-Instanz. 429-, Exhausted- und Invalid-
  Antworten führen zu lokalem Cooldown und Auswahl des nächsten Pool-Keys.
- Importierte oder außerhalb PBGui mitbenutzte Keys sind zulässig. Deren Usage
  kann zwischen zwei Provider-Snapshots abweichen.
- Historische Klartext-Replikation allein erzwingt keine Provider-Rotation.
  Rotation bleibt eine optionale Admin-Aktion.
- Der neue Store verhindert zukünftige unnötige Verteilung und Browser-/INI-
  Exposition; er versucht nicht, bereits erfolgte Kenntnis kryptografisch
  rückwirkend zu widerrufen.

## Warum Cluster Sync die Verteilung übernehmen kann

Cluster Sync repliziert heute bereits Operationen aller Actors anhand eines
State Vectors. Ein Knoten leitet dabei nicht nur eigene Operationen weiter,
sondern jede Operation, die der nächste Peer noch nicht besitzt.

Damit funktioniert beispielsweise:

```text
Master A -> VPS Relay <- Master B
```

1. Master A schreibt eine Operation und synchronisiert zum VPS.
2. Der VPS behält Actor, Sequenz, Operation und referenzierte Blobs unverändert.
3. Master B synchronisiert später mit demselben VPS.
4. Master B zieht die Operation von Master A und deren Blobs vom VPS.

Auch eine Kette mit ausgehendem Sync des VPS funktioniert, sofern die
`sync_peers` explizit konfiguriert sind:

```text
Master A -> VPS Relay -> Master B
```

Die Zustellung ist eventual consistent und kann einen weiteren Sync-Zyklus
benötigen. Direkte Erreichbarkeit zwischen Master A und Master B ist nicht
erforderlich.

Direkte Master-zu-Master-Kanten bleiben zulässig, wenn sie in einer Installation
erreichbar und autorisiert sind. Weder Secret-Verteilung noch Lease-Protokoll
dürfen sie jedoch voraussetzen; jede Funktion muss über eine reine Relay-
Topologie dieselben Ergebnisse erreichen.

## Sicherheitslücken der heutigen Secret-Blobs

Die bestehende Secret-Blob-Pipeline ist noch keine ausreichende Grundlage für
den neuen Store:

- Secret-Blobs enthalten Klartext. Base64 ist nur Encoding.
- SSH schützt die Übertragung, aber jeder State-Replica-Knoten speichert den
  Klartext lokal.
- VPS-Knoten materialisieren heute ebenfalls die vollständige PB7
  `api-keys.json`.
- Secret-Blobs besitzen keine Audience oder Empfängerliste.
- Operationen sind nicht anwendungseitig signiert.
- Ein Relay kann den angegebenen Actor derzeit nicht kryptografisch prüfen.
- `get-secret-blob` ist nicht nach Empfänger oder aktuellem Desired State
  autorisiert.
- `--allow-join` wird im normalen Forced-Command-Pfad zu breit verwendet.
- API-Key-Operationen besitzen keinen Parent-Generation-Konfliktschutz.
- State Vectors verwenden die höchste beobachtete statt der höchsten
  lückenlosen Sequenz.

Vor der Credential-Verteilung wird Cluster Sync deshalb auf ein signiertes,
verschlüsseltes Secret-Format erweitert.

## Kryptografisches Secret-Modell

### Knotenschlüssel

Jeder Cluster-Knoten erhält getrennte Anwendungsschlüssel:

```text
data/cluster/crypto/
  operation_signing_ed25519
  operation_signing_ed25519.pub
  secret_encryption_x25519
  secret_encryption_x25519.pub
```

- Private Dateien: `0600`.
- Verzeichnis: `0700`.
- Ed25519 signiert Operationen und Lease-Nachrichten.
- Die Empfängerverschlüsselung verwendet eine vollständige, geprüfte
  HPKE-Konstruktion nach RFC 9180 mit X25519, HKDF-SHA256 und einem AEAD.
- SSH-Schlüssel bleiben ausschließlich Transport-Authentisierung und werden
  nicht als Anwendungs- oder Encryption-Key wiederverwendet.
- Öffentliche Schlüssel und Rollen werden durch autorisierte, signierte
  Membership-Operationen an eine Node-ID gebunden.
- Ein extern bestätigter signierter v2-Membership-Checkpoint bildet den
  Genesis-Trust. Key-Rotation, Key-Historie und Revocation sind Teil dieses
  Membership-State.
- Der Fingerprint des Genesis-Signers wird beim Join über einen separaten,
  administrativ bestätigten Kanal geprüft. Membership-Operationen enthalten
  Parent-Generation und Parent-Hash, bilden eine lineare Kette und gehen bei
  einem Fork fail-closed. Secret-Envelopes werden nur gegen eine kanonische
  Ancestor-Membership akzeptiert.

### Sealed Secret

Für jede Credential-Version wird ein zufälliger 256-Bit-Datenschlüssel erzeugt:

1. Das Secret wird mit dem in der Envelope-Version festgelegten AEAD-Verfahren
   verschlüsselt.
2. Cluster-ID, Secret-ID, Secret-Kind, Version und Audience werden als
   Additional Authenticated Data gebunden.
3. Der Datenschlüssel wird pro berechtigtem Empfänger über RFC-9180-HPKE
   eingepackt; Envelope enthält Algorithmus-IDs, Ephemeral Public Key, Nonce,
   Recipient-ID und vollständigen HPKE-Kontext.
4. Cluster Sync repliziert nur Ciphertext, Recipient-Manifest und signierte
   Metadaten.
5. Es wird kein Hash des Klartext-Secrets veröffentlicht.

Empfängerregeln:

| Secret-Kind | Audience | Empfänger |
| --- | --- | --- |
| `cmc_api_key` | `cluster` | alle aktiven `state_replica` Master und VPS |
| `tradfi_profile` | `masters` | nur aktive Nodes mit autorisierter Rolle `master` |
| Exchange/PB7-Credentials | separat festlegen | nicht implizit mit TradFi koppeln |

Ein VPS-Relay darf einen TradFi-Ciphertext und dessen signierte Operation
speichern und weiterleiten. Da sein Recipient-Manifest keinen Wrapped Key für
diesen VPS enthält, kann er das Secret nicht entschlüsseln.

## Cluster-Sync-Protokoll v2

### Signierte Operationen

Neue Operationen:

```text
UPSERT_SECRET
ROTATE_SECRET
TOMBSTONE_SECRET
UPDATE_SECRET_RECIPIENTS
UPSERT_CMC_POOL_ENTRY
SET_CMC_KEY_STATE
SET_CMC_AUTHORITY
SET_TRADFI_ACTIVE_PROFILE
```

Jede Secret-Operation enthält mindestens:

```json
{
  "secret_id": "random-stable-id",
  "secret_kind": "cmc_api_key",
  "audience": "cluster",
  "generation": 4,
  "parent_generation": 3,
  "membership_generation": 27,
  "ciphertext_hash": "sha256:...",
  "recipient_manifest_hash": "sha256:...",
  "signing_key_id": "ed25519:...",
  "signature": "base64..."
}
```

- Zwei unterschiedliche Generationen vom selben Parent erzeugen einen
  Konflikt; der betroffene Key wird bis zur Auflösung nicht verwendet.
- Tombstones dominieren verspätete ältere Upserts.
- `created_at` ist nur Information und entscheidet keinen Gewinner.
- Relays verändern Actor, Signatur, Generation oder Blob-Referenzen nicht.
- Empfänger prüfen die Actor-Signatur statt den Actor mit dem unmittelbaren
  SSH-Peer gleichzusetzen.
- Signiert wird eine kanonische Kodierung des vollständigen Envelopes inklusive
  Cluster-ID, Actor, Sequenz, Operationstyp, Membership-Generation, Audience,
  Parent-Generation und aller Blob-Hashes.
- Die kanonische JSON-Kodierung folgt RFC 8785 JCS; eine spätere Änderung des
  Encodings erfordert eine neue Envelope-Version.

### Opaque Blob-Weiterleitung

- Blob-Referenzen werden explizit typisiert: Config, Ciphertext,
  Recipient-Manifest.
- Jeder State-Replica-Knoten darf Ciphertext weiterleiten.
- Nur Materializer prüfen, ob der lokale Node Empfänger ist.
- Nicht-Empfänger melden `not_recipient`, nicht `missing` oder `error`.
- Historische Klartext-Secret-Blobs werden nicht dauerhaft aufgrund alter
  Operationen weiter repliziert. Nach Rebuild werden nur Blobs des aktuellen
  Desired State gezogen.

### Protokoll-Härtung

- Operationen und Bundles vollständig validieren, bevor sie veröffentlicht
  werden.
- State Vector auf höchste lückenlose Actor-Sequenz umstellen.
- Fehlende Sequenzbereiche explizit nachfordern.
- Normale Sync- und Join-Autorisierung trennen; kein pauschales
  `--allow-join`.
- Inbound-Node und erlaubte Topologie bei jedem Forced-Command prüfen.
- Entfernte oder deaktivierte Nodes aus `authorized_keys` entfernen.
- Tatsächliche SSH-Hostkeys pinnen und prüfen.
- Rollenwechsel nur durch autorisierte Master-Signatur akzeptieren.
- Lokale Sequenzvergabe mit Cross-Process-Lock atomar machen.
- Ein Node ist erst `credential_active`, wenn er Protokoll v2 unterstützt,
  autorisierte Crypto-Keys besitzt, alle aktuellen CMC-Wrappers erhalten, jede
  Generation materialisiert und die Kataloggeneration bestätigt hat.
- Beim Hinzufügen eines Nodes werden alle aktiven CMC-Secrets vor dessen
  Aktivierung neu für ihn gewrappt. Master-Promotion materialisiert zusätzlich
  alle Master-Secrets. Demotion oder Entfernen kann bereits gelerntes Secret
  nicht widerrufen; die UI weist darauf hin und bietet eine optionale Provider-
  Rotation an.

## Lokaler Credential-Store

Jeder aktive Cluster-Knoten materialisiert:

```text
data/credentials/
  cmc/
    catalog.json
    keys/
      <key_id>/
        <generation>.json
    leases/
      <lease_id>.json
    local_usage/
      <key_id>.json
    journal/
      authority.wal
      local_usage.wal
    migration.json
  tradfi/
    profiles/
      <profile_id>.json
```

- Alle Verzeichnisse sind `0700`, alle Dateien `0600`.
- Jeder Read-Modify-Write-Vorgang verwendet Cross-Process-Lock und atomaren
  Austausch.
- Symlinks und Pfad-Traversal werden abgelehnt.
- Key-IDs sind zufällige stabile IDs und werden nicht aus Secret-Werten
  abgeleitet.
- CMC-Key-Dateien sind immutable pro Generation. Alte Generationen bleiben bis
  zum terminalen Zustand aller exakt daran gebundenen Leases erhalten.
- Der Katalog enthält ausschließlich nicht-sensitive Metadaten, Status,
  Generationen, Usage-Snapshots und Reset-Zeitpunkte.
- PBCoinData liest den Pool direkt aus diesem Store. Es gibt keine
  CMC-Runtime-Projektion in `pbgui.ini`.
- TradFi-Reader lesen ebenfalls den Store. Nur der reservierte TradFi-Subtree in
  PB7 `api-keys.json` ist bei Bedarf eine abgeleitete Projektion. Exchange-
  Credentials und übrige Metadaten derselben Datei bleiben separat verwaltet.

## Globaler CMC-Key-Pool

### Pool-Inhalt

Jeder Master und VPS besitzt alle aktiven CMC-Secrets. Ein Katalogeintrag
enthält:

```text
key_id
label
secret_generation
catalog_generation
state
quota_domain_id
authority_node_id
authority_epoch
provider_plan
minute_limit
daily_limit
monthly_limit
provider_usage_snapshot
daily_reset
monthly_reset
health
last_error
```

Key-Zustände:

```text
pending
active
draining
disabled
invalid
provider_disabled
minute_limited
day_exhausted
month_exhausted
conflicted
tombstoned
```

### Warum replizierte Credits-Left-Zähler nicht ausreichen

Cluster Sync ist eventual consistent. Wenn zwei partitionierte Nodes denselben
Wert `credits_left=100` sehen, können beide diese 100 Credits ausgeben. Ein
replizierter optimistischer Zähler kann das Provider-Limit daher nicht sicher
schützen.

Der Pool verwendet deshalb nicht „letzten Usage-Wert lesen und Request senden“,
sondern nicht überlappende Budget-Leases pro Provider-Quota-Domain.

### Quota-Domain und Authority

- Mehrere API-Keys oder Generationen können zum selben CMC-Subscription-Budget
  gehören. `quota_domain_id` bezeichnet dieses Provider-Budget unabhängig von
  der lokalen Key-ID.
- Wenn PBGui die gemeinsame Quota-Domain mehrerer Keys nicht sicher bestimmen
  kann, werden sie zunächst als getrennte Domains behandelt und nach den
  providerseitig gemeldeten Usage-Werten korrigiert.
- Jede Quota-Domain besitzt genau einen autorisierten Budget-Authority-Master,
  einen Authority-Epoch, ein Ledger, einen Minute-Limiter und eine gemeinsame
  Summe offener Leases.
- Die Authority verwaltet den kanonischen Usage-Epoch und vergibt
  nicht überlappende Leases.
- Eine Authority muss keinen anderen Master direkt erreichen. Lease-Nachrichten
  werden signiert und über dieselbe transitive Cluster-Topologie weitergeleitet.
- Andere Master spiegeln Authority-Snapshots für Diagnose und Recovery, dürfen
  erst nach einem expliziten Authority-Epoch-Wechsel neue Leases ausstellen.
- Automatische Leader-Wahl ist mit dem heutigen eventual-consistent Cluster
  nicht sicher und bleibt ausgeschlossen.
- Importierte und extern gemeinsam verwendete Keys dürfen normal am Pool
  teilnehmen. Provider-Snapshots und eine konfigurierbare Safety Margin
  reduzieren Kollisionen mit externer Nutzung; eine absolute Vermeidung von 429
  ist dabei ausdrücklich kein Ziel.

### Relay-Mailbox für Lease-Nachrichten

High-frequency Lease-Nachrichten gehören nicht dauerhaft in das Desired-State-
Oplog. Protokoll v2 ergänzt eine signierte, adressierte Relay-Mailbox:

```text
CMC_LEASE_REQUEST
CMC_LEASE_GRANT
CMC_LEASE_SETTLEMENT
CMC_PROVIDER_EVENT
CMC_LEASE_ACK
```

- Nachrichten besitzen Sender, Empfänger, Message-ID, Key-ID, Generation,
  Ablaufzeit und Signatur.
- Jeder Replica-Knoten darf Nachrichten opak weiterleiten.
- Nur der adressierte Empfänger verarbeitet den Inhalt.
- Zustellung ist at-least-once. Idempotenz entsteht nicht nur durch Message-IDs:
  Authority-Ledger, `request_id -> lease_id`, vollständige Budgetreservierung
  und durable Outbox werden in einer gemeinsamen WAL-Transaktion committed,
  bevor ein Grant sichtbar wird. Ein Duplicate erhält exakt dasselbe Lease.
- Dasselbe gilt für alle Authority-Nachrichten. Ein Settlement führt genau einen
  terminalen Lease-CAS aus; Duplicate Settlements können ungenutztes Budget
  niemals mehrfach freigeben. Nicht eindeutig terminale Leases bleiben bis
  Ablauf vollständig reserviert.
- ACK und TTL erlauben Garbage Collection ohne unendliches Oplog-Wachstum.
- Ein VPS-Relay benötigt weder direkte Master-Verbindung noch Sonderwissen über
  den Request-Inhalt.

### Vorab verteilte Budget-Leases

Damit jeder Node selbst einen Key wählen kann, verteilt die Authority proaktiv
kleine Budget-Leases an aktive Nodes:

```text
lease_id
key_id
secret_generation
quota_domain_id
node_id
max_daily_credits
max_monthly_credits
rolling_minute_request_tokens
max_concurrent_requests
allowed_endpoint_classes
not_before
expires_at
minute_windows
authority_epoch
signature
```

- Leases einer Quota-Domain überlappen weder Tages-/Monatscredits,
  Rolling-Minute-Request-Tokens noch Concurrent-Request-Slots.
- Die Summe aller offenen Leases bleibt in jeder Dimension unter dem vom
  Provider bestätigten Restbudget abzüglich Safety Margin und einer nicht
  verleasbaren In-flight-Contingency.
- Leases enden vor Minute-, Tages- oder Monatsreset mit Clock-Skew-Puffer.
- Die Authority reserviert das vollständige Lease-Budget bereits beim Grant.
- Nicht gemeldete oder unklare Nutzung gilt konservativ als verbraucht.
- Kleine Leases begrenzen Schaden bei Partition, Disable und Node-Verlust.
- Ein Node fordert rechtzeitig Nachschub an; die Antwort kann über mehrere
  Relay-Hops kommen.
- Lokaler Lease-Verbrauch, In-flight-Reservierung und Settlement werden
  ebenfalls in einer gemeinsamen crash-konsistenten Journal-Transaktion
  geschrieben. Ein Absturz nach Claim darf das Lease nicht erneut nutzbar
  machen.

### Lokale Key-Auswahl

Vor jedem HTTP-Versuch führt der lokale CMC-Client eine atomare Auswahl durch:

1. Alle lokal materialisierten Pool-Keys laden.
2. Nur `active` Keys mit passender Secret-Generation berücksichtigen.
3. Keys mit gültigem lokalem Lease bevorzugen. Falls kein Lease verfügbar oder
   die Authority nicht erreichbar ist, anhand des letzten Provider-Snapshots
   und des lokalen Soft-Budgets weiterarbeiten.
4. Den maximalen Credit-Verbrauch des konkreten Endpoints reservieren.
5. Nach Restbudget, Reset-Pacing und Health fair auswählen.
6. Reservierung vor dem Netzwerkrequest persistent schreiben.
7. Genau einen HTTP-Versuch ausführen.
8. Mit `status.credit_count` abrechnen; jeder Retry benötigt eine neue lokale
   Reservierung.

Damit entscheidet jeder VPS selbst zwischen allen Pool-Keys. Leases reduzieren
globale Doppelbelegung im Normalbetrieb; bei Partitionen bleibt die Koordination
best effort und CMC selbst setzt das endgültige Limit durch.

### Credit-Kosten und Provider-Status

- Listings reserviert `ceil(fetch_limit / 200)` Credits.
- Metadata reserviert `ceil(number_of_ids / 100)` Credits.
- `/v1/key/info` kostet keine Credits, zählt aber als Request im Minute-Limit.
- Ein valides HTTP 200 wird mit dem tatsächlichen `status.credit_count`
  abgerechnet.
- Bekannte Endpoints reservieren den dokumentierten Credit-Upper-Bound. Eine
  kleine In-flight-Contingency reduziert unerwartete Überschreitungen.
- Wenn `credit_count` die Reservierung überschreitet, wird der tatsächliche Wert
  belastet, das Kostenmodell aktualisiert und für den nächsten Request
  konservativer reserviert. Der übrige Pool bleibt nutzbar.
- Ein malformed HTTP 200 verbraucht konservativ die volle Reservierung.
- Timeout oder Verbindungsabbruch gelten als `uncertain_spent`.
- Ein empfangener Provider-Fehler verbraucht keine Credits, aber einen
  Request-Slot.
- Provider-Code für invalid/disabled quarantiniert den Key.
- Minute-, Tages- und Monats-Limits setzen jeweils einen passenden Zustand mit
  providerbestätigtem Reset.
- Provider-Snapshots werden beim Add/Rotate, Authority-Start, nach Limits und
  regelmäßig während aktiver Nutzung erneuert.
- Minute-Limits werden als rollendes Provider-Fenster modelliert, nicht als
  unpräziser lokaler Kalenderminuten-Zähler. Clock-Skew und maximale
  Request-Dauer werden beim Lease-Ende abgezogen.
- Externe Nutzung wird beim nächsten Provider-Snapshot sichtbar. Bis dahin kann
  die lokale Schätzung abweichen; ein Provider-Limit führt zum normalen
  Key-Cooldown und Failover auf den nächsten Pool-Key.

### Partitionen und Authority-Ausfall

| Situation | Verhalten |
| --- | --- |
| Node erreicht Authority nicht | Bereits erteilte Leases weiter lokal nutzen |
| Node besitzt kein gültiges Lease | Lokales Soft-Budget verwenden und bei 429 wechseln |
| Settlement erreicht Authority nicht | Reserviertes Budget bleibt verbraucht |
| Catalog oder Secret-Version fehlt | Key nicht verwenden |
| Authority startet neu | Erst State prüfen und Provider-Usage aktualisieren |
| Authority fällt dauerhaft aus | Keine neuen Leases bis kontrollierter Transfer |
| Cluster ist gesplittet | Bestehende Leases nutzen; Provider-429 führt zu Cooldown |

Jeder Node persistiert den höchsten akzeptierten `authority_epoch` und lehnt
kleinere Epochen dauerhaft ab. `SET_CMC_AUTHORITY` folgt einer eindeutigen
CAS-Parent-Linie. Ein manueller Authority-Transfer erhöht den Epoch und wartet
nach Möglichkeit auf das Ende alter Leases. Provider-Key-Rotation ist dafür
nicht erforderlich.

Während einer Partition können alte und neue Authority kurzfristig
unterschiedliche Sichten besitzen. Dieses begrenzte Split-Brain-Risiko wird für
kostenlose CMC-Keys akzeptiert: Leases bleiben klein, Usage wird nach
Wiederverbindung abgeglichen, und CMC-429 beziehungsweise Exhausted-Antworten
setzen den betroffenen Key auf Cooldown. Eine automatische Authority-Promotion
bleibt zunächst ausgeschlossen, um unnötige Parallelvergabe zu vermeiden.

## Rotation und Entfernen von CMC-Keys

Vor jeder Provider-Aktion wird verbindlich entschieden, ob der Provider alte und
neue Generation gleichzeitig gültig halten kann. Erst danach beginnt genau
einer der folgenden Abläufe:

**Provider erlaubt sichere überlappende Generationen innerhalb derselben
Quota-Domain:**

1. Neue immutable Secret-Generation als `pending` erzeugen.
2. An alle aktiven Nodes replizieren, materialisieren und providerseitig
   validieren.
3. Alte Generation auf `draining` setzen und keine neuen Leases ausgeben.
4. Neue Generation unter demselben Domain-Ledger aktivieren.
5. Offene alte Leases auslaufen oder vollständig abrechnen lassen.
6. Alte Provider-Generation deaktivieren und tombstonen.

**Provider erlaubt keine sichere Überlappung:**

1. Alte Grants stoppen und alle alten Leases auslaufen lassen.
2. Alten Provider-Key invalidieren beziehungsweise beim Provider rotieren.
3. Erst jetzt die neue immutable Generation als `pending` anlegen.
4. Während des gesamten Fensters Cache-only arbeiten.
5. Neue Generation an alle Nodes verteilen, materialisieren und
   providerseitig validieren.
6. Neue Generation aktivieren und erst dann neue Leases ausgeben.
7. Alte Generation tombstonen.

Ciphertext wird in beiden Fällen erst gelöscht, wenn alle aktiven Replicas den
Tombstone bestätigt haben.

Das Entfernen eines Cluster-Knotens kann bereits kopierte CMC-Keys nicht von
dessen Datenträger zurückholen. Da jeder aktive VPS absichtlich den gesamten
Pool kennt, zeigt PBGui in diesem Fall eine optionale Empfehlung zur Rotation;
der Pool-Betrieb wird dadurch nicht automatisch blockiert.

## TradFi-Zielmodell

- TradFi-Profile werden als einzelne `masters`-Secrets gespeichert.
- Tiingo wird ebenfalls aus `[tradfi_profiles]` entfernt.
- Alle aktiven Master können die Profile entschlüsseln, sodass ein Master hinter
  einer Firewall nach Relay-Sync selbstständig übernehmen kann.
- VPS-Knoten leiten dieselben Ciphertexts transitiv weiter, erhalten aber keine
  Recipient-Wrappers.
- PBGui-Market-Data-Werkzeuge lesen direkt aus `data/credentials/tradfi/`.
- Der aktive PB7-Provider wird aus dem Vault ausschließlich in einen reservierten
  TradFi-Subtree des unvermeidbaren PB7-Runtime-Stores `api-keys.json`
  projiziert. Die übrige Datei bleibt Quelle für Exchange-Credentials.
- Alle Writer derselben Datei verwenden einen gemeinsamen Cross-Process-Lock,
  atomaren Merge und Generation/CAS-Prüfung. `Users.save()` darf den projizierten
  TradFi-Subtree weder als Quelle zurückimportieren noch in einen allgemeinen
  Cluster-API-Key-Blob aufnehmen.
- Projection-Status und Retry-State sind persistent. Ein fehlgeschlagener
  PB7-Write verliert den neuen Vault-Stand nicht.
- Bereits vorhandene TradFi-Werte werden in den Master-Store migriert und aus
  künftigen VPS-replizierten Klartext-Blobs entfernt. Eine Provider-Rotation ist
  optional und wird nicht als Voraussetzung für den Cutover behandelt.

## API und UI

CMC-Pool-API:

```text
GET    /api/services/cmc-pool
POST   /api/services/cmc-pool/keys
PATCH  /api/services/cmc-pool/keys/{key_id}
POST   /api/services/cmc-pool/keys/{key_id}/rotate
POST   /api/services/cmc-pool/keys/{key_id}/disable
DELETE /api/services/cmc-pool/keys/{key_id}
GET    /api/services/cmc-pool/usage
GET    /api/services/cmc-pool/leases
```

- Create und Rotate akzeptieren das Secret nur in einem authentifizierten
  Request-Body.
- Es gibt keinen Reveal-Endpunkt.
- Responses enthalten ID, Label, Status, Usage, Reset, Health,
  Materialization-Stand und Authority-Erreichbarkeit, niemals den Key.
- Browser-Felder werden nach erfolgreichem Submit geleert.
- VPS Manager enthält kein per-Host-CMC-Key-Feld mehr.
- Services zeigt Pool statt Einzelkey.
- Warnung ab 80 Prozent Tages- oder Monatsnutzung.
- Provider-Usage, reservierte Credits und `uncertain_spent` werden getrennt
  dargestellt.
- Stale Usage- und Authority-Daten werden mit Alter markiert.
- Die permanente Denylist betrifft ausschließlich Legacy-Felder und Legacy-
  Endpoints. Die neuen Create-/Rotate-Endpunkte dürfen ein noch ungespeichertes
  Secret im authentifizierten Body annehmen und übergeben es direkt an den
  Credential-Service.
- Bestehende Reveal- und Roundtrip-Pfade für gespeicherte CMC-/TradFi-Werte
  werden entfernt. Server-seitige Tools lösen Credentials direkt aus dem Store
  auf; Browser erhalten nur `configured` und maskierte Metadaten.

## PBCoinData und weitere Consumer

- Ein gemeinsamer `CmcPoolClient` ersetzt `CoinData.api_key` und alle temporären
  Key-Swaps.
- Standalone PBCoinData, FastAPI Coin-Data-Jobs, Services-Key-Status und VPS-
  Checks verwenden denselben Cross-Process-Pool und dieselben lokalen Leases.
- Ein Host-weiter Single-Flight-Lock verhindert parallele identische Refreshes.
- PBCoinData kann ohne verfügbares Lease über das lokale Soft-Budget weiter CMC
  verwenden; vorhandene Caches bleiben zusätzlicher Fallback.
- Dynamic-Ignore-Gates prüfen `pool_ready` und mindestens einen aktiven lokal
  materialisierten Key, nicht Lease- oder INI-Key-Präsenz.
- Monitor-Agent und SSH-Fallback melden nur Pool-Generation, Key-Anzahl,
  Authority-Reachability und nicht-sensitive Usage-Metriken.
- Kein Collector liest oder konstruiert einen CMC-Key für Telemetrie.

## Migration

### Phase 0: Cluster-Sync-Sicherheitsbasis

1. Extern bestätigten signierten v2-Membership-Checkpoint erzeugen.
2. Signatur- und HPKE-Keys pro Node erzeugen und an autorisierte Membership
   binden.
3. Protokoll v2, vollständige Envelope-Signaturen, Recipient-Manifests,
   lückenlose State Vectors und transaktionale Bundles einführen.
4. Transitive Drei-Knoten-Relay-Tests ergänzen; direkte Master-Verbindungen
   dürfen weder vorausgesetzt noch für Recovery benötigt werden.
5. Capability Barrier einführen: Keine Sealed Secrets, solange ein aktiver
   Replica-Knoten nur Protokoll v1 unterstützt.

### Phase 1: Legacy-Replikation und Cutover-Schutz vorbereiten

1. TradFi aus neuen monolithischen `UPSERT_API_KEYS`-Secret-Blobs ausschließen.
2. Einen signierten v2-Checkpoint setzen, ab dem historische Klartext-Blob-
   Referenzen bei Replay und Rebuild nicht mehr nachgeladen werden.
3. Gemeinsamen PB7-Writer für Exchange-Bereich und reservierten TradFi-Subtree
   mit Lock, CAS und atomarem Merge einführen.
4. Einen clusterweiten Restart-/Migration-Blocker und zentrale Denylists für
   neue Legacy-CMC-/TradFi-Writes in INI, VPS-Inventar, Pending-State, alten
   API-Feldern und Playbooks vorbereiten.
5. Source-Generation-Marker ergänzen, damit während der späteren Inventur kein
   paralleler Legacy-Write unbemerkt bleibt.

### Phase 2: Lokaler Pool und Accounting im Shadow-Modus

1. Owner-only Credential-Store und `CmcPoolClient` implementieren.
2. Endpoint-Kosten, `credit_count`, Provider-Fehler, Rolling-Minute-Fenster und
   Reset-Felder auswerten.
3. Lokale WAL-Transaktion für Lease-Claim, Reservierung, In-flight und
   Settlement implementieren.
4. PBCoinData und manuelle Refreshes im Shadow-Modus auf den Pool-Client
   vorbereiten, ohne Legacy-Quellen schon zu löschen.

### Phase 3: Clusterweiter CMC-Katalog

1. CMC-Secret-Operationen und clusterweite Recipient-Wrappers implementieren.
2. Immutable Key-Generationen und `quota_domain_id` modellieren.
3. Node-Aktivierungsbarriere und Rewrap für später hinzugefügte Nodes
   implementieren.
4. Generation und Materialization pro Node bestätigen.
5. Add, Disable, Rotate, Tombstone und Parent-Konflikte testen.

### Phase 4: Quota-Domain-Allocator und Relay-Mailbox

1. Authority-Ledger und Outbox als eine transaktionale WAL implementieren.
2. Signierte Relay-Mailbox mit ACK, TTL und idempotentem
   `request_id -> lease_id` ergänzen.
3. Proaktive, in allen Quota-Dimensionen nicht überlappende Leases vergeben.
4. Lokale faire Key-Auswahl und konservatives Settlement aktivieren.
5. Partition, Authority-Restart, Epoch-Wechsel, Split-Brain-Reconciliation und
   Provider-429-Failover testen.

### Phase 5: Clusterweiter Writer-Freeze, Inventur und Import

1. Solange mindestens eine aktive State-Replica Protocol v2 nicht explizit mit
   vollstaendiger Crypto-Registrierung meldet oder ein laufender lokaler
   Credential-Consumer keinen frischen, zu PID-Startzeit, Service, Code-Serial
   und Capability-Generation passenden v2-Eintrag besitzt, bleibt der passive Status
   `waiting_for_upgrade`. Es gibt noch keinen Freeze, keine globale Inventur und
   kein Cleanup; deaktivierte, entfernte und nicht laufende Services zaehlen nicht
   zur Barriere. Der owner-only Registry-State wird atomar und gesperrt gepflegt;
   Crash-Reste werden ueber PID plus Create-Time erkannt.
2. Sobald die letzte aktive Replica v2 synchronisiert, startet ein beliebiger
   Master-Worker/API-Zyklus den vorbereiteten Write-Freeze mit eindeutiger
   Freeze-Generation auf allen
   aktiven Nodes über Relay-Sync aktivieren. Die API meldet währenddessen einen
   passiven Rolling-Migrationsstatus.
3. Von jedem aktiven Legacy-Writer einen signierten, restart-persistenten
   `WRITER_FREEZE_ACK` für exakt diese Generation abwarten. Ein nicht erreichbarer
   Node wird explizit aus der aktiven Membership entfernt oder die Migration
   bricht ab; ohne vollständige Barriere beginnt keine Inventur. Freeze- und
   Inventory-ACKs enthalten nur begrenzte, secret-freie Service-Readiness-Metadaten.
4. Source-Generationen aller Legacy-Stores erfassen.
5. Jeder Node inventarisiert seine lokalen INI-, VPS- und Pending-Werte selbst;
   nur signierte Migrationsergebnisse und Sealed Secrets laufen über Relays.
6. Gleiche CMC-Werte nur im Arbeitsspeicher deduplizieren und zufällige Key-IDs
   vergeben; Secret-Hashes weder persistieren noch loggen.
7. Bereits lokal aktive `legacy_shadow`-Records bei Import und Publikation mit
   derselben Credential-ID und Generation zu clusterverwalteten Records machen.
8. Unterschiedliche CMC-Werte als getrennte aktive Pool-Einträge erhalten.
9. Aktuelle TradFi-Werte aus PB7 `api-keys.json` und `[tradfi_profiles]`
   erfassen; Abweichungen als expliziten Konflikt behandeln.
10. Vor Commit prüfen, dass alle Source-Generationen unverändert sind. Bei einer
   Abweichung Migration abbrechen und keine Legacy-Quelle löschen.

### Phase 6: TradFi trennen

1. TradFi als `masters`-Audience an alle Master verteilen und opaque Relay über
   VPS verifizieren.
2. Die Master-only Generation von allen aktiven Mastern materialisieren lassen;
   VPS bestätigen nur Ciphertext-Relay und `not_recipient`.
3. Nach vollständigem ACK das aktive Profil atomar setzen.
4. Market-Data-Reader auf den Vault umstellen.
5. Reservierten PB7-TradFi-Subtree mit persistentem Projection-Retry aktivieren.
6. Alle aktiven Master bestätigen Reader und Projektion; VPS bestätigen nur
   Ciphertext-Relay und `not_recipient`.
7. Optionale Provider-Rotation als separaten Admin-Vorgang anbieten, aber den
   Cutover nicht davon abhängig machen.

### Phase 7: CMC-Replikation und Cutover

1. Jeden CMC-Key an jeden `credential_active` Master und VPS replizieren.
2. Materialization-ACKs der exakten Generation aller aktiven Nodes prüfen.
3. Importierte, neue und extern mitgenutzte CMC-Keys nach lokaler Basisprüfung
   direkt auf `active` setzen. Ihre Nutzung hängt nicht von Authority-
   Erreichbarkeit oder der ersten Lease-Zuteilung ab.
4. Authorities pro Quota-Domain und erste kleine Leases aktivieren.
5. PBCoinData, Services, Coin Data, VPS Manager, Monitor, PBRun und V7-Gates
   gleichzeitig auf Pool-Metadaten umstellen.
6. Mixed-Version-Betrieb ab diesem Punkt blockieren.

### Phase 8: Klartext-Altlasten entfernen und Freeze lösen

1. `api_key` und Usage-Felder atomar aus `[coinmarketcap]` entfernen.
2. `[tradfi_profiles]` entfernen.
3. `coinmarketcap_api_key` aus VPS-Inventar und Pending-State entfernen.
4. CMC-Key-Felder aus API-, WebSocket- und Browser-Payloads entfernen.
5. CMC-Ansible-Variablen und INI-Schreib-Tasks entfernen.
6. Remote Collector und SSH-Fallback secret-frei machen.
7. Alle aktiven Nodes auf bekannte Test-Secrets und Legacy-Felder scannen.
8. Historische Klartext-Cluster-Blobs nach v2-Checkpoint und Replica-Bestätigung
   entfernen; Provider-Rotation ist keine Cleanup-Voraussetzung.
9. Zentrale Legacy-Denylists dauerhaft aktiv lassen, damit alte Writer Secrets
   nicht erneut in INI, Inventar, Pending-State oder Legacy-Responses anlegen
   können. Neue Credential-Create/Rotate-Bodies bleiben erlaubt.
10. Downgrade auf Versionen blockieren, die INI-Secrets oder Klartext-Cluster-
    Blobs verlangen.
11. Writer-Freeze erst nach erfolgreicher clusterweiter Abschlussprüfung lösen.
12. Nach erfolgreichem Cutoff nicht uebernommene `legacy_shadow`-Records
    deaktivieren, damit kein lokaler Shadow-Pfad Desired-State-Regeln umgeht.

## Rollback

- Vor dem Cutover werden owner-only Legacy-Backups unter dem Credential-Store
  angelegt, nie im allgemeinen Migration-State.
- Solange nicht alle Nodes Pool-Capability und Materialization bestätigt haben,
  werden alte Quellen nicht gelöscht.
- Nach dem Cutover erfolgt Rollback ausschließlich aus dem Vault in die neue
  Pool-Runtime, nicht zurück in `pbgui.ini`.
- Ein fehlgeschlagenes Lease-System fällt auf lokales Soft-Budget plus Cache
  zurück und schreibt keine Einzelkeys in die INI.
- Secrets erscheinen weder in Logs, URLs, Prozessargumenten, Telemetrie noch
  Fehlerpayloads.

## Verbindliche Tests und Done-Kriterien

- Master A synchronisiert CMC und TradFi über einen VPS-Relay zu Master B, ohne
  direkte Master-Verbindung.
- Operationen und Ciphertexts laufen transitiv in beide Richtungen.
- CMC lässt sich auf Master und VPS entschlüsseln.
- TradFi lässt sich nur auf Mastern entschlüsseln; VPS meldet `not_recipient`.
- Relay-Weiterleitung ruft keine Decryption auf.
- Forged Actor, falsche Signatur, Replay, Sequence Gap und unautorisierte
  Membership-Änderung werden abgelehnt.
- Jeder aktive Node materialisiert jeden aktiven CMC-Key in exakter Generation.
- Ein Node kann erst nach vollständigem Rewrap, Materialization und ACK
  `credential_active` werden.
- Parallel laufende Prozesse eines Nodes verlieren keine Reservierungen.
- Authority-Ledger, Duplicate-Request-Mapping, Budgetreservierung und Outbox
  überstehen Crashs als eine Transaktion.
- Bei stabiler Authority überlappen Leases verschiedener Nodes weder Tages-/
  Monatscredits, Rolling-Minute-Tokens noch Concurrent-Slots derselben
  Quota-Domain.
- Timeout, malformed 200, Retry und fehlendes Settlement werden konservativ
  verbucht.
- Nodes wählen lokal fair zwischen allen Keys; gültige Leases werden bevorzugt,
  fehlende Leases blockieren die CMC-Nutzung nicht.
- Exhausted, invalid, disabled und conflicted Keys werden automatisch
  übersprungen.
- Authority-Epochs konvergieren nach Wiederverbindung; mögliche kurzzeitige
  Doppelvergabe während einer Partition wird über kleine Leases, Usage-
  Reconciliation und Provider-429 begrenzt.
- Importierte und extern verwendete Keys nehmen am Pool teil und aktualisieren
  ihre Schätzung über regelmäßige Provider-Snapshots.
- Rotation wartet auf Materialization und offene Leases.
- API und Browser erhalten niemals gespeicherte Secret-Werte.
- Kein CMC- oder TradFi-Secret verbleibt in `pbgui.ini`.
- Kein CMC-Secret verbleibt in VPS-Inventar, Pending-State, Ansible-Variablen
  oder Monitor-Payloads.
- Nur der reservierte TradFi-Subtree in PB7 `api-keys.json` ist eine verifizierte
  Runtime-Projektion; parallele Exchange-Änderungen bleiben erhalten.
- Neue Cluster-Sync-Generationen verteilen TradFi nur noch als Master-Secret;
  Provider-Rotation bleibt eine optionale Admin-Aktion.
- Ein allowlist-bewusster Migrationstest scannt alle verbotenen Dateien, Logs,
  Payloads, Prozessargumente, VPS-TradFi-Pfade, INI, Inventar und Legacy-Blobs
  auf bekannte Test-Secrets. Erwarteter Klartext ist nur in `0600`-Credential-
  Dateien berechtigter Empfänger, im reservierten PB7-TradFi-Subtree eines
  Masters und in zeitlich begrenzten owner-only Migration-Backups erlaubt.
