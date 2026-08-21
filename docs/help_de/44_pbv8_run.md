# PBv8 Run

PBv8 Run verwaltet Passivbot-V8-Live-Instanzen. PB7 und PB8 Run verwenden dasselbe Editor-Template; ein Versionsadapter bildet die sichtbaren Felder auf die passenden Config-Pfade und API-Vertraege ab.

## Run-Liste

Unter **PBv8 -> Run** werden Configs aus `data/run_v8` angezeigt. PB7 und PB8 verwenden dieselbe responsive Run-Listenansicht mit Sidebar-Suche und Statusfiltern. Die Tabelle zeigt die aktive PB8-Strategie, Exchange-User, Zielhost, Config- und laufende Version, Exposure-Uebersicht, bestaetigte Laufzeit-Hosts, Notiz und PBCluster-Sollzustand. Strategy ist sortierbar und Teil der Listensuche.

Die Hauptspalte **Status** kombiniert den veroeffentlichten Sollzustand mit exakten PB8-Prozessbeobachtungen des lokalen Runners und der Remote-Monitore:

- **synced** bedeutet, dass der exakt zugeordnete PB8-Prozess mit der aktuellen Config-Version laeuft.
- **outdated**, **sync needed** und **stop needed** kennzeichnen eine bestaetigte Abweichung zwischen Runtime und Config.
- **blocked** meldet einen konkreten Cluster-Gate- oder PB8-Runtime-Fehler. Nur wenn PBRun meldet, dass die validierte PB8-Runtime nicht bereit ist, zeigt die Run-Liste fuer den betroffenen Host den Hinweis **Open VPS Manager -> Update PB8**. Cluster-Gates und normale Prozess-Exit-Fehler zeigen diesen Update-Hinweis nicht. PBRun versucht den Start nach dem validierten Update erneut.
- **PB8 update required** bedeutet, dass der lokale Master wegen einer fehlgeschlagenen Readiness-Pruefung seiner eigenen PB8-Runtime keine PB8-Config laden kann. Das dauerhafte Banner und die Statuszelle zeigen den exakten sicheren Runtime-Grund und verlinken direkt auf **VPS Manager -> Update PB8**. Ein Fehler in nur einer Config bleibt **config error** und zeigt den eigenen Loader-Grund dieser Config, statt faelschlich ein Runtime-Update anzufordern.
- **collecting** bedeutet, dass noch keine exakte Prozessbeobachtung vorliegt; PBGui behauptet dann nicht, der Bot sei gestoppt.
- **disabled** bedeutet, dass das Ziel deaktiviert ist und kein laufender Prozess gemeldet wurde.
- **conflicted** kennzeichnet konkurrierende Cluster-Operationen, die aufgeloest werden muessen.

Die getrennte Spalte **Desired** zeigt weiterhin die veroeffentlichte Cluster-Anforderung. Der authentifizierte WebSocket aktualisiert beide Ansichten; veraltete REST-Antworten koennen neueren Socket-Status nicht ueberschreiben.

Die Zeilenaktionen **P**, **G** und **T** setzen nach einer expliziten Bestaetigung global `panic`, `graceful_stop` oder `tp_only` fuer Long- und Short-PB8-Positionen. Jede Aktion verwendet die normale PB8-Bundle-Pipeline: Sie erstellt ein vollstaendiges Backup, erhoeht die Config-Version, validiert Config und Sparse Overrides durch PB8, veroeffentlicht die Cluster-Operation und versucht die sofortige Aktivierung auf dem Zielhost.

## Erstellen Oder Bearbeiten

Der Editor bietet denselben Arbeitsablauf wie PBv7 Run:

- **User**, **Enabled on**, **Config version** und **Note** verwalten Deployment-Identitaet und PBGui-Metadaten. Wie bei PB7 ist der ausgewaehlte User zugleich der Instanzname; PBGui lehnt eine zweite Live-Instanz oder einen abweichenden Namen fuer denselben Exchange-User ab.
- **strategy_kind** wird aus den Metadaten der installierten PB8-Runtime befuellt und steht am Anfang von **Bot Configuration**. Ein Wechsel ersetzt sofort die aktiven Schluessel unter `bot.long.strategy` und `bot.short.strategy`, stellt beim Zurueckwechseln zuvor bearbeitete Werte wieder her oder laedt fuer eine noch nicht konfigurierte Strategie die Runtime-Defaults. Die Synchronisierung arbeitet in beide Richtungen: Wird in Long- oder Short-JSON genau ein unterstuetzter Strategie-Schluessel eingetragen, aktualisiert PBGui auch `strategy_kind` und die jeweils andere Seite. Aus Runtime-Defaults erzeugte Strategie-Bloecke werden bis zur Bearbeitung rot als **review** markiert.
- Die normalen Felder behalten von User und Enabled on bis zu den Execution-Flags die gewohnte PB7-Reihenfolge.
- Approved-/Ignored-Listen verwenden PB8s offiziellen Marktresolver. Normale Maerkte bleiben kurz; echte Kollisionen wie `CAT` und `1000CAT` werden mit kurzen Labels gezeigt, aber mit PB8s exaktem Exchange-qualifiziertem Identifier gespeichert. Importierte native IDs, CCXT-Symbole und Namespaced-Identifier bleiben unveraendert; ungueltige oder mehrdeutige Werte bleiben zur Korrektur sichtbar.
- **Apply Filters** ist eine explizite Sidebar-Aktion statt einer einmaligen Checkbox. Coin-Filter verwenden weiterhin PBGui-CoinData-Regeln, projizieren jedes aufgeloeste Ergebnis aber auf PB8s kollisionssicheren Marktkatalog; nicht verfuegbare Eintraege werden gemeldet und uebersprungen, waehrend gueltige Listen erhalten bleiben.
- `dynamic_ignore` wird deaktiviert als PB7-only Runtime-Funktion angezeigt. PB8s Supervisor ueberwacht die dynamischen PB7-Listendateien nicht; PB8 Run verwendet deshalb die expliziten Listen aus Apply Filters und speichert keinen wirkungslosen Schalter.
- **Coin Overrides** unterstuetzt eingebettete und separate sparse Override-Dateien. Exakte PB8-Marktschluessel bleiben getrennt; referenzierte Dateien werden gemeinsam mit der Config als exaktes Bundle gespeichert.
- Die Coin-Override-Auswahl stammt aus PB8s offizieller Policy fuer die aktive Kombination aus `strategy_kind` und `hsl_signal_mode`. HSL-Felder werden nur im Modus `coin` angeboten. Explizites `false`, Null als Zahl und Default-Werte bleiben sparse Overrides; JSON-`null` ist ungueltig und ein fehlender Key bedeutet Vererbung. Beim Save prueft PB8s Runtime-Parser Inline- und referenzierte Datei-Overrides.
- Long-/Short-Exposure und Positionsanzahl werden auf `bot.<side>.risk` abgebildet; die vollstaendigen verschachtelten Side-Configs bleiben als JSON editierbar.
- Die normalen Felder und **Advanced Settings** zeigen alle von der installierten PB8-Runtime gemeldeten Live-, Logging- und Monitoring-Parameter. PB8-spezifische Felder fuer Fees, Order-Churn, WebSocket-Forager, Startup, Logging und Monitoring werden automatisch ausgeblendet, wenn eine aeltere Runtime sie nicht anbietet.
- **Advanced Settings** enthaelt fuer `hsl_signal_mode` die PB8-Auswahlwerte `coin`, `pside` und `unified`; der Vorgabewert des installierten Templates bleibt beim Oeffnen und Speichern erhalten.
- Strukturierte Felder, Long-/Short-JSON und Raw JSON werden in beide Richtungen synchronisiert. Numerische Nullwerte, nullable Auto-Werte, unbekannte Runtime-Felder sowie unbekannte verschachtelte oder Top-Level-JSON-Werte bleiben erhalten und werden nicht durch Editor-Defaults ersetzt.
- **Additional Parameters** ist fuer neu eingefuehrte Runtime-Live-Felder ohne eigenes Bedienelement reserviert. Sie bleiben editierbar und werden beim Speichern erhalten.
- PB8.1 besitzt eigene Felder fuer WebSocket-Forager-Candles, `exchange_symbol_unavailable_cooldown_hours`, die vier Order-Replacement-Churn-Gate-Werte und Expert-/Diagnosewerte unter `startup_phase_budgets`. Startup-Budgets beeinflussen nur das Reporting und sind kein Trading Gate. Beim Oeffnen normalisiert PB8 eine v8.0-Config nur im Speicher; die Quelldatei aendert sich erst beim expliziten Save.
- **Raw JSON** bleibt mit den strukturierten Feldern synchron und erhaelt unbekannte Top-Level- und verschachtelte Felder.

Import, Copy, Backtest-Uebergabe, **Strategy Explorer**, Live-Logs und Raw-JSON-Bearbeitung stehen im gleichen Sidebar-Ablauf bereit. Strategy Explorer erhaelt die aktuelle ungespeicherte PB8-Config und alle referenzierten Sparse Overrides ueber einen authentifizierten opaken Draft. Der Import-Dialog bietet durchsuchbare User-Vorschlaege und weist Namen ausserhalb des konfigurierten Exchange-User-Katalogs ab. **Balance Calculator** oeffnet den gemeinsamen Rechner mit der aktuellen ungespeicherten Config; **Calc Balance** berechnet die Empfehlung direkt und kann sie als `balance_override` uebernehmen. Browser-Anfragen verwenden das HttpOnly-PBGui-Session-Cookie; der Editor rendert kein Session-Token.

Waehlt ein neuer Backtest-/Archive-Handoff-Draft einen User mit bereits vorhandener PB8-Run-Config, laedt Save die verbindliche aktuelle Version und fragt vor dem Ersetzen nach. Die Bestaetigung sichert das vorhandene Bundle, erhoeht dessen aktuelle Version und synchronisiert das ausgewaehlte `enabled_on`-Ziel; Abbrechen laesst die bestehende Instanz unveraendert.

Jedes Speichern verwendet PB8s installierte Prepare-/Save-Pipeline. PBGui prueft die erwartete Editor-Version, ersetzt das vollstaendige Config-und-Override-Verzeichnis atomar unter einem prozessuebergreifenden Lock, veroeffentlicht ein unveraenderliches Manifest und haengt eine explizite `UPSERT_PB8_CONFIG`-Operation an. Eine laufende Remote-Zuordnung wird als einzelnes Cluster-Bundle mit einem Transportlimit von drei Sekunden direkt an den Zielhost gesendet; falls diese schnelle Aktivierung nicht abgeschlossen werden kann, bleibt PBCluster der dauerhafte Wiederholungspfad. PBRun prueft PB8-Sollzustand und Config-Signaturen jede Sekunde, damit eine erfolgreiche Materialisierung sofort verarbeitet wird. Falls Operation oder lokale Platzierung fehlschlagen, bleibt das vorherige lokale Bundle erhalten oder wird wiederhergestellt.

## Backups

PBv8 Run verwendet denselben **Backups**-Ablauf wie PBv7. Bevor eine bestehende Instanz ueberschrieben oder geloescht wird, speichert PBGui das vollstaendige vorherige Bundle unter `data/backup/v8`: `config.json` und alle referenzierten sparse Override-Dateien. Die Retention-Einstellung bestimmt, wie viele Versionen pro Instanz erhalten bleiben.

Beim Oeffnen eines Backups wird ein kurzlebiger Editor-Draft erzeugt. Nach der Pruefung stellt die normale Save-Aktion das Backup ueber PB8-Validierung, optimistische Versionsbehandlung, atomare Bundle-Speicherung und Cluster-Veroeffentlichung wieder her. Das Loeschen eines Backups betrifft nur dieses unveraenderliche Backup-Bundle.

PBRun ueberwacht PB7 und PB8 mit demselben Controller-Dienst. Ein Neustart dieses Controllers stoppt bereits laufende Bots nicht; nach dem Start uebernimmt er passende Prozesse erneut. Explizites Deaktivieren, Verschieben, Loeschen, Runtime-Profilwechsel und Cluster-Tombstones stoppen den betroffenen Bot weiterhin.

## Zulaessige Hosts

Die Zielauswahl ist fail-closed. Ein Host erscheint nur, wenn eine dieser Quellen seine PB8-Faehigkeit bestaetigt und sein gemeldetes `pb8_config_schema` mindestens so neu wie `config_version` der aktuellen Config ist:

- Der lokale `pb8_runtime_status` ist bereit.
- VPS Manager meldet das Runtime-Profil `pb8` oder `pb7_pb8` und ein erfolgreiches Setup.
- Ein nicht verwalteter Remote-Host meldet ueber frische Host-Metadaten einen `pb8ready`-Wert.

Reine PB7-, nicht bereite, veraltete, schema-inkompatible und unbekannte neue Ziele werden mit HTTP 409 abgelehnt. Eine `v8.1.0`-Config kann zum Beispiel keinen Host verwenden, der nur Schema `v8.0.0` meldet; zuerst muss PB8 auf diesem Host aktualisiert werden. Ein unveraendertes unbekanntes Ziel aus einer aelteren gespeicherten Config darf erhalten bleiben, damit die Config ohne erzwungenen unsicheren Umzug bearbeitet werden kann; fuer ein neues Deployment kann es nicht ausgewaehlt werden.

## Cluster-Rollout

PB8-Live-Operationen verwenden einen getrennten Cluster-Protokoll-Namespace, damit aeltere Nodes sie niemals als PB7-Configs interpretieren koennen. Vor dem ersten PB8-Speichern oder -Loeschen muessen alle aktiven Cluster-State-Replikas auf eine PBGui-Version aktualisiert werden, die `pb8_instances_v1` meldet. Danach muss ein frischer erfolgreicher Cluster-Sync-Lauf abgewartet werden. Bis dahin lehnt die API PB8-Veroeffentlichungen mit HTTP 409 ab.

## Loeschen

Loeschen veroeffentlicht `DELETE_PB8_INSTANCE`, bevor das lokale Bundle entfernt wird. PB8-Tombstones sind von PB7-Tombstones getrennt, sodass gleichnamige PB7- und PB8-Instanzen einander nicht beeinflussen. Cluster Sync und PBRun verwenden den Tombstone, um das PB8-Deployment zu stoppen und zu entfernen.
