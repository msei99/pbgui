# PBv8 Run

PBv8 Run verwaltet Passivbot-V8-Live-Instanzen. PB7 und PB8 Run verwenden dasselbe Editor-Template; ein Versionsadapter bildet die sichtbaren Felder auf die passenden Config-Pfade und API-Vertraege ab.

## Run-Liste

Unter **PBv8 -> Run** werden Configs aus `data/run_v8` angezeigt. PB7 und PB8 verwenden dieselbe responsive Run-Listenansicht mit Sidebar-Suche und Statusfiltern. Die Tabelle zeigt Exchange-User, Zielhost, Config- und laufende Version, Exposure-Uebersicht, bestaetigte Laufzeit-Hosts, Notiz und PBCluster-Sollzustand.

Die Hauptspalte **Status** kombiniert den veroeffentlichten Sollzustand mit exakten PB8-Prozessbeobachtungen des lokalen Runners und der Remote-Monitore:

- **synced** bedeutet, dass der exakt zugeordnete PB8-Prozess mit der aktuellen Config-Version laeuft.
- **outdated**, **sync needed** und **stop needed** kennzeichnen eine bestaetigte Abweichung zwischen Runtime und Config.
- **collecting** bedeutet, dass noch keine exakte Prozessbeobachtung vorliegt; PBGui behauptet dann nicht, der Bot sei gestoppt.
- **disabled** bedeutet, dass das Ziel deaktiviert ist und kein laufender Prozess gemeldet wurde.
- **conflicted** kennzeichnet konkurrierende Cluster-Operationen, die aufgeloest werden muessen.

Die getrennte Spalte **Desired** zeigt weiterhin die veroeffentlichte Cluster-Anforderung. Der authentifizierte WebSocket aktualisiert beide Ansichten; veraltete REST-Antworten koennen neueren Socket-Status nicht ueberschreiben.

## Erstellen Oder Bearbeiten

Der Editor bietet denselben Arbeitsablauf wie PBv7 Run:

- **User**, **Enabled on**, **Config version** und **Note** verwalten Deployment-Identitaet und PBGui-Metadaten. Wie bei PB7 ist der ausgewaehlte User zugleich der Instanzname; PBGui lehnt eine zweite Live-Instanz oder einen abweichenden Namen fuer denselben Exchange-User ab.
- **strategy_kind** wird aus den Metadaten der installierten PB8-Runtime befuellt und steht am Anfang von **Bot Configuration**. Ein Wechsel ersetzt sofort die aktiven Schluessel unter `bot.long.strategy` und `bot.short.strategy`, stellt beim Zurueckwechseln zuvor bearbeitete Werte wieder her oder laedt fuer eine noch nicht konfigurierte Strategie die Runtime-Defaults. Die Synchronisierung arbeitet in beide Richtungen: Wird in Long- oder Short-JSON genau ein unterstuetzter Strategie-Schluessel eingetragen, aktualisiert PBGui auch `strategy_kind` und die jeweils andere Seite. Aus Runtime-Defaults erzeugte Strategie-Bloecke werden bis zur Bearbeitung rot als **review** markiert.
- Die normalen Felder behalten von User und Enabled on bis zu den Execution-Flags die gewohnte PB7-Reihenfolge.
- Coin-Filter, Approved-/Ignored-Listen und Coin-Statuspruefung verwenden dieselben aktuellen CoinData-Mappings wie PBv7.
- **Coin Overrides** unterstuetzt eingebettete und separate sparse Override-Dateien. Referenzierte Dateien werden gemeinsam mit der Config als exaktes Bundle gespeichert.
- Long-/Short-Exposure und Positionsanzahl werden auf `bot.<side>.risk` abgebildet; die vollstaendigen verschachtelten Side-Configs bleiben als JSON editierbar.
- Die normalen Felder und **Advanced Settings** zeigen alle von der installierten PB8-Runtime gemeldeten Live-, Logging- und Monitoring-Parameter. PB8-spezifische Felder fuer Fees, Order-Churn, WebSocket-Forager, Startup, Logging und Monitoring werden automatisch ausgeblendet, wenn eine aeltere Runtime sie nicht anbietet.
- **Advanced Settings** enthaelt fuer `hsl_signal_mode` die PB8-Auswahlwerte `coin`, `pside` und `unified`; der Vorgabewert des installierten Templates bleibt beim Oeffnen und Speichern erhalten.
- Strukturierte Felder, Long-/Short-JSON und Raw JSON werden in beide Richtungen synchronisiert. Numerische Nullwerte, nullable Auto-Werte, unbekannte Runtime-Felder sowie unbekannte verschachtelte oder Top-Level-JSON-Werte bleiben erhalten und werden nicht durch Editor-Defaults ersetzt.
- **Additional Parameters** ist fuer neu eingefuehrte Runtime-Live-Felder ohne eigenes Bedienelement reserviert. Sie bleiben editierbar und werden beim Speichern erhalten.
- **Raw JSON** bleibt mit den strukturierten Feldern synchron und erhaelt unbekannte Top-Level- und verschachtelte Felder.

Import, Copy, Backtest-Uebergabe, Live-Logs und Raw-JSON-Bearbeitung stehen im gleichen Sidebar-Ablauf bereit. Der Import-Dialog bietet durchsuchbare User-Vorschlaege und weist Namen ausserhalb des konfigurierten Exchange-User-Katalogs ab. **Balance Calculator** oeffnet den gemeinsamen Rechner mit der aktuellen ungespeicherten Config; **Calc Balance** berechnet die Empfehlung direkt und kann sie als `balance_override` uebernehmen. Browser-Anfragen verwenden das HttpOnly-PBGui-Session-Cookie; der Editor rendert kein Session-Token.

Jedes Speichern verwendet PB8s installierte Prepare-/Save-Pipeline. PBGui prueft die erwartete Editor-Version, ersetzt das vollstaendige Config-und-Override-Verzeichnis atomar unter einem prozessuebergreifenden Lock, veroeffentlicht ein unveraenderliches Manifest und haengt eine explizite `UPSERT_PB8_CONFIG`-Operation an. Falls Operation oder lokale Platzierung fehlschlagen, bleibt das vorherige lokale Bundle erhalten oder wird wiederhergestellt.

## Backups

PBv8 Run verwendet denselben **Backups**-Ablauf wie PBv7. Bevor eine bestehende Instanz ueberschrieben oder geloescht wird, speichert PBGui das vollstaendige vorherige Bundle unter `data/backup/v8`: `config.json` und alle referenzierten sparse Override-Dateien. Die Retention-Einstellung bestimmt, wie viele Versionen pro Instanz erhalten bleiben.

Beim Oeffnen eines Backups wird ein kurzlebiger Editor-Draft erzeugt. Nach der Pruefung stellt die normale Save-Aktion das Backup ueber PB8-Validierung, optimistische Versionsbehandlung, atomare Bundle-Speicherung und Cluster-Veroeffentlichung wieder her. Das Loeschen eines Backups betrifft nur dieses unveraenderliche Backup-Bundle.

PBRun ueberwacht PB7 und PB8 mit demselben Controller-Dienst. Ein Neustart dieses Controllers stoppt bereits laufende Bots nicht; nach dem Start uebernimmt er passende Prozesse erneut. Explizites Deaktivieren, Verschieben, Loeschen, Runtime-Profilwechsel und Cluster-Tombstones stoppen den betroffenen Bot weiterhin.

## Zulaessige Hosts

Die Zielauswahl ist fail-closed. Ein Host erscheint nur, wenn eine dieser Quellen seine PB8-Faehigkeit bestaetigt:

- Der lokale `pb8_runtime_status` ist bereit.
- VPS Manager meldet das Runtime-Profil `pb8` oder `pb7_pb8` und ein erfolgreiches Setup.
- Ein nicht verwalteter Remote-Host meldet ueber frische Host-Metadaten einen `pb8ready`-Wert.

Reine PB7-, nicht bereite, veraltete und unbekannte neue Ziele werden mit HTTP 409 abgelehnt. Ein unveraendertes unbekanntes Ziel aus einer aelteren gespeicherten Config darf erhalten bleiben, damit die Config ohne erzwungenen unsicheren Umzug bearbeitet werden kann; fuer ein neues Deployment kann es nicht ausgewaehlt werden.

## Cluster-Rollout

PB8-Live-Operationen verwenden einen getrennten Cluster-Protokoll-Namespace, damit aeltere Nodes sie niemals als PB7-Configs interpretieren koennen. Vor dem ersten PB8-Speichern oder -Loeschen muessen alle aktiven Cluster-State-Replikas auf eine PBGui-Version aktualisiert werden, die `pb8_instances_v1` meldet. Danach muss ein frischer erfolgreicher Cluster-Sync-Lauf abgewartet werden. Bis dahin lehnt die API PB8-Veroeffentlichungen mit HTTP 409 ab.

## Loeschen

Loeschen veroeffentlicht `DELETE_PB8_INSTANCE`, bevor das lokale Bundle entfernt wird. PB8-Tombstones sind von PB7-Tombstones getrennt, sodass gleichnamige PB7- und PB8-Instanzen einander nicht beeinflussen. Cluster Sync und PBRun verwenden den Tombstone, um das PB8-Deployment zu stoppen und zu entfernen.
