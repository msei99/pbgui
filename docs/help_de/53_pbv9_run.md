# PB9 Run

PB9 ist optional und experimentell. Ein PBGui-Update installiert oder startet
PB9 nicht. Aktualisiere vor dem Speichern einer aktivierten PB9-Instanz im
bestehenden Cluster PBGui und die laufenden Cluster-Prozesse aller aktiven
Zustandsreplikate und warte auf deren PB9-Fähigkeitsbestätigung. Das gilt auch
für die Zuweisung zum lokalen Master. Nur PB9-Ausführungshosts benötigen eine
Passivbot-9-Installation. Ältere PBGui-Versionen ohne PB9-Unterstützung können
den gemeinsamen Operationsstrom danach nicht lesen. Die praktische Remote-/
Live-Trading-Abnahme steht noch aus; siehe den im Repository-README verlinkten
PB9-Entwicklungsstand.

Ein Config-Import verwirft zwischengespeicherte Strategiewerte der vorherigen Config. Raw-Änderungen am Konto oder Strategiebaum leeren diesen Cache ebenfalls; andere Raw-Änderungen behalten ihn. Beim Strategiewechsel innerhalb derselben Config bleiben Änderungen erhalten. Deaktivierung und destruktive Änderungen sind mit lokalen Prozessstarts koordiniert.

Unter **PBv9 → Run** verwaltest du PB9-Live-Instanzen. Liste und Editor verwenden dieselbe Oberfläche wie PB7/PB8 Run. PB9 hat einen eigenen Quellbaum, eine eigene Python-Umgebung sowie getrennte Instanz- und Backup-Verzeichnisse.

## Instanz konfigurieren

Die grundlegenden Live-Felder bleiben wie bei PB8 sichtbar. Advanced Settings bündelt die übrigen Live-, Logging- und Monitoring-Felder. PB9 zeigt seine eigenen Feldnamen und Einheiten. Insbesondere ist risk_bootstrap_max_lookback_minutes eine eigenständige Startup-Einstellung und kein Ersatz mit gleicher Bedeutung für den entfernten PB8-PnL-Rückblick.

Long- und Short-JSON verwenden den gemeinsamen Editor. **With global defaults** zeigt die geerbten Seiteneinstellungen; nur geänderte Werte werden zu expliziten Overrides. **Overrides only** zeigt die tatsächlich gespeicherte, sparsame Seitenkonfiguration. Lösche dort eine Eigenschaft, um wieder zu erben. Unveränderte Standardwerte bleiben beim Speichern ausgelassen; vorhandene explizite Werte bleiben erhalten. Die Seitenaktivierung ist immer explizit. Die Ansichtsauswahl bleibt beim Neuladen erhalten.

Wähle **Add PB9 Instance**, ein Börsenkonto und bearbeite Strategie, Portfolio, globale Vorgaben sowie Long-/Short-Overrides. Das Formular liest Felddefinitionen und Strategiemetadaten der installierten PB9-Version. Geerbte Seitenwerte bleiben geerbt, bis du ausdrücklich einen Override setzt. Aktiviere die gewünschten Seiten ausdrücklich.

Die Coin-Auswahl verwendet den gemeinsamen PB8-Katalogablauf mit dem nativen PB9-Resolver und vorhandenen lokalen öffentlichen Marktmetadaten. Normale Coins erscheinen einmal unter ihrem Namen; exakte Kennungen unterscheiden tatsächlich mehrdeutige Märkte. Bereits gewählte exakte Kennungen behalten ihre Identität. Apply Filters verwendet dieselbe Filterzuordnung wie PB8. Coin-Overrides sind eingebettete PB9-Patches. Dateibasierte Overrides und externe Konfigurationsverweise werden abgelehnt.

Der Katalog verwendet die native Standard-Abrechnungswährung der Börse: USDT für die üblichen Börsen und USDC für Hyperliquid/Lighter. Hyperliquid-HIP-3-Namensräume und numerische native Marktkennungen bleiben unterscheidbar.

Save erzeugt eine neue Konfigurationsversion. Bei vorhandenen Instanzen muss die geladene Version noch aktuell sein; konkurrierende Änderungen werden abgelehnt. PBGui speichert Laufzeit, Version, Notiz und Host-Zuordnung in einer privaten Begleitdatei. Die native Konfiguration enthält ausschließlich Passivbot-Felder. Für fortgeschrittene Bearbeitung gibt es Raw JSON.

Felder aus der installierten Vorlage oder geladenen Konfiguration ohne eigenes Bedienelement erscheinen unter **Advanced Settings → Additional Parameters**. Unbearbeitete Vorgaben bleiben geerbt. Bearbeitung und Entfernen expliziter Werte erhalten native Datentypen und Vererbung.

## Lokaler Betrieb und Cluster

**Enabled on → disabled** speichert ohne geplanten Trading-Start. Mit Auswahl dieses PBGui-Hosts übernimmt der PBRun-Daemon die Instanz. Voraussetzung sind eine funktionierende separate PB9-Installation, eine aktuelle Rust-Erweiterung, verfügbare Kontozugangsdaten und der laufende PBRun-Dienst. Eine konkurrierende PB7-/PB8-/PB9-Zuordnung desselben Kontos blockiert die Aktivierung. Speichern umgeht diese Startprüfungen nicht.

Starte nach dem PBGui-Update die lokalen Dienste API und PBRun neu, damit sie den neuen Code laden. Die einzelnen Dienste findest du unter **System → PBGUI Services**. Die Neustartanzeige nennt Dienste, die noch mit einer älteren Code-Serial laufen.

**Enabled on** bietet auch beigetretene, erreichbare Cluster-Hosts mit aktuellen PB9-Laufzeit- und Schema-Metadaten an. Vor dem Aktivieren oder Verschieben müssen alle aktiven Zustandsreplikate das PB9-Protokoll bestätigen. Installiere beziehungsweise aktualisiere PBGui und die separate PB9-Laufzeit auf dem Ziel und schließe zuerst Cluster Join ab. Fehlende, alte oder inkompatible Metadaten halten das Ziel gesperrt; die vorhandene automatische Überwachung aktualisiert die Auswahl.

Beim Speichern einer verwalteten Instanz werden eine signierte PB9-Operation und ein unveränderliches Manifest aus nativer Config und Begleitdatei über denselben Cluster-Transport wie bei PB8 veröffentlicht. Die Synchronisierung kann bis zur automatischen Abgleichrunde ausstehen. PBRun prüft Zielknoten, vollständiges Manifest und Version vor dem Start. PB8- und PB9-Zustände bleiben auch bei gleichen Namen getrennt. Bestehende lokale PB9-Instanzen bleiben lokal, bis sie ausdrücklich in den Cluster gespeichert werden. Ein replizierter Name allein darf eine andere lokale Konfiguration nicht ersetzen; die Übernahme muss zu deren genauem Vorgängermanifest passen. Bereits verwaltete Instanzen lassen sich auch bei einem unerreichbaren Peer deaktivieren; eine ausstehende Synchronisierung bestätigt keinen Remote-Stopp.

Backtest ist über den gleichnamigen Editor-Button angebunden. Strategy Explorer bleibt offen. Eine geänderte Host-Zuordnung kann Trading starten oder stoppen; wähle nur beabsichtigte Konten und Einstellungen.

Deaktiviere eine bestehende Instanz vor dem Verschieben auf einen anderen Host und warte auf den frisch bestätigten Stopp. Die deaktivierte Config bleibt dafür zunächst dem bisherigen Host zugeordnet.

## Backups und Logs

Vorherige gespeicherte Versionen werden einschließlich PBGui-Begleitdatei gesichert. Öffne ein Backup als Editor-Entwurf oder stelle es als neue Version wieder her. Wiederherstellungen sind zunächst deaktiviert. Deaktiviere und stoppe die Instanz vor dem Löschen. Bei Remote-Instanzen muss zusätzlich ein frischer PB9-fähiger Snapshot ausdrücklich den gestoppten Zustand melden; alte Cache-Daten erlauben keine Bereinigung. Das gemeinsame Log-Panel nutzt die native PB9-Ausgabe des zuletzt aktiven Hosts und fällt auf lokale Logs beziehungsweise Backups zurück. Restart verwendet denselben exakten nativen Prozesstransport wie PB8 und lehnt unbekannte oder gesperrte Remote-Zustände ab. Lokales Speichern erhält den Inode geöffneter stderr-Logs.

Die Liste aktualisiert sich automatisch. Browser-Neuladen erhält Suche, Sortierung und Navigationskontext. Guide öffnet sich über der aktuellen Seite; Information → Help öffnet die vollständige Dokumentation.

## Backtest aus dem Editor

**Backtest** übernimmt die aktuelle, auch ungespeicherte native Konfiguration als privaten Entwurf in die gemeinsame PB9-Backtest-Seite. Die Run-Instanz wird dabei nicht geändert. Details stehen im Guide **PB9 Backtest**.
