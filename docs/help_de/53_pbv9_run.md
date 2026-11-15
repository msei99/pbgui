# PB9 Run

Unter **PBv9 → Run** verwaltest du PB9-Live-Instanzen. Liste und Editor verwenden dieselbe Oberfläche wie PB7/PB8 Run. PB9 hat einen eigenen Quellbaum, eine eigene Python-Umgebung sowie getrennte Instanz- und Backup-Verzeichnisse.

## Instanz konfigurieren

Die grundlegenden Live-Felder bleiben wie bei PB8 sichtbar. Advanced Settings bündelt die übrigen Live-, Logging- und Monitoring-Felder. PB9 zeigt seine eigenen Feldnamen und Einheiten. Insbesondere ist risk_bootstrap_max_lookback_minutes eine eigenständige Startup-Einstellung und kein Ersatz mit gleicher Bedeutung für den entfernten PB8-PnL-Rückblick.

Long- und Short-JSON verwenden den gemeinsamen Editor. **With global defaults** zeigt die geerbten Seiteneinstellungen; nur geänderte Werte werden zu expliziten Overrides. **Overrides only** zeigt die tatsächlich gespeicherte, sparsame Seitenkonfiguration. Lösche dort eine Eigenschaft, um wieder zu erben. Unveränderte Standardwerte bleiben beim Speichern ausgelassen; vorhandene explizite Werte bleiben erhalten. Die Seitenaktivierung ist immer explizit. Die Ansichtsauswahl bleibt beim Neuladen erhalten.

Wähle **Add PB9 Instance**, ein Börsenkonto und bearbeite Strategie, Portfolio, globale Vorgaben sowie Long-/Short-Overrides. Das Formular liest Felddefinitionen und Strategiemetadaten der installierten PB9-Version. Geerbte Seitenwerte bleiben geerbt, bis du ausdrücklich einen Override setzt. Aktiviere die gewünschten Seiten ausdrücklich.

Die Coin-Auswahl verwendet veröffentlichte lokale Coin-Data-Mappings. Exakte, börsenqualifizierte Marktkennungen unterscheiden unabhängig gehandelte Märkte mit gleichem Coin-Namen. Coin-Overrides sind eingebettete PB9-Patches. Dateibasierte Overrides und externe Konfigurationsverweise werden abgelehnt.

Der Katalog verwendet die native Standard-Abrechnungswährung der Börse: USDT für die üblichen Börsen und USDC für Hyperliquid/Lighter. Hyperliquid-HIP-3-Namensräume und numerische native Marktkennungen bleiben unterscheidbar.

Save erzeugt eine neue Konfigurationsversion. Bei vorhandenen Instanzen muss die geladene Version noch aktuell sein; konkurrierende Änderungen werden abgelehnt. PBGui speichert Laufzeit, Version, Notiz und Host-Zuordnung in einer privaten Begleitdatei. Die native Konfiguration enthält ausschließlich Passivbot-Felder. Für fortgeschrittene Bearbeitung gibt es Raw JSON.

## Lokaler Betrieb

**Enabled on → disabled** speichert ohne geplanten Trading-Start. Mit Auswahl dieses PBGui-Hosts übernimmt der PBRun-Daemon die Instanz. Voraussetzung sind eine funktionierende separate PB9-Installation, eine aktuelle Rust-Erweiterung, verfügbare Kontozugangsdaten und der laufende PBRun-Dienst. Eine konkurrierende PB7-/PB8-/PB9-Zuordnung desselben Kontos blockiert die Aktivierung. Speichern umgeht diese Startprüfungen nicht.

Starte nach dem PBGui-Update die lokalen Dienste API und PBRun neu, damit sie den neuen Code laden. Die einzelnen Dienste findest du unter **System → PBGUI Services**. Die Neustartanzeige nennt Dienste, die noch mit einer älteren Code-Serial laufen.

PB9 unterstützt derzeit lokalen Betrieb. Remote-Hosts, Backtests und Strategy Explorer benötigen eine eigene PB9-Anbindung und werden nicht auf PB8 umgeleitet. Eine geänderte Host-Zuordnung kann Trading starten oder stoppen; wähle nur beabsichtigte Konten und Einstellungen.

## Backups und Logs

Vorherige gespeicherte Versionen werden einschließlich PBGui-Begleitdatei gesichert. Öffne ein Backup als Editor-Entwurf oder stelle es als neue Version wieder her. Wiederherstellungen sind zunächst deaktiviert. Laufende Instanzen müssen vor dem Löschen gestoppt sein. Das gemeinsame Log-Panel zeigt lokale Laufzeitausgaben. Restart stoppt den exakt zugeordneten Prozess, damit PBRun ihn über die regulären Prüfungen neu starten kann.

Die Liste aktualisiert sich automatisch. Browser-Neuladen erhält Suche, Sortierung und Navigationskontext. Guide öffnet sich über der aktuellen Seite; Information → Help öffnet die vollständige Dokumentation.
