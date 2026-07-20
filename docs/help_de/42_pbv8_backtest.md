# PBv8 Backtest

PBv8 Backtest verwaltet Passivbot-V8-Konfigurationen und Jobs getrennt von PBv7. PBGui validiert jede Konfiguration vor dem Speichern oder Starten mit dem aktuell installierten PB8-Loader.

Die Seite rendert exakt dieselbe Seitenvorlage und denselben visuellen Konfigurationseditor wie PBv7 Backtest. Es gibt keine separate PB8-Editorimplementierung. PB8-spezifisch bleiben nur Pfad-/API-Adapter, Config-Validierung, Prozess-Runner und Ergebnisdaten.

PBGui hält PB8-Templates und bereits validierte Config-Dateien kurzzeitig in einem begrenzten Cache. Die erste PB8-Aktion nach einem API-Neustart kann weiterhin die isolierte PB8-Python-Runtime initialisieren; nachfolgende Editor-, Queue- und Start-Schritte verwenden kanonische Ergebnisse wieder, während Dateisignaturen geänderte Configs invalidieren.

## Configs

- **New Config** lädt die Standardwerte der installierten PB8-Version.
- Eine Zeile doppelklicken oder mit **Edit** den vollständigen visuellen PBv7-Editor öffnen: Zeitraum, Exchanges, Gebühren, Marktdaten, Coin-Filter, Approved/Ignored Coins, Suites, Coin Overrides, PB8-Resultmetriken und Market-Settings-Overrides, Long/Short JSON und Raw JSON.
- **Queue** oder **Save & Queue** speichert einen unveränderlichen Snapshot der Config.
- Coin-Override-JSON-Dateien und `backtest.json` werden als ein Config-Bundle validiert und veröffentlicht. Bei einem fehlgeschlagenen Speichern bleiben die bisherige Config und ihre Override-Dateien unverändert; beim Entfernen einer Override-Referenz verschwindet die veraltete Datei aus dem Bundle.
- PBGui verwaltet `backtest.base_dir`; Ergebnisse landen unter `<pb8dir>/backtests/pbgui/<config>`.
- Gespeicherte PBv7-Backtest-Configs, einzelne PBv7-Ergebnisse und PBv7-Run-Zeilen bieten die **V8**-Konvertierung. Sie nutzt PB8s offizielle `migrate-config-v7`-Implementierung, lässt die V7-Quelle unverändert und speichert `migration_report.json` neben der neuen V8-Config. Bei Result-Konvertierungen stellt PBGui vorher die durch `fills.csv` belegten effektiven Maker- und Taker-Raten linearer Märkte wieder her und protokolliert jede Korrektur unter `pbgui_result_fee_adjustments`. PBGui-eigene Metadaten und veraltete temporäre `live.base_config_path`-Werte werden vor dem PB8-Aufruf entfernt.

Die Migration stoppt, wenn nicht unterstützte Felder oder manuell zu prüfende Punkte verbleiben. Diese Punkte müssen geprüft werden, statt identisches Verhalten von V7 und V8 anzunehmen.

## Editor

Die visuellen Controls und die JSON-Synchronisierung werden direkt mit PBv7 geteilt. Bei kanonischen V8-Configs liest und schreibt der Adapter Exposure und Positionszahl unter `bot.<side>.risk`; die Long/Short- und Raw-JSON-Editoren erhalten alle verschachtelten V8-Bereiche wie `risk`, `strategy`, `forager`, `hsl` und `unstuck`. Import und Speichern werden mit dem installierten PB8-Loader vorbereitet; ungültiges JSON blockiert das Speichern. PBGui-eigene Metadaten im Top-Level-Objekt `pbgui` bleiben unverändert, auch wenn die installierte PB8-Version diese Felder nicht kennt.

V8 verwendet die bestehenden PBGui-Komponenten für Coin-Metadaten, Filter, Suites, Overrides, Date Picker, Validierung, OHLCV Readiness und Auswahl. **Balance Calculator** öffnet den gemeinsamen Calculator unter Information mit der aktuellen PB8-Config; **Calc Balance** führt dieselbe Berechnung inline aus. Versionsspezifische externe PBv7-Aktionen wie Add to Run und Optimize werden auf der V8-Seite nicht angezeigt.

Häufig verwendete PB8-spezifische Backtest-Felder besitzen strukturierte Controls:

- **Market Settings Overrides** steht unter `market_settings_sources`. Dort lassen sich globale oder Exchange-spezifische Coin-Zeilen mit Quantity-/Price-Step, Mindestmenge/-wert und Contract-Multiplier anlegen. Leere Werte werden von der gewählten Quelle übernommen; Exchange-spezifische Zeilen haben Vorrang. Der aktuellen PBGui-Version unbekannte Felder bleiben unverändert erhalten. Backtest-Gebühren werden weiterhin über die separaten Maker-/Taker-Fee-Overrides oberhalb gesteuert, weil PB8 sie vor den Market-Settings-Overrides auflöst.
- **Result Metrics** befindet sich unter **Additional Parameters**, weil es nur die Terminal- und Queue-Log-Ausgabe steuert. **Default** verwendet die durch Optimize Scoring und Limits bestimmten Metriken, **All** zeigt alle Metriken und **Custom additions** bietet eine durchsuchbare, kategorisierte Auswahl aus der installierten PB8-Runtime. Vollständige Metriken werden in jedem Modus weiterhin berechnet und gespeichert.

**Additional Parameters** enthält Result Metrics, den von PBGui verwalteten `base_dir` als Read-only-Feld sowie die Expertenoptionen `hlcvs_data_dir` und `hlcvs_data_override_mode`. Prepared-Dataset-Replay benötigt einen serverseitigen PB8-Dataset-Pfad mit gültigem Manifest und bleibt normalerweise auf `null`; PB8 löst Datasets dann automatisch auf. Zukünftige unbekannte Top-Level-Backtest-Felder erscheinen ebenfalls in diesem Fallback-Bereich.

## Queue

Die V8-Queue liegt separat unter `data/bt_v8_queue`. **Start** startet `<venv_pb8>/bin/passivbot backtest <snapshot>`. **Stop**, **Restart**, **Delete** und **Clear Finished** betreffen nur V8-Queue-Einträge.

Laufende Backtests sind unabhängige Jobs. Ein Neustart von PBGui oder ein PB8-Update stoppt sie nicht, und sie blockieren kein Update. Neue Starts bleiben während einer PB8-Installation oder eines Updates in der Queue und laufen danach weiter. Jeder neue Start verwendet die zu diesem Zeitpunkt installierte PB8-Version; Version und Git-Commit werden am Queue-Eintrag gespeichert.

Die Log-Aktion einer Queue-Zeile öffnet `data/logs/backtests_v8/<queue-id>.log`. Die Glocke oben rechts öffnet `PBGui.log`; dort werden die kurzen GUI-Meldungen und Fehler dauerhaft angezeigt, die sonst nach wenigen Sekunden verschwinden. Technische PB8-Backend-Diagnosen bleiben getrennt in `BacktestV8.log` verfügbar.

**Settings** wird über die Queue-Aktionen in der Sidebar geöffnet. Dort können **Start queued jobs automatically**, die Anzahl paralleler Jobs und **Use PBGui Market Data** eingestellt werden. Die Marktdatenoption wird unmittelbar vor jedem Start oder Restart auf eine frische Kopie des unveränderlichen Queue-Snapshots angewendet und verändert deshalb nie die gespeicherte Config.

## Results

Der **Version**-Filter ist standardmäßig auf PBv8 gesetzt und kann auf PBv7 oder **Both** umgeschaltet werden. Die Liste zeigt Version, Config, Exchange, Run-Verzeichnis und kompakte skalare Werte. Finale Balance und Equity stammen aus den letzten Werten von PB8s `balance_and_equity.csv` beziehungsweise der komprimierten `.csv.gz`-Datei, wenn die Analyse keine expliziten Endwerte enthält. PBv7- und PBv8-Zeilen können gemeinsam ausgewählt und über **Compare** verglichen werden; jede Equity-Datei wird aus dem passenden Resultroot geladen. **Delete Selected** unterstützt ebenfalls eine gemischte Auswahl und sendet jedes Result an sein zuständiges PBv7- oder PBv8-Backend.

PB8 kann beim Backtest historische Daten herunterladen. Config, Exchanges, Coin-Auswahl, Zeitraum und Migrationsbericht sollten vor einem großen Backtest geprüft werden.
