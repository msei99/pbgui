# Logging

Die Logging-Seite bietet einen Echtzeit-Log-Viewer für alle PBGui-Dienste.  
Logs werden live per WebSocket gestreamt — kein Neuladen der Seite erforderlich.

## Aufbau

- **Sidebar (links)**: Liste aller verfügbaren Log-Dateien in `data/logs/`
- **Toolbar**: Level-Filter, Zeilenanzahl, Versionsauswahl, Stream-Steuerung
- **Log-Bereich**: scrollbares Terminal-Ausgabefeld mit Suche und Hervorhebung

## Log-Datei auswählen

Auf einen Dateinamen in der Sidebar klicken, um ihn zu laden.  
Dateiname und Größe werden in der Toolbar angezeigt.  
Das Streaming startet automatisch — neue Zeilen werden live angehängt.

## Lines-Dropdown

Legt fest, wie viele Zeilen beim Öffnen oder Wechseln einer Datei geladen werden.  
Optionen: 200 / 500 / 1000 / 2000 / 5000 / 10000 / 25000 / All.  
Beim Ändern des Werts wird die Datei mit der neuen Anzahl neu geladen.

## Version-Dropdown

Erscheint, wenn die gewählte Log-Datei rotierte Backups hat (`.1`, `.old` usw.).  
- **Current**: aktive Datei mit Live-Streaming
- **.1 / .old / …**: archivierter Snapshot, einmalig geladen (kein Streaming)

Zurückschalten auf **Current** setzt das Live-Streaming fort.

## Level-Filter

Einzelne Log-Level ein- und ausblenden:

| Schaltfläche | Level    |
|--------------|----------|
| DBG          | DEBUG    |
| INF          | INFO     |
| WRN          | WARNING  |
| ERR          | ERROR    |
| CRT          | CRITICAL |

Zeilen, die keinem aktiven Level entsprechen, werden sofort ausgeblendet — ohne Nachlade-Vorgang.

## Suche

- Text im **Search**-Feld eingeben, um passende Zeilen zu filtern oder hervorzuheben
- **Filter**-Checkbox: aktiviert → nicht passende Zeilen ausblenden; deaktiviert → nur hervorheben
- **▲ / ▼**-Schaltflächen zum Springen zwischen Treffern
- **Preset**-Dropdown: häufige Suchmuster (Errors, Warnings, Traceback …)

## Stream-Steuerung

| Schaltfläche | Aktion                                                   |
|--------------|----------------------------------------------------------|
| ⏸ Pause     | Keine neuen Zeilen empfangen (Puffer bleibt erhalten)    |
| ▶ Stream    | Live-Streaming ab aktueller Position fortsetzen          |
| 🗑 Clear     | Anzeigepuffer leeren (Datei wird nicht gelöscht)         |
| ⬇ Download  | Aktuellen Pufferin-halt als Textdatei speichern          |
| ## Lines    | Zeilennummern ein-/ausblenden                            |

## Einstellungen

Auf **⚙ Settings** in der Sidebar klicken, um die Log-Rotation zu konfigurieren:

- **Standard-Rotation**: maximale Dateigröße (MB) und Anzahl der Backup-Dateien für alle Dienste
- **Einzelne Log-Rotation**: Größe und Backup-Anzahl pro Dienst überschreiben

Änderungen wirken sich aus, wenn der Logger des Dienstes neu initialisiert wird (Dienst bei Bedarf neu starten).

## Fehlerbehebung

- **Keine Log-Dateien aufgelistet**: Sicherstellen, dass PBGui-Dienste mindestens einmal gestartet wurden
- **Streaming stoppt**: API-Server-WebSocket-Verbindung unterbrochen — der Viewer verbindet sich automatisch neu
- **„All" ist langsam**: Sehr große Dateien können einen Moment brauchen; bei großen Logs ein Zeilenlimit setzen

---

## Wo finde ich was?

Alle Log-Dateien liegen in `data/logs/`. In der Sidebar kann jede direkt geöffnet werden.

### PBGui.log

Enthält Meldungen aller GUI-Hilfskomponenten:

| Komponente | Was steht dort |
|------------|---------------|
| VPSManager | VPS-Verbindungen, Remote-Befehle |
| Instance | Bot-Instanz laden/speichern, Symbol-Infos |
| Config | Fehler beim Laden/Speichern von Konfigurationen |
| Multi | Multi-Bot-Konfigurationen |
| Backtest / BacktestV7 | Backtest-Ergebnisse laden, beschädigte Dateien |
| BacktestMulti | Multi-Symbol-Backtest-Operationen |
| Optimize / OptimizeV7 / OptimizeMulti | Optimizer-Operationen |
| ParetoDataLoader | Pareto-Ergebnisse laden |
| Status | Status-Seiten-Ereignisse |
| HyperliquidAWS | Hyperliquid AWS-Integration |

### Eigene Log-Dateien

| Datei | Dienst | Was steht dort |
|-------|--------|---------------|
| `PBRun.log` | PBRun | Live-Bot Start/Stop, Order-Loop |
| `PBRemote.log` | PBRemote | Remote-Sync, VPS-Kommunikation |
| `PBCoinData.log` | PBCoinData | CMC-Daten-Updates, Symbol-Listen |
| `VPSMonitor.log` | VPS Monitor | SSH-Verbindungen, Host-Metriken, Service-Auto-Heal |
| `PBApiServer.log` | PBAPIServer | FastAPI-Start, REST/WebSocket-Requests |
| `PBStat.log` | PBStat | Statistik-Erfassung |
| `Database.log` | Database | DB-Abfragen, Verbindungsfehler |
| `Exchange.log` | Exchange | Marktdaten, Symbol-Infos, CCXT-Fehler |
| `PBData.log` | PBData | OHLCV-Download, Marktdaten-Pipeline |
