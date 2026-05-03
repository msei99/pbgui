# PBGUI Services Übersicht

Die Services-Seite zeigt und steuert alle PBGui-Hintergrunddienste an einem Ort.

## Service-Übersicht

Die Seite öffnet mit einem Kachel-Raster, das alle Services auf einen Blick zeigt. Jede Kachel zeigt:

- Den Service-Namen
- Einen Status-Indikator (grüner Punkt = läuft, roter Punkt = gestoppt)
- Aktions-Buttons: **Start** wenn gestoppt, **Stop + Restart** wenn aktiv

Klicke auf eine Kachel, um das Detail-Panel des Service zu öffnen.

Die Übersicht enthält jetzt auch eine eigene **Workers**-Kachel. Sie öffnet den nur für Administration gedachten Worker-Bereich für Queue-Worker, Sync-/Watcher-Worker und interne Helper-Tasks.

| Service | Funktion |
|---|---|
| **PBRun** | Startet/stoppt lokale Passivbot-Prozesse und verwaltet dynamische Coin-Filter |
| **PBRemote** | Synchronisiert Instanzen und Befehle zwischen lokalem Rechner und Remote-VPS über einen Cloud-Bucket |
| **PBMon** | Überwacht laufende Bots und sendet Telegram-Alarme bei ungewöhnlichem Verhalten |
| **PBStat** | Sammelt Spot-Handelsstatistiken – nur für den alten v6 Single Bot |
| **PBData** | Lädt Account-Daten (Balances, Positionen, Orders, History, Executions) via REST und Live-Preise über öffentliche WebSockets |
| **PBCoinData** | Ruft CoinMarketCap-Daten ab und erstellt Exchange-Symbol-Mappings für dynamische Filter |
| **PBAPIServer** | Betreibt das FastAPI-Backend (REST + WebSocket), das Dashboard, VPS Monitor, Job Queue und alle Echtzeit-Features versorgt |

## Services starten und stoppen

Die **Start**-, **Stop**- oder **Restart**-Buttons auf den Kacheln oder im Control-Strip oben im Detail-Panel verwenden. Änderungen wirken sofort.

## Service-Detail-Panels

Klicke auf eine Service-Kachel (oder den Sidebar-Eintrag), um ein dediziertes Detail-Panel zu öffnen mit:

- Einem Control-Strip mit Service-Status und Aktions-Buttons
- Tabs für verschiedene Ansichten (wo verfügbar):
  - **Log**: Live gefilterter Log-Viewer
  - **Settings**: Service-spezifische Konfiguration
  - **Status**: Laufzeit-Status (nur PBData)
  - **Info**: Remote-Server-Info (nur PBRemote)

Über die Sidebar links zwischen Services wechseln oder zur Übersicht zurückkehren.

## Workers-Panel

Der Sidebar-Eintrag **Workers** öffnet ein eigenes Admin-Panel innerhalb der Services-Seite. Es ist für Betrieb und Fehlersuche gedacht, nicht für die tägliche Bot-Bedienung.

Das Panel gruppiert Worker in:

- **Queue Workers**: gemeinsamer Market-Data-Queue-Worker, Backtest-Queue-Worker, Optimize-Queue-Worker
- **Sync / Watchers**: API-Key-Dateisync, V7-Config-Sync
- **Internal Helpers**: Archive-Sync und HLCVS-Cleanup-Hintergrundtasks

Pro Worker lassen sich ansehen:

- Laufend/gestoppt-Status
- Kleine Laufzeitstatistiken wie Queue-Größe, aktive Jobs, verbundene Hosts oder Watchdog-Status
- Start/Stop/Restart-Aktionen, sofern unterstützt
- Ein lokaler Log-Viewer, wenn der Worker in eine eigene Logdatei schreibt

Stop- und Restart-Aktionen im Workers-Panel fragen vor dem Senden des Befehls zusätzlich nach einer Bestätigung.

Einige Worker nutzen statt eines dedizierten lokalen Logs einen passenden Monitor. Der gemeinsame Market-Data-Queue-Worker verwendet zum Beispiel den Job Monitor, weil die Logs dort pro Queue-Job geführt werden. In diesen Fällen wird der Monitor beim Auswählen des Workers direkt im rechten Log-Bereich eingebettet, bleibt auch bei Worker-Refreshes stabil offen und man bleibt innerhalb der Services-Seite. Der eingebettete Job Monitor bietet jetzt außerdem `View` für die vollständigen Job-Details und `Run` auf Pending-Zeilen; `Run` fordert einen zusätzlichen manuellen Parallel-Slot für denselben Job-Typ an, sodass genau ein ausgewählter Pending-Job neben dem bereits laufenden Job dieses Typs starten kann. Aktive Zeilen bleiben jetzt außerdem in einer stabilen Queue-/Start-Reihenfolge, sodass zwei laufende Jobs bei jedem Progress-Update nicht mehr ihre Plätze tauschen. `View`- und `Log`-Dialoge werden jetzt außerdem auf den sichtbaren Browser-Viewport begrenzt und berücksichtigen zusätzlich sowohl den Scroll-Offset der äußeren Seite als auch clippende Eltern-Panels, sodass ihr Close-Button auch dann erreichbar bleibt, wenn der Monitor in einem höheren eingebetteten Bereich sitzt und dessen Kopf bereits über den sichtbaren Browserbereich hinaus gescrollt wurde.

## Typische Startreihenfolge

Ein stabiles Setup startet die Dienste üblicherweise in dieser Reihenfolge:

1. **PBCoinData** — erstellt Symbol-Mappings (erforderlich für dynamische Ignore/Approve-Listen)
2. **PBRun** — startet Bot-Prozesse (nutzt Mappings von PBCoinData)
3. **PBData** — liefert Live-Marktdaten für das Dashboard
4. **PBStat** — sammelt Spot-Handelsstatistiken (nur v6 Single Bot)
5. **PBAPIServer** — aktiviert Dashboard, VPS Monitor, Job Queue und Echtzeit-Features
6. **PBRemote** — verbindet mit Remote-VPS (wenn genutzt)
7. **PBMon** — aktiviert Monitoring und Telegram-Alarme (wenn genutzt)

## Schnelle Fehlersuche

- Ein Dienst zeigt einen roten Punkt, sollte aber laufen: das zugehörige Log im Log-Tab des Dienstes auf Fehler prüfen
- **PBRun**-Listen wirken veraltet: zuerst prüfen, ob **PBCoinData** seine Mappings erfolgreich erstellt hat
- Nach Konfigurationsänderung: betroffenen Dienst über den Restart-Button neu starten
