# PBGUI Services Übersicht

Die Services-Seite zeigt und steuert alle PBGui-Hintergrunddienste an einem Ort.

## Service-Übersicht

Die Seite öffnet mit einem Kachel-Raster, das alle Services auf einen Blick zeigt. Jede Kachel zeigt:

- Den Service-Namen
- Einen Status-Indikator (grüner Punkt = läuft, roter Punkt = gestoppt)
- Aktions-Buttons: **Start** wenn gestoppt, **Stop + Restart** wenn aktiv

Klicke auf eine Kachel, um das Detail-Panel des Service zu öffnen.

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
