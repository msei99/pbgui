# PBGUI Services Übersicht

Die Services-Seite zeigt und steuert alle PBGui-Hintergrunddienste an einem Ort.

## Service-Übersicht

Die Seite zeigt Service-Spalten, jede mit:

- Einem Toggle zum Starten/Stoppen des Dienstes
- Einem Status-Indikator (✅ läuft / ❌ gestoppt)
- Einem **Show Details**-Button zur Service-Detailansicht

| Service | Funktion |
|---|---|
| **PBRun** | Startet/stoppt lokale Passivbot-Prozesse und verwaltet dynamische Coin-Filter |
| **PBRemote** | Synchronisiert Instanzen und Befehle zwischen lokalem Rechner und Remote-VPS über einen Cloud-Bucket |
| **PBMon** | Überwacht laufende Bots und sendet Telegram-Alarme bei ungewöhnlichem Verhalten |
| **PBStat** | Sammelt Spot-Handelsstatistiken – nur für den alten v6 Single Bot |
| **PBData** | Ruft Echtzeit-Marktdaten (OHLCV, Orders, Positionen) von Exchanges ab |
| **PBCoinData** | Ruft CoinMarketCap-Daten ab und erstellt Exchange-Symbol-Mappings für dynamische Filter |
| **PBMaster** | Hält persistente SSH-Verbindungen zu VPS-Hosts und versorgt den VPS Monitor mit Live-Daten |

## Services ein-/ausschalten

Auf den Toggle klicken, um einen Dienst zu starten oder zu stoppen. Die Änderung wirkt sofort — PBGui startet oder stoppt den entsprechenden Hintergrundprozess.

## Show Details

Jeder Service hat einen **Show Details**-Button, der eine dedizierte Detailansicht öffnet mit:

- Aktuellem Service-Status
- Servicespezifischen Konfigurationsoptionen (wo verfügbar)
- Integriertem gefiltertem Log-Viewer

Über den Zurück-Button (`:back:`) in der Sidebar oder die Navigation oben links kommt man zur Übersicht zurück.

## Typische Startreihenfolge

Ein stabiles Setup startet die Dienste üblicherweise in dieser Reihenfolge:

1. **PBCoinData** — erstellt Symbol-Mappings (erforderlich für dynamische Ignore/Approve-Listen)
2. **PBRun** — startet Bot-Prozesse (nutzt Mappings von PBCoinData)
3. **PBData** — liefert Live-Marktdaten
4. **PBStat** — sammelt Spot-Handelsstatistiken (nur v6 Single Bot)
5. **PBMaster** — aktiviert persistente VPS-Verbindungen und Live-Monitoring (wenn genutzt)
6. **PBRemote** — verbindet mit Remote-VPS (wenn genutzt)
7. **PBMon** — aktiviert Monitoring und Alarme (wenn genutzt)

## Schnelle Fehlersuche

- Ein Dienst zeigt ❌, Toggle ist aber an: das zugehörige Log unter `data/logs/` auf Fehler prüfen
- **PBRun**-Listen wirken veraltet: zuerst prüfen, ob **PBCoinData** seine Mappings erfolgreich erstellt hat
- Nach Konfigurationsänderung: betroffenen Dienst durch An-/Ausschalten neu starten
