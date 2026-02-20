# PBMon Service Details

PBMon ist ein Hintergrunddienst, der laufende Passivbot-Instanzen überwacht und bei ungewöhnlichem Verhalten oder Fehlern Alarme über Telegram sendet.

## Was PBMon macht

- Überprüft kontinuierlich den Zustand und Status aktiver Bot-Prozesse
- Überwacht auf feststeckende Positionen, übermäßige Fehler oder unerwartete Stopps
- Sendet Echtzeit-Alarmmeldungen an einen konfigurierten Telegram-Chat
- Schreibt Service-Logs nach `data/logs/PBMon.log`

## Konfiguration

Um PBMon zu nutzen, musst du einen Telegram-Bot konfigurieren.

1. Erstelle einen Bot über [@BotFather](https://t.me/botfather) auf Telegram und kopiere den **Bot Token**
2. Starte einen Chat mit deinem neuen Bot und sende eine Nachricht
3. Finde deine **Chat ID** heraus (z.B. über Bots wie `@userinfobot`)
4. Trage beide Werte auf der PBMon-Detailseite ein:
   - `Telegram Bot Token`
   - `Telegram Chat ID`

Änderungen werden automatisch gespeichert. Starte den PBMon-Service neu, damit der neue Token wirksam wird.

## PBMon-Detailseite

Auf `System → Services → PBMon → Show Details` kannst du:

- Den aktuellen PBMon-Status prüfen (läuft/gestoppt)
- Den Service ein-/ausschalten
- Telegram-Zugangsdaten konfigurieren
- Den integrierten gefilterten PBMon-Log-Viewer nutzen

## Schnelle Fehlersuche

- **Keine Alarme erhalten**: Prüfe `data/logs/PBMon.log` auf Telegram-API-Fehler. Stelle sicher, dass Token und Chat-ID korrekt sind.
- **Bot muss gestartet sein**: Du musst mindestens eine Nachricht (z.B. `/start`) an deinen Telegram-Bot senden, bevor er dir Nachrichten schicken kann.
