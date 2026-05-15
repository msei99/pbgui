# VPS Monitoring Alerts

Die VPS-Monitoring-Alarme laufen jetzt innerhalb des **PBAPIServer** zusammen
mit dem Live-VPS-Monitor. Einen separaten `PBMon`-Daemon gibt es nicht mehr.

## Was überwacht wird

Der API-Server hält den VPS-Monitor verbunden und bewertet aktive Alarmzustände
direkt aus dem Live-In-Memory-State.

| Alarmtyp | Wird ausgelöst wenn |
|----------|----------------------|
| **Offline Host** | Die SSH-Verbindung zu einem überwachten VPS verloren geht |
| **Service-Problem** | Ein überwachter VPS-Service down ist oder ein Neustart angestoßen wurde |
| **System-Schwellenwert** | Ein Host konfigurierte Speicher-, CPU-, Swap- oder Disk-Grenzen überschreitet |
| **Instanz-Schwellenwert** | Eine überwachte Passivbot-Instanz konfigurierte Limits überschreitet |
| **HL API-Key-Ablauf** | Ein Hyperliquid API-Key bald abläuft |

Aktive Alarme werden in der Navigationsleiste als eigener Alarm-Indikator
angezeigt. Das Badge zeigt die Zähler `new/ack`.

## Wo es konfiguriert wird

Öffnen:

1. **System -> Services**
2. **PBAPIServer** auswählen
3. Den Tab **Settings** öffnen
4. Zum Bereich **VPS Monitoring** gehen

Im Block `Alerts / Telegram` kannst du konfigurieren:

- **Telegram Bot Token**
- **Telegram Chat ID**
- Welche aktiven Alarmgruppen im GUI sichtbar sind
- Welche Problem- und Recovery-Ereignisse an Telegram gesendet werden

Die Einstellungen sind gruppiert nach:

- **Offline Hosts**
- **Services**
- **System Thresholds**
- **Instance Thresholds**

So bleibt die UI kompakt, während Telegram trotzdem sehr gezielt steuerbar ist.

## Verhalten im GUI

- Das GUI zeigt **nur aktuell aktive Probleme**
- Behobene Alarme verschwinden automatisch
- Wenn ein Problem später wieder auftritt, wird es wieder **new/unacknowledged**
- Einzelne Alarme oder alle sichtbaren Alarme lassen sich im Nav-Overlay quittieren

## Telegram einrichten

1. In Telegram mit **[@BotFather](https://t.me/botfather)** chatten
2. `/newbot` senden und den **Bot Token** kopieren
3. Mit dem Bot per `/start` eine Unterhaltung beginnen
4. Die **Chat ID** herausfinden, z. B. über **[@userinfobot](https://t.me/userinfobot)**
5. Beide Werte unter **PBAPIServer -> Settings -> VPS Monitoring** speichern

> Der Bot kann dir erst Nachrichten senden, nachdem du ihm mindestens eine Nachricht geschickt hast.

## Logs

Alarm-Routing und VPS-Monitoring-Aktivität werden über die API-Server- und
VPS-Monitor-Logs protokolliert, hauptsächlich:

- `PBApiServer.log`
- `VPSMonitor.log`

## Fehlerbehebung

| Symptom | Prüfen |
|---------|--------|
| Keine Telegram-Alarme | Bot Token und Chat ID in den PBAPIServer-Settings prüfen · `PBApiServer.log` und `VPSMonitor.log` kontrollieren |
| Alarm-Badge bleibt leer | Prüfen, ob die Alarmgruppe für GUI-Sichtbarkeit aktiviert ist und der VPS in den überwachten Hosts enthalten ist |
| „Chat not found" | Dem Bot vor dem Testen von Alarmen `/start` senden |
| Alarme verschwinden nach Recovery | Erwartet: Im GUI werden nur aktive Probleme angezeigt |
