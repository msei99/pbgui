# PBMon Service

PBMon ist ein schlanker Hintergrunddienst, der deine PBRemote-Server und laufende
Passivbot-Instanzen überwacht. Bei einem Problem sendet er dir sofort eine Telegram-Nachricht,
damit du reagieren kannst — ohne die UI manuell prüfen zu müssen.

## Was PBMon überwacht

PBMon ruft alle 60 Sekunden die Fehlerprüfung von PBRemote ab und prüft auf:

| Alarmtyp | Wird ausgelöst wenn |
|----------|----------------------|
| **Server offline** | Ein entfernter VPS nicht erreichbar ist |
| **Systemressource** | Ein Server den konfigurierten Speicher-, CPU-, Swap- oder Disk-Schwellenwert überschreitet |
| **Instanzfehler** | Eine laufende Passivbot-Instanz einen Fehler oder unerwarteten Stopp meldet |
| **HL API-Key-Ablauf** | Ein Hyperliquid API-Key bald abläuft (konfigurierbare Warntage in `pbgui.ini`) |

Jeder Alarm wird **einmalig** pro Fehlerereignis gesendet. PBMon merkt sich, welche Fehler
bereits gemeldet wurden, und sendet nur dann eine neue Nachricht, wenn ein zuvor behobener
Fehler erneut auftritt. HL-Key-Ablauf-Warnungen werden auf einmal pro User pro Tag dedupliziert.

## Einrichtung: Telegram-Bot anlegen

1. Öffne Telegram und chatte mit **[@BotFather](https://t.me/botfather)**
2. Sende `/newbot` und folge den Anweisungen — kopiere den **Bot Token**
3. Starte eine Unterhaltung mit deinem neuen Bot (sende `/start`)
4. Finde deine **Chat ID** heraus — am einfachsten über **[@userinfobot](https://t.me/userinfobot)**
5. Trage beide Werte in den PBMon-Einstellungen ein (siehe unten) und speichere

> **Wichtig:** Der Bot kann dir keine Nachrichten schicken, bis du ihm mindestens eine
> Nachricht gesendet hast. Der Befehl `/start` genügt.

## PBMon-Detail-Panel — Log-Tab (Standard)

Klicke auf die PBMon-Kachel in der Services-Übersicht (oder nutze die Sidebar), um das Detail-Panel zu öffnen.
Der **Log**-Tab lädt standardmäßig und zeigt einen Live-Stream von `PBMon.log`.

- Nutze die **Level-Filter**-Buttons (DBG / INF / WRN / ERR / CRT), um relevante Zeilen zu fokussieren
- Nutze **Search**, um einen bestimmten Servernamen oder Fehlertext zu finden
- Öffne die **Files**-Sidebar, um zu rotierten Log-Archiven (`.1`, `.old`) zu wechseln, falls vorhanden

## PBMon-Detail-Panel — Settings-Tab

Wechsle zum **Settings**-Tab, um Telegram-Benachrichtigungen zu konfigurieren.

| Feld | Beschreibung |
|------|--------------|
| **Telegram Bot Token** | Token von @BotFather (gespeichert in `pbgui.ini`) |
| **Telegram Chat ID** | Deine persönliche oder Gruppen-Chat-ID |

Klicke **Save** zum Speichern.

> Änderungen wirken beim **nächsten Überwachungszyklus** (innerhalb von 60 Sekunden) —
> kein Neustart erforderlich.

## PBMon starten und stoppen

Nutze die **Start**- / **Stop**-Buttons im Control-Strip oben im PBMon-Detail-Panel oder die Buttons auf der PBMon-Übersichtskachel.

- **Start** → startet PBMon als abgekoppelten Hintergrundprozess
- **Stop** → stoppt den Prozess sauber

## Log-Format

PBMon nutzt den zentralen PBGui-Logger. Jede Zeile folgt dem Format:

```
2026-03-01T12:55:50.123 [PBMon] [INFO] Start: PBMon
2026-03-01T12:55:52.456 [PBMon] [INFO] Send Message: Server: *myVPS* is offline
2026-03-01T12:58:00.789 [PBMon] [ERROR] Something went wrong, but continue: ...
```

## Fehlerbehebung

| Symptom | Prüfen |
|---------|--------|
| Keine Telegram-Alarme | Token und Chat ID in den Settings prüfen · `PBMon.log` auf `[ERROR]`-Zeilen durchsuchen |
| Fehler „Chat not found" | Sicherstellen, dass du `/start` an den Bot gesendet hast, bevor PBMon das erste Mal lief |
| PBMon startet nicht | Möglicherweise läuft eine weitere Instanz — `data/pid/pbmon.pid` prüfen |
| Alarme hören auf | PBMon könnte abgestürzt sein — Status im Overview-Tab prüfen |
