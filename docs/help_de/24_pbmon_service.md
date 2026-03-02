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

Jeder Alarm wird **einmalig** pro Fehlerereignis gesendet. PBMon merkt sich, welche Fehler
bereits gemeldet wurden, und sendet nur dann eine neue Nachricht, wenn ein zuvor behobener
Fehler erneut auftritt.

## Einrichtung: Telegram-Bot anlegen

1. Öffne Telegram und chatte mit **[@BotFather](https://t.me/botfather)**
2. Sende `/newbot` und folge den Anweisungen — kopiere den **Bot Token**
3. Starte eine Unterhaltung mit deinem neuen Bot (sende `/start`)
4. Finde deine **Chat ID** heraus — am einfachsten über **[@userinfobot](https://t.me/userinfobot)**
5. Trage beide Werte in den PBMon-Einstellungen ein (siehe unten) und speichere

> **Wichtig:** Der Bot kann dir keine Nachrichten schicken, bis du ihm mindestens eine
> Nachricht gesendet hast. Der Befehl `/start` genügt.

## PBMon-Tab — Viewer-Modus (Standard)

Wenn du **System → Services → PBMon** öffnest, lädt sofort der vollhöhige Log-Viewer
mit dem Live-Stream von `PBMon.log`.

- Nutze die **Level-Filter**-Buttons (DBG / INF / WRN / ERR), um relevante Zeilen zu fokussieren
- Nutze **Search**, um einen bestimmten Servernamen oder Fehlertext zu finden
- Das **Version**-Dropdown zeigt rotierte Archive (`.1`, `.old`)

## PBMon-Tab — Einstellungsmodus

Klicke auf **⚙ Settings** in der linken Seitenleiste, um die Einstellungsansicht zu öffnen.

| Feld | Beschreibung |
|------|--------------|
| **Telegram Bot Token** | Token von @BotFather (gespeichert in `pbgui.ini`) |
| **Telegram Chat ID** | Deine persönliche oder Gruppen-Chat-ID |

Drücke **💾** zum Speichern und Rückkehr zum Viewer. Der Speichern-Button wird
**primär (rot)**, sobald es ungespeicherte Änderungen gibt.

> Änderungen wirken beim **nächsten Überwachungszyklus** (innerhalb von 60 Sekunden) —
> kein Neustart erforderlich.

## PBMon starten und stoppen

Nutze den **PBMon-Toggle** in der linken Seitenleiste im PBMon-Tab:

- Toggle **ein** → startet PBMon als abgekoppelten Hintergrundprozess
- Toggle **aus** → stoppt den Prozess sauber

Der **Overview**-Tab zeigt ✅ / ❌ Statusindikatoren für alle Services auf einen Blick.

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
