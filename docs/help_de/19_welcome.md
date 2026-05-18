# Welcome & Login

Die **Welcome**-Seite ist der Standalone-Einstieg in PBGui. Hier erledigst du den ersten Login, die Grundkonfiguration und die wichtigsten Laufzeitprüfungen, bevor du in die restliche Anwendung wechselst.

## Wofür die Seite da ist

Nutze die Welcome-Seite, um:

- dich mit dem aktuellen PBGui-Passwort anzumelden
- das Passwort zu ändern
- den lokalen PBv7-Pfad und Interpreter zu konfigurieren
- festzulegen, ob diese Maschine als **Master** oder **Slave** arbeitet
- zu prüfen, ob der API-Server die aktuelle Runtime-Konfiguration korrekt lesen kann

## Bereich Overview

Der Standardbereich **Overview** fasst den aktuellen lokalen Zustand zusammen:

- **Session**: ob du eingeloggt bist oder noch als Gast arbeitest
- **PB7**: ob die konfigurierte PBv7-Runtime verwendbar aussieht
- **Identity**: aktuelle Host-Rolle und konfigurierter Bot-Name
- **Runtime Status**: detaillierte Readiness-Pruefungen aus dem Backend

Dieser Bereich ist als schneller Kontrollpunkt nach dem ersten Start, nach Passwortwechseln oder nach Pfad-Anpassungen gedacht.

## Bereich Setup

Im Bereich **Setup** bearbeitest du die Werte aus `pbgui.ini`.

Wichtige Felder:

- **Passivbot V7 path**: Stammverzeichnis des lokalen PBv7-Checkouts
- **Passivbot V7 python interpreter**: voller Pfad zur Python-Binary in der PBv7-Virtualenv
- **Bot name**: lokale Bot-Identitaet von PBGui
- **Role**: **Master** waehlen, wenn dieser Host Remote-VPS verwaltet, sonst **Slave**

Mit den **Browse**-Buttons kannst du Verzeichnisse und den Python-Interpreter direkt aus dem Server-Dateisystem auswaehlen.

Nach dem Speichern gelten die Aenderungen sofort und werden sowohl vom Streamlit- als auch vom FastAPI-Pfad verwendet.

## Bereich Password

Die Aktion **Password** in der linken Sidebar oeffnet das Passwort-Formular.

Damit kannst du:

- das aktuelle Login-Passwort ersetzen
- den Passwortschutz entfernen, indem du das neue Passwort leer laesst

Zum Aendern des Passworts musst du eingeloggt sein.

## Typischer Ablauf beim ersten Einrichten

1. Welcome-Seite oeffnen.
2. Mit dem aktuellen PBGui-Passwort einloggen.
3. Den **Passivbot V7 path** setzen.
4. Den **Passivbot V7 python interpreter** setzen.
5. Die richtige **Role** waehlen.
6. Das Setup speichern.
7. Den **Runtime Status** erneut pruefen, bis PBv7 bereit ist.

## Schnelle Fehlersuche

- **PB7 blocked**: der konfigurierte PBv7-Pfad oder Interpreter fehlt oder ist ungueltig
- **Save Setup** bleibt deaktiviert: zuerst einloggen
- **Browse** funktioniert nicht: Authentifizierung und Server-Pfadrechte pruefen
- **Du willst nur das Passwort aendern**: die Sidebar-Aktion **Password** verwenden statt die Setup-Felder anzupassen
