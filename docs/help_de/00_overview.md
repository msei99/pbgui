# Hilfe Übersicht

Das separate **Guide**-Fenster zeigt beim fehlgeschlagenen Laden der Themenliste oder Inhalte eine sichtbare Fehlermeldung und protokolliert den Fehler in der Browserkonsole. Eine erfolgreiche Themenauswahl oder erneutes Öffnen von Guide ersetzt den Fehler durch die aktuelle Dokumentation. Überholte Anfragen überschreiben keine neueren Inhalte und zeigen keine veralteten Fehler. **Information → Help** bleibt der Einstieg zur vollständigen Hilfeseite.

Die Hilfe behält das zuletzt gewählte Thema und die Sprache bei, auch wenn Anfragen in anderer Reihenfolge enden; ältere Inhalte und Suchergebnisse werden ignoriert.

Dies ist die zentrale Hilfeseite für PBGui. Sie enthält allgemeine Dokumentation und Tutorials, die auf mehreren Seiten verwendet werden, z. B. Strategy Explorer (inkl. Movie Builder) und Pareto Explorer.

- Wähle ein Thema aus der **Contents**-Liste.
- Verfügbare Sprachen: EN / DE

Die obere Navigation funktioniert wie ein Anwendungsmenü: Öffne ein Menü per Klick und fahre danach mit der Maus über die anderen Menütitel, um direkt umzuschalten. Ein Klick außerhalb schließt das Menü. Auf Touchscreens öffnest oder schließt du das jeweilige Menü durch Antippen.

Die Glocke **Notification log** in der oberen Navigation öffnet das nichtmodale Panel **Notifications**. Schließe es mit **Escape**, seinem Schließen-Button oder der Glocke; Klicks außerhalb lassen es geöffnet. Escape gibt den Fokus an die Glocke zurück und beendet den Logstream des Panels. Liegt ein Fenster für Confirm action, VPSMonitor alerts, About oder Guide darüber, schließe zuerst dieses Fenster. Escape bricht auch eine noch ladende Öffnung ab. Der Abschluss einer verzögerten Öffnung erhält den Fokus in einem inzwischen geöffneten Dialog oder Seiten-Eingabefeld. Gemeinsame Bestätigungs- und Eingabedialoge verarbeiten Escape, ohne das darunterliegende Panel zu schließen.

Help-Inhalte werden vor der Anzeige bereinigt. Links und Bilder akzeptieren nur HTTP(S)-URLs; Suchmarkierungen werden als Textknoten statt als ausführbares HTML aufgebaut. Help-Seite und Overlay behalten den konfigurierten ASGI-Mount-Prefix für lokale Assets und API-Anfragen.

Auf Seiten mit Sidebar kannst du deren rechten Rand ziehen, um die Breite einzustellen. PBGui speichert eine Sidebar-Breite in diesem Browser und stellt sie seitenübergreifend sowie nach einem Neuladen wieder her. Der fokussierte Trennbalken unterstützt auch die Pfeiltasten Links/Rechts sowie Pos1/Ende. Auf schmalen Bildschirmen steht die Sidebar in voller Breite über dem Inhalt; Navigation und Inhalt lassen sich getrennt scrollen.


Alle Seiten-Sidebars verwenden dieselben Abstände und Bedienelemente: 8 px Außenabstand, 4 px Zwischenraum, 13 px Button-Schrift und mindestens 32 px hohe Buttons mit 6 px Eckenradius. Lange Beschriftungen brechen um. Das aktive Ziel beziehungsweise ausgewählte Konto wird blau hervorgehoben. Aktionsfarben behalten ihre Bedeutung: Speichern/Anlegen (Türkis), Information (Blau), Warnung (Gelb), Erfolg (Grün) und destruktive Aktionen (Rot). Lange Aktionslisten lassen sich innerhalb der Sidebar scrollen.

Die eigenständige Help-Seite verwendet die gemeinsame verstellbare Sidebar für Sprache, Themenfilter und Themenliste. Der Hilfetext nutzt den Hauptbereich; darüber bleiben die Suche im Thema und die Suche über alle Themen verfügbar. Thema, Sprache, Themenfilter, Suchtext und Suchmodus werden nach dem Neuladen aus der Seiten-URL wiederhergestellt. Das Guide-Fenster auf anderen Seiten bleibt separat.

Die Sprachbuttons EN/DE verwenden die kompakte gemeinsame Button-Leiste und bleiben auch bei minimaler Sidebar-Breite nebeneinander sichtbar.
