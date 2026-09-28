# Sidebar-Vereinheitlichung – Prüfliste

Stand: 28. September 2026. Diese Liste beschreibt die Vereinheitlichung nach dem Sidebar-Audit.

## Gemeinsamer Standard

- 8 px Außenabstand, 4 px Abstand zwischen Bedienelementen.
- Buttons: mindestens 32 px hoch, 6/10 px Innenabstand, 6 px Eckenradius, 13 px Schrift.
- Lange Beschriftungen umbrechen; die Höhe wächst mit dem Inhalt.
- Gleiche neutrale Buttons, Hover-Zustände, blaue Auswahl und sichtbarer Tastaturfokus.
- Aktionsfarben: Türkis für primäre Aktionen, Blau für Information, Rot für destruktive Aktionen; Grün/Gelb für Erfolg/Warnung. Zustandsanzeigen behalten ihre Bedeutung.
- Eingaben und Auswahlfelder: 32 px hoch, gleiche Ränder und Schrift.
- Konto-, Dashboard- und VPS-Einträge: gleiche Abstände und Auswahlumrandung; zusätzliche Informationen bleiben erhalten.
- Die bereits gespeicherte, seitenübergreifende Breite bleibt bestehen (160–420 px). Auf schmalen Fenstern wird die Sidebar oberhalb des Inhalts dargestellt.

Zentrale Regeln: `frontend/css/sidebar_layout.css`. Überholte lokale Strukturregeln wurden entfernt. `sidebar_controls.css` bleibt für zusätzliche Inhalts-/Editor-/Vorschau-Komponenten bestehen; die Sidebar-Darstellung wird zentral bestimmt. Alle 19 Seiten laden die aktualisierte Asset-Version.

## Seitenweise Prüfung

| Seite / Datei unter `frontend/` | Was angepasst wurde | Was prüfen? |
| --- | --- | --- |
| API Keys – `api_keys_editor.html` | Header-/Toolbar-Abstände, Aktionsfarben, Button-Geometrie und Auswahl vereinheitlicht. | Add User, Expiry Check, TradFi, Comments, Backups und Logs; aktiver Eintrag. |
| Services – `services_monitor.html` | Navigation von 2 px auf 4 px Zwischenraum; gleiche Buttons und Auswahl auch im schmalen Fenster. | Overview, Workers und einzelne Dienste anklicken; Statuspunkte sichtbar. |
| Logging – `logging_monitor.html` | Header und Toolbar in gemeinsamen Sticky-Bereich aufgenommen; Abstände und Buttons vereinheitlicht. | Log Viewer/Settings wechseln; Log-Viewer-Inhalt unverändert bedienbar. |
| DB Tools – `db_tools.html` | Flache Sidebar in gemeinsamen Header-/Toolbar-Aufbau überführt; 12 px Außenabstand durch 8 px ersetzt; 8 px Button-Radius durch 6 px. | Alle sechs Bereiche; schmaler Bildschirm mit sichtbarem Titel und vertikaler Navigation. |
| VPS Manager – `vps_manager.html` | Aktions-, Metadaten- und Hostlistenabstände, Buttons, Statusfarben und Eingaben vereinheitlicht. Lange Aktionsbereiche scrollen; Icon-Buttons umbrechen bei Platzmangel. | Overview, Master und VPS auswählen; lange Update-Beschriftungen, Warn-/Erfolgsfarben, Hostliste und Ende langer Aktionslisten. |
| VPS Monitor – `vps_monitor.html` | Header, Toolbar, Buttons und Auswahl vereinheitlicht; keine kleinere Sidebar-Button-Schrift bei schmalen Fenstern. | Dashboard, Instances, Services, Live Logs und vorhandene Checkboxen. |
| Dashboard – `dashboard_main.html` | Toolbar-Aktionen als kompakte, umbrechende Icon-Leiste (32 × 32 px); Suche und Listenabstände angeglichen; Auswahlrahmen vereinheitlicht. | Anzeigen/Bearbeiten, Speichern, Suche, Dashboard-Auswahl und eingebettete Inhalte. |
| Market Data – `market_data_main.html` | Header-/Toolbar-Abstände und Buttons vereinheitlicht; aktive Navigation blau; Warnzustand ungespeicherter Einstellungen bleibt erhalten. | Settings, Status Monitor, OHLCV-Unterbereiche, Kontextaktionen und Save-Needed-Hervorhebung. |
| Coin Data – `coin_data.html` | Header, Toolbar, Aktionsbuttons und Farben vereinheitlicht; lange Beschriftungen umbrechen. | Exchange-/CMC-Aktionen und Wechsel zwischen Symbolansichten. |
| Profit Sweep – `profit_sweep.html` | Kontoliste erhält 8 px Außenabstand/4 px Zwischenraum; Buttons, Suche und Kontoauswahl vereinheitlicht. | Overview/Kontowechsel, Suchfilter, lange Kontonamen und Metadaten. |
| Transfers – `transfers.html` | Gleiche Suche, Kontozeilen und Auswahl wie Profit Sweep; unterschiedliche feste Mindesthöhen der Kontozeilen entfallen zugunsten inhaltsabhängiger Höhe. | Suche, Kontowechsel, Kontotyp-/Exchange-Angaben und deaktivierte Kontowahl während einer Aktion. |
| Welcome – `welcome.html` | Header, Toolbar, Buttons und Auswahl vereinheitlicht. | Overview/Setup/Password, Status-Badges und gesperrte Password-Aktion. |
| Cluster – `cluster.html` | Gemeinsame Navigation mit 13 px Schrift, 32 px Mindesthöhe und 8 px Außenabstand. | Bereichswechsel, Zähler und aktive Auswahl. |
| PB7/PB8 Run – `v7_run.html` | Filter-/Toolbar-Abstände und Controls vereinheitlicht; redundanter Refresh-Button entfernt, bestehende Live-Aktualisierung bleibt aktiv. | Search/Status, Add Instance, Backups und automatische Tabellenaktualisierung. |
| PB7/PB8 Run-Editor – `v7_edit.html` | Header, Aktionsbuttons, Abstände und Farben vereinheitlicht. | Save, Import, Copy, Navigation zu anderen Werkzeugen und lange Beschriftungen. |
| PB7/PB8 Backtest – `v7_backtest.html` | Navigation und Kontextaktionen vereinheitlicht, einschließlich der dynamisch erzeugten Sidebar und Editor-Ansicht. | Configs/Queue/Results/Archive, jeweilige Aktionen und Editor öffnen/schließen. |
| PB7/PB8 Optimize – `v7_optimize.html` | Navigation, Kontextaktionen und Eingaben vereinheitlicht. | Configs/Queue/Results/Paretos, Editor, ausgeblendete Aktionen und Zähler. |
| Pareto Explorer – `v7_pareto_explorer.html` | Navigation und Aktionsgruppe vereinheitlicht; bisheriges `display:block` entfernt, damit 4 px Zwischenräume greifen. | Bereichswechsel, Queue Validation, Periodenauswahl und deaktivierte Scan-Aktion. |
| Strategy Explorer – `v7_strategy_explorer.html` | Navigation, Außenabstände und Auswahl vereinheitlicht. | Alle Ansichten einschließlich Compare/Movie Builder; schmale Fenster. |

## Empfohlener Prüfdurchlauf

1. Browser neu laden, damit die neuen CSS-Versionen verwendet werden; kein Dienstneustart erforderlich.
2. Sidebar einmal schmal und einmal breit ziehen, Seite wechseln und gespeicherte Breite prüfen.
3. Auf den oben genannten Seiten aktive, deaktivierte und farbige Aktionen vergleichen.
4. Fenster unter 760 px verkleinern: Navigation und Hauptinhalt müssen erreichbar und scrollbar bleiben.
5. Besonders VPS Manager mit langen Aktionslisten, Dashboard im Edit-Modus sowie Backtest/Optimize mit wechselnden Kontextaktionen prüfen.

Die automatischen Browserprüfungen verwenden echte Seitenlayouts und lokale CSS-Dateien mit isolierten/mocked Requests. Produktive Aktionen wie Transfers, Speichern oder Dienststeuerung wurden dabei nicht ausgeführt.

Kompakte Icon-Leisten sind eine gemeinsame Sidebar-Variante: `sb-icon-row` am Toolbar-Container. Direkte `sb-btn`-Kinder werden quadratisch und zentriert; Farben und Fokuszustände bleiben dieselben. Text-Aktionslisten verwenden weiterhin volle Breite.
