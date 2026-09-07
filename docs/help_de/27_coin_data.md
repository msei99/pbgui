# Coin Data

Die Coin-Data-Seite ist jetzt in der FastAPI-UI verfuegbar und konzentriert sich auf Mapping-Qualitaet, CoinMarketCap-Abdeckung und Exchange-Limits, ohne die eigentliche CoinData-Service-Logik zu veraendern.

## Was diese Seite macht

- Exchange-spezifische Symbol-Mappings erstellen und aktualisieren
- CoinMarketCap-Daten wie Rank, Market Cap, Tags und Metadaten zusammenfuehren
- Live-Preise und das abgeleitete `vol/mcap` aktualisieren
- Copy-Trading-Verfuegbarkeit und Exchange-Limits wie Min Amount, Min Cost, Precision und Leverage anzeigen

## FastAPI-Layout

Die Seite nutzt die normale FastAPI-Shell:

- gemeinsame Top-Navigation und About-Dialog
- linke Sidebar fuer Aktionen und View-Toggles, mit anpassbarer Breite auf dem Desktop
- Hauptbereich mit Filterleiste oben und genau einer aktiven Haupttabelle darunter
- Tabellenbereiche mit demselben Header-Stil wie die bestehenden FastAPI-Seiten fuer Backtest und Run

Der Guide-Button im FastAPI-Header oeffnet das gemeinsame Help-System.

Browser-Anfragen verwenden das HttpOnly-PBGui-Session-Cookie; der Session-Token wird weder in die Seite gerendert noch in einem Browser-Bearer-Header gesendet. API- und lokale Asset-Pfade behalten den konfigurierten ASGI-Mount-Prefix. Help-Markdown wird bereinigt; Links und Bilder werden vor der Anzeige auf HTTP(S)-URLs begrenzt.

## Sidebar-Aktionen

- `Refresh Selected Exchange`
  - Holt Markets, aktualisiert den Copy-Trading-Cache, baut das Mapping neu und aktualisiert Preise fuer die aktuelle Exchange
- `Refresh All Exchanges`
  - Fuehrt denselben Ablauf fuer alle V7-Exchanges aus
  - Wenn nur einige Exchanges erfolgreich sind, ist deren neuer Stand sofort verfuegbar und die Statuszeile nennt die fehlgeschlagenen Exchanges; schlagen alle Exchanges fehl, wird der Lauf als Fehler gemeldet
- `Refresh CMC + Selected Exchange`
  - Laedt CMC-Listings und Metadaten neu und aktualisiert danach die gewaehlte Exchange, damit die sichtbare Tabelle die neuen CMC-Daten sofort nutzt
  - Zeigt waehrend des laufenden bestehenden Refresh-Workflows ein zentriertes Busy-Overlay mit echten Prozentwerten auf Basis der bereits abgeschlossenen Refresh-Schritte
- `Refresh CMC + All Exchanges`
  - Laedt CMC-Listings und Metadaten neu und baut danach alle Exchanges neu auf, damit alle Exchange-Mappings in einem Lauf auf den neuen CMC-Datenstand gebracht werden
  - Nutzt dasselbe Busy-Overlay mit echten Prozentwerten fuer den laengeren Komplettlauf
  - Nutzt dieselbe Partial-/Fehler-Zusammenfassung wie `Refresh All Exchanges` und behaelt Exchange-spezifische Diagnosen fuer das Polling
- `Matched Symbols`
  - Zeigt nur die gematchte Haupttabelle an
- `CMC Unmatched`
  - Zeigt nur die nicht gematchten CMC-Symbole an
- `HIP-3 Symbols`
  - Zeigt nur die Hyperliquid-HIP-3-Tabelle an und wird nur fuer die Exchange `hyperliquid` angezeigt
- `Only Copy Trading`
  - Beschraenkt die Haupttabelle auf Copy-Trading-Symbole und wird nur fuer Exchanges mit unterstuetztem Copy-Trading-Filter angezeigt (`bybit`, `binance`, `bitget`)

## Frische-Info

Coin Data zeigt den Frische-Status als eine gemeinsame Inline-Zeile neben `Filtered symbols`.

- Sichtbar ist eine kompakte Zusammenfassung fuer den Refresh der gewaehlten Exchange und den letzten CMC-Refresh.
- Hover auf diese Inline-Zeile zeigt die detaillierten Zeitstempel fuer Markets, Mapping, Preise, Copy-Trading-Cache, Listings und Metadaten.

## CMC-Readiness

CMC-Refresh-Aktionen benötigen mindestens eine aktive Credential-Generation im lokal materialisierten CMC-Pool. Coin Data prüft das vor dem Start eines CMC-Jobs und meldet bei fehlendem Pool einen direkten Fehler; reine Exchange-Refreshes bleiben verfügbar. Keys werden unter **System -> Services -> PBCoinData -> Pool** angelegt und geprüft, nicht in `pbgui.ini`.

Auf VPS-Zielen besteht Readiness aus secret-freien Capability-Metadaten: Protokollversion, aktive Key-Anzahl sowie Katalog-/Materialisierungs-Generation. Ein Node ist erst bereit, nachdem die benötigte versiegelte Generation lokal materialisiert wurde.

## Filter und Tabellenverhalten

Hauptfilter:

- Exchange
- Quotes ueber einen kompakten Multiselect. Coin Data verwendet standardmaessig `USDT`, wenn diese Quote vorhanden ist; bei Hyperliquid sind `USDC` und `USDT0` gemeinsam vorausgewaehlt. Jede Quote-Familie aus dem Mapping der gewaehlten Exchange kann explizit ausgewaehlt werden.
- Minimum `market_cap` wird bereits waehrend der Eingabe angewendet, behaelt stabile Dezimaleingaben waehrend des Tippens und nutzt `250` als Editor-typischen `+/-`-Schritt
- Maximum `vol/mcap` wird bereits waehrend der Eingabe angewendet, behaelt direkte Dezimaleingaben wie `0.` und `0,` waehrend des Tippens und laesst `+/-` ueber lesbare gerundete Schwellen aus den aktuellen Exchange-Daten springen statt ueber winzige Rohwert-Schritte
- Tags ueber denselben suchbaren Chip-Multiselect wie in PBv7 Run/Backtest, ohne Checkboxen im Dropdown
- `Reset`-Button rechts in der Filterzeile, um den Standard-Filterzustand wiederherzustellen

FastAPI-UI-Verbesserungen:

- Sticky-Header in scrollbaren Tabellen
- die HIP-3-Tabelle behaelt auf dem Desktop ihren eigenen Scroll-Container, damit lange Symbol-Listen benutzbar bleiben, und der dedizierte `DEX`-Selektor sitzt in der HIP-3-Kopfzeile statt in der globalen Filterleiste
- dichtere Tabellenzeilen und kompakte Tag-Chips, damit weniger vertikaler Platz verschwendet wird
- Vollbreiten-Tabellen mit ausgewogener Spaltenverteilung, damit die verfuegbare Breite genutzt wird ohne uebergrosse Luecken zwischen den Werten
- die aktive Desktop-Tabelle nutzt die verbleibende Fensterhoehe, statt unten Leerraum unter der Tabelle zu lassen
- sortierbare Tabellen-Header fuer Matched-, Unmatched- und HIP-3-Ansicht
- nicht verfuegbare CMC-Werte fuer Market Cap, 24-Stunden-Volumen und `vol/mcap` werden als `N/A` angezeigt und nach bekannten Werten sortiert
- Hover-Tooltips fuer Tags, Notices und lange Werte
- Row-Selection mit zentrierter schwebender Detailkarte, die sich beim Oeffnen soweit moeglich automatisch an den Inhalt anpasst, sich an jeder Seite und Ecke in der Groesse aendern und verschieben laesst, alle Tags ohne Abschneiden zeigt, einen direkten `Open CMC`-Link bei vorhandener CoinMarketCap-Zuordnung bietet und per `X` statt nur ueber Notice-Text geschlossen wird
- genau eine aktive Haupttabelle, umschaltbar ueber die Sidebar, statt mehrere Bereiche gleichzeitig anzuzeigen
- eine einzeilige Desktop-Filterleiste ohne separaten `Filters`-Titelblock

Die Seite enthaelt:

- **Matched symbols** Tabelle: gematchte Nicht-HIP-3-Zeilen nach Filtern
- **CMC unmatched** Tabelle: Symbole ohne aktuelle CMC-Zuordnung
- **HIP-3 symbols** Tabelle (nur Hyperliquid)
- Der Sidebar-Button `HIP-3 Symbols` ist bei allen Nicht-Hyperliquid-Exchanges ausgeblendet.
- Der Sidebar-Button `Only Copy Trading` ist bei Exchanges ohne unterstuetzte Copy-Trading-Erkennung ausgeblendet.

## Hyperliquid Hinweise

- Quote-Prioritaet ist standardmaessig `USDC`, danach `USDT0`
- Sind beide Quote-Familien verfuegbar, umfasst die Voreinstellung beide; eine explizite Auswahl filtert Matched-, Unmatched- und HIP-3-Zeilen. Diese Auswahl in Coin Data aendert nicht die USDT-Linear-Regeln fuer PB7-/PB8-Konfigurationen.
- Wenn keine HIP-3-Symbole gefunden werden, kann Coin Data das Hyperliquid-Mapping einmal automatisch neu bauen
- HIP-3-Zeilen werden separat angezeigt und nutzen den eigenen `DEX`-Filter; CMC-basierte Filter wie `market_cap`, `vol/mcap` und Tags gelten fuer gematchte Nicht-HIP-3-Zeilen

## Daten-Dateien

Coin Data liest und schreibt unter:

- `data/coindata/coindata.json`
- `data/coindata/metadata.json`
- `data/coindata/<exchange>/ccxt_markets.json`
- `data/coindata/<exchange>/mapping.json`
- `data/coindata/<exchange>/copy_trading.json`

## Troubleshooting

### Keine Zeilen sichtbar

- Die ausgewaehlte Exchange aktualisieren
- Filter testweise lockern (`market_cap=0`, hoeheres `vol/mcap`, keine Tags)
- CMC- und Mapping-Zeitstempel pruefen
- `Only Copy Trading` deaktivieren, falls aktiv

### Preis fehlt bei einigen Symbolen

- Die ausgewaehlte Exchange erneut aktualisieren
- Pruefen, ob der Markets-Datei-Zeitstempel aktuell ist
- Bei Hyperliquid beachten: manche Symbole nutzen Market-Info-Fallback-Preise

### CMC unmatched ist hoch

- Erst CMC-Daten aktualisieren, dann die ausgewaehlte Exchange
- Pruefen, ob Symbole neu gelistet sind oder Exchange-spezifische Namensvarianten verwenden
- Wenn der CMC-Refresh blockiert ist, vor dem Retry die Readiness des PBCoinData-Pools prüfen
