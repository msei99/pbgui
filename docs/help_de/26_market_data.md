# Market Data

Diese Seite steuert die PBGui-Market-Data-Workflows für Hyperliquid, Binance USDM und Bybit: l2Book-Archiv-Downloads, TradFi-Symbol-Mapping, 1m Auto-Refresh-Loops und Build best 1m OHLCV Jobs.

## Empfohlener Workflow — Best Practice

Das ist der schnellste und speichereffizienteste Weg, damit alle Coins aktuell sind und Backtests sofort starten.

### Schritt 1 — Alle Coins für Auto-Refresh aktivieren

1. **Settings (Binance USDM Latest 1m Auto-Refresh)** öffnen → **Select all** → **Save**
2. **Settings (Latest 1m Auto-Refresh) — Hyperliquid** öffnen → **Select all** → **Save**
3. Exchange-Dropdown auf **Bybit** umschalten → **Settings (Bybit Latest 1m Auto-Refresh)** öffnen → **Select all** → **Save**

Damit sind alle Coins für den fortlaufenden Update-Loop registriert. Der Loop hält die letzten Tage automatisch aktuell — nach dem ersten vollständigen Download ist kein weiterer manueller Eingriff nötig.

### Schritt 2 — „Build best 1m all" für den initialen Backfill starten

Unter **Build best 1m OHLCV** auf **Build best 1m all** klicken (oder alle Coins auswählen und abschicken).

Damit wird ein Background-Job pro Exchange gestartet, der die komplette Historie von Inception bis heute herunterlädt:

| Exchange | Download-Methode | Erwartete Dauer (erstmaliger Download) |
|---|---|---|
| **Binance** | Parallele monatl. + tägl. ZIPs (data.binance.vision) + CCXT-Lückenfüllung | ~2–4 Stunden (~550 Coins) |
| **Bybit** | CCXT (async) | ~3 Stunden (~550 Coins) || **Hyperliquid** (Crypto) | l2Book-Archiv + 1m\_api-Konvertierung | abhängig von der l2Book-Archivgröße |
| **Hyperliquid** (XYZ Stock-Perps) | Tiingo IEX/FX 1m | abhängig von der Anzahl gemappter Symbole + Tiingo-Quota |
**Gemessene Werte aus echten Jobs:**
- Binance LINK (6+ Jahre, 2 239 Tage, 74 monatliche ZIPs): **41 Sekunden** mit parallelem ZIP-Download
- Binance alle ~550 Coins (parallele ZIPs): **geschätzt 2–4 h** (Hochrechnung: Ø Coin ~3 Jahre ≈ 24 monatliche ZIPs → ~20 s/Coin)
- Bybit alle 548 Coins (CCXT, gemessen): **~3 h** (BTC allein = 102 min, kurze Coins anteilig wenig)

Beide Jobs laufen im Hintergrund. Browser schließen und später zurückkommen ist problemlos. Im **Running**-Panel kann der Fortschritt beobachtet werden.

### Schritt 3 — Letzten abgeschlossenen Job prüfen

Nach Abschluss des Jobs den **Done**-Eintrag im Job-Panel öffnen und **🔍** (Raw JSON) anklicken. Prüfen:
- `status: done` (nicht `failed`)
- `last_result.days_checked` — entspricht der erwarteten Abdeckung
- `last_result.minutes_written` > 0
- Eventuelle `notes`-Einträge (z. B. `monthly_download_failed=...` bedeutet, dass der Daily-ZIP-Fallback für diesen Monat verwendet wurde — normal, wenn das neuste Monats-ZIP noch nicht veröffentlicht ist)

### Schritt 4 — Auto-Refresh hält Daten aktuell

Nach dem initialen Backfill ist das tägliche Update automatisch:

- Binance: letzte **2–7 Tage** werden per CCXT alle 3 600 s (1 h) pro Zyklus aktualisiert
- Bybit: letzte **2–7 Tage** werden per CCXT alle 3 600 s (1 h) pro Zyklus aktualisiert
- Hyperliquid: letzte **2–4 Tage** werden per API alle 1 800 s (30 min) pro Zyklus aktualisiert

Für sofortiges Update auf **⏩ Run now** im jeweiligen **Market Data Status**-Panel klicken.

### Warum diese Vorgehensweise

- **Minimaler Speicherbedarf** — Daten werden als komprimierte `.npz`-Dateien gespeichert (eine pro Tag und Coin); `.npz` ist ~35% kleiner als der unkomprimierte `.npy`-Cache von PB7 — z. B. BTC/USDT Binance: **61 MB** (pbgui `.npz`, Sep 2019 – heute) vs **89 MB** (PB7 `.npy`-Cache, Dez 2019 – heute)
- **Backtests starten sofort** — kein On-Demand-Fetch nötig; die lokalen Dateien sind fertig aufgebaut
- **Inkrementell** — bei einem erneuten „Build best 1m all" werden bereits vollständige Tage übersprungen (Pre-Scan), nur neue Daten werden heruntergeladen
- **Kein doppelter Speicher** — eine `.npz` pro Tag und Coin ersetzt jede frühere partielle Version

---

## Seitenaufbau

Die Expander erscheinen in dieser Reihenfolge:
1. Settings (Latest 1m Auto-Refresh) — Hyperliquid
2. Settings (Binance USDM Latest 1m Auto-Refresh)
3. Market Data status (Hyperliquid)
4. Market Data status (Binance USDM)
5. Build best 1m OHLCV
6. TradFi Symbol Mappings
7. Download l2Book from AWS

## Market-Data-Seite

Die Seite `Market Data` läuft jetzt direkt auf der FastAPI-Implementierung und teilt den Bereich `Settings` in der Sidebar in drei klare Unterbereiche auf:

Die Sidebar selbst ist jetzt reine Navigation: Sie enthält nur die Hauptbereiche der Seite plus die kontextbezogenen `Settings`-Aktionen, aber keine separate Overview- oder Status-Infofläche mehr.

- `Coin Refresh` — Exchange-Refresh-Einstellungen und der Enabled-Coins-Workflow
- `AWS / l2Book` — Hyperliquid-Archiv- und Download-Einstellungen
- `TradFi / Tiingo` — Tiingo-Zugangsdaten und TradFi-Mapping-Steuerung

Der gemeinsame `Guide`-Button auf dieser Seite öffnet dieses `Market Data`-Thema direkt in einem Overlay innerhalb der Seite, sodass die aktuelle Market-Data-Ansicht beim Lesen sichtbar bleibt.

Die Sidebar zeigt keinen separaten Bereich `Actions` mehr. Stattdessen gibt es dort direkte Shortcuts, die innerhalb der Seite bleiben:

- `OHLCV Data` bleibt ebenfalls vollständig in FastAPI: Wenn dieses Panel aktiv ist, zeigt die Sidebar datasetspezifische Buttons für die gewählte Exchange statt interner Tabs.
- `Build Best 1m` öffnet ein eigenes FastAPI-Panel für die aktuell gewählte Exchange.
- `Download l2Books` öffnet bei `Hyperliquid` direkt das eingebettete Hyperliquid-Data-Actions-Panel.

`Build Best 1m` und `Download l2Books` verwenden jetzt außerdem dieselbe aktive Button-Markierung wie die übrigen Market-Data-Sidebar-Einträge, sodass der aktuell geöffnete Shortcut-Bereich direkt in der Sidebar sichtbar bleibt.

Innerhalb dieses FastAPI-`Best 1m`-Panels nutzt Hyperliquid die vollständige Download-/Build-Komponente weiter, aber fokussiert: `Best 1m` zeigt nur den Build-Inhalt, und `Download l2Books` zeigt nur den Download-Inhalt. Die zusätzliche äußere Header-Karte, die Fenster-in-Fenster-Optik und auch der Expander-Kopf entfallen dort, sodass nur noch der eigentliche Formularinhalt sichtbar bleibt.

Hyperliquid `Best 1m` folgt jetzt auch den neueren FastAPI-Editor-Mustern: Für den Build-Zeitraum wird derselbe Popup-Kalender wie in den Backtest-/Optimize-Editoren verwendet, und die Coin-Auswahl erscheint als mehrspaltiges Enabled-Coins-Grid mit `Filter enabled coin list`, `Select visible` und `Clear all` statt des alten kompakten Dropdowns. Die sichtbaren Coin-Zeilen sind jetzt direkt anklickbar und unterstützen zusätzlich Maus-Drag-Selektion, damit größere Bereiche ohne Checkbox-Klicks markiert oder entfernt werden können. Auch schnelle Drag-Bewegungen interpolieren jetzt die Zwischenzeilen, sodass beim zügigen Durchziehen keine Coins mehr übersprungen werden.

Hyperliquid `Download l2Books` verwendet jetzt ebenfalls genau dieses Coin-Grid-Muster statt des alten kompakten Dropdowns. Du kannst die aktivierten Coins filtern, sichtbare Zeilen direkt anklicken, den aktuell gefilterten Ausschnitt gesammelt auswählen, die explizite Auswahl zurücksetzen oder per Maus-Drag größere Download-Bereiche schnell durchziehen. `XYZ-*`- bzw. TradFi-Symbole werden dort ausgefiltert, weil es für sie keinen Hyperliquid-l2Book-Archivdownload gibt. Wenn keine explizite Auswahl gesetzt ist, werden weiterhin alle verbleibenden downloadbaren Coins eingereiht.

Das fokussierte Hyperliquid-Panel passt beim Wechsel zwischen `Best 1m` und `Download l2Books` jetzt außerdem seine eingebettete Höhe korrekt neu an, sodass die kürzere Download-Ansicht keinen leeren Restbereich und keinen zusätzlichen Scrollbalken der zuvor höheren Build-Ansicht mehr mitzieht.

Die eingebettete Hyperliquid-Ansicht vermeidet jetzt außerdem einen zweiten internen Seiten-Scrollbalken, sodass das Scrollen auf der eigentlichen Market-Data-Seite bleibt und nicht zwischen Seite und fokussiertem Panel aufgeteilt wird.

Für diese archivbasierten Exchanges nutzt die Coin-Auswahl jetzt direkt ein Settings-ähnliches Enabled-Coins-Grid im FastAPI-Panel: `Filter enabled coin list` schränkt das Grid ein, `Select visible` übernimmt den aktuell gefilterten Ausschnitt, `Clear all` setzt die explizite Auswahl zurück, und du kannst mit der Maus über sichtbare Coin-Zeilen ziehen, um größere Bereiche schnell zu markieren oder wieder zu entfernen. Schnelle Drag-Bewegungen füllen jetzt auch die Zwischenzeilen, sodass beim zügigen Durchziehen keine Coins mehr zwischen zwei Maus-Events verloren gehen. Wenn die Auswahl leer bleibt, werden weiterhin alle aktivierten Coins eingereiht; sobald du Coins explizit auswählst, wird der `Best 1m`-Job auch genau auf diese Auswahl begrenzt.

Diese FastAPI-`Best 1m`-Ansicht startet für Binance und Bybit jetzt außerdem direkt mit den eigentlichen Build-Feldern. Der redundante Einleitungskopf mit Text und der zusätzliche obere `Refresh`-Button wurden entfernt.

Für Binance und Bybit zeigt das FastAPI-`Best 1m`-Build-Panel außerdem wieder direkt unter dem vollständigen Build-Formular den gefilterten Job Monitor an. Damit bleiben eingereihte, laufende, erfolgreiche und fehlgeschlagene `Best 1m`-Jobs der gewählten Exchange sichtbar, ohne in das separate Activity-Log-Panel wechseln zu müssen.

Dieser Build-Bereich ist jetzt außerdem flacher aufgebaut: Der Coin-/Build-Teil sitzt nicht mehr in einer zusätzlichen abgerundeten Kartenhülle, und der eingebettete Job Monitor blendet seinen Standalone-Seitenkopf aus, sodass die Ansicht wie ein durchgehendes Market-Data-Panel wirkt.

Dieser eingebettete Job Monitor wächst jetzt außerdem mit seiner eigenen Inhaltshöhe mit, sodass im Monitorbereich kein zweiter unnötiger Scrollbalken mehr erscheint, während die äußere Market-Data-Seite bereits scrollbar ist.

Die URL des eingebetteten Monitors trägt jetzt zusätzlich die aktuelle PBGui-Serial als Cache-Buster. Dadurch lädt das iframe nach Frontend-Updates auch wirklich die neue `jobs_monitor.html`, und neue Aktionen wie `View` bleiben nicht mehr an einer alten gecachten Monitor-Version hängen.

Hyperliquid verwendet statt dieses gemeinsamen iframes eine eigene Inline-Data-Actions-Seite. Dieser Inline-Job-Monitor zeigt jetzt ebenfalls die `View`-Aktion für aktive, erfolgreiche und fehlgeschlagene Jobs an, sodass dieselbe Detailansicht wie unter `System -> Services` verfügbar ist. Pending-Zeilen in beiden Monitor-Varianten bieten jetzt zusätzlich `Run`; damit wird ein zusätzlicher manueller Parallel-Slot für denselben Job-Typ angefordert, sodass genau ein ausgewählter Pending-Job neben dem bereits laufenden Job dieses Typs starten kann. Aktive Zeilen bleiben jetzt außerdem in einer stabilen Queue-/Start-Reihenfolge, sodass Live-Progress-Updates zwei laufende Jobs nicht mehr ständig gegeneinander umsortieren. `View`- und `Log`-Dialoge sind jetzt ebenfalls auf den sichtbaren Browser-Viewport begrenzt und berücksichtigen zusätzlich sowohl die Browser-Scrollposition als auch clippende Eltern-Panels wie den scrollbar eingebetteten `Build Best 1m`-Bereich, sodass der Close-Button innerhalb des tatsächlich sichtbaren Monitorbereichs bleibt statt darüber zu öffnen.

Auch seine Aktionsdialoge sind jetzt im Seitenstil eingebettet: Cancel-, Delete-, Retry-, Requeue- und Bulk-Delete-Bestätigungen fallen nicht mehr auf browsernative Popup-Fenster zurück.

Das FastAPI-Panel `OHLCV Data` folgt jetzt demselben Paritätsziel. Für die gewählte Exchange erscheinen die Dataset-Buttons direkt in der Sidebar: `1m` und `PB7 cache` sind immer verfügbar, auf Hyperliquid zusätzlich `1m_api` und `l2Book`. Im Hauptbereich bleibt dann derselbe Ablauf wie in Streamlit erhalten: Summary-Metriken, eine filterbare Inventory-Tabelle, Delete-Tools für schreibbare Datasets, eine Coverage-Heatmap, bei Verfügbarkeit eine Minute-Heatmap und optional ein OHLCV-Detailchart. `PB7 cache` bleibt read-only.

Dieses FastAPI-OHLCV-Detailchart nutzt jetzt ebenfalls dieselbe Lazy-Zoom-Strategie wie die Streamlit-Version. Das Iframe lädt anfangs nur grobe Layer, dadurch öffnen lange Historien wieder zuverlässig, und feinere Kerzen werden beim Reinzoomen gezielt nachgeladen statt den kompletten `15m`-/`5m`-/`1m`-Pyramiden-Block sofort einzubetten.

Auch das Iframe-Template selbst wird jetzt wieder als echtes HTML/JS ausgeliefert, sodass der Chart nicht mehr wegen versehentlich escapeter Anführungszeichen im eingebetteten Script auf einem leeren `Loading chart...`-Panel stehen bleibt.

In Hyperliquid `OHLCV Data` → `l2Book` gibt es jetzt zusätzlich einen standardmäßig ausgeschalteten Toggle neben `Select All` / `Deselect`, der aktivierte non-XYZ Coins ohne jegliche l2Book-Dateien mit in die Tabelle aufnimmt. Dadurch lassen sich Coins mit komplett fehlender l2Book-Abdeckung direkt in der Inventory-Tabelle erkennen, statt nur Coins zu sehen, für die bereits mindestens eine Archivstunde vorhanden ist.

Die `OHLCV Data`-Sidebar bleibt jetzt wieder button-only. `Delete older than` wurde durch `Delete by Date` ersetzt; ein Klick darauf öffnet ein kleines Fenster mit Date-Picker und Löschvorschau, statt diesen Zusatzblock dauerhaft in der Sidebar einzublenden.

Dieses Fenster folgt jetzt außerdem stärker dem klareren Datumsfeld-Muster aus dem Backtest-Editor: Das Cutoff-Feld hat einen gut sichtbaren Kalender-Button, und der aktuelle Löschumfang zeigt die ausgewählten Coin-Namen in einer kleinen scrollbaren Liste, damit Mehrfachlöschungen vor dem Bestätigen eindeutig bleiben.

Auch die letzte Löschbestätigung bleibt jetzt im PBGui-Stil: Statt des browsernativen Popups öffnen Delete-Aktionen ein zentriertes Bestätigungsfenster mit aktuellem Scope und, falls passend, den ausgewählten Coins.

Sobald du in `OHLCV Data` einen oder mehrere Coins auswählst, erscheint in der Sidebar jetzt die Queue-Aktion passend zum aktuellen Dataset-View. In `1m`, `1m_api` und `PB7 cache` bleibt das `Build best 1m` für die ausgewählten Coins auf der aktuellen Exchange. In Hyperliquid `l2Book` erscheint stattdessen eine l2Book-Download-Queue-Aktion für genau diese ausgewählten Coins, sodass dort nicht mehr der unpassende Best-1m-Job angeboten wird. Die Inventory-Sidebar selbst ist jetzt button-only: Queue-/Delete-Bestätigungen und Fehler bleiben nicht mehr als persistente Sidebar-Hinweise stehen, sondern laufen über die normalen Toast-/Notification-Meldungen bzw. die vorhandenen Bestätigungsdialoge. Die sichtbaren Coin-Beschriftungen in diesem Inventory-Bereich zeigen jetzt außerdem nur noch den Short-Namen, also in Tabelle, Sidebar-Aktionsbuttons sowie in den Heatmap-/OHLCV-Beschriftungen.

Im `PB7 cache` gibt es oberhalb der Tabelle jetzt außerdem einen kleinen Timeframe-Schnellfilter direkt neben `Select All` und `Deselect`. Damit kannst du vor der Auswahl zwischen `all`, `1m` und `1h` umschalten, sodass Short-Name-Dubletten wie `ADA` getrennt nach Cache-Timeframe eingegrenzt werden können.

In den Hyperliquid-Inventory-Views unterstützt der Typ-Filter jetzt zusätzlich `xyz only`, `xyz mapped` und `xyz not mapped`. Außerdem zeigt die Tabelle dort eine Spalte `mapping`, damit du direkt den effektiven TradFi-Mapping-Status für jedes sichtbare XYZ-Instrument siehst, also z. B. `mapped`, `no provider` oder `pending`. Aktive XYZ-Instrumente werden dabei nicht mehr nur wegen eines veralteten Eintrags in `tradfi_symbol_map.json` als `delisted` angezeigt; solange das aktuelle Hyperliquid-Mapping den Coin noch führt, löst PBGui jetzt einen aktiven Nicht-`delisted`-Status auf.

Die Inventory-Tabelle verwendet jetzt außerdem dasselbe Maus-Selektionsverhalten wie die FastAPI-Tabellen in Backtest/Optimize: Ein Klick toggelt genau eine Zeile, ein Drag über mehrere Zeilen fügt einen zusammenhängenden Bereich hinzu oder entfernt ihn wieder, und `Select All` markiert nur die aktuell nach Filter sichtbaren Zeilen.

Zusätzlich sind die Spaltenköpfe der Inventory-Tabelle jetzt sortierbar. Ein Klick auf einen Header schaltet für die aktuell sichtbaren Zeilen in dieser Dataset-Ansicht zwischen aufsteigender und absteigender Reihenfolge um.

## Settings (Latest 1m Auto-Refresh) — Hyperliquid

Steuert den automatischen 1m-Candle-Refresh-Loop für Hyperliquid-Symbole.

- **Enabled coins** — Multiselect aus allen bekannten Hyperliquid-Symbolen
- **Select all / Clear all** — alle Coins schnell aktivieren oder deaktivieren
- **Cycle interval (s)** — wie oft alle aktivierten Coins aktualisiert werden (Standard: 1800s)
- **Pause between coins (s)** — Pause zwischen Coins um Rate-Limits zu vermeiden (Standard: 0,5s)
- **API timeout per coin (s)** — Timeout pro Coin (Standard: 30s)
- **Min / Max lookback days** — Fenster für den letzten Fetch (Standard: 2 / 4 Tage)
- Änderungen werden in `pbgui.ini` gespeichert und im nächsten Zyklus wirksam — kein Neustart nötig.

Hyperliquid-Latest-1m-Catch-up-Requests können jetzt korrekt das volle konfigurierte 4-Tage-`candle_snapshot`-Budget reservieren. Zuvor konnte ein Burst-Cap-Mismatch im lokalen Rate-Limiter wiederholte `budget_timeout`-Resultate erzwingen, obwohl der API-Request selbst gültig war.

## Settings (Binance USDM Latest 1m Auto-Refresh)

Steuert den automatischen 1m-Candle-Refresh-Loop für Binance USDM Perpetuals.

- **Enabled coins** — Multiselect aus allen bekannten Binance USDM Coins
- **Select all / Clear all** — alle Coins schnell aktivieren oder deaktivieren
- **Cycle interval (s)** — wie oft alle aktivierten Coins aktualisiert werden (Standard: 3600s)
- **Pause between coins (s)** — Pause zwischen Coins (Standard: 0,5s)
- **API timeout per coin (s)** — Timeout pro Coin (Standard: 30s)
- **Min / Max lookback days** — Fenster für den letzten Fetch (Standard: 2 / 7 Tage)
- Änderungen werden in `pbgui.ini` gespeichert und im nächsten Zyklus wirksam — kein Neustart nötig.

## Market Data Status

Dieser Bereich dient zur Überwachung von Fetch-Loops, Inventar und Background-Jobs.

Der Status-Expander aktualisiert sich automatisch alle 5 Sekunden.

Kurze Toast-Meldungen aus dem Market-Data-Status-Panel und der Gap-Heatmap werden jetzt zusätzlich in PBGuis globales Notification-Log geschrieben. Dadurch lassen sie sich später auch über die Glocke oben rechts erneut öffnen, statt nur kurz im Panel sichtbar zu sein.

### Steuer-Buttons

- **⏩ Run now** — überspringt die verbleibende Wartezeit und startet den nächsten Refresh-Zyklus sofort
- **⏹ Cancel queued refresh** — erscheint statt Run now, wenn bereits ein Refresh eingereiht ist; bricht ihn vor dem Start ab
- **⏹ Stop current run** — erscheint während eines laufenden Zyklus; sendet ein Stop-Signal, sodass PBData nach dem aktuellen Coin abbricht

### Fortschrittsbalken

Während ein Zyklus läuft, zeigt ein Fortschrittsbalken `erledigte / gesamt Coins` und den aktuellen Coin.

### Status-Tabelle

Zeigt das Ergebnis des letzten abgeschlossenen Zyklus pro Coin:
- Angezeigt werden nur Coins aus der aktuell wirksamen Enabled-Coins-Menge; der FastAPI-Monitor filtert veraltete Zeilen sofort heraus, und der nächste PBData-Zyklus entfernt sie zusätzlich aus dem gespeicherten Status.
- `last_fetch` — Zeitstempel des letzten Versuchs
- `result` — `ok`, `error` oder `skipped`
- `lookback_days` — abgerufene Tage
- `minutes_written` — geschriebene Candles in diesem Lauf
- `note` — `no_local_data` bedeutet: noch keine lokalen Daten vorhanden; maximales Lookback-Fenster wurde automatisch verwendet
- `next_run_in_s` — geschätzte Sekunden bis zum nächsten Zyklus

### Verhalten nach Neustart

Wenn PBData neu gestartet wird, liest es den letzten Lauf-Timestamp und wartet die verbleibende Intervallzeit ab — kein sofortiger Re-Fetch. Bei einem Absturz mitten im Zyklus wird der Lauf ab dem letzten abgeschlossenen Coin fortgesetzt.

---
- Read-only Inventar für PBGui- und PB7-Cache-Daten
- Source-Code-basierte Coverage-Ansichten
- Job-Fortschritt mit Tages-/Monatskontext bei Stock-Perp-Builds
- In der Stock-Perp-Minute-Ansicht können die Overlays `market holiday` und `expected out-of-session gap` ausgeschaltet werden, um rohe Missing-Gaps direkt zu sehen
- Die Minute-Ansicht enthält optional einen `OHLCV chart`-Expander mit interaktiven Plotly-Candles und Volume-Balken zur schnellen visuellen Prüfung
- Die Overview- und Minute-Heatmaps auf der FastAPI-Seite haben Plotly-Wheel-Zoom jetzt deaktiviert, und ihre Plotly-Modebar erscheint nur noch bei Hover. Normales Scrollen über diese Heatmaps zoomt sie dadurch nicht mehr versehentlich, aber die Plot-Tools bleiben bei Bedarf erreichbar
- Der Chart nutzt Lazy-Zoom: vollständig herausgezoomt werden grobe Kerzen (typisch `1d`) angezeigt, beim Reinzoomen wird automatisch auf feinere Timeframes umgerechnet — keine manuelle Timeframe-Auswahl nötig
- Auf der FastAPI-Seite werden diese feineren Kerzen im Iframe bedarfsgesteuert nachgeladen; dadurch bleiben auch sehr lange Historien reaktionsfähig, statt die komplette Feinauflösung direkt vorab zu laden
- Diese FastAPI-Lazy-Loads nutzen jetzt deutlich kleinere timeframe-spezifische Fenster und laden nur noch genau den aktuell benötigten feinen Layer nach, wodurch sich Zooms spürbar direkter anfühlen
- Das FastAPI-Chart startet außerdem im Pan-Modus und hält seine Y-Achsen beweglich, sodass du die sichtbaren Kerzen nach dem Zoomen auch vertikal nach oben oder unten ziehen kannst, statt an der Auto-Position festzuhängen
- Das FastAPI-Chart behält jetzt außerdem den von dir gewählten Plotly-Interaktionsmodus über Rerenders hinweg bei und begrenzt Pan/Zoom wieder auf den echten Candle-Bereich, sodass es nicht mehr unerwartet das Tool wechselt oder in ein leeres Chartfenster abrutscht
- Veraltete FastAPI-Zoom-Requests werden jetzt sofort abgebrochen, sobald du den sichtbaren Bereich erneut veränderst, und Pans innerhalb desselben Timeframes vermeiden unnötige Re-Layouts, solange sich der sichtbare Span nicht wirklich geändert hat. Dadurch reagiert das Chart bei schneller Inspektion ruhiger und direkter
- An den Datenrändern behält FastAPI jetzt deinen aktuellen Zoom-Span bei und schiebt ihn nur an die nächstgültige Grenze, statt auf den Vollbereich zurückzuspringen. Dadurch fühlt sich das Ziehen am Rand deutlich natürlicher an
- FastAPI merged jetzt außerdem neu geladene feine Fenster in den bereits vorhandenen Client-Layer, statt ihn zu ersetzen. Dadurch verschwinden gerade betrachtete Candles nicht sofort wieder, sobald du ein Stück weiter schiebst
- FastAPI behandelt jetzt außerdem Zoom- und Pan-Clamps unterschiedlich: Zooms werden auf die tatsächlich ausgewählte Datenüberlappung zugeschnitten, während Pans am Rand ihren Span behalten. Dadurch verhält sich ein Rechteck-Zoom deutlich näher an dem Bereich, den du wirklich markiert hast
- Wenn du wieder herauszoomst, aber noch im selben feinen Timeframe bleibst, lädt FastAPI jetzt denselben Timeframe erneut nach, sobald das gecachte Client-Fenster den sichtbaren Bereich nicht mehr weitgehend abdeckt. Damit verschwinden keine großen Lücken mehr, obwohl der Chart weiterhin z.B. `1m` anzeigt
- FastAPI verfolgt jetzt außerdem bereits geladene feine Fenster als getrennte Client-seitige Coverage-Intervalle, statt sie zu einem einzigen `erste Candle .. letzte Candle`-Block zusammenzufassen. Dadurch erkennt der Zoom-Out-Check echte Lücken zwischen zuvor geladenen Fenstern und lädt sie nach, statt leere Bereiche im sichtbaren Chart stehen zu lassen
- Wenn FastAPI denselben feinen Timeframe nachlädt, zeichnet es jetzt auch die Plotly-Traces neu statt nur das Layout zu verschieben. Dadurch werden neu geholte Candles sofort sichtbar, statt dass der Chart weiterhin z.B. `1m` anzeigt, aber der fehlende Abschnitt optisch leer bleibt
- FastAPI prüft jetzt außerdem die tatsächliche Anzahl geladener Candles im aktuell sichtbaren Fenster desselben Timeframes. Wenn ein `1m`-/`5m`-/`15m`-Ausschnitt trotz passendem Badge effektiv leer ist, wird derselbe Timeframe erneut geladen, statt sich nur auf die Coverage-Heuristik zu verlassen
- FastAPI normalisiert jetzt außerdem Plotly-Relayout-Ranges ohne explizite Zeitzone, bevor der Chart geklemmt oder neu gerendert wird. Tiefe `1m`-Zoom-Outs bleiben dadurch im beabsichtigten Zeitfenster und springen nicht mehr um den lokalen Browser-Zeitzonen-Offset zurueck
- FastAPI normalisiert jetzt außerdem Plotly-Wheel-/Relayout-Zeitstempel mit hoeherer Nachkommastellen-Praezision, bevor sie erneut verwendet werden. Dadurch kollabieren seltene tiefe `1m`-Wheel-Zoom-Outs nicht mehr in ein leeres Mini-Fenster, obwohl im beabsichtigten Bereich Candles vorhanden sind
- Der Coin-Name wird oben links im Chart als Label angezeigt
- Für Equity-Stock-Perps werden historische Aktiensplit-Daten als vertikale gestrichelte orangefarbene Linien mit Annotationen (z.B. "Split 20:1") angezeigt; OHLCV-Daten werden automatisch für Splits angepasst
- Split-Faktor-Daten werden pro Exchange in `data/coindata/hyperliquid/split_factors.json` gespeichert (via Tiingo Daily API abgerufen)

## TradFi Symbol Mappings

Dieser Bereich ist die zentrale Steuerung für XYZ-Stock-Perp-Symbolrouting.

### Tabelle

Die Mapping-Tabelle wird aus folgenden Quellen zusammengeführt:
- Hyperliquid Mapping (`mapping.json`)
- Manuelle/angereicherte Einträge (`tradfi_symbol_map.json`)

Angezeigte Spalten u. a.:
- Symbol (Hyperliquid-Link)
- HL Price / Tiingo Price
- Description / Type / Status
- Start Date / Fetch Start
- Pyth-Link
- Verification und Notes

Tabellen-Filter:
- Filter by status
- Filter by symbol (matcht XYZ-Symbol und Tiingo-Symbol/Ticker)
- Filter by type (canonical type, z. B. `equity_us`, `fx`)

Startdate-Semantik:
- Start Date: Provider-Metadatum (`tiingo_start_date`)
- Fetch Start: effektives frühestes Fetch-Datum
  - IEX Equity nutzt `max(Start Date, 2016-12-12)`
  - Leer, wenn Start Date unbekannt ist

### Action Buttons

Die Buttons sind in zwei ausgerichteten Reihen angeordnet.

Der Inline-Mapping-Editor ist standardmäßig ausgeblendet und öffnet sich erst, wenn du bewusst `Edit` klickst.

Reihe 1 (Workflow pro ausgewähltem Symbol):
- Search ticker
- Edit
- Test Resolve
- Fetch start date
- Refresh spec

Reihe 2 (globale Aktionen):
- Auto-Map
- Fetch all start dates
- Refresh metadata
- Refresh prices
- View specs

Die Ergebnisbox unter den Buttons ist wieder schließbar, und Auto-Map-Ergebnisse bieten aufklappbare Kategorien wie `Not found` und `Skipped`, damit du die betroffenen Symbole direkt prüfen kannst.

Das Tiingo-Widget oberhalb dieses Bereichs ist ein lokaler PBGui-Tracker und nicht die autoritative Usage-Anzeige aus dem Tiingo-Dashboard. PBGui kennzeichnet diese Karten jetzt explizit als lokale Zaehler und zeigt zusaetzlich einen Warnhinweis, wenn Tiingo gerade einen live `server_429`-Backoff zurueckgibt. Dadurch siehst du die aktuelle Retry-Wartezeit direkt im UI, auch wenn die lokalen Zaehler fuer `Hour`, `Day` und `Month Bandwidth` noch nicht auf null stehen.

Die Auto-Map-Summen folgen jetzt denselben nicht-delisteten Mapping-Zeilen, die auch in der Tabelle sichtbar sind, sodass alte delistete Altlasten aus der rohen JSON-Datei nicht mehr in die Resultate hineinlaufen.

Auto-Map gleicht diese sichtbaren Zeilen jetzt außerdem vor dem Skippen mit der aktuellen Hyperliquid-XYZ-Aktivliste ab. Dadurch werden aktive Zeilen mit einem veralteten rohen `delisted`-Flag in `tradfi_symbol_map.json` wieder als aktiv verarbeitet, und beschreibende Equity-Texte wie `LLY tracks ... Eli Lilly and Company` bestehen die Tiingo-Namenspruefung jetzt ebenfalls statt unter `Skipped` zu landen.

Pending-Zeilen behalten nur noch einen einzelnen Marker `auto-map: not found`, sodass wiederholte Auto-Map-Läufe die Note-Spalte nicht mehr mit Duplikaten vollschreiben.

Die TradFi-Typableitung folgt jetzt enger dem live geladenen XYZ-Spec-Cache: Der Parser liest die getrennten Spalten für Description und Underlying korrekt, und Auto-Map entscheidet zwischen direktem Lookup, FX-Mapping und `no_provider` aus dem abgeleiteten Instrumenttyp statt nur aus einer statischen Symbolliste.

`Search ticker` öffnet sich jetzt direkt im schwebenden PBGui-Hilfsfenster: Dort kannst du den Tiingo-Suchbegriff editieren, die sichtbare Trefferliste inklusive aktuellem Tiingo-Preis prüfen, wenn Tiingo einen Quote liefert, ihn direkt mit dem aktuellen Hyperliquid-Preis des ausgewählten XYZ-Symbols vergleichen und ein Ergebnis mit `Apply` direkt aus demselben Fenster übernehmen. Falls Tiingo für einen Treffer keinen Quote liefert, wird der Preis als nicht verfügbar angezeigt statt irreführend als `0.0000`. Treffer mit Tiingo-Exchange-Suffixen wie `BNO:BAT` werden außerdem automatisch auf den zugrunde liegenden Quote-Ticker wie `BNO` aufgelöst, damit der korrekte Preis trotzdem angezeigt wird.

### Specs Popup

`View specs` öffnet ein Popup mit:
- Source/Fetched-Timestamp/Row-Count
- Link zur originalen XYZ-Spec-Seite
- einem verschiebbaren, resizablen und schließbaren Fenster wie bei den anderen PBGui-Hilfsfenstern
- großer Tabellenansicht (nutzt die Fensterhöhe)
- klickbaren Links:
  - Pyth Link
  - HL Link

Pyth-Links behalten jetzt die für `pythdata.app` nötige encodierte Symbol-Trennung bei, sodass Symbole wie `AMZN/USD` über `%2F` geöffnet werden und nicht mehr auf einer 404-Seite landen.

### Hinweise

- `Fetch start date` gilt nur für Equity (Daily-Metadaten-Endpoint).
- Für FX gibt es keinen dedizierten Startdate-Metadata-Fetch-Button.
- Auto-Map sowie Metadata/Price-Refresh benötigen einen konfigurierten Tiingo API-Key.

## Download l2Book from AWS

Lädt Hyperliquid l2Book-Archivdateien (Requester Pays).

Auf der FastAPI-Seite nutzt das Hyperliquid-Download-Panel jetzt denselben Enabled-Coins-Grid-Selector wie `Best 1m`: `Filter enabled coin list` grenzt die sichtbaren Coins ein, `Select visible` übernimmt den aktuell gefilterten Ausschnitt gesammelt, `Clear all` setzt die explizite Auswahl zurück, und sichtbare Zeilen lassen sich direkt anklicken oder per Maus-Drag in größeren Bereichen markieren. `XYZ-*`- bzw. TradFi-Symbole werden hier ausgefiltert, weil es für sie keinen Hyperliquid-l2Book-Archivdownload gibt. Wenn die Auswahl leer bleibt, reiht PBGui weiterhin alle verbleibenden downloadbaren Hyperliquid-Coins ein.

Workflow:
1. AWS-Profil und Region konfigurieren
2. Coins und Datumsbereich auswählen
3. Auto-Download-Job starten

UI-Verhalten:
- Die Download-Job-Queue wird direkt unter den Download-Controls angezeigt
- `Last download job` ist als einklappbare Zusammenfassung verfügbar
- Die Zusammenfassung zeigt Status, Coins, Range, Counts (downloaded/skipped/failed), Größen-Statistik, Fortschritt % und Laufzeit

Kostenverhalten:
- Lokale Dateien werden zuerst geprüft und übersprungen
- Übersprungene Dateien verursachen keinen zusätzlichen Transfer-/Download-Aufwand

Speicherpfad:
- `data/ohlcv/hyperliquid/l2Book/<COIN>/<YYYYMMDD>-<H>.lz4`

## Build best 1m OHLCV

Startet Background-Build-Jobs für berechtigte Symbole.

Auf der FastAPI-Seite verwenden Binance USDM und Bybit jetzt direkt ein Settings-ähnliches Enabled-Coins-Grid im `Best 1m`-Build-Panel. Du kannst die Liste über `Filter enabled coin list` eingrenzen, einzelne sichtbare Zeilen anklicken, mit der Maus über sichtbare Zeilen ziehen, um größere Bereiche schnell zu markieren oder zu entfernen, oder mit `Select visible` den aktuell gefilterten Ausschnitt gesammelt übernehmen. Wenn keine explizite Auswahl gesetzt ist, reiht PBGui weiterhin alle aktivierten Coins der aktuellen Exchange ein.

Auf Hyperliquid verwendet das fokussierte `Best 1m`-Build-Panel jetzt ebenfalls das Muster `Filter enabled coin list` + mehrspaltiges Coin-Grid sowie den gemeinsamen Popup-Kalenderstil für `Start date` / `End date` anstelle des früheren Ein-Zeilen-Dropdowns und der nativen Browser-Datumsfelder. Die sichtbaren Coin-Zeilen lassen sich direkt anklicken oder per Maus-Drag in größeren Bereichen markieren.

### Job-Typen

**`hl_best_1m`** — Hyperliquid XYZ Stock-Perps:
- Berechtigung: Mapping-Status `ok` + Tiingo-Ticker vorhanden
- Controls: Build best 1m, Start date, End date, Refetch TradFi from scratch

**`binance_best_1m`** — Binance USDM vollständiger historischer Backfill:
- Lädt komplette 1m OHLCV-Daten von Inception bis heute aus offiziellen Binance-Archiven (data.binance.vision) — monatliche + tägliche ZIPs — mit CCXT-Lückenfüllung
- Coin-Auswahl aus allen aktivierten Binance Coins
- Controls: Start date, End date, Refetch
- Speicherpfad: `data/ohlcv/binanceusdm/1m/<COIN>/YYYY-MM-DD.npz` (komprimiertes NumPy-Archiv; PB7-Cache nutzt unkomprimiertes `.npy` — ~35% mehr Speicher für dieselben Daten)

### Job-Verwaltung

Das Job-Panel zeigt drei Bereiche:
- **Pending** — eingereihte Jobs
- **Running** — aktuell laufender Job mit Live-Fortschritt
- **Failed / Done** — abgeschlossene Jobs

Aktionen:
- **Run** — markiert einen Pending-Job für manuelle Priorität und erlaubt einen zusätzlichen parallelen Slot für denselben Job-Typ neben dem bereits laufenden Job
- **View** — öffnet die vollständigen Job-Details (Summary, Payload, Progress, Last Result)
- **Cancel** — fordert für einen laufenden Job im eingebetteten Monitor einen kooperativen Abbruch an; der Worker stoppt am nächsten sicheren Checkpoint
- **Retry** — stellt einen fehlgeschlagenen Job wieder in Pending ein
- **Delete** — löscht einen einzelnen Job
- **Delete selected / Delete all** — Bulk-Löschen aus Failed- oder Done-Liste

### Fortschrittsanzeige

Während ein Job läuft, zeigt das Panel:
- Stage: `starting`, `running`, `done`
- Aktueller Coin
- Chunk erledigt / gesamt
- Geschriebene Minuten
- Laufzeit
- Für Binance: abgerufene Pages, abgedeckte Tage
- Für HL TradFi: Monat YYYY-MM Tag X/Y, Tiingo-Quota-Auslastung, 429-Wartezustände

### Datenstrategie (hl_best_1m)

Build best 1m läuft im gewählten Datumsfenster immer von neu → alt.

Für Crypto-Symbole (non-XYZ):
- Nutzt zuerst lokales `1m_api` und lokale `l2Book`-Konvertierung
- Füllt verbleibende Lücken über Perp-Exchange-Fallback
- `l2Book` wird nur in diesem Crypto-Pfad genutzt (nicht für XYZ-Stock-Perps)

Für FX-gemappte Stock-Perps (`tiingo_fx_ticker`):
- Nutzt Tiingo FX 1m in Wochen-Chunks (weniger Requests)
- Nutzt bestehende `other_exchange`-Historie als Anker, wenn kein Refetch aktiv ist
  - Start-Cursor = ältester vorhandener `other_exchange`-Tag minus 1 Tag
- `Refetch` startet am gewählten/End-Tag und baut rückwärts im erlaubten Bereich neu auf
- Weekend-Sessiongrenze folgt dem beobachteten Feed-Verhalten:
  - Freitag-Close = 17:00 New York Lokalzeit (DST-aware in UTC)
  - Sonntag-Reopen ≈ 22:00 UTC (fix)
- Bekannte reduzierte FX-Feiertagssessions:
  - `12-24` und `12-31`: frühes Close um ca. 22:00 UTC
  - `12-25` und `01-01`: spätes Reopen um ca. 23:00 UTC

Für Equity-gemappte Stock-Perps (`tiingo_ticker`):
- Nutzt Tiingo IEX 1m
- Nutzt bestehende `other_exchange`-Historie als Anker, wenn kein Refetch aktiv ist
  - Start-Cursor = ältester vorhandener `other_exchange`-Tag minus 1 Tag
- Untere Grenze bleibt `max(tiingo_start_date, 2016-12-12)`
- Raw-first-Write-Verhalten: alle von Tiingo gelieferten Minuten werden geschrieben (kein zusätzliches Market-Hours-Clipping im Write-Pfad)

Write-Sicherheitsregeln:
- TradFi-Write (`other_exchange`) füllt nur fehlende Minuten oder Minuten, die bereits als `other_exchange` markiert sind
- Bereits vorhandene `api` / `l2Book_mid` Minuten werden durch TradFi nicht überschrieben

Datums-Controls:
- `Start date` begrenzt den ältesten zu verarbeitenden Tag
- `End date` begrenzt den neuesten zu verarbeitenden Tag (Standard = heute)

### Fortschritt und Wartezustände (hl_best_1m)

Im Job-Panel können u. a. angezeigt werden:
- `month YYYY-MM day X/Y`
- Tiingo month request usage
- Quota/429-Wartezustände mit Sekunden und Grund

## Tiingo Settings (im Settings-Bereich)

Die Seite enthält Tiingo-Controls:
- `tiingo_api_key`
- Test Tiingo Button
- Runtime-Quota-Anzeigen (Stunde/Tag/Monatsbandbreite)
- Externe Links für API-Key-Signup und Usage-Dashboard

## Troubleshooting

Wenn ein Build-Job kurz erscheint und wieder verschwindet:
1. Neuesten Failed-Job in `data/ohlcv/_tasks/failed` prüfen
2. Sicherstellen, dass der Worker mit aktuellem Code läuft (ggf. Worker neu starten)
3. Tiingo-Key und Symbol-Mapping-Status prüfen
4. `Test Resolve` für das ausgewählte Symbol verwenden

Wenn die Build-Coin-Liste leer ist:
- Prüfen, ob Symbole gemappt sind und Status `ok` haben
- Prüfen, ob Tiingo Ticker oder FX Ticker im Mapping gesetzt ist
