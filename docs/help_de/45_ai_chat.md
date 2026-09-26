# AI Chat

Stop funktioniert auch während eine neue Unterhaltung noch angelegt wird. Der wartende Prompt wird nach der verspäteten Antwort nicht mehr gesendet; das Eingabefeld wird wieder freigegeben.

## Zweck

AI Chat ist die erste produktive PBGui-AI-Integration. Sie verbindet Provider-Chat mit kontrollierten PBGui-Capabilities.

Der Agent kann PB7/PB8-Optimizer-Configs, Optimizer-Run-Summaries, Backtest-Summaries und dynamische Optimizer-Metadaten auflisten und lesen. Logs, Zugangsdaten, beliebige Dateien, Checkpoints und rohe Result-Artefakte bleiben unzugaenglich.

Der Agent kann ausserdem eine normale Python-Analyse ueber begrenzte JSON-Daten vorschlagen, die der Unterhaltung bereits zur Verfuegung stehen. PBGui entfernt Felder mit Secret- oder Host-Pfad-Namen, zeigt vor der Freigabe das exakte Skript und das bereinigte JSON und startet erst nach einer expliziten owner-, conversation- und digest-gebundenen Freigabe.

Bei Passivbot-Fragen kann der Agent Dokumentation und Source-Dateien aus dem exakt installierten PB7/PB8-Checkout durchsuchen. Ergebnisse enthalten installierten Git-Commit, relativen Pfad, Zeilenbereich und bei Verwendung des offiziellen Passivbot-Repositories einen commit-gepinnten GitHub-Link. Source-Inspektion ist rein textuell; PBGui fuehrt den gelesenen Code niemals aus, importiert oder veraendert ihn nicht.

Fuer PB8 kann er eine vollstaendige Optimizer-Config validieren und Speichern, Speichern plus Queueing einer neuen Config oder Queueing einer bestehenden Config vorschlagen. Queueing allein startet keinen Optimizer. Bei einem expliziten Startauftrag kann der Assistent pfadfreie PB8-Queue-IDs auflisten und den sofortigen Start von bis zu vier exakten Queue-Jobs in einer separat geprueften Aktion vorschlagen. Diese Tools erzeugen nur ein Proposal. PBGui zeigt die exakte Aktion und verlangt vor der Ausfuehrung eine explizite Freigabe. PB7-Mutationen bleiben deaktiviert, weil die aktuellen Queue-Snapshots noch nicht die erforderlichen Immutabilitaets- und Concurrency-Garantien besitzen.

Versionsuebergreifende Vergleiche behalten ihre echten Runtimes. Ein Auftrag PB7-Trailing gegen PB8-`trailing_martingale` verwendet eine reale PB7-Quellconfig und eine separate PB8-Config; PBGui ersetzt die PB7-Seite nicht still durch PB8s Kompatibilitaetsstrategie `trailing_grid_v7`. Der Assistent kann die PB7-Quelle lesen und die PB8-Seite vorbereiten, PB7-Mutation, Queueing und Start bleiben jedoch manuell, bis die PB7-Approval-Grenze sicher ist.

Die ChatGPT-Runtime startet in einem privaten leeren Workspace. Lokale Ausfuehrung, Websuche, Memory, Multi-Agent und MCP sind deaktiviert. Nur der PBGui-Dynamic-Capability-Namespace ist verfuegbar. Command- oder Datei-Aenderungsanfragen werden abgelehnt.

Freigegebene Python-Analyse ist eine separate Fail-closed-Capability. Sie laeuft mit Bubblewrap in einem leeren temporaeren Workspace, mit read-only Python-Runtime und installierten Bibliotheken wie NumPy/Pandas, soweit vorhanden. Sie besitzt einen isolierten Netzwerk-Namespace, keinen Zugriff auf Host-Home, PBGui-Daten, Zugangsdaten oder andere Host-Dateien, eine bereinigte Umgebung, JSON ueber Standard Input, begrenztes Standard Output/Error, Resource-Limits und einen kurzen Timeout. Wenn Bubblewrap oder Resource-Limiting fehlt, gibt es keinen unsandboxed Fallback.

## ChatGPT

ChatGPT verwendet den offiziellen, mit PBGui installierten Codex-Login.

1. **Browser login** waehlen, wenn PBGui und Browser auf demselben Rechner laufen.
2. Die angezeigte HTTPS-Adresse oeffnen.
3. Die normale ChatGPT-Browserautorisierung abschliessen.
4. Warten, bis PBGui ChatGPT als verbunden meldet.
5. Ein fuer das Konto sichtbares Modell waehlen und eine Nachricht senden.

Fuer einen entfernten oder headless PBGui-Host stattdessen **Device code** waehlen. Device Login erfordert aktivierte Geraetecode-Autorisierung in den ChatGPT-Sicherheitseinstellungen und die Eingabe des angezeigten Einmalcodes.

PBGui verwendet fuer diese Verbindung keinen OpenAI-Platform-API-Key. Verfuegbare Modelle und Limits haengen vom verbundenen ChatGPT-Konto ab.
Der Modell-Picker laedt alle sichtbaren Textmodelle aus saemtlichen Seiten von Codex `model/list`; PBGui pflegt keine feste ChatGPT-Modellliste. Die Verfuegbarkeit haengt weiterhin vom verbundenen Konto ab.

Wenn Codex eine Geschwindigkeitsstufe meldet, kannst du **Speed** im AI Chat oder im AI-Drawer waehlen. **Model default** uebernimmt die Vorgabe des Modellkatalogs, **Standard** fordert normale Geschwindigkeit an und **Fast** die angebotene schnellere Stufe fuer diesen Turn. Die Auswahl wird in der Unterhaltung gespeichert und kann vor der naechsten Nachricht geaendert werden. Fast verbraucht mehr ChatGPT-Credits; PBGui zeigt nur vom gewaehlten Modell gemeldete Stufen.

## OpenCode Zen und Go

OpenCode Zen und OpenCode Go verwenden denselben OpenCode-Workspace-Key. Zen enthaelt wechselnde Gratis- und Pay-go-Modelle; Go ergaenzt Abo-Modelle und inkludierte Nutzung.

Die OpenCode-Provider-Karte enthaelt **Get OpenCode Go**. Der Button oeffnet die Abo-Seite ueber einen PBGui-Referral-Link. Dies ist ein Affiliate-aehnlicher Referral: Nach dem aktuellen OpenCode-Programm koennen Einladender und neuer Abonnent ein Account-Guthaben erhalten. Abo-Bedingungen und Referral-Praemien werden von OpenCode kontrolliert und koennen sich aendern.

1. Den Key in der OpenCode-Account-Konsole erzeugen oder kopieren.
2. In der OpenCode-Karte eingeben und **Connect** waehlen.
3. PBGui prueft den Key und speichert ihn serverseitig in einer owner-only Datei.
4. Ein verfuegbares Modell waehlen und eine Nachricht senden.

PBGui unterstuetzt beide Kataloge ueber Responses-, Chat-Completions- und Messages-Endpunkte. Verfuegbare IDs kommen aus den Live-Zen-/Go-Katalogen; Namen, Protokoll, Kosten und Limits aus OpenCodes Live-Modellmetadaten. Gratis-Modelle werden aus Nullkosten erkannt, zuerst angezeigt und mit **Free** markiert. Neue Modelle erscheinen automatisch, wenn sie ein unterstuetztes Protokoll verwenden; entfernte Modelle verschwinden. Contributor-Modelle, die Prompts und Antworten fuer Training verwenden koennen, sind deutlich markiert.

PBGui prueft die Verfuegbarkeit kostenloser Modelle automatisch im Hintergrund und zeigt den letzten owner-spezifischen Status beim Modell. Modelle mit Training-Opt-in werden niemals automatisch geprueft.

PBGui-Capability-Tools werden ueber OpenCode Chat Completions, OpenAI Responses und Anthropic Messages mit dem jeweils nativen Tool-Call-Vertrag unterstuetzt. PBGui aktiviert sie nur, wenn die Live-Modellmetadaten Tool-Calls anbieten. Tool-faehige Auswahlen tragen das Label **PBGui tools**; Modelle ohne diese Faehigkeit bleiben als **Chat only** markiert.

Modelle mit angebotenen Reasoning-Varianten zeigen eine **Effort**-Auswahl. **Standard** sendet keinen Override und behaelt den Provider-Default. Alle weiteren Optionen kommen in Provider-Reihenfolge vom ausgewaehlten Modell. Deshalb koennen Namen wie `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, `max`, `ultra` oder providerspezifische Werte erscheinen. PBGui ergaenzt keine feste Variantenliste.

Mit **Chat only** markierte Modelle koennen installierte Passivbot-Dokumentation, Source oder aktuelle PBGui-Daten nicht einsehen. Sie erhalten keine Capability-Regeln oder Tool-Namen und antworten direkt aus allgemeinem Wissen. Fuer Antworten mit lokalen oder installierten Runtime-Belegen ein Modell mit **PBGui tools** waehlen. Responses- und Messages-Modelle koennen nun installierte Versionen, Dokumentation und Source ueber ihre nativen Function-Result-Formate verarbeiten, ohne unbeschraenkten Dateisystemzugriff zu erhalten.

## OpenRouter und Jev

Du kannst auch im normalen Chat mit ChatGPT oder OpenCode bleiben und fragen: „Nutze Jev, um die Pareto-Kandidaten dieses Optimize-Resultats zu analysieren. Bevorzuge niedrigen Drawdown und stabile Rendite; erklaere mir die zehn Kandidaten fuer einen Backtest.“ Der Assistent loest den exakten Optimize-Lauf auf und erstellt einen Vorschlag **Jev analysis** mit den genauen Jev-Anfragen, Lauf, Frage und USD-Limit. Pruefe und genehmige ihn in PBGui. Erst dann ruft PBGui Jev auf; der normale Assistent erhaelt das Jev-Ergebnis und erklaert es. Jev kann Pareto-Zeilen markieren, stellt aber keine Backtests in die Queue. Der Drawer zeigt an, wenn Jev mangels verbundenem OpenRouter-Key fuer dieses PBGui-Konto nicht verfuegbar ist. Bei verbundenem OpenRouter-Key kann der Assistent auch ohne ausdrueckliches „Jev“ einen pruefbaren Jev-Vorschlag anbieten. Erst deine Freigabe startet eine kostenpflichtige Jev-Anfrage. Eine genannte Hoechstzahl von Kandidaten wird in den Vorschlag uebernommen; sonst bestimmen Jevs eigene Ja/Nein-Entscheidungen, welche Kandidaten markiert werden.

Unter **Jev preflight budget per analysis (USD)** in der OpenRouter-Karte der vollstaendigen AI-Chat-Seite stellst du das Limit ein. Standard sind $0.01. Vor jeder Jev-Anfrage holt PBGui den aktuellen OpenRouter-Preis des festen Modells und prueft die gesamte serialisierte Nutzlast mit einer vorsichtigen Token-Obergrenze. Wenn der Preis nicht abrufbar ist, Ausgabetokens kostenpflichtig werden oder die Schaetzung das Limit ueberschreitet, sendet PBGui keine Jev-Anfrage. Diese Schutzpruefung ist eine Schaetzung und keine vom Anbieter gemeldete Abbuchung; die OpenRouter-Nutzungsanzeige enthaelt weiterhin ausschliesslich vom Anbieter gemeldete Key-Werte.


Einen OpenRouter-API-Key in der OpenRouter-Karte der vollstaendigen AI-Chat-Seite verbinden. PBGui prueft ihn bei OpenRouter und speichert ihn getrennt in einer owner-only Serverdatei. Das Browser-Eingabefeld wird geleert; die AI-API gibt den Key niemals zurueck. OpenRouter rechnet Jev-Anfragen ueber dieses Konto ab.

Die OpenRouter-Karte und der AI-Drawer zeigen die von OpenRouter gemeldeten taeglichen, woechentlichen und monatlichen USD-Ausgaben des API-Keys sowie dessen Ausgabenlimit. Diese Werte gelten fuer den Key, nicht fuer den gesamten Workspace. **View full usage on OpenRouter** oeffnet die Activity-Seite des Providers mit Anfragen, Tokens und der vollstaendigen Kontoaktivitaet. PBGui aktualisiert die Anzeige automatisch, solange Seite oder Drawer offen sind.

Im AI-Drawer **OpenRouter** und **Jev 1.13 · Structured decisions** auswaehlen. Auf einer PB7- oder PB8-Optimize-Seite ein abgeschlossenes Resultat oeffnen und **Include page context** aktiviert lassen. Beispiel: „Welche zehn Pareto-Kandidaten dieses Resultats sollte ich zuerst backtesten?“ Auf einer anderen Seite kann der exakte Resultatname in der Frage stehen. Falls mehrere Laeufe passen, fordert PBGui zur Auswahl auf.

TypeSafe beschreibt [Jev](https://docs.typesafe.ai/concepts/system-one) als System-One-Modell, das bereitgestellten Text oder JSON auswertet und typisierte Antworten mit Wahrscheinlichkeiten statt frei formulierter Erklaerungen liefert. PBGui erkennt den exakt ausgewaehlten Optimize-Lauf, auch wenn mehrere Laeufe denselben Anzeigenamen haben. PBGui liest jeden Pareto-Kandidaten und alle verfuegbaren numerischen PB8-Metrikspalten lokal. An Jev gehen kleine, begrenzte Gruppen mit hoechstens 24 unterschiedlichen Metrikspalten pro Kandidat sowie kompakte Profile aus den uebrigen Spalten. Jeder Kandidat wird bewertet; Jev erhaelt aber nicht jeden rohen Metrikwert. PBGui buendelt die Kandidaten in kontextsichere Anfragen von hoechstens 30 KB. Es gibt keine zusaetzliche Gesamtgrenze in Bytes: Vor jedem kostenpflichtigen Jev-Aufruf prueft PBGui die vollstaendig geplante Analyse mit den aktuellen OpenRouter-Preisen gegen das konfigurierte USD-Limit. Fragen ueber dem USD-Limit oder Jev-Kontext werden ohne Uebermittlung von Kandidatendaten abgelehnt. PBGui markiert die Kandidaten gemaess Jevs Ja/Nein-Entscheidungen unter Beachtung einer in der Frage genannten Hoechstzahl und nennt den Kandidaten mit der hoechsten Prioritaet als Backtest-Champion. Bei PB7 werden die Metriken der Pareto-Zusammenfassung verwendet. Eine angezeigte Prioritaetswahrscheinlichkeit ist keine Gewinnwahrscheinlichkeit fuer Live-Trading oder einen Holdout-Backtest. Jev queued keine Backtests und aendert keine Configs; die markierten Kandidaten mit den vorhandenen Optimize-Controls pruefen und in die Queue stellen.


Jev kann auch aus deinen eigenen Angaben eine strukturierte Wahl treffen. Schreibe **Optionen:** in eine eigene Zeile und darunter 2–20 nummerierte Alternativen mit den relevanten Daten. Beispiel:

```text
Welcher GPU-Plan eignet sich bei geringem Preis und hoher Zuverlässigkeit besser?
Optionen:
1. Plan A: 12 GB VRAM, $0.20/Stunde, 96% Zuverlässigkeit
2. Plan B: 16 GB VRAM, $0.32/Stunde, 99% Zuverlässigkeit
```

Die Antwort zeigt Jevs Wahl, die vom Provider gemeldete Konfidenz und Wahrscheinlichkeiten. PBGui ergänzt keine fehlenden Fakten. Für verwaltete Daten frage etwa: „Welche aktuellen PBGui-Backtest-Resultate bieten das beste Verhältnis aus Drawdown und Rendite?“ Jev liest bis zu zehn aktuelle Resultate pro PB7/PB8-Version samt projizierten Kennzahlen. Dieser Vergleich liest nur Daten und wählt oder queued nichts.

Für eine einfache Ja/Nein-Frage oder Bewertung kannst du eine Textvorlage verwenden:

```text
Daten:
Die GPU ist zwei Tage nicht verfügbar; CPU-Backtests laufen weiter.
Ja/Nein: Sind alle Backtests blockiert?
```

Für eine Bewertung ersetze die letzte Zeile durch `Bewertung: Wie schwer ist die Störung?` und ergänze `Stufen:` mit 2–10 nummerierten Beschreibungen von gering bis schwer.

Für andere strukturierte Entscheidungen kannst du einen `jev`-JSON-Block mit deinen eigenen `state`-Daten und `questions` senden. Jev unterstützt `choice`, `score` (geordnete Stufen) und `noul` (Ja-Wahrscheinlichkeit), auch mehrere Fragen in einem Aufruf. Ohne `sources` werden nur deine Eingaben gesendet. Beispiel:

```jev
{"state":{"worker":"GPU ist zwei Tage nicht verfügbar; CPU-Backtests laufen weiter"},"questions":{"blocked":{"type":"noul","instructions":"Sind Backtests vollständig blockiert?"},"impact":{"type":"score","instructions":"Wie schwer ist die Störung?","criteria":["Keine Auswirkung","Ein Teil der Arbeit verzögert","Alle Validierungen stehen still"]}}}
```

Für weitere PBGui-Daten kannst du 1–4 unterstützte Lese- oder Analysefunktionen in `sources` angeben. PBGui liest die ausgewählten Daten und zeigt die **vollständige Anfrage an OpenRouter** mit deinen Eingaben und den PBGui-Ergebnissen. Erst nach **Send to OpenRouter** wird sie gesendet; Abbrechen verwirft die Vorschau. Die Anfrage ist auf 24 KB begrenzt. Beispiel für aktuelle PB8-Backtest-Zusammenfassungen:

```jev
{"state":{"ziel":"Niedriger Drawdown ist wichtiger als maximaler Profit"},"sources":[{"name":"recent","tool":"list_backtests","args":{"version":"v8","limit":5}}],"questions":{"low_risk":{"type":"noul","instructions":"Unterstützen die Backtests in state.pbgui.recent einen Kandidaten für eine risikoarme Validierung?"}}}
```

Verfügbar sind die aktuell im PBGui-Capability-Katalog als `read` oder `analyze` markierten Funktionen, darunter Optimizer-Läufe und Pareto-Analysen, Backtests, Configs, Dashboards, Entwürfe, Hilfe und Installations-Zusammenfassungen. Verwende die exakten Capability-Namen und Argumente. Diese Abfragen starten keine Jobs und ändern keine Konfigurationen. Die bisherigen direkten Fragen zu Optimize-Pareto und aktuellen Backtests funktionieren weiter. Gib in deinem eigenen `state` keine Zugangsdaten oder Geheimnisse an.

### Beispielfragen

Bei geöffnetem Optimize-Resultat und aktiviertem **Include page context** kannst du eine dieser Fragen in den AI-Drawer kopieren:

- **Vorsichtig:** „Bewerte die kompakten Profile, die PBGui aus allen Pareto-Kandidaten und verfügbaren Kennzahlen erstellt. Bevorzuge niedrigen maximalen Drawdown und stabile risikobereinigte Rendite gegenüber maximalem Gewinn. Markiere zehn Kandidaten für separate Holdout-Backtests.“
- **Ausgewogen:** „Welche zehn Pareto-Kandidaten sollte ich als Nächstes validieren? Berücksichtige Drawdown, Sortino und Rendite gemeinsam und setze die stärkste Backtest-Priorität an die erste Stelle.“
- **Über Szenarien hinweg:** „Falls dieses Resultat Kennzahlen für einzelne Szenarien enthält, bevorzuge Kandidaten, die über diese Szenarien hinweg stabil abschneiden, statt nur in einem Fenster stark zu sein. Markiere zehn für Validierungs-Backtests.“

Jede Frage bewertet einen ausgewählten Optimize-Lauf und markiert bis zu zehn Pareto-Zeilen. Die Antwort ordnet Backtest-Prioritäten anhand der Trainingskennzahlen; separate Holdout-Backtests zeigen, wie die Kandidaten in ungesehenen Zeiträumen abschneiden.

## Aktionsfreigabe

PBGui trennt reversible Browseraktionen von persistenten Aktionen. Eine ausdrueckliche Aufforderung zur Auswahl von Pareto-Kandidaten kann die exakten, an den Run gebundenen Zeilen direkt in der offenen Optimize-Seite markieren. Backtest-Compare-Aktionen koennen die aktuelle Auswahl entweder durch 2-20 exakt verwaltete Results ersetzen oder 1-20 neu angeforderte Results wie einen fertigen Holdout zur bestehenden Auswahl hinzufuegen; der Browser entfernt Duplikate und erzwingt weiterhin das Limit von 20 Results. Seiten koennen ausserdem wiederverwendbare Aktionen fuer ihre Elemente anbieten. `show_log` bildet zum Beispiel ausgewaehlte oder laufende Optimize- und Backtest-Jobs oder eine aktive Bot-Config auf die bereits vorhandene Log-Viewer-Funktion der jeweiligen Seite ab, statt ein eigenes Modell-Tool oder eine Python-Analyse zu benoetigen. Die Aktion darf eine andere registrierte PBGui-Seite als Ziel haben: PBGui navigiert dorthin, stellt die aktuelle Unterhaltung automatisch wieder her, behaelt die Aktion waehrend des Ladens der Zieldaten offen und bestaetigt sie erst, nachdem die Zielseite das exakte Element validiert und ihren registrierten Callback ausgefuehrt hat. Die gemeinsame Bruecke kann weder modellgeneriertes JavaScript noch beliebige URLs, Pfade oder DOM-Selektoren ausfuehren.

Eine automatische AI-Navigation wird nur einmal versucht. Verlaesst der Benutzer das Ziel danach manuell, bevor die Bestaetigung abgeschlossen ist, verwirft PBGui die alte ausstehende Navigation, statt den Browser wieder zurueckzuziehen. Ausdrueckliche Aktionsauftraege akzeptieren ausserdem keine unbelegten Zukunftsversprechen: ChatGPT erhaelt genau eine begrenzte Korrekturfortsetzung, um einen echten Tool-Nachweis, ein Approval-Proposal, einen konkreten Blocker oder eine gezielte Rueckfrage zu erzeugen; andernfalls meldet PBGui, dass nichts ausgefuehrt wurde.

PBGui inventarisiert ausserdem aktuell sichtbare, nicht sensitive Controls wie Buttons, same-origin Links, Textfelder, Checkboxen und Selects. Jedes erhaelt eine opake kurzlebige ID sowie die erlaubte Operation `activate` oder `set_value`. Der Assistent kann damit normale PBGui-Controls verwenden, einschliesslich des Schliessens eines schwebenden Log-Fensters, ohne eine funktionsspezifische Action zu benoetigen. Passwort-, Datei-, Credential-, Token-, Session-, Cookie- und andere sensitive Controls werden ausgelassen; Feldwerte werden niemals in dieses Inventar kopiert. Bestehende Bestaetigungsdialoge und Proposal-Freigabegrenzen bleiben verbindlich. Ein Proposal-Review bleibt waehrend der Modellarbeit verborgen und erscheint erst nach der zugehoerigen vollstaendigen Assistentenantwort. Nach einer ausgefuehrten Freigabe setzt PBGui denselben Assistenten-Workflow automatisch fort, damit verbleibende angeforderte Schritte ihr eigenes Review erzeugen koennen.

Nach der bestaetigten Freigabe oder Ablehnung verschwindet die Proposal-Karte sofort, waehrend PBGui die serverseitige Entscheidung ausfuehrt. Ein sichtbarer Applying-/Rejecting-Status ersetzt sie. Schlaegt der Request fehl, erscheint die Karte mit wieder aktivierten Controls erneut, damit die Aktion sicher geprueft oder wiederholt werden kann.

Der Drawer behaelt den letzten bestaetigten sichtbaren Nachrichten-Snapshot, solange ein Turn oder eine Fortsetzung nach einer Freigabe arbeitet. Beim Polling zeichnet er Nachrichten, Page-Context-Chips und Usage-Werte nur neu, wenn sich deren angezeigter Inhalt aendert. Grosser Page-Context wird nur mit dem aktiven Provider-Request gesendet und nicht im dauerhaften User-Nachrichtentext gespeichert; History-Trimming kann dadurch die sichtbare Frage und Proposal-Antwort waehrend eines Follow-ups nicht entfernen. Ein gerade bestaetigtes Proposal bleibt ueber seine ID verborgen, bis der finale Serverstatus eintrifft, und kann durch einen veralteten Poll nicht erneut als klickbare Review-Karte erscheinen.

Eindeutige reversible Befehle wie das Anzeigen des einzigen verfuegbaren Logs, das Schliessen eines sichtbaren Log-Fensters oder der ausdrueckliche Klick auf genau ein eindeutig benanntes sichtbares Control verwenden einen lokalen Browser-Fast-Path. PBGui fuehrt die Aktion sofort aus, zeichnet Anfrage und Abschluss in der owner-gebundenen Unterhaltung auf und kontaktiert den ausgewaehlten AI-Provider nicht. Mehrdeutige, analytische oder veraendernde Anfragen verwenden weiterhin den normalen Modell- und Freigabeablauf.

Der globale Drawer speichert Breite, Open-/Closed-Zustand und Pin-Modus in owner-gebundenen Server-Preferences. Der Stecknadel-Button wechselt zwischen dem normalen Overlay und einer Side-by-side-Ansicht, die PBGui verkleinert, damit der Drawer die aktive Seite nicht mehr verdeckt; auf Mobilgeraeten bleibt das vollbreite Overlay aktiv. Der Drawer oeffnet sich nach normaler PBGui-Seitennavigation automatisch wieder, wenn er davor offen war; ein ausdrueckliches Einklappen bleibt dagegen geschlossen. Beim Ziehen der Breite schuetzt ein temporaerer browserweiter Layer davor, dass Dashboard-Iframe-Widgets Mausereignisse abfangen, und eine verspaetete initiale Preference-Antwort kann einen bereits laufenden Drag nicht zuruecksetzen.

Bei einem unklaren Auftrag prueft der tool-faehige Assistent zuerst die verfuegbaren PBGui-Fakten und entscheidet dann, ob eine fehlende Nutzerpraeferenz das Ergebnis wesentlich aendern wuerde. Falls ja, formuliert das Modell selbst eine gezielte Frage und 2–5 passende anklickbare Antworten in der Sprache des Nutzers. PBGui ergaenzt **Write your own answer…** als letzte Option im vollstaendigen Chat und im Drawer. Ein Klick darauf setzt den Fokus ins Eingabefeld, damit du frei antworten kannst. Die Antworten werden erst nach Abschluss des laufenden Modell-Turns anklickbar; ein Klick startet dann den naechsten Turn. Eine angenommene Antwort entfernt die offene Frage und setzt dieselbe Unterhaltung fort. Das Modell entscheidet, ob fehlende Praeferenzen eine Empfehlung aendern koennten, und formuliert bei Bedarf selbst eine Rueckfrage. Es erklaert die verwendeten Metriken und Abwaegungen, ohne still feste Gewichte anzunehmen. Null strikte Treffer erlauben keine automatische Lockerung: Alternativen aus dem Vollscan bleiben getrennt und erfordern deine Auswahl vor Markierung oder Queueing.

Wenn der Agent eine PB8-Save-/Queue-Aktion, eine Backtest-Matrix aus Pareto-Kandidaten und Exchanges, Sweep-Holdout-/Full-Timerange-Validierung, Dashboard-Erstellung oder -Bearbeitung oder eine Python-Analyse vorschlaegt, zeigt PBGui die Approval-Karte sowohl auf der vollstaendigen Seite als auch im Drawer. Fuer Python zeigt die Karte exakten Code, bereinigte Eingabe, Input-Summary und Payload-Digest. Nach Approval speichert PBGui den begrenzten Analysis-Status, Output und Diagnosen dauerhaft in der Conversation, bevor die optionale Modell-Zusammenfassung startet; das vollstaendige Result bleibt in Action History verfuegbar. Ein Pareto-Proposal kann bis zu 1000 Kandidaten und 1000 daraus entstehende Backtest-Jobs binden. Backtest-Proposals zeigen alle Kandidaten, den Validierungsmodus, die gesamte Jobanzahl und ob Queue-Autostart sofort beginnen kann. Dashboard-Proposals koennen ein Template oder ein freies semantisches Layout mit 1-10 Reihen, 1-2 Spalten, Widget-Positionen, Usern, Perioden, Chart-Modi, Widget-Optionen, Hoehen und Orders-zu-Positions-Verknuepfungen verwenden. Bestehende Dashboards koennen zellenweise gelesen und geaendert werden, waehrend andere Einstellungen erhalten bleiben; bei einer Aenderung nach dem Review schlaegt die Freigabe fehl. **Review & approve** oeffnet den gemeinsamen PBGui-Bestaetigungsdialog; nur dieses exakte owner-, conversation- und digest-gebundene Payload darf laufen. Ablehnen oder Schliessen veraendert nichts. PBGui laedt offene Proposals beim Wiederherstellen einer Unterhaltung und nach jeder Freigabe oder Ablehnung erneut vom Server. Eine abgelaufene oder bereits erledigte Karte gilt dadurch nicht als aktuell. Ein Timeout der optionalen Modell-Fortsetzung nach erfolgreicher Freigabe erscheint als fehlerfreier Abschlusshinweis und entfernt weder das Analysis-Result noch aendert oder rollt es die ausgefuehrte Aktion zurueck.

PBGui wiederholt einen transienten Timeout der optionalen Post-Approval-Zusammenfassung genau einmal. Die genehmigte Aktion selbst wird niemals erneut ausgefuehrt; schlaegt auch der Retry fehl, bleibt das persistierte Result sichtbar und die Conversation erhaelt den fehlerfreien Abschlusshinweis.

Nachdem eine Pareto-Backtest-Matrix aus Optimize oder AI Chat freigegeben und gequeued wurde, merkt PBGui sich die exakten Queue-IDs im aktuellen Browser-Tab und navigiert direkt zur PB8-Backtest-Queue. Die zugehoerigen Zeilen bleiben markiert, waehrend konfigurierte Timeframe-, Holdout- und Full-Range-Jobs laufen. Sobald die gesamte Gruppe einen Endstatus erreicht, loest PBGui die erfolgreichen Result-Batches auf und oeffnet automatisch den vorhandenen Results-Compare-Chart; fehlgeschlagene oder gestoppte Jobs werden gemeldet und uebersprungen. Der Handoff ueberlebt Reloads bis zu sieben Tage und verwendet keine vom Modell geratenen Result-Pfade.

Offene Reviews bleiben sieben Tage verfuegbar und ueberleben API-Restarts. Beim Approve wird der aktuelle Config-Digest weiterhin erneut geprueft, sodass ein altes Proposal keine zwischenzeitlich geaenderte Config ueberschreiben kann.

Python-stdout wird wenn moeglich als striktes JSON, sonst als begrenzter Text zurueckgegeben. Begrenztes stderr, Exit-Status, Timeout-Status und Truncation werden mit dem Resultat angezeigt. Fuer eigene Berechnungen ueber einen ganzen Optimizer-Run kann der Agent Python direkt an eine Optimizer-Run-Resource binden: PBGui loest alle Pareto-Kandidaten und deren bereinigte Metriken serverseitig in Sandbox-stdin auf, ohne den kompletten Datensatz durch Modell-Toolargumente zu senden. Das Review zeigt den exakten Code sowie Resource, Kandidatenzahl, Bytemenge und Dataset-Digest. Gewoehnliche gewichtete Min-/Max-Rankings koennen das native Complete-Run-Ranking verwenden, das ebenfalls alle Kandidaten scannt und keine Gesamtaussage aus einem auf 200 Zeilen begrenzten Preview ableitet.

Genehmigte Optimizer-Dataset-Python-Analysen verwenden unprivilegierte Landlock-Dateisystemisolation plus einen Kernel-Seccomp-Filter und benoetigen daher weder UID-Mapping noch ein isoliertes Loopback-Interface. Sie koennen nur Python-Runtime und genehmigtes Skript lesen, keine Dateien schreiben, kein Netzwerk verwenden und keine Geschwisterprozesse untersuchen oder signalisieren. Nach ausdruecklicher Freigabe kann Workspace-Python zusaetzlich maskierte Read-only-Mounts unter `/workspace/pbgui_data`, `/workspace/pb7` und `/workspace/pb8` erhalten; dieser staerkere Pfad benoetigt Bubblewrap-Namespace-Unterstuetzung und schlaegt fail-closed fehl, wenn der Host sie verweigert. Credential-/API-Key-/Token-/Passwort-/Session-/Cookie-/SSH-/Private-Key-/Zertifikatspfade, `.env`, Git-Metadaten, virtuelle Environments und alle symbolischen Links werden immer maskiert. Proposal-Entscheidungen und Resultate verwenden die vorhandene owner-only durable Action-History. Python-Analyse ist kein Restart-Blocker: Graceful Shutdown bricht sie ab und wartet auf das Prozessende; ein hart unterbrochener Lauf wird als interrupted gespeichert und niemals erneut ausgefuehrt.

Der Key wird nach dem Verbindungsversuch im Browser geleert. PBGui zeigt einen gespeicherten Key niemals an.

## Unterhaltungen

PBGui loescht Gespraeche nach 30 Tagen ohne Aktivitaet automatisch, sobald die Historie geoeffnet oder ein neuer Chat erstellt wird. Pro Benutzer bleiben hoechstens 100 Gespraeche gespeichert; ein weiterer Chat entfernt das aelteste inaktive Gespraech. Eine laufende Antwort wird niemals geloescht. Falls das ausgewaehlte Gespraech abgelaufen ist, waehlt PBGui das naechste verfuegbare und erklaert den Wechsel. **Delete** entfernt ein ausgewaehltes Gespraech weiterhin sofort.

Unterhaltungen und abgeschlossene Nachrichten werden serverseitig in einer owner-only History gespeichert. Die vollstaendige AI-Chat-Seite und der globale Drawer verwenden dieselbe Conversation-Liste. Die Auswahl eines History-Eintrags stellt Nachrichten, zuletzt verwendeten Provider, Modell, Reasoning-Aufwand, laufenden Zustand, Fehler und offene Proposals wieder her. Beim Senden einer Folgefrage bleibt dieses bestaetigte Transcript sichtbar, waehrend PBGui den neuen ausstehenden Prompt ergaenzt. Provider, Modell und Reasoning-Aufwand koennen zwischen Turns frei gewechselt werden, ohne eine neue Conversation zu erzeugen. Wenn dafuer ein neuer zustandsbehafteter Provider-Thread erforderlich ist, uebergibt PBGui ein begrenztes Transcript, damit das ausgewaehlte Modell im bestehenden Kontext fortfahren kann. **New chat** bereitet eine weitere Unterhaltung vor; bei vollem Limit kann die Aufbewahrungsregel die aelteste inaktive entfernen. **Delete**, **Rewind** und Proposal-Freigaben verwenden den gemeinsamen PBGui-Dialog, den der globale Drawer auf jeder Seite beim Oeffnen mitlaedt. Delete entfernt nur die ausgewaehlte Unterhaltung. Der globale AI-Button in der oberen Navigation oeffnet auf jeder authentifizierten PBGui-Hauptseite einen einklappbaren Drawer von rechts; die vollstaendige Seite bleibt fuer Provider-Setup und groessere Sitzungen erhalten.

Auf dem Desktop laesst sich die Breite am linken Rand vom kompakten bedienbaren Minimum bis zur gesamten Browserbreite ziehen. PBGui speichert sie als owner-only serverseitige Einstellung und stellt sie auf anderen Seiten und in spaeteren Sitzungen wieder her. Ein kleineres Browserfenster begrenzt den gespeicherten Wert nur temporaer auf den verfuegbaren Viewport. Auf Mobilgeraeten bleibt der Drawer unabhaengig davon vollbreit.

In beiden Oberflaechen gestartete Turns gehoeren dem API-Prozess und nicht dem Browserrequest. Ein Wechsel auf eine andere PBGui-Seite, das Schliessen der vollstaendigen Seite, der Wechsel der Unterhaltung, **New chat** oder das Einklappen des Drawers beendet den Turn nicht. Beide Oberflaechen verbinden sich ueber den persistenten Conversation-Snapshot erneut; nur **Stop** bricht aktive Arbeit ab. Bei einem API-Restart wird unfertige Arbeit als unterbrochen markiert und niemals automatisch erneut ausgefuehrt.

Jede Nachricht bietet **Copy**. User-Nachrichten bieten zusaetzlich **Rewind**. Rewind entfernt diese Nachricht und alle folgenden Antworten persistent, verwirft offene Proposals des entfernten Zweigs, setzt den Provider-Kontext zurueck, behaelt die aktuell im Drawer ausgewaehlte Provider-/Modellkombination bei und stellt den Prompt zum Bearbeiten oder erneuten Senden wieder her.

Proposal-Reviews zeigen einen rot/gruenen Feld-Diff statt rohem JSON. Entfernte Werte beginnen mit `-`, neue Werte mit `+`, darueber steht der geaenderte Config-Pfad. Review- und Raw-JSON-Bereich sind vertikal vergroesserbar; Reject und Review & approve bleiben in einem Sticky-Footer sichtbar.

Wenn ein Turn fehlschlaegt und sein Prompt in der aktuellen Browserseite noch vorhanden ist, sendet **Retry** exakt diesen Prompt erneut. PBGui speichert fehlgeschlagene Prompts absichtlich nicht separat. Eine spaeter wiederhergestellte Seite zeigt deshalb den Fehler, rät aber keinen frueheren Prompt und bietet keinen unsicheren Retry an, der eine abgeschlossene Anfrage duplizieren koennte.

Der Drawer kann einen kleinen strukturierten Seitenkontext mitsenden: Page-Key, passendes Help-Topic, aktuellen Abschnitt, explizit registrierte Ressourcenreferenzen und optional das fokussierte Feld. Wenn im Run-Editor das Passivbot-Logpanel offen ist, werden bis zu 120 aktuell sichtbare Zeilen als begrenzter und gegen Zugangsdaten redigierter Auszug angehaengt. Einfache Bot-Statusfragen benoetigen dadurch kein Python-Proposal. Der Optimize-Pareto-Kontext identifiziert den aktuell offenen Run ueber Name, Pareto-Anzahl und Modified-Zeit und enthaelt bis zu den aktuell markierten Kandidatennamen. Dadurch behaelt eine Folgeanweisung wie „backteste diese drei“ die sichtbare Auswahl. Context-Chips zeigen, was angehaengt wird. Kontext und Logauszuege gelten als nicht vertrauenswuerdige Daten und gewaehren niemals weitere Rechte. Produktive Seiten registrieren ausgewaehlte Configs, Dashboards, Coins, Exchanges, Hosts, Abschnitte und explizit freigegebene nicht-sensitive Fokusfelder ueber `PBGuiAI.registerPageContext()`.

PBGui durchsucht keine beliebigen Seitentexte, Tabellen, Formulare, URLs oder Browser-Storage. Die gemeinsame Context-Grenze verwirft Credential-, Passwort-, Token-, API-Key-, Private-Key-, Session-, Cookie-, SSH-, Secret- und allgemeine Log-Felder. Die einzige Log-Ausnahme ist der oben beschriebene begrenzte Passivbot-Auszug, der im Browser und erneut in der API redigiert wird. API Keys und Logging liefern nur eine nicht-sensitive Benutzeridentitaet beziehungsweise den Abschnitt.

Ein neuer User-Turn verwirft jedes noch offene Proposal des vorherigen Conversation-Zweigs. Ein Proposal sollte deshalb vor einer anderen Frage genehmigt oder abgelehnt werden. So kann eine alte gepruefte Aktion nicht gegen einen neueren Unterhaltungsstand fortgesetzt werden. Wenn eine genehmigte Aktion erfolgreich war, aber der automatische AI-Folge-Turn einen Timeout hat, meldet PBGui die abgeschlossene Aktion und den Folgefehler getrennt.

Fuer kontextbezogene Hilfe kann der Agent die kanonischen englischen/deutschen PBGui-Guides lesen oder durchsuchen und bei Implementierungsdetails anschliessend die vorhandenen Tools fuer installierte Passivbot-Dokumentation und Source verwenden.

Waerend eine Antwort laeuft, zeigt die Statuszeile Laufzeit und sichere Aktivitaetsangaben wie Dokumentationssuche oder Source-Lesen. Tool-Argumente, Provider-Reasoning, Prompts und Resultate werden dort nicht offengelegt. **Stop** bricht den aktiven Provider-Request ab.

Wenn eine Stop-Anfrage fehlschlaegt, zeigt AI Chat den Fehler und aktiviert die Bedienelemente erneut, damit der Abbruch wiederholt werden kann.

Activity und Retry stehen unten im Chat beim Composer. Wenn ein OpenAI-Responses-Modell eine ausdrueckliche Reasoning-Summary liefert, speichert und zeigt PBGui sie eingeklappt unter **Reasoning summary**. Verborgene oder verschluesselte Gedankengaenge werden niemals offengelegt.

Reasoning-Varianten wie `high`, `xhigh`, `max` oder `ultra` koennen nach einem Tool-Resultat mehrere Minuten weiterarbeiten, bevor Antworttext erscheint. Nach Abschluss einer PBGui-Capability wechselt der Status deshalb von der Tool-Aktion zu **model is processing results**, damit eine langsame Reasoning-Phase nicht wie eine haengende lokale Suche wirkt. PBGui beendet einen Turn, der den begrenzten Zeitrahmen ueberschreitet, mit einem normalen Timeout-Fehler.

## Datenschutz

Nachrichten und aktivierter Seitenkontext werden an den ausgewaehlten externen Anbieter gesendet. PBGui speichert die Conversation-History privat, schreibt Prompts und Antworten aber nicht in operative Logs. Vor dem Senden sensibler Informationen sollten die aktuellen Datenschutz-, Aufbewahrungs- und Abobedingungen des Anbieters geprueft werden.

## Fehlerbehebung

- **Runtime missing:** Die PBGui-Abhaengigkeiten inklusive `openai-codex-cli-bin` installieren und den API-Service neu starten.
- **Browser-Login-Callback schlaegt fehl:** Browser Login setzt voraus, dass Browser und PBGui API auf demselben Rechner laufen; fuer einen entfernten Host Device code verwenden.
- **Device login unavailable:** Falls erforderlich, Device-Code-Login in den Sicherheitseinstellungen des ChatGPT-Kontos aktivieren.
- **Authentication failed:** Den Anbieter erneut verbinden und Abo beziehungsweise Key pruefen.
- **Usage limit reached:** Auf das Zuruecksetzen des Anbieterlimits warten oder einen anderen verbundenen Anbieter waehlen.
- **Selected model is currently unavailable:** Das Modell wird angeboten, besitzt aktuell aber keine gesunde Upstream-Kapazitaet; ein anderes Modell waehlen und spaeter erneut versuchen.
- **Eine Tool-faehige Antwort benoetigt mehrere Requests:** Jedes Capability-Resultat muss an das zustandslose Modell zurueckgesendet werden. PBGui begrenzt dies auf drei Capability-Runden und fordert danach eine finale Antwort aus den gesammelten Resultaten an.
- **Python analysis sandbox is unavailable:** Bubblewrap und `prlimit` auf dem PBGui-Host installieren. PBGui bietet absichtlich keinen unsandboxed Fallback.
- **Python-Analyse hat einen Timeout oder gekuerzte Ausgabe:** Kleinere Eingabedaten, eine einfachere Berechnung oder ein kompakteres JSON-Resultat anfordern.
- **ChatGPT bleibt bei model is processing results:** Hoeherer Reasoning-Aufwand kann nach Abschluss des lokalen Tools deutlich laenger dauern. Falls sich das Warten nicht mehr lohnt, **Stop** waehlen oder einen neuen Chat mit Standard beziehungsweise einer niedrigeren vom Modell angebotenen Variante starten.
- **No supported models:** Nach dem Verbinden aktualisieren und pruefen, ob das OpenCode-Konto aktuell Modelle im Zen- oder Go-Live-Katalog bereitstellt.

Bei fehlgeschlagenen ChatGPT-Antworten unterscheidet PBGui erkannte Nutzungs-/Anfragelimits, Modellzugriff, Anmeldung, Kontextlimit, Verbindungsprobleme und Serverfehler. Unbekannte Ursachen werden ausdrücklich so angezeigt und belegen keine Free-Sperre. Anbietertexte werden zum Schutz sensibler Inhalte nicht ungefiltert ausgegeben.

### Mehrere ChatGPT-Abos

Unter **ChatGPT profile** lassen sich getrennte Anmeldungen für deinen PBGui-Benutzer verwalten. Die bisherige Anmeldung und ältere Chats gehören zu **Default**. Einen Profilnamen eingeben, **Add profile** wählen und das gewünschte Abo per Browser login oder Device code anmelden. Jedes Profil kann separat umbenannt, abgemeldet oder entfernt werden. Nach der Anmeldung Konto und Tarif prüfen: Im Browser kann noch das vorherige OpenAI-Konto angemeldet sein.

Die Auswahl eines anderen Profils beginnt einen neuen Chat. Bestehende Chats behalten ihre Profilzuordnung, auch nach API-Neustart oder Browser-Refresh. ChatGPT-Chats zeigen den Profilnamen im Verlauf. Beim Entfernen eines Profils bleibt der Verlauf erhalten; diese Chats werden nicht auf ein anderes Abo umgeleitet. Ein ausgeschöpftes Limit löst niemals einen automatischen Kontowechsel aus.

Konto, Tarif und verfügbare Nutzungsfenster aktualisieren sich für das ausgewählte Profil automatisch. Limits werden nur angezeigt, wenn OpenAI sie bereitstellt. Zugangsdaten bleiben in getrennten Profilverzeichnissen auf dem Server. In der Browser-URL stehen ausschließlich Profilkennungen und Navigationskontext. Pro PBGui-Benutzer sind bis zu 20 Profile möglich.

Das obere **Provider**-Menü zeigt jedes ChatGPT-Profil separat als **ChatGPT · Profilname**. Die Auswahl synchronisiert das Profil in der Seitenleiste und die verfügbaren Modelle.

Auch der kompakte AI-Seitenbereich zeigt jedes ChatGPT-Profil im Provider-Menü. Ein Profilwechsel beginnt einen separaten Chat.

Nutzungslimits zeigen den verbleibenden Prozentsatz mit Balken pro gemeldetem Zeitfenster (monatlich, wöchentlich, täglich oder 5 Stunden) und den Rücksetzzeitpunkt. Das Zeitfenster ist keine verfügbare Rechenzeit.

Wurde eine Anmeldung unterbrochen oder ging ihr Link nach dem Neuladen verloren, erneut Browser login oder Device code anklicken. PBGui beendet den eigenen vorherigen Anmeldeversuch und zeigt einen neuen Link.

Der kompakte AI-Seitenbereich zeigt die Nutzung des ausgewählten Anbieters und ChatGPT-Profils und aktualisiert sie automatisch alle 30 Sekunden. OpenCode Go zeigt 5-Stunden-, Wochen- und Monatslimits mit Rücksetzdatum in beiden Chat-Ansichten. Das Zen-Guthaben ist weiterhin in der OpenCode-Konsole verfügbar.

Beim Löschen eines Chats wird dessen laufender Antwortstatus bereinigt. Während der Prüfung eines Vorschlags bleiben andere Vorschläge sichtbar; ein Abbruch stellt die betreffende Karte wieder her.
