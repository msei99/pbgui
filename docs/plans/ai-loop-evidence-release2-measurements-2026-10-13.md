# Release 2: Messbericht zur KI-Loop-Evidenz

Stand: 13.10.2026. Schritt 1 abgeschlossen. Keine Implementierungs-, Profil- oder Release-Freigabe wird daraus abgeleitet. Produktivcode, laufende Jobs, Mieten und Anbieter wurden für diese Messung nicht verändert oder aufgerufen.

## 1. Der Fehlernachbau ist jetzt exakt

Quelle: Loop `c1a107cbb59845108260ee73882b864e`, gespeicherte Bewertungsanfrage der sechsten Runde (interner Index 5). Der Wissensstand wurde vor der Auswahl der letzten zwölf Einträge auf den Anfragebeginn begrenzt. Der Controller lieferte seine Evidenz im isolierten Replay; Persistenz und externe Backend-Aktionen waren blockiert. Die unveränderte Request-Erzeugung wurde bis vor Reservierung und Anbieteraufruf ausgeführt.

Der rekonstruierte, bereits mit der damaligen Verdichtung versehene Inhaltsbaum stimmt mit `pending_ai.sha256` überein:

```text
f9f3c09f2a900c6de1d4e1d6b0f7f38c00026d4941a7df06b4f7e249ff003341
```

Damit ist der JSON-Inhaltsbaum bewiesen, einschließlich der damaligen zwölf Wissenseinträge. Dies beweist weder den damaligen Systemprompt noch ein internes Anbieter-Envelope oder tatsächliche Modell-Tokenzahlen.

| Darstellung desselben Request-Inhalts | Zeichen | Proxy-Tokens |
| --- | ---: | ---: |
| Damaliges JSON mit Standard-Abständen | 1.049.701 | nicht separat gemessen |
| Derselbe Inhalt, kompakte JSON-Schreibweise | 994.505 | 336.461 |

Allein das damalige JSON überschritt die Zeichenbegrenzung von 1.048.576 um **1.125 Zeichen**. Kompakte Schreibweise genügt für diesen einzelnen Fall, löst aber die Kontextgröße nicht. Die Proxy-Zahl verwendet das lokale, hashgeprüfte `cl100k`-Vokabular; sie ist keine gemessene Codex-Tokenzahl.

| Anteil am kompakten JSON | Zeichen | Proxy-Tokens |
| --- | ---: | ---: |
| `knowledge` | 420.245 | 118.860 |
| `evidence` | 526.027 | 205.457 |
| `initial_config` | 12.560 | 3.770 |
| `goals` | 8.167 | 1.625 |
| `rubric` | 9.046 | 1.814 |
| `optimizer_metric_policy` | 16.102 | 4.314 |

Die sechs aktuellen Beobachtungen enthalten je acht Reports. Summen einzelner Tokenmessungen müssen wegen der Tokenisierung von Trennzeichen nicht exakt die Gesamtmessung ergeben.

## 2. Messmethode und Grenzen des A-Prototyps

- Nur `evaluate` ist ein exakt rekonstruierter historischer Request. Die anderen Phasen wurden mit demselben gespeicherten Zustand durch die jeweiligen echten Controller-Zweige geführt. Sie sind **phasenspezifische Belastungsfälle**, keine Rekonstruktion ihrer damaligen Anfragen. Insbesondere verwendet `bootstrap` die 26 gespeicherten Kandidaten als historische Beispielkandidaten; `interpret` enthält konservativ die bereits vorhandene Bewertungsregel. Ein frischer Interpretationslauf hat diese noch nicht.
- Vollständige Rohdaten bleiben für B verfügbar. A behält vorhandene exakte Ziel-/Sicherheitswerte, Bewertungen und Kandidatenänderungen. Fehlende Liquidationsfelder werden nicht ergänzt. Baseline und bestätigter Gewinner wurden auch in den übrigen Phasen als konservativer Mindestnachweis mitgegeben.
- Der Prototyp liest Zielmetriken aus den 17 vorhandenen Regeln und erhält zusätzlich Abschluss-, Drawdown- und vorhandene native Liquidationsfelder. Diese werden aus den Rohreports übernommen: Der alte Verdichter legt einige Liquidationsfelder in die Zusatztabellen; bloß diese Tabellen zu entfernen wäre falsch.
- Identische Ziel-/Vergleichsblöcke stehen einmal in A und werden dort eindeutig referenziert. Exakte Gleichheit beim Wiederaufbau wurde per Assertion geprüft. Keine freien Texte wurden zusammengefasst.
- Für `select` sind die tatsächlichen Kandidatenkonfigurationen berücksichtigt, obwohl der heutige Controller nur ID, Optimizer-Herkunft und Metriken übergibt. Gemeinsame Konfigurationsänderungen stehen einmal in A; variable Änderungen in einer verlustfreien Spaltentabelle. Die ursprünglichen Änderungslisten werden exakt rekonstruiert und geprüft. Herkunft und Overrides bleiben enthalten.
- Ausführliche Job-Konfigurationskopien und native Zusatzmetriken sind B-Daten. Die erforderlichen Report-Werte und experimentrelevanten Kandidatenänderungen bleiben in A.
- „Mindesthistorie“ bedeutet in dieser Messung: vollständiger jüngster Wissenseintrag plus Verzeichnis aller zwölf Einträge, separat Baseline/Gewinner und aktuelle Fehlerhinweise. Das ist bewusst umfangreicher als ein minimaler jüngster Ergebnisdatensatz. Die Umsetzung muss ältere Einträge schrittweise verschieben; diese Messung zeigt zunächst beide Endpunkte.
- Der Pager bündelt kleine Teilbäume bis 1.000 Proxy-Tokens zu adressierbaren Einträgen und diese zu Seiten. Lange skalare Texte werden mit Zeichenbereichen geteilt und exakt wieder zusammengesetzt. Die **vollständig serialisierte Seitenantwort** einschließlich Hash, Pfad, Offset, Einträgen und Fortsetzung wird gemessen. Kein bloßes Teilen der Tokenzahl durch die Seitengröße.
- Der Pager ist ein isolierter Messprototyp unter `/tmp`, kein implementiertes Tool oder persistenter Abdeckungscontroller. Ein zukünftiges Inhaltsverzeichnis, zusätzliche Protokollfelder und das endgültige Wrapperformat müssen erneut gemessen werden. Die unten genannte Wrapperreserve ersetzt diesen Test nicht.

## 3. Die vier Phasen

JSON-Zeichen und Proxy-Tokens beziehen sich hier auf kompakte Schreibweise. „Bisher verdichtet“ bezeichnet das vollständige bisherige Format vor einer möglichen Teilübertragung.

| Phase | Roh-JSON Zeichen | Bisher verdichtet Zeichen | Bisher verdichtet Tokens | A mit Historie Tokens | A mit Mindesthistorie Tokens |
| --- | ---: | ---: | ---: | ---: | ---: |
| `interpret` | 1.196.464 | 468.133 | 130.862 | 112.219 | 25.960 |
| `bootstrap` | 1.864.542 | 1.077.739 | 330.924 | 220.145 | 133.886 |
| `select` | 1.415.089 | 552.497 | 170.641 | 121.645 | 35.386 |
| `evaluate` | 2.844.746 | 994.505 | 336.461 | 137.487 | 51.228 |

Tatsächlich gebündelte Pflichtseiten, jeweils **volle Historie / Mindesthistorie**:

| Phase | 2.048 Proxy-Tokens/Seite | 4.096 | 5.000 |
| --- | ---: | ---: | ---: |
| `interpret` | 80 / 21 | 36 / 10 | 29 / 8 |
| `bootstrap` | 157 / 98 | 69 / 43 | 55 / 34 |
| `select` | 86 / 27 | 39 / 13 | 31 / 10 |
| `evaluate` | 99 / 41 | 45 / 19 | 35 / 14 |

Die festen 2.048-Token-Seiten würden bereits den echten Bewertungsfall mit Historie unnötig ausschließen. Das bestätigt die Entscheidung, Seitengröße und Historienumfang anhand der Budgets zu bestimmen. `bootstrap` ist bewusst konservativ: Seine historischen Beispielkandidaten enthalten weiterhin volle Konfigurationen; deren normalisierte Deltas wären eine weitere mögliche Einsparung, wurden für diese Phase nicht vorausgesetzt.

## 4. Auswahl mit vielen Kandidaten

Gespeichert sind **26 Kandidaten**. Eingestellt sind `pareto_max_size = 1000` und ein paralleler Optimizer-Job. Für 100 und 1.000 Kandidaten wurden die vorhandenen 26 Datensätze mit neuen synthetischen IDs zyklisch verwendet. Werte, Herkunft und Konfigurationsunterschiede stammen aus diesen Datensätzen; dies ist **kein Worst-Case-Beweis** für 1.000 beliebige andere Kandidaten.

| Kandidaten | A mit Historie Tokens | A mit Mindesthistorie Tokens | Pflichtseiten bei 5.000, volle / Mindesthistorie |
| --- | ---: | ---: | ---: |
| 100 | 144.435 | 58.176 | 36 / 15 |
| 1.000 | 425.197 | 338.938 | 99 / 78 |

**Neuer, gemessener Auslegungsbedarf:** Bei 1.000 Kandidaten passen selbst mit Spaltentabellen und verschobener Historie noch **78 Pflichtseiten** nicht in das Beispielbudget von 48 Seiten. Ein größeres Kontextfenster allein beseitigt dieses Aufrufproblem nicht. Mit voller Historie überschreitet auch das A-JSON wieder 1 MB; mit Mindesthistorie bleibt es darunter, enthält aber rund 339.000 Proxy-Tokens.

Vor einer Freigabe für diesen Umfang muss der Plan konkretisieren, welche Konfigurationsfelder die Auswahl zwingend benötigt. Alle Kandidaten-IDs, exakten Auswahlmetriken und vorhandenen Sicherheitsnachweise bleiben erhalten. Vollständige Konfigurationsunterschiede dürfen nur dann nach B, wenn sie für die betreffende Auswahl nicht verpflichtend sind; die Änderung dieser Pflichtdefinition bedarf einer ausdrücklichen Planentscheidung. Andernfalls braucht es einen gesondert geprüften Vergleichs-/Auswahlablauf oder ein anderes tragfähiges Profil. **Keine stillschweigende Kandidatenkürzung, Rangliste oder pauschale Grenze von 100 einbauen.**

## 5. Vorläufiges Entwicklungsprofil, noch nicht freigegeben

Für den Prototyp ist folgende Ausgangsvariante sinnvoll:

| Parameter | Vorschlag | Nachweisstatus |
| --- | --- | --- |
| `T_page` | höchstens 5.000 Proxy-Tokens der gesamten Antwort | Offline-Pager gemessen |
| `F` | 1,25 | konservative Annahme, keine gemessene Modellabweichung |
| `W` | 512 Tokens zusätzliche Protokollreserve | vorläufig |
| `L_tool` | 8.192 tatsächliche Tokens | erst im genehmigten echten Codex-Gate zu bestätigen |
| `H / N / B_min / R` | 64 / 1 / 8 / 8 | vorläufige Kontingente, kein Bedarfsnachweis für B |
| Pflichtseitenbudget | `(64 − 8 − 8) / 1 = 48` | rechnerisch, zusätzlich Kontext-/Zeichenbudget prüfen |

Für jede Seite gilt zusätzlich `1,25 × Tokens_proxy(gesamte Antwort) + 512 ≤ 8192`. Mit 5.000 Proxy-Tokens wären das höchstens 6.762 Tokens nach Aufrundung. Damit bleibt eine Reserve; ob sie für die echte Runtime genügt, zeigt ausschließlich das Release-Gate.

Die acht B-Aufrufe sind ein Entwicklungswert. Ohne echtes Modellverhalten lässt sich nicht belegen, dass sie in allen Phasen reichen. Fehlermeldungen und Inhaltsverzeichnis waren noch keine produktiven Tool-Antworten und müssen ebenfalls gegen das Profil getestet werden.

**Kontext ist eine unabhängige Grenze:** Aktuelle Instruktionen benötigen 1.717 Proxy-Tokens. Beispielhaft, ausdrücklich ohne Behauptung eines tatsächlich verfügbaren 400k-Fensters: Bei `C = 400.000`, Kompaktierungsschwelle `0,75 × C`, Ziel `0,5 × Schwelle`, 32.768 Antwortreserve und 512 Rahmenreserve gilt konservativ:

```text
ceil(1,25 × (Tokens_proxy(A) + 1717)) + 512 + 32768 ≤ 150000
```

Damit bleiben höchstens etwa **91.659 Proxy-Tokens für A**. Die 100-Kandidaten-Messung mit Mindesthistorie passt in dieses Beispiel; volle Historie oder der Bootstrap-Belastungsfall passen nicht automatisch. Seitenbudget allein ist also keine Startfreigabe. Tatsächliche Kontextgröße, Antwortreserve und Kompaktierungsschwelle sind pro Runtime/Modell zu ermitteln; Historie wird anschließend nur soweit nötig verschoben.

## 6. Kosten des Wissenslesens

Die Wissensdatei umfasst **55.861.406 Bytes** (53,27 MiB). Gemessen wurde `store.context()` einschließlich Lesen, JSON-Parsing und Auswahl der zwölf Einträge, ohne Anbieterzugriff.

Der letzte instrumentierte Durchlauf benötigte 1,482 Sekunden; bei den wiederholten Messungen lagen die Zeiten ungefähr zwischen 1,0 und 3,0 Sekunden. `tracemalloc` meldete **106,56 MiB** Spitzenallokation im Messabschnitt. Prozessweiter Spitzen-RSS: etwa **240,5 MiB**, einschließlich Imports und vorheriger Record-Ladung; dies ist kein inkrementeller Speicherverbrauch von `context()`.

Instrumentierung und Betriebssystemcache beeinflussen die Laufzeit. Das vollständige Lesen der Wissensdatei verursacht messbaren Aufwand, erklärt aber nicht den Zeichenüberlauf. Ein größerer Speicherumbau bleibt außerhalb dieses Schritts.

## 7. Prüfungen und nächster Freigabepunkt

- Historischer Request-Hash: identisch.
- Alle zwölf Phase-/Seitengrößenkombinationen jeweils mit voller und Mindesthistorie sowie beide Auswahl-Belastungsfälle durchgerechnet; maximal serialisierte Seitenantwort gegen das jeweilige Proxy-Limit geprüft.
- Exakter Wiederaufbau referenzierter Ziel-/Vergleichsblöcke, gemeinsamer und tabellierter Konfigurationsänderungen sowie geteilter skalarer Texte per Assertion geprüft.
- Kein vollständiger Testlauf und kein Anbieter-/Codex-Test ausgeführt. Dies ist ein Messprototyp, keine Abnahme des geplanten Controllers.
- Bestehende uncommittete Änderungen anderer Arbeitsschritte blieben bestehen. Kein Commit, Push, Release oder API-Neustart.
- Vor Umsetzung: Messbefund für große `select`-Mengen entscheiden und vorläufiges Profil prüfen lassen. Danach Prototyp/Offline-Tests; der echte Codex-Test bleibt ein gesondert genehmigtes Release-Gate.

Aggregierte Messdaten: [JSON](ai-loop-evidence-release2-measurements-2026-10-13.json). Keine Rohreports, Konfigurationen, freien Erkenntnistexte oder Zugangsdaten wurden in diesen Dateien abgelegt.

Messskript dieser Sitzung: `/tmp/pbgui-measure-evidence.py`. Prüfsumme:

```text
25744c0c45c88de5340a06788214ceee0d4b3102aa3d4a8ba62cbb97e1de3993
```

## 8. Nachmessung nach Claudes Codeprüfung

Diese Nachmessung präzisiert Abschnitt 4. Dessen Zahlen bleiben als Messung der damaligen, umfangreicheren Pflichtdefinition erhalten. Für den folgenden Auswahlentwurf werden Konfigurationen ausdrücklich B zugeordnet; der vorherige A-Prototyp hatte sie zusätzlich aufgenommen. **Das grundlegende Problem ist die Datenmenge im Kontext, nicht die Seitengröße.**

### Bestätigte bestehende Begrenzungen

- `pb8_loop_backend.py`, `_result_candidates`: `files[:512]` begrenzt das Einlesen ohne Anzeige ausgelassener Kandidaten. Die Dateiliste kommt aus `_pareto_files_for_listing` alphabetisch nach Dateinamen, nicht als geprüfte fachliche Rangliste. Ein Job mit 1.000 vorhandenen Artefakten erreicht den bisherigen Controller somit nicht vollständig.
- `existing_candidates` sammelt normalerweise höchstens drei Ergebnisquellen und verwendet denselben Leser: bis zu 1.536 historische Kandidaten mit vollständigen Konfigurationen. Die Zahl zusätzlicher ausgeschlossener Quellen wird ebenfalls nicht als Auswahlentscheidung ausgewiesen.
- Große `select`-Mengen bleiben über mehrere Jobs sowie nachgeholte Jobs möglich. Die bisherige 1.000er-Messung war ausdrücklich synthetisch, kein beobachteter vollständiger Import aus einem Job.

### Neue Pflichtdefinition für die Messung

`select`: jede ID, Optimizer-Herkunft und konkrete Artefakt-Provenienz; vorhandene Ziel-/Proxy-/Sicherheitsmetriken aus derselben eingefrorenen Regel. Metriken stehen in einer Spaltentabelle; fehlend und null bleiben unterscheidbar. Konfigurationen und Overrides bleiben im vollständigen Snapshot B und können gezielt nachgelesen werden. Baseline/Gewinner und Historien-Mindestumfang sind weiterhin enthalten.

`bootstrap`: historische Beispielkandidaten einschließlich Herkunft und Overrides; Konfigurationen als exakte Deltas zur Startkonfiguration, gemeinsame Änderungen einmalig, variable Änderungen in Spaltentabellen. Metriken ebenfalls tabelliert. Gleichheitsprüfungen decken sowohl den Wiederaufbau der Metrikvektoren als auch der Konfigurationsänderungslisten ab.

Es wurde **keine Duplikatgruppierung** für die Budgetmessung angewendet. Die synthetischen Datensätze wiederholen die vorhandenen 26 Kandidaten zyklisch mit neuen IDs. Eine Gruppierung hätte diese künstliche Wiederholung ausgenutzt und eine irreführend günstige Kapazität ergeben. Die Messung ist trotzdem keine universelle Obergrenze für andere Regeln, Zahlendarstellungen oder Konfigurationen.

### Ergebnisse

5.000 Proxy-Tokens sind weiterhin nur die vorläufige Obergrenze einer gesamten Seitenantwort.

| Phase / Kandidaten | A mit Historie Tokens | A mit Mindesthistorie Tokens | Pflichtseiten, volle / Mindesthistorie |
| --- | ---: | ---: | ---: |
| `select` / 100 | 129.546 | 43.287 | 33 / 12 |
| `select` / 512 | 200.900 | 114.641 | 50 / 29 |
| `select` / 1.000 | 285.414 | 199.155 | 70 / 50 |
| `bootstrap` / 26 | 120.954 | 34.695 | 31 / 10 |
| `bootstrap` / 512 | 263.814 | 177.555 | 65 / 44 |
| `bootstrap` / 1.536 | 566.443 | 480.184 | 138 / 117 |

**Die Kontextgrenze greift deutlich früher:** Bei 512 Auswahlkandidaten genügen 29 Pflichtseiten, aber 114.641 Proxy-Tokens überschreiten bereits die beispielhaften 91.659 für A. Bei 1.000 sind es 199.155 Tokens und 50 Seiten. Bootstrap mit 512 benötigt trotz Deltas 177.555 Tokens; 44 Seiten allein würden dagegen passen. Ein höheres Aufruflimit wäre daher keine Lösung.

### Ableitung eines sichtbaren Quellbudgets

Die ungruppierten Auswahlmessungen wachsen zwischen 100, 512 und 1.000 Kandidaten um ungefähr 173,2 Proxy-Tokens je zusätzlichem Kandidaten. Für diesen Datensatz umschließt `25.887 + 174 × K` alle drei gemessenen Mindesthistorien-Werte. Bootstrap benötigt in den gemessenen Intervallen ungefähr 294–296 je Kandidat; `26.999 + 296 × K` umschließt die drei hier gemessenen Werte.

Nur als Rechenbeispiel zum bisherigen hypothetischen A-Budget 91.659:

```text
select:    floor((91659 − 25887) / 174) = 378 Kandidaten insgesamt
bootstrap: floor((91659 − 26999) / 296) = 218 Kandidaten insgesamt
```

**378 und 218 sind keine freigegebenen Produktwerte oder garantierten Obergrenzen.** Die gerundeten Steigungen sind empirische Kosten dieses Datensatzes, nicht ein Beweis für beliebige Konfigurationen. Zusätzliche Varianzreserve, anderes Kontextfenster, Antwortreserve, Inhaltsverzeichnis, Regelumfang, Herkunftslängen und ein anderer Pager verändern sie. Token-, Zeichen- und Seitenbudget gelten gleichzeitig; die endgültige serialisierte Anfrage wird weiter genau geprüft.

Für J eigene Optimizer-Jobs wäre das gemeinsam verfügbare Kandidatenbudget auf J aufzuteilen, statt jedem Job die gesamte Kapazität zuzuteilen. Historien-Zusatzdaten dürfen vor dieser Planung nach B verschoben werden. Nachgeholte Jobs und tatsächlich größere Datensätze werden vor der Entscheidung zusätzlich berücksichtigt; sie dürfen keine erneute stille Kürzung verursachen.

### Bewertung des Vorschlags

1. Die reduzierte Pflichtdefinition für `select` und die zusätzlichen Messungen sind sinnvoll. Konfigurationen bleiben zugänglich. Bei einer späteren exakten Gewinnervalidierung bleiben die erforderlichen Konfigurationen und Sicherheitsnachweise verpflichtend.
2. Duplikate dürfen nur bei tatsächlich gleicher Konfiguration **einschließlich Overrides**, gleichem Metrikvektor und vergleichbarem Szenario-/Datenkontext gruppiert werden. Alle IDs und individuellen Provenienzen bleiben erhalten. Gruppierung ist keine Auswahl und darf keine Metrikmittelung erzeugen. Das Budget darf keine günstige Duplikatrate voraussetzen.
3. Ein sichtbares, budgetabhängiges `pareto_max_size` für eigene neue Loop-Jobs ist als **Planvorschlag** tragfähig. Es verändert jedoch das native Archiv und möglicherweise den Optimierungsverlauf; es ist keine informationsneutrale Verdichtung. Das konkrete PB8-Archivverfahren und die wirksame Durchsetzung müssen vor Implementierung geprüft werden. Bestehende/gewöhnliche Optimizer-Konfigurationen werden nicht global umgeschrieben.
4. Historische Quellen sind anders: Ein kleineres Budget kann deren bestehende Archive nicht nachträglich ändern. „Die übrigen bleiben in B“ allein definiert noch keine vertretbare Auswahl für A. Herkunftsauswahl, Reihenfolge und Anzahl müssen vor dem Lauf ausdrücklich sichtbar und freigegeben sein. Keine automatische Übernahme der ersten N Dateinamen oder heimliche Proxy-Rangliste. Ohne solche Auswahl bleibt ein zu großer Bootstrap pausiert; ein ausdrücklich angebotener Wechsel zu frischer Optimierung ohne historische Bootstrap-Beratung ist ein möglicher, noch freizugebender Ausweg. Frische Populationen und Seeds bleiben in jedem Fall vorgeschrieben.
5. Restüberlauf bekommt `context_budget`, mit getrennten Diagnosen für Tokens, Zeichen und Seiten. Ein Modellwechsel muss die tatsächlich verletzte Grenze beheben. Gruppierte Vergleiche und Dominanzfilter werden nicht nebenbei eingebaut.

Vor der Umsetzung sind damit zwei inhaltliche Entscheidungen zu bestätigen: das sichtbare native Archivbudget als Ressourcengrenze sowie die konkrete historische Bootstrap-Auswahl/Überspring-Regel. Die Nachmessung allein autorisiert beides nicht.

Aggregierte Nachmessung: [JSON](ai-loop-evidence-release2-followup-2026-10-13.json). Messskript: `/tmp/pbgui-measure-followup.py`; SHA-256:

```text
b062f9a60dd3297a351f3c4be27d8852bce95f4e10b42bb90c5e0316385f56cd
```

## 9. Stand nach eigener PB8-Prüfung und Planabstimmung

Die in Abschnitt 8 noch offenen Planentscheidungen sind jetzt in §4a des Hauptplans konkretisiert. Vorschau vor Start bleibt vorläufig; nach `interpret` und vor Bootstrap erfolgt die verbindliche Neuberechnung. Abweichungen werden angezeigt und protokolliert, ohne erneute Bestätigung. Die Option ohne historische Beratung sowie der frische Fallback bei ausschließlich zu großer/ungeeigneter historischer Beratung sind ausdrücklich beschrieben. Zahlen und Runtime-Eigenschaften bleiben vorläufig.

Die eigene nur lesende PB8-Prüfung bestätigt den Ergebnisarchiv-Datenfluss für frische Läufe; Einzelheiten und geprüfter Stand stehen unter §4b des Hauptplans. Ein zusätzlicher relevanter Randfall wurde bestätigt: `prune_front_with_extremes` schützt alle Achsenextreme und kann deshalb ein zu kleines `max_size` überschreiten. Ein isolierter Offline-Helpercheck behielt sechs nichtdominierte Extrempunkte trotz Limit 2. Die Kapazitätsplanung darf das native Limit daher nicht ungeprüft als strikte Höchstzahl verwenden; tatsächliche Ergebnisse und drei Budgets bleiben zu prüfen. Kein PB8-Code oder produktiver Prozess wurde verändert.

## 10. Neue Zielarchitektur: kompaktes Wissen, frische Entscheidungen

Nach der anschließenden Grundsatzentscheidung wird die Historie nicht mehr standardmäßig mit Bewertungen und Beobachtungsblöcken in A übernommen. Jede logische Entscheidung startet frisch; ein separat begrenztes kompaktes Erkenntnispaket übernimmt Beratung und Übergabe. Historische Vollnachweise bleiben in B gezielt abrufbar. Aktuelle Pflichtnachweise, Baseline/Gewinner und explizit einbezogene Bootstrap-Kandidaten folgen weiterhin ihren Sicherheitsregeln.

Die bisherigen Messungen und die exakte Fehlerrekonstruktion bleiben unverändert als Befund bestehen. Die Varianten „volle Historie / Mindesthistorie“ bilden jedoch nicht mehr den endgültigen A-Entwurf ab. Vor Freigabe der Zahlenprofile müssen Wissensbudget, feste Overheads, Pflichtseiten und daraus abgeleitete native Archivkapazität mit der neuen Paketbildung nachgemessen werden. Aus dieser Dokumentänderung folgt keine Produktiv- oder Prototypausführung.

## 11. Präzisierung nach Vertrags-/Speicherprüfung

Der Hauptplan legt unter §4c den Generator und die durchsetzbaren Grenzen fest: neue KI-Texte maximal 1.000 Codepoints, separate kurze Hypothese maximal 600; strukturierte Zahlen und Status kommen vom Controller. Alte Texte werden ausschließlich deterministisch projiziert und bei Überlänge nach B referenziert, ohne nachträgliche KI-Zusammenfassung. `improved` ist im heutigen Controller ein Incumbent-Wechsel, kein generischer Beweis eines numerischen Gewinnanstiegs. Die Wissensauswahl ist fest nach Loop-Bezug, offenen Widersprüchen, Zeit und ID sortiert.

Historische Archive werden einmalig referenziert, nicht pro Snapshot kopiert. Weil der vorhandene Dateinamens-Digest aus der Evidenz-ID stammt, wird zusätzlich der echte Dateiinhalt gehasht und im Snapshot-Manifest gebunden. Snapshot-Bereinigung löscht keine historischen Originalarchive.

**Anbieterentscheidung:** Die neue kompakte Wissenssicht gilt für ChatGPT. OpenCode behält bewusst das vollständige bisherige Wissens-/Eingabeformat ohne Tool. Frühere Formulierungen über eine gemeinsame reduzierte Wissenssicht sind damit ersetzt. Für OpenCode bleibt die tatsächliche vollständige Eingabegröße maßgeblich. Es wurde ausschließlich der Plan präzisiert; Zahlenprofile sind weiterhin nachzumessen und noch nicht freigegeben.

## 12. Nicht blockierender Text-Fallback und Integritätsfehler

Die Textgrenzen 1.000/600 steuern die direkte Aufnahme in das kompakte Paket. Neue überlange Texte werden wie Altbestände vollständig archiviert und nur referenziert; eine ansonsten gültige Entscheidung bleibt gültig. Keine Zusatzanfrage oder Fehlerzähler allein zum Kürzen. ChatGPTs Thread-Korrekturen und OpenCodes bestehender `repair_error`-Pfad bleiben für andere Vertragsfehler ausdrücklich getrennt.

`rejected_exact` ist als beobachtet/nicht qualifiziert mit dem gespeicherten Unsicherheits-/Ablehnungsgrund abgebildet. Ein Archiv mit abweichendem Inhalts-Hash wird niemals ausgeliefert. Erforderliche Nachweise pausieren mit `evidence_recovery`; optionale Daten melden den Fehler ohne automatische Pause. Wiederaufnahme bindet nicht einfach einen neuen Hash, sondern verlangt den ursprünglichen geprüften Inhalt. Diese Präzisierungen ändern nur den Plan, keine Produktivdateien.

## 13. Aufhebung der OpenCode-Ausnahme

Auf ausdrücklichen Nutzerwunsch entfällt die bisher geplante Ausnahme für OpenCode. Beide Anbieter sollen dasselbe kompakte Wissenspaket und denselben manifestgebundenen Evidenzzugriff nutzen. OpenCode bekommt dafür einen begrenzten Tool-Dialogadapter für die tatsächlich angebotenen Responses-, Chat-Completions- und Messages-Protokolle. Frühere Berichtsaussagen über Beibehaltung des alten OpenCode-Wissensformats sind als überholte Planentscheidung zu lesen.

Planungsbasis sind mindestens 256.000 Kontexttokens und nachgewiesene Tool-Fähigkeit. Protokoll-/Verlaufs-Overheads, sämtliche Folgerunden und Usage werden separat budgetiert; Codex-spezifische Komprimierungsereignisse/Tool-Optionen werden OpenCode nicht unterstellt. Offline-Tests und ein separat genehmigtes begrenztes echtes OpenCode-Gate ergänzen das weiterhin erforderliche Codex-Gate. Die bisherigen Messzahlen sind dadurch kein freigegebenes OpenCode-Profil; neue Adaptermessungen bleiben erforderlich. Kein Produktivcode oder laufender Job wurde geändert.

## 14. Funktionale Anbieterparität und verbindliche Betriebsgrenzen

Der Nutzer verlangt ausdrücklich funktionale Gleichheit von OpenCode und ChatGPT: gleicher Wissens-/Evidenzvertrag, dasselbe Lese-Tool und dieselbe Gewinner-/Wiederaufnahmeprüfung. Ein gemeinsamer Entscheidungs-Controller bekommt lediglich unterschiedliche Transport-/Kontext-/Abrechnungsadapter. OpenCodes fachliche Korrekturen dürfen nicht zusätzlich über den alten Drei-Fehler-Pfad scheitern.

Die Codeprüfung bestätigt die bisherigen Engstellen: äußeres `min(180, remaining)` für die gesamte OpenCode-Ausführung, einmalige Reservierung vor `_request` und optimistische Tool-Metadaten bei fehlendem `tool_call`. §5b des Plans trennt jetzt Modellaufruf und Entscheidungsdeadline, budgetiert den kompletten erneut gesendeten OpenCode-Verlauf und führt ein Ledger pro Folgeanfrage mit bestätigten Zen-Token-/USD-Limits je logischer Entscheidung ein. Die N-/Pflichtseitenformel bleibt ausdrücklich ChatGPTs Kompaktierungs-Wiederherstellungsbudget, keine OpenCode-Kontextgarantie.

Nur explizit positive OpenCode-Tool-Metadaten plus echtes Modell-/Protokollgate und bekannter Kontext von mindestens 256k qualifizieren für den Workflow. Ungeeignete Bestandsloops pausieren nachvollziehbar vor der nächsten Entscheidung; kein `failed` oder Rückfall auf Vollhistorie. Diese Änderung betrifft ausschließlich den Plan. Neue Zahlen-/Timeoutprofile und echte Provider-Abnahmetests stehen weiterhin aus.

## 15. Timeout-Präzisierung ohne Codex-Regression

Die bisherigen 180 Sekunden sind keine allgemeine maximale Denkzeit: Codex verwendet einen fortschrittsabhängigen Inaktivitäts-Timeout (180 bzw. 300 Sekunden bei hoher Denktiefe), OpenCode heute dagegen einen nicht streamenden HTTP-Gesamttimeout pro Aufruf. Der Plan erhält Codex' bestehende Streaming-Überwachung und führt keine pauschale Gesamtgrenze um `chat()` ein.

Die vorgeschlagene Standard-Entscheidungsgrenze von 1.800 Sekunden ist gestrichen. Standard bleibt die verbleibende Loop-Zeit abzüglich Reserve; eine kürzere Entscheidungsgrenze ist optional und muss ausdrücklich vom Nutzer gesetzt sein. Für nicht streamende OpenCode-Aufrufe ist eine kürzere Aufrufgrenze ebenfalls optional; ohne sie bleibt die Entscheidungsdeadline bindend, statt ungeprüft jedes Modell nach 180 Sekunden zu unterbrechen. Beide Adapter haben denselben fachlichen Ablauf, aber unterschiedliche technische Fortschrittssignale. Diese Korrektur betrifft ausschließlich die Planung.

## 16. OpenCode-Hängerschutz: Streaming oder endliches Profil

Die zuletzt vorgeschlagene Option, nicht streamende OpenCode-Aufrufe ohne eigene Grenze bis zur Loop-Deadline warten zu lassen, ist verworfen. Der Adapter soll bevorzugt echtes SSE-Streaming mit verwertbarem Fortschritt und Inaktivitätsüberwachung für alle drei tatsächlich angebotenen Protokolle nutzen. Keepalives allein zählen nicht als Modellfortschritt; Teilantworten dürfen keine Entscheidung oder unvollständige Tool-Ausführung auslösen.

Ohne nachgewiesenen Streaming-Fortschritt ist eine endliche Standardgrenze pro Aufruf verpflichtend. Sie wird anhand des Gates und einer Sicherheitsmarge für die konkrete Modell-/Protokoll-/Denktiefenkonfiguration festgelegt und beim Start sichtbar; der bisherige Wert 180 Sekunden wird nicht ungeprüft übernommen. Fehlt ein gültiges Schutzprofil, startet keine neue Anfrage. Hänger folgen dem begrenzten technischen Retry innerhalb aller Budgets. Die optionale Gesamt-Entscheidungsgrenze bleibt optional; Codex' gesundes Streaming wird weiterhin nicht mit einer neuen pauschalen Gesamtdauer begrenzt. Nur Dokumentation geändert.
