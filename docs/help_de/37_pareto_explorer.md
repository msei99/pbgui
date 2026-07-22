# Pareto Explorer (PBv7 / PBv8)

Der Pareto Explorer hilft dir, PBv7- und PBv8-Optimierungsergebnisse zu analysieren, Trade-offs zu vergleichen, Configs zu selektieren und Folge-Optimize-Presets zu erstellen. Er ist fuer Multi-Objective-Ergebnisse gedacht, bei denen keine einzelne Metrik die ganze Antwort liefert.

## Wo du ihn öffnest

- PBGui: **PBv7/PBv8 -> Optimize -> Results** oder ueber den passenden Navigationseintrag **Pareto Explorer**.
- Öffne ein Ergebnis mit **Pareto Explorer** aus der Optimize-Ergebnisliste oder der Ergebnis-Sidebar.
- Die Seite kann im schnellen Pareto-only-Modus starten und später über die Sidebar den kompletten `all_results.bin`-Datensatz laden.

## Grundidee

Jeder Score, jedes Chart und jeder Pareto-Stern ist relativ zur aktuell geladenen und sichtbaren Config-Menge.

- Fast Mode vergleicht hauptsächlich die Passivbot-Pareto-JSON-Kandidaten.
- Full Mode vergleicht gegen den größeren `all_results.bin`-Kandidatenraum.
- Display Range ändert den sichtbaren Slice, deshalb können sich Rankings und sichtbare Pareto-Sterne ändern.
- Nutze Rang und Score als Shortlisting-Signale, nicht als endgültige Live-Trading-Entscheidung.

## Overview

Overview ist das Entscheidungs-Dashboard. Nutze es zuerst nach dem Laden eines Ergebnisses.

- **Top Champions** zeigt die stärksten Kandidaten im aktuell sichtbaren Slice.
- **Insights** hebt offensichtliche Signale hervor, z. B. Parameter an Bounds oder Stil-Diversität.
- **Pareto Front Preview** zeigt die Form der aktuellen Trade-off-Frontier.
- **Robustness vs Performance** zeigt, ob Return durch Konsistenz gestützt wird.
- Die Details der ausgewählten Config erscheinen unter den Charts, sobald eine Config selektiert ist.

Empfohlener Ablauf:

1. Top Champions scannen.
2. Einen Champion oder Chart-Punkt anklicken.
3. Metrics, Trading Style, Robustness, Scenario Metrics und Full Configuration prüfen.
4. **Run Backtest** nutzen, bevor du einem Kandidaten vertraust.
5. **Create Optimize Preset from this Config** erst nutzen, wenn die Config wirklich nach Refinement aussieht. PBGui erhaelt dabei die PB7-/PB8-Generation des Ergebnisses.

Im PB8-Full-Mode rekonstruiert PBGui inkrementell komprimierte `all_results.bin`-Eintraege vor der Analyse. Verschachtelte PB8-Bot-Parameter und Bounds erscheinen als punktierte Pfade und werden fuer Presets und Backtest-Handoffs wieder in kanonische verschachtelte Config-Objekte umgewandelt.

## Explorer

Explorer ist für interaktive Trade-off-Analyse gedacht.

- **Visualization** wechselt zwischen 2D Scatter, 3D Scatter, 3D Projections und Radar Charts.
- **Quick Views** wählen sinnvolle Metrik-Kombinationen für typische Entscheidungen.
- **Custom** erlaubt eigene X-, Y- und optional Z-Metriken.
- **Color by** ergänzt eine weitere Metrik-Dimension über Punktfarbe.
- **Show all configs** vergleicht den Kandidaten gegen die komplette sichtbare Punktwolke statt nur gegen Pareto-Punkte.
- **Performance Priority**, **Risk Aversion** und **Robustness Importance** steuern den Best-Match-Helfer.

Nutze Explorer für Fragen wie:

- Liegt diese Config wirklich auf einer guten Frontier oder ist sie nur in einer Metrik stark?
- Welche nahe Alternative verliert kaum Profit, reduziert aber deutlich Risiko?
- Ist ein Radar-Kandidat ausgewogen oder kaschiert eine extreme Stärke mehrere Schwächen?

## Deep Intelligence Parameters

Parameters Intelligence erklärt, wie sich die Optimize-Suche um Parameterwerte verhalten hat.

- **Parameter Influence Heatmap** zeigt Korrelationen zwischen variablen Parametern und Performance-Metriken.
- **Parameters Near Bounds** zeigt Parameter nahe an ihren Optimize-Bounds.
- **Top N Parameters** steuert, wie viele Parameter angezeigt werden.

Nutze diesen Tab vor einem Folge-Preset. Parameter nahe an Bounds sind gute Refinement-Kandidaten, weil der Optimizer eventuell weiter in diese Richtung suchen wollte.

## Deep Intelligence Scenarios

Scenario Analysis vergleicht die sichtbare Config-Menge über geladene Backtest-Szenarien.

- Der Metric-Selector bestimmt den Wert für Boxplots und Statistik-Karten.
- Chart und Statistik-Karten sind aggregierte Ansichten über die sichtbare Config-Menge.
- Dieser Tab beschreibt keine einzelne selektierte Config, sondern das Verhalten der sichtbaren Population über Szenarien.

Nutze ihn, um keine Config zu wählen, die nur in einem engen Szenario gut aussieht.

## Deep Intelligence Evolution

Optimization Evolution zeigt, ob der Optimize-Lauf im Zeitverlauf noch sinnvoll bessere Configs gefunden hat. Dafür ist Full Mode nötig, weil die schnellen Pareto-JSON-Dateien den ursprünglichen `all_results.bin`-Config-Index nicht erhalten.

- **Metric** wählt den Timeline-Wert.
- **Show all configs** wechselt zwischen Pareto-only-Punkten und allen sichtbaren Configs.
- **Hide liquidation outliers** verhindert, dass Extremwerte die Chart-Skala zerstören.
- **Meaningful Improvement (%)** ignoriert winzige Best-so-far-Änderungen, damit Rauschen nicht wie Fortschritt wirkt.
- Die blaue **Best So Far**-Linie zeigt den besten Wert, der bis zum jeweiligen Punkt gefunden wurde.
- Ein Klick auf einen Punkt selektiert diese Config im Full Mode zur Prüfung unter dem Chart.

Im Fast Mode zeigt dieser Tab nur einen Hinweis statt eines Charts. Nutze den Sidebar-Button **Load all_results**, wenn du die echte Timeline brauchst.

Nutze die Zusammenfassung, um einzuschätzen, ob ein weiterer Lauf wahrscheinlich hilft:

- **Last meaningful improvement** nahe am Ende spricht dafür, dass die Suche noch produktiv sein kann.
- **Final 20% improvement** nahe null spricht dafür, dass der Lauf bereits abgeflacht ist.
- **Suggested minimum iterations** liefert ein praktisches Ziel für den nächsten Lauf, basierend darauf, wo die letzte sinnvolle Verbesserung lag.

## Deep Intelligence Correlations

Multi-Metric Correlation vergleicht mehrere Configs über normalisierte Risiko-/Profil-Dimensionen.

- **Selection Strategy** bestimmt die Auswahl: Top Performers, Diverse Styles oder Risk Spectrum.
- **Configs** steuert, wie viele Radar-Traces angezeigt werden.
- Weighted- und BTC-Toggles wählen bevorzugte Metrik-Varianten, wenn sie verfügbar sind.

Nutze es, um Kandidatenprofile schnell zu vergleichen. Ein ausgewogenes Radar ist meist leichter zu validieren als eine Config, die eine Achse gewinnt und mehrere andere verliert.

## Settings and Loading

Settings steuert, welche Daten geladen werden.

- **Result Path** ist das Optimize-Ergebnisverzeichnis oder Pareto-Verzeichnis.
- **Max Configs** limitiert den schnell geladenen Ausschnitt.
- **Load Strategy** bestimmt, wie Kandidaten beim Laden eines Subsets ausgewählt werden.
- **Persist defaults** speichert die aktuellen Lade-Voreinstellungen.
- Nutze den Sidebar-Button **Load all_results** für Full Mode.
- Nutze **Show Passivbot Paretos**, um zurück in den schnellen Pareto-only-Modus zu wechseln.

Wenn die UI träge wirkt, reduziere Max Configs oder arbeite im Pareto-only-Modus, bis klar ist, welcher Bereich des Ergebnisses eine tiefere Analyse lohnt.

## Optimize Preset Refinement

Das Preset-Panel erstellt eine PBv7-Optimize-Config aus der selektierten Pareto-Config.

- Wähle zuerst **Optimization goal**. Die Standardoption Balanced behält das Run-Scoring bei.
- Den generierten **Preset name** kannst du meistens unverändert lassen.
- Lasse **Only adjust parameters near optimize bounds** für normale Refinement-Runs aktiviert.
- Nutze **Bounds window (%)**, um Search-Bounds um selektierte Werte zu verengen.
- Nutze **Risk adjustment**, um risikorelevante Bounds und Limits enger oder lockerer zu setzen.
- **Create Optimize Preset** speichert die Config und öffnet Optimize.
- **Create & Queue** speichert und queued sie ohne Optimize zu öffnen.

Starte mit einem kleinen Bounds Window. Ein enges Window ist gut für Refinement, aber zu starkes Einengen kann bessere nahe Bereiche ausblenden.

## Best Practices

1. Starte in Overview, nicht in Deep Intelligence. Finde zuerst Kandidaten, die Analyse wert sind.
2. Lade `all_results.bin`, bevor du finale Entscheidungen triffst, falls die Datei verfügbar ist.
3. Nutze Display Range bewusst. Eine Config, die in den Top 500 stark ist, kann in den Top 5000 gewöhnlich wirken.
4. Bevorzuge ausgewogene Kandidaten mit akzeptablem Risiko gegenüber dem absoluten Profit-Maximum.
5. Validiere selektierte Configs immer im Backtest, bevor du sie als Live-Kandidaten betrachtest.
6. Nutze Deep Intelligence Parameters, bevor du ein Folge-Optimize-Preset erzeugst.
7. Bei Refinement-Presets zuerst near-bound Parameter anpassen und Bounds-Änderungen moderat halten.
8. Vergleiche mindestens zwei nahe Alternativen. Der beste Live-Kandidat ist oft nicht der höchste Ranking-Punkt.

## Verwandt

- PBv7 Optimize: Folge-Optimize-Runs erstellen und queuen.
- PBv7 Backtest: eine selektierte Config validieren, bevor du ihr vertraust.
- Strategy Explorer: eine einzelne Config visuell prüfen, nachdem die Shortlist eingegrenzt ist.
