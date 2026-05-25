# Pareto Explorer (PBv7)

Der Pareto Explorer ist eine interaktive UI zur Analyse von PB7-Optimierungsergebnissen (Multi-Objective-Suche). Damit findest du robuste Konfigurationen, vergleichst Trade-offs und exportierst Kandidaten.

## Wo du ihn öffnest

- PBGui: **PBv7 → Optimize → Results**
- Auf einem Optimierungsergebnis auf **🎯 Pareto Explorer** klicken.

## Was benötigt wird

Ein PB7-Optimierungsergebnis-Ordner, typischerweise mit:

- `pareto_front.json` (oder ähnliche Pareto-JSON-Dateien)
- `all_results.bin` (optional, aber empfohlen für vollständige Exploration)

Wenn `all_results.bin` fehlt, läuft der Pareto Explorer im **Fast Mode** mit eingeschränkten Ansichten.

## So nutzt du ihn

### 1) Im Fast Mode starten
Der Fast Mode lädt zuerst nur Pareto-JSONs, damit die UI schnell öffnet.

- Gut, um früh vielversprechende Konfigurationen zu erkennen.
- Wenn du den kompletten Kandidatenraum brauchst, Full Load aktivieren.

### 2) Alle Ergebnisse laden (Full Mode)
Im Full Mode wird `all_results.bin` geladen.

- Mehr verfügbare Konfigurationen
- Zuverlässigeres Filtern und Selektieren
- Je nach Dateigröße und Disk-Geschwindigkeit langsamer

### 3) Trade-offs analysieren
Typische Workflows:

- Konfigurationen mit bestem **Profit-vs-Drawdown**-Kompromiss finden
- Nach **Stuck Time**, **Exposure** oder anderen Sicherheitsmetriken filtern
- Eine kleine Gruppe Top-Kandidaten direkt vergleichen

## Overview-Seite

Die erste Seite im Pareto Explorer ist **Overview**. Sie ist als schnelle Entscheidungsansicht gedacht:

- **Top Champions** zeigt die aktuell 5 besten Kandidaten für den geladenen sichtbaren Slice.
- **Insights** hebt offensichtliche Signale hervor, z. B. Parameter an Bounds oder Stil-Diversität.
- **Pareto Front Preview** hilft dir, die Form der aktuellen Kandidatenmenge visuell zu verstehen, bevor du tiefer einsteigst.

Der normale Ablauf für die meisten Runs ist:

1. Zuerst die Top-Champions-Liste scannen.
2. Einen Champion oder einen Punkt im Chart anklicken.
3. Unten die Details der selektierten Config lesen.
4. Wenn sie gut aussieht, die selektierte Config über die Sidebar im Backtest öffnen.

## So liest du Top Champions und Scores

Top Champions ist eine **Ranking-Hilfe**, aber keine Garantie, dass Rang 1 automatisch die beste Live-Config für dein Ziel ist.

- **Score** ist der kombinierte Ranking-Score des Explorers für den aktuell sichtbaren Slice.
- Ein höherer **Score** ist innerhalb derselben geladenen Ansicht besser.
- Der Score eignet sich am besten zum Shortlisting, nicht als einzige Wahrheit.

Die unterstützenden Chips zeigen dir, *warum* eine Config dort rankt:

- **Perf**: Stärke bei Return/Performance. Höher ist meist besser.
- **Rob**: Robustheit/Konsistenz. Höher ist meist besser.
- **Risk**: Risikodruck aus Drawdown-/Choppiness-/Tail-Risk-artigen Metriken. Niedriger ist meist besser.

Praktische Regel:

- Starte mit den Configs mit dem höchsten **Score**.
- Bevorzuge Configs, bei denen **Perf** und **Rob** beide stark sind.
- Sei vorsichtig, wenn eine Config nur wegen Performance gewinnt, aber bei **Risk** deutlich schlechter ist als nahe Alternativen.

## So liest du die Preview-Charts

Die beiden Charts auf der Overview-Seite sind schnelle visuelle Zusammenfassungen der aktuell sichtbaren Config-Menge.

### Pareto Front Preview (links)

Dieser Chart zeigt zwei zentrale Metriken der aktuell sichtbaren Configs gegeneinander.

- Jeder Punkt ist eine Config.
- Stern-markierte Punkte sind Pareto-Mitglieder im aktuell sichtbaren Slice.
- Der hervorgehobene Stern markiert die aktuell selektierte Config.
- Die Farbleiste zeigt eine zusätzliche Metrik, damit du auf einen Blick noch eine weitere Dimension siehst.

So interpretierst du ihn:

- Achte auf die äußere Kante/Frontier der Punkte statt auf dichte Cluster in der Mitte.
- Eine Config auf dieser Frontier ist interessant, weil eine Verbesserung auf einer Achse meist eine Verschlechterung auf der anderen bedeutet.
- Nahe beieinander liegende Frontier-Punkte sind oft die eigentlichen Entscheidungskandidaten.
- Wenn eine Config auf einer Achse nur minimal besser ist, auf der anderen aber klar schlechter, ist sie oft nicht der bessere Trade-off.

### Robustness vs Performance (rechts)

Dieser Chart beantwortet eine einfachere Frage: Wie viel Performance bekommst du für den Robustheitsgrad?

- **X-Achse**: Performance-Metrik. Weiter rechts ist besser.
- **Y-Achse**: Robustheits-Score. Höher ist besser.
- Die gestrichelten Linien markieren den aktuellen Durchschnittssplit.

Die Quadranten kannst du so lesen:

- **Oben rechts**: starke Performance und starke Robustheit. Meist der beste Suchbereich.
- **Oben links**: stabil, aber langsamer. Gut, wenn Sicherheit wichtiger als maximale Rendite ist.
- **Unten rechts**: schnell, aber fragil. Diese Configs brauchen mehr Vorsicht.
- **Unten links**: meist schwächere Kandidaten, außer für sehr spezielle Zwecke.

## Wichtiger Hinweis zu Scores

Scores und Chart-Positionen sind immer relativ zur **aktuell geladenen und aktuell sichtbaren** Menge.

- Im **Fast Mode** vergleichst du vor allem Pareto-JSON-Kandidaten.
- Im **Full Mode** vergleichst du gegen das größere `all_results.bin`-Sample.
- Wenn du den **Display Range** änderst, können sich Rankings und sichtbare Pareto-Sterne ändern, weil sich der Vergleichs-Slice geändert hat.

Die richtige Frage ist also nicht nur: "Welche Config hat den besten Score?", sondern auch:

- "Sieht diese Config immer noch stark aus, wenn ich sie gegen die breitere geladene Menge vergleiche?"
- "Ist sie ausgewogen stark oder gewinnt sie nur, weil eine einzige Metrik dominiert?"

### 4) Kandidaten exportieren
Sobald du eine Shortlist hast:

- Config-JSONs exportieren und in Backtests testen
- Notieren, welche Filter/Metriken die besten Kandidaten geliefert haben
- Mit **Run Backtest** auf einer ausgewählten Config öffnest du genau diese Config direkt im FastAPI-Backtest-Editor.

## Tipps

- Wenn die UI träge ist, Anzahl der angezeigten Konfigurationen/Filter reduzieren.
- Für faire Vergleiche zwischen mehreren Runs immer konsistente Zeiträume und Exchanges nutzen.

## Verwandt

- Strategy Explorer: ideal für visuelles Debugging einer einzelnen Konfiguration.
