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

### 4) Kandidaten exportieren
Sobald du eine Shortlist hast:

- Config-JSONs exportieren und in Backtests testen
- Notieren, welche Filter/Metriken die besten Kandidaten geliefert haben

## Tipps

- Wenn die UI träge ist, Anzahl der angezeigten Konfigurationen/Filter reduzieren.
- Für faire Vergleiche zwischen mehreren Runs immer konsistente Zeiträume und Exchanges nutzen.

## Verwandt

- Strategy Explorer: ideal für visuelles Debugging einer einzelnen Konfiguration.
