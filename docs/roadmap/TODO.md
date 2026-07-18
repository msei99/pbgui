# PBGui – Gemeinsame TODO / Roadmap

Stand: 2026-07-17
Ziel: Nur offene Produktarbeit festhalten. Bereits vorhandene Grundlagen werden als Ist-Stand genannt und nicht erneut geplant.

## Prioritäten
- **P0 (kritisch):** Stabilität, Sicherheit, Betriebsfähigkeit
- **P1 (hoch):** Produktivität, Monitoring, Datenqualität
- **P2 (mittel):** Komfort, Konsistenz, kontinuierliche Verbesserung

---

## P2 – Qualität & Dokumentation

### Flexibles Dashboard-Widget-Layout
**Ist-Stand**
- Widgets lassen sich in festen Slots tauschen und unabhängig vertikal vergrößern.
- Das Datenmodell bleibt auf ein oder zwei Spalten und `dashboard_type_R_C` begrenzt.

**Offen**
- Pro Widget `{type, x, y, w, h}` unterstützen, inklusive horizontalem/vertikalem Resize, freier Platzierung und Collision/Auto-Packing.
- Bestehende 1-/2-Spalten-Konfigurationen verlustfrei migrieren.
- Editor und View-Only-Modus sowie Layout-Migration testen.
- Vor Umsetzung zwischen Erweiterung der eigenen Engine und lokal eingebundenem Gridstack entscheiden; keine CDN-Abhängigkeit.

**Done wenn**
- Widgets sind frei verschiebbar und resizebar, alte Layouts werden automatisch migriert und beide Modi funktionieren konsistent.

---

## Arbeitsmodus
- Neue Ideen mit **Ist-Stand**, **Offen** und **Done wenn** erfassen.
- Erst priorisieren, dann in kleine testbare Teilaufgaben schneiden.
- Erledigte oder durch eine neue Architektur ersetzte Punkte beim Release aus dieser Datei entfernen.
