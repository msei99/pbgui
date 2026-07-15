# PBGui – Gemeinsame TODO / Roadmap

Stand: 2026-07-13
Ziel: Nur offene Produktarbeit festhalten. Bereits vorhandene Grundlagen werden als Ist-Stand genannt und nicht erneut geplant.

## Prioritäten
- **P0 (kritisch):** Stabilität, Sicherheit, Betriebsfähigkeit
- **P1 (hoch):** Produktivität, Monitoring, Datenqualität
- **P2 (mittel):** Komfort, Konsistenz, kontinuierliche Verbesserung

---

## P0 – Sicherheit & Stabilität

### CMC API Pool mit globaler Usage-Steuerung
**Abgeschlossen**
- Der clusterweite Pool, transitive HPKE-Verteilung, Authority-Epochs,
  Best-effort-Leases, lokales Failover und Usage-Warnungen sind umgesetzt.
- Master und VPS materialisieren CMC-Secrets nur für ihre aktuelle signierte
  Recipient-Generation; VPS-Relays können Master-only TradFi-Secrets nicht
  entschlüsseln.
- Die Migration ist als Zero-Order Rolling Upgrade umgesetzt: lokale Shadows
  halten alte und neue Consumer in beliebiger Update-/Restart-Reihenfolge aktiv,
  der globale Freeze startet erst nach All-Active-v2 und der letzte v2-Sync
  setzt den Cutover automatisch fort.
- Migration, beliebige Update-Reihenfolgen, lange Pausen, Neustarts,
  gleichzeitige Requests, Key-Rotation, erschöpfte Keys, Recipient-Rewrap und
  Failover sind offline getestet.

---

## P2 – Qualität & Dokumentation

### Guide-Abdeckung und Pflegeprozess abschließen
**Ist-Stand**
- Shared Help Overlay, automatische Topic-Erkennung und nahezu vollständige EN/DE-Guides sind vorhanden.

**Offen**
- Einen dedizierten DB-Tools-Guide in EN und DE ergänzen.
- Produktive Seiten/Routes verbindlich auf eigene oder gemeinsame Guide-Topics abbilden.
- Strategy Explorer an die vorhandenen vollständigen zweisprachigen Shared-Help-Themen anbinden.
- EN/DE-Parität und Page-to-Topic-Mapping automatisiert testen.
- In `AGENTS.md` Guide-Review bei produktiven UI-Änderungen verpflichtend machen.

**Done wenn**
- Jede produktive Seite besitzt ein definiertes aktuelles Help-Topic und die Zuordnung wird automatisch geprüft.

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
