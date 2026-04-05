# PBv7 Run

Die **PBv7 Run** Seite verwaltet deine live handelnden Passivbot v7 Instanzen.
Jede Instanz verbindet einen API-Key-Benutzer, eine Bot-Konfiguration und einen Ziel-VPS.

---

## Instanzliste

Zeigt alle konfigurierten V7 Instanzen in einer Tabelle.

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| `:recycle:` | Alle Instanzen und Remote-Status neu laden |
| **Add** | Neue leere Instanz erstellen |
| **Activate ALL** | Aktivierung für alle Instanzen auf einmal anstoßen |

Tabellenspalten:

| Spalte | Beschreibung |
|--------|-------------|
| **Edit** | Instanz im Bearbeitungsformular öffnen |
| **User** | Der dieser Instanz zugewiesene API-Key-Benutzer |
| **Enabled On** | VPS, auf dem der Bot läuft (`disabled` = nicht aktiviert) |
| **TWE** | Total Wallet Exposure — `L=` Long / `S=` Short |
| **Version** | Lokal gespeicherte Config-Version |
| **Remote** | Live-Status vom VPS (siehe Status-Icons unten) |
| **Remote Version** | Derzeit auf dem VPS laufende Config-Version |
| **Note** | Freitext-Notiz für eigene Zwecke |
| **Delete** | Instanz löschen (nicht möglich während sie läuft) |

**Remote-Status-Icons:**

| Icon | Bedeutung |
|------|-----------|
| ✅ Running … | Bot läuft auf dem erwarteten VPS mit der aktuellen Config-Version |
| 🔄 Running … | Bot läuft, aber Config-Version weicht ab (Aktivierung erforderlich) |
| 🔄 Activation required | Instanz ist einem VPS zugewiesen, aber noch nicht aktiviert |
| ❌ | Instanz ist deaktiviert |

---

## Bearbeitungsformular

Öffnet sich beim Klick auf **Edit** in einer Zeile oder nach **Add**.

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| 🏠 Home | Zurück zur Instanzliste |
| 💾 Save | Änderungen speichern und Config zum VPS synchronisieren |
| 📥 Import | Bestehende Passivbot-Config-Datei importieren |
| 📊 Backtest | Config dieser Instanz auf der Backtest-Seite öffnen |
| 🔍 Strategy Explorer | Strategy Explorer mit dieser Config vorladen |
| 💰 Balance Calculator | Eigenständigen Balance Calculator für diese Instanz öffnen |
| ⚡ Calc Balance | Empfohlene Balance direkt berechnen (wird als Popup angezeigt) |
| 📖 Guide | Diesen Guide öffnen |

Wichtige Einstellungen im Bearbeitungsformular:

| Bereich | Beschreibung |
|---------|------|
| **User** | API-Key-Benutzer (Exchange-Konto) auswählen |
| **Enabled On** | Ziel-VPS für den Einsatz |
| **Note** | Optionales Label, das in der Liste angezeigt wird |
| **Long / Short** | Bot-Parameter — Positionen, TWE, Entry/Close-Bereiche |
| **Filters** | CoinMarketCap-basierter Symbol-Filter für diese Instanz |
| **Coin Overrides** | Coin-spezifische Parameterüberschreibungen (Bot-Parameter, Live-Modus, separate Config-Dateien) |
| **Dynamic Ignore** | Vorschau der automatisch ignorierten Symbole basierend auf den Filter-Einstellungen |

---

## Typische Arbeitsabläufe

### Neue Live-Instanz starten
1. **Add** → **User** und **Enabled On** (Ziel-VPS) auswählen
2. **Long / Short** Parameter und Coin-Filter konfigurieren → **💾 Save**
3. Status-Spalte zeigt 🔄, bis der VPS die Aktivierung bestätigt

### Laufenden Bot aktualisieren
1. Instanz mit **Edit** öffnen → Parameter anpassen → **💾 Save**
2. Die Config wird automatisch zum VPS übertragen; Status zeigt 🔄, bis bestätigt

### Parameter vor dem Live-Betrieb validieren
1. Instanz mit **Edit** öffnen
2. **📊 Backtest** klicken → Backtest mit derselben Config starten
3. **🔍 Strategy Explorer** klicken → Pareto-Metriken interaktiv erkunden

### Benötigte Balance prüfen
1. Instanz mit **Edit** öffnen
2. **⚡ Calc Balance** klicken → empfohlene Balance für die aktuelle Config anzeigen
3. Oder **💰 Balance Calculator** für den vollständigen Rechner öffnen

### Bot deaktivieren
1. Instanz mit **Edit** öffnen → **Enabled On** auf `disabled` setzen → **💾 Save**
2. Der Bot wird automatisch auf dem VPS gestoppt
