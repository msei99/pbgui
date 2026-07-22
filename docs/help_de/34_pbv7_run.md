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
| **P** | Globalen Panic Forced Mode für Long und Short setzen, Config speichern und nach Sicherheitsabfrage synchronisieren |
| **G** | Globalen Graceful Stop Forced Mode für Long und Short setzen, Config speichern und nach Sicherheitsabfrage synchronisieren |
| **T** | Globalen Take Profit Only Forced Mode für Long und Short setzen, Config speichern und nach Sicherheitsabfrage synchronisieren |
| **Edit** | Instanz im Bearbeitungsformular öffnen |
| **V8** | Genau diese V7-Run-Config mit PB8s offiziellem Migrator konvertieren und die neue Config in PBv8 Backtest öffnen |
| **User** | Der dieser Instanz zugewiesene API-Key-Benutzer |
| **Enabled On** | VPS, auf dem der Bot läuft (`disabled` = nicht aktiviert) |
| **TWE** | Total Wallet Exposure — `L=` Long / `S=` Short |
| **Version** | Lokal gespeicherte Config-Version |
| **Remote** | Live-Status vom VPS (siehe Status-Icons unten) |
| **Remote Version** | Derzeit auf dem VPS laufende Config-Version |
| **Note** | Freitext-Notiz für eigene Zwecke |
| **Delete** | Instanz löschen (nicht möglich während sie läuft) |

Die Zeilenbuttons `P`, `G` und `T` schreiben PB7 `live.forced_mode_long` und `live.forced_mode_short` in `config.json`, erhöhen die Config-Version, erstellen ein Backup der vorherigen Config und synchronisieren die Änderung zum Ziel-Host. Es sind Passivbot-Forced-Mode-Aktionen, keine direkten Exchange-Orders.

**V8** lässt die V7-Run-Config unverändert. PBGui entfernt vor dem Aufruf von PB8 ausschließlich eigene Metadaten und einen veralteten temporären Loader-Pfad. Meldet PB8 nicht unterstützte oder manuell zu prüfende Strategie-Felder, stoppt die Konvertierung und zeigt diese Felder an, statt eine lauffähige V8-Config zu veröffentlichen.

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
| 📊 Backtest | Diese Instanz-Config direkt als Draft auf der FastAPI-Backtest-Seite öffnen |
| 🔍 Strategy Explorer | Strategy Explorer mit dieser Config vorladen |
| 💰 Balance Calculator | Eigenständigen Balance Calculator für diese Instanz öffnen |
| ⚡ Calc Balance | Empfohlene Balance direkt berechnen (wird als Popup angezeigt) |
| 📖 Guide | Diesen Guide öffnen |

Wichtige Einstellungen im Bearbeitungsformular:

| Bereich | Beschreibung |
|---------|------|
| **User** | API-Key-Benutzer (Exchange-Konto) auswählen |
| **Enabled On** | Ziel-VPS für den Einsatz. Der Selektor zeigt nur Hostnamen; technische Credential-Diagnosen werden erst bei der Validierung einer betroffenen Aktion gemeldet |
| **Note** | Optionales Label, das in der Liste angezeigt wird |
| **Logging level** | Passivbot-Selektor für die Log-Verbosity mit `warning`, `info`, `debug` und `trace` |
| **Long / Short** | Bot-Parameter — Positionen, TWE, Entry/Close-Bereiche |
| **JSON-Editoren** | Raw JSON, Long JSON, Short JSON, Import JSON und JSON-basierte Additional Parameters werden beim Tippen validiert; ungültiges JSON zeigt die genaue Zeile/Spalte und blockiert Speichern bis der Fehler behoben ist. Ältere in Run geladene Configs, einschließlich gepasteter Importe und Backtest→Run-Drafts, behalten außerdem die `neutralized`- / `review`-Markierungen im Long/Short-JSON |
| **Filters** | CoinMarketCap-basierter Symbol-Filter für diese Instanz |
| **Approved / Ignored coins** | Die Approved-Coin-Picker verwenden jetzt direkt Passivbots kanonisches `all`-Verhalten. Der alte Schalter `empty_means_all_approved` wird nicht mehr angezeigt und beim Speichern auch nicht mehr zurückgeschrieben |
| **Coin Overrides** | Coin-spezifische Parameterüberschreibungen (Bot-Parameter, Live-Modus, separate Config-Dateien) |
| **Dynamic Ignore** | Vorschau der automatisch ignorierten Symbole basierend auf den Filter-Einstellungen |

### Dynamic Ignore und der CMC-Pool

Dynamic Ignore ist eine Capability des Ziel-Hosts und keine Key-Einstellung pro Instanz oder VPS. Vor Save, Sync oder Start prüft PBGui secret-freie Host-Metadaten auf Credential Protocol v2, einen aktiven lokalen CMC-Pool und passende Katalog-/Materialisierungs-Generationen. Meldet das Ziel keinen aktiven Pool oder ist sein Status noch unbekannt, wird die Aktion mit dem gemeldeten Grund blockiert. Zuerst den Cluster-CMC-Pool auf diesem Host materialisieren. Deaktivierte Instanzen benötigen keine Pool-Readiness.

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
