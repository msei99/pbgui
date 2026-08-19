# PBv7 Run

Die **PBv7 Run** Seite verwaltet deine live handelnden Passivbot v7 Instanzen.
Jede Instanz verbindet einen API-Key-Benutzer, eine Bot-Konfiguration und einen Ziel-VPS.

---

## Instanzliste

Zeigt alle konfigurierten V7 Instanzen in einer Tabelle.

Sidebar-Aktionen:

| Schaltfläche | Aktion |
|--------|--------|
| **Search / Status** | Gemeinsame Run-Tabelle filtern, ohne Instanzzustände zu ändern |
| **Refresh** | Alle Instanzen und Remote-Status neu laden |
| **Add Instance** | Neue leere Instanz erstellen |
| **Backups** | V7-Config-Backups durchsuchen, filtern, laden oder löschen |

Tabellenspalten:

| Spalte | Beschreibung |
|--------|-------------|
| **Name** | Stabiler Instanzname |
| **User** | Der dieser Instanz zugewiesene API-Key-Benutzer |
| **Enabled On** | VPS, auf dem der Bot läuft (`disabled` = nicht aktiviert) |
| **Status** | Bestaetigter Synchronisations-/Runtime-Zustand; `collecting` bedeutet, dass noch keine exakte Beobachtung vorliegt |
| **Cfg Ver / Run Ver** | Lokal gespeicherte Config-Version und vom laufenden Prozess bestaetigte Version |
| **TWE** | Total Wallet Exposure — `L=` Long / `S=` Short |
| **Running On** | Hosts, die die exakte verwaltete Prozessidentitaet melden |
| **Desired** | Cluster-Sollzustand, falls die Runtime einen veroeffentlicht; andernfalls `-` fuer V7 |
| **Note** | Freitext-Notiz für eigene Zwecke |
| **Actions** | P/G/T Forced Modes, Edit, Balance Calculator, V8-Migration und Delete |

Die Zeilenbuttons `P`, `G` und `T` schreiben PB7 `live.forced_mode_long` und `live.forced_mode_short` in `config.json`, erhöhen die Config-Version, erstellen ein Backup der vorherigen Config und synchronisieren die Änderung zum Ziel-Host. Es sind Passivbot-Forced-Mode-Aktionen, keine direkten Exchange-Orders. Der Editor zeigt kanonische Werte wie `graceful_stop` über die passende PB7-Dropdown-Option an, auch wenn die gespeicherte Config die Langform statt des Kurz-Alias verwendet.

**V8** laesst die V7-Run-Config unveraendert und uebergibt die vollstaendige Strategy-, Backtest- und Optimize-Struktur an PB8s offiziellen Migrator. PBGui entfernt davor eigene Metadaten und den veralteten temporaeren Loader-Pfad, extrahiert retired Price-Distance-Namen und entfernt deaktivierte retired Volatility-Filter. Nach der V7-Shape-Konvertierung wird der extrahierte Wert an PB8s kanonische Config-Aufbereitung uebergeben: Ein positiver Wert wird zu `live.order_replacement_churn_gate_market_dist_pct`, ein deaktivierter Wert zu `live.order_replacement_churn_gate_activation_count = 0`; explizit widerspruechliche alte und neue Einstellungen lehnt PB8 ab. Keiner der retired Distance-Namen wird in den V8-Draft geschrieben oder zum manuellen Review angezeigt. Erfolgreiche und review-pflichtige Run-Migrationen bleiben im Run-Workflow und oeffnen als kurzlebige ungespeicherte PB8-Run-Editor-Drafts; sie werden niemals im Backtest-Configspeicher abgelegt. Das Run-Review zeigt nur verbleibende Run-relevante Befunde und keine `backtest.*`- oder `optimize.*`-Befunde. Ein dauerhafter Hinweis zeigt offene Felder und urspruengliche V7-Werte, ohne retired V7-Pfade in die V8-Config einzufuegen. Das bestehende V7-Review-Styling markiert betroffene kanonische Bot-Bereiche rot. Nicht pruefbarer oder ungueltiger Output stoppt weiterhin mit einer kompakten Fehlerliste.

**Statuswerte:**

| Icon | Bedeutung |
|------|-----------|
| **synced** | Bot laeuft auf dem erwarteten VPS mit der aktuellen Config-Version |
| **outdated** | Bot laeuft, aber die Config-Version weicht ab |
| **sync needed** | Instanz ist zugewiesen, aber die aktuelle Version wurde noch nicht laufend bestaetigt |
| **stop needed** | Trotz deaktivierter Instanz wird noch ein Prozess gemeldet |
| **collecting** | Es liegt noch keine exakte Prozessbeobachtung vor |
| **disabled** | Instanz ist deaktiviert und kein Prozess wird gemeldet |

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
| **Enabled On** | Ziel-VPS für den Einsatz. Der Selektor zeigt nur Hostnamen; ein bereits konfiguriertes Ziel bleibt sichtbar, wenn seine aktuelle Capability nicht bestätigt werden kann, während die Validierung unsichere Zielwechsel weiterhin blockiert |
| **Note** | Optionales Label, das in der Liste angezeigt wird |
| **Logging level** | Passivbot-Selektor für die Log-Verbosity mit `warning`, `info`, `debug` und `trace` |
| **Long / Short** | Bot-Parameter — Positionen, TWE, Entry/Close-Bereiche |
| **JSON-Editoren** | Raw JSON, Long JSON, Short JSON, Import JSON und JSON-basierte Additional Parameters werden beim Tippen validiert; ungültiges JSON zeigt die genaue Zeile/Spalte und blockiert Speichern bis der Fehler behoben ist. Ältere in Run geladene Configs, einschließlich gepasteter Importe und Backtest→Run-Drafts, behalten außerdem die `neutralized`- / `review`-Markierungen im Long/Short-JSON |

Das **User**-Feld im Import-Dialog ist durchsuchbar. Einen Teil des konfigurierten Exchange-User-Namens eingeben und den passenden Vorschlag waehlen; unbekannte freie Namen werden abgewiesen.
| **Filters** | CoinMarketCap-basierter Symbol-Filter für diese Instanz |
| **Approved / Ignored coins** | Die Approved-Coin-Picker verwenden jetzt direkt Passivbots kanonisches `all`-Verhalten. Der alte Schalter `empty_means_all_approved` wird nicht mehr angezeigt und beim Speichern auch nicht mehr zurückgeschrieben |
| **Coin Overrides** | Coin-spezifische Parameterüberschreibungen (Bot-Parameter, Live-Modus, separate Config-Dateien). Erlaubte Inline-Parameter werden aus der installierten PB7-Runtime geladen; ein bereits geöffneter Editor aktualisiert sich nach Eintreffen der Metadaten und zeigt bei einem Ladefehler eine klare Meldung statt leerer Bereiche |
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
3. **🔍 Strategy Explorer** klicken → Entry-/Close-Orders pruefen, Parameteraenderungen testen, begrenzte Simulationen ausfuehren, Fills vergleichen und einen Replay-Movie bauen

### Benötigte Balance prüfen
1. Instanz mit **Edit** öffnen
2. **⚡ Calc Balance** klicken → empfohlene Balance für die aktuelle Config anzeigen
3. Oder **💰 Balance Calculator** für den vollständigen Rechner öffnen

### Bot deaktivieren
1. Instanz mit **Edit** öffnen → **Enabled On** auf `disabled` setzen → **💾 Save**
2. Der Bot wird automatisch auf dem VPS gestoppt
