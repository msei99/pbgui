# Profit Sweep

## Zweck

Profit Sweep verschiebt einen konfigurierbaren Anteil des realisierten Trading-Gewinns vom Trading-Account zum festen internen Ziel der Exchange. Die Berechnung verwendet kumulierten realisierten Netto-PnL, Funding, Fees, Exchange-Korrekturen und eine High-Water Mark. Auszahlungen und interne Transfers gelten nicht als Trading-Verlust. Ein Verlust muss aufgeholt werden, bevor neuer Gewinn berechtigt ist.

Jeder Exchange-User besitzt eine unabhaengige Policy mit dem Status **Disabled**, **Dry**, **Live** oder **Paused Unknown**. Profit Sweep akzeptiert keine externe Adresse und fuehrt keine On-chain-Auszahlung aus.

## Einrichtung Und Berechtigungen

Den Exchange-User zuerst unter **System > API Keys** einrichten und danach **System > Profit Sweep** oeffnen. Nur die kleinste benoetigte Berechtigung fuer Account-Reads und interne Transfers vergeben:

- **Hyperliquid Standard/Manual** benoetigt den konfigurierten und freigegebenen API Agent. Der Account muss im Standard/Manual-Modus bleiben.
- **Bybit** benoetigt Read/Write und die Wallet-Berechtigung `AccountTransfer`. Eine Withdrawal-Berechtigung ist nicht erforderlich.
- **Binance** benoetigt Read und **Permits Universal Transfer**. Eine Withdrawal-Berechtigung ist nicht erforderlich.
- **Bitget** benoetigt Read, Transfer und die API-Passphrase. UTA-Transfers werden ohne Borrowing gesendet.
- **Hyperliquid Vault** verwendet den konfigurierten API Agent des Leaders. Der Vault selbst besitzt keinen Private Key.

Die Overview zeigt die Read-Capability sofort. Die Write-Capability wird bei Live oder einem Test-Transfer mit einem frischen serverseitigen Snapshot geprueft. Eine angezeigte Route ueberstimmt keine fehlenden Credentials, einen falschen Account-Modus, veraltete History, Liabilities, Lockups oder Exchange-Limits.

Die Account-Sidebar zeigt fuer jede Policy den aktuellen Modus und Due-Betrag. Beim Accountwechsel erscheint ein eigener Ladezustand, bis gespeicherte Policy, Journal, Intents und Test-Transfers dieses Accounts geladen sind; PBGui zeigt Schema-Defaults nicht mehr kurz so an, als gehoerten sie zum ausgewaehlten Account. Nach erfolgreicher Live-Aktivierung aktualisiert PBGui das ausgewaehlte Sidebar-Badge sofort aus der verbindlichen Aktivierungsantwort und gleicht es danach mit der gespeicherten Policy ab.

Hyperliquid-Vault-Accounts zeigen unter **Exchange / Vault** die Aktion **Fund account**. Sie oeffnet die getrennte Transfers-Seite mit dem ausgewaehlten Vault fuer einen festen Top Up von Leader Main Perps zum Vault; manuelles Funding bleibt ausserhalb der Profit-Sweep-Abrechnung.

## Basic-Felder

- **Reference capital** ist das Trading-Kapital, das vor einem Sweep erhalten bleibt.
- **Baseline mode** startet die Abrechnung beim Aktivieren der Policy oder mit der verfuegbaren Lifetime-History.
- **Trigger percent** definiert die Gewinnschwelle relativ zum Referenzkapital.
- **Sweep percent** waehlt den Anteil jedes neuen High-Water-Mark-Gewinnanstiegs.
- **Minimum transfer amount** sammelt kleinere Due-Betraege und gilt fuer Dry- und Live-Entscheidungen.
- **Transfer rounding step** rundet den finalen Transferbetrag nach Reserven und Limits auf ein Settlement-Asset-Inkrement ab. `1` verwendet volle USDC/USDT, `0` deaktiviert die Rundung; jeder Nachkomma-Rest bleibt fuer einen spaeteren Sweep Due.
- **Safety reserve amount** behaelt zusammen mit dem gewaehlten Reserve-Modus transferierbares Guthaben im Quellkonto.

**Keep trading capital** setzt Trigger percent auf `0` und Sweep percent auf `100`. Das Preset aktiviert oder speichert die Policy nicht. High-Water Mark und Verlustausgleich bleiben aktiv.

Neue Policies starten **Disabled** mit **From Enable**, Trigger percent `0`, Sweep percent `50`, Minimum transfer amount `1`, Transfer rounding step `1` und Safety reserve amount `0`. Die Live-Baseline verwendet standardmaessig **Include Dry Period**. Hyperliquid-Vault-Defaults sind **Flat Only**, **Main Perps**, Vault minimum amount `1`, Retained leader equity `100` und Vault reserve amount `100`. Reference capital bleibt `0`, bis PBGui es aus dem Account-Snapshot initialisiert; das Settlement Asset bleibt Exchange-spezifisch. Bereits gespeicherte Policies behalten ihre Werte.

## Advanced-Felder

Policy-Limits enthalten feste, prozentuale oder Max-of-Both-Reserven, optionale Limits pro Transfer und UTC-Tag sowie ein separates Limit fuer den ersten Live-Catch-up. Schedule-Felder steuern Debounce, Quiet Period, Stabilization, normale und Vault-Cooldowns, Jitter, maximales History-Alter und maximales Preflight-Alter.

Vault Advanced steuert Withdrawal Mode, retained Leader Equity, Leader-Share Safety Buffer, Vault Reserve, den Umgang mit bedingten Kosten und die Activity Policy des Main-Ziels. Exchange-Precision, Mindestwerte, transferierbarer Bestand, Margin, Lockup, Liabilities, Borrowing und Exactly-once-Sicherheitsregeln bleiben serverseitig fest.

## Dry Und Live

**Enable Dry** startet geplante read-only Entscheidungen. Die Auswahl eines Accounts startet automatisch einen nicht commitenden Background-Account-Read: Er erzeugt keinen Intent, veraendert keine bestaetigten Summen, signiert keinen Request und bewegt kein Guthaben. Berechtigte Dry-Ergebnisse erscheinen als `WOULD TRANSFER` im Dry Decision Journal.

Der automatische Account-Read aktualisiert die oberen Statuskarten sowie unter Exchange / Vault die Saldo-Karten fuer Quelle, konfiguriertes internes Ziel und aktuell transferierbaren Betrag. Ein doppeltes Account-Preview-Panel gibt es nicht mehr. Vorhandene Werte bleiben waehrend eines Refreshs sichtbar; nur ein kleiner Freshness-Hinweis im Account-State-Header zeigt Laden, letzte Aktualisierung oder Retry an. Reads laufen bei Accountauswahl, nach relevanten Aktionen, alle 60 Sekunden bei sichtbarer Seite und wenn ein veralteter Tab wieder sichtbar wird. Fehler werden nur bei sichtbarer Seite nach 15 Sekunden erneut gelesen. Account-Reads verwenden denselben accountweiten Lock wie Echtgeldoperationen und koennen deshalb eine Submission oder Reconciliation nicht ueberlappen. Der Accountkatalog selbst wird einmal beim Oeffnen der Seite geladen und nach einem initialen transienten Fehler automatisch erneut versucht, damit er waehrend einer Echtgeldoperation nicht ersetzt wird. Positives **Sweep Due** wird als `pending` markiert; ein High-Water-Mark-Block erscheint als **Blocked until HWM recovery** mit einem getrennten Wert **Recovery needed**. **Next scheduled check** zeigt einen laufenden Countdown und die lokale Uhrzeit aus dem persistierten Next-Run-Zeitstempel des Schedulers; bei Disabled und Paused Unknown ist keine Pruefung geplant. Der Tab Exchange / Vault zeigt bereinigte offene Positionen aus demselben Snapshot mit Seite, Groesse, Positionswert, Entry, unrealisiertem PnL, Liquidationspreis und Leverage. Ein Hinweis erklaert, dass verschobenes Collateral das Wallet-Exposure-Sizing von Passivbot beeinflussen kann, ohne Positionen zu veraendern oder zu schliessen. Bei Vault-Accounts ist **Your Vault Equity** die aktuelle leader-eigene Equity, **Vault TVL** die gesamte Equity aller Depositors und **Your Share** der Leader-Anteil an dieser TVL. Bei Vaults wechselt eine Aenderung des Ziels die Anzeige zwischen Main Perps und Main Spot. Ein bestaetigt leeres Binance Funding Wallet wird als Null angezeigt; fehlgeschlagene oder nicht unterstuetzte Exchange-Saldo-Reads erscheinen als unavailable.

Bei einem Hyperliquid-Leader im Unified- oder Portfolio-Margin-Modus zeigt PBGui **Main Unified** aus dem gemeinsamen USDC-Spot-Clearing-Saldo. Hyperliquid bezeichnet separate Perp-`marginSummary`-Werte in diesen Modi als nicht aussagekraeftig; sie sind oft Null. Standard/Manual-Leader behalten getrennte Main-Perps- und Main-Spot-Salden.

Eine erfolgreiche Hyperliquid-Spot-Clearing-Antwort mit leerer Balance-Liste bedeutet null Spot-USDC und wird als `0 USDC` angezeigt. Nur eine fehlende oder ungueltige Balance-Antwort erscheint als unavailable.

Vor **Enable Live** die Live-Baseline waehlen:

- **Fresh** startet die Berechtigung beim Activation Snapshot und schliesst vorherige Dry-Berechtigung aus.
- **Include Dry Period** berechnet die Berechtigung ab der aktuellen Dry-Generation-Baseline neu.

Wenn **Include Dry Period** ausgewaehlt ist, die aktuelle **From Enable**-Generation aber noch keine Dry-Auswertung abgeschlossen hat, existiert keine Dry-Periode zum Einbeziehen. Live initialisiert die Baseline dann wie bei einem frischen Start aus dem Activation Snapshot, damit historischer Account-PnL nicht sofort Due wird. Eine ausdruecklich zurueckgesetzte Baseline bleibt verbindlich.

Der aktive Baseline-Modus wird getrennt von der ausgewaehlten Einstellung gespeichert. Solange noch kein Live-Transfer bestaetigt wurde, bei einer aktiven **Fresh**-Policy **Include Dry Period** waehlen und **Apply baseline to active Live** mit der ausdruecklichen Real-Funds-Bestaetigung verwenden. PBGui berechnet die Live-Baseline dann rueckwirkend aus der Dry-Periode und plant eine frische Live-Auswertung; vorheriger Dry-Gewinn kann dadurch sofort Due werden. Normales Speichern der Policy startet diese Neuberechnung niemals. Nach einem bestaetigten Live-Transfer oder waehrend eines offenen Intents wird die Aktion blockiert, damit keine Berechtigung doppelt entsteht.

Das optionale First-Live-Catch-up-Limit begrenzt nur den ersten Catch-up; der Rest bleibt Due. Live erfordert eine gemeinsame Bestaetigung, speichert die gewaehlten Werte und startet die serverseitige Preflight-Pruefung. Danach wertet Live aus, persistiert vor Exchange-I/O einen dauerhaften Intent, sendet hoechstens einmal und reconciliert das Ergebnis. **Disable** verhindert kuenftige geplante Submissions, ohne Transfer-History zu loeschen.

Nach einer akzeptierten Live-Submission fragt PBGui die Exchange-History bis zu zehnmal im Abstand von einer Sekunde ab, bevor ein weiterhin unsichtbarer Eintrag als **Unknown** eingestuft wird. Diese Versuche sind reine read-only Reconciliation; der Transferrequest selbst wird niemals wiederholt.

## Zeitplanung

**Hybrid** kombiniert Income-Hinweise von PBData oder neu lokal importierter DB-Sync-History mit einem periodischen Fallback. Ein Hinweis startet das Settlement Debounce; Quiet Period und Stabilization lassen Fills, Fees, Rebates und Funding eintreffen. **Interval** verwendet nur periodische Auswertungen. Jitter verteilt Accounts zeitlich, Cooldowns begrenzen erfolgreiche Transfers und Freshness-Limits lehnen alte oder unvollstaendige Daten ab.

Hinweise wecken nur den Scheduler. Jede commitete Entscheidung liest frische Exchange-Daten und bricht bei unvollstaendiger History oder unvollstaendigem finalen Snapshot fail-closed ab.

## Exchange-Routen

- **Hyperliquid Standard/Manual:** USDC, Perps zum eigenen Spot-Bestand des Users.
- **Bybit:** USDT oder USDC, Unified Trading Account zu Funding.
- **Binance:** USDT oder USDC, USD-M Futures zu Funding.
- **Bitget Classic:** USDT, USDT Futures zu Spot.
- **Bitget UTA:** USDT, UTA zu Spot/Funding ohne Borrowing.
- **Hyperliquid Legacy Vault:** USDC, leader-eigene Vault-Equity zu Leader Main Perps, optional danach Main Perps zu Main Spot.

Der Server ermittelt den aktuellen Bitget-Modus und validiert jede feste Route gegen den ausgewaehlten Exchange-User. Routen fuehren nie zu einer anderen UID oder einem externen Ziel.

## Vaults Und Depositors

Die Vault-Abrechnung verwendet die eigene aktuelle Vault-Equity, den Anteil und die Cashflows des Leaders. Gesamt-PnL des Vaults wird nicht dem Leader zugerechnet. Einzahlungen, Auszahlungen und Gewinn anderer Depositors erzeugen keine Sweep-Berechtigung des Leaders. Zurechenbare Leader-Commission liegt bereits in Main Perps und ist in diesem Release deshalb nur diagnostisch; sie erzeugt niemals einen weiteren Withdrawal aus dem Vault.

Die Berechtigung beruecksichtigt ausserdem `maxWithdrawable`, gemeinsame Margin, retained Leader Equity, den verpflichtenden Leader-Anteil plus Safety Buffer, Lockup, Positionen und die konfigurierte Reserve. **Flat Only** erfordert keine Position mit Groesse ungleich null; ruhende Orders blockieren nicht, weil Hyperliquids withdrawable-Werte die verfuegbare Margin bereits beruecksichtigen. **Margin Buffered** erlaubt aktive Positionen nur innerhalb des konservativen withdrawable Caps. `alwaysCloseOnWithdraw` ist ein exchange-eigener Risikoschutz und kein PBGui-Transferverbot. Unklare Eigentumsdaten, ein geschlossener oder gesperrter Vault, inkonsistente Anteile oder verbotene Aktivitaet brechen fail-closed ab.

**Main Perps** endet nach der Vault-Auszahlung. **Main Spot** erzeugt einen zweiten dauerhaften Intent und leitet nur den reconcilierten Empfangsbetrag weiter. Closing Cost, Forced Reduction, Cancellation, fehlender Empfangsbetrag oder unerwartete Zielaktivitaet koennen weitere Sweeps pausieren.

## Fees Und Bedingte Kosten

Bybit dokumentiert die interne Route als gebuehrenfrei. Binance und Bitget liefern fuer diese internen Routen kein Transfer-Fee-Feld; PBGui erfasst deshalb keine direkte Fee, behandelt dies aber nicht als Exchange-Garantie. Hyperliquid Perps zu Spot verursacht fuer die eigene aktive Adresse normalerweise kein Gas, Trading und keine Slippage.

Eine Vault-Auszahlung kann `closingCost`, Trading-Fees oder Slippage verursachen, wenn margin-nutzende Positionen reduziert werden muessen. PBGui speichert reconciliierte Fee- und Cost-Felder und wendet die konfigurierte Conditional-Cost-Policy an. Main Perps vermeidet den optionalen zweiten Forwarding-Request.

## Test-Transfer Und Ruecktransfer

Unterstuetzte Standard- und Hyperliquid-Vault-Accounts zeigen **Test transfer** in **Exchange / Vault**. Die Funktion ist von Policy, Dry Journal, Sweep-Berechtigung und bestaetigten Live-Summen getrennt.

1. **Test transfer** anklicken, einen positiven Dezimalbetrag eingeben (Default `1` fuer Standard-Accounts, `5` fuer Vaults) und fortfahren.
2. Quelle, Ziel, Asset und den ausdruecklichen Hinweis pruefen, dass echtes Guthaben bewegt wird.
3. Bestaetigen, um genau eine persistierte Forward-Operation ueber die feste Route zu senden.
4. Den Status in der Tabelle Test Transfers pruefen.
5. Wenn die neueste Forward-Operation **Confirmed** und berechtigt ist, **Transfer back** anklicken und die feste Rueckroute bestaetigen.

Bei einem Hyperliquid Vault fuehrt die Forward-Route vom Vault zu Leader Main Perps. Diese Vault-to-Main-Route funktioniert auch mit Unified Account Mode des Leaders; nur das optionale Forwarding von Main Perps zu Spot erfordert Standard/Manual. Der ausdruecklich bestaetigte manuelle Test uebernimmt nicht die automatische **Flat Only**-Policy und erlaubt jeden positiven Test-Withdrawal innerhalb des frischen konservativen leader-eigenen Vault-Caps. Hyperliquids `alwaysCloseOnWithdraw` bleibt als exchange-eigener Risikoschutz aktiv, setzt PBGui's Cap aber nicht auf null. Der Default bleibt 5 USDC. **Transfer back** wird nur angeboten, wenn der reconciliierte Empfangsbetrag mindestens 5 USDC betraegt, weil Hyperliquid kleinere Vault-Deposits ablehnt.

Bei Standard-Accounts verwendet der Ruecktransfer den reconcilierten Empfangsbetrag, falls vorhanden, sonst den angeforderten Betrag. Ein Ruecktransfer sendet die Forward-Operation niemals erneut. **Unknown** bietet weder Retry noch Ruecktransfer; stattdessen Exchange und Logs pruefen und kein blindes Duplikat erzeugen.

Nach einer Forward- oder Ruecktransfer-Operation plant PBGui automatisch ein weiteres frisches read-only Preview. Schlaegt dieser Refresh fehl, bleibt der dauerhafte Operation-Status massgeblich und das Preview versucht den Read erneut, ohne den Transfer zu wiederholen.

Nachdem Hyperliquid eine manuelle Test-Submission akzeptiert hat, pollt PBGui die feste read-only Ledger-Abfrage bis zu zehn Sekunden, bevor das Ergebnis als Unknown eingestuft wird. Eine Ledger-Indexierungsverzoegerung erzeugt niemals eine weitere Submission; nur Reconciliation-Reads werden wiederholt.

Jede Forward-Test-Aktion traegt eine im Browser erzeugte Idempotency-UUID. PBGui beansprucht die persistierte Operation atomar vor dem Exchange-I/O. Parallele Requests oder ein exakt wiederholter Forward-Request nach einer verlorenen HTTP-Antwort liefern dadurch dieselbe Operation, ohne erneut zu senden. Transfer back ist an die bestaetigte Forward-Operation gebunden, erlaubt nur eine persistierte Rueckoperation und lehnt eine Wiederholung ab, statt erneut zu senden. Ein Test-Transfer im Status Submitting blockiert einen API-Neustart. Beim Start reconciliert PBGui unterbrochene bereits gesendete Tests ueber die Exchange-History und wiederholt niemals deren Write-Request.

Hyperliquid speichert erfolgreiche `agentSendAsset`-Bewegungen aktuell als Non-Funding-Ledger-Events mit `delta.type = "send"`. Die signierte Action enthaelt die kanonische Token-ID (`USDC:0x…`), waehrend das Ledger-Event nur das Symbol (`USDC`) meldet. PBGui prueft dieses Symbol sowie Ziel, DEX-Paar, Betrag, Nonce und Zeitfenster exakt, bevor die Operation bestaetigt wird.

Bei Spot-to-Perps-Rueckwegen ist das logische Descriptor-Ziel `default_perps`, waehrend signierte Action und Ledger-Event die eigene Wallet-Adresse als `destination` verwenden. Die Reconciliation vergleicht deshalb das Ziel der signierten Action, sodass Forward- und Reverse-Route dieselbe Provider-Identitaet nutzen.

PBGui sendet signierte Hyperliquid-Aktionen ueber einen festen versiegelten Endpoint und speichert nur einen begrenzten, um Adressen bereinigten Provider-Ablehnungsgrund. Signaturen und Request-Bodies werden niemals persistiert oder angezeigt. Aeltere fehlgeschlagene Vault-Testoperationen von vor dieser Diagnoseunterstuetzung koennen nur anzeigen, dass Hyperliquid die Aktion abgelehnt hat; fuer den exakten bereinigten Provider-Hinweis ist ein neuer ausdruecklich bestaetigter Test erforderlich.

Hyperliquid-L1-Submissions verwenden den aktuellen kanonischen Envelope nur mit `action`, `signature` und `nonce`, wenn kein optionaler Signaturkontext oder Ablauf gesetzt ist. Null-Felder fuer `vaultAddress` und `expiresAfter` werden exakt wie im offiziellen SDK weggelassen. Der Ziel-Vault bleibt innerhalb der signierten `vaultTransfer`-Aktion. PBGui zieht bei der Berechnung der Leader-Retention ein Micro-USDC ab, damit nach dem Withdrawal strikt mehr als 100 USDC und der konfigurierte Share-Floor verbleiben statt exakt gleich viel. Hyperliquid kann bei einem erfolgreichen Vault-Withdrawal auch `requestedUsd` genau ein Micro-USDC unter dem signierten Integer-Micro-Betrag melden; die Reconciliation akzeptiert ausschliesslich dieses exakte Abwaerts-Quantum und lehnt groessere Betragsabweichungen weiterhin ab.

Persistierte Descriptors verwenden sortiertes JSON fuer stabile Integritaetspruefungen, waehrend Hyperliquid-MessagePack-Hashes von der Object-Key-Reihenfolge abhaengen. Vor jeder Signatur und Submission rekonstruiert PBGui `agentSendAsset`- und `vaultTransfer`-Actions in der aktuellen offiziellen Schema-Reihenfolge. Das bleibt ueber API-Neustarts und vorbereitete Operationen deterministisch. Standard-Account- und Vault-Live-Transfers verwenden beide ihre validierten API-Agent-Pfade.

## Intents Und Reconciliation

Die Tabelle **Live Transfer Intents** zeigt die dauerhaften Statuswerte **Prepared**, **Submitting**, **Confirmed**, **Failed** und **Unknown**. Prepared wird vor Exchange-I/O persistiert. Confirmed aktualisiert die Abrechnung erst nach Reconciliation. Failed ist ein eindeutig nicht ausgefuehrtes Ergebnis.

Das **Dry Decision Journal** ist einklappbar. Bei einer bereits **Live** oder **Paused Unknown** laufenden Policy startet es geschlossen, bei Dry oder Disabled offen. Eine manuelle Expand-/Collapse-Auswahl bleibt beim Refresh desselben Accounts erhalten; beim Wechsel in Live oder zu einem anderen Live-Account wird der geschlossene Default einmalig angewendet.

Unknown bedeutet, dass PBGui nicht beweisen kann, ob die Exchange den Request ausgefuehrt hat. Die Policy wechselt zu **Paused Unknown** und blockiert neue Live-Submissions. **Reconcile** fragt die Exchange mit derselben dauerhaften Operation Identity erneut ab und sendet niemals blind einen zweiten Transfer. Test-Transfer-Operationen bleiben getrennt und bieten bei Unknown absichtlich keine Retry-Aktion.

Aenderungen an einer aktiven Live-Policy benoetigen eine ausdrueckliche Finanzbestaetigung und den exakten aktuellen Policy-Fingerprint. Dadurch kann ein veralteter Browser-Tab weder neuere Einstellungen ueberschreiben noch andere als die geprueften Einstellungen aktivieren. Settlement-Asset oder Baseline-Abrechnung, Baseline-Reset und Policy-Loeschung erfordern zuerst das Deaktivieren von Live. Kann nach einem bestaetigten Vault-Withdrawal das Main-Spot-Forwarding nicht sofort erstellt werden, pausiert PBGui die Policy und bietet Reconciliation fuer dasselbe erste Leg an; ein weiterer Vault-Withdrawal wird niemals erzeugt.

## Fehlerbehebung

- **Unsupported oder unavailable:** Exchange-Typ, Credentials, Berechtigungen, Hyperliquid-Agent-Freigabe und Account-Modus unter API Keys pruefen.
- **Live-Aktivierung abgelehnt:** Grund in der Overview lesen und danach vollstaendige History, Snapshot-Freshness, Asset, Liabilities, Margin, Lockup und Transfer-Berechtigung pruefen.
- **Kein Sweep:** Mode, Trigger, High-Water-Mark-Recovery, Mindestbetrag, Reserve, Limits, Cooldown, Due-Betrag und naechste Auswertung pruefen.
- **Test-Transfer abgelehnt:** Einen positiven Betrag innerhalb des frischen transferierbaren Bestands verwenden. Vault-Withdrawals unter 5 USDC sind erlaubt, koennen aber kein Transfer back anbieten. Ein ruecktransferierbarer Vault-Test benoetigt ein positives konservatives leader-eigenes Withdrawal-Cap, nach dem Withdrawal strikt mehr als 100 USDC und 5% Leader-Retention, mindestens 5 USDC Empfangsbetrag sowie fuer den Rueckweg genuegend frischen Leader-Main-Bestand. Bei Binance **Internal/Universal Transfer** fuer den API-Key aktivieren; Withdrawals sind nicht erforderlich.
- **Bybit Evaluate funktioniert, Transfers sind aber unavailable:** Fuer den API-Key die Berechtigung **Account Transfer** aktivieren. Wallet- und Transaction-History-Reads reichen fuer Dry-Evaluation; PBGui leitet aus Multi-Asset-Collateral-Summen keinen transferierbaren USDT-Betrag ab.
- **Bitget Spot zeigt unavailable:** Wallet Transfer reicht zum Verschieben der Funds. Bitget Spot read muss nur aktiviert werden, wenn PBGui den Spot-Saldo anzeigen und Transfer-History abfragen soll. Liefert Bitget eine erfolgreiche synchrone Transfer-ID, waehrend History-Reads verboten sind, bestaetigt PBGui anhand dieser Exchange-Antwort ohne erneute Submission.

Die Bitget-Classic-Transfer-History-Reconciliation verwendet die erforderlichen Filter `coin`, `fromType` und die persistierte `clientOid`. Bitget nennt den uebertragenen Betrag `size`; PBGui vergleicht ihn fuer Futures-to-Spot und Spot-to-Futures exakt mit dem angeforderten Betrag.
- **Unknown Operation:** Nicht erneut senden und nicht zuruecktransferieren. Zeitpunkt und Betrag mit der Exchange-History vergleichen, Logs oeffnen und Reconcile nur fuer einen Live Intent verwenden.
- **Vault pausiert:** Lockup, Positionen/Orders, Leader-Anteil, retained Equity, Zielaktivitaet, Empfangsbetrag und Closing Cost oder Forced Reduction pruefen.

Browser-Requests verwenden das PBGui-HttpOnly-Session-Cookie. API Keys, Private Keys, Passphrases, Descriptors, feste Route-Payloads und rohe Exchange-Antworten werden auf dieser Seite nicht angezeigt.
