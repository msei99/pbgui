# Transfers

## Zweck

**System > Transfers** bietet ausdrueckliche manuelle Transfers zwischen festen internen Accounts. Die Seite akzeptiert keine externe Adresse und veraendert weder Profit-Sweep-Due noch Baselines, High-Water Marks oder bestaetigte Sweep-Summen.

Die verfuegbaren Routen werden aus dem frischen Snapshot jedes Accounts abgeleitet:

- Hyperliquid Standard: Perps zu Spot und Spot zu Perps.
- Hyperliquid Vault: Vault zu Leader Main Perps, Leader Main Perps zu Vault und bei Standard/Manual-Leader-Modus Leader Main Perps zu Main Spot sowie Main Spot zu Main Perps.
- Bybit: Unified zu Funding und Funding zu Unified.
- Binance: USD-M Futures zu Funding und Funding zu USD-M Futures.
- Bitget Classic: USDT Futures zu Spot und Spot zu USDT Futures.
- Bitget UTA: UTA zu Spot und Spot zu UTA.

## Interner Transfer

1. Einen unterstuetzten Exchange-Account in der Sidebar auswaehlen.
2. Eine der serverseitig angebotenen festen Routen waehlen.
3. Frischen Quellbestand, tatsaechlich transferierbaren Betrag, Zielbestand, Asset und Routenminimum pruefen.
4. Einen Betrag eingeben, der **Available to transfer** nicht ueberschreitet.
5. **Review transfer** anklicken.
6. Exakten Account, Route, Betrag, Quelle und Ziel im PBGui-Dialog bestaetigen.

PBGui leitet jede Quelle und jedes Ziel ausschliesslich aus dem ausgewaehlten konfigurierten Account ab. Der Route-Selector enthaelt nur serverseitig abgeleitete Allowlist-Routen; Adressen und Assets sind nicht frei editierbar. Eine Profit-Sweep-Policy ist nicht erforderlich.

Der **Direction**-Selector steht direkt neben **Amount** und **Review transfer**. Verwendet der Leader eines Hyperliquid Vaults Unified oder Portfolio Margin, stellt Hyperliquid Main Perps und Main Spot als einen gemeinsamen **Main Unified**-Bestand bereit. PBGui laesst Main-zu-Spot-Richtungen dann korrekt weg und zeigt die Erklaerung direkt bei den Transfer-Controls; zwischen diesen zusammengelegten Accounts ist kein interner Transfer erforderlich.

Bei Vault-Accounts trennt das Preview **Your Vault Equity** vom vollstaendigen **Vault Account Value** und Hyperliquids userspezifischem Wert **Your Max Withdrawable**. Ausserdem zeigt es jede bereinigte offene Vault-Position mit Coin, Seite, Groesse, Positionswert, Entry-Preis, unrealisiertem PnL, Liquidationspreis und Leverage-Typ. Ein Transfer veraendert oder schliesst Positionen nicht direkt, kann durch verschobenes Collateral aber Passivbots wallet-exposure-basierte Groessenberechnung, verfuegbare Margin und spaetere Ordergroessen beeinflussen.

## Operationssicherheit

Jeder Transfer erhaelt eine browsergenerierte Idempotency-UUID und wird vor Exchange-I/O persistiert. Der accountweite Operation-Lock verhindert Rennen mit Profit Sweep oder anderen manuellen Transfers. Ein nicht aufgeloester Profit-Sweep-Intent oder manueller Transfer blockiert einen neuen Transfer.

PBGui sendet jede Operation hoechstens einmal und fuehrt eine begrenzte Ledger-Reconciliation aus. Eine verlorene Browserantwort darf nur mit derselben behaltenen Operation-ID, Route und demselben Betrag erneut angefragt werden. Bei **Unknown** bietet die Seite **Reconcile** an; diese Aktion fragt nur die Exchange-History ab und sendet den Transfer niemals erneut. Laufende Submissions blockieren einen API-Neustart. Beim Start reconciliert PBGui bereits gesendete Operationen und markiert einen vor der Submission unterbrochenen Transfer als fehlgeschlagen, ohne ihn zu senden.

Einige Exchange-History-Zeilen enthalten PBGui's Operation-UUID nicht. PBGui verhindert deshalb eigene Transfers mit gleicher Route und gleichem Betrag innerhalb desselben zehnminuetigen Matching-Fensters. Waehrend einer PBGui-Operation keinen identischen manuellen Transfer ueber einen anderen Client starten; eine extern erstellte identische Zeile laesst sich eventuell nicht zuverlaessig unterscheiden.

Die Transfer-History ist von Profit-Sweep-Live-Intents und Test Transfers getrennt. Sie zeigt Route, angeforderten und empfangenen Betrag, Zeitstempel, Status und begrenzte Fehler- oder Reconciliation-Gruende, aber keine Adressen, Descriptors, Signaturen, Credentials oder rohen Providerantworten.

## Profit-Sweep-Handoff

Bei einem Hyperliquid Vault oeffnet **Profit Sweep > Exchange / Vault > Fund account** Transfers mit exakt diesem Account und **Main Perps zu Vault** vorausgewaehlt. Ein vorhandener `PAUSED_UNKNOWN`-Profit-Sweep-Intent muss vor einer Geldbewegung reconciliert werden.

## Fehlerbehebung

- **Transfer blockiert:** zuerst den nicht aufgeloesten Profit-Sweep- oder manuellen Transfer reconciliieren.
- **Nicht genug Bestand:** Betrag unter den frischen Wert **Available to transfer** der ausgewaehlten Route reduzieren.
- **Minimum abgelehnt:** das angezeigte Routenminimum verwenden; Hyperliquid-Vault-Deposits erfordern mindestens `5 USDC`.
- **Unknown:** keinen weiteren identischen Transfer erstellen. **Reconcile** fuer die vorhandene Operation verwenden.
