# Hyperliquid Limits

Unter **Information → Hyperliquid Limits** stehen alle gespeicherten Hyperliquid-Konten mit ihren laufenden PB7/PB8-Bots und VPS-Hosts. Mehrere Bots mit derselben Wallet teilen sich eine Zeile und dasselbe Adresslimit. Die Tabelle zeigt Used, Cap, verbleibende Requests, Auslastung, Messstatus und Zeitpunkt. Der Filter nach Account, Bot oder VPS bleibt nach einem Browser-Reload erhalten.

Nur eine VPS, auf der ein Bot für die Wallet läuft, fragt Hyperliquid `userRateLimit` automatisch ab: einmal je Wallet alle fünf Minuten auf dieser VPS. Master lesen den zwischengespeicherten Messwert des VPS-Agenten und stellen keine periodischen Hyperliquid-Anfragen. Mehrere Bots mit derselben Wallet auf einer VPS erzeugen nur eine Abfrage. Läuft dieselbe Wallet auf mehreren VPS, fragt derzeit jede dieser VPS separat ab. Angezeigt wird die neueste gültige VPS-Messung. Veraltete oder fehlende Werte werden gekennzeichnet.

Ein Klick auf einen aktiven Account zeigt den 24-Stunden-Verlauf für Used und Cap. Der Master sammelt History nur, solange ein Bot dieses Accounts läuft. Accounts ohne laufenden Bot erscheinen als **Not sampled**. Für eine einmalige aktuelle Abfrage den gespeicherten Eintrag unter **System → API-Keys** öffnen: PBGui zeigt das Limit neben **Test Connection**. Diese Einzelabfrage startet keine laufende History. Erneutes Öffnen löst wieder genau eine Abfrage aus.

Die Zähler betreffen das adressbezogene Aktionslimit von Hyperliquid, nicht das separate IP-REST-Gewichtslimit. Die Seite aktualisiert sich automatisch und hat keinen manuellen Refresh-Button.
