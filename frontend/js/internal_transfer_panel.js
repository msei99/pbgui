/* Inline access to the existing fixed-route, persisted Transfers API. */
(function () {
    'use strict';
    if (window.PBGuiInternalTransferPanel) return;
    const PENDING_KEY = 'pbgui:transfers:pending-top-up:v1';
    function mount(host, options) {
        const root = document.createElement('details');
        root.className = 'balance-box balance-guidance';
        const summary = document.createElement('summary');
        summary.textContent = 'Transfer Spot ↔ Perps';
        summary.style.cursor = 'pointer';
        root.appendChild(summary);
        const form = document.createElement('div');
        Object.assign(form.style, {display: 'flex', flexWrap: 'wrap', gap: '12px', alignItems: 'end', marginTop: '12px'});
        root.appendChild(form);
        function field(label, tag) {
            const wrapper = document.createElement('label');
            wrapper.textContent = label + ' ';
            const input = document.createElement(tag);
            wrapper.appendChild(input);form.appendChild(wrapper);
            return input;
        }
        const direction = field('Direction', 'select');
        const amount = field('Amount (USDC)', 'input');
        amount.type = 'number';amount.step = 'any';amount.min = '0.000001';amount.value = '5';
        function button(label, handler) {
            const node = document.createElement('button');node.type = 'button';node.className = 'btn btn-info';
            node.textContent = label;node.addEventListener('click', handler);form.appendChild(node);return node;
        }
        const max = button('Max', () => { if (route() && !max.disabled) { amount.value = String(route().max_transferable); update(); } });
        const review = button('Review transfer', submit);
        const available = document.createElement('p');root.appendChild(available);
        const status = document.createElement('p');status.setAttribute('role', 'status');root.appendChild(status);
        const history = document.createElement('a');history.textContent = 'Transfer history / resolve pending transfer';
        history.href = options.base + '/transfers/main_page?user=' + encodeURIComponent(options.user);
        root.appendChild(history);
        // These controls are not edits to the account's saved credentials.
        root.addEventListener('input', event => event.stopPropagation());
        root.addEventListener('change', event => event.stopPropagation());
        host.appendChild(root);
        let preview = null, busy = false, generation = 0;
        const current = () => root.isConnected && options.isCurrent();
        const pending = () => { try { return !!sessionStorage.getItem(PENDING_KEY); } catch (_) { return true; } };
        const route = () => preview && preview.routes.find(item => item.id === direction.value);
        async function request(path, body) {
            const response = await fetch(options.base + path, {method: body ? 'POST' : 'GET', credentials: 'same-origin',
                cache: 'no-store', headers: {'Content-Type': 'application/json'}, signal: body ? undefined : options.signal,
                body: body ? JSON.stringify(body) : undefined});
            const value = await response.json();
            if (!response.ok) throw new Error(typeof value.detail === 'string' ? value.detail : 'Transfer request failed.');
            return value;
        }
        function update() {
            const selected = route(), value = Number(amount.value);
            const blocked = busy || !current() || !selected || !!preview.blocked_reason || pending();
            direction.disabled = busy || !current() || !preview || pending();
            amount.disabled = blocked;
            max.disabled = blocked || !Number.isFinite(Number(selected && selected.max_transferable)) || Number(selected && selected.max_transferable) <= 0;
            review.disabled = blocked || !Number.isFinite(value) || value <= 0 || value < Number(selected.minimum_amount) || value > Number(selected.max_transferable);
            available.textContent = selected ? 'Available: ' + selected.max_transferable + ' USDC · Minimum: ' + selected.minimum_amount + ' USDC' : '';
        }
        async function load() {
            const id = ++generation;
            status.textContent = 'Loading fresh transferable balances...';
            const value = await request('/transfers/preview/' + encodeURIComponent(options.user));
            if (!current() || id !== generation) return;
            const previous = direction.value || 'spot_to_perp';
            preview = Object.assign({}, value, {routes: (value.routes || []).filter(item => ['spot_to_perp', 'perp_to_spot'].includes(item.id) && item.asset === 'USDC')});
            direction.replaceChildren();
            preview.routes.forEach(item => {
                const option = document.createElement('option');option.value = item.id;
                option.textContent = item.id === 'spot_to_perp' ? 'Spot → Perps' : 'Perps → Spot';
                direction.appendChild(option);
            });
            if (preview.routes.some(item => item.id === previous)) direction.value = previous;
            status.textContent = pending() ? 'A transfer is pending. Open transfer history to resolve it; no new transfer will be sent here.' : preview.blocked_reason || 'Review and confirm before transferring. Manual transfers do not change Profit Sweep accounting.';
            if (options.onBalances) options.onBalances(preview);
            update();
        }
        async function submit() {
            update();if (review.disabled) return;
            busy = true;update();
            try {
                await load();
                if (!current()) return;
                const selected = route(), value = amount.value.trim();
                if (!selected || preview.blocked_reason || pending() || !Number.isFinite(Number(value)) || Number(value) < Number(selected.minimum_amount) || Number(value) > Number(selected.max_transferable) || Number(value) <= 0) return;
                const accepted = await window.PBGuiDialogs.confirm({title: 'Review internal transfer',
                    message: 'Move ' + value + ' USDC from ' + selected.source + ' to ' + selected.destination + ' for ' + options.user + '?',
                    detail: 'The existing Transfers checks apply. This does not change Profit Sweep accounting.', confirmText: 'Submit transfer'});
                if (!accepted || !current() || pending()) return;
                const bytes = new Uint8Array(16);window.crypto.getRandomValues(bytes);bytes[6] = (bytes[6] & 15) | 64;bytes[8] = (bytes[8] & 63) | 128;
                const hex = Array.from(bytes, value => value.toString(16).padStart(2, '0')).join('');
                const operationId = hex.slice(0,8)+'-'+hex.slice(8,12)+'-'+hex.slice(12,16)+'-'+hex.slice(16,20)+'-'+hex.slice(20);
                const operation = {user: options.user, route: selected.id, amount: value, operation_id: operationId, created_at: Date.now()};
                // Same recovery record as Transfers; failure to persist prevents submission.
                sessionStorage.setItem(PENDING_KEY, JSON.stringify(operation));
                status.textContent = 'Submitting the reviewed transfer...';
                const result = await request('/transfers/execute/' + encodeURIComponent(options.user), {route: selected.id, amount: value, operation_id: operationId});
                if (['confirmed', 'failed'].includes(result.status)) {
                    const stored = JSON.parse(sessionStorage.getItem(PENDING_KEY) || 'null');
                    if (stored && stored.operation_id === operationId) sessionStorage.removeItem(PENDING_KEY);
                }
                if (!current()) return;
                const outcome = result.status === 'confirmed' ? 'Transfer confirmed.' : 'Transfer ' + String(result.status || 'unknown') + '. Open transfer history for details; no automatic retry was sent.';
                if (result.status === 'confirmed') await options.onConfirmed();
                else { await load(); status.textContent = outcome; }
            } catch (error) {
                if (current()) status.textContent = error.message + (pending() ? ' Open transfer history to resolve the pending operation. Do not submit another transfer.' : '');
            } finally { busy = false; if (current()) update(); }
        }
        direction.addEventListener('change', update);amount.addEventListener('input', update);
        root.addEventListener('toggle', async () => {
            if (!root.open || busy) return;
            busy = true;update();
            try { await load(); } catch (error) { if (current()) status.textContent = error.message; }
            finally { busy = false;if (current()) update(); }
        });
        let polling = false;
        const timer = setInterval(async () => {
            if (!current()) { clearInterval(timer); return; }
            if (busy || polling || document.visibilityState !== 'visible') return;
            polling = true;
            try {
                const history = await request('/transfers/operations/' + encodeURIComponent(options.user));
                if (!current() || busy) return;
                const stored = JSON.parse(sessionStorage.getItem(PENDING_KEY) || 'null');
                const resolved = stored && stored.user === options.user && (history.operations || []).find(
                    item => item.operation_id === stored.operation_id && ['confirmed', 'failed'].includes(item.status));
                if (resolved) {
                    sessionStorage.removeItem(PENDING_KEY);
                    if (resolved.status === 'confirmed') { await options.onConfirmed(); return; }
                }
                await load();
            } catch (error) { if (current()) status.textContent = error.message; }
            finally { polling = false; }
        }, 5000);
        if (options.signal) options.signal.addEventListener('abort', () => clearInterval(timer), {once: true});
        update();
    }
    window.PBGuiInternalTransferPanel = Object.freeze({mount});
}());
