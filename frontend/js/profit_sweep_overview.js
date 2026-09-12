/* Cached Profit Sweep overview. No exchange calls or browser persistence. */
(function () {
  'use strict';
  function totalAmount(values) {
    const parsed = values.filter(value => value !== null && value !== undefined)
      .map(value => String(value).match(/^([+-]?)(\d+)(?:\.(\d+))?$/)).filter(Boolean);
    if (!parsed.length) return '—';
    const scale = Math.max(2, ...parsed.map(parts => (parts[3] || '').length));
    const units = parsed.reduce((sum, parts) => sum + (parts[1] === '-' ? -1n : 1n)
      * BigInt(parts[2] + (parts[3] || '').padEnd(scale, '0')), 0n);
    const absolute = units < 0n ? -units : units;
    const divisor = 10n ** BigInt(scale - 2);
    const cents = (absolute + divisor / 2n) / divisor;
    return (units < 0n && cents ? '-' : '') + (cents / 100n).toLocaleString('en-US') + '.' + String(cents % 100n).padStart(2, '0');
  }
  function summaryGroups(rows, field) {
    const groups = new Map();
    for (const row of rows) {
      const simulation = row.mode === 'dry' && ['net_pnl', 'swept', 'sweep_due'].includes(field);
      const key = row.asset + (simulation ? ' · sim' : '');
      if (!groups.has(key)) groups.set(key, {label: key, rows: [], targets: new Map(), missing: 0});
      const group = groups.get(key);
      if (field === 'target_balance') {
        if (!row.target_key) { group.missing++; continue; }
        const previous = group.targets.get(row.target_key);
        if (!previous || (previous.target_balance == null && row.target_balance != null)
            || (row.target_balance != null && (row.target_updated_at || row.updated_at || 0) > (previous.target_updated_at || previous.updated_at || 0))) {
          group.targets.set(row.target_key, row);
        }
      } else group.rows.push(row);
    }
    return Array.from(groups.values()).sort((a, b) => a.label.localeCompare(b.label)).map(group => {
      const values = (field === 'target_balance' ? Array.from(group.targets.values()) : group.rows).map(row => row[field]);
      const missing = group.missing + values.filter(value => value == null).length;
      return {label: group.label, amount: totalAmount(values), missing};
    });
  }
  class ProfitSweepOverview {
    constructor(request, selectAccount) {
      this.request = request;
      this.selectAccount = selectAccount;
      this.generation = 0;
      this.active = false;
      this.timer = null;
      this.controller = null;
      this.stopped = false;
      this.rows = [];
      this.anonymized = false;
      this.aliases = new Map();
      this.refreshMinutes = 15;
      this.settingsController = null;
      this.visibility = () => {
        if (document.hidden) this.cancel();
        else if (this.active && !this.stopped) this.refresh();
      };
      document.addEventListener('visibilitychange', this.visibility);
    }
    cancel() {
      this.generation++;
      if (this.timer) window.clearTimeout(this.timer);
      if (this.controller) this.controller.abort();
      this.timer = null;
      this.controller = null;
    }
    accountLabel(name) {
      if (!this.anonymized) return name;
      if (!this.aliases.has(name)) this.aliases.set(name, 'Account ' + String(this.aliases.size + 1).padStart(2, '0'));
      return this.aliases.get(name);
    }
    setAnonymized(enabled) {
      this.anonymized = enabled;
      document.getElementById('account-search').value = '';
      this.render();
      if (this.onAnonymize) this.onAnonymize();
    }
    show() {
      this.active = true;
      document.getElementById('main-content').classList.add('overview-active');
      document.getElementById('show-overview').classList.add('active');
      document.getElementById('show-overview').setAttribute('aria-pressed', 'true');
      document.getElementById('account-heading').textContent = 'Profit Sweep Overview';
      document.getElementById('account-subtitle').textContent = 'Active accounts · balances refresh in the background';
      if (this.onAnonymize) this.onAnonymize();
      this.refresh();
    }
    hide() {
      this.active = false;
      if (this.settingsController) this.settingsController.abort();
      this.cancel();
      document.getElementById('main-content').classList.remove('overview-active');
      document.getElementById('show-overview').classList.remove('active');
      document.getElementById('show-overview').setAttribute('aria-pressed', 'false');
    }
    destroy() {
      this.stopped = true;
      if (this.settingsController) this.settingsController.abort();
      this.cancel();
      document.removeEventListener('visibilitychange', this.visibility);
      this.rows = [];
      document.getElementById('sweep-overview-rows').replaceChildren();
      document.getElementById('sweep-overview-summary').replaceChildren();
    }
    async refreshNow() {
      if (this.stopped || this.settingsController) return;
      this.cancel();
      const button = document.getElementById('overview-refresh-now');
      const controller = this.settingsController = new AbortController();
      const deadline = window.setTimeout(() => controller.abort(), 10000);
      button.disabled = true;
      button.textContent = 'Requesting…';
      try {
        await this.request('/overview/refresh', {method: 'POST', signal: controller.signal});
        if (controller.signal.aborted || this.stopped || !this.active) return;
        document.getElementById('sweep-overview-message').textContent = 'Balance refresh queued. Values update as background reads complete.';
      } catch (error) {
        if (this.stopped || !this.active) return;
        if (error.status === 401 || error.status === 403) {
          this.destroy();
          document.getElementById('sweep-overview-message').textContent = 'Session expired. Please sign in again.';
        } else {
          document.getElementById('sweep-overview-message').textContent = 'Balance refresh could not be requested. Please try again.';
        }
      } finally {
        window.clearTimeout(deadline);
        button.disabled = false;
        button.textContent = 'Refresh now';
        this.settingsController = null;
        if (this.active && !this.stopped) this.timer = window.setTimeout(() => this.refresh(), 5000);
      }
    }
    async setRefreshMinutes(value) {
      if (this.stopped || this.settingsController) return;
      this.cancel();
      const select = document.getElementById('overview-refresh-minutes');
      const controller = this.settingsController = new AbortController();
      const deadline = window.setTimeout(() => controller.abort(), 10000);
      select.disabled = true;
      try {
        const data = await this.request('/overview/settings', {
          method: 'PUT', body: JSON.stringify({refresh_minutes: Number(value)}), signal: controller.signal
        });
        if (controller.signal.aborted || this.stopped) return;
        this.refreshMinutes = data.refresh_minutes;
      } catch (error) {
        if (this.stopped) return;
        if (error.status === 401 || error.status === 403) {
          this.destroy();
          document.getElementById('sweep-overview-message').textContent = 'Session expired. Please sign in again.';
        } else if (this.active) {
          document.getElementById('sweep-overview-message').textContent = 'Refresh interval could not be saved. Please try again.';
        }
      } finally {
        window.clearTimeout(deadline);
        select.value = String(this.refreshMinutes);
        select.disabled = false;
        this.settingsController = null;
        if (this.active && !this.stopped) this.timer = window.setTimeout(() => this.refresh(), 5000);
      }
    }
    async refresh() {
      if (this.settingsController) return;
      this.cancel();
      if (!this.active || document.hidden || this.stopped) return;
      const generation = this.generation;
      const controller = this.controller = new AbortController();
      const deadline = window.setTimeout(() => controller.abort(), 10000);
      try {
        const data = await this.request('/overview', {signal: controller.signal});
        if (generation !== this.generation || this.stopped) return;
        this.rows = Array.isArray(data.accounts) ? data.accounts : [];
        this.refreshMinutes = data.refresh_minutes || 15;
        document.getElementById('overview-refresh-minutes').value = String(this.refreshMinutes);
        this.render();
        document.getElementById('sweep-overview-message').textContent = this.rows.length
          ? ''
          : 'No active Profit Sweep accounts. Select an account in the sidebar to configure one.';
      } catch (error) {
        if (generation !== this.generation || this.stopped) return;
        if (error.status === 401 || error.status === 403) {
          this.destroy();
          document.getElementById('sweep-overview-message').textContent = 'Session expired. Please sign in again.';
          return;
        }
        this.rows.forEach(row => { row.stale = true; });
        this.render();
        document.getElementById('sweep-overview-message').textContent = 'Overview refresh failed. Showing previous values; retrying shortly.';
      } finally {
        window.clearTimeout(deadline);
        if (generation === this.generation && this.active && !this.stopped) {
          this.controller = null;
          this.timer = window.setTimeout(() => this.refresh(), 5000);
        }
      }
    }
    render() {
      const body = document.getElementById('sweep-overview-rows');
      const focused = body.contains(document.activeElement) ? document.activeElement.closest('tr')?.dataset.account : null;
      const query = document.getElementById('account-search').value.trim().toLowerCase();
      const fragment = document.createDocumentFragment();
      const date = value => value ? new Date(value * 1000).toLocaleString() : '—';
      const amount = value => value === null || value === undefined ? '—' : Number(value).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
      const visible = [];
      for (const row of this.rows) {
        const label = this.accountLabel(row.name);
        if (query && !(label + ' ' + row.asset).toLowerCase().includes(query)) continue;
        visible.push(row);
        const tr = document.createElement('tr');
        tr.tabIndex = 0;
        tr.dataset.account = label;
        tr.setAttribute('aria-label', 'Open ' + label);
        const cell = (value, cls, tip) => {
          const td = document.createElement('td');
          td.textContent = value;
          if (cls) td.className = cls;
          if (tip) td.title = tip;
          tr.appendChild(td);
          return td;
        };
        const account = cell('', 'sweep-account');
        const identity = document.createElement('span');
        identity.className = 'sweep-account-identity';
        const name = document.createElement('span');
        name.className = 'sweep-account-name';
        name.textContent = label;
        name.title = label;
        const badge = document.createElement('span');
        badge.className = 'mode-badge ' + (row.mode === 'live' ? 'live' : row.mode === 'dry' ? 'dry' : 'paused');
        badge.textContent = row.mode === 'paused_unknown' ? 'PAUSED' : row.mode.toUpperCase();
        const asset = document.createElement('span');
        asset.className = 'sweep-account-asset';
        asset.textContent = row.asset;
        identity.append(name, badge, asset);
        account.appendChild(identity);
        const details = [
          'Last decision: ' + row.reason.replaceAll('_', ' '),
          'Recovery needed: ' + (row.recovery_needed ?? '—'),
          'Reference capital: ' + (row.reference_capital ?? '—'),
          'High-water mark: ' + (row.high_watermark ?? '—'),
          'Max transferable: ' + (row.max_transferable ?? '—'),
          'Last successful sweep: ' + date(row.last_sweep),
          'Next check: ' + date(row.next_check),
          'Sweep evaluated: ' + date(row.evaluated_at),
          'Balances updated: ' + date(row.updated_at),
          'Source balance updated: ' + date(row.source_updated_at ?? row.updated_at),
          'Target balance updated: ' + date(row.target_updated_at ?? row.updated_at),
          ...(row.issues || []).map(issue => issue.source + ': ' + issue.message),
          row.read_error && !(row.issues || []).length ? 'Snapshot incomplete. The previous cache did not retain the cause; waiting for the next background refresh.' : '',
          row.mode === 'dry' ? 'Swept is simulated; no real transfer.' : ''
        ].filter(Boolean).join('\n');
        cell('● ' + row.status + (row.stale ? ' · Stale' : '') + ' ⓘ', 'sweep-status ' + row.level, details);
        cell(amount(row.balance), 'sweep-number', row.balance == null ? 'Not available yet' : row.balance + ' ' + row.asset);
        cell((Number(row.net_pnl) > 0 ? '+' : '') + amount(row.net_pnl), 'sweep-number', row.net_pnl + ' ' + row.asset + '\nRelative to the current ' + (row.mode === 'dry' ? 'Dry' : 'Live') + ' baseline.');
        cell(amount(row.swept) + (row.mode === 'dry' ? ' sim' : ''), 'sweep-number', row.swept + ' ' + row.asset + (row.mode === 'dry' ? ' simulated' : ' confirmed') + ' in the current state; baseline resets may change the accounting period.');
        cell(amount(row.sweep_due), 'sweep-number', row.sweep_due + ' ' + row.asset + '\nAccrued amount, not a submitted transfer.');
        cell(row.target);
        cell(amount(row.target_balance), 'sweep-number', row.target_balance == null ? 'Not available yet' : row.target_balance + ' ' + row.asset);
        tr.addEventListener('click', () => this.selectAccount(row.name));
        tr.addEventListener('keydown', event => {
          if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); this.selectAccount(row.name); }
        });
        fragment.appendChild(tr);
      }
      body.replaceChildren(fragment);
      const footer = document.getElementById('sweep-overview-summary');
      footer.replaceChildren();
      if (visible.length) {
        const total = document.createElement('tr');
        for (const field of ['name', 'status', 'balance', 'net_pnl', 'swept', 'sweep_due', 'target', 'target_balance']) {
          const cell = document.createElement('td');
          if (field === 'name') cell.textContent = 'Summary · ' + visible.length + ' accounts';
          else if (field === 'status') cell.textContent = visible.filter(row => row.level === 'error').length + ' errors';
          else if (field === 'target') cell.textContent = 'Unique targets';
          else {
            cell.className = 'sweep-number';
            for (const group of summaryGroups(visible, field)) {
              const line = document.createElement('div');
              line.textContent = group.amount + ' ' + group.label + (group.missing ? ' *' : '');
              line.title = group.missing ? 'Partial total: ' + group.missing + ' unavailable or unidentified values.' : 'Total of visible accounts' + (field === 'target_balance' ? '; shared targets counted once.' : '.');
              cell.appendChild(line);
            }
          }
          total.appendChild(cell);
        }
        footer.appendChild(total);
      }
      if (focused) Array.from(body.children).find(tr => tr.dataset.account === focused)?.focus({preventScroll: true});
    }
  }
  window.ProfitSweepOverview = ProfitSweepOverview;
  ProfitSweepOverview.summaryGroups = summaryGroups;
}());
