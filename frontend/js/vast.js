/* Vast account and marketplace: same-origin requests, no persisted credentials. */
(async function () {
  'use strict';
  if (window.OPTIMIZE_VERSION !== 'v8') return;
  const apiBase = window.API_BASE.replace(/\/optimize-v8$/, '/vast');
  const host = document.getElementById('vast-queue-host');
  const bootstrap = new AbortController();
  let leaving = false;
  window.addEventListener('pagehide', () => { leaving = true; bootstrap.abort(); }, {once:true});
  const bootstrapTimer = setTimeout(() => bootstrap.abort(), 30000);
  try {
    const componentResponse = await fetch(apiBase + '/fragment', {credentials:'same-origin', cache:'no-store', signal:bootstrap.signal});
    if (!componentResponse.ok) throw new Error('Cloud component unavailable');
    const parsed = new DOMParser().parseFromString(await componentResponse.text(), 'text/html');
    if (leaving) return;
    host.appendChild(document.importNode(parsed.querySelector('.vast-component'), true));
    const jobsHost = document.getElementById('vast-jobs-host');
    if (jobsHost) jobsHost.appendChild(document.getElementById('vast-jobs'));
  } catch (error) {
    if (!leaving) host.textContent = 'Cloud controls could not load. Check the API restart indicator and reload this page.';
    return;
  } finally { clearTimeout(bootstrapTimer); }
  let worker = null, queueState = {}, selectedOffer = null, renting = false;
  let hostBlockBusy = false, offerRows = [];
  let hostProfiles = new Map(), hostGeneration = 0;

  const el = id => document.getElementById(id);
  const settingsSidebar = el('vast-sidebar-controls');
  if (settingsSidebar) {
    settingsSidebar.appendChild(el('vast-settings-nav'));
    settingsSidebar.appendChild(el('vast-performance-actions'));
    const actions = document.createElement('div');
    actions.className = 'ctx-actions';
    actions.id = 'vast-rental-actions';
    const rentalTitle = document.createElement('div');
    rentalTitle.className = 'sb-label'; rentalTitle.textContent = 'Rental';
    actions.appendChild(rentalTitle);
    ['rent-offer', 'settings-end-rental'].forEach(id => {
      const button = el(id);
      if (button) { button.classList.add('sb-btn'); actions.appendChild(button); }
    });
    settingsSidebar.appendChild(actions);
    el('settings-host-block').classList.add('ctx-actions');
    settingsSidebar.appendChild(el('settings-host-block'));
  }
  const performanceView = window.PBGuiVastPerformance?.create({request});
  el('vast-settings-nav').querySelectorAll('[data-vast-view]').forEach(button => {
    button.addEventListener('click', () => {
      const view = button.dataset.vastView;
      el('vast-offers').hidden = view !== 'offers';
      el('vast-setup').hidden = view !== 'offers';
      el('vast-known-hosts').hidden = view !== 'known';
      el('vast-blocked-hosts').hidden = view !== 'blocked';
      el('vast-performance').hidden = view !== 'performance';
      el('vast-performance-actions').hidden = view !== 'performance';
      if (el('vast-rental-actions')) el('vast-rental-actions').hidden = view === 'performance';
      el('settings-host-block').hidden = view === 'performance';
      if (view === 'performance') performanceView?.show();
      else performanceView?.hide();
      const local = el('settings-modal')?.querySelector(':scope > .modal-body');
      if (local) local.hidden = view !== 'offers';
      el('vast-settings-nav').querySelectorAll('[data-vast-view]').forEach(item => {
        item.classList.toggle('active', item === button);
        item.setAttribute('aria-pressed', String(item === button));
      });
      if (view !== 'offers') clearSecrets();
    });
    button.setAttribute('aria-pressed', String(button.classList.contains('active')));
  });
  let generation = 0;
  let offerGeneration = 0;
  let accountGeneration = 0;
  let disposed = false;
  let selectedJobId = null;
  let savedPreferences = null, jobRows = [], supervision = false, jobGeneration = 0, jobTimer = null;
  const controllers = new Set();
  const requeuingJobs = new Set();
  const stoppingJobs = new Map();
  const deletingJobs = new Set();
  let workerAction = null;
  function stopPhase(job) {
    if (!job || ['completed','cancelled','failed'].includes(job.status)) return null;
    if (job.status === 'collecting') return 'Collecting results…';
    return stoppingJobs.get(job.id) || (job.stop_requested ? 'Stop requested' : null);
  }
  let startingQueue = false;
  let startingJobId = null;
  let billingSnapshot = null, billingLease = null, billingNextCheck = 0, billingPending = false;

  function clearSecrets() {
    el('api-key').value = '';

  }

  function message(text, error) {
    ['message', 'queue-message', 'rent-message'].forEach(id => {
      const target = el(id);
      if (target) { target.textContent = text; target.classList.toggle('error', !!error); }
    });
  }

  async function request(path, options) {
    if (disposed) throw new Error('Cloud controls are disconnected. Sign in and reload this page.');
    const controller = new AbortController();
    controllers.add(controller);
    const timer = setTimeout(() => controller.abort(), (["/jobs/prepare", "/queue/start"].includes(path) || path.endsWith('/requeue')) ? 600000 : 30000);
    try {
      const response = await fetch(apiBase + path, {
        credentials: 'same-origin', cache: 'no-store', ...options,
        headers: {'Content-Type': 'application/json'}, signal: controller.signal
      });
      if ([401, 403].includes(response.status) && response.headers.get('X-PBGui-Error-Source') !== 'vast') {
        clearSecrets(); disposed = true; clearTimeout(jobTimer); jobGeneration++; closeLog();
        controllers.forEach(item => { if (item !== controller) item.abort(); });
        generation++; accountGeneration++; offerGeneration++;
        el('balance').textContent = 'Authentication required';
        message('PBGui session expired. Sign in again and reload this page.', true);
        if (typeof toast === 'function') toast('PBGui session expired. Sign in again and reload this page.', 'err');
        throw new Error('Please sign in to PBGui again.');
      }
      let data;
      try { data = await response.json(); }
      catch (error) {
        if (controller.signal.aborted) throw error;
        throw new Error(response.ok ? 'Invalid response from PBGui. Reload or retry.' : 'Request failed (HTTP ' + response.status + '). Retry when the service is available.');
      }
      if (!response.ok) throw new Error(data && typeof data.detail === 'string' ? data.detail : 'Request failed (HTTP ' + response.status + '). Check the entered values.');
      return data;
    } finally {
      clearTimeout(timer);
      controllers.delete(controller);
    }
  }

  function renderSettings(data) {
    el('key-status').textContent = data.configured ? 'API key saved locally.' : 'No API key saved.';
    el('vast-setup').open = !data.configured;
  }

  async function saveSecret(event, field, input) {
    event.preventDefault();
    const value = el(input).value.trim();
    if (!value) { message('Enter a key before saving.', true); return; }
    const current = ++generation;
    accountGeneration++; offerGeneration++;
    const buttons = document.querySelectorAll('#credentials-form button');
    buttons.forEach(button => button.disabled = true);
    try {
      const data = await request('/credentials', {method:'POST', body:JSON.stringify({[field]:value})});
      if (disposed || current !== generation) return;
      clearSecrets(); renderSettings(data); el('refresh-balance').disabled = false;
      el('balance').textContent = 'Not checked'; el('balance-time').textContent = '';
      el('selection').textContent = 'Save GPU requirements for future queue starts.';
      el('offers-body').replaceChildren(); el('find-offers').disabled = false; renderJob();
      message('Credentials saved. Use the balance button to test the connection.', false);
    } catch (error) { if (!disposed) message(error.message, true); }
    finally { if (!disposed) buttons.forEach(button => button.disabled = false); }
  }

  async function refreshBalance() {
    const current = ++accountGeneration;
    el('refresh-balance').disabled = true;
    try {
      const data = await request('/account', {method:'POST'});
      if (disposed || current !== accountGeneration) return;
      el('balance').textContent = data.balance_usd == null ? 'Unavailable' : new Intl.NumberFormat('en-US', {style:'currency', currency:'USD'}).format(data.balance_usd);
      el('balance-time').textContent = data.checked_at ? 'Checked ' + new Date(data.checked_at * 1000).toLocaleString() : '';
      message('Connected to Vast.ai' + (data.account_id == null ? '.' : ' account ' + data.account_id + '.'), false);
    } catch (error) {
      if (!disposed && current === accountGeneration) { el('balance-time').textContent = 'Refresh failed — previous value may be outdated'; message(error.message, true); }
    } finally { if (!disposed && current === accountGeneration) el('refresh-balance').disabled = false; }
  }

  function fmt(value, digits) { return value == null ? 'Unknown' : Number(value).toFixed(digits == null ? 2 : digits); }

  function offerDuration(seconds) {
    if (seconds == null) return 'Unknown';
    const hours = Math.floor(seconds / 3600), days = Math.floor(hours / 24);
    return days ? days + 'd ' + (hours % 24) + 'h' : hours ? hours + 'h' : '<1h';
  }

  function renderOffers(rows) {
    offerRows = rows;
    selectedOffer = null;
    renderJob();
    const body = el('offers-body'); body.replaceChildren();
    el('selection').textContent = 'Save GPU requirements for future queue starts.';
    if (!rows.length) {
      const row = document.createElement('tr'), cell = document.createElement('td');
      cell.colSpan = 11; cell.textContent = 'No matching offers. Adjust your filters and try again.';
      row.appendChild(cell); body.appendChild(row); return;
    }
    rows.forEach(offer => {
      const row = document.createElement('tr'); row.dataset.offer = String(offer.id); row.tabIndex = 0;
      const reasons = [];
      if (offer.cuda_max_good == null) reasons.push('CUDA unknown');
      else if (offer.cuda_max_good < 13) reasons.push('Requires CUDA 13');
      if (offer.duration_seconds != null && offer.duration_seconds < Number(el('job-hours').value) * 3600) reasons.push('Rental duration too short');
      const values = [offer.gpu_name, fmt(offer.vram_gb, 1) + ' GB', fmt(offer.cpu_cores, 1) + ' / ' + fmt(offer.ram_gb, 1) + ' GB',
        '$' + fmt(offer.price_hour_usd, 4), fmt(offer.download_gb_usd, 4) + ' / ' + fmt(offer.upload_gb_usd, 4),
        fmt(offer.inet_down_mbps, 0) + ' / ' + fmt(offer.inet_up_mbps, 0), offer.location + (offer.verified ? ' · verified' : ''),
        offer.reliability == null ? 'Unknown' : fmt(offer.reliability * 100, 1) + '%', offerDuration(offer.duration_seconds), reasons.length ? reasons.join(' · ') : 'Compatible'];
      values.forEach(value => { const cell = document.createElement('td'); cell.textContent = value; row.appendChild(cell); });
      const secondary = (cell, value) => { const line = document.createElement('div'); line.className = 'muted offer-secondary'; line.textContent = value; cell.appendChild(line); };
      secondary(row.cells[0], offer.tflops == null ? 'TFLOPS unknown' : fmt(offer.tflops, 1) + ' TFLOPS');
      row.cells[0].title = 'GPU compute capacity reported by Vast. Optimizer speed also depends on CPU and memory.';
      secondary(row.cells[1], fmt(offer.gpu_mem_bw_gbps, 0) + ' GB/s');
      secondary(row.cells[2], offer.cpu_name || 'CPU model unknown');
      secondary(row.cells[6], offer.machine_id ? 'Machine ' + offer.machine_id : 'Machine ID unavailable');
      const history = hostProfiles.get(offer.machine_id) || {};
      secondary(row.cells[6], hostStatus(history));
      secondary(row.cells[9], 'CUDA ' + fmt(offer.cuda_max_good, 1));
      row.cells[9].classList.add(reasons.length ? 'offer-incompatible' : 'offer-compatible');
      row.cells[9].title = 'Marketplace compatibility check. Requirements are checked again at rental start.';
      const detailsRow = document.createElement('tr'), detailsCell = document.createElement('td');
      detailsRow.className = 'offer-details'; detailsRow.hidden = true; detailsCell.colSpan = 11;
      const details = document.createElement('div'); details.className = 'offer-detail-grid';
      const entries = [
        ['GPU compute (Vast)', offer.tflops == null ? 'TFLOPS unknown' : fmt(offer.tflops, 2) + ' TFLOPS'],
        ['PCIe', 'PCIe ' + fmt(offer.pci_gen, 1) + ' ×' + fmt(offer.gpu_lanes, 0) + ' · ' + fmt(offer.pcie_bw_gbps, 1) + ' GB/s'],
        ['Storage', (offer.disk_name || 'Unknown device') + ' · ' + fmt(offer.disk_bw_mbps, 0) + ' MB/s'],
        ['Allocated disk', fmt(offer.disk_gb, 0) + ' GB'],
        ['Maximum rental duration', offerDuration(offer.duration_seconds) + ' (at search time)']
      ];
      entries.forEach(([label, value]) => { const item = document.createElement('div'), title = document.createElement('strong'); title.textContent = label; item.appendChild(title); secondary(item, value); details.appendChild(item); });
      detailsCell.appendChild(details); detailsRow.appendChild(detailsCell);
      if (offer.machine_id) details.appendChild(hostPreferenceControls(offer.machine_id));
      const actionCell = document.createElement('td'), expand = document.createElement('button');
      expand.type = 'button'; expand.textContent = 'Details'; expand.setAttribute('aria-expanded', 'false');
      expand.addEventListener('click', event => { event.stopPropagation(); detailsRow.hidden = !detailsRow.hidden; expand.setAttribute('aria-expanded', String(!detailsRow.hidden)); expand.textContent = detailsRow.hidden ? 'Details' : 'Close details'; });
      expand.addEventListener('keydown', event => event.stopPropagation());
      actionCell.appendChild(expand); row.appendChild(actionCell);
      if (offer.machine_id) actionCell.appendChild(hostBlockButton(offer.machine_id));
      const select = () => {
        body.querySelectorAll('tr').forEach(other => { other.classList.remove('selected'); other.setAttribute('aria-selected', 'false'); });
        selectedOffer = reasons.length ? null : offer;
        row.classList.add('selected'); row.setAttribute('aria-selected', 'true');
        el('selection').hidden = false;
        el('selection').textContent = offer.gpu_name + ' · ' + offer.location + ' · $' + fmt(offer.price_hour_usd, 4) + '/hour · not reserved. Rent starts billing immediately.';
        renderJob();
      };
      row.addEventListener('click', select);
      row.addEventListener('keydown', event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); select(); } });
      body.appendChild(row); body.appendChild(detailsRow);
    });
  }

  function hostStatus(profile) {
    return [profile.used ? 'Previously used' : 'No recorded use',
      profile.working ? 'Working' : '', profile.preferred ? 'Preferred' : ''].filter(Boolean).join(' · ');
  }

  function acceptHostProfiles(data) {
    if (Array.isArray(data.hosts)) hostProfiles = new Map(data.hosts.map(profile => [profile.machine_id, profile]));
  }

  function hostPreferenceControls(machineId, rentalId) {
    const profile = hostProfiles.get(machineId) || {};
    const container = document.createElement('span'); container.className = 'fields';
    const label = document.createElement('span'); label.textContent = hostStatus(profile);
    label.title = profile.working_detected ? 'Working: exact optimization results were recorded on this machine.'
      : profile.working_marked ? 'Working: manually marked by you.' : 'Rental history is local to PBGui.';
    container.appendChild(label);
    const choices = [['preferred', profile.preferred ? 'Remove preference' : 'Prefer host', !profile.preferred]];
    if (!profile.working_detected) choices.push(['working', profile.working_marked ? 'Clear working mark' : 'Mark working', !profile.working_marked]);
    choices.forEach(([field, text, value]) => {
      const button = document.createElement('button'); button.type = 'button'; button.className = 'btn';
      button.textContent = text; button.dataset.hostBlock = ''; button.disabled = hostBlockBusy;
      button.title = field === 'preferred' ? 'Prioritize this machine within your rental limits. Blocked hosts remain excluded.' : 'Record your own working assessment for this machine.';
      button.addEventListener('keydown', event => event.stopPropagation());
      button.addEventListener('click', event => { event.stopPropagation(); changeHostPreference(machineId, {[field]:value}, rentalId); });
      container.appendChild(button);
    });
    return container;
  }

  function renderKnownHosts() {
    const list = el('known-hosts-list'); list.replaceChildren();
    if (!hostProfiles.size) list.textContent = 'No identified rental history or host marks yet.';
    [...hostProfiles.values()].sort((a,b) => Number(b.preferred) - Number(a.preferred) || a.machine_id - b.machine_id).forEach(profile => {
      const item = document.createElement('div'); item.className = 'fields';
      const label = document.createElement('span');
      label.textContent = 'Machine ' + profile.machine_id + ' · ' + (profile.rentals || 0) + ' rentals'
        + ((queueState.blocked_machine_ids || []).includes(profile.machine_id) ? ' · Blocked' : '');
      item.append(label, hostPreferenceControls(profile.machine_id)); list.appendChild(item);
    });
  }

  async function refreshHostHistory() {
    const current = ++hostGeneration;
    try {
      const data = await request('/hosts');
      if (disposed || current !== hostGeneration) return;
      acceptHostProfiles(data); renderHostBlocks();
    } catch (error) { if (!disposed && current === hostGeneration) message(error.message, true); }
  }

  async function changeHostPreference(machineId, changes, rentalId) {
    if (disposed || hostBlockBusy) return;
    hostBlockBusy = true; hostGeneration++; jobGeneration++; offerGeneration++;
    el('find-offers').disabled = true; renderJob();
    try {
      const path = rentalId ? '/jobs/' + encodeURIComponent(rentalId) + '/host-preferences' : '/host-preferences';
      const data = await request(path, {method:'POST', body:JSON.stringify(rentalId ? changes : {machine_id:machineId, ...changes})});
      if (disposed) return;
      hostGeneration++; jobGeneration++; offerGeneration++;
      acceptHostProfiles(data);
      if (worker && worker.id === rentalId && data.machine_id) worker.host_machine_id = data.machine_id;
      renderOffers([...offerRows].sort((a,b) => Number(!!hostProfiles.get(b.machine_id)?.preferred) - Number(!!hostProfiles.get(a.machine_id)?.preferred)
        || a.price_hour_usd - b.price_hour_usd || a.id - b.id));
      message('Host preference saved. Rental limits and host blocks still apply.');
      await refreshJobs();
    } catch (error) { if (!disposed) message(error.message, true); }
    finally {
      hostBlockBusy = false;
      if (!disposed) {
        el('find-offers').disabled = false; renderJob();
        const displayed = jobRows.find(row => window.state && row.id === window.state.cloudLogId);
        if (displayed) renderCloudDashboard(displayed);
      }
    }
  }

  function hostBlockButton(machineId, rentalId) {
    const blocked = (queueState.blocked_machine_ids || []).includes(machineId);
    const button = document.createElement('button'); button.type = 'button'; button.className = 'btn';
    button.dataset.hostBlock = '';
    button.textContent = blocked ? 'Unblock host' : 'Block host';
    button.disabled = hostBlockBusy;
    button.title = 'Exclude this machine from future rentals. The current rental continues.';
    button.addEventListener('keydown', event => event.stopPropagation());
    button.addEventListener('click', event => {
      event.stopPropagation();
      changeHostBlock(machineId, !blocked, rentalId);
    });
    return button;
  }

  function renderHostBlocks() {
    const list = el('blocked-hosts-list');
    list.replaceChildren();
    const ids = queueState.blocked_machine_ids || [];
    if (!ids.length) list.textContent = 'No blocked hosts.';
    ids.forEach(id => {
      const item = document.createElement('div'); item.className = 'fields';
      const label = document.createElement('span'); label.textContent = 'Machine ' + id;
      item.append(label, hostBlockButton(id)); list.appendChild(item);
    });
    el('block-machine-submit').disabled = hostBlockBusy;
    const rental = el('settings-host-block'); rental.replaceChildren();
    if (worker) {
      const machine = worker.host_machine_id || (queueState.selected_offer || {}).machine_id;
      rental.appendChild(hostBlockButton(machine, worker.id));
      rental.appendChild(hostPreferenceControls(machine, worker.id));
    }
    if (settingsSidebar) rental.querySelectorAll('button').forEach(button => {
      button.classList.remove('btn'); button.classList.add('sb-btn');
    });
    renderKnownHosts();
    document.querySelectorAll('[data-host-block]').forEach(button => { button.disabled = hostBlockBusy; });
  }

  async function changeHostBlock(machineId, blocked, rentalId) {
    if (disposed || hostBlockBusy) return;
    hostBlockBusy = true; hostGeneration++; jobGeneration++; offerGeneration++;
    el('find-offers').disabled = true;
    renderJob();
    try {
      const path = rentalId ? '/jobs/' + encodeURIComponent(rentalId) + '/host-block' : '/blocked-hosts';
      const data = await request(path, {method:'POST', body:JSON.stringify(rentalId ? {blocked} : {machine_id:machineId, blocked})});
      if (disposed) return;
      jobGeneration++; offerGeneration++;
      queueState.blocked_machine_ids = data.blocked_machine_ids;
      if (worker && worker.id === rentalId && data.machine_id) worker.host_machine_id = data.machine_id;
      const ids = data.blocked_machine_ids;
      renderOffers(offerRows.filter(offer => !ids.length || (offer.machine_id && !ids.includes(offer.machine_id))));
      message(blocked ? 'Host blocked for future rentals. Any current rental continues.' : 'Host unblocked for future rentals.');
      await refreshJobs();
      await refreshHostHistory();
    } catch (error) { if (!disposed) message(error.message, true); }
    finally {
      hostBlockBusy = false;
      if (!disposed) {
        el('find-offers').disabled = false;
        renderJob();
        const displayed = jobRows.find(row => window.state && row.id === window.state.cloudLogId);
        if (displayed) renderCloudDashboard(displayed);
      }
    }
  }

  async function findOffers(event) {
    event.preventDefault();
    if (hostBlockBusy) return;
    const current = ++offerGeneration;
    el('find-offers').disabled = true;
    const params = new URLSearchParams({max_price:el('max-price').value, min_vram:el('min-vram').value,
      min_ram:el('min-ram').value, min_cpu:el('min-cpu').value, min_tflops:el('min-tflops').value, disk_gb:el('disk').value,
      verified_only:el('verified').value, gpu_name:el('gpu-model').value.trim(),
      include_incompatible:String(el('show-incompatible').checked), rental_hours:el('job-hours').value});
    try {
      const data = await request('/offers?' + params);
      if (disposed || current !== offerGeneration) return;
      hostGeneration++; acceptHostProfiles(data);
      renderOffers(data.offers); el('offer-time').textContent = 'Updated ' + new Date().toLocaleTimeString();
      message(data.offers.length + ' offers found (up to 100 shown).', false);
    } catch (error) { if (!disposed && current === offerGeneration) message(error.message, true); }
    finally { if (!disposed && current === offerGeneration) el('find-offers').disabled = false; }
  }

  const preferenceFields = {gpu_name:'gpu-model', max_price:'max-price', min_vram:'min-vram',
    min_ram:'min-ram', min_cpu:'min-cpu', min_tflops:'min-tflops', disk_gb:'disk', verified_only:'verified', hours:'job-hours', budget:'job-budget', idle_seconds:'worker-idle', convergence_enabled:'convergence-enabled', convergence_min_exact:'convergence-min', convergence_patience:'convergence-patience', convergence_tolerance_pct:'convergence-tolerance'};
  function applyPreferences(data) {
    data = {min_tflops:0, convergence_enabled:false, convergence_min_exact:512, convergence_patience:512, convergence_tolerance_pct:0.1, ...data};
    savedPreferences = {...data};
    Object.entries(preferenceFields).forEach(([key, id]) => { el(id).value = data[key] == null ? '' : String(data[key]); });
    el('saved-requirements').textContent = 'Saved: ' + (data.gpu_name || 'any GPU type') + ' · max $' + fmt(data.max_price, 4) + '/hour · ' + data.min_vram + ' GB VRAM / ' + data.min_ram + ' GB RAM / ' + data.min_cpu + ' CPU cores / min ' + fmt(data.min_tflops || 0, 1) + ' TFLOPS. Current matching offers are selected only at start.';
    renderJob();
  }
  el('save-gpu-preferences').addEventListener('click', async () => {
    if (!el('offers-form').reportValidity()) return;
    const values = {};
    Object.entries(preferenceFields).forEach(([key, id]) => {
      values[key] = key === 'gpu_name' ? el(id).value.trim() : ['verified_only','convergence_enabled'].includes(key) ? el(id).value === 'true' : Number(el(id).value);
    });
    el('save-gpu-preferences').disabled = true;
    try {
      const data = await request('/gpu-preferences', {method:'POST', body:JSON.stringify(values)});
      if (!disposed) { applyPreferences(data); message('GPU requirements saved. No GPU has been rented.', false); }
    } catch (error) { if (!disposed) message(error.message, true); }
    finally { if (!disposed) el('save-gpu-preferences').disabled = false; }
  });
  el('save-gpu-preferences').disabled = true;
  request('/gpu-preferences').then(data => { if (!disposed) applyPreferences(data); })
    .catch(error => { if (!disposed) message(error.message, true); })
    .finally(() => { if (!disposed) el('save-gpu-preferences').disabled = false; });

  function selectedJob() { return jobRows.find(row => row.id === selectedJobId); }

  function workerActivity() {
    const active = worker && !['none', 'deletion_verified'].includes(worker.rental_state);
    let text = active ? worker.gpu_name + ' · ' + (worker.rental_state === 'creation_pending' ? 'provisioning (instance not confirmed)' : worker.status) + ' · $' + fmt(worker.price_hour_usd, 4) + '/hour · rental: ' + worker.rental_state + ' · deadline: ' + new Date(worker.deadline * 1000).toLocaleString() + (worker.idle_since ? ' · idle deletion: ' + new Date((worker.idle_since + worker.idle_seconds) * 1000).toLocaleTimeString() : '') : 'No active GPU rental.' + (worker ? ' Last rental: ' + worker.rental_state + '.' : '') + ' GPU requirements are configured in Settings; a matching offer is selected at queue start.';
    if (active && worker.cleanup_wait_until) {
      text = 'Cleanup: no instance found · waiting until ' + new Date(worker.cleanup_wait_until * 1000).toLocaleTimeString()
        + ' · last checked ' + new Date(worker.provider_checked_at * 1000).toLocaleTimeString();
    }
    else if (active && worker.instance_id) {
      text = worker.gpu_name + ' · instance ' + worker.instance_id + ' · '
        + (worker.provider_status || worker.status) + ' · $' + fmt(worker.price_hour_usd, 4) + '/hour';
    }
    return text;
  }

  function renderJob() {
    renderHostBlocks();
    const job = selectedJob();
    const active = worker && !['none', 'deletion_verified'].includes(worker.rental_state);
    el('rent-offer').disabled = !selectedOffer || active || renting || startingQueue || !!workerAction || !supervision;
    el('rent-offer').textContent = renting ? 'Renting…' : 'Rent';
    el('settings-rental-status').textContent = workerActivity() + (active && worker.awaiting_queue_start ? (queueState.paused ? ' · Reserved for queue start' : ' · Queue started — waiting for input preparation') : '');
    const settingsEndRental = el('settings-end-rental');
    if (settingsEndRental) {
      settingsEndRental.hidden = !active;
      settingsEndRental.disabled = !!workerAction || renting || startingQueue;
      settingsEndRental.textContent = workerAction === 'end' ? 'Ending rental…' : 'End rental';
    }
    el('start-job').hidden = !job || job.status !== 'ready';
    el('start-job').disabled = !!workerAction || renting || startingQueue || !job || job.status !== 'ready' || (!savedPreferences && !(worker && !['none','deletion_verified'].includes(worker.rental_state))) || !supervision;
    el('requeue-job').hidden = !job || !['failed','cancelled'].includes(job.status) || !job.can_delete;
    el('requeue-job').disabled = !!job && (requeuingJobs.has(job.id) || deletingJobs.has(job.id));
    el('requeue-job').textContent = job && requeuingJobs.has(job.id) ? 'Preparing…' : 'Requeue';
    const labels = {pause:'Pause queue', resume:'Start queue', end:'End rental', recover:'Resume supervision'};
    const pendingLabels = {pause:'Pausing…', resume:'Resuming…', end:'Ending rental…', recover:'Resuming supervision…'};
    Object.keys(labels).forEach(action => {
      const button = el(action + '-worker');
      button.disabled = !active || !!workerAction || startingQueue;
      button.textContent = workerAction === action ? pendingLabels[action] : labels[action];
    });
    el('pause-worker').disabled ||= !!queueState.paused;
    el('resume-worker').disabled ||= !queueState.paused;
    el('stop-job').textContent = stopPhase(job) || 'Stop & collect';
    el('stop-job').disabled = !!stopPhase(job) || !job || !['provisioning', 'uploading', 'running', 'collecting'].includes(job.status);
    el('recover-job').disabled = !job || !job.final_collected || (!!job.result_path && !job.result_partial);
    el('job-results').hidden = !job || !job.result_path;
  }

  function renderQueueOverview() {
    const banner = el('queue-rental');
    if (banner) {
      banner.hidden = false;
      const active = worker && !['none', 'deletion_verified'].includes(worker.rental_state);
      el('queue-rental-status').textContent = workerActivity() + (active && worker.awaiting_queue_start ? (queueState.paused ? ' · Reserved for queue start' : ' · Queue started — waiting for input preparation') : '');
      const start = el('queue-start-rental');
      if (start) {
        start.hidden = !active;
        start.disabled = !queueState.paused || !!workerAction || renting || startingQueue;
        start.textContent = startingQueue || workerAction === 'resume' ? 'Starting…' : 'Start queue';
      }
      const end = el('queue-end-rental');
      end.hidden = !active; end.disabled = !!workerAction || renting || startingQueue;
      end.textContent = workerAction === 'end' ? 'Ending rental…' : 'End rental';
    }
    if (window.state) window.state.cloudQueueCount = jobRows.length;
    if (typeof renderQueueMaybeDeferred === 'function') renderQueueMaybeDeferred();
    else if (typeof updateMetaCounts === 'function') updateMetaCounts();
  }

  async function refreshJobs(preferred) {
    const current = ++jobGeneration;
    try {
      const data = await request('/jobs');
      if (disposed || current !== jobGeneration) return;
      const selected = preferred || selectedJobId;
      const previousResults = new Map(jobRows.map(job => [job.id, [job.result_path, job.last_backup_at, job.result_partial].join('|')]));
      const resultsChanged = data.jobs.some(job => job.result_path && previousResults.get(job.id) !==
        [job.result_path, job.last_backup_at, job.result_partial].join('|'));
      jobRows = data.jobs; worker = data.worker; queueState = data.queue; supervision = data.supervision_available;
      for (const id of stoppingJobs.keys()) {
        const row = jobRows.find(item => item.id === id);
        if (!row || ['completed','failed','cancelled'].includes(row.status)) stoppingJobs.delete(id);
      }
      selectedJobId = jobRows.some(job => job.id === selected) ? selected : (jobRows[0]?.id || null);
      ['supervision-status', 'settings-supervision-status'].forEach(id => {
        const notice = el(id); if (!notice) return;
        notice.hidden = !!supervision;
        notice.textContent = supervision ? '' : 'GPU start unavailable: rental supervision requires a working user systemd manager and OpenSSH on the PBGui host.';
      });
      renderJob();
      const displayed = jobRows.find(row => window.state && row.id === window.state.cloudLogId);
      if (displayed) renderCloudDashboard(displayed);
      renderQueueOverview();
      if (resultsChanged && typeof refreshLiveResultsDuringRun === 'function') await refreshLiveResultsDuringRun(true);
    } catch (error) { if (!disposed && current === jobGeneration) message(error.message, true); }
  }

  async function pollJobs() {
    await refreshJobs();
    if (!disposed) jobTimer = setTimeout(pollJobs, 10000);
  }

  let validationGeneration = 0, validationTimer = null, cloudMetrics = null;
  function setQueueBlocked(blocked) {
    ['btn-cloud-save-queue', 'btn-editor-save-queue'].forEach(id => {
      const button = el(id);
      if (!button) return;
      button.dataset.queueBlocked = String(blocked);
      button.disabled = blocked || !!window.state?.editorSaving;
    });
  }
  function applyCloudAlternative(action, index) {
    if (!el('opted-execution') || el('opted-execution').value !== 'vast') return;
    if (action === 'local') {
      el('opted-execution').value = 'local';
      window.PBGuiVast.updateEditor();
      if (window.scheduleStructuredEditorSync) window.scheduleStructuredEditorSync();
      return;
    }
    if (action !== 'adg' || !Number.isInteger(index) || index < 0) return;
    if (window.state && window.state.scoringEditIndex >= 0) return;
    const entries = window.getScoringEntries();
    if (!entries[index] || entries[index].metric !== 'gain_strategy_eq') { scheduleValidation(); return; }
    entries[index] = {...entries[index], metric:'adg_strategy_eq'};
    window.setScoringEntries(entries, {preserveAddForm:true, preserveAddVisible:true});
    scheduleValidation();
  }

  function showValidation(errors, pending, report) {
    const box = el('opted-vast-validation');
    if (!box) return;
    box.setAttribute('aria-busy', String(!!pending));
    if (pending) return; // Keep layout, focus and expanded details while checking.
    document.querySelectorAll('.cloud-invalid').forEach(node => { node.classList.remove('cloud-invalid'); node.removeAttribute('title'); });
    if (!errors.length) {
      box.hidden = true; box.replaceChildren(); delete box.dataset.signature;
      return;
    }
    const editing = window.state && (window.state.scoringEditIndex >= 0 || window.state.limitEditIndex >= 0);
    const signature = JSON.stringify([errors, editing]);
    const unchanged = box.dataset.signature === signature;
    const expanded = !!(box.querySelector('details') && box.querySelector('details').open);
    const content = document.createDocumentFragment();
    box.hidden = false;
    box.classList.toggle('cloud-validation-error', !!errors.length);
    const heading = document.createElement('strong');
    heading.textContent = errors.some(error => error.path === 'validation') ? 'GPU validation unavailable' : 'Cloud configuration needs attention (' + errors.length + ')';
    heading.dataset.tip = 'Saving remains available. Resolve these errors before queueing. Market data is checked during preparation.';
    content.appendChild(heading);
    if (report) heading.dataset.tip += ' Worker PB8 ' + report.revision.slice(0, 7) + '.';
    const list = document.createElement('ul');
    const extra = document.createElement('details'), extraList = document.createElement('ul');
    if (errors.length > 6) { const summary = document.createElement('summary'); summary.textContent = (errors.length - 6) + ' more issues'; extra.appendChild(summary); extra.appendChild(extraList); }
    let localOffered = false;
    errors.forEach((error, index) => {
      const item = document.createElement('li'), issue = document.createElement('div'); issue.className = 'cloud-issue-title'; issue.textContent = error.message; item.appendChild(issue);
      (index < 6 ? list : extraList).appendChild(item);
      issue.dataset.tip = [error.path].concat(error.suggestions || []).join('\n');
      if (error.path) {
        const location = document.createElement('small');
        location.className = 'muted'; location.textContent = 'Field: ' + error.path;
        item.appendChild(location);
      }
      const actions = document.createElement('div'); actions.className = 'cloud-fix-actions';
      const addAction = (label, help, action, entryIndex, callback) => {
        const button = document.createElement('button'); button.type = 'button'; button.className = action === 'adg' ? 'btn primary' : 'btn ghost';
        button.textContent = label; issue.dataset.tip += '\n' + help;
        if (action !== 'local' && editing) button.disabled = true;
        button.addEventListener('click', callback || (() => applyCloudAlternative(action, entryIndex))); actions.appendChild(button);
        return button;
      };
      if (['validation', 'config'].includes(error.path)) {
        addAction('Retry validation', 'Check the current draft again. Queueing requires successful validation; Save remains available.', 'retry', null, scheduleValidation);
      }
      const objectiveMatch = /^optimize\.scoring\.(\d+)\.metric$/.exec(error.path);
      if (objectiveMatch && error.message.includes('gain_strategy_eq')) {
        addAction('Use ADG', 'Replace this objective with average daily growth in the draft. This changes the objective; it is not identical to total-period gain. Goal and scenario stay unchanged.', 'adg', Number(objectiveMatch[1]));
      }
      const metricMatch = /^optimize\.(scoring|limits)\.(\d+)\.metric$/.exec(error.path);
      if (metricMatch && error.message.includes('Unsupported cloud metrics')) {
        const group = metricMatch[1], entryIndex = Number(metricMatch[2]);
        if (group === 'scoring' && !error.message.includes('gain_strategy_eq')) {
          const label = document.createElement('label'), caption = document.createElement('span');
          caption.textContent = 'GPU metric';
          caption.dataset.tip = 'Choose a supported objective. Metrics have different meanings. Goal and scenario are preserved; check the goal after replacing.';
          const select = document.createElement('select'); select.className = 'panel-input';
          select.setAttribute('aria-label', 'GPU metric');
          ['', ...(cloudMetrics || [])].forEach(metric => {
            const option = document.createElement('option'); option.value = metric;
            option.textContent = metric || 'Choose metric'; select.appendChild(option);
          });
          label.append(caption, select); actions.appendChild(label);
          const apply = addAction('Replace metric', 'Replace only this scoring metric in the draft. This changes the objective.', 'replace', entryIndex, () => {
            if (editing || !cloudMetrics.includes(select.value)) return;
            const entries = window.getScoringEntries();
            if (!entries[entryIndex] || !error.message.includes(entries[entryIndex].metric)) { scheduleValidation(); return; }
            entries[entryIndex] = {...entries[entryIndex], metric:select.value};
            window.setScoringEntries(entries, {preserveAddForm:true, preserveAddVisible:true}); scheduleValidation();
          });
          apply.disabled = true;
          select.addEventListener('change', () => { apply.disabled = !!editing || !select.value; });
        }
        if (group === 'limits') {
          addAction('Edit limit', 'Select a supported metric and enter an appropriate threshold in the Limits editor. Threshold units are not interchangeable.', 'edit', entryIndex, () => {
            window.startLimitEdit(entryIndex); el('opted-limits-panel').scrollIntoView({block:'center'});
          });
          addAction('Remove limit', 'Remove this unsupported restriction from the draft. This relaxes the optimization constraints.', 'remove', entryIndex, () => {
            if (editing) return;
            const entries = window.getLimitEntries();
            if (!entries[entryIndex] || !error.message.includes(entries[entryIndex].metric)) { scheduleValidation(); return; }
            entries.splice(entryIndex, 1); window.setLimitEntries(entries, {preserveAddForm:true, preserveAddVisible:true}); scheduleValidation();
          });
        }
      }
      if (error.path === 'live.approved_coins') {
        addAction('Choose coins', 'Select 1–64 distinct approved coins across the long and short lists.', 'coin', null, () => {
          const target = el('ms-opt-app-long');
          if (target) { target.scrollIntoView({block:'center'}); const input = target.querySelector('input'); if (input) input.focus(); }
        });
      }
      if (!localOffered && Array.isArray(error.suggestions) && error.suggestions.some(text => text.includes('Execution → Local'))) {
        localOffered = true;
        addAction('Use Local', 'Switch the draft to local execution and keep the existing strategy and objectives.', 'local');
      }
      if (actions.childElementCount) item.appendChild(actions);
      const fields = {'live.strategy_kind':'opted-strategy-kind', 'optimize.iters':'opted-iters',
        'optimize.gpu.exact_workers':'opted-gpu-exact-workers', 'backtest.btc_collateral_cap':'opted-btc-collateral-cap',
        'optimize.objective_scenario':'opted-objective-scenario-name',
        'bot.long.hsl.enabled':'opted-runtime-long-hsl-enabled', 'bot.short.hsl.enabled':'opted-runtime-short-hsl-enabled',
        'optimize.gpu.successive_halving.enabled':'opted-gpu-halving-enabled'};
      const field = fields[error.path] && el(fields[error.path]);
      if (field) { field.classList.add('cloud-invalid'); field.title = error.message; }
      const match = /^optimize\.(scoring|limits)\.(\d+)/.exec(error.path);
      if (match) {
        const rows = document.querySelectorAll('#opted-' + match[1] + '-panel tbody tr');
        const row = rows[Number(match[2])];
        if (row) { row.classList.add('cloud-invalid'); row.title = error.message; }
      }
    });
    if (errors.length) content.appendChild(list);
    if (errors.length > 6) content.appendChild(extra);
    extra.open = expanded;
    if (!unchanged) { box.replaceChildren(content); box.dataset.signature = signature; }
  }
  async function validateEditorConfig(config) {
    const current = ++validationGeneration;
    clearTimeout(validationTimer); setQueueBlocked(true); showValidation([], true);
    try {
      const result = await request('/validate-config', {method:'POST', body:JSON.stringify({config})});
      if (disposed || current !== validationGeneration) return false;
      cloudMetrics = result.metrics; showValidation(result.errors, false, result); setQueueBlocked(!result.valid);
      return result.valid;
    } catch (error) {
      if (!disposed && current === validationGeneration) showValidation([{path:'validation',message:error.message}], false);
      return false;
    }
  }
  function scheduleValidation() {
    const current = ++validationGeneration; clearTimeout(validationTimer);
    const execution = el('opted-execution');
    if (!execution || execution.value !== 'vast') {
      setQueueBlocked(false);
      const box = el('opted-vast-validation'); if (box) box.hidden = true;
      document.querySelectorAll('.cloud-invalid').forEach(node => node.classList.remove('cloud-invalid'));
      return;
    }
    setQueueBlocked(true); showValidation([], true);
    validationTimer = setTimeout(() => {
      if (disposed || current !== validationGeneration) return;
      try {
        const config = window.collectEditorConfig ? window.collectEditorConfig(null, {strict:false}).config : window.state.editorLastConfig;
        validateEditorConfig(config);
      } catch (error) { showValidation([{path:'config', message:error.message}], false); }
    }, 350);
  }

  let deadlineChanging = false;
  let budgetChanging = false;
  let reserveChanging = false;
  async function adjustRentalDeadline(rental, minutes) {
    if (deadlineChanging || disposed) return;
    deadlineChanging = true;
    renderRentalDetails(rental, billingSnapshot);
    try {
      await request('/queue/deadline', {method:'POST', body:JSON.stringify({worker_id:rental.id, expected_deadline:rental.deadline, minutes})});
      await refreshJobs();
    } catch (error) {
      if (!disposed) { message(error.message, true); if (typeof toast === 'function') toast(error.message, 'err'); }
    } finally {
      deadlineChanging = false;
      const job = jobRows.find(row => window.state && row.id === window.state.cloudLogId);
      if (job && !disposed) renderRentalDetails(job.rental, billingSnapshot);
    }
  }

  async function adjustRentalBudget(rental, budget) {
    if (budgetChanging || disposed) return;
    budgetChanging = true;
    renderRentalDetails(rental, billingSnapshot);
    try {
      await request('/queue/budget', {method:'POST', body:JSON.stringify({worker_id:rental.id, expected_budget_usd:rental.budget_usd, budget_usd:budget})});
      await refreshJobs();
    } catch (error) {
      if (!disposed) { message(error.message, true); if (typeof toast === 'function') toast(error.message, 'err'); }
    } finally {
      budgetChanging = false;
      const job = jobRows.find(row => window.state && row.id === window.state.cloudLogId);
      if (job && !disposed) renderRentalDetails(job.rental, billingSnapshot);
    }
  }

  async function adjustRentalTransferReserve(rental, reserve) {
    if (reserveChanging || disposed) return;
    reserveChanging = true;
    renderRentalDetails(rental, billingSnapshot);
    try {
      await request('/queue/transfer-reserve', {method:'POST', body:JSON.stringify({worker_id:rental.id, expected_reserve_usd:rental.transfer_reserve_usd, reserve_usd:reserve})});
      await refreshJobs();
    } catch (error) {
      if (!disposed) { message(error.message, true); if (typeof toast === 'function') toast(error.message, 'err'); }
    } finally {
      reserveChanging = false;
      const job = jobRows.find(row => window.state && row.id === window.state.cloudLogId);
      if (job && !disposed) renderRentalDetails(job.rental, billingSnapshot);
    }
  }

  function renderRentalDetails(rental, billing) {
    const panel = el('optlog-rental');
    if (!panel) return;
    const sameRental = rental && panel.dataset.rentalId === rental.id;
    const focused = sameRental && panel.contains(document.activeElement) ? document.activeElement : null;
    const inputs = new Map(sameRental ? [...panel.querySelectorAll('input[aria-label]')].map(input => [input.getAttribute('aria-label'), input]) : []);
    function retainInput(input, label) {
      // Reuse the input node, including its unsaved value, across polling renders.
      const previous = inputs.get(label);
      if (!previous) { input.defaultValue = input.value; return input; }
      if (previous.value === previous.defaultValue || Number(previous.value) === Number(input.value)) {
        previous.value = input.value;
        previous.defaultValue = input.value;
      }
      return previous;
    }
    panel.hidden = !rental;
    panel.replaceChildren();
    panel.dataset.rentalId = rental ? rental.id || '' : '';
    if (!rental) return;
    const o = rental.offer || {};
    const fields = [
      ['GPU / VRAM', (o.gpu_name || '—') + ' · ' + fmt(o.vram_gb, 1) + ' GB', 'GPU and VRAM from the rented offer.'],
      ['Memory bandwidth', fmt(o.gpu_mem_bw_gbps, 0) + ' GB/s', 'Provider-reported GPU memory bandwidth, not live optimizer throughput.'],
      ['CPU / RAM', fmt(o.cpu_cores, 1) + ' cores · ' + fmt(o.ram_gb, 1) + ' GB', o.cpu_name || 'Allocated resources from the rented offer.'],
      ['Rental rate', '$' + fmt(o.price_hour_usd, 4) + '/hour', 'Hourly offer estimate including allocated disk. Transfers are charged separately; this is not an invoice.'],
      ['Network ↓ / ↑', fmt(o.inet_down_mbps, 0) + ' / ' + fmt(o.inet_up_mbps, 0) + ' Mbps', 'Provider-reported download / upload bandwidth; not current transfer speed.'],
      ['Transfer in / out', '$' + fmt(o.download_gb_usd, 4) + ' / $' + fmt(o.upload_gb_usd, 4) + ' per GB', 'Transfer prices from the rented offer, into / out of the GPU host.'],
      ['Disk', fmt(o.disk_gb, 0) + ' GB · ' + fmt(o.disk_bw_mbps, 0) + ' MB/s', o.disk_name || 'Allocated disk and provider-reported disk bandwidth.'],
      ['Host', (o.location || '—') + ' · ' + (o.reliability == null ? '—' : fmt(o.reliability * 100, 1) + '%'), 'Location and provider reliability. ' + (o.verified ? 'Verified host.' : 'Host not marked verified.')],
      ['PCIe', fmt(o.pci_gen, 0) + ' ×' + fmt(o.gpu_lanes, 0) + ' · ' + fmt(o.pcie_bw_gbps, 1) + ' GB/s', 'Provider-reported PCIe generation, lanes and bandwidth.'],
      ['Budget', '$' + fmt(rental.budget_usd, 2), 'Shared rental budget target. Changing it recalculates the deadline and requires worker acknowledgement. This is not a provider spending cap.'],
      ['Deadline', rental.deadline ? new Date(rental.deadline * 1000).toLocaleString() : '—', 'Confirmed deletion deadline. Enter an adjustment in minutes, then use −/+. Older rental guards are upgraded automatically when you request a change; the optimizer keeps running.'],
      ['Transfer reserve', '$' + fmt(rental.transfer_reserve_usd, 4), 'Amount retained inside the budget for uploading input and returning results. Increase it when a queued job reports insufficient transfer reserve; PBGui shortens the deadline by the required amount.']
    ];
    const parts = billing && billing.breakdown || {};
    fields.push(['Vast instance charges', billing && billing.amount_usd != null ? '$' + fmt(billing.amount_usd, 4) + (billing.error ? ' (last retrieved)' : '') : billing && billing.error ? 'Unavailable' : 'Pending',
      'Provider-reported charges for the entire rented instance, including other jobs sharing it. Reporting may lag behind usage.'
      + (billing && billing.reported_at ? ' Retrieved: ' + new Date(billing.reported_at * 1000).toLocaleString() + '.' : '')
      + (billing && billing.amount_usd != null ? ' GPU: $' + fmt(parts.gpu || 0, 4) + '; disk: $' + fmt(parts.disk || 0, 4)
        + '; transfer: $' + fmt((parts.bwd || 0) + (parts.bwu || 0), 4) + '.' : '')
      + (billing && billing.error ? ' Refresh failed: ' + billing.error : '')]);
    fields.forEach(([label, value, help]) => {
      const card = document.createElement('div'); card.className = 'optlog-card';
      const heading = document.createElement('div'); heading.className = 'optlog-label';
      const caption = document.createElement('span'); caption.textContent = label; caption.dataset.tip = help;
      heading.appendChild(caption);
      const content = document.createElement('div'); content.className = 'optlog-detail-text'; content.textContent = value;
      if (label === 'Host') {
        const identity = document.createElement('div');
        identity.textContent = o.machine_id ? 'Machine ' + o.machine_id : 'Machine ID will be resolved when blocking';
        content.append(identity, hostBlockButton(o.machine_id, rental.id), hostPreferenceControls(o.machine_id, rental.id));
      }
      if (label === 'Budget') {
        card.classList.add('optlog-rental-edit-card');
        const active = worker && worker.id === rental.id && worker.rental_state === 'active';
        let budgetInput = document.createElement('input');
        budgetInput.type = 'number'; budgetInput.className = 'deadline-minutes';
        budgetInput.min = '0.1'; budgetInput.max = '100'; budgetInput.step = '0.01'; budgetInput.value = String(rental.budget_usd);
        budgetInput.title = 'Budget target in USD'; budgetInput.setAttribute('aria-label', 'Budget target in USD');
        budgetInput = retainInput(budgetInput, 'Budget target in USD');
        const budgetButton = document.createElement('button'); budgetButton.type = 'button'; budgetButton.className = 'btn'; budgetButton.textContent = budgetChanging ? 'Saving…' : 'Save budget';
        budgetButton.title = rental.deadline_protocol !== 2 ? 'Requires the updated budget-control worker guard' : 'Save budget target and recalculate deadline';
        budgetButton.disabled = rental.deadline_pending || budgetChanging;
        budgetButton.addEventListener('click', () => {
          const value = Number(budgetInput.value);
          if (!Number.isFinite(value) || value < .1 || value > 100) { message('Choose a budget between $0.10 and $100.00', true); return; }
          adjustRentalBudget(rental, value);
        });
        const controls = document.createElement('span'); controls.className = 'optlog-rental-controls';
        controls.append('$', budgetInput, budgetButton);
        content.append(controls);
      }
      if (label === 'Deadline') {
        card.classList.add('optlog-rental-edit-card');
        const active = worker && worker.id === rental.id && worker.rental_state === 'active';
        let minutesInput = document.createElement('input');
        minutesInput.type = 'number'; minutesInput.className = 'deadline-minutes';
        minutesInput.min = '1'; minutesInput.max = '1440'; minutesInput.step = '1'; minutesInput.value = '60';
        minutesInput.title = 'Deadline adjustment in minutes';
        minutesInput.setAttribute('aria-label', 'Deadline adjustment in minutes');
        minutesInput = retainInput(minutesInput, 'Deadline adjustment in minutes');
        const deadlineControls = document.createElement('span'); deadlineControls.className = 'optlog-rental-controls';
        deadlineControls.append(minutesInput, ' min');
        for (const direction of [-1, 1]) {
          const button = document.createElement('button'); button.type = 'button'; button.className = 'btn';
          button.textContent = direction < 0 ? '−' : '+';
          button.title = rental.deadline_protocol !== 1 && rental.deadline_protocol !== 2 ? 'Requires an updated worker deadline guard' : (direction < 0 ? 'Shorten' : 'Extend') + ' deadline by entered minutes';
          button.setAttribute('aria-label', button.title);
          button.disabled = rental.deadline_pending || deadlineChanging;
          button.addEventListener('click', () => {
            const value = Number(minutesInput.value);
            if (!Number.isInteger(value) || value < 1 || value > 1440) {
              minutesInput.focus();
              message('Enter a whole number between 1 and 1,440 minutes', true);
              return;
            }
            adjustRentalDeadline(rental, direction * value);
          });
          deadlineControls.append(button);
        }
        content.append(deadlineControls);
        if (rental.deadline_pending || deadlineChanging) content.append(' Awaiting worker confirmation…');
        if (rental.deadline_error) content.append(' ' + rental.deadline_error);
      }
      if (label === 'Transfer reserve') {
        card.classList.add('optlog-rental-edit-card');
        const active = worker && worker.id === rental.id && worker.rental_state === 'active';
        let reserveInput = document.createElement('input');
        reserveInput.type = 'number'; reserveInput.className = 'deadline-minutes';
        reserveInput.min = '0.05'; reserveInput.max = '100'; reserveInput.step = '0.0001'; reserveInput.value = Number.isFinite(rental.transfer_reserve_usd) ? (Math.ceil(rental.transfer_reserve_usd * 10000) / 10000).toFixed(4) : '';
        reserveInput.title = 'Transfer reserve in USD'; reserveInput.setAttribute('aria-label', 'Transfer reserve in USD');
        reserveInput = retainInput(reserveInput, 'Transfer reserve in USD');
        const reserveButton = document.createElement('button'); reserveButton.type = 'button'; reserveButton.className = 'btn'; reserveButton.textContent = reserveChanging ? 'Saving…' : 'Save reserve';
        reserveButton.title = rental.deadline_protocol !== 2 ? 'Requires the updated budget-control worker guard' : 'Save transfer reserve and shorten deadline if needed';
        reserveButton.disabled = rental.deadline_pending || reserveChanging;
        reserveButton.addEventListener('click', () => {
          const value = Number(reserveInput.value);
          if (!Number.isFinite(value) || value < .05 || value > 100) { message('Choose a transfer reserve between $0.05 and $100.00', true); return; }
          adjustRentalTransferReserve(rental, value);
        });
        const controls = document.createElement('span'); controls.className = 'optlog-rental-controls';
        controls.append('$', reserveInput, reserveButton);
        content.append(controls);
      }
      if (['Budget', 'Deadline', 'Transfer reserve'].includes(label)) {
        const active = worker && worker.id === rental.id && worker.rental_state === 'active';
        card.addEventListener('click', event => {
          if (!event.target.closest('button')) return;
          let reason = '';
          if (!active) reason = 'This rental is not active.';
          else if (![1, 2].includes(rental.deadline_protocol)) reason = 'Worker is not ready for changes yet. Please try again after startup.';
          else if (rental.deadline_protocol === 1 && label !== 'Deadline') reason = 'This worker version does not support changing this value.';
          if (reason) {
            event.stopPropagation();
            if (typeof toast === 'function') toast(reason, 'err');
            else message(reason, true);
          }
        }, true);
      }
      card.append(heading, content); panel.appendChild(card);
    });
    if (focused && panel.contains(focused)) focused.focus({preventScroll:true});
  }

  function renderUtilization(job) {
    const queue = job.exact_queue || {};
    const age = Number.isFinite(queue.sampled_at) ? Math.max(0, Math.floor(Date.now()/1000 - queue.sampled_at)) : null;
    const active = ['running', 'collecting'].includes(job.status);
    el('cloud-exact-queue').textContent = active && Number.isInteger(queue.outstanding) && age !== null ?
      fmt(queue.outstanding, 0) + (age > 45 ? ' (last sample)' : '') : '—';
    el('cloud-exact-queue-age').textContent = active && age !== null ? 'Generation sample: ' + age + 's ago' : 'No current generation sample';
    const sample = job.runtime_metrics || {};
    el('cloud-utilization').hidden = !job.lease_id && !job.runtime_metrics;
    const fresh = sample.available && Date.now()/1000 - sample.sampled_at <= 45 && ['running','uploading','provisioning','collecting'].includes(job.status);
    const percent = value => fresh && Number.isFinite(value) ? fmt(value, 1) + '%' : '—';
    const memory = (used, total) => fresh && Number.isFinite(used) && total > 0 ?
      fmt(used/1073741824, 2) + ' / ' + fmt(total/1073741824, 2) + ' GiB · ' + fmt(100*used/total, 1) + '%' : '—';
    el('cloud-gpu-util').textContent = percent(sample.gpu_percent);
    el('cloud-cpu-util').textContent = percent(sample.cpu_percent);
    el('cloud-ram-util').textContent = memory(sample.ram_used_bytes, sample.ram_total_bytes);
    el('cloud-vram-util').textContent = memory(sample.vram_used_bytes, sample.vram_total_bytes);
    el('cloud-utilization').setAttribute('aria-label', fresh ? 'Live utilization' : 'Utilization currently unavailable');
  }

  function renderThroughput(job) {
    const sample = job.throughput;
    const terminal = ['completed', 'cancelled', 'failed'].includes(job.status);
    const age = sample && Number.isFinite(sample.sampled_at) ? Math.max(0, Math.floor(Date.now()/1000 - sample.sampled_at)) : null;
    const current = age !== null && (terminal || (['running', 'collecting'].includes(job.status) && age <= 90));
    const number = value => Number.isFinite(value) ? value.toLocaleString(undefined, {maximumFractionDigits:1}) : '—';
    const rate = value => current ? number(value) : '—';
    el('cloud-proxy-rate').textContent = rate(sample?.proxy_per_minute);
    el('cloud-exact-rate').textContent = rate(sample?.exact_per_minute);
    el('cloud-proxy-total').textContent = sample ? number(sample.proxy_total) + ' logged proxy evaluations' : '';
    el('cloud-exact-total').textContent = sample ? number(sample.exact_total) + ' logged exact evaluations' : '';
    el('cloud-proxy-exact').textContent = number(sample?.proxy_per_exact);
    const price = job.rental?.offer?.price_hour_usd;
    const efficiency = Number.isFinite(price) && price > 0 && Number.isFinite(sample?.exact_per_minute) ? sample.exact_per_minute * 60 / price : null;
    el('cloud-cost-efficiency').textContent = rate(efficiency);
    el('cloud-throughput-sample').textContent = !sample ? 'Waiting for timestamped optimizer counters.' :
      (terminal ? 'Last recorded interval' : !current ? 'Stale sample — rates unavailable' : 'Latest measured interval') +
      ' · ' + number(sample.window_seconds) + 's · sample ' + age + 's ago' +
      (sample.proxy_per_minute == null ? ' · waiting for at least 10 seconds between counters' : '');
  }

  function renderCloudDashboard(job) {
    if (!window.state || window.state.cloudLogId !== job.id) return;
    if (typeof renderOptimizeLogDashboard !== 'function') return;
    const uploadPhase = job.status === 'uploading' ? {preparing_files:'Preparing file synchronization', checking_cache:'Checking cache', packing:'Preparing transfer archive', installing:'Installing input data', verifying:'Verifying input data', reconnecting:'Waiting to retry upload'}[job.upload_progress?.stage] : null;
    renderOptimizeLogDashboard({name:job.config_name, phase:stopPhase(job) || uploadPhase || job.status,
      progress:{exact_evaluations:job.exact_completed || 0,target_exact_evaluations:job.iterations,
        proxy_evaluations:job.gpu_candidates || 0,percent:job.iterations ? 100*(job.exact_completed || 0)/job.iterations : 0},
      runtime:{backend:'gpu',algorithm:'Vast.ai',config_n_cpus:job.auto_cpu_workers && !job.cpu_allocation_resolved ? null : job.workers},
      process:{started_at:job.started_at ? new Date(job.started_at*1000).toISOString() : null},
      queue:{running:jobRows.filter(row => ['running','provisioning','uploading','collecting'].includes(row.status)).length,
        queued:jobRows.filter(row => row.status==='ready').length,error:jobRows.filter(row => row.status==='failed').length},
      log:{last_error:(worker && worker.creation_error) || job.error || job.log_error || job.cleanup_error || (worker && worker.cleanup_error) || '',last_line:workerActivity(),
        updated_at:job.updated_at ? new Date(job.updated_at*1000).toISOString() : null}});
    if (job.elapsed_seconds != null && ['completed','cancelled','failed'].includes(job.status)) {
      el('optlog-elapsed').textContent = formatDurationCompact(job.elapsed_seconds);
    }
    el('optlog-pareto').textContent = job.pareto_count == null ? '—' : Number(job.pareto_count).toLocaleString();
    if (job.auto_cpu_workers && !job.cpu_allocation_resolved) el('optlog-cpu').textContent = 'Auto (instance allocation)';
    renderUtilization(job);
    renderThroughput(job);
    const config = job.convergence_config || {}, history = job.convergence || {};
    const convergence = history.final || history;
    const terminal = ['completed', 'cancelled', 'failed'].includes(job.status);
    const finalChecked = !!history.final && convergence.phase !== 'waiting' && convergence.checked_exact != null;
    el('convergence-help').dataset.tip = 'First collect at least ' + (config.convergence_min_exact || 512) +
      ' exact CPU evaluations. Then stop and collect after ' + (config.convergence_patience || 512) +
      ' further exact evaluations without a Pareto hypervolume improvement greater than ' +
      (config.convergence_tolerance_pct ?? 0.1) +
      '%. Hypervolume measures the feasible front across all configured objectives on a fixed scale, not just the number of results. Small gains accumulate against the last accepted baseline. Checked after verified snapshots (normally about a minute plus transfer time). Zero means a baseline or significant improvement was recorded. This is not overall run progress.';
    el('cloud-convergence').hidden = !config.convergence_enabled;
    el('convergence-sample').hidden = !config.convergence_enabled;
    el('convergence-sample').textContent = convergence.checked_exact == null ? 'Waiting for the first verified result snapshot.' :
      'Checked through exact result ' + Number(convergence.checked_exact).toLocaleString() +
      (convergence.last_improvement_exact == null ? '' : ' · Last significant improvement at ' + Number(convergence.last_improvement_exact).toLocaleString()) +
      (finalChecked ? ' · Final snapshot checked (not a per-result replay)' : terminal ? ' · Final convergence check unavailable' :
        ' · ' + Math.max(0, (job.exact_completed || 0) - convergence.checked_exact).toLocaleString() + ' newer results awaiting snapshot check');
    if (terminal && !finalChecked && convergence.checked_exact == null) el('convergence-sample').textContent = 'Final convergence check unavailable';
    el('convergence-phase').textContent = job.completion_reason === 'convergence' ? 'Completed: no significant improvement' :
      job.completion_reason === 'rental_deadline' ? 'Stopped: rental time limit' :
      terminal && finalChecked ? (convergence.threshold_reached ? 'Final snapshot: stagnation threshold reached' : 'Final snapshot checked') :
      terminal ? 'Final convergence check unavailable' :
      convergence.reason || ({warming:'Minimum evaluations', tracking:'Monitoring', stopping:'Collecting results'}[convergence.phase] || 'Waiting for exact results');
    el('convergence-since').textContent = convergence.last_improvement_exact == null ? '—' :
      Math.max(0, (job.exact_completed || 0) - convergence.last_improvement_exact).toLocaleString() + ' exact evaluations';
    el('convergence-progress').textContent = convergence.phase === 'warming' ?
      (job.exact_completed || 0).toLocaleString() + ' / ' + Number(config.convergence_min_exact).toLocaleString() + ' minimum' :
      Number(convergence.stalled_exact || 0).toLocaleString() + ' / ' + Number(config.convergence_patience || 0).toLocaleString() + ' exact evaluations';
    el('convergence-bar').value = config.convergence_patience ? Math.min(100, 100 * (convergence.stalled_exact || 0) / config.convergence_patience) : 0;
    window.state.logResultName = job.result_path ? job.result_path.split('/').pop() : job.config_name;
    el('optlog-open-results').disabled = !job.result_path;
    el('optlog-open-pareto-explorer').disabled = !job.result_path;
    renderRentalDetails(job.rental, billingLease === job.lease_id ? billingSnapshot : null);
    if (job.lease_id && !billingPending && (billingLease !== job.lease_id || Date.now() >= billingNextCheck)) {
      billingPending = true;
      const lease = job.lease_id;
      request('/jobs/' + encodeURIComponent(job.id) + '/charges').then(data => {
        if (disposed) return;
        billingLease = lease; billingSnapshot = data.billing; billingNextCheck = Date.now() + 300000;
        const active = jobRows.find(row => window.state && row.id === window.state.cloudLogId && row.lease_id === lease);
        if (active) renderRentalDetails(active.rental, billingSnapshot);
      }).catch(error => {
        if (!disposed) {
          billingSnapshot = {...(billingLease === lease ? billingSnapshot : null), error:error.message};
          billingLease = lease; billingNextCheck = Date.now() + 60000;
          const active = jobRows.find(row => window.state && row.id === window.state.cloudLogId && row.lease_id === lease);
          if (active) renderRentalDetails(active.rental, billingSnapshot);
        }
      }).finally(() => { billingPending = false; });
    }
    const imageProgress = job.image_progress;
    el('optlog-progress-fill').classList.toggle('is-indeterminate', job.status === 'provisioning');
    if (job.status === 'provisioning') {
      const hasLayers = imageProgress && imageProgress.total > 0;
      const hasBytes = hasLayers && imageProgress.total_bytes > 0 && Number.isFinite(imageProgress.completed_bytes);
      const phase = !hasLayers ? 'Preparing worker' : imageProgress.ready === imageProgress.total ? 'Starting worker'
        : imageProgress.downloaded === imageProgress.total ? 'Extracting worker image' : 'Downloading worker image';
      const now = Date.now() / 1000;
      const started = job.dispatch_at || (worker && worker.id === job.lease_id ? worker.started_at : null);
      const elapsed = started ? Math.max(0, Math.floor(now - started)) : null;
      const fetched = Number(job.provider_log_fetched_at);
      const age = fetched > 0 ? Math.max(0, Math.floor(now - fetched)) : null;
      const duration = seconds => Math.floor(seconds / 60) + 'm ' + seconds % 60 + 's';
      const detail = phase + (elapsed == null ? '' : ' · ' + duration(elapsed))
        + (hasBytes ? ' · At least ' + fmt(imageProgress.completed_bytes / 1e9, 2) + ' / '
          + fmt(imageProgress.total_bytes / 1e9, 2) + ' GB available (' + fmt(imageProgress.download_percent, 1) + '%)' : '')
        + (hasLayers ? ' · Layers: ' + imageProgress.downloaded + '/' + imageProgress.total + ' downloaded, '
          + imageProgress.ready + '/' + imageProgress.total + ' ready' : '')
        + (job.has_provider_log ? ' · Host log (Extra Debug Logs)' : ' · Waiting for host log')
        + (age == null ? '' : ' · Fetched ' + duration(age) + ' ago');
      el('optlog-progress-fill').classList.toggle('is-indeterminate', !hasBytes);
      el('optlog-progress-fill').style.width = hasBytes ? imageProgress.download_percent + '%' : '100%';
      el('optlog-progress-label').textContent = detail;
      el('optlog-progress-label').dataset.tip = (hasBytes ? 'Compressed sizes from this pinned image manifest. Only completed or already cached layers count, so this is a lower bound; in-flight bytes are not estimated. This is image availability, not billed network traffic. 100% available still requires extraction and worker startup. ' : 'Layer sizes are unavailable for this image; the animated bar is not a byte percentage. ') + 'Fetch time is when PBGui retrieved the host log, not when the host produced a new line.';
      el('optlog-activity').textContent = detail;
      if (elapsed != null) el('optlog-elapsed').textContent = duration(elapsed);
    } else {
      delete el('optlog-progress-label').dataset.tip;
    }
    if (job.status === 'uploading') {
      const upload = job.upload_progress || {}, total = upload.total || job.transfer_input_bytes || job.input_bytes || 0;
      const rsync = upload.transport === 'rsync';
      const directFiles = rsync && upload.mode === 'files';
      const verified = upload.bytes === total && total > 0;
      const sent = rsync ? upload.transferred_bytes : upload.bytes;
      const checkingCache = upload.stage === 'checking_cache';
      const filesTotal = Number(upload.files_total), filesChecked = Number(upload.files_checked);
      const fileProgress = checkingCache && Number.isInteger(filesTotal) && filesTotal >= 0 && Number.isInteger(filesChecked) && filesChecked >= 0 && filesChecked <= filesTotal;
      const percent = checkingCache ? (fileProgress && filesTotal > 0 ? 100 * filesChecked / filesTotal : 0)
        : total && sent != null ? Math.min(100, 100 * sent / total) : 0;
      const stageLabel = {preparing_files:'Preparing file synchronization', checking_cache:'Checking cached input data', packing:'Preparing transfer archive', installing:'Installing input data', synchronized:'Input files synchronized'}[upload.stage];
      const retrySeconds = Math.max(0, Math.ceil((job.upload_retry_at || 0) - Date.now()/1000));
      const label = upload.stage === 'verifying' ? 'Verifying input data' : upload.stage === 'reconnecting' ?
        (job.upload_retry_at ? 'Upload retry in ' + retrySeconds + 's (partial data retained)' :
          rsync ? 'Reconnecting upload (partial data retained)' : 'Reconnecting upload (verified chunks retained)') : directFiles ? 'Synchronizing input files' : 'Uploading input';
      let eta = '', speed = '';
      const measured = Number(upload.bytes_per_second), network = Number(job.rental?.offer?.inet_down_mbps);
      const transferring = !stageLabel && upload.stage !== 'verifying' && upload.stage !== 'reconnecting';
      if (transferring) {
        if (Number.isFinite(measured) && measured > 0) speed = (directFiles ? ' · Rsync: ' : ' · Upload: ') + fmt(measured * 8 / 1e6, 1) + ' Mbps';
        if (Number.isFinite(network) && network > 0) {
          speed += ' · Host: ' + fmt(network, 0) + ' Mbps';
          if (Number.isFinite(measured) && measured > 0) speed += ' · ' + fmt(measured * 8 / 1e6 / network * 100, 1) + '% reached';
        }
        if (total > 0 && (sent == null || sent < total)) {
          const duration = rate => {
            const seconds = Math.max(1, Math.ceil((total - (sent || 0)) / rate));
            const minutes = Math.ceil(seconds / 60);
            return seconds < 60 ? seconds + ' s' : minutes < 60 ? minutes + ' min' : Math.floor(minutes / 60) + ' h ' + minutes % 60 + ' min';
          };
          if (Number.isFinite(measured) && measured > 0) eta += ' · Measured remaining: ~' + duration(measured);
          if (Number.isFinite(network) && network > 0) eta += ' · Host-rate remaining (theoretical): ~' + duration(network * 1e6 / 8);
        }
      }
      const cacheDetail = fileProgress ? 'Checking cached input data · ' + filesChecked.toLocaleString('en-US') + ' / ' + filesTotal.toLocaleString('en-US') + ' files checked (' + fmt(percent, 1) + '%)' : '';
      const detail = cacheDetail || stageLabel || label + ' · ' + (sent != null ? fmt(sent / 1e6, 1) + ' / ' : '') + fmt(total / 1e6, 1) + ' MB'
        + (sent != null ? (directFiles ? ' processed (' : rsync && !verified ? ' transferred (' : ' verified (') + fmt(percent, 1) + '%)' : '')
        + (upload.in_flight_bytes > 0 ? ' · current block: ' + fmt(upload.in_flight_bytes / 1e6, 2) + ' MB received' : '')
        + speed + eta;
      el('optlog-progress-fill').style.width = percent + '%';
      el('optlog-progress-label').textContent = detail;
      el('optlog-progress-label').dataset.tip = checkingCache ? 'Cache checking counts files acknowledged by the worker, before input transfer begins. Each confirmed page updates the counter; this is not uploaded bytes.' : 'Progress counts checksum-verified blocks. Current block bytes are acknowledged by the receiver and may be retried. Speed is verified data per second during this attempt. Remaining time uses remaining verified bytes divided by measured speed and excludes final verification/install. The theoretical remaining time uses the rented host download Mbps and is shown alongside the measured estimate. 1 byte = 8 bits. Local uplink, network route, SSH overhead and retries can limit throughput; the reached percentage alone does not prove an inaccurate host claim.';
      if (rsync) el('optlog-progress-label').dataset.tip = 'Rsync resumes the partial archive over one SSH connection. Progress and estimated speed come from rsync, excluding the retained prefix; buffered data may still be in transit. The complete archive is SHA256-verified before installation. Remaining time excludes this final check and installation. Host-rate remaining uses advertised download Mbps and is theoretical; local uplink and routing also limit speed. 1 byte = 8 bits.';
      if (directFiles) el('optlog-progress-label').dataset.tip = 'Rsync synchronizes individual immutable files over one SSH connection. Content-hash filenames and file sizes identify reusable data without reading it again. Partial files are retained separately and completed transfers are checked by rsync before publication. Progress reflects logical file bytes processed by rsync, not measured network traffic. The total includes reusable files, so remaining-time estimates can overstate the transfer. Host-rate remaining is theoretical. 1 byte = 8 bits.';
      el('optlog-activity').textContent = detail;
    }
    const rentalLog = worker && worker.has_provider_log && !['none','deletion_verified'].includes(worker.rental_state)
      && (job.lease_id === worker.id || (!job.lease_id && ['preparing','ready'].includes(job.status)));
    const optimizerLog = job.has_log ? 'optimizes_v8/vast_' + job.id + '.log'
      : job.has_provider_log ? 'optimizes_v8/vast_' + job.id + '_provider.log'
      : rentalLog ? 'optimizes_v8/vast_' + worker.id + '_provider.log' : '';
    if (!optimizerLog) {
      el('log-waiting').textContent = ({
        preparing:'Preparing local input data. No worker log exists yet.',
        ready:'Input ready. Waiting for the queue to dispatch this job.',
        provisioning:'Preparing the GPU worker. Waiting for its startup log.',
        uploading:'Transferring input data. The optimizer log becomes available after startup.',
        failed:'No log file is available. See Last error for the failure reason.',
        cancelled:'No log file was collected for this cancelled job.'
      })[job.status] || 'Waiting for this run’s log output.';
    }
    if (optimizerLog && window.state.logFile !== optimizerLog && typeof ensureLogViewer === 'function') {
      el('log-waiting').hidden = true;
      el('log-viewer-target').hidden = false;
      const viewer = ensureLogViewer(); viewer.open(); viewer.setHost('local');
      window.state.logFile = optimizerLog;
      viewer.setFile(optimizerLog);
    }
  }

  function openCloudLog(job) {
    if (typeof openLogPanel !== 'function') return;
    openLogPanel(job.id, job.config_name, {cloud:true,hasLog:!!job.has_log,hasProviderLog:!!job.has_provider_log});
    selectedJobId = job.id;
    renderJob();
    el('cloud-job-details').hidden = false;
    renderCloudDashboard(job);
  }

  function cloudQueueItems() {
    return jobRows.map(job => ({cloudJob:job, name:job.config_name,
      status:({ready:'queued',completed:'complete',failed:'error'})[job.status] || job.status,
      exchange:job.exchange || '', created:job.created_at ? new Date(job.created_at * 1000).toISOString() : ''}));
  }

  function cloudQueueRow(item) {
    const job = item.cloudJob, row = document.createElement('tr');
    row.dataset.cloudId = job.id;
    ['', item.name, item.exchange || '—', item.status,
      item.created ? new Date(item.created).toLocaleString() : '', 'Vast.ai'].forEach((value, index) => {
      const cell = document.createElement('td');
      if (index === 3) {
        const badge = document.createElement('span');
        badge.className = 'badge badge-' + item.status;
        badge.textContent = deletingJobs.has(job.id) ? 'Deleting…' : requeuingJobs.has(job.id) ? 'Preparing…' : stopPhase(job) || (startingJobId === job.id ? 'Starting…' : value); cell.appendChild(badge);
        const progress = job.input_progress || {};
        if (job.status === 'preparing') {
          const total = Number(progress.bytes_total), completed = Number(progress.bytes_completed);
          const filesTotal = Number(progress.files_total), filesCompleted = Number(progress.files_completed);
          const determinate = Number.isFinite(total) && total > 0 && Number.isFinite(completed);
          const percent = determinate ? Math.max(0, Math.min(100, 100 * completed / total)) : 0;
          const details = document.createElement('div'); details.className = 'cloud-prepare-progress';
          const track = document.createElement('div'); track.className = 'cloud-prepare-progress-track';
          track.setAttribute('role', 'progressbar'); track.setAttribute('aria-label', 'Preparing cloud input data');
          track.setAttribute('aria-valuemin', '0'); track.setAttribute('aria-valuemax', '100');
          if (determinate) track.setAttribute('aria-valuenow', String(Math.round(percent)));
          const fill = document.createElement('div'); fill.className = 'cloud-prepare-progress-fill';
          fill.style.width = determinate ? percent + '%' : '100%';
          if (!determinate) fill.classList.add('is-indeterminate');
          track.appendChild(fill);
          const label = document.createElement('span'); label.className = 'cloud-prepare-progress-label';
          label.textContent = progress.stage === 'compressing' ? 'Compressing input archive…' : determinate
            ? fmt(completed / 1e6, 1) + ' / ' + fmt(total / 1e6, 1) + ' MB · ' + Math.round(percent) + '%'
            : 'Selecting input files…';
          if (Number.isFinite(filesTotal) && filesTotal > 0) label.textContent += ' · ' + (Number.isFinite(filesCompleted) ? Math.max(0, filesCompleted) : 0) + ' / ' + filesTotal + ' files';
          details.append(track, label); cell.appendChild(details);
        }
      } else cell.textContent = value;
      row.appendChild(cell);
    });
    const actions = document.createElement('td'); actions.className = 'actions-cell';
    const button = (label, title, action) => {
      const node = document.createElement('button'); node.className = 'icon-btn';
      node.textContent = label; node.title = title;
      if (title === 'Start') {
        node.disabled = startingQueue || !!workerAction || !supervision || (!savedPreferences && !(worker && !['none','deletion_verified'].includes(worker.rental_state))) || deletingJobs.has(job.id);
        if (!supervision) node.dataset.tip = 'Start unavailable: rental supervision requires user systemd and OpenSSH on the PBGui host.';
      }
      if (['Requeue', 'Delete queue item'].includes(title)) node.disabled = requeuingJobs.has(job.id) || deletingJobs.has(job.id);
      if (title === 'Stop') node.disabled = !!stopPhase(job);
      node.addEventListener('click', event => { event.stopPropagation(); action(); });
      actions.appendChild(node);
    };
    if (item.status === 'queued') button('▶', 'Start', () => {
      selectedJobId = job.id; renderJob();
      startCloudQueue(job);
    });
    if (['error','cancelled'].includes(item.status) && job.can_delete) {
      button('↺', 'Requeue', () => requeueCloudJob(job));
    }
    if (!['queued','complete','error','cancelled'].includes(item.status)) button('⬛', 'Stop', () => {
      selectedJobId = job.id; renderJob(); el('stop-job').click();
    });
    if (job.has_log || job.has_provider_log || job.error || job.creation_error || job.cleanup_error || (worker && !['none','deletion_verified'].includes(worker.rental_state)) || ['provisioning','running','failed','error','completed'].includes(job.status)) button('📋', 'Open log', () => openCloudLog(job));
    if (typeof openConfigEditor === 'function') button('✏️', 'Edit config', () => {
      openConfigEditor(job.config_name).catch(handleError);
    });
    if (job.can_delete) button('🗑', 'Delete queue item', () => deleteCloudJob(job));
    if (job.can_delete) actions.lastElementChild.classList.add('danger');
    row.appendChild(actions); return row;
  }

  async function deleteCloudJob(job) {
    if (disposed || deletingJobs.has(job.id) || requeuingJobs.has(job.id)) return;
    deletingJobs.add(job.id); renderJob(); renderQueueOverview();
    try {
      if (!window.PBGuiDialogs || !window.PBGuiDialogs.confirm) throw new Error('Delete confirmation unavailable. Reload this page.');
      if (!await window.PBGuiDialogs.confirm({title:'Delete queue item',
        message:'Delete "' + job.config_name + '" from the queue?', confirmText:'Delete'})) return;
      if (disposed) return;
      await request('/jobs/' + encodeURIComponent(job.id), {method:'DELETE'});
      if (disposed) return;
      if (window.state && window.state.cloudLogId === job.id) closeLog();
      await refreshJobs();
    } catch (error) { if (!disposed) message(error.message, true); }
    finally {
      deletingJobs.delete(job.id);
      if (!disposed) { renderJob(); renderQueueOverview(); }
    }
  }

  window.PBGuiVast = {
    closeLog,
    queueItems: cloudQueueItems,
    queueRow: cloudQueueRow,
    scheduleValidation,
    validateConfig: validateEditorConfig,
    metricAllowed: function (metric) { return !!cloudMetrics && cloudMetrics.includes(metric); },
    hideSettings: function () { clearSecrets(); closeLog(); performanceView?.hide(); },
    showSettings: function () {
      if (!el('vast-performance').hidden) performanceView?.show();
    },
    openEditor: function (config) {
      const execution = el('opted-execution');
      if (execution) execution.value = config && config.pbgui && config.pbgui.execution === 'vast' ? 'vast' : 'local';
      this.updateEditor(); renderJob();
    },
    updateEditor: function () {
      const execution = el('opted-execution'), target = el('opted-vast-worker');
      if (target) target.hidden = !execution || execution.value !== 'vast';
      const cloud = !!execution && execution.value === 'vast';
      ['opted-n-cpus', 'opted-gpu-exact-workers'].forEach(id => {
        const input = el(id);
        const group = input?.closest('.form-group');
        if (!group) return;
        group.classList.toggle('cloud-cpu-auto', cloud);
        group.querySelectorAll('input, button').forEach(control => { control.disabled = cloud; });
        const help = group.querySelector('label [data-tip]');
        if (help) {
          help.toggleAttribute('data-tip-context', cloud);
          if (!Object.prototype.hasOwnProperty.call(help.dataset, 'localCpuTip')) {
            help.dataset.localCpuTip = help.dataset.tip;
          }
          help.dataset.tip = cloud
            ? 'Disabled because CPU allocation is automatic on Vast.ai. Set Min CPU cores in Settings → Vast GPU settings to choose the minimum rental capacity. The cloud run uses the effective CPU allocation available to the container, capped by the rented CPU quota. This local setting is not used.'
            : help.dataset.localCpuTip;
        }
      });
      const iterations = el('opted-iters');
      if (execution && execution.value === 'vast' && iterations
          && Object.prototype.hasOwnProperty.call(iterations.dataset, 'vastDefaultFrom')) {
        if (iterations.value === iterations.dataset.vastDefaultFrom) iterations.value = '20000';
        delete iterations.dataset.vastDefaultFrom;
      }
      scheduleValidation();
    },
    closeEditor: function () { validationGeneration++; clearTimeout(validationTimer); setQueueBlocked(false); },
    queue: async function (name, config) {
      const job = await request('/jobs/prepare', {method:'POST', body:JSON.stringify({config_name:name,
        iterations:Number(config.optimize.iters), workers:Number(config.optimize.gpu && config.optimize.gpu.exact_workers || config.optimize.n_cpus), use_adg:false})});
      if (!disposed) {
        // Publish the confirmed job immediately; a full refresh must not delay navigation.
        ++jobGeneration;
        jobRows = [job, ...jobRows.filter(row => row.id !== job.id)];
        selectedJobId = job.id;
        renderQueueOverview();
        void refreshJobs(job.id);
      }
      return job;
    }
  };
  if (el('opted-execution')) window.PBGuiVast.openEditor(window.state.editorLastConfig);
  function closeLog() {
    el('optlog-progress-fill')?.classList.remove('is-indeterminate');
    el('cloud-job-details').hidden = true;
    if (window.state) window.state.cloudLogId = null;
  }
  function showRequeuePending(identifier, pending) {
    document.querySelectorAll('tr[data-cloud-id]').forEach(row => {
      if (row.dataset.cloudId !== identifier) return;
      row.setAttribute('aria-busy', String(pending));
      row.querySelectorAll('button').forEach(button => {
        if (['Requeue', 'Delete queue item'].includes(button.title)) button.disabled = pending || deletingJobs.has(identifier);
      });
      const badge = row.querySelector('.badge');
      if (badge && pending) {
        badge.dataset.beforeRequeue = badge.textContent;
        badge.textContent = 'Preparing…';
      } else if (badge && badge.dataset.beforeRequeue != null) {
        badge.textContent = badge.dataset.beforeRequeue;
        delete badge.dataset.beforeRequeue;
      }
    });
  }
  async function requeueCloudJob(job) {
    if (disposed || !job || requeuingJobs.has(job.id) || deletingJobs.has(job.id)) return;
    requeuingJobs.add(job.id); renderJob(); renderQueueOverview();
    showRequeuePending(job.id, true);
    try {
      const replacement = await request('/jobs/' + encodeURIComponent(job.id) + '/requeue', {method:'POST'});
      await refreshJobs(replacement.id);
      if (!disposed && window.state && window.state.cloudLogId === job.id) openCloudLog(replacement);
    } catch (error) {
      if (!disposed) { message(error.message, true); await refreshJobs(); }
    } finally {
      requeuingJobs.delete(job.id);
      if (!disposed) {
        showRequeuePending(job.id, false);
        renderJob(); renderQueueOverview();
        if (typeof renderQueueMaybeDeferred === 'function') renderQueueMaybeDeferred();
      }
    }
  }
  el('requeue-job').addEventListener('click', () => requeueCloudJob(selectedJob()));
  async function startCloudQueue(job) {
    if (renting || startingQueue || workerAction || disposed || !supervision || (!savedPreferences && !(worker && !['none','deletion_verified'].includes(worker.rental_state))) || (job && deletingJobs.has(job.id))) return;
    startingQueue = true;
    startingJobId = job && job.id;
    document.querySelectorAll('tr[data-cloud-id]').forEach(row => {
      const start = row.querySelector('button[title="Start"]');
      if (start) start.disabled = true;
      if (row.dataset.cloudId === startingJobId) {
        const badge = row.querySelector('.badge');
        if (badge) badge.textContent = 'Starting…';
      }
    });
    renderJob();
    message('Starting GPU queue…');
    try {
      await request('/queue/start', {method:'POST', body:JSON.stringify({use_saved_settings:true, accept_rental_and_cleanup:true})});
      await refreshJobs(job && job.id);
      if (!disposed) message('');
    } catch (error) {
      if (!disposed) { message(error.message, true); if (typeof toast === 'function') toast(error.message, 'err'); }
    } finally {
      startingQueue = false; startingJobId = null;
      if (!disposed) {
        renderJob();
        document.querySelectorAll('tr[data-cloud-id]').forEach(row => {
          const start = row.querySelector('button[title="Start"]');
          if (start) start.disabled = !!workerAction || !supervision || (!savedPreferences && !(worker && !['none','deletion_verified'].includes(worker.rental_state)));
          const item = cloudQueueItems().find(item => item.cloudJob.id === row.dataset.cloudId);
          const badge = row.querySelector('.badge');
          if (item && badge) badge.textContent = item.status;
        });
        renderQueueOverview();
      }
    }
  }
  el('rent-offer').addEventListener('click', async () => {
    if (!selectedOffer || renting || startingQueue || workerAction || !supervision || disposed) return;
    if (!el('offers-form').reportValidity()) return;
    const offer = selectedOffer;
    renting = true; renderJob();
    try {
      await request('/queue/start', {method:'POST', body:JSON.stringify({
        rent_only:true, offer_id:offer.id, accept_rental_and_cleanup:true,
        preferences:{gpu_name:offer.gpu_name, max_price:offer.price_hour_usd,
          min_vram:offer.vram_gb ?? 0, min_ram:offer.ram_gb ?? 0, min_cpu:offer.cpu_cores ?? 0,
          min_tflops:offer.tflops ?? 0,
          disk_gb:offer.disk_gb ?? Number(el('disk').value), verified_only:!!offer.verified},
        hours:Number(el('job-hours').value), budget:Number(el('job-budget').value), idle_seconds:Number(el('worker-idle').value)
      })});
      await refreshJobs();
      if (!disposed) message('GPU rental started. Queued jobs remain paused until Start queue.');
    } catch (error) { if (!disposed) { message(error.message,true); await refreshJobs(); } }
    finally { renting=false; if (!disposed) { renderJob(); renderQueueOverview(); } }
  });
  el('settings-end-rental')?.addEventListener('click', () => el('end-worker')?.click());
  if (el('queue-start-rental')) el('queue-start-rental').addEventListener('click', () => el('resume-worker').click());
  if (el('queue-end-rental')) el('queue-end-rental').addEventListener('click', () => el('end-worker').click());
  el('start-job').addEventListener('click', () => startCloudQueue(selectedJob()));
  ['stop', 'recover'].forEach(action => el(action + '-job').addEventListener('click', async () => {
    const job = selectedJob(); if (!job) return;
    if (action === 'stop' && stopPhase(job)) return;
    const repaint = () => { if (!disposed) { renderJob(); renderCloudDashboard(job); renderQueueOverview(); } };
    if (action === 'stop') { stoppingJobs.set(job.id, 'Stopping…'); repaint(); }
    el(action + '-job').disabled = true;
    try {
      await request('/jobs/' + job.id + '/' + action, {method:'POST'});
      if (action === 'stop') { stoppingJobs.set(job.id, 'Stop requested'); repaint(); }
      await refreshJobs(job.id);
    }
    catch (error) {
      if (action === 'stop') stoppingJobs.delete(job.id);
      repaint();
      if (!disposed) message(error.message, true);
    }
    finally { if (!disposed) renderJob(); }
  }));
  ['pause', 'resume', 'end', 'recover'].forEach(action => el(action + '-worker').addEventListener('click', async () => {
    if (disposed || renting || workerAction || startingQueue) return;
    workerAction = action; renderJob(); renderQueueOverview();
    message(el(action + '-worker').textContent);
    try {
      await request('/queue/' + action, {method:'POST'});
      if (!disposed) { message(''); await refreshJobs(); }
    } catch (error) { if (!disposed) message(error.message, true); }
    finally {
      workerAction = null;
      if (!disposed) { renderJob(); renderQueueOverview(); }
    }
  }));
  if (new URLSearchParams(location.search).get('view') === 'queue' && window.setPanel) window.setPanel('queue');
  pollJobs();

  el('credentials-form').addEventListener('submit', event => saveSecret(event, 'api_key', 'api-key'));
  el('refresh-balance').addEventListener('click', refreshBalance);
  el('offers-form').addEventListener('submit', findOffers);
  el('block-machine-form').addEventListener('submit', event => {
    event.preventDefault();
    const machine = Number(el('block-machine-id').value);
    if (Number.isSafeInteger(machine) && machine > 0) changeHostBlock(machine, true);
  });
  el('mark-machine-form').addEventListener('submit', event => {
    event.preventDefault();
    const machine = Number(el('mark-machine-id').value);
    if (Number.isSafeInteger(machine) && machine > 0) changeHostPreference(machine, {[event.submitter?.dataset.mark || 'preferred']:true});
  });
  el('show-incompatible').addEventListener('change', () => { if (!disposed) el('offers-form').requestSubmit(); });
  document.addEventListener('visibilitychange', () => { if (document.hidden) clearSecrets(); });
  window.addEventListener('pagehide', () => { disposed = true; performanceView?.dispose(); validationGeneration++; clearTimeout(validationTimer); closeLog(); clearTimeout(jobTimer); jobGeneration++; generation++; accountGeneration++; offerGeneration++; clearSecrets(); controllers.forEach(controller => controller.abort()); });
  const initial = generation;
  refreshHostHistory();
  request('/settings').then(data => { if (!disposed && initial === generation) renderSettings(data); }).catch(error => { if (!disposed) message(error.message, true); });
})();
