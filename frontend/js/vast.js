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
  let workers = [];
  let worker = null, queueState = {}, selectedOffer = null, renting = false;
  let hostBlockBusy = false, offerRows = [];
  let hostProfiles = new Map(), hostGeneration = 0;
  let calibrationInfo = null, calibrationGeneration = 0, calibrationStarting = false, watchCancelling = false;
  let calibrationWorkerAvailable = false;
  let gpuRecommendation = null, gpuRecommendationGeneration = 0;

  const el = id => document.getElementById(id);
  function setCalibrationOptionsOpen(open) {
    el('calibration-options').hidden = !open;
    el('calibration-open').setAttribute('aria-expanded', String(open));
    const state = window.history.state;
    window.history.replaceState({
      ...(state && typeof state === 'object' ? state : {}),
      vastCalibrationOptionsOpen:open,
    }, '');
  }
  setCalibrationOptionsOpen(!!window.history.state?.vastCalibrationOptionsOpen);
  el('calibration-open').addEventListener('click', () => {
    setCalibrationOptionsOpen(el('calibration-options').hidden);
  });
  el('calibration-close').addEventListener('click', () => setCalibrationOptionsOpen(false));
  const calibrationDefaults = {
    small:{start:4096, step:4096, maximum:131072, gain:10, timeout:390},
    medium:{start:5632, step:512, maximum:12288, gain:5, timeout:3600},
    large:{start:5632, step:512, maximum:12288, gain:5, timeout:5400},
  };
  const calibrationFields = ['population', 'step', 'maximum', 'gain', 'timeout'];
  function calibrationSelection() {
    const preset = el('calibration-preset').value;
    const values = Object.fromEntries(calibrationFields.map(key =>
      [key === 'population' ? 'start' : key, Number(el('calibration-' + key).value)]));
    const defaultPlan = calibrationDefaults[preset];
    const legacy = preset === 'small' && defaultPlan && Object.keys(defaultPlan)
      .every(key => values[key] === defaultPlan[key]);
    return {preset, ...values, legacy};
  }
  function saveCalibrationSelection() {
    const state = window.history.state;
    window.history.replaceState({
      ...(state && typeof state === 'object' ? state : {}),
      vastCalibrationSelection:calibrationSelection(),
    }, '');
    renderCalibration();
  }
  const restoredCalibration = window.history.state?.vastCalibrationSelection;
  if (restoredCalibration && calibrationDefaults[restoredCalibration.preset]) {
    el('calibration-preset').value = restoredCalibration.preset;
    for (const key of calibrationFields) {
      const field = key === 'population' ? 'start' : key;
      if (Number.isFinite(Number(restoredCalibration[field]))) {
        el('calibration-' + key).value = String(restoredCalibration[field]);
      }
    }
  }
  el('calibration-preset').addEventListener('change', () => {
    const defaults = calibrationDefaults[el('calibration-preset').value];
    for (const key of calibrationFields) {
      el('calibration-' + key).value = defaults[key === 'population' ? 'start' : key];
    }
    saveCalibrationSelection();
  });
  calibrationFields.forEach(key => el('calibration-' + key).addEventListener('change', saveCalibrationSelection));
  const savedSettingsDetails = el('offer-saved-settings');
  savedSettingsDetails.open = !!window.history.state?.vastOfferSettingsOpen;
  savedSettingsDetails.addEventListener('toggle', () => {
    const state = window.history.state;
    window.history.replaceState({
      ...(state && typeof state === 'object' ? state : {}),
      vastOfferSettingsOpen: savedSettingsDetails.open,
    }, '');
  });
  function idlePolicyLabel(seconds) {
    const value = Number(seconds);
    if (value < 0) return 'keep idle GPUs until their rental deadline';
    if (!value) return 'delete immediately when idle';
    if (value >= 3600 && value % 3600 === 0) return 'delete after ' + (value / 3600) + ' idle hour' + (value === 3600 ? '' : 's');
    return 'delete after ' + (value / 60) + ' idle minutes';
  }
  const settingsSidebar = el('vast-sidebar-controls');
  if (settingsSidebar) {
    settingsSidebar.appendChild(el('vast-settings-nav'));
  }
  const performanceView = window.PBGuiVastPerformance?.create({request});
  let settingsView = null, settingsVisible = false, accountConfigured = null, firstVastVisit = true;
  const viewTitles = {offers:'GPU & Offers', rental:'Rental & Automation', hosts:'Hosts', performance:'Performance History', account:'Account'};
  function showSettings(view) {
    if (!Object.hasOwn(viewTitles, view)) view = settingsView || (accountConfigured === false ? 'account' : 'offers');
    if (firstVastVisit && view === 'offers' && accountConfigured === false) view = 'account';
    firstVastVisit = false;
    settingsView = view; settingsVisible = true;
    el('vast-offers').hidden = view !== 'offers';
    el('vast-rental').hidden = view !== 'rental';
    el('vast-setup').hidden = view !== 'account';
    el('vast-hosts-filter').hidden = view !== 'hosts';
    el('vast-known-hosts').hidden = view !== 'hosts' || el('host-status-filter').value === 'blocked';
    el('vast-blocked-hosts').hidden = view !== 'hosts' || !['all','blocked'].includes(el('host-status-filter').value);
    el('vast-performance').hidden = view !== 'performance';
    el('vast-performance-actions').hidden = view !== 'performance';
    if (el('vast-page-title')) el('vast-page-title').textContent = viewTitles[view];
    if (window.state?.panel === 'vast') { window.state.vastView = view; location.hash = 'vast-' + view; }
    el('vast-settings-nav').querySelectorAll('[data-vast-view]').forEach(button => {
      const active = button.dataset.vastView === view;
      button.classList.toggle('active', active); button.setAttribute('aria-pressed', String(active));
    });
    if (view === 'performance') performanceView?.show(); else performanceView?.hide();
    if (view !== 'account') clearSecrets();
  }
  el('vast-settings-nav').querySelectorAll('[data-vast-view]').forEach(button => {
    button.setAttribute('aria-pressed', 'false');
    button.addEventListener('click', () => {
      if (window.setPanel) window.setPanel('vast', button.dataset.vastView);
      else showSettings(button.dataset.vastView);
    });
  });
  el('host-status-filter').addEventListener('change', () => { renderKnownHosts(); showSettings('hosts'); });
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
  const workerActions = new Map();
  let workerAction = null;
  function stopPhase(job) {
    if (!job || ['completed','cancelled','failed'].includes(job.status)) return null;
    if (job.status === 'collecting') return 'Collecting results…';
    return stoppingJobs.get(job.id) || (job.stop_requested ? 'Stop requested' : null);
  }
  function currentObservedOptimizer(item) {
    const observed = item?.observed_optimizer;
    const sampledAt = Number(observed?.sampled_at);
    return observed?.running && Number.isFinite(sampledAt) && Date.now() / 1000 - sampledAt <= 45 ? observed : null;
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
    accountConfigured = !!data.configured;
    if (!accountConfigured && settingsVisible && settingsView === 'offers') showSettings('account');
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
    calibrationInfo = null; calibrationGeneration++;
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
      secondary(row.cells[0], 'Vast power limit: ' + (Number(offer.gpu_max_power_watts) > 0
        ? fmt(offer.gpu_max_power_watts, 0) + ' W' : 'not reported'));
      row.cells[0].title = 'GPU compute capacity and advertised power limit reported by Vast. The actual limit is checked after rental; optimizer speed also depends on CPU and memory.';
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
        calibrationInfo = null;
        row.classList.add('selected'); row.setAttribute('aria-selected', 'true');
        el('selection').hidden = false;
        el('selection').textContent = offer.gpu_name + ' · ' + offer.location + ' · $' + fmt(offer.price_hour_usd, 4) + '/hour · not reserved. Rent starts billing immediately.';
        renderJob();
        void refreshCalibration();
        if (el('opted-gpu-apply-measured')) scheduleValidation();
      };
      row.addEventListener('click', select);
      row.addEventListener('keydown', event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); select(); } });
      body.appendChild(row); body.appendChild(detailsRow);
    });
  }

  function calibrationOffer(offer) {
    return {id:Number(offer.id), machine_id:Number(offer.machine_id), gpu_name:String(offer.gpu_name || ''),
      vram_gb:Number(offer.vram_gb), gpu_mem_bw_gbps:offer.gpu_mem_bw_gbps == null ? null : Number(offer.gpu_mem_bw_gbps),
      tflops:offer.tflops == null ? null : Number(offer.tflops),
      price_hour_usd:Number(offer.price_hour_usd),
      gpu_max_power_watts:offer.gpu_max_power_watts == null ? null : Number(offer.gpu_max_power_watts)};
  }

  function rentalGpuProfileSource(profile) {
    if (!profile) return 'Checking the selected card profile…';
    const source = profile.source === 'local' ? 'Local GPU measurement'
      : profile.source === 'pbgui' ? 'PBGui measurement' : 'Hardware fallback (no usable measurement)';
    const power = Number(profile.profile_power_limit_watts);
    const actual = Number(profile.card_power_limit_watts);
    const reference = power > 0 ? ' · profile ' + fmt(power, 0) + ' W' : '';
    const card = actual > 0 ? ' · card ' + fmt(actual, 0) + ' W' : '';
    const match = profile.match_type === 'preliminary_nearest' ? ' · nearest advertised power' : '';
    const verified = profile.runtime_verified ? ' · runtime verified' : ' · preliminary Vast offer';
    const floor = profile.work_limit_is_floor
      ? ' · work limit is a base value; Auto may raise it for a larger job' : '';
    return source + reference + card + match + verified + floor;
  }

  function setRentalGpuFields(prefix, values) {
    el(prefix + '-gpu-population').value = values?.population_size ?? '';
    el(prefix + '-gpu-batch').value = values?.batch_size ?? '';
    el(prefix + '-gpu-work-limit').value = values?.max_dispatch_candidate_bars ?? '';
  }

  function rentalGpuFields(prefix) {
    const population_size = Number(el(prefix + '-gpu-population').value);
    const batch_size = Number(el(prefix + '-gpu-batch').value);
    const max_dispatch_candidate_bars = Number(el(prefix + '-gpu-work-limit').value);
    if (!Number.isSafeInteger(population_size) || population_size < 1024 || population_size > 131072
        || !Number.isSafeInteger(batch_size) || batch_size < 1 || batch_size > population_size
        || !Number.isSafeInteger(max_dispatch_candidate_bars)
        || max_dispatch_candidate_bars < 1 || max_dispatch_candidate_bars > 1e12) {
      throw new Error('Enter a population of 1,024–131,072, a batch up to that population, and a positive work limit up to 1 trillion.');
    }
    return {population_size, batch_size, max_dispatch_candidate_bars};
  }

  const offerJobSelections = new Map(), activeJobSelections = new Map();
  function gpuNumber(value) {
    return Number.isFinite(Number(value)) && Number(value) > 0
      ? Number(value).toLocaleString() : '—';
  }
  function gpuWorkBillions(value) {
    const bars = Number(value);
    return Number.isSafeInteger(bars) && bars > 0
      ? (bars / 1e9).toLocaleString(undefined, {maximumFractionDigits:9}) + ' billion candidate-bars' : '—';
  }
  function validatedGpuTriple(values) {
    const population_size = Number(values?.population_size);
    const batch_size = Number(values?.batch_size);
    const max_dispatch_candidate_bars = Number(values?.max_dispatch_candidate_bars);
    if (!Number.isSafeInteger(population_size) || population_size < 1024 || population_size > 131072
        || !Number.isSafeInteger(batch_size) || batch_size < 1 || batch_size > population_size
        || !Number.isSafeInteger(max_dispatch_candidate_bars)
        || max_dispatch_candidate_bars < 1 || max_dispatch_candidate_bars > 1e12) {
      throw new Error('Enter a population of 1,024–131,072, a batch up to that population, and a positive work limit up to 1 trillion.');
    }
    return {population_size, batch_size, max_dispatch_candidate_bars};
  }
  function gpuJobOverrides(previews, selections) {
    const result = {};
    for (const row of previews?.jobs || []) {
      const selection = selections.get(row.id);
      if (row.mode !== 'auto' || !selection || selection.mode === 'auto') continue;
      const measured = selection.mode.startsWith('measured:')
        ? row.measurements?.[Number(selection.mode.slice(9))] : null;
      if (selection.mode.startsWith('measured:') && !measured) {
        throw new Error('The selected measurement is no longer available for ' + row.name + '.');
      }
      result[row.id] = validatedGpuTriple(measured || selection.values);
    }
    return result;
  }
  function renderGpuJobPreviews(containerId, previews, selections, locked, onEdit, force = false) {
    const container = el(containerId);
    const stamp = JSON.stringify({previews, locked});
    if (!force && container.dataset.gpuStamp === stamp) return;
    container.dataset.gpuStamp = stamp;
    container.replaceChildren();
    if (!previews) { container.textContent = 'Checking prepared queue jobs…'; return; }
    const rows = previews.jobs || [];
    if (!rows.length) {
      container.textContent = 'No prepared optimizer jobs are waiting. A later job will use Auto sizing when it starts.';
      return;
    }
    for (const row of rows) {
      const card = document.createElement('article'); card.className = 'rental-gpu-job';
      const title = document.createElement('strong'); title.textContent = row.name; card.appendChild(title);
      if (row.error) {
        const issue = document.createElement('p'); issue.className = 'muted';
        issue.textContent = row.error; card.appendChild(issue); container.appendChild(card); continue;
      }
      const workload = document.createElement('p'); workload.className = 'muted';
      workload.textContent = 'Total across scenarios: ' + gpuNumber(row.estimated_coin_candles)
        + ' candles/candidate · largest scenario: ' + gpuNumber(row.candidate_bars_per_largest_scenario)
        + ' candidate-bars/candidate';
      card.appendChild(workload);
      const choices = row.measurements || [];
      const state = selections.get(row.id) || {mode:'auto', values:{...row.auto}};
      if (row.mode !== 'auto') {
        const fixed = document.createElement('p');
        fixed.textContent = 'Config sizing (not overridden): population ' + gpuNumber(row.auto?.population_size)
          + ' · batch ' + gpuNumber(row.auto?.batch_size)
          + ' · work limit ' + gpuWorkBillions(row.auto?.max_dispatch_candidate_bars);
        card.appendChild(fixed); container.appendChild(card); continue;
      }
      const selector = document.createElement('select');
      selector.setAttribute('aria-label', 'GPU setting for ' + row.name);
      const options = [{value:'auto', label:'Auto · calculated for this job'}];
      choices.forEach((choice, index) => {
        options.push({value:'measured:' + index,
          label:'Measured ' + gpuNumber(choice.measured_power_limit_watts) + ' W · population '
            + gpuNumber(choice.population_size) + ' · ' + choice.candidates_per_second + ' candidates/s'});
      });
      options.push({value:'custom', label:'Custom for this job'});
      for (const option of options) {
        const node = document.createElement('option'); node.value = option.value;
        node.textContent = option.label; selector.appendChild(node);
      }
      if (!options.some(option => option.value === state.mode)) state.mode = 'auto';
      selector.value = state.mode; selector.disabled = locked;
      selector.addEventListener('change', () => {
        state.mode = selector.value;
        if (state.mode === 'custom' && !state.values) state.values = {...row.auto};
        selections.set(row.id, state);
        onEdit();
        renderGpuJobPreviews(containerId, previews, selections, locked, onEdit, true);
      });
      card.appendChild(selector);
      const selectedMeasured = state.mode.startsWith('measured:')
        ? choices[Number(state.mode.slice(9))] : null;
      const values = state.mode === 'custom' ? (state.values || row.auto)
        : selectedMeasured || row.auto;
      if (state.mode === 'custom') {
        const fields = document.createElement('div'); fields.className = 'fields';
        for (const [key, labelText] of [
          ['population_size','Population'], ['batch_size','Batch size'],
          ['max_dispatch_candidate_bars','Work limit (billions of candidate-bars)']]) {
          const label = document.createElement('label'); label.textContent = labelText;
          const isWorkLimit = key === 'max_dispatch_candidate_bars';
          const input = document.createElement('input'); input.type = 'number';
          input.min = isWorkLimit ? '0.000001' : key === 'population_size' ? '1024' : '1';
          input.max = isWorkLimit ? '1000' : '131072';
          input.step = isWorkLimit ? '0.000001' : '1';
          input.value = isWorkLimit ? (Number(values?.[key]) / 1e9 || '') : (values?.[key] ?? '');
          input.disabled = locked;
          input.addEventListener('input', () => {
            const parsed = isWorkLimit ? Math.round(Number(input.value) * 1e9) : input.value;
            state.values = {...(state.values || row.auto), [key]: input.value === '' ? '' : parsed};
            selections.set(row.id, state); onEdit();
          });
          label.appendChild(input); fields.appendChild(label);
        }
        card.appendChild(fields);
      } else {
        const summary = document.createElement('p'); summary.className = 'rental-gpu-values';
        summary.textContent = 'Population ' + gpuNumber(values?.population_size)
          + ' · batch ' + gpuNumber(values?.batch_size)
          + ' · work limit ' + gpuWorkBillions(values?.max_dispatch_candidate_bars);
        card.appendChild(summary);
      }
      if (selectedMeasured) {
        const evidence = document.createElement('p'); evidence.className = 'muted';
        const offeredWatts = Number(selectedMeasured.offered_power_limit_watts);
        const delta = offeredWatts > 0 ? offeredWatts - Number(selectedMeasured.measured_power_limit_watts) : 0;
        evidence.textContent = 'Exact workload measurement at ' + gpuNumber(selectedMeasured.measured_power_limit_watts)
          + ' W' + (Number.isFinite(delta) && delta !== 0
            ? ' · selected offer advertises ' + gpuNumber(selectedMeasured.offered_power_limit_watts) + ' W (not the same power limit)' : '')
          + ' · peak VRAM ' + gpuNumber(selectedMeasured.peak_vram_gib) + ' GiB. Runtime performance may differ.';
        card.appendChild(evidence);
      }
      const bars = Number(row.candidate_bars_per_largest_scenario);
      const population = Number(values?.population_size), batch = Number(values?.batch_size);
      const cap = Number(values?.max_dispatch_candidate_bars);
      if (bars > 0 && population > 0 && batch > 0 && cap > 0) {
        const effective = Math.min(population, batch, Math.max(1, Math.floor(cap / bars)));
        const dispatch = document.createElement('p'); dispatch.className = 'muted';
        dispatch.textContent = 'Largest scenario: estimated ' + Math.ceil(population / effective)
          + ' dispatch' + (Math.ceil(population / effective) === 1 ? '' : 'es')
          + ' using a conservative calendar estimate; one full batch needs '
          + gpuWorkBillions(population * bars) + '.';
        card.appendChild(dispatch);
      }
      container.appendChild(card);
    }
    if (previews.truncated) {
      const note = document.createElement('p'); note.className = 'muted';
      note.textContent = 'Only the first 100 waiting jobs are shown; remaining jobs keep Auto sizing.';
      container.appendChild(note);
    }
  }
  function restoreGpuJobSelections(selections, previews, overrides) {
    selections.clear();
    for (const row of previews?.jobs || []) {
      const chosen = overrides?.[row.id];
      if (!chosen || row.mode !== 'auto') continue;
      const index = (row.measurements || []).findIndex(measured =>
        ['population_size','batch_size','max_dispatch_candidate_bars'].every(key => Number(measured[key]) === Number(chosen[key])));
      selections.set(row.id, {mode:index >= 0 ? 'measured:' + index : 'custom', values:{...chosen}});
    }
  }

  function setRentalGpuFieldMode(prefix, custom, locked) {
    ['population', 'batch', 'work-limit'].forEach(key => {
      el(prefix + '-gpu-' + key).disabled = !custom || !!locked;
    });
  }

  let offerGpuProfileId = null;
  function renderOfferGpuProfile() {
    const panel = el('offer-gpu-profile');
    panel.hidden = !selectedOffer;
    if (!selectedOffer) { offerGpuProfileId = null; offerJobSelections.clear(); return; }
    if (offerGpuProfileId !== selectedOffer.id) {
      offerGpuProfileId = selectedOffer.id;
      offerJobSelections.clear();
      el('offer-gpu-custom').checked = false;
      setRentalGpuFields('offer', null);
    }
    const preview = calibrationInfo?.rental_profile;
    el('offer-gpu-profile-source').textContent = preview
      ? 'Preliminary Vast offer · ' + (Number(preview.card_power_limit_watts) > 0
        ? gpuNumber(preview.card_power_limit_watts) + ' W advertised · ' : '')
        + 'Auto values below are calculated separately for each frozen queue job.'
      : 'Checking queued jobs for the selected offer…';
    el('offer-gpu-custom').disabled = !preview;
    if (preview && !el('offer-gpu-custom').checked) setRentalGpuFields('offer', preview);
    setRentalGpuFieldMode('offer', el('offer-gpu-custom').checked, !preview);
    renderGpuJobPreviews('offer-gpu-jobs', calibrationInfo?.queued_gpu_previews,
                         offerJobSelections, !calibrationInfo, () => {});
  }

  let activeGpuProfile = null, activeGpuWorkerId = null, activeGpuStamp = null;
  let activeGpuDirty = false, activeGpuGeneration = 0, activeGpuSaving = false;
  function renderActiveGpuProfile() {
    const panel = el('active-rental-gpu-profile');
    panel.hidden = !activeGpuWorkerId;
    if (!activeGpuWorkerId) return;
    const record = activeGpuProfile;
    const preview = record?.preview;
    el('active-gpu-profile-source').textContent = preview
      ? 'Reserved GPU · ' + (Number(preview.card_power_limit_watts) > 0
        ? gpuNumber(preview.card_power_limit_watts) + ' W · ' : '')
        + 'settings below apply to each waiting job.'
        + (record.override ? ' Saving per-job choices replaces the earlier card-wide override.' : '')
      : 'Checking queued jobs for this rental…';
    if (record && !activeGpuDirty) {
      el('active-gpu-custom').checked = !!record.override;
      setRentalGpuFields('active', record.override || preview);
      restoreGpuJobSelections(activeJobSelections, record.queued_gpu_previews, record.job_overrides);
    }
    const locked = !record?.can_edit || activeGpuSaving;
    el('active-gpu-custom').disabled = locked;
    setRentalGpuFieldMode('active', el('active-gpu-custom').checked, locked);
    el('active-gpu-save').disabled = locked || !activeGpuDirty;
    renderGpuJobPreviews('active-gpu-jobs', record?.queued_gpu_previews, activeJobSelections,
                         locked, () => { activeGpuDirty = true;
                           el('active-gpu-save').disabled = false; renderQueueOverview(); });
  }

  async function refreshActiveGpuProfile() {
    const item = workers.find(row => row.awaiting_queue_start
      && !['none', 'deletion_verified'].includes(row.rental_state));
    if (!item) {
      activeGpuWorkerId = null; activeGpuProfile = null; activeGpuStamp = null; activeJobSelections.clear();
      activeGpuDirty = false; activeGpuGeneration++; renderActiveGpuProfile(); return;
    }
    if (activeGpuWorkerId !== item.id) {
      activeGpuWorkerId = item.id; activeGpuProfile = null; activeGpuStamp = null; activeJobSelections.clear();
      activeGpuDirty = false; renderActiveGpuProfile();
    }
    const waitingIds = jobRows.filter(row => row.status === 'ready' && !['worker', 'calibration'].includes(row.kind))
      .map(row => row.id).sort().join(',');
    const stamp = item.id + ':' + item.generation + ':' + waitingIds;
    if (stamp === activeGpuStamp && activeGpuProfile) return;
    const current = ++activeGpuGeneration;
    try {
      const record = await request('/queue/rental-gpu-profile/' + encodeURIComponent(item.id));
      if (disposed || current !== activeGpuGeneration || activeGpuWorkerId !== item.id) return;
      activeGpuProfile = record; activeGpuStamp = stamp; renderActiveGpuProfile();
    } catch (error) {
      if (disposed || current !== activeGpuGeneration || activeGpuWorkerId !== item.id) return;
      activeGpuStamp = null;
      el('active-gpu-profile-source').textContent = error.message;
    }
  }

  el('offer-gpu-custom').addEventListener('change', () => renderOfferGpuProfile());
  el('active-gpu-custom').addEventListener('change', () => {
    activeGpuDirty = true;
    if (el('active-gpu-custom').checked && !activeGpuProfile?.override) {
      setRentalGpuFields('active', activeGpuProfile?.preview);
    }
    renderActiveGpuProfile(); renderQueueOverview();
  });
  ['population', 'batch', 'work-limit'].forEach(key => {
    el('active-gpu-' + key).addEventListener('input', () => {
      activeGpuDirty = true;
      renderActiveGpuProfile(); renderQueueOverview();
    });
  });
  el('active-gpu-save').addEventListener('click', async () => {
    if (!activeGpuWorkerId || !activeGpuProfile?.can_edit || activeGpuSaving) return;
    let jobProfiles;
    try { jobProfiles = gpuJobOverrides(activeGpuProfile.queued_gpu_previews, activeJobSelections); }
    catch (error) { message(error.message, true); return; }
    activeGpuSaving = true; renderActiveGpuProfile();
    try {
      await request('/queue/rental-gpu-profile/' + encodeURIComponent(activeGpuWorkerId), {
        method:'POST', body:JSON.stringify({profile:null, job_profiles:jobProfiles})
      });
      activeGpuDirty = false; activeGpuStamp = null;
      el('active-gpu-jobs').dataset.gpuStamp = '';
      await refreshJobs();
      if (!disposed) message('GPU profile saved for this rental. Queue jobs have not started.');
    } catch (error) { if (!disposed) message(error.message, true); }
    finally { activeGpuSaving = false; if (!disposed) renderActiveGpuProfile(); }
  });

  function formatElapsedSeconds(value) {
    const seconds = Math.max(0, Math.round(Number(value) || 0));
    if (seconds < 60) return seconds + 's';
    const minutes = Math.floor(seconds / 60);
    const remainder = seconds % 60;
    return minutes + 'm' + (remainder ? ' ' + remainder + 's' : '');
  }

  function completedCalibration(job) {
    return job?.kind === 'calibration' && job.status === 'completed'
      && job.calibration_profile_candidate && Object.keys(job.calibration_profile_candidate.cases || {}).length;
  }

  function calibrationMatchesOffer(job, offer) {
    if (!offer || !completedCalibration(job)) return false;
    const identity = job.calibration_profile_candidate.hardware_identity || {};
    const provider = identity.provider;
    return provider?.gpu_name === offer.gpu_name && Number.isFinite(Number(provider.vram_gb))
      && Number(provider.vram_gb) === Number(offer.vram_gb);
  }

  function latestCompletedCalibration({pendingOnly=false, offer=selectedOffer}={}) {
    if (!offer) return null;
    const offerPower = Number(offer.gpu_max_power_watts);
    const selection = calibrationSelection();
    const candidates = jobRows.map((job, order) => ({job, order}))
      .filter(({job}) => calibrationMatchesOffer(job, offer)
        && !job.calibration_source_id
        && (selection.legacy
          ? !job.calibration_preset
          : job.calibration_preset === selection.preset && Number(job.calibration_profile_candidate?.protocol) === 4)
        && (!pendingOnly || !job.calibration_profile_accepted_at));
    candidates.sort((left, right) => {
      const powerOf = ({job}) => Number(job.calibration_profile_candidate.hardware_identity?.gpu_power_limit_watts);
      const leftPower = powerOf(left), rightPower = powerOf(right);
      const leftDelta = offerPower > 0 && leftPower > 0 ? Math.abs(leftPower - offerPower) : Infinity;
      const rightDelta = offerPower > 0 && rightPower > 0 ? Math.abs(rightPower - offerPower) : Infinity;
      return leftDelta - rightDelta || left.order - right.order;
    });
    return candidates[0]?.job || null;
  }

  function renderCalibrationResult(container, job) {
    container.replaceChildren();
    if (!completedCalibration(job)) { container.hidden = true; return; }
    const candidate = job.calibration_profile_candidate;
    const identity = candidate.hardware_identity || {};
    const head = document.createElement('div'); head.className = 'calibration-result-head';
    const title = document.createElement('span'); title.className = 'calibration-result-title';
    title.textContent = identity.gpu_name || 'GPU performance test'; head.appendChild(title);
    const summary = document.createElement('span'); summary.className = 'calibration-result-summary';
    summary.textContent = 'Population / batch ' + Number(candidate.population_size).toLocaleString()
      + (Number(candidate.max_dispatch_candidate_bars) > 0
        ? ' · measured work cap ' + Number(candidate.max_dispatch_candidate_bars).toLocaleString() + ' candidate-bars' : '');
    head.appendChild(summary);
    container.appendChild(head);
    const meta = document.createElement('div'); meta.className = 'calibration-result-meta';
    const metaParts = [];
    if (Number(candidate.protocol) === 4) metaParts.push(job.calibration_source_id
      ? 'Exact frozen queue input · source ' + job.calibration_source_id
      : 'EMA-anchor ' + (job.calibration_preset || 'custom') + ' reference');
    if (Number(candidate.protocol) === 3) metaParts.push('EMA-anchor calibration');
    else if (Number(candidate.protocol) === 2) metaParts.push('Legacy trailing-martingale calibration; rate may cover only one logged generation');
    if (Number.isFinite(Number(identity.vram_mib))) metaParts.push(fmt(Number(identity.vram_mib) / 1024, 2) + ' GB runtime VRAM');
    if (identity.compute_capability) metaParts.push('CUDA ' + identity.compute_capability);
    if (Number.isFinite(Number(identity.gpu_power_limit_watts)) && Number(identity.gpu_power_limit_watts) > 0) {
      metaParts.push('Measured power limit ' + fmt(Number(identity.gpu_power_limit_watts), 0) + ' W');
    }
    if (identity.pci_device_id) metaParts.push('PCI ' + identity.pci_device_id);
    const stopReason = job.calibration_result?.stop_reason || candidate.stop_reason;
    const stopLabels = {scaling_plateau:'Scaling plateau reached',performance_regression:'Next step was slower',
      vram_limit:'VRAM safety limit reached',rental_deadline:'Rental deadline reached; faster populations may exist',
      maximum_safety_limit:'131k safety limit reached'};
    if (stopLabels[stopReason]) metaParts.push(stopLabels[stopReason]);
    metaParts.push(job.calibration_profile_accepted_at ? 'Local profile active' : 'Ready to use as a local profile');
    meta.textContent = metaParts.join(' · '); container.appendChild(meta);
    const wrap = document.createElement('div'); wrap.className = 'table-wrap';
    const table = document.createElement('table'); table.setAttribute('aria-label', 'GPU performance test results');
    const thead = document.createElement('thead'), header = document.createElement('tr');
    ['Population', 'Candidates / s', 'Rate window', 'Completed GPU proxy', 'Effective dispatch', 'Measured', 'Evidence', 'Result'].forEach(label => {
      const cell = document.createElement('th'); cell.textContent = label; header.appendChild(cell);
    });
    thead.appendChild(header); table.appendChild(thead);
    const tbody = document.createElement('tbody');
    Object.values(candidate.cases || {}).filter(row => row && Number.isFinite(Number(row.population_size)))
      .sort((left, right) => Number(left.population_size) - Number(right.population_size)).forEach(row => {
        const tr = document.createElement('tr');
        if (Number(row.population_size) === Number(candidate.population_size)) tr.className = 'is-recommended';
        const dispatchSize = Number(row.dispatch_batch_size);
        const dispatchCount = Number(row.dispatch_count);
        const lastDispatch = Number(row.last_dispatch_size);
        const dispatch = dispatchSize > 0 && dispatchCount > 0
          ? dispatchCount.toLocaleString() + ' × ≤' + dispatchSize.toLocaleString()
            + (lastDispatch > 0 ? ' · last ' + lastDispatch.toLocaleString() : '')
          : '—';
        const rateWindow = Number(row.rate_window_seconds);
        const generations = Number(row.generation_count);
        const values = [
          Number(row.population_size).toLocaleString(),
          Number.isFinite(Number(row.rate_per_second)) ? fmt(Number(row.rate_per_second), 1) : '—',
          rateWindow > 0 ? formatElapsedSeconds(rateWindow) + (generations > 0 ? ' · ' + generations + ' gen' : '') : '—',
          Number(row.proxy_seconds) > 0 ? formatElapsedSeconds(Number(row.proxy_seconds)) : '—',
          dispatch,
          Number.isFinite(Number(row.wall_seconds)) ? formatElapsedSeconds(Number(row.wall_seconds)) : '—',
          row.source === 'heartbeat' ? '5-minute heartbeat' : row.source === 'generation_profile'
            ? (row.all_dispatches_full ? 'One batch per scenario · ' + Number(row.scenario_count || 1)
              + ' scenarios' : 'PB8 GPU profile')
            : row.source === 'completed_generation' ? 'Sparse legacy log' : String(row.source || '—'),
          row.valid ? (Number(row.population_size) === Number(candidate.population_size) ? 'Recommended' : 'Valid') : 'Invalid',
        ];
        values.forEach(value => { const cell = document.createElement('td'); cell.textContent = value; tr.appendChild(cell); });
        tbody.appendChild(tr);
      });
    table.appendChild(tbody); wrap.appendChild(table); container.appendChild(wrap); container.hidden = false;
  }

  function renderCalibrationWatch(activeRental, activeCalibration) {
    const watch = queueState.calibration_watch;
    const watchedJob = watch ? jobRows.find(row => row.id === watch.job_id) : null;
    const activeTest = watch ? null : jobRows.find(row => row.kind === 'calibration'
      && row.calibration_watch_preferences
      && (['provisioning', 'uploading', 'running', 'collecting'].includes(row.status)
        || workers.some(worker => worker.calibration_job_id === row.id
          && !['none', 'deletion_verified'].includes(worker.rental_state))));
    const criteria = watch?.preferences || activeTest?.calibration_watch_preferences;
    const start = el('calibration-watch-start');
    const cancel = el('calibration-watch-cancel');
    const panel = el('calibration-watch-panel');
    const status = el('calibration-watch-status');
    let reason = '';
    if (watch) reason = 'A waiting performance test is already active.';
    else if (!calibrationWorkerAvailable) reason = 'The EMA-anchor 4k/8k worker has not been published and pinned yet.';
    else if (!supervision) reason = 'Rental supervision is unavailable.';
    else if (!savedPreferences) reason = 'Rental settings are still loading.';
    else if (activeRental) reason = 'Finish the active GPU rental first.';
    else if (activeCalibration) reason = 'A performance test is already preparing or running.';
    else if (calibrationStarting || watchCancelling) reason = 'A performance-test action is in progress.';
    else if (!el('offers-form').checkValidity()) reason = 'Enter valid GPU requirements.';
    else if (!el('gpu-model').value.trim()) reason = 'Enter a GPU type to watch.';
    else if (Number(savedPreferences.hours) < .75) reason = 'Set maximum rental hours to at least 0.75.';
    else if (Number(savedPreferences.hours) * Number(el('max-price').value) > Number(savedPreferences.budget) + 1e-6)
      reason = 'Budget must cover the maximum rate for the approved duration.';
    start.disabled = !!reason;
    start.hidden = !!criteria;
    start.title = reason;
    start.textContent = calibrationStarting && !watch ? 'Starting waiting test…' : 'Test when matching GPU is available';
    cancel.hidden = !watch;
    cancel.disabled = watchCancelling;
    cancel.dataset.watchId = watch?.id || '';
    cancel.textContent = watchCancelling ? 'Cancelling…' : 'Cancel waiting test';
    panel.hidden = !criteria;
    if (!criteria) { status.textContent = ''; status.title = ''; return; }

    const label = criteria.gpu_name || 'matching GPU';
    el('calibration-watch-title').textContent = watch ? 'Waiting for ' + label : 'Performance test: ' + label;
    const rental = workers.find(row => row.calibration_job_id === (watch?.job_id || activeTest?.id));
    let phase = 'Searching offers';
    let detail = 'Checks about every 30 seconds until you cancel.';
    if (rental?.cleanup_wait_until) {
      phase = 'Verifying failed rental';
      detail = 'No second rental attempt until Vast confirms that no instance was created.';
    } else if (rental?.rental_state === 'creation_pending' && !rental.instance_id) {
      phase = 'Offer found · awaiting Vast';
      detail = 'Rental request pending; another offer will not be rented yet.';
    } else if (activeTest || rental) {
      phase = activeTest?.status === 'running' ? 'Test running' : 'GPU found · starting test';
      detail = 'Follow rental and test progress in Queue.';
    } else if (watchedJob?.status === 'preparing') {
      phase = 'Preparing test data';
      detail = 'No rental starts until the fixed test input is ready.';
    } else if (watchedJob?.status === 'failed') {
      phase = 'Preparing another attempt';
      detail = 'The earlier attempt did not complete; PBGui will retry a matching offer.';
    }
    el('calibration-watch-phase').textContent = phase;
    const items = [
      '≤$' + fmt(criteria.max_price, 4) + '/hour',
      Number(criteria.min_power_watts) > 0 ? '≥' + fmt(criteria.min_power_watts, 0) + ' W advertised' : 'Any power limit',
      Number(criteria.min_reliability_pct) > 0 ? '≥' + fmt(criteria.min_reliability_pct, 1) + '% reliability' : 'Any reliability',
      criteria.verified_only ? 'Verified hosts' : 'All hosts',
      '≥' + fmt(criteria.min_vram, 0) + ' GB VRAM',
      '≥' + fmt(criteria.min_cpu, 0) + ' CPU',
      '≥' + fmt(criteria.min_ram, 0) + ' GB RAM',
      '≥' + fmt(criteria.disk_gb, 0) + ' GB disk',
    ];
    if (watch) items.push('Up to ' + fmt(watch.hours, 2) + ' h', '$' + fmt(watch.budget, 2) + ' budget');
    const target = el('calibration-watch-target');
    target.replaceChildren();
    items.forEach(value => {
      const chip = document.createElement('span');
      chip.textContent = value;
      target.appendChild(chip);
    });
    const checked = watch?.last_checked_at ? ' Last checked ' + new Date(watch.last_checked_at * 1000).toLocaleTimeString() + '.' : '';
    status.textContent = detail + checked + (watch?.last_error ? ' Last search error: ' + watch.last_error : '');
    status.title = status.textContent;
  }

  function renderCalibration() {
    renderOfferGpuProfile();
    const status = el('calibration-status');
    const match = calibrationInfo?.match;
    const selection = calibrationSelection();
    const pending = latestCompletedCalibration({pendingOnly:true});
    const latest = pending || latestCompletedCalibration();
    if (!selectedOffer) status.textContent = 'Select an offer to check PBGui performance coverage.';
    else if (pending) {
      const identity = pending.calibration_profile_candidate.hardware_identity || {};
      const measuredPower = Number(identity.gpu_power_limit_watts);
      const offerPower = Number(selectedOffer?.gpu_max_power_watts);
      const proximity = measuredPower > 0 && offerPower > 0
        ? (Math.abs(measuredPower - offerPower) < 0.5 ? 'exact power-limit match'
          : 'nearest saved test at ' + fmt(measuredPower, 0) + ' W for this offer at ' + fmt(offerPower, 0) + ' W')
        : 'closest saved test for this GPU/VRAM variant';
      status.textContent = 'Completed performance test ready: population '
        + Number(pending.calibration_profile_candidate.population_size).toLocaleString() + ' · ' + proximity
        + '. Review the measured populations below and use the profile without running another test.';
    }
    else if (!calibrationInfo) status.textContent = 'Checking performance coverage…';
    else if (!(selection.legacy ? calibrationInfo.calibration_worker : calibrationInfo.configurable_worker))
      status.textContent = selection.legacy ? 'The pinned Small-test worker is unavailable.'
        : 'This reference mode requires the protocol-4 worker release; no GPU will be rented yet.';
    else if (match) status.textContent = (match.source === 'local' ? 'Local test' : 'PBGui reference') + ' available: population '
      + Number(match.population_size).toLocaleString() + '. Hardware match only; workload compatibility is checked separately.';
    else status.textContent = 'No verified profile for this advertised GPU variant. Selected reference: '
      + selection.preset + '. Runtime identity distinguishes power limits and modified-VRAM cards.';
    const selectedWorkerAvailable = selection.legacy
      ? calibrationInfo?.calibration_worker : calibrationInfo?.configurable_worker;
    const activeRental = workers.some(item => !['none','deletion_verified'].includes(item.rental_state));
    const activeCalibration = jobRows.some(job => job.kind === 'calibration'
      && !['completed','failed','cancelled'].includes(job.status));
    renderCalibrationWatch(activeRental, activeCalibration);
    if (!selection.legacy) {
      el('calibration-watch-start').disabled = true;
      el('calibration-watch-start').title = 'Waiting tests currently use the fixed Small reference only.';
    }
    let blockedReason = '';
    if (!selectedOffer) blockedReason = 'Select an offer first.';
    else if (!calibrationInfo) blockedReason = 'Checking performance-test availability.';
    else if (!selectedWorkerAvailable) {
      blockedReason = selection.legacy
        ? 'The pinned calibration worker is unavailable or mismatched.'
        : 'Configurable tests require a published, pinned protocol-4 worker; no rental will start.';
    } else if (!supervision) blockedReason = 'Rental supervision is unavailable.';
    else if (!savedPreferences) blockedReason = 'Rental settings are still loading.';
    else if (activeRental) blockedReason = 'Finish the active GPU rental first.';
    else if (activeCalibration) blockedReason = 'A performance test is already preparing or running.';
    else if (queueState.calibration_watch) blockedReason = 'Cancel the waiting performance test first.';
    else if (calibrationStarting) blockedReason = 'A performance test is already starting.';
    else if (renting) blockedReason = 'A rental request is already starting.';
    else if (startingQueue) blockedReason = 'A queue start is already in progress.';
    else if (workerAction) blockedReason = 'A GPU worker action is in progress.';
    if (blockedReason && selectedOffer) status.textContent += ' Performance test disabled: ' + blockedReason;
    el('calibration-start').textContent = calibrationStarting ? 'Starting performance test…'
      : (selection.legacy ? (match || pending ? 'Repeat performance test' : 'Run performance test')
        : 'Run ' + selection.preset + ' performance test');
    el('calibration-start').disabled = !!blockedReason;
    el('calibration-start').title = blockedReason;
    el('calibration-start').setAttribute('aria-busy', String(calibrationStarting));
    el('calibration-accept').hidden = !pending;
    el('calibration-accept').textContent = pending ? 'Use completed test profile ('
      + Number(pending.calibration_profile_candidate.population_size).toLocaleString() + ')' : 'Use completed test profile';
    el('calibration-accept').dataset.jobId = pending?.id || '';
    const result = el('calibration-result');
    renderCalibrationResult(result, latest);
    const showingWatch = !el('calibration-watch-panel').hidden;
    status.hidden = showingWatch;
    if (showingWatch) result.hidden = true;
  }

  async function refreshCalibration() {
    const offer = selectedOffer, current = ++calibrationGeneration;
    if (!offer || !offer.machine_id) { calibrationInfo = null; renderCalibration(); return; }
    try {
      const data = await request('/calibration/status', {method:'POST', body:JSON.stringify(calibrationOffer(offer))});
      if (disposed || current !== calibrationGeneration || selectedOffer?.id !== offer.id) return;
      calibrationInfo = data; renderJob();
    } catch (error) {
      if (!disposed && current === calibrationGeneration) {
        calibrationInfo = {calibration_worker:false, error:error.message};
        el('calibration-status').textContent = error.message;
        renderJob();
      }
    }
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
    const filter = el('host-status-filter').value;
    const profiles = [...hostProfiles.values()].filter(profile => !['preferred','working'].includes(filter) || !!profile[filter]);
    if (!profiles.length && hostProfiles.size) list.textContent = 'No matching hosts.';
    profiles.sort((a,b) => Number(b.preferred) - Number(a.preferred) || a.machine_id - b.machine_id).forEach(profile => {
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
      min_ram:el('min-ram').value, min_cpu:el('min-cpu').value, min_tflops:el('min-tflops').value,
      min_power_watts:el('min-power-watts').value, min_reliability_pct:el('min-reliability-pct').value, disk_gb:el('disk').value,
      verified_only:el('verified').value, gpu_name:el('gpu-model').value.trim(),
      include_incompatible:String(el('show-incompatible').checked), rental_hours:savedPreferences?.hours ?? 1});
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
    min_ram:'min-ram', min_cpu:'min-cpu', min_tflops:'min-tflops', min_power_watts:'min-power-watts', min_reliability_pct:'min-reliability-pct', disk_gb:'disk', verified_only:'verified', hours:'job-hours', budget:'job-budget', idle_seconds:'worker-idle', max_rentals:'max-rentals', auto_rent:'auto-rent', convergence_enabled:'convergence-enabled', convergence_min_exact:'convergence-min', convergence_patience:'convergence-patience', convergence_tolerance_pct:'convergence-tolerance'};
  const preferenceGroups = {
    offers: ['gpu_name','max_price','min_vram','min_ram','min_cpu','min_tflops','min_power_watts','min_reliability_pct','disk_gb','verified_only'],
    rental: ['hours','budget','idle_seconds','max_rentals','auto_rent','convergence_enabled','convergence_min_exact','convergence_patience','convergence_tolerance_pct']
  };
  const preferenceEdits = {};
  let preferenceSavePending = false;
  Object.entries(preferenceFields).forEach(([key,id]) => {
    preferenceEdits[key] = 0;
    ['input','change'].forEach(event => el(id).addEventListener(event, () => {
      preferenceEdits[key]++;
      if (preferenceGroups.offers.includes(key)) renderCalibration();
    }));
  });
  function applyPreferences(data, keys, editSnapshot) {
    data = {max_rentals:1, auto_rent:false, min_tflops:0, convergence_enabled:false, convergence_min_exact:512, convergence_patience:512, convergence_tolerance_pct:0.25, ...data};
    savedPreferences = {...data};
    el('saved-rental-policy').textContent = 'Saved rental limits: auto rent & start ' + (data.auto_rent ? 'on' : 'off') + ' · up to ' + data.max_rentals + ' concurrent GPUs · maximum simultaneous budget targets $' + fmt(data.max_rentals * data.budget, 2) + ' · per GPU: up to ' + data.hours + ' hours · budget target $' + fmt(data.budget, 2) + ' · ' + idlePolicyLabel(data.idle_seconds) + '. Change these in Rental & Automation.';
    (keys || Object.keys(preferenceFields)).forEach(key => {
      if (!editSnapshot || preferenceEdits[key] === editSnapshot[key]) el(preferenceFields[key]).value = data[key] == null ? '' : String(data[key]);
    });
    el('saved-requirements').textContent = 'Saved: ' + (data.gpu_name || 'any GPU type') + ' · max $' + fmt(data.max_price, 4) + '/hour · ' + data.min_vram + ' GB VRAM / ' + data.min_ram + ' GB RAM / ' + data.min_cpu + ' CPU cores / min ' + fmt(data.min_tflops || 0, 1) + ' TFLOPS. Current matching offers are selected only at start.';
    renderJob();
  }
  function setPreferenceSaving(pending) {
    preferenceSavePending = pending;
    el('save-gpu-preferences').disabled = pending || !savedPreferences;
    el('save-rental-preferences').disabled = pending || !savedPreferences;
  }
  async function savePreferences(group) {
    if (disposed || preferenceSavePending || !savedPreferences) return;
    if (!el(group === 'offers' ? 'offers-form' : 'rental-form').reportValidity()) return;
    const keys = preferenceGroups[group], values = {}, edits = {...preferenceEdits};
    keys.forEach(key => {
      const value = el(preferenceFields[key]).value;
      values[key] = key === 'gpu_name' ? value.trim() : ['verified_only','auto_rent','convergence_enabled'].includes(key) ? value === 'true' : Number(value);
    });
    if (group === 'rental' && values.auto_rent) {
      if (!window.PBGuiDialogs?.confirm) { message('Rental confirmation unavailable. Reload this page.', true); return; }
      const accepted = await window.PBGuiDialogs.confirm({
        title:'Enable auto rent & start',
        message:'Automatically rent and start prepared Vast queue jobs using up to ' + values.max_rentals + ' GPUs? The budget target is $' + fmt(values.budget,2) + ' per rental (up to $' + fmt(values.max_rentals * values.budget,2) + ' simultaneously), for at most ' + values.hours + ' hours per rental. Replacement rentals may start while queued jobs remain. Pause prevents new rentals; End rentals disables automation and cleans up all rentals.',
        confirmText:'Enable auto rent & start'
      });
      if (!accepted || disposed) return;
    }
    setPreferenceSaving(true);
    try {
      const data = await request('/gpu-preferences', {method:'PATCH', body:JSON.stringify(values)});
      if (!disposed) {
        applyPreferences(data, keys, edits);
        if (group === 'rental') await refreshJobs();
        const savedMessage = group === 'offers' ? 'GPU requirements saved. No GPU has been rented.'
          : data.auto_rent ? 'Auto rent & start enabled. Prepared queue jobs will rent and start automatically.'
          : 'Auto rent & start disabled. No new GPUs will be rented; active rentals continue until cleanup.';
        message(savedMessage, false);
      }
    } catch (error) { if (!disposed) message(error.message, true); }
    finally { if (!disposed) setPreferenceSaving(false); }
  }
  el('save-gpu-preferences').addEventListener('click', () => savePreferences('offers'));
  el('rental-form').addEventListener('submit', event => { event.preventDefault(); savePreferences('rental'); });
  setPreferenceSaving(true);
  const initialPreferenceEdits = {...preferenceEdits};
  request('/gpu-preferences').then(data => { if (!disposed) applyPreferences(data, Object.keys(preferenceFields), initialPreferenceEdits); })
    .catch(error => { if (!disposed) message(error.message, true); })
    .finally(() => { if (!disposed) setPreferenceSaving(false); });

  function selectedJob() { return jobRows.find(row => row.id === selectedJobId); }

  function uploadProgressMetrics(job) {
    const upload = job?.upload_progress || {};
    const rawTotal = Number(upload.total || job?.transfer_input_bytes || job?.input_bytes || 0);
    const total = Number.isFinite(rawTotal) && rawTotal > 0 ? rawTotal : 0;
    const rsync = upload.transport === 'rsync';
    const directFiles = rsync && upload.mode === 'files';
    const rawSent = rsync ? upload.transferred_bytes : upload.bytes;
    const sentValue = Number(rawSent);
    const sent = rawSent == null || !Number.isFinite(sentValue) ? null : Math.max(0, sentValue);
    const checkingCache = upload.stage === 'checking_cache';
    const filesTotal = Number(upload.files_total), filesChecked = Number(upload.files_checked);
    const fileProgress = checkingCache && Number.isInteger(filesTotal) && filesTotal >= 0
      && Number.isInteger(filesChecked) && filesChecked >= 0 && filesChecked <= filesTotal;
    const percent = checkingCache ? (fileProgress && filesTotal > 0 ? 100 * filesChecked / filesTotal : 0)
      : total > 0 && sent != null ? Math.min(100, 100 * sent / total) : 0;
    const networkBytesValue = Number(upload.network_bytes ?? job?.sync_statistics?.sent_bytes);
    const networkBytes = Number.isFinite(networkBytesValue) && networkBytesValue >= 0 ? networkBytesValue : null;
    const reusedBytesValue = Number(upload.reused_bytes ?? job?.sync_statistics?.reused_bytes);
    const reusedBytes = Number.isFinite(reusedBytesValue) && reusedBytesValue >= 0 ? reusedBytesValue : null;
    return {
      upload, total, rsync, directFiles, sent, checkingCache, filesTotal, filesChecked, fileProgress, percent,
      networkBytes, reusedBytes, measured: Number(upload.bytes_per_second), network: Number(job?.rental?.offer?.inet_down_mbps)
    };
  }

  function directFileSyncText(metrics, complete=false) {
    const {total, sent, networkBytes, reusedBytes} = metrics;
    if (complete && networkBytes != null) {
      const parts = ['Cached input ready'];
      if (reusedBytes != null) parts.push(fmt(reusedBytes / 1e6, 1) + ' MB reused');
      parts.push(fmt(networkBytes / 1e6, 1) + ' MB sent');
      return parts.join(' · ');
    }
    const parts = ['Checking and synchronizing GPU input cache'];
    if (total > 0) {
      parts.push(fmt((sent || 0) / 1e6, 1) + ' / ' + fmt(total / 1e6, 1) + ' MB compared');
    }
    return parts.join(' · ');
  }

  function compactJobProgress(job) {
    if (!job) return 'waiting / cleanup';
    const phase = {
      preparing:'Preparing input', ready:'Waiting to start', provisioning:'Preparing worker',
      uploading:'Uploading input', running:'Running', collecting:'Collecting results',
      completed:'Complete', failed:'Failed', cancelled:'Cancelled'
    }[job.status] || job.status || 'Waiting';
    if (job.kind === 'calibration') {
      const calibration = job.calibration_result || {};
      const cases = Object.values(calibration.cases || {}).filter(item => item && item.valid);
      const parts = [phase, cases.length + ' valid population' + (cases.length === 1 ? '' : 's')];
      if (calibration.active_population) parts.push('testing ' + Number(calibration.active_population).toLocaleString());
      const progress = calibration.active_progress || {};
      if (Number.isFinite(progress.elapsed_seconds)) parts.push(Math.round(progress.elapsed_seconds) + 's sample');
      if (Number.isFinite(progress.rate_per_second)) parts.push(fmt(progress.rate_per_second, 3) + '/s');
      const recommendation = job.calibration_profile_candidate?.population_size || calibration.population_size;
      if (recommendation) parts.push('recommended ' + Number(recommendation).toLocaleString());
      return parts.join(' · ');
    }
    if (job.status === 'provisioning' && job.image_progress) {
      const image = job.image_progress;
      if (image.total > 0) {
        if (image.ready === image.total) return 'Starting worker';
        if (image.downloaded === image.total) return 'Extracting worker image';
        return 'Downloading worker image' + (Number.isFinite(image.download_percent) ? ' · ' + fmt(image.download_percent, 1) + '%' : '');
      }
    }
    if (job.status === 'uploading') {
      const metrics = uploadProgressMetrics(job);
      const {upload, total, directFiles, sent, checkingCache, filesTotal, filesChecked, fileProgress, percent, measured, network} = metrics;
      const label = ({
        preparing_files:'Preparing file synchronization', preparing_worker:'Transferring job metadata',
        checking_cache:'Checking cached input data', packing:'Preparing transfer archive',
        installing:'Installing input data', verifying:'Verifying input data',
        reconnecting:'Waiting to retry upload', synchronized:'Input synchronized'
      })[upload.stage] || phase;
      if (directFiles && !['preparing_files', 'preparing_worker', 'reconnecting'].includes(upload.stage)) {
        return directFileSyncText(metrics, ['installing', 'synchronized'].includes(upload.stage));
      }
      const parts = [label];
      if (checkingCache && fileProgress && filesTotal > 0) {
        parts.push(filesChecked.toLocaleString() + ' / ' + filesTotal.toLocaleString() + ' files · ' + fmt(percent, 1) + '%');
      } else if (upload.stage === 'preparing_worker' && Number(upload.metadata_total) > 0) {
        const submitted = Math.min(Number(upload.metadata_total), Math.max(0, Number(upload.metadata_submitted) || 0));
        parts.push(fmt(submitted / 1e6, 1) + ' / ' + fmt(Number(upload.metadata_total) / 1e6, 1) + ' MB · ' + fmt(100 * submitted / Number(upload.metadata_total), 1) + '%');
      } else if (total > 0 && sent != null) {
        parts.push(fmt(sent / 1e6, 1) + ' / ' + fmt(total / 1e6, 1) + ' MB · ' + fmt(percent, 1) + '%');
      }
      if (Number.isFinite(measured) && measured > 0) {
        const measuredMbps = measured * 8 / 1e6;
        parts.push(fmt(measuredMbps, 1) + ' Mbps');
        if (Number.isFinite(network) && network > 0) {
          const saturation = Math.max(0, 100 * measuredMbps / network);
          parts.push(fmt(saturation, saturation < 1 ? 2 : saturation < 10 ? 1 : 0) + '% of host bandwidth');
        }
      }
      else if (sent != null && sent > 0 && upload.stage === 'reconnecting') parts.push(fmt(sent / 1e6, 1) + ' MB retained');
      return parts.join(' · ');
    }
    if (!['running', 'collecting', 'completed'].includes(job.status)) return phase;
    const exact = Number(job.exact_completed) || 0;
    const target = Number(job.iterations) || 0;
    const proxy = Number(job.gpu_candidates) || 0;
    const progress = target > 0 ? 100 * exact / target : null;
    const parts = [phase];
    if (target > 0) parts.push(exact.toLocaleString() + ' / ' + target.toLocaleString() + ' exact');
    if (proxy > 0 || exact > 0) parts.push(proxy.toLocaleString() + ' proxy' + (progress == null ? '' : ' (' + fmt(progress, 1) + '%)'));
    const throughput = job.throughput || {};
    const sampleAge = Number.isFinite(throughput.sampled_at) ? Date.now() / 1000 - throughput.sampled_at : Infinity;
    if (sampleAge <= 90 && Number.isFinite(throughput.proxy_per_minute)) {
      parts.push(throughput.proxy_per_minute.toLocaleString(undefined, {maximumFractionDigits:1}) + ' proxy/min');
    }
    return parts.join(' · ');
  }

  function workerActivity(rental) {
    if (!rental && typeof workers !== 'undefined' && (workers.length > 1 || queueState.pool_authorization)) {
      const limit = queueState.pool_authorization?.settings?.max_rentals || savedPreferences?.max_rentals || 1;
      return 'GPU pool: ' + workers.length + ' / ' + limit + ' rentals · ' + (queueState.paused ? 'paused' : queueState.pool_enabled ? 'automatic scheduling enabled' : 'stopped')
        + (queueState.pool_error ? ' · ' + queueState.pool_error : '')
        + workers.map(item => {
          const job = jobRows.find(row => row.id === item.active_job)
            || jobRows.find(row => row.lease_id === item.id && ['provisioning', 'uploading', 'running', 'collecting'].includes(row.status));
          return '\n' + (item.gpu_name || 'GPU') + ' · instance ' + (item.instance_id || 'pending') + ' · '
            + (job ? job.config_name + ' · ' + compactJobProgress(job) : 'waiting / cleanup');
        }).join('');
    }
    const currentWorker = rental || worker;
    const active = currentWorker && !['none', 'deletion_verified'].includes(currentWorker.rental_state);
    let text = active ? currentWorker.gpu_name + ' · ' + (currentWorker.rental_state === 'creation_pending' ? 'provisioning (instance not confirmed)' : currentWorker.status) + ' · $' + fmt(currentWorker.price_hour_usd, 4) + '/hour · rental: ' + currentWorker.rental_state + ' · deadline: ' + new Date(currentWorker.deadline * 1000).toLocaleString() + (currentWorker.idle_since ? ' · idle deletion: ' + new Date((currentWorker.idle_since + currentWorker.idle_seconds) * 1000).toLocaleTimeString() : '') : 'No active GPU rental.' + (currentWorker ? ' Last rental: ' + currentWorker.rental_state + '.' : '') + ' GPU requirements are configured in GPU & Offers; a matching offer is selected at queue start.';
    if (active && currentWorker.cleanup_wait_until) {
      text = 'Cleanup: no instance found · waiting until ' + new Date(currentWorker.cleanup_wait_until * 1000).toLocaleTimeString()
        + ' · last checked ' + new Date(currentWorker.provider_checked_at * 1000).toLocaleTimeString();
    }
    else if (active && currentWorker.instance_id) {
      text = currentWorker.gpu_name + ' · instance ' + currentWorker.instance_id + ' · '
        + (currentWorker.provider_status || currentWorker.status) + ' · $' + fmt(currentWorker.price_hour_usd, 4) + '/hour';
    }
    return text;
  }

  function jobActivity(job, rental) {
    if (job?.observed_optimizer?.activity) return job.observed_optimizer.activity;
    const labels = {completed:'Job completed', cancelled:'Job cancelled', failed:'Job failed'};
    if (labels[job?.status]) {
      const updated = Number(job.updated_at);
      return labels[job.status] + (Number.isFinite(updated) ? ' · ' + new Date(updated * 1000).toLocaleString() : '');
    }
    return workerActivity(rental);
  }

  function jobLastError(job, rental) {
    const ownError = job?.error || job?.log_error || job?.cleanup_error || '';
    if (rental?.creation_error && !rental.instance_id && !rental.provider_seen_at) {
      return 'Rental was not created: ' + rental.creation_error
        + '. Vast confirms no instance exists; refresh offers and select another host.';
    }
    if (['completed', 'cancelled', 'failed'].includes(job?.status)) return ownError;
    return (rental && (rental.creation_error || rental.cleanup_error)) || ownError;
  }

  function renderJob() {
    renderHostBlocks();
    const job = selectedJob();
    const activeRental = workers.some(item => !['none', 'deletion_verified'].includes(item.rental_state));
    const active = activeRental || queueState.pool_enabled;
    const rentReason = !selectedOffer ? 'Select an offer first.'
      : !supervision ? 'Rental supervision is unavailable.'
      : activeRental ? 'Finish the active GPU rental first.'
      : queueState.calibration_watch ? 'Cancel the waiting performance test first.'
      : !calibrationInfo?.rental_profile ? 'Waiting for the selected GPU profile.' : '';
    el('rent-offer').disabled = !!rentReason || renting || startingQueue || !!workerAction;
    el('rent-offer').textContent = renting ? 'Renting…' : 'Rent';
    el('rent-offer').title = rentReason;
    renderCalibration();
    renderCalibrationResult(el('cloud-calibration-result'), job?.kind === 'calibration' ? job : null);
    const queueAccept = el('cloud-calibration-accept');
    queueAccept.hidden = !completedCalibration(job) || !!job.calibration_profile_accepted_at;
    queueAccept.dataset.jobId = queueAccept.hidden ? '' : job.id;
    const actionReasons = [];
    if (el('calibration-watch-panel').hidden) {
      const calibrationReason = el('calibration-start').title;
      if (rentReason && rentReason === calibrationReason) actionReasons.push(rentReason);
      else {
        if (rentReason) actionReasons.push('Rent: ' + rentReason);
        if (calibrationReason) actionReasons.push('Performance test: ' + calibrationReason);
      }
    }
    el('offer-action-status').hidden = actionReasons.length === 0;
    el('offer-action-status').textContent = actionReasons.join(' ');
    el('settings-rental-status').textContent = workerActivity() + (active && worker && worker.awaiting_queue_start ? (queueState.paused ? ' · Reserved for queue start' : ' · Queue started — waiting for input preparation') : '');
    const settingsEndRental = el('settings-end-rental');
    if (settingsEndRental) {
      settingsEndRental.hidden = !active || !!queueState.pool_authorization;
      settingsEndRental.disabled = !!workerAction || renting || startingQueue;
      settingsEndRental.textContent = workerAction === 'end' ? 'Ending rental…' : (queueState.pool_authorization ? 'End rentals' : 'End rental');
    }
    el('start-job').hidden = !job || job.status !== 'ready';
    el('start-job').disabled = !!workerAction || renting || startingQueue || !job || job.status !== 'ready' || (!savedPreferences && !(worker && !['none','deletion_verified'].includes(worker.rental_state))) || !supervision;
    el('requeue-job').hidden = !job || job.kind === 'calibration' || !['failed','cancelled'].includes(job.status) || !job.can_delete;
    el('requeue-job').disabled = !!job && (requeuingJobs.has(job.id) || deletingJobs.has(job.id));
    el('requeue-job').textContent = job && requeuingJobs.has(job.id) ? 'Preparing…' : 'Requeue';
    const labels = {pause:'Pause queue', resume:'Start queue', end:queueState.pool_authorization ? 'End rentals' : 'End rental', recover:'Resume supervision'};
    const pendingLabels = {pause:'Pausing…', resume:'Resuming…', end:'Ending rental…', recover:'Resuming supervision…'};
    Object.keys(labels).forEach(action => {
      const button = el(action + '-worker');
      if (['resume','end'].includes(action)) button.hidden = !!queueState.pool_authorization;
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

  function openCloudResults(job) {
    if (!job?.result_path || typeof openLogPanelResults !== 'function') return;
    selectedJobId = job.id;
    if (window.state) window.state.logResultName = job.result_path.split('/').pop() || job.config_name;
    openLogPanelResults();
  }

  function renderQueueOverview() {
    const banner = el('queue-rental');
    if (banner) {
      const active = workers.filter(item => !['none', 'deletion_verified'].includes(item.rental_state));
      const limit = queueState.pool_authorization?.settings?.max_rentals || savedPreferences?.max_rentals || 1;
      const summary = el('queue-rental-status');
      summary.replaceChildren();
      const title = document.createElement('strong'); title.textContent = 'GPU pool'; summary.appendChild(title);
      const capacity = document.createElement('span');
      capacity.textContent = active.length + ' / ' + limit + ' rentals';
      summary.appendChild(capacity);
      const state = document.createElement('span');
      state.className = 'queue-pool-state' + (queueState.paused ? ' is-paused' : '');
      state.textContent = queueState.paused ? 'Paused' : queueState.pool_enabled ? 'Automatic scheduling' : 'Manual';
      summary.appendChild(state);
      if (queueState.pool_error) {
        const error = document.createElement('span'); error.className = 'queue-gpu-error'; error.textContent = queueState.pool_error; summary.appendChild(error);
      }
      const waiting = jobRows.some(row => row.status === 'ready');
      if (queueState.paused && queueState.pool_enabled && waiting) {
        const resume = document.createElement('button');
        resume.type = 'button'; resume.className = 'act-btn queue-pool-resume';
        resume.textContent = startingQueue ? 'Resuming…' : 'Resume auto start';
        resume.title = 'Resume the authorized GPU pool and automatically start queued jobs';
        resume.disabled = startingQueue || !!workerAction || !supervision;
        resume.addEventListener('click', () => startCloudQueue());
        summary.appendChild(resume);
      }
      const cards = el('queue-worker-cards'); cards.replaceChildren();
      active.forEach(item => {
        const job = jobRows.find(row => row.id === item.active_job)
          || jobRows.find(row => row.lease_id === item.id && ['provisioning','uploading','running','collecting'].includes(row.status));
        const observed = !job ? currentObservedOptimizer(item) : null;
        const card = document.createElement('article'); card.className = 'queue-gpu-card';
        const head = document.createElement('div'); head.className = 'queue-gpu-card-head';
        const name = document.createElement('span'); name.className = 'queue-gpu-name'; name.textContent = item.gpu_name || 'GPU'; head.appendChild(name);
        const instance = document.createElement('span'); instance.className = 'queue-gpu-instance'; instance.textContent = 'instance ' + (item.instance_id || 'pending'); head.appendChild(instance);
        const phase = document.createElement('span'); phase.className = 'queue-gpu-phase';
        const directInputCache = job?.status === 'uploading' && job.upload_progress?.transport === 'rsync' && job.upload_progress?.mode === 'files';
        phase.textContent = job ? (directInputCache ? 'Input cache' : ({provisioning:'Provisioning',uploading:'Upload',running:'Running',collecting:'Collecting'}[job.status] || job.status)) : observed ? 'Observed run' : (item.status || 'Idle');
        head.appendChild(phase);
        card.appendChild(head);
        const jobName = document.createElement('div'); jobName.className = 'queue-gpu-job'; jobName.textContent = job?.config_name || observed?.name || 'Waiting for a queued job'; card.appendChild(jobName);
        const progress = document.createElement('div'); progress.className = 'queue-gpu-progress';
        if (job) progress.textContent = compactJobProgress(job);
        else if (observed) {
          const metrics = item.runtime_metrics || {};
          const parts = [observed.activity || 'Optimizer process detected'];
          if (Number.isFinite(Number(metrics.gpu_percent))) parts.push(fmt(Number(metrics.gpu_percent), 0) + '% GPU');
          if (Number.isFinite(Number(metrics.gpu_power_watts)) && Number.isFinite(Number(metrics.gpu_power_limit_watts))) {
            parts.push(fmt(Number(metrics.gpu_power_watts), 0) + ' / ' + fmt(Number(metrics.gpu_power_limit_watts), 0) + ' W');
          }
          progress.textContent = parts.join(' · ');
        }
        else if (item.idle_since) {
          if (Number(item.idle_seconds) < 0) progress.textContent = 'Idle · retained until rental deadline · uploaded data and worker cache stay available';
          else {
            const remaining = Math.max(0, Number(item.idle_since) + Number(item.idle_seconds ?? -1) - Date.now() / 1000);
            progress.textContent = 'Idle · retained for ' + formatElapsedSeconds(remaining) + ' · uploaded data and worker cache stay available';
          }
        } else progress.textContent = 'No active optimizer';
        if (job?.status === 'uploading' && Number(job.rental?.offer?.inet_down_mbps) > 0) progress.title = 'Upload saturation compares the measured transfer rate with the provider-advertised host download bandwidth.';
        card.appendChild(progress);
        if (job?.kind === 'calibration') {
          const measured = Object.values(job.calibration_result?.cases || {})
            .filter(row => row?.valid && Number.isFinite(Number(row.population_size))
              && Number.isFinite(Number(row.rate_per_second)))
            .sort((left, right) => Number(left.population_size) - Number(right.population_size));
          if (measured.length) {
            const speeds = document.createElement('div'); speeds.className = 'queue-gpu-tuning';
            speeds.textContent = 'Measured: ' + measured.map(row =>
              Number(row.population_size).toLocaleString() + ' → '
              + fmt(Number(row.rate_per_second), 1) + ' candidates/s').join(' · ');
            card.appendChild(speeds);
          }
        }
        const selectedJobProfiles = item.rental_job_gpu_profiles || {};
        if (Object.keys(selectedJobProfiles).length) {
          const selected = document.createElement('div'); selected.className = 'queue-gpu-tuning';
          selected.textContent = Object.keys(selectedJobProfiles).length + ' queued job GPU profile selection(s) on this rental';
          card.appendChild(selected);
        }
        if (item.rental_gpu_profile) {
          const rental = item.rental_gpu_profile;
          const summary = document.createElement('div'); summary.className = 'queue-gpu-tuning';
          summary.textContent = 'Rental GPU override · population ' + Number(rental.population_size).toLocaleString()
            + ' · batch ' + Number(rental.batch_size).toLocaleString()
            + ' · work limit ' + gpuWorkBillions(rental.max_dispatch_candidate_bars);
          card.appendChild(summary);
        }
        if (job?.gpu_tuning) {
          const tuning = document.createElement('div'); tuning.className = 'queue-gpu-tuning';
          const automatic = job.gpu_tuning.automatic || {};
          const sizing = {...automatic, ...(job.gpu_tuning.preserved || {})};
          const details = [];
          if (Number(sizing.max_dispatch_candidate_bars) > 0) {
            details.push(gpuWorkBillions(sizing.max_dispatch_candidate_bars));
          }
          if (Number(sizing.population_size) > 0) details.push('population ' + Number(sizing.population_size).toLocaleString());
          if (Number(sizing.batch_size) > 0) details.push('batch ' + Number(sizing.batch_size).toLocaleString());
          if (Number.isFinite(Number(job.gpu_tuning.cpu_workers))) details.push(Number(job.gpu_tuning.cpu_workers).toLocaleString() + ' exact workers');
          if (job.gpu_tuning.calibration_profile_match === 'exact') {
            const watts = Number(job.gpu_tuning.gpu_power_limit_watts);
            details.push('exact calibration profile' + (watts > 0 ? ' · ' + fmt(watts, 0) + ' W' : ''));
          } else if (job.gpu_tuning.calibration_profile_match === 'nearest_power_limit') {
            const watts = Number(job.gpu_tuning.calibration_profile_power_limit_watts);
            const actual = Number(job.gpu_tuning.gpu_power_limit_watts);
            const delta = Number(job.gpu_tuning.calibration_power_limit_delta_watts);
            details.push('nearest profile ' + (watts > 0 ? fmt(watts, 0) + ' W' : '')
              + (actual > 0 ? ' for ' + fmt(actual, 0) + ' W GPU' : '')
              + (Number.isFinite(delta) ? ' (Δ ' + fmt(delta, 0) + ' W)' : ''));
          }
          tuning.textContent = (job.gpu_tuning.applied_by === 'rental_override' ? 'Rental GPU override'
            : Object.keys(automatic).length ? 'Auto tuned' : 'Execution settings') + (details.length ? ' · ' + details.join(' · ') : ' · explicit GPU values preserved');
          tuning.title = 'Resolved from the rented GPU and frozen optimizer workload. Explicitly configured values are preserved.';
          card.appendChild(tuning);
        }
        const actions = document.createElement('div'); actions.className = 'queue-gpu-actions';
        const pending = workerActions.get(item.id);
        const log = document.createElement('button'); log.type = 'button'; log.className = 'act-btn'; log.textContent = 'Log';
        log.title = job ? 'Open the detailed job log' : observed ? 'Open the observed optimizer log' : 'No optimizer is assigned to this GPU'; log.disabled = !job && !observed;
        log.addEventListener('click', () => {
          if (job) openCloudLog(job);
          else if (observed) openObservedOptimizer(item);
        }); actions.appendChild(log);
        const results = document.createElement('button'); results.type = 'button'; results.className = 'act-btn'; results.textContent = 'Results';
        results.title = job?.result_path ? 'Open this optimizer result' : 'Results become available after the first verified result snapshot';
        results.dataset.tip = results.title; results.disabled = !job?.result_path;
        results.addEventListener('click', () => openCloudResults(job)); actions.appendChild(results);
        if (item.awaiting_queue_start) {
          const profile = document.createElement('button'); profile.type = 'button'; profile.className = 'act-btn'; profile.textContent = 'GPU profile';
          profile.addEventListener('click', () => {
            if (typeof window.setPanel === 'function') window.setPanel('vast', 'rental');
            else if (typeof showSettings === 'function') showSettings('rental');
            el('active-rental-gpu-profile').scrollIntoView({block:'center'});
          }); actions.appendChild(profile);
          const start = document.createElement('button'); start.type = 'button'; start.className = 'act-btn'; start.textContent = pending === 'start' ? 'Starting…' : 'Start queue'; start.disabled = !!pending || (activeGpuWorkerId === item.id && activeGpuDirty); start.title = activeGpuWorkerId === item.id && activeGpuDirty ? 'Save GPU profile changes first' : 'Start queued jobs on this rental'; start.addEventListener('click', () => runWorkerAction(item, 'start')); actions.appendChild(start);
        }
        const autoReplace = !!(queueState.pool_enabled && (queueState.gpu_preferences?.auto_rent || savedPreferences?.auto_rent));
        const end = document.createElement('button'); end.type = 'button'; end.className = 'act-btn';
        end.textContent = pending ? (pending === 'replace' ? 'Replacing…' : 'Ending…') : (autoReplace ? 'Replace GPU' : 'End rental');
        end.title = autoReplace ? 'End this rental. After provider cleanup is verified, Auto rent & start restores the pool capacity. Unstarted input is returned to the queue.' : 'End only this GPU rental.';
        end.disabled = !!pending; end.addEventListener('click', () => runWorkerAction(item, autoReplace ? 'replace' : 'end')); actions.appendChild(end);
        card.appendChild(actions); cards.appendChild(card);
      });
      banner.hidden = !active.length && !queueState.pool_enabled;
    }
    if (window.state) window.state.cloudQueueCount = jobRows.length;
    if (typeof renderQueueMaybeDeferred === 'function') renderQueueMaybeDeferred();
    else if (typeof updateMetaCounts === 'function') updateMetaCounts();
  }

  async function runWorkerAction(item, action) {
    if (disposed || workerActions.has(item.id)) return;
    if (action === 'start' && activeGpuWorkerId === item.id && activeGpuDirty) {
      message('Save the GPU profile changes before starting queued jobs.', true); return;
    }
    if (action === 'replace') {
      if (!window.PBGuiDialogs?.confirm) { message('GPU replacement confirmation unavailable. Reload this page.', true); return; }
      const job = jobRows.find(row => row.id === item.active_job)
        || jobRows.find(row => row.lease_id === item.id && ['provisioning','uploading','running','collecting'].includes(row.status));
      const accepted = await window.PBGuiDialogs.confirm({
        title:'Replace GPU?',
        message:'End ' + (item.gpu_name || 'this GPU') + ' instance ' + (item.instance_id || 'pending')
          + (job ? ' running "' + job.config_name + '"' : '')
          + '? Current results will be collected. After verified cleanup, Auto rent & start may rent a replacement GPU.',
        confirmText:'Replace GPU'
      });
      if (!accepted || disposed || workerActions.has(item.id)) return;
    }
    workerActions.set(item.id, action); renderQueueOverview();
    try {
      await request('/queue/workers/' + encodeURIComponent(item.id) + '/' + action, {method:'POST'});
      if (!disposed) { message(''); await refreshJobs(); }
    } catch (error) { if (!disposed) message(error.message, true); }
    finally { workerActions.delete(item.id); if (!disposed) renderQueueOverview(); }
  }

  async function refreshJobs(preferred) {
    clearTimeout(jobTimer);
    jobTimer = null;
    const current = ++jobGeneration;
    try {
      const data = await request('/jobs');
      if (disposed || current !== jobGeneration) return;
      const previousPollError = el('queue-message');
      if (previousPollError && previousPollError.dataset.jobPollError === previousPollError.textContent) {
        delete previousPollError.dataset.jobPollError;
        message('');
      } else if (previousPollError) {
        delete previousPollError.dataset.jobPollError;
      }
      const selected = preferred || selectedJobId;
      const previousResults = new Map(jobRows.map(job => [job.id, [job.result_path, job.last_backup_at, job.result_partial].join('|')]));
      const resultsChanged = data.jobs.some(job => job.result_path && previousResults.get(job.id) !==
        [job.result_path, job.last_backup_at, job.result_partial].join('|'));
      jobRows = data.jobs; workers = data.workers || (data.worker && !['none','deletion_verified'].includes(data.worker.rental_state) ? [data.worker] : []); worker = workers[0] || data.worker; queueState = data.queue; supervision = data.supervision_available; calibrationWorkerAvailable = data.calibration_worker_available === true;
      const globalMessage = el('message');
      if (globalMessage?.textContent?.startsWith('Preparing the canonical calibration input')
          && jobRows.some(job => job.kind === 'calibration' && job.status !== 'preparing')) message('');
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
      else {
        const observedWorker = workers.find(item => window.state && item.id === window.state.cloudLogId && currentObservedOptimizer(item));
        if (observedWorker) renderObservedOptimizer(observedWorker);
      }
      renderQueueOverview();
      void refreshActiveGpuProfile();
      if (typeof selectedOffer !== 'undefined' && selectedOffer) void refreshCalibration();
      if (resultsChanged && typeof refreshLiveResultsDuringRun === 'function') await refreshLiveResultsDuringRun(true);
    } catch (error) {
      if (!disposed && current === jobGeneration) {
        message(error.message, true);
        const pollError = el('queue-message');
        if (pollError) pollError.dataset.jobPollError = error.message;
      }
    } finally {
      if (!disposed && current === jobGeneration) jobTimer = setTimeout(pollJobs, 10000);
    }
  }

  async function pollJobs() {
    await refreshJobs();
  }

  let validationGeneration = 0, validationTimer = null, cloudMetrics = null;
  function setQueueBlocked(blocked) {
    ['btn-cloud-save-queue', 'btn-editor-save-queue'].forEach(id => {
      const button = el(id);
      if (!button) return;
      button.dataset.queueBlocked = String(blocked);
      // Queue clicks perform their own authoritative validation. Keep the action
      // available while background validation is pending or reports issues so a
      // click always produces visible progress or a concrete error.
      button.disabled = !!window.state?.editorSaving;
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
        'optimize.backend':'opted-opt-backend',
        'optimize.gpu.population_size':'opted-gpu-population-size',
        'optimize.gpu.batch_size':'opted-gpu-batch-size',
        'optimize.gpu.max_dispatch_candidate_bars':'opted-gpu-max-dispatch-bars',
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
  function manualGpuErrors(config) {
    if (config?.pbgui?.execution === 'vast' && config?.optimize?.backend !== 'gpu') {
      return [{path:'optimize.backend', message:'Vast.ai runs use the GPU backend. Choose GPU before queueing.'}];
    }
    return [];
  }

  function renderGpuRecommendation() {
    const status = el('opted-gpu-recommendation');
    const preview = el('opted-gpu-dispatch-preview');
    const button = el('opted-gpu-apply-measured');
    if (!status || !preview || !button) return;
    const info = gpuRecommendation;
    const profile = info?.result?.suggestion?.profile;
    const matching = !!profile && info.offerId === selectedOffer?.id;
    button.hidden = !matching;
    button.disabled = !matching;
    status.hidden = !matching;
    status.textContent = matching ? 'Measured ' + Number(profile.population_size).toLocaleString()
      + ' · ' + Number(profile.measured_power_limit_watts).toLocaleString() + ' W · '
      + Number(profile.candidates_per_second).toFixed(2) + ' candidates/s' : '';
    const row = info?.result?.preview;
    const sized = ['opted-gpu-population-size', 'opted-gpu-batch-size', 'opted-gpu-max-dispatch-bars']
      .every(id => Number(el(id)?.value) > 0);
    preview.hidden = !sized || !row?.dispatches_per_largest_scenario_estimate;
    preview.textContent = preview.hidden ? '' : 'Largest scenario: ~'
      + Number(row.dispatches_per_largest_scenario_estimate).toLocaleString() + ' dispatches · batch up to '
      + Number(row.effective_batch_estimate).toLocaleString() + ' · one batch ≈'
      + Number(row.minimum_one_batch_bars_estimate).toLocaleString() + ' candidate-bars';
  }

  async function refreshGpuRecommendation(config) {
    const current = ++gpuRecommendationGeneration;
    const offer = selectedOffer;
    if (config?.optimize?.backend !== 'gpu') {
      gpuRecommendation = null; renderGpuRecommendation(); return;
    }
    gpuRecommendation = {pending:true, offerId:offer?.id};
    renderGpuRecommendation();
    try {
      const result = await request('/gpu/recommendation', {method:'POST',
        body:JSON.stringify({config, offer:offer ? calibrationOffer(offer) : null})});
      if (disposed || current !== gpuRecommendationGeneration || selectedOffer?.id !== offer?.id) return;
      gpuRecommendation = {result, offerId:offer?.id};
    } catch (error) {
      if (disposed || current !== gpuRecommendationGeneration) return;
      gpuRecommendation = {error:error.message, offerId:offer?.id};
    }
    renderGpuRecommendation();
  }

  function applyMeasuredGpuRecommendation() {
    const info = gpuRecommendation;
    const profile = info?.result?.suggestion?.profile;
    if (!profile || info.offerId !== selectedOffer?.id) return;
    const fields = {
      'opted-gpu-population-size':profile.population_size,
      'opted-gpu-batch-size':profile.batch_size,
      'opted-gpu-max-dispatch-bars':profile.max_dispatch_candidate_bars
    };
    Object.entries(fields).forEach(([id, value]) => { if (el(id)) el(id).value = String(value); });
    if (el('opted-gpu-auto-lean')) el('opted-gpu-auto-lean').checked = false;
    if (typeof scheduleStructuredEditorSync === 'function') scheduleStructuredEditorSync();
    scheduleValidation();
  }
  document.addEventListener('click', event => {
    if (event.target?.id === 'opted-gpu-apply-measured') applyMeasuredGpuRecommendation();
  });
  async function validateEditorConfig(config, options) {
    const current = ++validationGeneration;
    clearTimeout(validationTimer); setQueueBlocked(true); showValidation([], true);
    void refreshGpuRecommendation(config);
    const manualErrors = manualGpuErrors(config);
    if (manualErrors.length) {
      if (!disposed && current === validationGeneration) showValidation(manualErrors, false);
      return false;
    }
    try {
      const result = await request('/validate-config', {method:'POST', body:JSON.stringify({config})});
      if (disposed) return false;
      // Background checks own current UI state; a queue check validates its captured payload.
      if (current !== validationGeneration) return !!(options && options.forQueue && result.valid);
      cloudMetrics = result.metrics; showValidation(result.errors, false, result); setQueueBlocked(!result.valid);
      return result.valid;
    } catch (error) {
      if (!disposed && current === validationGeneration) showValidation([{path:'validation',message:error.message}], false);
      if (options && options.forQueue) throw error;
      return false;
    }
  }
  function scheduleValidation() {
    const current = ++validationGeneration; clearTimeout(validationTimer);
    const execution = el('opted-execution');
    if (!execution || execution.value !== 'vast') {
      setQueueBlocked(false);
      showValidation([], false);
      validationTimer = setTimeout(() => {
        if (disposed || current !== validationGeneration) return;
        try {
          const config = window.collectEditorConfig?.(null, {strict:false})?.config;
          if (config) void refreshGpuRecommendation(config);
        } catch (error) { gpuRecommendation = {error:error.message}; renderGpuRecommendation(); }
      }, 350);
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
    el('cloud-gpu-power').textContent = fresh && Number.isFinite(sample.gpu_power_watts) && Number.isFinite(sample.gpu_power_limit_watts) ?
      fmt(sample.gpu_power_watts, 0) + ' / ' + fmt(sample.gpu_power_limit_watts, 0) + ' W' : '—';
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
    const rentalWorker = typeof workers !== 'undefined' ? workers.find(item => item.id === job.lease_id) : worker;
    const directInputCache = job.status === 'uploading' && job.upload_progress?.transport === 'rsync' && job.upload_progress?.mode === 'files';
    const uploadPhase = job.status === 'uploading' ? (directInputCache
      ? ({preparing_worker:'Transferring job metadata', installing:'Publishing cached input', synchronized:'Cached input ready', reconnecting:'Waiting to retry input sync'}[job.upload_progress?.stage] || 'Checking GPU input cache')
      : {preparing_files:'Preparing file synchronization', preparing_worker:'Transferring job metadata', checking_cache:'Checking cache', packing:'Preparing transfer archive', installing:'Installing input data', verifying:'Verifying input data', reconnecting:'Waiting to retry upload'}[job.upload_progress?.stage]) : null;
    renderOptimizeLogDashboard({name:job.config_name, phase:stopPhase(job) || uploadPhase || job.status,
      progress:{exact_evaluations:job.exact_completed || 0,target_exact_evaluations:job.iterations,
        proxy_evaluations:job.gpu_candidates || 0,percent:job.iterations ? 100*(job.exact_completed || 0)/job.iterations : 0},
      runtime:{backend:'gpu',algorithm:'Vast.ai',config_n_cpus:job.auto_cpu_workers && !job.cpu_allocation_resolved ? null : job.workers},
      process:{started_at:job.started_at ? new Date(job.started_at*1000).toISOString() : null},
      queue:{running:jobRows.filter(row => ['running','provisioning','uploading','collecting'].includes(row.status)).length,
        queued:jobRows.filter(row => row.status==='ready').length,error:jobRows.filter(row => row.status==='failed').length},
      log:{last_error:jobLastError(job, rentalWorker),last_line:jobActivity(job, rentalWorker),
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
      (config.convergence_tolerance_pct ?? 0.25) +
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
      const started = job.dispatch_at || (rentalWorker && rentalWorker.id === job.lease_id ? rentalWorker.started_at : null);
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
      const metrics = uploadProgressMetrics(job);
      const {upload, total, rsync, directFiles, sent, checkingCache, filesTotal, filesChecked, fileProgress, percent, measured, network} = metrics;
      const verified = upload.bytes === total && total > 0;
      const stageLabel = {preparing_files:'Preparing file synchronization', preparing_worker:'Transferring job metadata', checking_cache:'Checking cached input data', packing:'Preparing transfer archive', installing:'Installing input data', synchronized:'Input files synchronized'}[upload.stage];
      const retrySeconds = Math.max(0, Math.ceil((job.upload_retry_at || 0) - Date.now()/1000));
      const label = upload.stage === 'verifying' ? 'Verifying input data' : upload.stage === 'reconnecting' ?
        (job.upload_retry_at ? 'Upload retry in ' + retrySeconds + 's (partial data retained)' :
          rsync ? 'Reconnecting upload (partial data retained)' : 'Reconnecting upload (verified chunks retained)') : directFiles ? 'Synchronizing input files' : 'Uploading input';
      let eta = '', speed = '';
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
      const metadata = upload.stage === 'preparing_worker';
      const metadataDetail = metadata ? 'Job metadata · ' + fmt(Number(upload.metadata_submitted || 0) / 1e6, 2) + ' / ' + fmt(Number(upload.metadata_total || 0) / 1e6, 2) + ' MB submitted to SSH · waiting for worker acknowledgement' : '';
      const directDetail = directFiles && !metadata && upload.stage !== 'reconnecting'
        ? directFileSyncText(metrics, ['installing', 'synchronized'].includes(upload.stage)) : '';
      const detail = metadataDetail || cacheDetail || directDetail || stageLabel || label + ' · ' + (sent != null ? fmt(sent / 1e6, 1) + ' / ' : '') + fmt(total / 1e6, 1) + ' MB'
        + (sent != null ? (directFiles ? ' processed (' : rsync && !verified ? ' transferred (' : ' verified (') + fmt(percent, 1) + '%)' : '')
        + (upload.in_flight_bytes > 0 ? ' · current block: ' + fmt(upload.in_flight_bytes / 1e6, 2) + ' MB received' : '')
        + speed + eta;
      el('optlog-progress-fill').style.width = (metadata && upload.metadata_total > 0 ? Math.min(100, 100 * (upload.metadata_submitted || 0) / upload.metadata_total) : percent) + '%';
      el('optlog-progress-label').textContent = detail;
      el('optlog-progress-label').dataset.tip = metadata ? 'Compressed config and file manifest submitted to the local SSH process. This is not confirmation of remote receipt. Input candle transfer starts after worker acknowledgement.' : checkingCache ? 'Cache checking counts files acknowledged by the worker, before input transfer begins. Each confirmed page updates the counter; this is not uploaded bytes.' : 'Progress counts checksum-verified blocks. Current block bytes are acknowledged by the receiver and may be retried. Speed is verified data per second during this attempt. Remaining time uses remaining verified bytes divided by measured speed and excludes final verification/install. The theoretical remaining time uses the rented host download Mbps and is shown alongside the measured estimate. 1 byte = 8 bits. Local uplink, network route, SSH overhead and retries can limit throughput; the reached percentage alone does not prove an inaccurate host claim.';
      if (rsync && !metadata) el('optlog-progress-label').dataset.tip = 'Rsync resumes the partial archive over one SSH connection. Progress and estimated speed come from rsync, excluding the retained prefix; buffered data may still be in transit. The complete archive is SHA256-verified before installation. Remaining time excludes this final check and installation. Host-rate remaining uses advertised download Mbps and is theoretical; local uplink and routing also limit speed. 1 byte = 8 bits.';
      if (directFiles && !metadata) el('optlog-progress-label').dataset.tip = 'Rsync compares immutable content-hash files with the GPU cache and sends only missing data. While synchronization runs, the bar shows logical data compared, not network traffic. After rsync finishes, PBGui shows its actual sent and reused byte totals.';
      el('optlog-activity').textContent = detail;
    }
    const rentalLog = rentalWorker && rentalWorker.has_provider_log && !['none','deletion_verified'].includes(rentalWorker.rental_state)
      && (job.lease_id === rentalWorker.id || (!job.lease_id && ['preparing','ready'].includes(job.status)));
    const optimizerLog = job.has_log ? 'optimizes_v8/vast_' + job.id + '.log'
      : job.has_provider_log ? 'optimizes_v8/vast_' + job.id + '_provider.log'
      : rentalLog ? 'optimizes_v8/vast_' + rentalWorker.id + '_provider.log' : '';
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
    if (optimizerLog && typeof ensureLogViewer === 'function') {
      el('log-waiting').hidden = true;
      el('log-viewer-target').hidden = false;
      const viewer = ensureLogViewer(); viewer.open();
      if (window.state.logFile !== optimizerLog) {
        viewer.setHost('local');
        window.state.logFile = optimizerLog;
        viewer.setFile(optimizerLog);
      }
    }
  }

  function observedOptimizerJob(item) {
    const observed = currentObservedOptimizer(item) || {};
    return {
      id:item.id,
      config_name:observed.name || 'Observed optimizer',
      status:'running',
      iterations:0,
      exact_completed:Number(observed.exact_completed) || 0,
      gpu_candidates:Number(observed.gpu_candidates) || 0,
      workers:null,
      auto_cpu_workers:true,
      cpu_allocation_resolved:false,
      runtime_metrics:item.runtime_metrics || {},
      exact_queue:{},
      throughput:null,
      convergence_config:{convergence_enabled:false},
      convergence:{},
      observed_optimizer:observed,
      rental:item,
      lease_id:null,
      has_log:true,
      has_provider_log:false,
      result_path:null,
      updated_at:observed.sampled_at || null
    };
  }

  function renderObservedOptimizer(item) {
    const observed = currentObservedOptimizer(item);
    if (!window.state || window.state.cloudLogId !== item?.id || !observed) return;
    renderCloudDashboard(observedOptimizerJob(item));
    el('optlog-activity').textContent = observed.activity || 'Optimizer process detected';
    el('stop-job').disabled = true;
    el('recover-job').disabled = true;
    el('requeue-job').hidden = true;
    el('start-job').hidden = true;
    el('job-results').hidden = true;
  }

  function openObservedOptimizer(item) {
    const observed = currentObservedOptimizer(item);
    if (!observed || typeof openLogPanel !== 'function') return;
    openLogPanel(item.id, observed.name || 'Observed optimizer', {cloud:true,hasLog:true});
    renderObservedOptimizer(item);
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
    return jobRows.map(job => ({cloudJob:job, filename:'vast:' + job.id, name:job.config_name,
      status:({ready:'queued',completed:'complete',failed:'error'})[job.status] || job.status,
      exchange:job.exchange || '', created:job.created_at ? new Date(job.created_at * 1000).toISOString() : ''}));
  }

  function cloudQueueRow(item) {
    const job = item.cloudJob, row = document.createElement('tr');
    row.dataset.cloudId = job.id;
    row.dataset.filename = item.filename;
    if (window.state?.selectedQueue?.has(item.filename)) row.classList.add('selected');
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
          if (progress.stage === 'compressing') {
            const rate = progress.bytes_per_second == null ? NaN : Number(progress.bytes_per_second);
            const eta = progress.eta_seconds == null ? NaN : Number(progress.eta_seconds);
            label.textContent = determinate
              ? 'Compressing input archive · ' + fmt(completed / 1e6, 1) + ' / ' + fmt(total / 1e6, 1) + ' MB · ' + Math.round(percent) + '%'
              : 'Compressing input archive…';
            if (Number.isFinite(rate) && rate > 0) label.textContent += ' · ' + fmt(rate / 1e6, 1) + ' MB/s';
            if (Number.isFinite(eta) && eta >= 0) {
              const seconds = Math.ceil(eta);
              label.textContent += ' · ~' + (seconds >= 3600 ? Math.floor(seconds / 3600) + 'h ' : '')
                + (seconds >= 60 ? Math.floor(seconds % 3600 / 60) + 'm ' : '') + seconds % 60 + 's remaining';
            }
          } else label.textContent = determinate
            ? fmt(completed / 1e6, 1) + ' / ' + fmt(total / 1e6, 1) + ' MB · ' + Math.round(percent) + '%'
            : 'Selecting input files…';
          if (Number.isFinite(filesTotal) && filesTotal > 0) label.textContent += ' · ' + (Number.isFinite(filesCompleted) ? Math.max(0, filesCompleted) : 0) + ' / ' + filesTotal + ' files';
          details.append(track, label); cell.appendChild(details);
        }
      } else cell.textContent = value;
      row.appendChild(cell);
    });
    const estimate = document.createElement('td');
    estimate.title = 'Estimated coin candles per full candidate evaluation, summed across active training scenarios. Excludes warm-up and actual data availability; not the total optimizer run.';
    estimate.textContent = Number.isFinite(job.estimated_coin_candles) ? job.estimated_coin_candles.toLocaleString(undefined, {notation:'compact', maximumFractionDigits:2}) : '—';
    row.appendChild(estimate);
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
    if (item.status === 'queued' && job.kind !== 'calibration') {
      button('⚙', 'Tune GPU with this frozen input', () => startQueuedCalibration(job));
    }
    if (job.kind !== 'calibration' && ['error','cancelled'].includes(item.status) && job.can_delete) {
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
      await deleteCloudQueueItems(['vast:' + job.id]);
    } catch (error) { if (!disposed) message(error.message, true); }
    finally {
      deletingJobs.delete(job.id);
      if (!disposed) { renderJob(); renderQueueOverview(); }
    }
  }

  async function deleteCloudQueueItems(queueKeys) {
    if (disposed) return;
    const requested = new Set(Array.isArray(queueKeys) ? queueKeys : []);
    const jobs = jobRows.filter(job => requested.has('vast:' + job.id));
    if (jobs.length !== requested.size || jobs.some(job => !job.can_delete)) {
      throw new Error('One or more selected Vast.ai jobs cannot be deleted in their current state.');
    }
    jobs.forEach(job => deletingJobs.add(job.id));
    renderJob(); renderQueueOverview();
    try {
      if (disposed) return;
      await request('/jobs/delete', {method:'POST', body:JSON.stringify({ids:jobs.map(job => job.id)})});
      if (window.state && jobs.some(job => window.state.cloudLogId === job.id)) closeLog();
      if (!disposed) await refreshJobs();
    } finally {
      jobs.forEach(job => deletingJobs.delete(job.id));
      if (!disposed) { renderJob(); renderQueueOverview(); }
    }
  }

  window.PBGuiVast = {
    closeLog,
    deleteQueueItems: deleteCloudQueueItems,
    queueItems: cloudQueueItems,
    queueRow: cloudQueueRow,
    scheduleValidation,
    validateConfig: validateEditorConfig,
    metricAllowed: function (metric) { return !!cloudMetrics && cloudMetrics.includes(metric); },
    hideSettings: function () {
      settingsVisible = false; clearSecrets(); closeLog(); performanceView?.hide();
      el('vast-performance-actions').hidden = true;
      el('vast-settings-nav').querySelectorAll('[data-vast-view]').forEach(button => { button.classList.remove('active'); button.setAttribute('aria-pressed','false'); });
    },
    showSettings,
    openEditor: function (config) {
      const execution = el('opted-execution');
      if (execution) execution.value = config && config.pbgui && config.pbgui.execution === 'vast' ? 'vast' : 'local';
      this.updateEditor(); renderJob();
    },
    updateEditor: function () {
      const execution = el('opted-execution'), target = el('opted-vast-worker');
      if (target) target.hidden = !execution || execution.value !== 'vast';
      const cloud = !!execution && execution.value === 'vast';
      const backend = el('opted-opt-backend');
      if (backend) {
        if (cloud && !Array.from(backend.options || []).some(option => option.value === 'gpu')) {
          const option = document.createElement('option');
          option.value = 'gpu';
          option.textContent = 'gpu (Vast.ai worker)';
          backend.appendChild(option);
        }
        if (cloud && backend.value !== 'gpu') {
          backend.value = 'gpu';
          backend.dispatchEvent(new Event('change', {bubbles:true}));
        }
        backend.disabled = cloud;
        const backendRow = backend.closest?.('.form-row');
        if (backendRow) backendRow.style.display = cloud ? 'none' : '';
      }
      if (typeof updateOptimizeBackendSections === 'function') updateOptimizeBackendSections();
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
            ? 'Disabled because CPU allocation is automatic on Vast.ai. Set Min CPU cores in GPU & Offers to choose the minimum rental capacity. The cloud run uses the effective CPU allocation available to the container, capped by the rented CPU quota. This local setting is not used.'
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
    closeEditor: function () { validationGeneration++; gpuRecommendationGeneration++; gpuRecommendation = null; clearTimeout(validationTimer); setQueueBlocked(false); },
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
  async function startQueuedCalibration(job) {
    if (disposed || calibrationStarting || !job || job.status !== 'ready') return;
    if (!selectedOffer) {
      message('Select an offer under GPU & Offers, then tune this prepared queue item.', true);
      showSettings('offers');
      return;
    }
    if (!calibrationInfo?.configurable_worker) {
      message('Exact-input tuning needs the published protocol-4 worker; no rental was started.', true);
      return;
    }
    if (!savedPreferences || !supervision || !window.PBGuiDialogs?.confirm) {
      message('Rental settings or supervision are unavailable.', true); return;
    }
    const selection = calibrationSelection();
    const search = selection.legacy
      ? {start:5632, step:512, maximum:12288, gain:5, timeout:3600}
      : selection;
    const offer = selectedOffer;
    const accepted = await window.PBGuiDialogs.confirm({
      title:'Tune GPU for this queued job?',
      message:'Create a separate immutable test copy of "' + job.config_name
        + '" using exactly its already prepared data. The original queued job will not run or change. Rent '
        + offer.gpu_name + ' on machine ' + offer.machine_id + ' for up to '
        + savedPreferences.hours + ' hours and a $' + fmt(savedPreferences.budget,2)
        + ' budget target? Test populations ' + search.start.toLocaleString() + ' to '
        + search.maximum.toLocaleString() + ' in ' + search.step.toLocaleString()
        + ' steps, requiring ' + search.gain + '% gain and one complete generation per case. '
        + 'Each case has a ' + search.timeout + '-second timeout. The rental is deleted when finished.',
      confirmText:'Rent and tune this job'
    });
    if (!accepted || disposed || selectedOffer?.id !== offer.id || jobRows.find(row => row.id === job.id)?.status !== 'ready') return;
    calibrationStarting = true; renderJob();
    try {
      const data = await request('/calibration/start-configurable', {method:'POST', body:JSON.stringify({
        offer:calibrationOffer(offer), hours:Number(savedPreferences.hours),
        budget:Number(savedPreferences.budget), source_job_id:job.id,
        start_population:search.start, population_step:search.step,
        max_population:search.maximum, min_scale_gain:search.gain / 100,
        case_timeout_seconds:search.timeout, accept_rental_and_cleanup:true,
      })});
      selectedJobId = data.job.id;
      await refreshJobs(data.job.id);
      if (!disposed) message('Exact-input tuning started. The original queue item is unchanged.');
    } catch (error) {
      if (!disposed) { message(error.message, true); await refreshJobs(); }
    } finally {
      calibrationStarting = false;
      if (!disposed) renderJob();
    }
  }
  async function startCloudQueue(job) {
    if (renting || startingQueue || workerAction || disposed || !supervision || (!savedPreferences && !(worker && !['none','deletion_verified'].includes(worker.rental_state))) || (job && deletingJobs.has(job.id))) return;
    if (((savedPreferences?.max_rentals || 1) > 1 || queueState.pool_authorization) && !(await window.PBGuiDialogs.confirm({
      title:'Start GPU pool',
      message:'Automatically rent up to ' + savedPreferences.max_rentals + ' GPUs for queued jobs, with a budget target of $' + fmt(savedPreferences.budget,2) + ' per rental (up to $' + fmt(savedPreferences.max_rentals * savedPreferences.budget,2) + ' simultaneously)? Each rental lasts at most ' + savedPreferences.hours + ' hours. Replacement rentals may be started while jobs remain queued. Pause queue prevents new rentals; End rentals stops the pool.',
      confirmText:'Start GPU pool'
    }))) return;
    if (disposed || startingQueue) return;
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
    if (!calibrationInfo?.rental_profile) { message('Wait for the selected GPU profile.', true); return; }
    let jobProfileOverrides;
    try { jobProfileOverrides = gpuJobOverrides(calibrationInfo.queued_gpu_previews, offerJobSelections); }
    catch (error) { message(error.message, true); return; }
    renting = true; renderJob();
    try {
      if (!savedPreferences) throw new Error('Rental defaults are still loading.');
      await request('/queue/start', {method:'POST', body:JSON.stringify({
        rent_only:true, offer_id:offer.id, accept_rental_and_cleanup:true,
        gpu_job_profile_overrides:jobProfileOverrides,
        preferences:{gpu_name:offer.gpu_name, max_price:offer.price_hour_usd,
          min_vram:offer.vram_gb ?? 0, min_ram:offer.ram_gb ?? 0, min_cpu:offer.cpu_cores ?? 0,
          min_tflops:offer.tflops ?? 0,
          disk_gb:offer.disk_gb ?? Number(el('disk').value), verified_only:!!offer.verified},
        hours:savedPreferences.hours, budget:savedPreferences.budget, idle_seconds:savedPreferences.idle_seconds
      })});
      await refreshJobs();
      if (!disposed) message('GPU rental started. Queued jobs remain paused until Start queue.');
    } catch (error) { if (!disposed) { message(error.message,true); await refreshJobs(); } }
    finally { renting=false; if (!disposed) { renderJob(); renderQueueOverview(); } }
  });
  el('calibration-watch-start').addEventListener('click', async () => {
    if (disposed || calibrationStarting || queueState.calibration_watch) return;
    if (!el('offers-form').reportValidity() || !savedPreferences) return;
    const preferences = {};
    preferenceGroups.offers.forEach(key => {
      const value = el(preferenceFields[key]).value;
      preferences[key] = key === 'gpu_name' ? value.trim()
        : key === 'verified_only' ? value === 'true' : Number(value);
    });
    if (!preferences.gpu_name) { message('Enter a GPU type to watch.', true); return; }
    const hours = Number(savedPreferences.hours), budget = Number(savedPreferences.budget);
    if (hours < .75 || hours * preferences.max_price > budget + 1e-6) {
      message('The rental budget must cover the selected maximum price for at least 0.75 hours.', true);
      return;
    }
    if (!window.PBGuiDialogs?.confirm) { message('Rental confirmation unavailable. Reload this page.', true); return; }
    const accepted = await window.PBGuiDialogs.confirm({
      title:'Wait for a matching GPU and run the performance test?',
      message:'PBGui will check about every 30 seconds until you cancel. When an offer for '
        + preferences.gpu_name + ' with at least ' + preferences.min_power_watts + ' advertised W, at least '
        + preferences.min_reliability_pct + '% host reliability and at most $' + fmt(preferences.max_price, 4)
        + '/hour appears, it may rent automatically without another confirmation. This authorizes one rental for at most '
        + hours + ' hours with a $' + fmt(budget, 2)
        + ' budget target plus provider transfer charges. PBGui measures the actual GPU limit after renting and stops the test if it is below your minimum. The frozen criteria remain in effect even if you change these fields later. Automatic queue rentals pause while waiting.',
      confirmText:'Authorize one future test rental'
    });
    if (!accepted || disposed || queueState.calibration_watch) return;
    calibrationStarting = true; renderJob();
    try {
      const data = await request('/calibration/watch', {method:'POST', body:JSON.stringify({
        preferences, hours, budget, accept_rental_and_cleanup:true
      })});
      selectedJobId = data.job.id;
      await refreshJobs(data.job.id);
      if (!disposed) message('');
    } catch (error) {
      if (!disposed) { message(error.message, true); await refreshJobs(); }
    } finally {
      calibrationStarting = false;
      if (!disposed) renderJob();
    }
  });
  el('calibration-watch-cancel').addEventListener('click', async () => {
    const watchId = queueState.calibration_watch?.id;
    if (!watchId || disposed || watchCancelling) return;
    watchCancelling = true; renderJob();
    try {
      await request('/calibration/watch/cancel', {method:'POST', body:JSON.stringify({watch_id:watchId})});
      if (!disposed) { await refreshJobs(); message(''); }
    } catch (error) {
      if (!disposed) { message(error.message, true); await refreshJobs(); }
    } finally {
      watchCancelling = false;
      if (!disposed) renderJob();
    }
  });
  el('calibration-start').addEventListener('click', async () => {
    const selection = calibrationSelection();
    if (calibrationStarting || disposed || !selectedOffer) return;
    if (!(selection.legacy ? calibrationInfo?.calibration_worker : calibrationInfo?.configurable_worker)) {
      message('The selected test requires a published, pinned calibration worker.', true); return;
    }
    for (const key of calibrationFields) {
      if (!el('calibration-' + key).reportValidity()) return;
    }
    if (!selection.legacy && selection.timeout < 600) {
      message('Set a case timeout of at least 600 seconds for configurable tests.', true); return;
    }
    if (!savedPreferences) { message('Rental defaults are still loading.', true); return; }
    if (Number(savedPreferences.hours) < .75) {
      message('Set Maximum rental hours to at least 0.75 in Rental & Automation before calibration.', true); return;
    }
    if (!window.PBGuiDialogs?.confirm) { message('Calibration confirmation unavailable. Reload this page.', true); return; }
    const offer = selectedOffer;
    const workload = selection.legacy
      ? 'fixed Small EMA-anchor reference (3 coins, Binance, 2024)'
      : selection.preset + ' EMA-anchor reference with population ' + selection.start.toLocaleString()
        + ' to ' + selection.maximum.toLocaleString() + ' in steps of ' + selection.step.toLocaleString()
        + ', at least ' + selection.gain + '% gain and up to ' + selection.timeout + ' seconds per case';
    const accepted = await window.PBGuiDialogs.confirm({
      title:calibrationInfo?.match ? 'Repeat GPU performance test?' : 'Run GPU performance test?',
      message:'Rent exactly ' + offer.gpu_name + ' on machine ' + offer.machine_id + ' at up to $'
        + fmt(offer.price_hour_usd,4) + '/hour for at most ' + savedPreferences.hours
        + ' hours and a $' + fmt(savedPreferences.budget,2) + ' budget target? PBGui will prepare the '
        + workload + ' only after this click, then measure full one-batch generations when using the configurable mode. The rental is deleted when finished. The result needs explicit acceptance.',
      confirmText:'Run performance test'
    });
    if (!accepted || disposed || selectedOffer?.id !== offer.id) return;
    calibrationStarting = true; renderJob();
    try {
      const body = {
        offer:calibrationOffer(offer), hours:Number(savedPreferences.hours),
        budget:Number(savedPreferences.budget), accept_rental_and_cleanup:true,
      };
      const endpoint = selection.legacy ? '/calibration/start' : '/calibration/start-configurable';
      if (!selection.legacy) Object.assign(body, {
        preset:selection.preset, start_population:selection.start, population_step:selection.step,
        max_population:selection.maximum, min_scale_gain:selection.gain / 100,
        case_timeout_seconds:selection.timeout,
      });
      const data = await request(endpoint, {method:'POST', body:JSON.stringify(body)});
      selectedJobId = data.job.id;
      await refreshJobs(data.job.id);
      if (!disposed) message('Preparing the selected reference input. Queue shows progress; no rental starts until preparation succeeds.');
    } catch (error) {
      if (!disposed) { message(error.message, true); await refreshJobs(); }
    } finally {
      calibrationStarting = false;
      if (!disposed) { renderJob(); void refreshCalibration(); }
    }
  });
  el('calibration-accept').addEventListener('click', async () => {
    const identifier = el('calibration-accept').dataset.jobId;
    const offer = selectedOffer;
    if (!identifier || !offer || disposed || latestCompletedCalibration({pendingOnly:true})?.id !== identifier) return;
    if (!window.PBGuiDialogs?.confirm) { message('Profile confirmation unavailable. Reload this page.', true); return; }
    const job = jobRows.find(item => item.id === identifier);
    const population = job?.calibration_profile_candidate?.population_size;
    if (!await window.PBGuiDialogs.confirm({title:'Use local GPU profile?',
      message:'Use population ' + Number(population).toLocaleString() + ' for future exact matches of this runtime-verified GPU variant and workload? The PBGui reference remains unchanged.',
      confirmText:'Use local profile'})) return;
    if (disposed || selectedOffer !== offer || latestCompletedCalibration({pendingOnly:true})?.id !== identifier) return;
    try {
      await request('/calibration/accept', {method:'POST', body:JSON.stringify({job_id:identifier})});
      await refreshJobs(identifier); await refreshCalibration();
      if (!disposed) message('Local GPU performance profile accepted.');
    } catch (error) { if (!disposed) message(error.message, true); }
  });
  el('cloud-calibration-accept').addEventListener('click', async () => {
    const identifier = el('cloud-calibration-accept').dataset.jobId;
    const job = jobRows.find(row => row.id === identifier);
    if (!job || !completedCalibration(job) || job.calibration_profile_accepted_at || disposed) return;
    if (!window.PBGuiDialogs?.confirm) { message('Profile confirmation unavailable. Reload this page.', true); return; }
    const candidate = job.calibration_profile_candidate;
    if (!await window.PBGuiDialogs.confirm({
      title:'Save measured GPU settings for this workload?',
      message:'Save population and batch ' + Number(candidate.population_size).toLocaleString()
        + ' with measured work cap ' + Number(candidate.max_dispatch_candidate_bars || 0).toLocaleString()
        + ' candidate-bars as a local profile for this exact workload and runtime GPU variant? '
        + 'The original queued job remains unchanged.',
      confirmText:'Save local profile',
    })) return;
    if (disposed || selectedJob()?.id !== identifier) return;
    try {
      await request('/calibration/accept', {method:'POST', body:JSON.stringify({job_id:identifier})});
      await refreshJobs(identifier);
      if (!disposed) message('Local workload-specific GPU profile saved. The original queue item was not changed.');
    } catch (error) { if (!disposed) message(error.message, true); }
  });
  el('settings-end-rental')?.addEventListener('click', () => el('end-worker')?.click());
  el('start-job').addEventListener('click', () => startCloudQueue(selectedJob()));
  ['stop', 'recover'].forEach(action => el(action + '-job').addEventListener('click', async () => {
    const job = selectedJob(); if (!job) return;
    if (action === 'stop' && stopPhase(job)) return;
    if (action === 'stop') {
      if (!window.PBGuiDialogs?.confirm) { message('Stop confirmation unavailable. Reload this page.', true); return; }
      const accepted = await window.PBGuiDialogs.confirm({
        title:'Stop optimizer?',
        message:'Stop "' + job.config_name + '" and collect its current results? The optimizer cannot continue after this action.',
        confirmText:'Stop & collect'
      });
      if (!accepted || disposed || selectedJobId !== job.id || stopPhase(job)) return;
    }
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
    if (action === 'resume' && savedPreferences && ((savedPreferences.max_rentals || 1) > 1 || queueState.pool_authorization)) {
      await startCloudQueue(); return;
    }
    workerAction = action; renderJob(); renderQueueOverview();
    message(el(action + '-worker').textContent);
    try {
      const autoRentEdit = preferenceEdits.auto_rent;
      const updated = await request('/queue/' + action, {method:'POST'});
      if (action === 'end' && updated.gpu_preferences) {
        applyPreferences(updated.gpu_preferences, ['auto_rent'], {auto_rent:autoRentEdit});
      }
      if (!disposed) { message(''); await refreshJobs(); }
    } catch (error) { if (!disposed) message(error.message, true); }
    finally {
      workerAction = null;
      if (!disposed) { renderJob(); renderQueueOverview(); }
    }
  }));
  if (new URLSearchParams(location.search).get('view') === 'queue' && !location.hash && window.setPanel) window.setPanel('queue');
  if (window.state?.panel === 'vast') showSettings(window.state.vastView);
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
