
(function() {
    var ROOT = document.getElementById('__HLDA_ROOT__');
    if (!ROOT) { console.error('[HLDA] Root element not found'); return; }

    // Config from data-* attributes (getAttribute — matches market_data_status.html pattern)
    var TOKEN = ROOT.getAttribute('data-token') || '';
    var API_BASE = ROOT.getAttribute('data-api-base') || '';
    var API_HOST = ROOT.getAttribute('data-api-host') || '';
    var P = '__HLDA__';
    var STORAGE_KEY = 'pbgui_hl_data_sections';

    var dlCoins = [], dlSelected = new Set(), dlArchive = {oldest_day:'',newest_day:''}, dlHasCreds = false;
    var buildCoins = [], buildSelected = new Set();
    var expandedJobs = { dl: new Set(), build: new Set() };
    var activeJobs = { dl: [], build: [] };
    var currentTab = { dl: 'running', build: 'running' };
    var ws = null, wsRetryCount = 0;

    function $(id) { return document.getElementById(P + id); }

    function loadState() { try { return JSON.parse(localStorage.getItem(STORAGE_KEY)||'{}'); } catch(e) { return {}; } }
    function saveState(id, open) { try { var s=loadState(); s[id]=open; localStorage.setItem(STORAGE_KEY, JSON.stringify(s)); } catch(e) {} }
    function isOpen(id) { var s=loadState(); return s[id] || false; }

    function fmtDay(d) { return (!d||d.length!==8)?'':d.slice(0,4)+'-'+d.slice(4,6)+'-'+d.slice(6,8); }
    function inputToDay(v) { return v?v.replace(/-/g,''):''; }

    // ── Init sections open/closed state ──
    ['download','build'].forEach(function(id) {
        var sec = $('sec-'+id);
        if (sec && isOpen(id)) sec.classList.add('open');
    });

    function toggleSection(id) {
        var el = $('sec-'+id);
        if (!el) return;
        var open = el.classList.toggle('open');
        saveState(id, open);
    }

    // ══════════════════════════════════════════════════════════════
    //  DIRECT addEventListener for static elements
    //  (belt-and-suspenders — also keep delegation below)
    // ══════════════════════════════════════════════════════════════
    var modalClose = ROOT.querySelector('.hlda-modal-close');
    if (modalClose) modalClose.addEventListener('click', function() { closeModal(); });
    
    var shDownload = $('sh-download');
    if (shDownload) shDownload.addEventListener('click', function() { toggleSection('download'); });
    
    var shBuild = $('sh-build');
    if (shBuild) shBuild.addEventListener('click', function() { toggleSection('build'); });

    // ══════════════════════════════════════════════════════════════
    //  CENTRAL EVENT DELEGATION for dynamic content
    // ══════════════════════════════════════════════════════════════
    ROOT.addEventListener('click', function(e) {
        var t = e.target.closest('[data-action]');
        if (!t) return;
        var action = t.getAttribute('data-action');
        var ns = t.getAttribute('data-ns') || '';
        switch (action) {
            case 'toggle-section':
                // Handled directly above via explicit addEventListener
                break;
            case 'close-modal':
                // handled by direct addEventListener above; skip here
                break;
            case 'toggle-dd':
                toggleDD(ns);
                break;
            case 'cs-all':
                e.stopPropagation(); csAll(ns);
                break;
            case 'cs-clear':
                e.stopPropagation(); csClear(ns);
                break;
            case 'cs-toggle':
                e.stopPropagation();
                csToggle(ns, t.getAttribute('data-coin'));
                break;
            case 'cs-remove':
                e.stopPropagation(); e.preventDefault();
                csRemove(ns, t.getAttribute('data-coin'));
                break;
            case 'submit-dl':
                submitDL();
                break;
            case 'submit-build':
                submitBuild();
                break;
            case 'switch-tab':
                switchTab(ns, t.getAttribute('data-tab'));
                break;
            case 'show-log':
                showLog(t.getAttribute('data-job'));
                break;
            case 'cancel-job':
                cancelJob(t.getAttribute('data-job'));
                break;
            case 'delete-job':
                deleteJob(t.getAttribute('data-job'));
                break;
            case 'retry-job':
                retryJob(t.getAttribute('data-job'));
                break;
            case 'requeue-job':
                requeueJob(t.getAttribute('data-job'));
                break;
            case 'tog-exp':
                e.stopPropagation();
                togExp(ns, t.getAttribute('data-job'), t);
                break;
        }
    });

    ROOT.addEventListener('input', function(e) {
        var t = e.target.closest('[data-action]');
        if (!t) return;
        if (t.getAttribute('data-action') === 'cs-filter') {
            csFilter(t.getAttribute('data-ns'));
        }
    });

    // Close dropdowns on outside click
    document.addEventListener('click', function(e) {
        ['dl','build'].forEach(function(ns) {
            var sel = $('cs-'+ns);
            if (sel && !sel.contains(e.target)) { var dd=$('dd-'+ns); if(dd) dd.classList.remove('open'); }
        });
    });

    // ══════════════════════════════════════════════════════════════
    //  INIT
    // ══════════════════════════════════════════════════════════════
    (async function init() {
        try {
            var r = await Promise.all([
                fetch(API_BASE + '/heatmap/l2book-download-info?token=' + TOKEN),
                fetch(API_BASE + '/heatmap/build-ohlcv-info?token=' + TOKEN),
            ]);
            if (!r[0].ok) throw new Error('Download info HTTP ' + r[0].status);
            if (!r[1].ok) throw new Error('Build info HTTP ' + r[1].status);
            var dlD = await r[0].json(), bD = await r[1].json();
            dlCoins = dlD.coins||[]; dlHasCreds = dlD.has_aws_creds;
            dlArchive = dlD.archive_range||{oldest_day:'',newest_day:''};
            buildCoins = bD.eligible_coins||[];
            populateDownload(); populateBuild();
            connectWS();
        } catch(e) {
            var err = '<div class="hlda-msg error" style="display:block">Failed to load: ' + escHtml(e.message) + '</div>';
            var dl=$('body-download'), bl=$('body-build');
            if(dl) dl.innerHTML=err; if(bl) bl.innerHTML=err;
        }
    })();

    // ══════════════════════════════════════════════════════════════
    //  POPULATE: Download l2Book
    // ══════════════════════════════════════════════════════════════
    function populateDownload() {
        var body = $('body-download');
        if (!body) return;
        if (!dlHasCreds) {
            body.innerHTML = '<div class="hlda-nocreds">⚠ No AWS credentials configured. Go to <strong>Settings (l2Book)</strong> above to enter your Access Key ID and Secret Access Key.</div>';
            return;
        }
        if (!dlCoins.length) {
            body.innerHTML = '<div class="hlda-msg warning" style="display:block">No enabled Hyperliquid coins configured.</div>';
            return;
        }
        var oldest = dlArchive.oldest_day ? fmtDay(dlArchive.oldest_day) : '?';
        var newest = dlArchive.newest_day ? fmtDay(dlArchive.newest_day) : '?';
        body.innerHTML =
            '<div class="hlda-fs">' +
                '<span class="hlda-lbl">Coins</span>' +
                '<div class="hlda-cs" id="' + P + 'cs-dl">' +
                    '<div class="hlda-ct" id="' + P + 'ct-dl" data-action="toggle-dd" data-ns="dl"><span class="ph">Select coins…</span></div>' +
                    '<div class="hlda-dd" id="' + P + 'dd-dl">' +
                        '<div class="hlda-ddh"><button data-action="cs-all" data-ns="dl">All</button><button data-action="cs-clear" data-ns="dl">Clear</button></div>' +
                        '<input type="text" class="hlda-dds" id="' + P + 'search-dl" placeholder="Search…" data-action="cs-filter" data-ns="dl">' +
                        '<div id="' + P + 'opts-dl"></div>' +
                    '</div>' +
                '</div>' +
            '</div>' +
            '<div class="hlda-dr">' +
                '<div class="hlda-fs"><span class="hlda-lbl">Start date</span><input type="date" id="' + P + 'dl-sd" value="' + fmtDay(dlArchive.oldest_day) + '"><div class="hlda-hint">Archive oldest: ' + oldest + '</div></div>' +
                '<div class="hlda-fs"><span class="hlda-lbl">End date</span><input type="date" id="' + P + 'dl-ed" value="' + fmtDay(dlArchive.newest_day) + '"><div class="hlda-hint">Archive newest: ' + newest + '</div></div>' +
            '</div>' +
            '<div class="hlda-cb"><input type="checkbox" id="' + P + 'dl-only" checked><label for="' + P + 'dl-only">Only missing 1m_src hours</label></div>' +
            '<div class="hlda-help">Downloads only l2Book hours without minute coverage in 1m_src yet. Skips days older than your local oldest l2Book day.</div>' +
            '<div class="hlda-ar"><button class="hlda-btn" id="' + P + 'dl-btn" data-action="submit-dl">Download</button><div class="hlda-lo" id="' + P + 'dl-lo"><div class="hlda-spin"></div><span>Preflight check…</span></div></div>' +
            '<div class="hlda-msg" id="' + P + 'dl-msg"></div>' +
            renderJobMonitorHTML('dl');
        renderCoinOpts('dl', dlCoins, dlSelected);
        updateTrigger('dl', dlCoins, dlSelected);
    }

    // ══════════════════════════════════════════════════════════════
    //  POPULATE: Build best 1m OHLCV
    // ══════════════════════════════════════════════════════════════
    function populateBuild() {
        var body = $('body-build');
        if (!body) return;
        if (!buildCoins.length) {
            body.innerHTML = '<div class="hlda-msg warning" style="display:block">No eligible coins found. XYZ symbols require Tiingo mapping with status \'ok\'.</div>';
            return;
        }
        body.innerHTML =
            '<div class="hlda-fs">' +
                '<span class="hlda-lbl">Coins for build</span>' +
                '<div class="hlda-cs" id="' + P + 'cs-build">' +
                    '<div class="hlda-ct" id="' + P + 'ct-build" data-action="toggle-dd" data-ns="build"><span class="ph">All (' + buildCoins.length + ' coins)</span></div>' +
                    '<div class="hlda-dd" id="' + P + 'dd-build">' +
                        '<div class="hlda-ddh"><button data-action="cs-all" data-ns="build">All</button><button data-action="cs-clear" data-ns="build">Clear</button></div>' +
                        '<input type="text" class="hlda-dds" id="' + P + 'search-build" placeholder="Search…" data-action="cs-filter" data-ns="build">' +
                        '<div id="' + P + 'opts-build"></div>' +
                    '</div>' +
                '</div>' +
            '</div>' +
            '<div class="hlda-br">' +
                '<div class="hlda-fs"><span class="hlda-lbl">Start date (optional)</span><input type="date" id="' + P + 'build-sd"></div>' +
                '<div class="hlda-fs"><span class="hlda-lbl">End date (optional)</span><input type="date" id="' + P + 'build-ed"></div>' +
            '</div>' +
            '<div class="hlda-cb"><input type="checkbox" id="' + P + 'build-refetch"><label for="' + P + 'build-refetch">Refetch TradFi data from scratch (stock-perps)</label></div>' +
            '<div class="hlda-help">Ignores existing TradFi 1m data and re-fetches from 2016-12-12. Use after symbol mapping corrections. Applies only to XYZ-* coins.</div>' +
            '<div class="hlda-ar"><button class="hlda-btn" id="' + P + 'build-btn" data-action="submit-build">Build best 1m</button><div class="hlda-lo" id="' + P + 'build-lo"><div class="hlda-spin"></div><span>Queuing…</span></div></div>' +
            '<div class="hlda-msg" id="' + P + 'build-msg"></div>' +
            renderJobMonitorHTML('build');
        renderCoinOpts('build', buildCoins, buildSelected);
        updateTrigger('build', buildCoins, buildSelected);
    }

    // ══════════════════════════════════════════════════════════════
    //  COIN SELECTOR
    // ══════════════════════════════════════════════════════════════
    function getCS(ns) { return ns==='dl' ? {coins:dlCoins,sel:dlSelected} : {coins:buildCoins,sel:buildSelected}; }

    function renderCoinOpts(ns, coins, sel, filter) {
        var el = $('opts-'+ns);
        if (!el) return;
        var s = (filter||'').toLowerCase();
        var list = s ? coins.filter(function(c){return c.toLowerCase().indexOf(s)>=0;}) : coins;
        el.innerHTML = list.map(function(c) {
            return '<div class="hlda-co" data-action="cs-toggle" data-ns="' + ns + '" data-coin="' + escAttr(c) + '">' +
                '<input type="checkbox" ' + (sel.has(c)?'checked':'') + ' tabindex="-1"><span>' + escHtml(c) + '</span></div>';
        }).join('');
    }

    function updateTrigger(ns, coins, sel) {
        var tr = $('ct-'+ns);
        if (!tr) return;
        if (sel.size === 0) {
            tr.innerHTML = '<span class="ph">' + (ns==='build'?'All ('+coins.length+' coins)':'Select coins…') + '</span>';
            return;
        }
        var MAX=8, sorted=[].concat(Array.from(sel)).sort();
        var h='';
        sorted.slice(0,MAX).forEach(function(c) {
            h+='<span class="hlda-chip">' + escHtml(c) + '<span class="rm" data-action="cs-remove" data-ns="' + ns + '" data-coin="' + escAttr(c) + '">×</span></span>';
        });
        if (sorted.length>MAX) h+='<span class="hlda-csm">+' + (sorted.length-MAX) + ' more (' + sorted.length + ' total)</span>';
        tr.innerHTML = h;
    }

    function toggleDD(ns) {
        var dd = $('dd-'+ns);
        if (!dd) return;
        var open = dd.classList.toggle('open');
        if (open) {
            var s=$('search-'+ns); if(s){s.value='';s.focus();}
            var r=getCS(ns); renderCoinOpts(ns,r.coins,r.sel);
        }
    }
    function csToggle(ns,coin) { var r=getCS(ns); r.sel.has(coin)?r.sel.delete(coin):r.sel.add(coin); var f=$('search-'+ns); renderCoinOpts(ns,r.coins,r.sel,f?f.value:''); updateTrigger(ns,r.coins,r.sel); }
    function csAll(ns) { var r=getCS(ns); r.coins.forEach(function(c){r.sel.add(c);}); var f=$('search-'+ns); renderCoinOpts(ns,r.coins,r.sel,f?f.value:''); updateTrigger(ns,r.coins,r.sel); }
    function csClear(ns) { var r=getCS(ns); r.sel.clear(); var f=$('search-'+ns); renderCoinOpts(ns,r.coins,r.sel,f?f.value:''); updateTrigger(ns,r.coins,r.sel); }
    function csFilter(ns) { var f=$('search-'+ns); var r=getCS(ns); renderCoinOpts(ns,r.coins,r.sel,f?f.value:''); }
    function csRemove(ns,coin) { var r=getCS(ns); r.sel.delete(coin); updateTrigger(ns,r.coins,r.sel); csFilter(ns); }

    // ══════════════════════════════════════════════════════════════
    //  SUBMIT
    // ══════════════════════════════════════════════════════════════
    async function submitDL() {
        var btn = $('dl-btn'), lo = $('dl-lo');
        var sd=inputToDay($('dl-sd')?$('dl-sd').value:''), ed=inputToDay($('dl-ed')?$('dl-ed').value:'');
        if (!sd||!ed) { showMsg('dl','error','Start and end dates are required.'); return; }
        if (ed<sd) { showMsg('dl','error','End date must be after start date.'); return; }
        btn.disabled=true; lo.classList.add('active'); hideMsg('dl');
        try {
            var coins = dlSelected.size===0||dlSelected.size===dlCoins.length ? ['All'] : Array.from(dlSelected);
            var resp = await fetch(API_BASE + '/heatmap/queue-l2book-download-bulk?token=' + TOKEN, {
                method:'POST', headers:{'Content-Type':'application/json'},
                body: JSON.stringify({ coins: coins, start_day:sd, end_day:ed,
                    only_missing_1m_src_hours: $('dl-only')?$('dl-only').checked:true }),
            });
            var data = await resp.json();
            if (data.error) { showMsg('dl','error',data.error); }
            else {
                var txt='Queued job <strong>' + data.job_id + '</strong> — ' + data.coins_count + ' coins, ' + fmtDay(data.start_day) + ' → ' + fmtDay(data.end_day);
                if (data.missing_coins && data.missing_coins.length) txt+='<br><small style="color:var(--hlda-warn);">Skipped (not in archive): ' + data.missing_coins.join(', ') + '</small>';
                showMsg('dl','success',txt);
            }
        } catch(e) { showMsg('dl','error','Request failed: ' + e.message); }
        finally { btn.disabled=false; lo.classList.remove('active'); }
    }

    async function submitBuild() {
        var btn=$('build-btn'), lo=$('build-lo');
        btn.disabled=true; lo.classList.add('active'); hideMsg('build');
        try {
            var coins = buildSelected.size===0||buildSelected.size===buildCoins.length ? ['All'] : Array.from(buildSelected);
            var sd=inputToDay($('build-sd')?$('build-sd').value:''), ed=inputToDay($('build-ed')?$('build-ed').value:'');
            if (sd&&ed&&ed<sd) { showMsg('build','error','Start date must be on or before End date.'); return; }
            var resp = await fetch(API_BASE + '/heatmap/queue-build-ohlcv?token=' + TOKEN, {
                method:'POST', headers:{'Content-Type':'application/json'},
                body: JSON.stringify({ coins: coins, start_day:sd, end_day:ed,
                    refetch: $('build-refetch')?$('build-refetch').checked:false }),
            });
            var data = await resp.json();
            if (data.error) { showMsg('build','error',data.error); }
            else {
                var txt='Queued job <strong>' + data.job_id + '</strong> — ' + data.coins_count + ' coins';
                if (data.start_day) txt+=', ' + fmtDay(data.start_day) + ' → ' + fmtDay(data.end_day);
                else txt+=', end: ' + fmtDay(data.end_day);
                if (data.refetch) txt+=' (refetch)';
                showMsg('build','success',txt);
            }
        } catch(e) { showMsg('build','error','Request failed: ' + e.message); }
        finally { btn.disabled=false; lo.classList.remove('active'); }
    }

    function showMsg(ns,type,html) { var m=$(ns+'-msg'); if(!m)return; m.className='hlda-msg '+type; m.innerHTML=html; m.style.display='block'; }
    function hideMsg(ns) { var m=$(ns+'-msg'); if(m){m.className='hlda-msg';m.style.display='none';} }

    // ══════════════════════════════════════════════════════════════
    //  JOB MONITOR
    // ══════════════════════════════════════════════════════════════
    var JOB_TYPES = { dl: 'hl_aws_l2book_auto', build: 'hl_best_1m' };

    function renderJobMonitorHTML(ns) {
        return '<div class="hlda-jm">' +
            '<div class="hlda-jm-title">Job Monitor <span class="hlda-jm-badge disconnected" id="' + P + 'jm-badge-' + ns + '">Connecting</span></div>' +
            '<div class="hlda-tabs">' +
                '<button class="hlda-tab active" id="' + P + 'jm-tab-' + ns + '-running" data-action="switch-tab" data-ns="' + ns + '" data-tab="running">Active</button>' +
                '<button class="hlda-tab" id="' + P + 'jm-tab-' + ns + '-done" data-action="switch-tab" data-ns="' + ns + '" data-tab="done">Done</button>' +
                '<button class="hlda-tab" id="' + P + 'jm-tab-' + ns + '-failed" data-action="switch-tab" data-ns="' + ns + '" data-tab="failed">Failed</button>' +
            '</div>' +
            '<div class="hlda-tp active" id="' + P + 'jm-panel-' + ns + '-running"><div class="hlda-empty">Connecting…</div></div>' +
            '<div class="hlda-tp" id="' + P + 'jm-panel-' + ns + '-done"></div>' +
            '<div class="hlda-tp" id="' + P + 'jm-panel-' + ns + '-failed"></div>' +
        '</div>';
    }

    function switchTab(ns, tab) {
        ['running','done','failed'].forEach(function(t) {
            var btn = $('jm-tab-' + ns + '-' + t);
            var panel = $('jm-panel-' + ns + '-' + t);
            if (btn) { if(t===tab) btn.classList.add('active'); else btn.classList.remove('active'); }
            if (panel) { if(t===tab) panel.classList.add('active'); else panel.classList.remove('active'); }
        });
        currentTab[ns] = tab;
        if (tab !== 'running') loadHistoryTab(ns, tab);
    }

    function connectWS() {
        var proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
        var wsUrl = proto + '//' + API_HOST + '/ws/jobs?token=' + TOKEN;
        try {
            ws = new WebSocket(wsUrl);
            ws.onopen = function() {
                wsRetryCount = 0;
                ['dl','build'].forEach(function(ns) {
                    var badge = $('jm-badge-' + ns);
                    if (badge) { badge.textContent='Connected'; badge.className='hlda-jm-badge connected'; }
                });
            };
            ws.onmessage = function(evt) {
                try {
                    var msg = JSON.parse(evt.data);
                    if (msg.type === 'jobs') {
                        var allJobs = msg.data || [];
                        ['dl','build'].forEach(function(ns) {
                            var jt = JOB_TYPES[ns];
                            var filtered = allJobs.filter(function(j) {
                                return (j.type||'').toLowerCase() === jt &&
                                       (j.status==='pending' || j.status==='running');
                            });
                            filtered.sort(function(a,b) {
                                if (a.status==='running'&&b.status!=='running') return -1;
                                if (b.status==='running'&&a.status!=='running') return 1;
                                return (b.updated_ts||0)-(a.updated_ts||0);
                            });
                            activeJobs[ns] = filtered;
                            if (currentTab[ns] === 'running') renderActivePanel(ns);
                        });
                    }
                } catch(ex) {}
            };
            ws.onclose = function() {
                ['dl','build'].forEach(function(ns) {
                    var badge = $('jm-badge-' + ns);
                    if (badge) { badge.textContent='Disconnected'; badge.className='hlda-jm-badge disconnected'; }
                });
                if (wsRetryCount < 5) { wsRetryCount++; setTimeout(connectWS, 3000); }
            };
            ws.onerror = function() { ws.close(); };
        } catch(ex) {
            ['dl','build'].forEach(function(ns) {
                var badge = $('jm-badge-' + ns);
                if (badge) { badge.textContent='Error'; badge.className='hlda-jm-badge disconnected'; }
            });
        }
    }

    function renderActivePanel(ns) {
        var panel = $('jm-panel-' + ns + '-running');
        if (!panel) return;
        var jobs = activeJobs[ns];
        if (!jobs.length) { panel.innerHTML = '<div class="hlda-empty">No active jobs.</div>'; return; }
        panel.innerHTML = jobs.map(function(j) { return renderActiveJob(ns, j); }).join('');
    }

    function renderActiveJob(ns, job) {
        var pr = job.progress||{};
        var pct = calcPct(pr);
        var coin = pr.coin||'';
        var chunk = pr.chunk_start ? (pr.chunk_start + '→' + pr.chunk_end) : '';
        var isExp = expandedJobs[ns].has(job.id);
        var sc = job.status==='running' ? 'running' : 'pending';
        var dl=pr.downloaded_total||0, sk=pr.skipped_existing_total||0, fl=pr.failed_total||0;
        var step=pr.step||0, total=pr.total||0, chD=pr.chunk_done||0, chT=pr.chunk_total||0;
        var stage=pr.stage||'', mode=pr.mode||'';

        var h = '<div class="hlda-jc">' +
            '<div class="hlda-jh">' +
                '<div class="hlda-ji">' +
                    '<span class="jid">' + escHtml(job.id) + '</span>' +
                    '<span class="hlda-sbadge ' + sc + '">' + job.status + '</span>' +
                    '<span class="jtype">' + escHtml(job.type) + '</span>' +
                '</div>' +
                '<div class="hlda-ja">' +
                    '<button class="hlda-jbtn" data-action="show-log" data-job="' + escAttr(job.id) + '">Log</button>' +
                    '<button class="hlda-jbtn danger" data-action="cancel-job" data-job="' + escAttr(job.id) + '">Cancel</button>' +
                '</div>' +
            '</div>' +
            '<div class="hlda-jd">' +
                (coin ? '<span>Coin: ' + escHtml(coin) + '</span>' : '') +
                (chunk ? '<span>Chunk: ' + chunk + '</span>' : '') +
                '<span>Updated: ' + fmtTS(job.updated_ts) + '</span>' +
            '</div>' +
            '<div class="hlda-pb"><div class="hlda-pf" style="width:' + pct + '%"></div><div class="hlda-pt">' + pct + '%</div></div>';
        if (total>0) {
            h += '<div class="hlda-pd">' +
                '<span>Step: ' + step + '/' + total + '</span>' +
                (chT>0 ? '<span>Chunk: ' + chD + '/' + chT + '</span>' : '') +
                (stage ? '<span>Stage: ' + escHtml(stage) + '</span>' : '') +
                (mode ? '<span>Mode: ' + escHtml(mode) + '</span>' : '') +
            '</div>';
        }
        if (dl+sk+fl>0) {
            h += '<div class="hlda-exp">' +
                '<button class="hlda-exp-toggle" data-action="tog-exp" data-ns="' + ns + '" data-job="' + escAttr(job.id) + '">' + (isExp?'▼':'▶') + ' Details</button>' +
                '<div class="hlda-exp-body ' + (isExp?'open':'') + '" id="' + P + 'exp-' + ns + '-' + job.id + '">' +
                    '<div class="dr"><strong>Downloads:</strong> ' + dl + ' files (' + fmtBytes(pr.downloaded_bytes_total) + ')</div>' +
                    '<div class="dr"><strong>Skipped:</strong> ' + sk + ' files (' + fmtBytes(pr.skipped_existing_bytes_total) + ')</div>' +
                    '<div class="dr"><strong>Failed:</strong> ' + fl + ' files (' + fmtBytes(pr.failed_bytes_total) + ')</div>' +
                '</div>' +
            '</div>';
        }
        h += '</div>';
        return h;
    }

    async function loadHistoryTab(ns, tab) {
        var panel = $('jm-panel-' + ns + '-' + tab);
        if (!panel) return;
        panel.innerHTML = '<div class="hlda-empty">Loading…</div>';
        try {
            var resp = await fetch(API_BASE + '/jobs/?states=' + tab + '&limit=20&token=' + TOKEN);
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            var jobs = await resp.json();
            var jt = JOB_TYPES[ns];
            var filtered = jobs.filter(function(j) { return (j.type||'').toLowerCase()===jt; });
            filtered.sort(function(a,b) { return (b.updated_ts||0)-(a.updated_ts||0); });
            if (!filtered.length) { panel.innerHTML = '<div class="hlda-empty">No ' + tab + ' jobs.</div>'; return; }
            panel.innerHTML = filtered.map(function(j) { return renderHistoryJob(ns, j); }).join('');
        } catch(e) { panel.innerHTML = '<div class="hlda-msg error" style="display:block">Failed to load: ' + escHtml(e.message) + '</div>'; }
    }

    function renderHistoryJob(ns, job) {
        var payload=job.payload||{}, pr=job.progress||{}, lr=pr.last_result||{};
        var coins=Array.isArray(payload.coins)?payload.coins:[];
        var coinPrev=coins.length<=8?coins.join(', '):coins.slice(0,8).join(', ')+' … (' + coins.length + ' total)';
        var sd=payload.start_day||'', ed=payload.end_day||'';
        var rangeStr=(sd||ed)?(fmtDay(sd)||'?')+' → '+(fmtDay(ed)||'?'):'';
        var dl=pr.downloaded_total||lr.downloaded||0, sk=pr.skipped_existing_total||lr.skipped_existing||0, fl=pr.failed_total||lr.failed||0;
        var hasStats=(dl+sk+fl)>0;
        var cTs=parseFloat(job.created_ts||0), uTs=parseFloat(job.updated_ts||0);
        var durStr='';
        if (cTs>0&&uTs>=cTs) { var d=Math.round(uTs-cTs); var hh=Math.floor(d/3600),mm=Math.floor((d%3600)/60),ss=d%60;
            if(hh>0) durStr=hh+'h '+String(mm).padStart(2,'0')+'m'; else if(mm>0) durStr=mm+'m '+String(ss).padStart(2,'0')+'s'; else durStr=ss+'s'; }
        var isExp=expandedJobs[ns].has(job.id);
        var isDone=job.status==='done', isFailed=job.status==='failed';

        var h = '<div class="hlda-jc">' +
            '<div class="hlda-jh">' +
                '<div class="hlda-ji">' +
                    '<span class="jid">' + escHtml(job.id) + '</span>' +
                    '<span class="jtype">' + escHtml(job.type) + '</span>' +
                    (durStr ? '<span class="jdur">' + durStr + '</span>' : '') +
                '</div>' +
                '<div class="hlda-ja">' +
                    '<button class="hlda-jbtn" data-action="show-log" data-job="' + escAttr(job.id) + '">Log</button>' +
                    (isFailed ? '<button class="hlda-jbtn" data-action="retry-job" data-job="' + escAttr(job.id) + '">Retry</button>' : '') +
                    (isDone ? '<button class="hlda-jbtn" data-action="requeue-job" data-job="' + escAttr(job.id) + '">Requeue</button>' : '') +
                    '<button class="hlda-jbtn danger" data-action="delete-job" data-job="' + escAttr(job.id) + '">Delete</button>' +
                '</div>' +
            '</div>' +
            '<div class="hlda-jd">' +
                '<span>' + fmtTS(job.updated_ts) + '</span>' +
                (rangeStr ? '<span>Range: ' + rangeStr + '</span>' : '') +
            '</div>';
        if (job.error) h += '<div class="hlda-jerr">' + escHtml(job.error) + '</div>';
        if (hasStats||coinPrev) {
            h += '<div class="hlda-exp">' +
                '<button class="hlda-exp-toggle" data-action="tog-exp" data-ns="' + ns + '" data-job="' + escAttr(job.id) + '">' + (isExp?'▼':'▶') + ' Details</button>' +
                '<div class="hlda-exp-body ' + (isExp?'open':'') + '" id="' + P + 'exp-' + ns + '-' + job.id + '">' +
                    (coinPrev ? '<div class="dr"><strong>Coins:</strong> ' + escHtml(coinPrev) + '</div>' : '') +
                    (payload.only_missing_1m_src_hours!==undefined ? '<div class="dr"><strong>Only missing 1m_src hours:</strong> ' + (payload.only_missing_1m_src_hours?'Yes':'No') + '</div>' : '') +
                    (hasStats ? '<div class="dr"><strong>Downloaded:</strong> ' + dl + ' files (' + fmtBytes(pr.downloaded_bytes_total||lr.downloaded_bytes||0) + ')</div>' +
                        '<div class="dr"><strong>Skipped:</strong> ' + sk + ' files (' + fmtBytes(pr.skipped_existing_bytes_total||lr.skipped_existing_bytes||0) + ')</div>' +
                        '<div class="dr"><strong>Failed:</strong> ' + fl + ' files (' + fmtBytes(pr.failed_bytes_total||lr.failed_bytes||0) + ')</div>' : '') +
                '</div>' +
            '</div>';
        }
        h += '</div>';
        return h;
    }

    // ── Job actions ──
    async function showLog(jobId) {
        var modal=$('modal'), title=$('modal-title'), body=$('modal-body');
        title.textContent='Log: '+jobId; body.textContent='Loading…';
        modal.classList.add('active');
        try {
            var r=await fetch(API_BASE + '/jobs/' + jobId + '/log?token=' + TOKEN);
            if (!r.ok) throw new Error('HTTP '+r.status);
            var d=await r.json();
            body.textContent = (d.lines||[]).join('\n') || 'No log entries.';
        } catch(e) { body.textContent='Failed to load log: '+e.message; }
    }
    function closeModal() { $('modal').classList.remove('active'); }

    async function cancelJob(id) {
        try { await fetch(API_BASE + '/jobs/' + id + '/cancel?token=' + TOKEN, {method:'POST'}); } catch(e) {}
    }
    async function deleteJob(id) {
        try { await fetch(API_BASE + '/jobs/' + id + '?token=' + TOKEN, {method:'DELETE'}); } catch(e) {}
        ['dl','build'].forEach(function(ns) { if(currentTab[ns]!=='running') loadHistoryTab(ns, currentTab[ns]); });
    }
    async function retryJob(id) {
        try { await fetch(API_BASE + '/jobs/' + id + '/retry?token=' + TOKEN, {method:'POST'}); } catch(e) {}
        ['dl','build'].forEach(function(ns) { if(currentTab[ns]==='failed') loadHistoryTab(ns,'failed'); });
    }
    async function requeueJob(id) {
        try { await fetch(API_BASE + '/jobs/' + id + '/requeue?token=' + TOKEN, {method:'POST'}); } catch(e) {}
        ['dl','build'].forEach(function(ns) { if(currentTab[ns]==='done') loadHistoryTab(ns,'done'); });
    }

    function togExp(ns, jobId, btn) {
        var el = $('exp-' + ns + '-' + jobId);
        if (!el) return;
        if (el.classList.contains('open')) {
            el.classList.remove('open'); expandedJobs[ns].delete(jobId);
            if(btn) btn.textContent='▶ Details';
        } else {
            el.classList.add('open'); expandedJobs[ns].add(jobId);
            if(btn) btn.textContent='▼ Details';
        }
    }

    // ── Helpers ──
    function calcPct(pr) {
        if (!pr.total) return 0;
        var s=pr.step||0, cD=pr.chunk_done||0, cT=pr.chunk_total||1;
        var frac=cT>0?cD/cT:0;
        return Math.min(100, Math.max(0, Math.round(((s-1+frac)/pr.total)*100)));
    }
    function fmtBytes(b) {
        if(!b) return '0 B';
        var k=1024, sz=['B','KB','MB','GB'];
        var i=Math.floor(Math.log(b)/Math.log(k));
        return (b/Math.pow(k,i)).toFixed(2)+' '+sz[i];
    }
    function fmtTS(ts) {
        if(!ts) return '';
        var d=new Date(Number(ts)*1000);
        if(isNaN(d.getTime())) return String(ts);
        function p(n){return String(n).padStart(2,'0');}
        return d.getFullYear()+'-'+p(d.getMonth()+1)+'-'+p(d.getDate())+' '+p(d.getHours())+':'+p(d.getMinutes())+':'+p(d.getSeconds());
    }
    function escHtml(t) { var d=document.createElement('div'); d.textContent=t; return d.innerHTML; }
    function escAttr(t) { return String(t).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/'/g,'&#39;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
})();
