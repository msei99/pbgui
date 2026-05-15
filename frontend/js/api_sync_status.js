function createApiSyncStatusController(config) {
  const state = {
    hostMap: {},
    syncState: {},
    syncInfo: {},
    eventSource: null,
    streamActive: false,
  };

  function hostLabel(hostname) {
    const info = state.hostMap[hostname] || {};
    return info.pbname ? info.pbname + ' (' + hostname + ')' : hostname;
  }

  function getManagedHosts(data) {
    return Array.isArray(data.summary_hostnames)
      ? data.summary_hostnames
      : (Array.isArray(data.connected_hosts) ? data.connected_hosts : []);
  }

  function getHostInfo(data, hostname) {
    if (data.summary_hosts && data.summary_hosts[hostname]) return data.summary_hosts[hostname];
    if (data.hosts && data.hosts[hostname]) return data.hosts[hostname];
    return {};
  }

  function isMissingRemoteApi(info) {
    return !!(info && info.connected === true && info.md5_detail && info.md5_detail.pb7 == null);
  }

  function md5Tooltip(md5Detail) {
    if (!md5Detail || Object.keys(md5Detail).length === 0) return '';
    const parts = [];
    if ('pb7' in md5Detail) {
      parts.push('api-keys.json: ' + (md5Detail.pb7 === true ? '\u2705' : md5Detail.pb7 === false ? '\u274C mismatch' : '\u2753 not installed'));
    }
    return parts.join(' | ');
  }

  function describeReason(hostname) {
    const info = state.syncInfo[hostname] || {};
    const parts = [hostLabel(hostname)];
    if (info.connected && info.md5_detail && info.md5_detail.pb7 == null) {
      parts.push('api-keys.json not installed');
    }
    if (info.md5_detail && 'pb7' in info.md5_detail) {
      if (info.md5_detail.pb7 === false) parts.push('api-keys.json mismatch');
    }
    if (info.remote_serial == null) {
      parts.push('remote serial unavailable');
    } else if (info.local_serial != null && info.remote_serial !== info.local_serial) {
      parts.push('serial ' + info.remote_serial + ' != local ' + info.local_serial);
    }
    return parts.join(' - ');
  }

  function getHosts() {
    return Object.keys(state.syncState);
  }

  function getSummary() {
    const hosts = getHosts();
    const outOfSync = hosts.filter(function (hostname) {
      const info = state.syncInfo[hostname] || {};
      return state.syncState[hostname] === false || isMissingRemoteApi(info);
    });
    const unknown = hosts.filter(function (hostname) {
      const info = state.syncInfo[hostname] || {};
      if (outOfSync.indexOf(hostname) >= 0) return false;
      return state.syncState[hostname] !== true && state.syncState[hostname] !== false
        && info.disconnected !== true;
    });
    const disconnected = hosts.filter(function (hostname) {
      const info = state.syncInfo[hostname] || {};
      return info.disconnected === true;
    });
    return {
      total: hosts.length,
      hosts: hosts,
      out_of_sync: outOfSync,
      unknown: unknown,
      disconnected: disconnected,
      all_unknown: hosts.length > 0 && unknown.length === hosts.length && outOfSync.length === 0,
      all_in_sync: hosts.length > 0 && outOfSync.length === 0 && unknown.length === 0 && disconnected.length === 0,
      reasons: outOfSync.map(describeReason),
    };
  }

  function getButtonModel() {
    const summary = getSummary();
    const extraNotes = [];
    if (summary.disconnected.length > 0) extraNotes.push(summary.disconnected.length + ' disconnected');
    if (summary.unknown.length > 0) extraNotes.push(summary.unknown.length + ' unknown');
    if (summary.total === 0) {
      return {
        className: 'sb-btn',
        text: 'API Sync',
        title: '',
        disabled: false,
        statusClass: 'ok',
      };
    }
    if (summary.out_of_sync.length > 0) {
      const inSyncCount = Math.max(0, summary.total - summary.out_of_sync.length);
      return {
        className: 'sb-btn danger',
        text: 'API ' + inSyncCount + '/' + summary.total + ' in sync',
        title: summary.reasons.concat(extraNotes).join('\n'),
        disabled: false,
        statusClass: 'danger',
      };
    }
    if (summary.all_unknown) {
      return {
        className: 'sb-btn',
        text: 'API Sync',
        title: (summary.disconnected.length > 0 ? summary.disconnected.length + ' disconnected' : 'API sync status not available yet.'),
        disabled: false,
        statusClass: 'ok',
      };
    }
    if (summary.disconnected.length > 0 || summary.unknown.length > 0) {
      return {
        className: 'sb-btn',
        text: 'API Sync',
        title: extraNotes.join('\n') || 'API sync status incomplete.',
        disabled: false,
        statusClass: 'ok',
      };
    }
    return {
      className: 'sb-btn',
      text: 'API all in sync',
      title: summary.total + ' host(s) in sync.',
      disabled: false,
      statusClass: 'ok',
    };
  }

  function updateButton() {
    const button = typeof config.getButton === 'function' ? config.getButton() : null;
    if (!button) return;
    const model = getButtonModel();
    button.className = model.className;
    button.textContent = model.text;
    button.title = model.title;
    if (!button.dataset.syncBusy) {
      button.disabled = !!model.disabled;
    }
    if (typeof config.onStateChange === 'function') {
      config.onStateChange(getSummary(), model, state);
    }
  }

  function updateSerialCell(hostname, remoteSerial, localSerial, inSync, md5Detail) {
    const cell = typeof config.getSerialCell === 'function' ? config.getSerialCell(hostname) : null;
    const existingInfo = state.syncInfo[hostname] || {};
    state.syncInfo[hostname] = {
      remote_serial: remoteSerial != null ? remoteSerial : null,
      local_serial: localSerial != null ? localSerial : null,
      in_sync: inSync != null ? inSync : (remoteSerial != null && localSerial != null ? remoteSerial === localSerial : null),
      md5_detail: md5Detail || existingInfo.md5_detail || {},
      connected: existingInfo.connected === true,
      disconnected: existingInfo.disconnected === true || !!((state.hostMap[hostname] || {}).disconnected),
    };
    if (remoteSerial == null) {
      state.syncState[hostname] = isMissingRemoteApi(state.syncInfo[hostname]) ? false : null;
      if (cell) {
        cell.textContent = '\u2014';
        cell.style.cssText = 'color:#64748b; font-size:var(--fs-sm); text-align:right;';
        cell.title = 'Remote serial unavailable';
      }
      updateButton();
      return;
    }
    const synced = isMissingRemoteApi(state.syncInfo[hostname]) ? false : (inSync != null ? inSync : (remoteSerial === localSerial));
    state.syncState[hostname] = synced;
    if (cell) {
      if (synced) {
        cell.textContent = remoteSerial;
        cell.style.cssText = 'color:#86efac; font-size:var(--fs-sm); text-align:right;';
        cell.title = 'In sync' + (md5Detail ? ' - ' + md5Tooltip(md5Detail) : '');
      } else {
        cell.innerHTML = '<span style="background:#991b1b; color:#fecaca; padding:1px 6px; border-radius:3px; font-weight:700; font-size:var(--fs-sm);">' + remoteSerial + ' \u2716</span>';
        cell.style.cssText = 'text-align:right;';
        cell.title = 'Out of sync' + (md5Detail ? ' - ' + md5Tooltip(md5Detail) : '');
      }
    }
    updateButton();
  }

  function updateLastPushCell(hostname, lastPush) {
    if (typeof config.updateLastPushCell === 'function') {
      config.updateLastPushCell(hostname, lastPush);
    }
  }

  function applyStatus(data) {
    const hosts = getManagedHosts(data);
    const nextHostSet = {};
    hosts.forEach(function (hostname) {
      nextHostSet[hostname] = true;
      const info = getHostInfo(data, hostname);
      state.hostMap[hostname] = Object.assign({}, state.hostMap[hostname] || {}, info);
      state.syncInfo[hostname] = {
        remote_serial: info.remote_serial != null ? info.remote_serial : null,
        local_serial: data.local_serial != null ? data.local_serial : null,
        in_sync: info.in_sync != null ? info.in_sync : null,
        md5_detail: info.md5_detail || {},
        connected: info.connected === true,
        disconnected: info.disconnected === true,
      };
      state.syncState[hostname] = isMissingRemoteApi(state.syncInfo[hostname]) ? false : (info.in_sync != null ? info.in_sync : null);
      if (data.last_push && data.last_push[hostname]) {
        updateLastPushCell(hostname, data.last_push[hostname]);
      }
    });
    Object.keys(state.syncState).forEach(function (hostname) {
      if (!nextHostSet[hostname]) {
        delete state.syncState[hostname];
        delete state.syncInfo[hostname];
        delete state.hostMap[hostname];
      }
    });
    updateButton();
    return getSummary();
  }

  async function refresh() {
    const data = await config.apiFetch('/sync/ssh-status');
    return applyStatus(data || {});
  }

  async function quickSync(options) {
    const button = typeof config.getButton === 'function' ? config.getButton() : null;
    if (button) {
      button.dataset.syncBusy = '1';
      button.disabled = true;
      button.textContent = '\u2026';
    }
    try {
      const body = Object.assign({ dry_run: false, no_propagate: false }, options || {});
      const data = await config.apiFetch('/sync/push-ssh', {
        method: 'POST',
        body: JSON.stringify(body),
      });
      const results = data.results || {};
      const total = Object.keys(results).length;
      const ok = Object.values(results).filter(function (result) { return result && result.success; }).length;
      const okHosts = Object.keys(results).filter(function (hostname) {
        const result = results[hostname];
        return result && result.success;
      });
      const failedHosts = Object.keys(results).filter(function (hostname) {
        const result = results[hostname];
        return !result || !result.success;
      });
      if (typeof config.showToast === 'function') {
        if (total === 0) config.showToast('API Sync: no VPS connected', 'warning');
        else if (ok === total) config.showToast('API Sync: ' + ok + '/' + total + ' OK - ' + okHosts.join(', '), 'success');
        else {
          const details = [];
          if (okHosts.length) details.push('OK: ' + okHosts.join(', '));
          if (failedHosts.length) details.push('Failed: ' + failedHosts.join(', '));
          config.showToast('API Sync: ' + ok + '/' + total + ' succeeded' + (details.length ? ' - ' + details.join(' | ') : ''), ok > 0 ? 'warning' : 'error');
        }
      }
      return data;
    } catch (error) {
      if (typeof config.showToast === 'function') {
        config.showToast('API Sync failed: ' + error.message, 'error');
      }
      throw error;
    } finally {
      if (button) {
        delete button.dataset.syncBusy;
      }
      await refresh().catch(function () {});
    }
  }

  function closeStream() {
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }
    state.streamActive = false;
  }

  function openStream() {
    closeStream();
    const url = config.buildStreamUrl();
    state.eventSource = new EventSource(url);
    state.streamActive = true;
    state.eventSource.onmessage = function (event) {
      try {
        const data = JSON.parse(event.data);
        if (data.type === 'init') {
          const localSerial = data.local_serial || 0;
          getManagedHosts(data).forEach(function (hostname) {
            const info = getHostInfo(data, hostname);
            state.hostMap[hostname] = Object.assign({}, state.hostMap[hostname] || {}, info);
            updateSerialCell(hostname, info.remote_serial, localSerial, info.in_sync, info.md5_detail);
            if (data.last_push && data.last_push[hostname]) {
              updateLastPushCell(hostname, data.last_push[hostname]);
            }
          });
          updateButton();
          return;
        }
        if (data.type === 'serial_update') {
          updateSerialCell(data.hostname, data.remote_serial, data.local_serial, data.in_sync, data.md5_detail);
          if (data.last_push) updateLastPushCell(data.hostname, data.last_push);
        }
      } catch (_) {}
    };
    state.eventSource.onerror = function () {};
  }

  return {
    state: state,
    applyStatus: applyStatus,
    refresh: refresh,
    quickSync: quickSync,
    updateButton: updateButton,
    updateSerialCell: updateSerialCell,
    openStream: openStream,
    closeStream: closeStream,
    getSummary: getSummary,
    getButtonModel: getButtonModel,
  };
}
