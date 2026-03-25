/* ══════════════════════════════════════════════════════════════════════
   Bitool Operations Console — Live API Integration Layer
   ══════════════════════════════════════════════════════════════════════ */

(function () {
  'use strict';

  // ─── Core API Wrapper ─────────────────────────────────────────────

  async function opsApi(path, opts) {
    opts = opts || {};
    var method = opts.method || 'GET';
    var fetchOpts = { method: method, headers: { 'Content-Type': 'application/json' } };
    if (opts.body) fetchOpts.body = JSON.stringify(opts.body);

    var url = '/ops' + path;
    if (opts.params) {
      var qs = new URLSearchParams();
      var keys = Object.keys(opts.params);
      for (var i = 0; i < keys.length; i++) {
        var k = keys[i];
        var v = opts.params[k];
        if (v != null && v !== '') qs.set(k, v);
      }
      var s = qs.toString();
      if (s) url += '?' + s;
    }

    var resp = await fetch(url, fetchOpts);
    var text = await resp.text();
    var json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch (_) {
      if (!resp.ok) throw new Error(text || ('HTTP ' + resp.status));
      return text;
    }
    if (!resp.ok) throw new Error((json && (json.message || json.error)) || ('HTTP ' + resp.status));
    if (!json.ok) throw new Error(json.message || json.error || 'Unknown error');
    return json.data;
  }

  // ─── Global State ─────────────────────────────────────────────────

  var activeScreen = 'pipeline-overview';
  var pollTimer = null;
  var pollInFlight = false;
  var consecutiveFailures = 0;
  var MAX_BACKOFF = 120000;
  var currentDriftEvent = null;

  var POLL_INTERVALS = {
    'pipeline-overview':  10000,
    'queue-workers':      5000,
    'source-health':      15000,
    'batches-manifests':  15000,
    'checkpoints-replay': 20000,
    'bad-records':        20000,
    'schema-drift':       15000,
    'schema-medallion':   30000,
    'admin-policies':     60000
  };

  var screenMeta = {
    'pipeline-overview':   { title: 'Pipeline Overview',       icon: '\u25EF' },
    'queue-workers':       { title: 'Queue & Workers',         icon: '\u2699' },
    'source-health':       { title: 'Source Health',            icon: '\u26A1' },
    'batches-manifests':   { title: 'Batches & Manifests',     icon: '\u2610' },
    'checkpoints-replay':  { title: 'Checkpoints & Replay',    icon: '\u21BA' },
    'bad-records':         { title: 'Bad Records',             icon: '\u26A0' },
    'schema-drift':        { title: 'Schema Drift',            icon: '\uD83D\uDD0D' },
    'schema-medallion':    { title: 'Schema & Medallion',      icon: '\u2605' },
    'admin-policies':      { title: 'Admin & Policies',        icon: '\u2692' }
  };

  var screenLoaders = {
    'pipeline-overview':  loadPipelineOverview,
    'queue-workers':      loadQueueWorkers,
    'source-health':      loadSourceHealth,
    'batches-manifests':  loadBatches,
    'checkpoints-replay': loadCheckpoints,
    'bad-records':        loadBadRecords,
    'schema-drift':       loadSchemaDrift,
    'schema-medallion':   loadSchemaMedallion,
    'admin-policies':     loadAdmin
  };

  // ─── Utility Helpers ──────────────────────────────────────────────

  function getWorkspaceKey() {
    var el = document.getElementById('pipelineSelect');
    if (!el) return '';
    var val = el.value;
    if (val === 'All Pipelines') return '';
    return val;
  }

  function getTimeRange() {
    var el = document.getElementById('timeRange');
    if (!el) return '24h';
    var val = el.value;
    if (val.indexOf('1 hour') !== -1) return '1h';
    if (val.indexOf('6 hour') !== -1) return '6h';
    if (val.indexOf('24 hour') !== -1) return '24h';
    if (val.indexOf('7 day') !== -1) return '7d';
    return '24h';
  }

  function el(id) {
    return document.getElementById(id);
  }

  function setText(id, text) {
    var node = document.getElementById(id);
    if (node) node.textContent = (text != null) ? text : '\u2014';
  }

  function setHtml(id, html) {
    var node = document.getElementById(id);
    if (node) node.innerHTML = (html != null) ? html : '';
  }

  function badge(status, text) {
    var cls = status || '';
    return '<span class="badge ' + escAttr(cls) + '">' + esc(text || status) + '</span>';
  }

  function mono(text) {
    return '<span class="mono">' + esc(text) + '</span>';
  }

  function esc(str) {
    if (str == null) return '\u2014';
    var s = String(str);
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  function escAttr(str) {
    if (str == null) return '';
    return String(str).replace(/[^a-zA-Z0-9_-]/g, '');
  }

  function timeAgo(isoStr) {
    if (!isoStr) return '\u2014';
    var diff = Date.now() - new Date(isoStr).getTime();
    if (diff < 0) return 'just now';
    if (diff < 60000) return Math.floor(diff / 1000) + 's ago';
    if (diff < 3600000) return Math.floor(diff / 60000) + 'm ago';
    if (diff < 86400000) return Math.floor(diff / 3600000) + 'h ago';
    return Math.floor(diff / 86400000) + 'd ago';
  }

  function fmtTime(isoStr) {
    if (!isoStr) return '\u2014';
    try {
      var d = new Date(isoStr);
      return d.toLocaleTimeString();
    } catch (_) {
      return String(isoStr);
    }
  }

  function fmtNumber(n) {
    if (n == null) return '\u2014';
    return Number(n).toLocaleString();
  }

  function fmtDuration(seconds) {
    if (seconds == null) return '\u2014';
    var s = Number(seconds);
    if (s < 60) return s + 's';
    if (s < 3600) return Math.floor(s / 60) + 'm ' + (s % 60) + 's';
    return Math.floor(s / 3600) + 'h ' + Math.floor((s % 3600) / 60) + 'm';
  }

  function sparkline(values, color) {
    if (!values || !values.length) return '';
    var max = Math.max.apply(null, values.concat([1]));
    var bars = '';
    for (var i = 0; i < values.length; i++) {
      var h = (values[i] / max) * 100;
      var style = 'left:' + (i * 8) + 'px;height:' + h + '%';
      if (color) style += ';background:' + color;
      bars += '<div class="sparkline-bar" style="' + style + '"></div>';
    }
    return '<div class="sparkline">' + bars + '</div>';
  }

  function jitter(ms) {
    return ms * (0.8 + Math.random() * 0.4);
  }

  function statusColor(status) {
    if (!status) return '';
    var s = String(status).toLowerCase();
    if (s === 'running' || s === 'active' || s === 'healthy' || s === 'ok' || s === 'fresh') return 'running';
    if (s === 'completed' || s === 'committed' || s === 'closed') return 'completed';
    if (s === 'failed' || s === 'down' || s === 'stale' || s === 'error' || s === 'high') return 'failed';
    if (s === 'warning' || s === 'degraded' || s === 'lag risk' || s === 'lag_risk' || s === 'medium' || s === 'half-open' || s === 'half_open') return 'warning';
    if (s === 'retrying' || s === 'throttled' || s === 'cooldown') return 'retrying';
    if (s === 'queued' || s === 'idle' || s === 'pending' || s === 'low') return 'queued';
    if (s === 'dlq') return 'dlq';
    if (s === 'preparing') return 'preparing';
    if (s === 'rolled_back' || s === 'rolled back') return 'rolled_back';
    if (s === 'pending_checkpoint' || s === 'pending ckp') return 'pending_checkpoint';
    if (s === 'archived') return 'archived';
    if (s === 'review' || s === 'in review') return 'review';
    if (s === 'approved' || s === 'published') return 'approved';
    if (s === 'draft') return 'draft';
    if (s === 'silenced') return 'queued';
    if (s === 'acknowledged') return 'warning';
    if (s === 'fired') return 'failed';
    return s;
  }

  function setLoadingState(screenId, loading) {
    var screen = document.getElementById('screen-' + screenId);
    if (!screen) return;
    var cards = screen.querySelectorAll('.ops-metric-card');
    var tables = screen.querySelectorAll('.ops-table');
    var opacity = loading ? '0.6' : '1';
    for (var i = 0; i < cards.length; i++) {
      cards[i].style.transition = 'opacity 0.2s';
      cards[i].style.opacity = opacity;
    }
    for (var j = 0; j < tables.length; j++) {
      tables[j].style.transition = 'opacity 0.2s';
      tables[j].style.opacity = opacity;
    }
  }

  // ─── Connection Status ────────────────────────────────────────────

  function setConnected() {
    var dot = el('connStatus');
    var txt = el('connStatusText');
    var overlay = el('staleOverlay');
    if (dot) dot.style.background = '#34d399';
    if (txt) txt.textContent = 'Connected';
    if (overlay) overlay.style.display = 'none';
    consecutiveFailures = 0;
    var lr = el('lastRefresh');
    if (lr) lr.textContent = new Date().toLocaleTimeString();
  }

  function setError(err) {
    consecutiveFailures++;
    var dot = el('connStatus');
    var txt = el('connStatusText');
    var overlay = el('staleOverlay');
    if (consecutiveFailures >= 2) {
      if (dot) dot.style.background = '#ef4444';
      if (txt) txt.textContent = 'Error';
      if (overlay) {
        overlay.style.display = '';
        var st = el('staleTime');
        if (st) st.textContent = new Date().toLocaleTimeString();
      }
    } else {
      if (dot) dot.style.background = '#fbbf24';
      if (txt) txt.textContent = 'Retrying...';
    }
    if (err) console.error('[OpsConsole] API error:', err);
  }

  function getBackoffMs() {
    if (consecutiveFailures <= 0) return 0;
    var base = POLL_INTERVALS[activeScreen] || 10000;
    var backoff = Math.min(base * Math.pow(2, consecutiveFailures - 1), MAX_BACKOFF);
    return jitter(backoff);
  }

  // ─── Screen Navigation ────────────────────────────────────────────

  function switchScreen(screenId) {
    if (!screenMeta[screenId]) return;
    activeScreen = screenId;

    var navItems = document.querySelectorAll('.ops-nav-item[data-screen]');
    var screens = document.querySelectorAll('.ops-screen');
    for (var i = 0; i < navItems.length; i++) {
      navItems[i].classList.toggle('active', navItems[i].dataset.screen === screenId);
    }
    for (var j = 0; j < screens.length; j++) {
      screens[j].classList.toggle('active', screens[j].id === 'screen-' + screenId);
    }

    var meta = screenMeta[screenId];
    setText('screenTitle', meta.title);
    setText('screenIcon', meta.icon);

    stopPoll();
    loadCurrentScreen().catch(function () { return null; });
    startPoll();
  }

  async function loadCurrentScreen() {
    var loader = screenLoaders[activeScreen];
    if (!loader) return;
    setLoadingState(activeScreen, true);
    try {
      await loader();
      setConnected();
    } catch (err) {
      setError(err);
      throw err;
    } finally {
      setLoadingState(activeScreen, false);
    }
  }

  function refreshAll() {
    return loadCurrentScreen();
  }

  // ─── Polling Engine ───────────────────────────────────────────────

  function startPoll() {
    stopPoll();
    var interval = consecutiveFailures > 0 ? getBackoffMs() : jitter(POLL_INTERVALS[activeScreen] || 10000);
    pollTimer = setTimeout(async function pollTick() {
      if (!pollInFlight) {
        pollInFlight = true;
        try {
          await loadCurrentScreen();
        } catch (_) {
          // Error state is handled by loadCurrentScreen.
        } finally {
          pollInFlight = false;
        }
      }
      var nextInterval = consecutiveFailures > 0 ? getBackoffMs() : jitter(POLL_INTERVALS[activeScreen] || 10000);
      pollTimer = setTimeout(pollTick, nextInterval);
    }, interval);
  }

  function stopPoll() {
    if (pollTimer) {
      clearTimeout(pollTimer);
      pollTimer = null;
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // SCREEN LOADERS
  // ═══════════════════════════════════════════════════════════════════

  // ─── Screen 1: Pipeline Overview ──────────────────────────────────

  async function loadPipelineOverview() {
    var ws = getWorkspaceKey();
    var tr = getTimeRange();

    var results = await Promise.allSettled([
      opsApi('/pipeline/kpis', { params: { workspace_key: ws, time_range: tr } }),
      opsApi('/pipeline/sourceStatus', { params: { workspace_key: ws } }),
      opsApi('/pipeline/recentActivity', { params: { workspace_key: ws, limit: 20 } }),
      opsApi('/alerts', { params: { workspace_key: ws, state: 'fired,acknowledged,silenced', limit: 10 } })
    ]);

    var anyFailed = false;

    // KPIs
    if (results[0].status === 'fulfilled') {
      var kpis = results[0].value;
      setText('kpi-active-sources', kpis.active_sources != null ? fmtNumber(kpis.active_sources) : '\u2014');
      setText('kpi-running-jobs', kpis.running_jobs != null ? fmtNumber(kpis.running_jobs) : '\u2014');
      setText('kpi-bad-records', kpis.bad_records != null ? fmtNumber(kpis.bad_records) : '\u2014');
      setText('kpi-failed-jobs', kpis.failed_jobs != null ? fmtNumber(kpis.failed_jobs) : '\u2014');
      setText('kpi-batches-committed', kpis.batches_committed != null ? fmtNumber(kpis.batches_committed) : '\u2014');
      setText('kpi-avg-freshness', kpis.avg_freshness || '\u2014');
      setHtml('kpi-active-sources-sub', kpis.active_sources_sub || '');
      setHtml('kpi-running-jobs-sub', kpis.running_jobs_sub || '');
      setHtml('kpi-bad-records-sub', kpis.bad_records_sub || '');
      setHtml('kpi-failed-jobs-sub', kpis.failed_jobs_sub || '');
      setHtml('kpi-batches-committed-sub', kpis.batches_committed_sub || '');
      setHtml('kpi-avg-freshness-sub', kpis.avg_freshness_sub || '');
    } else { anyFailed = true; }

    // Source Status
    if (results[1].status === 'fulfilled') {
      renderSourceStatusTable(results[1].value);
    } else { anyFailed = true; }

    // Recent Activity
    if (results[2].status === 'fulfilled') {
      renderRecentActivityTable(results[2].value);
    } else { anyFailed = true; }

    // Alerts
    if (results[3].status === 'fulfilled') {
      renderPipelineAlerts(results[3].value);
    } else { anyFailed = true; }

    if (anyFailed) throw new Error('One or more pipeline overview requests failed');
  }

  function renderSourceStatusTable(sources) {
    var tbody = el('sourceStatusBody');
    if (!tbody || !sources) return;
    if (!Array.isArray(sources)) { tbody.innerHTML = '<tr><td colspan="8">No data</td></tr>'; return; }
    if (sources.length === 0) { tbody.innerHTML = '<tr><td colspan="8">No sources found</td></tr>'; return; }

    var html = '';
    for (var i = 0; i < sources.length; i++) {
      var s = sources[i];
      html += '<tr>'
        + '<td class="primary">' + esc(s.name || s.source_key) + '</td>'
        + '<td>' + esc(s.type || s.source_type) + '</td>'
        + '<td>' + badge(statusColor(s.status), s.status) + '</td>'
        + '<td class="mono">' + timeAgo(s.last_run) + '</td>'
        + '<td>' + esc(s.throughput || '\u2014') + '</td>'
        + '<td>' + esc(s.freshness || '\u2014') + '</td>'
        + '<td>' + fmtNumber(s.bad_records) + '</td>'
        + '<td>' + sparkline(s.trend_values, s.trend_color || null) + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
  }

  function renderRecentActivityTable(activities) {
    var tbody = el('recentActivityBody');
    if (!tbody || !activities) return;
    if (!Array.isArray(activities)) { tbody.innerHTML = '<tr><td colspan="4">No data</td></tr>'; return; }
    if (activities.length === 0) { tbody.innerHTML = '<tr><td colspan="4">No recent activity</td></tr>'; return; }

    var html = '';
    for (var i = 0; i < activities.length; i++) {
      var a = activities[i];
      html += '<tr>'
        + '<td class="mono">' + fmtTime(a.timestamp || a.time) + '</td>'
        + '<td>' + badge(statusColor(a.event_type || a.type), a.event_type || a.type) + '</td>'
        + '<td>' + esc(a.source || a.source_key) + '</td>'
        + '<td>' + esc(a.details || a.message) + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
  }

  function renderPipelineAlerts(alerts) {
    var container = el('pipeline-alerts');
    if (!container) return;
    if (!Array.isArray(alerts) || alerts.length === 0) {
      container.innerHTML = '';
      return;
    }

    var html = '';
    for (var i = 0; i < alerts.length; i++) {
      var a = alerts[i];
      var severity = a.severity || 'warn';
      var alertClass = severity === 'critical' ? 'error' : severity === 'warning' ? 'warn' : severity;
      html += '<div class="ops-alert ' + escAttr(alertClass) + '">'
        + alertIconFor(severity) + ' '
        + esc(a.message || a.title || '')
        + '<span style="float:right;font-size:11px;opacity:0.7">' + timeAgo(a.fired_at || a.fired_at_utc || a.created_at) + '</span>';
      if (a.state === 'fired') {
        html += ' <button class="ops-btn sm" onclick="window._opsAckAlert(\'' + escAttr(a.alert_id) + '\')">Ack</button>';
        html += ' <button class="ops-btn sm" onclick="window._opsSilenceAlert(\'' + escAttr(a.alert_id) + '\')">Silence</button>';
      }
      if (a.state === 'fired' || a.state === 'acknowledged') {
        html += ' <button class="ops-btn sm success" onclick="window._opsResolveAlert(\'' + escAttr(a.alert_id) + '\')">Resolve</button>';
      }
      html += '</div>';
    }
    container.innerHTML = html;
  }

  function alertIconFor(severity) {
    if (severity === 'critical') return '\u274C';
    if (severity === 'warning') return '\u26A0';
    return '\u2139';
  }

  // ─── Screen 2: Queue & Workers ────────────────────────────────────

  async function loadQueueWorkers() {
    var ws = getWorkspaceKey();
    var tr = getTimeRange();

    var results = await Promise.allSettled([
      opsApi('/queue/statusCounts', { params: { workspace_key: ws, time_range: tr } }),
      opsApi('/queue/requests', { params: { workspace_key: ws, limit: 50 } }),
      opsApi('/queue/workers', { params: { workspace_key: ws } })
    ]);

    var anyFailed = false;

    // Status counts
    if (results[0].status === 'fulfilled') {
      var counts = results[0].value;
      setText('queue-queued', fmtNumber(counts.queued || 0));
      setText('queue-leased', fmtNumber(counts.leased || 0));
      setText('queue-running', fmtNumber(counts.running || 0));
      setText('queue-retrying', fmtNumber(counts.retrying || 0));
      setText('queue-failed', fmtNumber(counts.failed || 0));
      setText('queue-dlq', fmtNumber(counts.dlq || 0));

      // Update nav badge
      var failedTotal = (counts.failed || 0) + (counts.dlq || 0);
      updateNavBadge('queue-workers', failedTotal > 0 ? failedTotal : null);
    } else { anyFailed = true; }

    // Queue requests
    if (results[1].status === 'fulfilled') {
      renderQueueTable(results[1].value);
    } else { anyFailed = true; }

    // Workers
    if (results[2].status === 'fulfilled') {
      renderWorkerTable(results[2].value);
    } else { anyFailed = true; }

    if (anyFailed) throw new Error('One or more queue/worker requests failed');
  }

  function renderQueueTable(requests) {
    var tbody = el('queueBody');
    if (!tbody) return;
    if (!Array.isArray(requests) || requests.length === 0) {
      tbody.innerHTML = '<tr><td colspan="9">No requests in queue</td></tr>';
      return;
    }

    var html = '';
    for (var i = 0; i < requests.length; i++) {
      var r = requests[i];
      var rid = r.request_id || r.id;
      var status = r.status || '';
      var statusLower = status.toLowerCase();
      var actions = '';

      if (statusLower === 'running' || statusLower === 'leased' || statusLower === 'queued') {
        actions += '<button class="ops-btn sm danger" onclick="window._opsCancelRequest(\'' + escAttr(rid) + '\', this)">Cancel</button>';
      }
      if (statusLower === 'retrying' || statusLower === 'failed') {
        actions += '<button class="ops-btn sm warning" onclick="window._opsRetryRequest(\'' + escAttr(rid) + '\', this)">Retry</button>';
        actions += ' <button class="ops-btn sm danger" onclick="window._opsCancelRequest(\'' + escAttr(rid) + '\', this)">Cancel</button>';
      }
      if (statusLower === 'failed' || statusLower === 'dlq') {
        actions += '<button class="ops-btn sm" onclick="window._opsRequeueRequest(\'' + escAttr(rid) + '\', this)">Requeue</button>';
        actions += ' <button class="ops-btn sm" onclick="window._opsInspectCorrelation(\'' + escAttr(rid) + '\')">Inspect</button>';
      }

      var lastErr = r.last_error || '\u2014';
      var lastErrStyle = r.last_error ? ' style="color:var(--red);font-size:11px"' : '';

      html += '<tr data-id="' + escAttr(rid) + '" data-status="' + escAttr(statusLower) + '">'
        + '<td><input type="checkbox"></td>'
        + '<td class="mono primary">' + esc(rid) + '</td>'
        + '<td>' + esc(r.source || r.source_key) + '</td>'
        + '<td>' + badge(statusColor(status), status) + '</td>'
        + '<td class="mono">' + esc(r.worker_id || '\u2014') + '</td>'
        + '<td class="mono">' + timeAgo(r.requested_at || r.created_at) + '</td>'
        + '<td>' + (r.attempts != null ? r.attempts : '\u2014') + '</td>'
        + '<td' + lastErrStyle + '>' + esc(lastErr) + '</td>'
        + '<td class="row-actions">' + actions + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
  }

  function renderWorkerTable(workers) {
    var tbody = el('workerBody');
    if (!tbody) return;
    if (!Array.isArray(workers) || workers.length === 0) {
      tbody.innerHTML = '<tr><td colspan="7">No workers registered</td></tr>';
      return;
    }

    var html = '';
    for (var i = 0; i < workers.length; i++) {
      var w = workers[i];
      var wid = w.worker_id || w.id;
      var wstatus = w.status || 'unknown';
      var wstatusLower = wstatus.toLowerCase();
      var actions = '';

      if (wstatusLower === 'active' || wstatusLower === 'running' || wstatusLower === 'idle') {
        actions += '<button class="ops-btn sm danger" onclick="window._opsDrainWorker(\'' + escAttr(wid) + '\', this)">Drain</button>';
      }
      if (wstatusLower === 'draining' || wstatusLower === 'drained') {
        actions += '<button class="ops-btn sm success" onclick="window._opsUndrainWorker(\'' + escAttr(wid) + '\', this)">Undrain</button>';
      }
      if (wstatusLower === 'stale' || wstatusLower === 'unresponsive') {
        actions += '<button class="ops-btn sm warning" onclick="window._opsForceReleaseWorker(\'' + escAttr(wid) + '\', this)">Force Release</button>';
      }

      var hbStyle = '';
      if (w.heartbeat_stale) hbStyle = ' style="color:var(--red)"';

      html += '<tr data-id="' + escAttr(wid) + '">'
        + '<td class="mono primary">' + esc(wid) + '</td>'
        + '<td>' + badge(statusColor(wstatus), wstatus) + '</td>'
        + '<td class="mono">' + esc(w.current_request || '\u2014') + '</td>'
        + '<td class="mono"' + hbStyle + '>' + timeAgo(w.last_heartbeat) + '</td>'
        + '<td>' + esc(w.uptime || '\u2014') + '</td>'
        + '<td>' + fmtNumber(w.jobs_completed) + '</td>'
        + '<td class="row-actions">' + actions + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
  }

  // ─── Screen 3: Source Health ──────────────────────────────────────

  async function loadSourceHealth() {
    var ws = getWorkspaceKey();

    var results = await Promise.allSettled([
      opsApi('/sources/kafka', { params: { workspace_key: ws } }),
      opsApi('/sources/file', { params: { workspace_key: ws } }),
      opsApi('/sources/api', { params: { workspace_key: ws } }),
      opsApi('/sources/dataLossRisk', { params: { workspace_key: ws } })
    ]);

    var anyFailed = false;

    var allSources = [];

    // Kafka
    if (results[0].status === 'fulfilled') {
      var kafka = results[0].value;
      renderKafkaSourceTable(kafka);
      if (Array.isArray(kafka)) allSources = allSources.concat(kafka);
    } else { anyFailed = true; }

    // File
    if (results[1].status === 'fulfilled') {
      var file = results[1].value;
      renderFileSourceTable(file);
      if (Array.isArray(file)) allSources = allSources.concat(file);
    } else { anyFailed = true; }

    // API
    if (results[2].status === 'fulfilled') {
      var api = results[2].value;
      renderApiSourceTable(api);
      if (Array.isArray(api)) allSources = allSources.concat(api);
    } else { anyFailed = true; }

    // Data Loss Risk
    if (results[3].status === 'fulfilled') {
      renderDataLossTable(results[3].value);
    } else { anyFailed = true; }

    // Compute aggregate metrics
    var healthy = 0, degraded = 0, down = 0, totalThroughput = 0;
    for (var i = 0; i < allSources.length; i++) {
      var s = allSources[i];
      var st = (s.status || '').toLowerCase();
      if (st === 'healthy' || st === 'running' || st === 'active' || st === 'ok') healthy++;
      else if (st === 'degraded' || st === 'warning' || st === 'lag risk' || st === 'lag_risk' || st === 'throttled') degraded++;
      else if (st === 'failed' || st === 'down' || st === 'error') down++;
      else healthy++;
      totalThroughput += (s.throughput_num || 0);
    }
    setText('sh-healthy', fmtNumber(healthy));
    setText('sh-degraded', fmtNumber(degraded));
    setText('sh-down', fmtNumber(down));
    var tpDisplay = totalThroughput >= 1000 ? (totalThroughput / 1000).toFixed(1) + 'K' : fmtNumber(totalThroughput);
    setText('sh-throughput', tpDisplay);
    setHtml('sh-throughput-sub', 'rows/min across all');

    if (anyFailed) throw new Error('One or more source health requests failed');
  }

  function renderKafkaSourceTable(sources) {
    var tbody = el('kafkaSourceBody');
    if (!tbody) return;
    if (!Array.isArray(sources) || sources.length === 0) {
      tbody.innerHTML = '<tr><td colspan="10">No Kafka sources</td></tr>';
      return;
    }
    var html = '';
    for (var i = 0; i < sources.length; i++) {
      var s = sources[i];
      var lag = s.freshness_lag_seconds != null ? s.freshness_lag_seconds : s.lag;
      var lagRisk = s.overdue ? 'high' : 'low';
      var lagStyle = s.overdue ? ' style="color:var(--yellow);font-weight:600"' : '';
      html += '<tr>'
        + '<td class="primary">' + esc(s.endpoint_name || s.name || s.source_key) + '</td>'
        + '<td class="mono">' + esc(s.source_system || s.topic || '\u2014') + '</td>'
        + '<td>' + fmtNumber(s.api_node_id || s.partitions || 1) + '</td>'
        + '<td class="mono">' + esc(s.last_success_at_utc ? fmtTime(s.last_success_at_utc) : '\u2014') + '</td>'
        + '<td class="mono">' + esc(s.max_watermark || '\u2014') + '</td>'
        + '<td' + lagStyle + '>' + esc(lag != null ? (lag + 's') : '\u2014') + '</td>'
        + '<td>' + badge(statusColor(lagRisk), lagRisk || '\u2014') + '</td>'
        + '<td class="mono">' + fmtNumber(s.rows_written) + '</td>'
        + '<td>' + esc(s.updated_at_utc ? timeAgo(s.updated_at_utc) : '\u2014') + '</td>'
        + '<td>' + badge(statusColor(s.status), s.status || '\u2014') + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
  }

  function renderFileSourceTable(sources) {
    var tbody = el('fileSourceBody');
    if (!tbody) return;
    if (!Array.isArray(sources) || sources.length === 0) {
      tbody.innerHTML = '<tr><td colspan="8">No file sources</td></tr>';
      return;
    }
    var html = '';
    for (var i = 0; i < sources.length; i++) {
      var s = sources[i];
      var failStyle = '';
      html += '<tr>'
        + '<td class="primary">' + esc(s.endpoint_name || s.name || s.source_key) + '</td>'
        + '<td>' + esc(s.source_system || s.type || s.file_type) + '</td>'
        + '<td>' + fmtNumber(s.graph_id) + '</td>'
        + '<td>' + fmtNumber(s.api_node_id) + '</td>'
        + '<td>' + fmtNumber(s.rows_written) + '</td>'
        + '<td' + failStyle + '>' + esc(s.freshness_lag_seconds != null ? (s.freshness_lag_seconds + 's') : '\u2014') + '</td>'
        + '<td class="mono">' + esc(s.max_watermark || '\u2014') + '</td>'
        + '<td>' + badge(statusColor(s.status), s.status || '\u2014') + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
  }

  function renderApiSourceTable(sources) {
    var tbody = el('apiSourceBody');
    if (!tbody) return;
    if (!Array.isArray(sources) || sources.length === 0) {
      tbody.innerHTML = '<tr><td colspan="8">No API sources</td></tr>';
      return;
    }
    var html = '';
    for (var i = 0; i < sources.length; i++) {
      var s = sources[i];
      var duration = '\u2014';
      if (s.last_run_duration_seconds != null) {
        var dur = Number(s.last_run_duration_seconds);
        if (dur >= 3600) duration = Math.floor(dur / 3600) + 'h ' + Math.floor((dur % 3600) / 60) + 'm';
        else if (dur >= 60) duration = Math.floor(dur / 60) + 'm ' + (dur % 60) + 's';
        else duration = dur + 's';
      }
      var lastRun = s.last_success_at_utc ? timeAgo(s.last_success_at_utc) : '\u2014';
      var freshness = s.freshness_lag_seconds != null ? fmtDuration(s.freshness_lag_seconds) : '\u2014';
      html += '<tr>'
        + '<td class="primary">' + esc(s.endpoint_name || s.name || '') + '</td>'
        + '<td class="mono">' + esc(s.source_system || '') + '</td>'
        + '<td class="mono">' + esc(s.target_table || '\u2014') + '</td>'
        + '<td>' + esc(lastRun) + '</td>'
        + '<td class="mono">' + esc(duration) + '</td>'
        + '<td>' + fmtNumber(s.rows_written) + '</td>'
        + '<td>' + esc(freshness) + '</td>'
        + '<td>' + badge(statusColor(s.status), s.status || '\u2014') + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
  }

  function renderDataLossTable(risks) {
    var tbody = el('dataLossBody');
    if (!tbody) return;
    var rows = [];
    if (Array.isArray(risks)) rows = risks;
    else if (risks && typeof risks === 'object') {
      function pushRows(items, type, level) {
        if (!Array.isArray(items)) return;
        for (var i = 0; i < items.length; i++) {
          rows.push({
            source_key: (items[i].source_system || '') + '::' + (items[i].endpoint_name || ''),
            risk_type: type,
            risk_level: level,
            description: items[i].status || items[i].event_type || type,
            checked_at: items[i].finished_at_utc || items[i].updated_at_utc || items[i].last_success_at_utc
          });
        }
      }
      pushRows(risks.stale_sources, 'stale_source', 'high');
      pushRows(risks.high_lag_sources, 'high_lag', 'medium');
      pushRows(risks.checkpoint_gaps, 'checkpoint_gap', risks.risk_level || 'high');
    }
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="5">No data loss risks detected</td></tr>';
      return;
    }
    var html = '';
    for (var i = 0; i < rows.length; i++) {
      var r = rows[i];
      html += '<tr>'
        + '<td class="primary">' + esc(r.source || r.source_key) + '</td>'
        + '<td>' + esc(r.indicator || r.risk_type) + '</td>'
        + '<td>' + badge(statusColor(r.risk_level), r.risk_level || '\u2014') + '</td>'
        + '<td>' + esc(r.detail || r.description) + '</td>'
        + '<td class="mono">' + timeAgo(r.last_checked || r.checked_at) + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
  }

  // ─── Screen 4: Batches & Manifests ────────────────────────────────

  async function loadBatches() {
    var ws = getWorkspaceKey();
    var data = await opsApi('/batches/summary', { params: { workspace_key: ws } });

    if (data) {
      setText('batch-committed', fmtNumber(data.committed || 0));
      setText('batch-preparing', fmtNumber(data.preparing || 0));
      setText('batch-pending', fmtNumber(data.pending_checkpoint || 0));
      setText('batch-rolled-back', fmtNumber(data.rolled_back || 0));
      setText('batch-archived', fmtNumber(data.archived || 0));

      renderManifestTable(data.manifests || data.batches || []);
    }
  }

  function renderManifestTable(manifests) {
    var tbody = el('manifestBody');
    if (!tbody) return;
    if (!Array.isArray(manifests) || manifests.length === 0) {
      tbody.innerHTML = '<tr><td colspan="9">No manifests found</td></tr>';
      return;
    }
    var html = '';
    for (var i = 0; i < manifests.length; i++) {
      var m = manifests[i];
      var mid = m.manifest_id || m.id;
      var bid = m.batch_id || '';
      var status = m.status || '';
      var actions = '<button class="ops-btn sm" onclick="window._opsInspectManifest(\'' + escAttr(mid) + '\')">Inspect</button>';
      html += '<tr data-id="' + escAttr(mid) + '" data-batch="' + escAttr(bid) + '">'
        + '<td class="mono primary">' + esc(mid) + '</td>'
        + '<td>' + esc(m.source || m.source_key) + '</td>'
        + '<td class="mono">' + esc(bid) + '</td>'
        + '<td>' + badge(statusColor(status), status) + '</td>'
        + '<td>' + fmtNumber(m.rows) + '</td>'
        + '<td>' + fmtNumber(m.bad) + '</td>'
        + '<td>' + esc(m.size || '\u2014') + '</td>'
        + '<td class="mono">' + fmtTime(m.created_at || m.created) + '</td>'
        + '<td class="row-actions">' + actions + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
  }

  // ─── Screen 5: Checkpoints & Replay ──────────────────────────────

  async function loadCheckpoints() {
    var ws = getWorkspaceKey();

    var results = await Promise.allSettled([
      opsApi('/checkpoints/current', { params: { workspace_key: ws } }),
      opsApi('/replay/active', { params: { workspace_key: ws } })
    ]);

    var anyFailed = false;

    // Current checkpoints
    if (results[0].status === 'fulfilled') {
      renderCheckpointTable(results[0].value);
      populateReplaySourceOptions(results[0].value);
    } else { anyFailed = true; }

    // Active replays
    if (results[1].status === 'fulfilled') {
      var replays = results[1].value;
      var panel = el('activeReplays');
      var activeBody = el('activeReplayBody');
      if (panel && activeBody) {
        if (Array.isArray(replays) && replays.length > 0) {
          panel.style.display = '';
          renderActiveReplayTable(replays);
        } else {
          panel.style.display = 'none';
          activeBody.innerHTML = '';
        }
      }
    } else { anyFailed = true; }

    if (anyFailed) throw new Error('One or more checkpoint/replay requests failed');
  }

  function renderCheckpointTable(checkpoints) {
    var tbody = el('checkpointBody');
    if (!tbody) return;
    if (!Array.isArray(checkpoints) || checkpoints.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6">No checkpoints found</td></tr>';
      return;
    }
    var html = '';
    for (var i = 0; i < checkpoints.length; i++) {
      var c = checkpoints[i];
      var sourceKey = c.source_key || c.source || '';
      var staleStyle = c.stale ? ' style="color:var(--red)"' : '';
      html += '<tr data-source="' + escAttr(sourceKey) + '">'
        + '<td class="primary">' + esc(sourceKey) + '</td>'
        + '<td>' + esc(c.checkpoint_type || c.type) + '</td>'
        + '<td class="mono">' + esc(c.checkpoint_key || c.key) + '</td>'
        + '<td class="mono"' + staleStyle + '>' + esc(c.current_value || c.value) + '</td>'
        + '<td class="mono"' + staleStyle + '>' + timeAgo(c.last_updated || c.updated_at) + '</td>'
        + '<td class="row-actions">'
        + '<button class="ops-btn sm" onclick="window._opsViewCheckpointHistory(\'' + escAttr(sourceKey) + '\')">History</button>'
        + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
  }

  function populateReplaySourceOptions(checkpoints) {
    var select = el('replaySource');
    if (!select || !Array.isArray(checkpoints)) return;
    var html = '<option value="">Select source…</option>';
    for (var i = 0; i < checkpoints.length; i++) {
      var c = checkpoints[i];
      html += '<option value="' + escAttr(c.source_key) + '"'
        + ' data-graph-id="' + escAttr(c.graph_id) + '"'
        + ' data-node-id="' + escAttr(c.node_id) + '"'
        + ' data-source-kind="' + escAttr(c.source_kind) + '"'
        + ' data-endpoint-name="' + escAttr(c.endpoint_name) + '">'
        + esc(c.source_key)
        + '</option>';
    }
    select.innerHTML = html;
  }

  function renderActiveReplayTable(replays) {
    var tbody = el('activeReplayBody');
    if (!tbody) return;
    var html = '';
    for (var i = 0; i < replays.length; i++) {
      var r = replays[i];
      html += '<tr>'
        + '<td class="mono primary">' + esc(r.replay_id || r.id) + '</td>'
        + '<td>' + esc(r.source || r.source_key) + '</td>'
        + '<td>' + badge(statusColor(r.status), r.status) + '</td>'
        + '<td class="mono">' + esc(r.from_batch || '\u2014') + '</td>'
        + '<td class="mono">' + timeAgo(r.started_at || r.created_at) + '</td>'
        + '<td>' + esc(r.progress || '\u2014') + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
  }

  // ─── Screen 6: Bad Records ────────────────────────────────────────

  async function loadBadRecords() {
    var ws = getWorkspaceKey();
    var data = await opsApi('/badRecords/summary', { params: { workspace_key: ws } });

    if (data) {
      setText('br-total', fmtNumber(data.total || 0));
      setText('br-pending', fmtNumber(data.pending || 0));
      setText('br-replayed', fmtNumber(data.replayed || 0));
      setText('br-ignored', fmtNumber(data.ignored || 0));

      // Update nav badge
      var pendingBr = data.pending || 0;
      updateNavBadge('bad-records', pendingBr > 0 ? pendingBr : null);

      renderBadRecordTable(data.records || []);
    }
  }

  function renderBadRecordTable(records) {
    var tbody = el('badRecordBody');
    if (!tbody) return;
    if (!Array.isArray(records) || records.length === 0) {
      tbody.innerHTML = '<tr><td colspan="8">No bad records</td></tr>';
      return;
    }
    var html = '';
    for (var i = 0; i < records.length; i++) {
      var r = records[i];
      var rid = r.record_id || r.id;
      html += '<tr data-id="' + escAttr(rid) + '">'
        + '<td><input type="checkbox"></td>'
        + '<td class="mono primary">' + esc(rid) + '</td>'
        + '<td>' + esc(r.source || r.source_key) + '</td>'
        + '<td class="mono">' + esc(r.batch_id || r.batch) + '</td>'
        + '<td>' + badge(statusColor(r.failure_class), r.failure_class) + '</td>'
        + '<td style="font-size:11px">' + esc(r.reason || r.message) + '</td>'
        + '<td class="mono">' + timeAgo(r.timestamp || r.created_at) + '</td>'
        + '<td class="row-actions">'
        + '<button class="ops-btn sm" onclick="window._opsViewBadRecord(\'' + escAttr(rid) + '\')">View</button>'
        + ' <button class="ops-btn sm warning" onclick="window._opsReplayBadRecord(\'' + escAttr(rid) + '\', this)">Replay</button>'
        + ' <button class="ops-btn sm" onclick="window._opsIgnoreBadRecord(\'' + escAttr(rid) + '\', this)">Ignore</button>'
        + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
  }

  // ─── Screen 7: Schema & Medallion ─────────────────────────────────

  // ─── Screen: Schema Drift Detection ────────────────────────────────

  async function loadSchemaDrift() {
    var ws = getWorkspaceKey();
    var severityFilter = (el('driftSeverityFilter') || {}).value || '';

    var results = await Promise.allSettled([
      opsApi('/schema/driftEvents', { params: { workspace_key: ws, severity: severityFilter, acknowledged: 'false', limit: 100 } }),
      opsApi('/schema/notifications/unreadCount', { params: { workspace_key: ws } }),
      opsApi('/schema/notifications', { params: { workspace_key: ws, unread_only: 'true', limit: 10 } })
    ]);

    var anyFailed = false;

    if (results[0].status === 'fulfilled') {
      renderDriftEvents(results[0].value);
    } else { anyFailed = true; }

    if (results[1].status === 'fulfilled') {
      var unread = (results[1].value && results[1].value.unread_count) || 0;
      setText('driftUnreadCount', unread);
      var navBadge = el('driftNavBadge');
      if (navBadge) {
        if (unread > 0) {
          navBadge.textContent = unread > 99 ? '99+' : unread;
          navBadge.style.display = '';
        } else {
          navBadge.style.display = 'none';
        }
      }
    }

    if (results[2].status === 'fulfilled') {
      renderDriftNotificationBanner(results[2].value);
    }

    if (anyFailed) throw new Error('One or more schema drift requests failed');
  }

  function renderDriftEvents(events) {
    var tbody = el('driftEventBody');
    if (!tbody) return;
    if (!Array.isArray(events) || events.length === 0) {
      tbody.innerHTML = '<tr><td colspan="7" style="color:var(--text-secondary)">No drift events found</td></tr>';
      setText('driftUnackedCount', '0');
      setText('driftBreakingCount', '0');
      return;
    }

    var unacked = 0;
    var breaking = 0;
    var html = '';
    for (var i = 0; i < events.length; i++) {
      var e = events[i];
      if (!e.acknowledged) unacked++;
      if (e.drift_severity === 'breaking') breaking++;

      var severityBadge = badge(
        e.drift_severity === 'breaking' ? 'failed' :
        e.drift_severity === 'warning' ? 'warning' : 'queued',
        e.drift_severity
      );

      var actions = '<button class="ops-btn sm" onclick="window._opsViewDriftDetail(' + e.event_id + ')">View</button>';
      if (!e.acknowledged) {
        actions += ' <button class="ops-btn sm success" onclick="window._opsAckDrift(' + e.event_id + ', this)">Ack</button>';
      }
      actions += ' <button class="ops-btn sm" onclick="window._opsViewDriftTimeline(' + e.graph_id + ', \'' + escAttr(e.endpoint_name) + '\')">Timeline</button>';

      html += '<tr data-event-id="' + e.event_id + '">'
        + '<td class="primary" title="Graph ' + e.graph_id + '">' + esc(e.endpoint_name) + '</td>'
        + '<td>' + severityBadge + '</td>'
        + '<td>' + (e.new_field_count || 0) + '</td>'
        + '<td>' + (e.missing_field_count || 0) + '</td>'
        + '<td>' + (e.type_change_count || 0) + '</td>'
        + '<td class="mono">' + timeAgo(e.detected_at_utc) + '</td>'
        + '<td>' + actions + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
    setText('driftUnackedCount', unacked);
    setText('driftBreakingCount', breaking);
  }

  function renderDriftNotificationBanner(notifications) {
    var banner = el('driftNotificationBanner');
    var text = el('driftNotificationText');
    if (!banner || !text) return;
    if (!Array.isArray(notifications) || notifications.length === 0) {
      banner.style.display = 'none';
      return;
    }
    var latest = notifications[0];
    text.textContent = notifications.length + ' unread: ' + (latest.title || 'Schema drift detected');
    banner.style.display = '';
  }

  function renderDriftDetail(event) {
    var container = el('driftDetailContent');
    if (!container) return;
    currentDriftEvent = event || null;
    if (!event) {
      container.innerHTML = '<div style="color:var(--text-secondary);padding:16px">Select a drift event to view details</div>';
      return;
    }

    var drift = null;
    try { drift = typeof event.drift_json === 'string' ? JSON.parse(event.drift_json) : event.drift_json; } catch (_) {}

    var html = '<div style="margin-bottom:12px">'
      + '<strong>' + esc(event.endpoint_name) + '</strong><br>'
      + '<span style="color:var(--text-secondary)">Graph: ' + event.graph_id + ' &middot; Node: ' + event.api_node_id + ' &middot; Mode: ' + esc(event.enforcement_mode || '\u2014') + '</span><br>'
      + '<span style="color:var(--text-secondary)">Detected: ' + esc(event.detected_at_utc) + '</span>'
      + '</div>';

    html += '<div style="display:flex;gap:8px;margin-bottom:12px">'
      + '<button class="ops-btn sm" onclick="window._opsPreviewDriftDdl()">Preview DDL</button>'
      + '<button class="ops-btn sm success" onclick="window._opsApplyDriftDdl()">Apply DDL</button>'
      + '</div>';

    if (drift && drift.new_fields && drift.new_fields.length) {
      html += '<div style="margin-bottom:12px"><strong style="color:var(--green)">New Fields (+' + drift.new_fields.length + ')</strong>';
      html += '<table class="ops-table" style="margin-top:4px"><thead><tr><th>Column</th><th>Type</th><th>Coverage</th></tr></thead><tbody>';
      for (var i = 0; i < drift.new_fields.length; i++) {
        var nf = drift.new_fields[i];
        html += '<tr><td>' + esc(nf.column_name || nf.path) + '</td><td class="mono">' + esc(nf.type || '\u2014') + '</td><td>' + (nf.sample_coverage != null ? (nf.sample_coverage * 100).toFixed(0) + '%' : '\u2014') + '</td></tr>';
      }
      html += '</tbody></table></div>';
    }

    if (drift && drift.missing_fields && drift.missing_fields.length) {
      html += '<div style="margin-bottom:12px"><strong style="color:var(--red)">Missing Fields (-' + drift.missing_fields.length + ')</strong>';
      html += '<table class="ops-table" style="margin-top:4px"><thead><tr><th>Column</th><th>Type</th></tr></thead><tbody>';
      for (var j = 0; j < drift.missing_fields.length; j++) {
        var mf = drift.missing_fields[j];
        html += '<tr><td>' + esc(mf.column_name || mf.path) + '</td><td class="mono">' + esc(mf.type || '\u2014') + '</td></tr>';
      }
      html += '</tbody></table></div>';
    }

    if (drift && drift.type_changes && drift.type_changes.length) {
      html += '<div style="margin-bottom:12px"><strong style="color:var(--yellow)">Type Changes (' + drift.type_changes.length + ')</strong>';
      html += '<table class="ops-table" style="margin-top:4px"><thead><tr><th>Column</th><th>Was</th><th>Now</th></tr></thead><tbody>';
      for (var k = 0; k < drift.type_changes.length; k++) {
        var tc = drift.type_changes[k];
        html += '<tr><td>' + esc(tc.path || tc.column_name) + '</td><td class="mono">' + esc(tc.current_type) + '</td><td class="mono">' + esc(tc.inferred_type) + '</td></tr>';
      }
      html += '</tbody></table></div>';
    }

    if (!drift) {
      html += '<div style="color:var(--text-secondary)">Drift details not available</div>';
    }

    html += '<div id="driftDdlPanel" style="margin-top:12px"></div>';

    container.innerHTML = html;
  }

  function renderDriftDdlPanel(data, opts) {
    var panel = el('driftDdlPanel');
    if (!panel) return;
    opts = opts || {};
    if (!data) {
      panel.innerHTML = '';
      return;
    }
    var summary = data.summary || {};
    var ddlPlan = data.ddl_plan || {};
    var addCols = ddlPlan['add-columns'] || ddlPlan.addColumns || ddlPlan.add_columns || [];
    var widenCols = ddlPlan['widen-columns'] || ddlPlan.widenColumns || ddlPlan.widen_columns || [];
    var html = '<div class="ops-panel" style="margin-top:8px">'
      + '<div class="ops-panel-header"><div class="ops-panel-title"><span class="panel-icon">&#9881;</span>'
      + (opts.applied ? 'DDL Apply Results' : 'DDL Preview')
      + '</div></div>'
      + '<div class="ops-panel-body padded">'
      + '<div style="margin-bottom:10px"><strong>Table:</strong> <span class="mono">' + esc(data.table_name || '\u2014') + '</span>'
      + ' &middot; <strong>Warehouse:</strong> ' + esc(data.warehouse || '\u2014')
      + ' &middot; <strong>Add:</strong> ' + esc(String(summary.add_column_count != null ? summary.add_column_count : addCols.length))
      + ' &middot; <strong>Widen:</strong> ' + esc(String(summary.widen_column_count != null ? summary.widen_column_count : widenCols.length))
      + '</div>';

    if (opts.applied) {
      var results = data.results || [];
      if (results.length === 0) {
        html += '<div style="color:var(--text-secondary)">No DDL changes were required.</div>';
      } else {
        html += '<table class="ops-table"><thead><tr><th>Action</th><th>Column</th><th>Status</th><th>Error</th></tr></thead><tbody>';
        for (var i = 0; i < results.length; i++) {
          var r = results[i];
          html += '<tr><td>' + esc(r.action || '\u2014') + '</td>'
            + '<td class="mono">' + esc(r.column || '\u2014') + '</td>'
            + '<td>' + badge(r.status === 'applied' ? 'running' : 'failed', r.status || '\u2014') + '</td>'
            + '<td>' + esc(r.error || '\u2014') + '</td></tr>';
        }
        html += '</tbody></table>';
      }
    } else {
      if (addCols.length === 0 && widenCols.length === 0) {
        html += '<div style="color:var(--text-secondary)">No DDL changes are required for the currently promoted schema.</div>';
      } else {
        html += '<table class="ops-table"><thead><tr><th>Action</th><th>Column</th><th>Type</th><th>SQL</th></tr></thead><tbody>';
        for (var j = 0; j < addCols.length; j++) {
          var add = addCols[j];
          html += '<tr><td>add_column</td><td class="mono">' + esc((add.column || {}).column_name || '\u2014') + '</td>'
            + '<td class="mono">' + esc((add.column || {}).type || '\u2014') + '</td>'
            + '<td class="mono">' + esc(add.sql || '\u2014') + '</td></tr>';
        }
        for (var k = 0; k < widenCols.length; k++) {
          var widen = widenCols[k];
          html += '<tr><td>widen_type</td><td class="mono">' + esc(widen.column_name || '\u2014') + '</td>'
            + '<td class="mono">' + esc((widen.from_type || '\u2014') + ' \u2192 ' + (widen.to_type || '\u2014')) + '</td>'
            + '<td class="mono">' + esc(widen.sql || '\u2014') + '</td></tr>';
        }
        html += '</tbody></table>';
      }
    }

    html += '</div></div>';
    panel.innerHTML = html;
  }

  function renderDriftTimeline(data) {
    var panel = el('driftTimelinePanel');
    var tbody = el('driftTimelineBody');
    if (!panel || !tbody) return;
    panel.style.display = '';

    var timeline = (data && data.timeline) || [];
    if (timeline.length === 0) {
      tbody.innerHTML = '<tr><td colspan="4" style="color:var(--text-secondary)">No timeline entries</td></tr>';
      return;
    }

    var html = '';
    for (var i = 0; i < timeline.length; i++) {
      var t = timeline[i];
      var typeBadge = badge(
        t.type === 'drift_detected' ? 'warning' :
        t.type === 'ddl_applied' ? 'running' : 'queued',
        t.type
      );
      var details = '';
      if (t.type === 'drift_detected') {
        details = (t.new_field_count || 0) + ' new, ' + (t.missing_field_count || 0) + ' missing, ' + (t.type_change_count || 0) + ' type changes';
        details += ' \u2014 ' + badge(t.severity === 'breaking' ? 'failed' : t.severity === 'warning' ? 'warning' : 'queued', t.severity);
      } else if (t.type === 'ddl_applied') {
        details = esc(t.action || '') + ' ' + esc(t.column_name || '') + ' \u2014 ' + badge(statusColor(t.status), t.status);
      }

      html += '<tr>'
        + '<td>' + typeBadge + '</td>'
        + '<td class="mono">' + timeAgo(t.time) + '</td>'
        + '<td>' + details + '</td>'
        + '<td>' + esc(t.acknowledged_by || t.applied_by || '\u2014') + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
  }

  // ── Schema Drift Actions ──

  async function viewDriftDetail(eventId) {
    try {
      var event = await opsApi('/schema/driftEvents/' + eventId, { params: { workspace_key: getWorkspaceKey() } });
      renderDriftDetail(event);
    } catch (e) {
      alert('Error loading drift detail: ' + e.message);
    }
  }

  async function ackDrift(eventId, btn) {
    if (btn) { btn.disabled = true; btn.textContent = '...'; }
    try {
      await opsApi('/schema/driftEvents/' + eventId + '/ack', { method: 'POST', body: { actor: 'operator', workspace_key: getWorkspaceKey() } });
      await loadSchemaDrift();
    } catch (e) {
      alert('Error: ' + e.message);
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = 'Ack'; }
    }
  }

  async function viewDriftTimeline(graphId, endpointName) {
    try {
      var data = await opsApi('/schema/timeline', { params: { workspace_key: getWorkspaceKey(), graph_id: graphId, endpoint_name: endpointName, limit: 50 } });
      renderDriftTimeline(data);
    } catch (e) {
      alert('Error loading timeline: ' + e.message);
    }
  }

  function closeDriftTimeline() {
    var panel = el('driftTimelinePanel');
    if (panel) panel.style.display = 'none';
  }

  async function dismissSchemaNotifications() {
    var ws = getWorkspaceKey();
    try {
      var notifs = await opsApi('/schema/notifications', { params: { workspace_key: ws, unread_only: 'true', limit: 200 } });
      if (Array.isArray(notifs) && notifs.length > 0) {
        var ids = notifs.map(function (n) { return n.notification_id; });
        await opsApi('/schema/notifications/markRead', { method: 'POST', body: { workspace_key: ws, notification_ids: ids } });
      }
      await loadSchemaDrift();
    } catch (e) {
      alert('Error: ' + e.message);
    }
  }

  async function previewDriftDdl() {
    if (!currentDriftEvent) return;
    try {
      var data = await opsApi('/schema/previewDdl', {
        method: 'POST',
        body: {
          workspace_key: getWorkspaceKey(),
          graph_id: currentDriftEvent.graph_id,
          api_node_id: currentDriftEvent.api_node_id,
          endpoint_name: currentDriftEvent.endpoint_name,
          event_id: currentDriftEvent.event_id,
          schema_hash: currentDriftEvent.schema_hash_after
        }
      });
      renderDriftDdlPanel(data, { applied: false });
    } catch (e) {
      alert('Error previewing DDL: ' + e.message);
    }
  }

  async function applyDriftDdl() {
    if (!currentDriftEvent) return;
    if (!confirm('Apply schema DDL for ' + currentDriftEvent.endpoint_name + '?')) return;
    try {
      var data = await opsApi('/schema/applyDdl', {
        method: 'POST',
        body: {
          workspace_key: getWorkspaceKey(),
          graph_id: currentDriftEvent.graph_id,
          api_node_id: currentDriftEvent.api_node_id,
          endpoint_name: currentDriftEvent.endpoint_name,
          event_id: currentDriftEvent.event_id,
          schema_hash: currentDriftEvent.schema_hash_after,
          actor: 'operator'
        }
      });
      renderDriftDdlPanel(data, { applied: true });
      await loadSchemaDrift();
      await viewDriftTimeline(currentDriftEvent.graph_id, currentDriftEvent.endpoint_name);
    } catch (e) {
      alert('Error applying DDL: ' + e.message);
    }
  }

  async function loadSchemaMedallion() {
    var ws = getWorkspaceKey();

    var results = await Promise.allSettled([
      opsApi('/schema/freshnessChain', { params: { workspace_key: ws } }),
      opsApi('/schema/driftDetection', { params: { workspace_key: ws } }),
      opsApi('/medallion/releases', { params: { workspace_key: ws, limit: 20 } })
    ]);

    var anyFailed = false;

    // Freshness Chain
    if (results[0].status === 'fulfilled') {
      renderFreshnessChain(results[0].value);
    } else { anyFailed = true; }

    // Drift Detection (schema proposals)
    if (results[1].status === 'fulfilled') {
      renderSchemaProposals(results[1].value);
    } else { anyFailed = true; }

    // Medallion releases
    if (results[2].status === 'fulfilled') {
      renderMedallionReleases(results[2].value);
    } else { anyFailed = true; }

    if (anyFailed) throw new Error('One or more schema/medallion requests failed');
  }

  function renderFreshnessChain(chains) {
    var container = el('freshnessChain');
    if (!container) return;
    if (!chains || (!Array.isArray(chains) && !chains.layers)) {
      container.innerHTML = '<div style="padding:16px;color:var(--text-secondary)">No freshness data available</div>';
      return;
    }

    if (Array.isArray(chains) && chains.length && chains[0].bronze_table) {
      var tableHtml = '<table class="ops-table"><thead><tr><th>Bronze</th><th>Bronze Status</th><th>Bronze Lag</th><th>Silver</th><th>Silver Status</th></tr></thead><tbody>';
      for (var ci = 0; ci < chains.length; ci++) {
        var c = chains[ci];
        tableHtml += '<tr>'
          + '<td>' + esc(c.bronze_table) + '</td>'
          + '<td>' + badge(statusColor(c.bronze_status), c.bronze_status || '\u2014') + '</td>'
          + '<td>' + esc(c.bronze_lag_seconds != null ? (c.bronze_lag_seconds + 's') : '\u2014') + '</td>'
          + '<td>' + esc(c.silver_table || '\u2014') + '</td>'
          + '<td>' + badge(statusColor(c.silver_status), c.silver_status || '\u2014') + '</td>'
          + '</tr>';
      }
      tableHtml += '</tbody></table>';
      container.innerHTML = tableHtml;
      return;
    }

    var layers = Array.isArray(chains) ? chains : (chains.layers || []);
    if (layers.length === 0) {
      container.innerHTML = '<div style="padding:16px;color:var(--text-secondary)">No freshness data available</div>';
      return;
    }

    var layerColors = { bronze: 'var(--orange)', silver: 'var(--accent)', gold: 'var(--yellow)' };
    var html = '<div class="freshness-chain">';
    for (var i = 0; i < layers.length; i++) {
      if (i > 0) html += '<div class="freshness-arrow">\u27A4</div>';
      var l = layers[i];
      var layerName = (l.layer || l.name || '').toLowerCase();
      var color = layerColors[layerName] || 'var(--text-primary)';
      html += '<div class="freshness-node">'
        + '<div class="fn-label" style="color:' + color + '">' + esc(l.layer || l.name) + '</div>'
        + '<div class="fn-time">' + esc(l.freshness || l.latency || '\u2014') + '</div>'
        + '<div class="fn-status">' + badge(statusColor(l.status), l.status || '\u2014') + '</div>'
        + '</div>';
    }
    html += '</div>';
    container.innerHTML = html;
  }

  function renderSchemaProposals(proposals) {
    var tbody = el('schemaProposalBody');
    if (!tbody) return;
    if (!Array.isArray(proposals) || proposals.length === 0) {
      tbody.innerHTML = '<tr><td colspan="5">No schema proposals</td></tr>';
      return;
    }
    var html = '';
    for (var i = 0; i < proposals.length; i++) {
      var p = proposals[i];
      html += '<tr data-id="' + escAttr(p.proposal_id || p.id) + '">'
        + '<td class="primary">' + esc(p.proposal_id || p.id || p.graph_id) + '</td>'
        + '<td>' + esc(p.layer || p.target_layer || 'bronze') + '</td>'
        + '<td>' + badge(statusColor(p.status || 'drift'), p.status || 'drift') + '</td>'
        + '<td>' + esc(p.author || p.source_system || '\u2014') + '</td>'
        + '<td class="mono">' + esc(p.date || fmtTime(p.created_at || p.captured_at_utc)) + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
  }

  function renderMedallionReleases(releases) {
    var tbody = el('medallionReleaseBody');
    if (!tbody) return;
    if (!Array.isArray(releases) || releases.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6">No releases found</td></tr>';
      return;
    }
    var layerColors = { bronze: 'var(--orange)', silver: 'var(--accent)', gold: 'var(--yellow)' };
    var html = '';
    for (var i = 0; i < releases.length; i++) {
      var r = releases[i];
      var layerName = (r.layer || '').toLowerCase();
      var color = layerColors[layerName] || '';
      var layerStyle = color ? ' style="color:' + color + '"' : '';
      html += '<tr>'
        + '<td class="primary">' + esc(r.source || r.source_key) + '</td>'
        + '<td' + layerStyle + '>' + esc(r.layer) + '</td>'
        + '<td class="mono">' + fmtTime(r.last_release || r.released_at || r.released_at_utc) + '</td>'
        + '<td class="mono">' + esc(r.schema_version || '\u2014') + '</td>'
        + '<td>' + badge(statusColor(r.status), r.status || '\u2014') + '</td>'
        + '<td>' + esc(r.downstream_trigger || (r.active_release ? 'active' : '\u2014')) + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
  }

  // ─── Screen 8: Admin & Policies ───────────────────────────────────

  async function loadAdmin() {
    var ws = getWorkspaceKey();

    var results = await Promise.allSettled([
      opsApi('/admin/config', { params: { config_key: 'worker_settings', workspace_key: ws } }),
      opsApi('/admin/config', { params: { config_key: 'alert_thresholds', workspace_key: ws } }),
      opsApi('/admin/config', { params: { config_key: 'retention_policies', workspace_key: ws } }),
      opsApi('/admin/config', { params: { config_key: 'source_concurrency', workspace_key: ws } })
    ]);

    // Worker settings
    if (results[0].status === 'fulfilled' && results[0].value) {
      var wc = results[0].value.value || results[0].value;
      setInputValue('cfg-max-workers', wc.max_workers);
      setInputValue('cfg-poll-interval', wc.poll_interval);
      setInputValue('cfg-lease-duration', wc.lease_duration);
      setInputValue('cfg-heartbeat-interval', wc.heartbeat_interval);
      setInputValue('cfg-max-retries', wc.max_retries);
      setInputValue('cfg-orphan-timeout', wc.orphan_timeout);
    }

    // Alert thresholds
    if (results[1].status === 'fulfilled' && results[1].value) {
      var at = results[1].value.value || results[1].value;
      setInputValue('cfg-kafka-lag-warn', at.kafka_lag_warning || at.kafka_lag_warn);
      setInputValue('cfg-kafka-lag-critical', at.kafka_lag_critical);
      setInputValue('cfg-freshness-warn', at.freshness_warning || at.freshness_warn);
      setInputValue('cfg-freshness-critical', at.freshness_critical);
      setInputValue('cfg-bad-record-pct', at.bad_record_pct || at.bad_record_percent);
      setInputValue('cfg-heartbeat-timeout', at.heartbeat_timeout);
    }

    // Retention policies
    if (results[2].status === 'fulfilled' && results[2].value) {
      var rp = results[2].value.value || results[2].value;
      setInputValue('cfg-manifest-retention', rp.manifest_retention);
      setInputValue('cfg-bad-record-retention', rp.bad_record_retention);
      setInputValue('cfg-checkpoint-history', rp.checkpoint_history);
      setInputValue('cfg-dlq-retention', rp.dlq_retention);
      setInputValue('cfg-archive-dest', rp.archive_destination || rp.archive_dest);
    }

    // Source concurrency
    if (results[3].status === 'fulfilled' && results[3].value) {
      renderSourceConcurrencyTable(results[3].value);
    }
  }

  function setInputValue(id, val) {
    var input = el(id);
    if (input && val != null) input.value = val;
  }

  function getInputValue(id) {
    var input = el(id);
    return input ? input.value : null;
  }

  function getNumericValue(id) {
    var raw = getInputValue(id);
    if (raw == null || raw === '') return null;
    var n = Number(raw);
    return isNaN(n) ? null : n;
  }

  function renderSourceConcurrencyTable(sources) {
    var tbody = el('sourceConcurrencyBody');
    if (!tbody) return;
    var data = Array.isArray(sources) ? sources : (sources.sources || []);
    if (data.length === 0) {
      tbody.innerHTML = '<tr><td colspan="4">No source concurrency data</td></tr>';
      return;
    }
    var html = '';
    for (var i = 0; i < data.length; i++) {
      var s = data[i];
      html += '<tr data-source="' + escAttr(s.source_key || s.name) + '">'
        + '<td class="primary">' + esc(s.source_key || s.name) + '</td>'
        + '<td><input class="ops-form-input concurrency-input" style="width:60px;padding:4px 8px" type="number" value="' + (s.max_concurrent || 1) + '" data-source="' + escAttr(s.source_key || s.name) + '"></td>'
        + '<td>' + fmtNumber(s.current || 0) + '</td>'
        + '<td class="mono">' + esc(s.credential_pool || '\u2014') + '</td>'
        + '</tr>';
    }
    tbody.innerHTML = html;
  }

  // ═══════════════════════════════════════════════════════════════════
  // ACTION HANDLERS
  // ═══════════════════════════════════════════════════════════════════

  // ─── Alert Actions ────────────────────────────────────────────────

  async function ackAlert(alertId) {
    try {
      await opsApi('/alerts/' + alertId + '/ack', {
        method: 'POST',
        body: { actor: 'operator' }
      });
      await loadPipelineOverview();
    } catch (e) {
      alert('Error acknowledging alert: ' + e.message);
    }
  }

  async function silenceAlert(alertId) {
    var hours = prompt('Silence for how many hours?', '1');
    if (!hours) return;
    var silenceUntil = new Date(Date.now() + parseFloat(hours) * 3600000).toISOString();
    try {
      await opsApi('/alerts/' + alertId + '/silence', {
        method: 'POST',
        body: { actor: 'operator', silence_until: silenceUntil }
      });
      await loadPipelineOverview();
    } catch (e) {
      alert('Error silencing alert: ' + e.message);
    }
  }

  async function resolveAlert(alertId) {
    if (!confirm('Resolve this alert?')) return;
    try {
      await opsApi('/alerts/' + alertId + '/resolve', {
        method: 'POST',
        body: { actor: 'operator' }
      });
      await loadPipelineOverview();
    } catch (e) {
      alert('Error resolving alert: ' + e.message);
    }
  }

  // ─── Queue Actions ────────────────────────────────────────────────

  async function bulkQueueAction(action, btn) {
    var tbody = el('queueBody');
    if (!tbody) return;
    var checked = tbody.querySelectorAll('input[type="checkbox"]:checked');
    if (!checked.length) { alert('No items selected.'); return; }

    var ids = [];
    for (var i = 0; i < checked.length; i++) {
      var row = checked[i].closest('tr');
      if (row && row.dataset.id) ids.push(row.dataset.id);
    }
    if (ids.length === 0) { alert('No valid items selected.'); return; }
    if (ids.length > 100) { alert('Max 100 items per bulk action.'); return; }
    if (!confirm(action + ' ' + ids.length + ' item(s)?')) return;

    btn.disabled = true;
    var originalText = btn.textContent;
    btn.textContent = '...';
    try {
      await opsApi('/queue/requests/bulk' + action, {
        method: 'POST',
        body: { request_ids: ids, actor: 'operator' }
      });
      await loadQueueWorkers();
    } catch (e) {
      alert('Error: ' + e.message);
    } finally {
      btn.disabled = false;
      btn.textContent = originalText;
    }
  }

  async function singleQueueAction(action, requestId, btn) {
    if (!confirm(action + ' request ' + requestId + '?')) return;
    btn.disabled = true;
    var originalText = btn.textContent;
    btn.textContent = '...';
    try {
      await opsApi('/queue/requests/bulk' + action, {
        method: 'POST',
        body: { request_ids: [requestId], actor: 'operator' }
      });
      await loadQueueWorkers();
    } catch (e) {
      alert('Error: ' + e.message);
    } finally {
      btn.disabled = false;
      btn.textContent = originalText;
    }
  }

  // ─── Worker Actions ───────────────────────────────────────────────

  async function drainWorker(workerId, btn) {
    if (!confirm('Drain worker ' + workerId + '? It will finish its current job then stop taking new work.')) return;
    btn.disabled = true;
    btn.textContent = '...';
    try {
      await opsApi('/queue/workers/' + workerId + '/drain', {
        method: 'POST',
        body: { actor: 'operator' }
      });
      await loadQueueWorkers();
    } catch (e) {
      alert('Error: ' + e.message);
      btn.disabled = false;
      btn.textContent = 'Drain';
    }
  }

  async function undrainWorker(workerId, btn) {
    btn.disabled = true;
    btn.textContent = '...';
    try {
      await opsApi('/queue/workers/' + workerId + '/undrain', {
        method: 'POST',
        body: { actor: 'operator' }
      });
      await loadQueueWorkers();
    } catch (e) {
      alert('Error: ' + e.message);
      btn.disabled = false;
      btn.textContent = 'Undrain';
    }
  }

  async function forceReleaseWorker(workerId, btn) {
    if (!confirm('Force release worker ' + workerId + '? This will abandon its current work item.')) return;
    btn.disabled = true;
    btn.textContent = '...';
    try {
      await opsApi('/queue/workers/' + workerId + '/forceRelease', {
        method: 'POST',
        body: { actor: 'operator' }
      });
      await loadQueueWorkers();
    } catch (e) {
      alert('Error: ' + e.message);
      btn.disabled = false;
      btn.textContent = 'Force Release';
    }
  }

  // ─── Circuit Breaker ──────────────────────────────────────────────

  async function resetCircuitBreaker(sourceId, btn) {
    if (!confirm('Reset circuit breaker for source ' + sourceId + '?')) return;
    btn.disabled = true;
    btn.textContent = '...';
    try {
      await opsApi('/sources/' + sourceId + '/circuitBreaker/reset', {
        method: 'POST',
        body: { actor: 'operator' }
      });
      await loadSourceHealth();
    } catch (e) {
      alert('Error: ' + e.message);
      btn.disabled = false;
      btn.textContent = 'Reset';
    }
  }

  // ─── Batch / Manifest Actions ─────────────────────────────────────

  async function inspectManifest(manifestId) {
    var detailPanel = el('manifestDetail');
    var detailContent = el('manifestDetailContent');
    if (!detailPanel) return;

    detailPanel.style.display = 'block';
    if (detailContent) detailContent.innerHTML = '<div style="padding:16px;color:var(--text-secondary)">Loading...</div>';

    try {
      // Try fetching batch detail using the manifest/batch id
      var batchId = manifestId;
      var results = await Promise.allSettled([
        opsApi('/batches/' + batchId + '/detail'),
        opsApi('/batches/' + batchId + '/artifacts')
      ]);

      var html = '';

      if (results[0].status === 'fulfilled' && results[0].value) {
        var d = results[0].value;
        html += '<div class="ops-kv-grid">';
        html += kvItem('Manifest ID', d.manifest_id || d.id);
        html += kvItem('Batch ID', d.batch_id);
        html += kvItem('Source', d.source || d.source_key);
        html += '<div class="ops-kv-item"><div class="kv-label">Status</div><div class="kv-value">' + badge(statusColor(d.status), d.status) + '</div></div>';
        html += kvItem('Row Count', fmtNumber(d.rows || d.row_count));
        html += kvItem('Bad Records', fmtNumber(d.bad || d.bad_records));
        html += kvItem('Artifact Path', d.artifact_path || d.path);
        html += kvItem('Checkpoint After', d.checkpoint_value || d.checkpoint_after);
        html += '</div>';
      }

      if (results[1].status === 'fulfilled' && results[1].value) {
        var artifacts = results[1].value;
        html += '<div style="margin-top:16px"><div class="ops-kv-item"><div class="kv-label">Artifact Listing</div></div>';
        if (typeof artifacts === 'string') {
          html += '<div class="ops-json">' + esc(artifacts) + '</div>';
        } else if (Array.isArray(artifacts)) {
          var listing = artifacts.map(function (a) { return esc(a.path || a.name || a) + '  (' + esc(a.size || '') + ')'; }).join('\n');
          html += '<div class="ops-json">' + listing + '</div>';
        }
        html += '</div>';
      }

      if (!html) html = '<div style="padding:16px;color:var(--text-secondary)">No detail available</div>';
      if (detailContent) detailContent.innerHTML = html;

      // Update title
      var titleEl = detailPanel.querySelector('.ops-detail-title');
      if (titleEl) titleEl.textContent = 'Manifest ' + manifestId;

      detailPanel.scrollIntoView({ behavior: 'smooth' });
    } catch (e) {
      if (detailContent) detailContent.innerHTML = '<div style="padding:16px;color:var(--red)">Error loading detail: ' + esc(e.message) + '</div>';
    }
  }

  function kvItem(label, value) {
    return '<div class="ops-kv-item"><div class="kv-label">' + esc(label) + '</div><div class="kv-value">' + esc(value) + '</div></div>';
  }

  async function replayBatch(batchId) {
    if (!batchId) return;
    var target = el('replayTarget');
    if (target) target.value = batchId;
    switchScreen('checkpoints-replay');
    alert('Replay batch prefilled. Select the source checkpoint and start replay from the replay panel.');
  }

  // ─── Checkpoint Actions ───────────────────────────────────────────

  async function resetCheckpoint(sourceKey) {
    alert('Checkpoint reset is not available from the generic ops console yet for this source.');
  }

  async function viewCheckpointHistory(sourceKey) {
    var ws = getWorkspaceKey();
    var titleEl = el('checkpointHistoryTitle');
    var tbody = el('checkpointHistoryBody');
    if (titleEl) titleEl.textContent = 'Checkpoint History \u2014 ' + sourceKey;
    if (tbody) tbody.innerHTML = '<tr><td colspan="6">Loading...</td></tr>';

    try {
      var history = await opsApi('/checkpoints/history', { params: { workspace_key: ws, source_key: sourceKey, limit: 20 } });
      if (!tbody) return;
      if (!Array.isArray(history) || history.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6">No history found</td></tr>';
        return;
      }
      var html = '';
      for (var i = 0; i < history.length; i++) {
        var h = history[i];
        var actionStyle = '';
        if ((h.action || '').toLowerCase().indexOf('reset') !== -1) actionStyle = ' style="color:var(--yellow)"';
        html += '<tr>'
          + '<td class="mono">' + fmtTime(h.timestamp || h.created_at) + '</td>'
          + '<td' + actionStyle + '>' + esc(h.action || h.type) + '</td>'
          + '<td class="mono">' + esc(h.old_value || '\u2014') + '</td>'
          + '<td class="mono">' + esc(h.new_value || '\u2014') + '</td>'
          + '<td>' + esc(h.actor || h.by || 'system') + '</td>'
          + '<td class="mono">' + esc(h.batch_id || '\u2014') + '</td>'
          + '</tr>';
      }
      tbody.innerHTML = html;
    } catch (e) {
      if (tbody) tbody.innerHTML = '<tr><td colspan="6" style="color:var(--red)">Error: ' + esc(e.message) + '</td></tr>';
    }
  }

  // ─── Replay from Form ─────────────────────────────────────────────

  async function replayFromCheckpoint(dryRun) {
    var sourceEl = el('replaySource');
    var modeEl = el('replayMode');
    var targetEl = el('replayTarget');
    if (!sourceEl || !targetEl) { alert('Replay form not found'); return; }

    var source = sourceEl.value;
    var mode = modeEl ? modeEl.value : 'batch';
    var target = targetEl.value;
    if (!source || !target) { alert('Please select a source and enter a batch/run ID.'); return; }

    var ws = getWorkspaceKey();
    var sourceOpt = sourceEl.options[sourceEl.selectedIndex];
    var body = {
      workspace_key: ws,
      source_key: source,
      source_kind: sourceOpt ? sourceOpt.dataset.sourceKind : '',
      graph_id: sourceOpt ? sourceOpt.dataset.graphId : '',
      node_id: sourceOpt ? sourceOpt.dataset.nodeId : '',
      endpoint_name: sourceOpt ? sourceOpt.dataset.endpointName : '',
      from_batch: target,
      actor: 'operator'
    };
    if (dryRun) body.dry_run = true;

    var btnId = dryRun ? 'replayDryRunBtn' : 'replayBtn';
    var btn = el(btnId);
    if (btn) { btn.disabled = true; btn.textContent = '...'; }

    try {
      var result = await opsApi('/replay/fromCheckpoint', { method: 'POST', body: body });
      if (dryRun) {
        alert('Dry run result:\n' + JSON.stringify(result, null, 2));
      } else {
        alert('Replay started successfully.');
        await loadCheckpoints();
      }
    } catch (e) {
      alert('Error: ' + e.message);
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.textContent = dryRun ? 'Dry Run (Preview)' : 'Start Replay';
      }
    }
  }

  // ─── Bad Record Actions ───────────────────────────────────────────

  async function viewBadRecord(recordId) {
    var detailPanel = el('badRecordDetail');
    var detailContent = el('badRecordDetailContent');
    if (!detailPanel) return;
    detailPanel.style.display = 'block';
    if (detailContent) detailContent.innerHTML = '<div style="padding:16px;color:var(--text-secondary)">Loading...</div>';

    try {
      var data = await opsApi('/badRecords/' + recordId + '/payload');
      var html = '<div class="ops-kv-grid" style="margin-bottom:16px">';
      html += kvItem('Record ID', data.record_id || recordId);
      html += kvItem('Source', data.source || data.source_key);
      html += kvItem('Batch', data.batch_id || data.batch);
      html += kvItem('Failure Class', data.failure_class);
      html += kvItem('File', data.file || data.file_name || '\u2014');
      html += kvItem('Byte Offset', data.byte_offset != null ? fmtNumber(data.byte_offset) : '\u2014');
      html += '</div>';

      html += '<div class="ops-kv-item" style="margin-bottom:12px">'
        + '<div class="kv-label">Error Message</div>'
        + '<div class="kv-value" style="color:var(--red)">' + esc(data.error_message || data.reason || data.message) + '</div>'
        + '</div>';

      if (data.raw_payload || data.payload) {
        html += '<div class="ops-kv-item">'
          + '<div class="kv-label">Raw Payload</div>'
          + '<div class="ops-json">' + esc(data.raw_payload || (typeof data.payload === 'string' ? data.payload : JSON.stringify(data.payload, null, 2))) + '</div>'
          + '</div>';
      }

      if (detailContent) detailContent.innerHTML = html;

      var titleEl = detailPanel.querySelector('.ops-detail-title');
      if (titleEl) titleEl.textContent = 'Bad Record ' + recordId;

      detailPanel.scrollIntoView({ behavior: 'smooth' });
    } catch (e) {
      if (detailContent) detailContent.innerHTML = '<div style="padding:16px;color:var(--red)">Error loading record: ' + esc(e.message) + '</div>';
    }
  }

  async function bulkIgnoreBadRecords(btn) {
    var ws = getWorkspaceKey();
    var tbody = el('badRecordBody');
    if (!tbody) return;
    var checked = tbody.querySelectorAll('input[type="checkbox"]:checked');
    if (!checked.length) { alert('No records selected.'); return; }

    var ids = [];
    for (var i = 0; i < checked.length; i++) {
      var row = checked[i].closest('tr');
      if (row && row.dataset.id) ids.push(row.dataset.id);
    }
    if (ids.length === 0) { alert('No valid records selected.'); return; }
    if (!confirm('Mark ' + ids.length + ' record(s) as ignored?')) return;

    if (btn) { btn.disabled = true; btn.textContent = '...'; }
    try {
      await opsApi('/badRecords/bulkIgnore', {
        method: 'POST',
        body: { workspace_key: ws, record_ids: ids, actor: 'operator' }
      });
      await loadBadRecords();
    } catch (e) {
      alert('Error: ' + e.message);
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = 'Mark Ignored'; }
    }
  }

  async function bulkReplayBadRecords(btn, singleRecordId) {
    var ws = getWorkspaceKey();
    var ids = [];
    if (singleRecordId != null) {
      ids = [singleRecordId];
    } else {
      var tbody = el('badRecordBody');
      if (!tbody) return;
      var checked = tbody.querySelectorAll('input[type="checkbox"]:checked');
      if (!checked.length) { alert('No records selected.'); return; }
      for (var i = 0; i < checked.length; i++) {
        var row = checked[i].closest('tr');
        if (row && row.dataset.id) ids.push(row.dataset.id);
      }
    }
    if (ids.length === 0) { alert('No valid records selected.'); return; }
    if (!confirm('Replay from source batch for ' + ids.length + ' record(s)?')) return;

    if (btn) { btn.disabled = true; btn.textContent = '...'; }
    try {
      var result = await opsApi('/badRecords/replay', {
        method: 'POST',
        body: { workspace_key: ws, record_ids: ids, actor: 'operator' }
      });
      var queued = result && result.unique_batches != null ? result.unique_batches : ids.length;
      alert('Queued replay for ' + queued + ' batch(es).');
      await loadCheckpoints();
    } catch (e) {
      alert('Error: ' + e.message);
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.textContent = singleRecordId != null ? 'Replay' : 'Replay Selected';
      }
    }
  }

  async function exportBadRecords(format) {
    var ws = getWorkspaceKey();
    try {
      var data = await opsApi('/badRecords/export', {
        method: 'POST',
        body: { workspace_key: ws, format: format || 'csv', limit: 1000 }
      });
      if (data && data.download_url) {
        window.open(data.download_url, '_blank');
      } else if (typeof data === 'string') {
        var blob = new Blob([data], { type: 'text/csv' });
        var url = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        a.download = 'bad_records_export.' + (format || 'csv');
        a.click();
        URL.revokeObjectURL(url);
      } else {
        alert('Export completed. Check server response for download location.');
      }
    } catch (e) {
      alert('Error exporting: ' + e.message);
    }
  }

  // ─── Config Save Actions ──────────────────────────────────────────

  async function saveWorkerSettings(btn) {
    var ws = getWorkspaceKey();
    var value = {
      max_workers: getNumericValue('cfg-max-workers'),
      poll_interval: getNumericValue('cfg-poll-interval'),
      lease_duration: getNumericValue('cfg-lease-duration'),
      heartbeat_interval: getNumericValue('cfg-heartbeat-interval'),
      max_retries: getNumericValue('cfg-max-retries'),
      orphan_timeout: getNumericValue('cfg-orphan-timeout')
    };
    await saveConfig('worker_settings', value, btn, 'Save Worker Settings');
  }

  async function saveAlertThresholds(btn) {
    var ws = getWorkspaceKey();
    var value = {
      kafka_lag_warning: getNumericValue('cfg-kafka-lag-warn'),
      kafka_lag_critical: getNumericValue('cfg-kafka-lag-critical'),
      freshness_warning: getNumericValue('cfg-freshness-warn'),
      freshness_critical: getNumericValue('cfg-freshness-critical'),
      bad_record_pct: getNumericValue('cfg-bad-record-pct'),
      heartbeat_timeout: getNumericValue('cfg-heartbeat-timeout')
    };
    await saveConfig('alert_thresholds', value, btn, 'Save Thresholds');
  }

  async function saveRetentionPolicies(btn) {
    var value = {
      manifest_retention: getNumericValue('cfg-manifest-retention'),
      bad_record_retention: getNumericValue('cfg-bad-record-retention'),
      checkpoint_history: getNumericValue('cfg-checkpoint-history'),
      dlq_retention: getNumericValue('cfg-dlq-retention'),
      archive_destination: getInputValue('cfg-archive-dest')
    };
    await saveConfig('retention_policies', value, btn, 'Save Policies');
  }

  async function saveConcurrencySettings(btn) {
    var inputs = document.querySelectorAll('.concurrency-input');
    var sources = [];
    for (var i = 0; i < inputs.length; i++) {
      sources.push({
        source_key: inputs[i].dataset.source,
        max_concurrent: parseInt(inputs[i].value, 10) || 1
      });
    }
    await saveConfig('source_concurrency', { sources: sources }, btn, 'Save Concurrency');
  }

  async function saveConfig(configKey, value, btn, originalLabel) {
    var ws = getWorkspaceKey();
    if (btn) { btn.disabled = true; btn.textContent = 'Saving...'; }
    try {
      await opsApi('/admin/config', {
        method: 'POST',
        body: { config_key: configKey, workspace_key: ws, value: value, actor: 'operator' }
      });
      if (btn) {
        btn.textContent = 'Saved!';
        btn.style.color = 'var(--green)';
        btn.style.borderColor = 'rgba(52,211,153,0.3)';
        setTimeout(function () {
          btn.textContent = originalLabel || 'Save';
          btn.style.color = '';
          btn.style.borderColor = '';
          btn.disabled = false;
        }, 2000);
      }
    } catch (e) {
      alert('Error saving config: ' + e.message);
      if (btn) { btn.disabled = false; btn.textContent = originalLabel || 'Save'; }
    }
  }

  // ─── Correlation / Inspect Actions ─────────────────────────────────

  async function inspectCorrelation(requestId) {
    try {
      var data = await opsApi('/correlation/request/' + requestId);
      var detail = JSON.stringify(data, null, 2);
      var w = window.open('', '_blank', 'width=700,height=500');
      if (w) {
        w.document.write('<html><head><title>Request ' + requestId + '</title><style>body{background:#1a1b2e;color:#e0e0e0;font:13px/1.6 "JetBrains Mono",monospace;padding:20px;}</style></head><body><pre>' + esc(detail) + '</pre></body></html>');
        w.document.close();
      } else {
        alert(detail);
      }
    } catch (e) {
      alert('Error fetching correlation data: ' + e.message);
    }
  }

  // ─── Nav Badge Update ─────────────────────────────────────────────

  function updateNavBadge(screenId, count) {
    var navItem = document.querySelector('.ops-nav-item[data-screen="' + screenId + '"]');
    if (!navItem) return;
    var badge = navItem.querySelector('.nav-badge');
    if (count != null && count > 0) {
      if (!badge) {
        badge = document.createElement('span');
        badge.className = 'nav-badge';
        navItem.appendChild(badge);
      }
      badge.textContent = count > 99 ? '99+' : count;
    } else {
      if (badge) badge.remove();
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // EVENT BINDING
  // ═══════════════════════════════════════════════════════════════════

  // Expose action handlers to inline onclick attributes
  window._opsAckAlert = ackAlert;
  window._opsSilenceAlert = silenceAlert;
  window._opsResolveAlert = resolveAlert;
  window._opsCancelRequest = function (id, btn) { singleQueueAction('Cancel', id, btn); };
  window._opsRetryRequest = function (id, btn) { singleQueueAction('Retry', id, btn); };
  window._opsRequeueRequest = function (id, btn) { singleQueueAction('Requeue', id, btn); };
  window._opsInspectCorrelation = inspectCorrelation;
  window._opsDrainWorker = drainWorker;
  window._opsUndrainWorker = undrainWorker;
  window._opsForceReleaseWorker = forceReleaseWorker;
  window._opsResetCircuitBreaker = resetCircuitBreaker;
  window._opsInspectManifest = inspectManifest;
  window._opsReplayBatch = replayBatch;
  window._opsRollbackBatch = function (batchId, btn) {
    alert('Batch rollback is not available from the generic ops console.');
  };
  window._opsArchiveBatch = function (batchId, btn) {
    alert('Batch archive is not available from the generic ops console.');
  };
  window._opsViewDriftDetail = viewDriftDetail;
  window._opsAckDrift = ackDrift;
  window._opsViewDriftTimeline = viewDriftTimeline;
  window._opsPreviewDriftDdl = previewDriftDdl;
  window._opsApplyDriftDdl = applyDriftDdl;
  window._opsCloseTimeline = closeDriftTimeline;
  window._opsDismissSchemaNotifications = dismissSchemaNotifications;
  window._opsResetCheckpoint = resetCheckpoint;
  window._opsViewCheckpointHistory = viewCheckpointHistory;
  window._opsViewBadRecord = viewBadRecord;
  window._opsReplayBadRecord = function (recordId, btn) {
    bulkReplayBadRecords(btn, recordId);
  };
  window._opsIgnoreBadRecord = function (recordId, btn) {
    var ws = getWorkspaceKey();
    if (!confirm('Mark record ' + recordId + ' as ignored?')) return;
    btn.disabled = true;
    btn.textContent = '...';
    opsApi('/badRecords/bulkIgnore', { method: 'POST', body: { workspace_key: ws, record_ids: [recordId], actor: 'operator' } })
      .then(function () { return loadBadRecords(); })
      .catch(function (e) { alert('Error: ' + e.message); })
      .finally(function () { btn.disabled = false; btn.textContent = 'Ignore'; });
  };

  // Expose refreshAll globally for the toolbar button
  window.refreshAll = refreshAll;

  // ─── DOMContentLoaded Bootstrap ───────────────────────────────────

  document.addEventListener('DOMContentLoaded', function () {

    // ── Nav clicks ──
    var navItems = document.querySelectorAll('.ops-nav-item[data-screen]');
    for (var i = 0; i < navItems.length; i++) {
      navItems[i].addEventListener('click', (function (item) {
        return function () { switchScreen(item.dataset.screen); };
      })(navItems[i]));
    }

    // ── Tooltip: keyboard shortcut hints ──
    for (var j = 0; j < navItems.length; j++) {
      navItems[j].title = 'Alt+' + (j + 1);
    }

    // ── Select-all checkbox for queue ──
    var selectAllQueue = el('selectAllQueue');
    if (selectAllQueue) {
      selectAllQueue.addEventListener('change', function () {
        var tbody = el('queueBody');
        if (!tbody) return;
        var cbs = tbody.querySelectorAll('input[type="checkbox"]');
        for (var k = 0; k < cbs.length; k++) cbs[k].checked = selectAllQueue.checked;
      });
    }

    // ── Select-all checkbox for bad records ──
    var selectAllBadRecords = el('selectAllBadRecords');
    if (selectAllBadRecords) {
      selectAllBadRecords.addEventListener('change', function () {
        var tbody = el('badRecordBody');
        if (!tbody) return;
        var cbs = tbody.querySelectorAll('input[type="checkbox"]');
        for (var k = 0; k < cbs.length; k++) cbs[k].checked = selectAllBadRecords.checked;
      });
    }

    // ── Queue status filter ──
    var queueStatusFilter = el('queueStatusFilter');
    if (queueStatusFilter) {
      queueStatusFilter.addEventListener('change', function () {
        var filter = this.value.toLowerCase();
        var tbody = el('queueBody');
        if (!tbody) return;
        var rows = tbody.querySelectorAll('tr');
        for (var k = 0; k < rows.length; k++) {
          if (!filter) { rows[k].style.display = ''; continue; }
          var rowStatus = rows[k].dataset.status || '';
          rows[k].style.display = rowStatus.indexOf(filter) !== -1 ? '' : 'none';
        }
      });
    }

    // ── Drift severity filter ──
    var driftSeverityFilter = el('driftSeverityFilter');
    if (driftSeverityFilter) {
      driftSeverityFilter.addEventListener('change', function () {
        if (activeScreen === 'schema-drift') loadSchemaDrift().catch(function () {});
      });
    }

    // ── Search inputs (generic table filter) ──
    var searches = document.querySelectorAll('.ops-search');
    for (var si = 0; si < searches.length; si++) {
      searches[si].addEventListener('input', function () {
        var query = this.value.toLowerCase();
        var panel = this.closest('.ops-panel');
        if (!panel) return;
        var rows = panel.querySelectorAll('tbody tr');
        for (var r = 0; r < rows.length; r++) {
          var text = rows[r].textContent.toLowerCase();
          rows[r].style.display = text.indexOf(query) !== -1 ? '' : 'none';
        }
      });
    }

    // ── Bulk action buttons for queue ──
    bindBulkButton('bulkRetryQueue', function (btn) { bulkQueueAction('Retry', btn); });
    bindBulkButton('bulkCancelQueue', function (btn) { bulkQueueAction('Cancel', btn); });
    bindBulkButton('bulkRequeueQueue', function (btn) { bulkQueueAction('Requeue', btn); });

    // Also bind by text content fallback for static HTML bulk buttons
    var queueScreen = document.getElementById('screen-queue-workers');
    if (queueScreen) {
      var bulkBtns = queueScreen.querySelectorAll('.ops-btn');
      for (var bi = 0; bi < bulkBtns.length; bi++) {
        var btnText = bulkBtns[bi].textContent.trim();
        if (btnText === 'Retry Selected') {
          bulkBtns[bi].onclick = function () { bulkQueueAction('Retry', this); };
        } else if (btnText === 'Cancel Selected') {
          bulkBtns[bi].onclick = function () { bulkQueueAction('Cancel', this); };
        } else if (btnText === 'Requeue Selected') {
          bulkBtns[bi].onclick = function () { bulkQueueAction('Requeue', this); };
        } else if (btnText.indexOf('Force Orphan Recovery') !== -1) {
          bulkBtns[bi].onclick = function () {
            alert('Force orphan recovery requires targeted request selection and is not exposed as a blanket action.');
          };
        }
      }
    }

    // ── Bulk action buttons for bad records ──
    bindButtonById('bulkReplayBadRecordsBtn', function () {
      bulkReplayBadRecords(el('bulkReplayBadRecordsBtn'));
    });
    bindButtonById('bulkIgnoreBadRecordsBtn', function (btn) { bulkIgnoreBadRecords(btn); });
    bindButtonById('exportBadRecordsBtn', function () { exportBadRecords('csv'); });

    var brScreen = document.getElementById('screen-bad-records');
    if (brScreen) {
      var brBtns = brScreen.querySelectorAll('.ops-btn');
      for (var bri = 0; bri < brBtns.length; bri++) {
        var brText = brBtns[bri].textContent.trim();
        if (brText === 'Replay Selected') {
          brBtns[bri].onclick = function () {
            bulkReplayBadRecords(this);
          };
        } else if (brText === 'Mark Ignored') {
          brBtns[bri].onclick = function () { bulkIgnoreBadRecords(this); };
        } else if (brText === 'Export Selected' || brText === 'Export CSV') {
          brBtns[bri].onclick = function () { exportBadRecords('csv'); };
        } else if (brText === 'Replay All Eligible') {
          brBtns[bri].onclick = function () {
            bulkReplayBadRecords(this);
          };
        }
      }
    }

    // ── Save config buttons ──
    bindButtonById('saveWorkerSettingsBtn', function (btn) { saveWorkerSettings(btn); });
    bindButtonById('saveConcurrencyBtn', function (btn) { saveConcurrencySettings(btn); });
    bindButtonById('saveThresholdsBtn', function (btn) { saveAlertThresholds(btn); });
    bindButtonById('savePoliciesBtn', function (btn) { saveRetentionPolicies(btn); });

    // Also bind by text content fallback for static HTML save buttons
    var adminScreen = document.getElementById('screen-admin-policies');
    if (adminScreen) {
      var adminBtns = adminScreen.querySelectorAll('.ops-btn.primary, .ops-btn');
      for (var ai = 0; ai < adminBtns.length; ai++) {
        var aText = adminBtns[ai].textContent.trim();
        if (aText === 'Save Worker Settings') {
          adminBtns[ai].onclick = function () { saveWorkerSettings(this); };
        } else if (aText === 'Save Concurrency') {
          adminBtns[ai].onclick = function () { saveConcurrencySettings(this); };
        } else if (aText === 'Save Thresholds') {
          adminBtns[ai].onclick = function () { saveAlertThresholds(this); };
        } else if (aText === 'Save Policies') {
          adminBtns[ai].onclick = function () { saveRetentionPolicies(this); };
        }
      }
    }

    // ── Replay form buttons ──
    bindButtonById('replayBtn', function () { replayFromCheckpoint(false); });
    bindButtonById('replayDryRunBtn', function () { replayFromCheckpoint(true); });

    // Fallback: bind by text content in checkpoints screen
    var ckScreen = document.getElementById('screen-checkpoints-replay');
    if (ckScreen) {
      var ckBtns = ckScreen.querySelectorAll('.ops-btn');
      for (var ci = 0; ci < ckBtns.length; ci++) {
        var cText = ckBtns[ci].textContent.trim();
        if (cText === 'Start Replay') {
          ckBtns[ci].onclick = function () { replayFromCheckpoint(false); };
        } else if (cText === 'Dry Run (Preview)') {
          ckBtns[ci].onclick = function () { replayFromCheckpoint(true); };
        }
      }
    }

    // ── Manifest detail close button ──
    var manifestDetail = el('manifestDetail');
    if (manifestDetail) {
      var closeBtn = manifestDetail.querySelector('.ops-btn');
      if (closeBtn && closeBtn.textContent.trim() === 'Close') {
        closeBtn.onclick = function () { manifestDetail.style.display = 'none'; };
      }
    }

    // ── Bad record detail close button ──
    var brDetail = el('badRecordDetail');
    if (brDetail) {
      var brClose = brDetail.querySelector('.ops-btn');
      if (brClose && brClose.textContent.trim() === 'Close') {
        brClose.onclick = function () { brDetail.style.display = 'none'; };
      }
    }

    // ── Pipeline / time range selects ──
    var pipelineSelect = el('pipelineSelect');
    if (pipelineSelect) {
      pipelineSelect.addEventListener('change', function () { refreshAll(); });
    }

    var timeRange = el('timeRange');
    if (timeRange) {
      timeRange.addEventListener('change', function () { refreshAll(); });
    }

    // ── Keyboard shortcuts ──
    document.addEventListener('keydown', function (e) {
      if (e.altKey && e.key >= '1' && e.key <= '8') {
        e.preventDefault();
        var screenIds = Object.keys(screenMeta);
        var idx = parseInt(e.key, 10) - 1;
        if (idx < screenIds.length) switchScreen(screenIds[idx]);
      }
      if (e.altKey && (e.key === 'r' || e.key === 'R')) {
        e.preventDefault();
        refreshAll();
      }
    });

    // ── Hide mock banner if present ──
    var mockBanner = el('mockBanner');
    if (mockBanner) mockBanner.style.display = 'none';

    // ── Remove static mock rows from tbodies that will be dynamically populated ──
    clearStaticRows('sourceStatusBody');
    clearStaticRows('recentActivityBody');
    clearStaticRows('queueBody');
    clearStaticRows('workerBody');
    clearStaticRows('kafkaSourceBody');
    clearStaticRows('fileSourceBody');
    clearStaticRows('apiSourceBody');
    clearStaticRows('dataLossBody');
    clearStaticRows('manifestBody');
    clearStaticRows('checkpointBody');
    clearStaticRows('checkpointHistoryBody');
    clearStaticRows('activeReplayBody');
    clearStaticRows('badRecordBody');
    clearStaticRows('schemaProposalBody');
    clearStaticRows('medallionReleaseBody');
    clearStaticRows('sourceConcurrencyBody');

    // ── Initial load ──
    setText('lastRefresh', new Date().toLocaleTimeString());
    switchScreen('pipeline-overview');

    console.log('Bitool Operations Console loaded (live mode). Use Alt+1..8 to switch screens, Alt+R to refresh.');
  });

  // ─── Binding Helpers ──────────────────────────────────────────────

  function bindBulkButton(id, handler) {
    var btn = el(id);
    if (btn) btn.addEventListener('click', function () { handler(btn); });
  }

  function bindButtonById(id, handler) {
    var btn = el(id);
    if (btn) btn.addEventListener('click', function () { handler(btn); });
  }

  function clearStaticRows(tbodyId) {
    var tbody = el(tbodyId);
    if (tbody) tbody.innerHTML = '<tr><td colspan="12" style="text-align:center;color:var(--text-secondary);padding:24px">Loading...</td></tr>';
  }

})();
