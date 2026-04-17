/* ═══════════════════════════════════════════════════════════════════
   Bitool AI Reporting Dashboard
   ═══════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  /* ── Palette ───────────────────────────────────────────────────── */
  var C = {
    indigo: '#6366f1', indigoA: 'rgba(99,102,241,0.12)',
    blue:   '#3b82f6', blueA:   'rgba(59,130,246,0.12)',
    green:  '#10b981', greenA:  'rgba(16,185,129,0.12)',
    red:    '#ef4444', redA:    'rgba(239,68,68,0.12)',
    amber:  '#f59e0b', amberA:  'rgba(245,158,11,0.12)',
    purple: '#7c5cfc', purpleA: 'rgba(124,92,252,0.12)',
    cyan:   '#0ea5c7', cyanA:   'rgba(14,165,199,0.12)',
    pink:   '#ec4899', pinkA:   'rgba(236,72,153,0.12)',
    gray:   '#6b7280', grayA:   'rgba(107,114,128,0.08)'
  };

  /* ── State ─────────────────────────────────────────────────────── */
  var data = null;
  var activeView = 'executive';
  var charts = [];

  /* ── Helpers ────────────────────────────────────────────────────── */
  function $(s) { return document.querySelector(s); }
  function $$(s) { return document.querySelectorAll(s); }
  function el(id) { return document.getElementById(id); }

  var MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  function shortDate(s) {
    if (!s) return '';
    var p = s.split('-');
    return MONTHS[parseInt(p[1], 10) - 1] + ' ' + parseInt(p[2], 10);
  }
  function fmt(n, d) {
    if (n == null || isNaN(n)) return '—';
    d = d == null ? 1 : d;
    return Number(n).toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
  }
  function fmtInt(n) { return n == null ? '—' : Math.round(n).toLocaleString('en-US'); }
  function fmtK(n) {
    if (n == null) return '—';
    if (Math.abs(n) >= 1e6) return fmt(n / 1e6, 1) + 'M';
    if (Math.abs(n) >= 1e3) return fmt(n / 1e3, 1) + 'K';
    return fmtInt(n);
  }
  function fmtSmart(n, colName) {
    if (n == null || isNaN(n)) return '—';
    if (/[Mm]s$/.test(colName)) return fmt(n / 3600000, 1) + ' hrs';
    if (/[Mm]eters$/.test(colName)) return fmt(n / 1609.34, 1) + ' mi';
    if (Math.abs(n) >= 1e6) return fmt(n / 1e6, 1) + 'M';
    if (Math.abs(n) >= 1e4) return fmt(n / 1e3, 1) + 'K';
    return fmt(n, 2);
  }
  function pct(part, whole) { return whole ? fmt(100 * part / whole, 1) + '%' : '—'; }
  function sum(arr, key) { return arr.reduce(function (s, r) { return s + (Number(r[key]) || 0); }, 0); }
  function avg(arr, key) { var v = arr.filter(function(r){ return r[key] != null; }); return v.length ? sum(v, key) / v.length : 0; }

  function trendPct(arr, key) {
    if (!arr || arr.length < 14) return null;
    var mid = Math.floor(arr.length / 2);
    var first = arr.slice(0, mid);
    var second = arr.slice(mid);
    var a = avg(first, key);
    var b = avg(second, key);
    if (!a) return null;
    return ((b - a) / Math.abs(a)) * 100;
  }
  function trendHTML(val, invert) {
    if (val == null) return '';
    var up = val >= 0;
    var cls = (invert ? !up : up) ? 'up' : 'down';
    var arrow = up ? '&#9650;' : '&#9660;';
    return '<span class="rpt-kpi-trend ' + cls + '">' + arrow + ' ' + fmt(Math.abs(val), 1) + '%</span>';
  }

  /* ── Chart Helpers ──────────────────────────────────────────────── */
  function destroyCharts() { charts.forEach(function (c) { c.destroy(); }); charts = []; }

  var baseLineOpts = {
    responsive: true, maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { position: 'top', labels: { usePointStyle: true, padding: 14, font: { family: "'DM Sans'", size: 12 } } },
      tooltip: { backgroundColor: '#1a1d26', titleFont: { family: "'DM Sans'" }, bodyFont: { family: "'DM Sans'" }, cornerRadius: 8, padding: 10 }
    },
    scales: {
      x: { grid: { display: false }, ticks: { maxTicksLimit: 12, font: { family: "'DM Sans'", size: 11 }, color: '#8b91a3' } },
      y: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.04)' }, ticks: { font: { family: "'DM Sans'", size: 11 }, color: '#8b91a3' } }
    },
    elements: { line: { tension: 0.4, borderWidth: 2.5 }, point: { radius: 0, hitRadius: 20, hoverRadius: 4 } }
  };

  function lineChart(canvasId, labels, datasets, extraOpts) {
    var ctx = el(canvasId);
    if (!ctx) return null;
    var opts = JSON.parse(JSON.stringify(baseLineOpts));
    if (extraOpts) {
      if (extraOpts.yLabel) opts.scales.y.title = { display: true, text: extraOpts.yLabel, font: { family: "'DM Sans'", size: 12 }, color: '#8b91a3' };
      if (extraOpts.stacked) { opts.scales.y.stacked = true; opts.scales.x.stacked = true; }
      if (extraOpts.noLegend) opts.plugins.legend.display = false;
    }
    var ch = new Chart(ctx, { type: 'line', data: { labels: labels, datasets: datasets }, options: opts });
    charts.push(ch);
    return ch;
  }

  function barChart(canvasId, labels, datasets, extraOpts) {
    var ctx = el(canvasId);
    if (!ctx) return null;
    var opts = JSON.parse(JSON.stringify(baseLineOpts));
    opts.elements = {};
    if (extraOpts) {
      if (extraOpts.horizontal) { opts.indexAxis = 'y'; }
      if (extraOpts.stacked) { opts.scales.y.stacked = true; opts.scales.x.stacked = true; }
      if (extraOpts.noLegend) opts.plugins.legend.display = false;
      if (extraOpts.yLabel) opts.scales.y.title = { display: true, text: extraOpts.yLabel, font: { family: "'DM Sans'", size: 12 }, color: '#8b91a3' };
    }
    var ch = new Chart(ctx, { type: 'bar', data: { labels: labels, datasets: datasets }, options: opts });
    charts.push(ch);
    return ch;
  }

  function doughnutChart(canvasId, labels, values, colors) {
    var ctx = el(canvasId);
    if (!ctx) return null;
    var ch = new Chart(ctx, {
      type: 'doughnut',
      data: { labels: labels, datasets: [{ data: values, backgroundColor: colors, borderWidth: 0 }] },
      options: {
        responsive: true, maintainAspectRatio: false, cutout: '68%',
        plugins: {
          legend: { position: 'bottom', labels: { usePointStyle: true, padding: 14, font: { family: "'DM Sans'", size: 12 } } },
          tooltip: { backgroundColor: '#1a1d26', cornerRadius: 8, padding: 10, titleFont: { family: "'DM Sans'" }, bodyFont: { family: "'DM Sans'" } }
        }
      }
    });
    charts.push(ch);
    return ch;
  }

  /* ── DOM Builders ───────────────────────────────────────────────── */
  function kpiCard(icon, iconBg, value, label, trend) {
    return '<div class="rpt-kpi-card">' +
      '<div class="rpt-kpi-top">' +
        '<div class="rpt-kpi-icon" style="background:' + iconBg + '">' + icon + '</div>' +
        (trend || '') +
      '</div>' +
      '<div class="rpt-kpi-value">' + value + '</div>' +
      '<div class="rpt-kpi-label">' + label + '</div>' +
    '</div>';
  }

  function chartCard(id, title, subtitle, full) {
    return '<div class="rpt-chart-card' + (full ? ' rpt-chart-full' : '') + '">' +
      '<div class="rpt-chart-header"><div><div class="rpt-chart-title">' + title + '</div>' +
      (subtitle ? '<div class="rpt-chart-subtitle">' + subtitle + '</div>' : '') +
      '</div></div>' +
      '<div class="rpt-chart-body"><canvas id="' + id + '"></canvas></div>' +
    '</div>';
  }

  function insightBar(icon, html) {
    return '<div class="rpt-insight-bar"><span class="rpt-insight-icon">' + icon + '</span><div class="rpt-insight-text">' + html + '</div></div>';
  }

  function sectionTitle(text, color) {
    return '<div class="rpt-section-title"><span class="dot" style="background:' + color + '"></span>' + text + '</div>';
  }

  function tableWrap(title, headHTML, bodyHTML) {
    return '<div class="rpt-table-wrap"><div class="rpt-table-title">' + title + '</div>' +
      '<table class="rpt-table"><thead><tr>' + headHTML + '</tr></thead><tbody>' + bodyHTML + '</tbody></table></div>';
  }

  /* ── View: Executive Summary ────────────────────────────────────── */
  function renderExecutive() {
    var ft = data.fuel_trend || [];
    var ut = data.utilization_trend || [];
    var st = data.safety_trend || [];
    var ct = data.cold_chain_trend || [];

    var at = data.asset_health_trend || [];
    var et = data.emissions_trend || [];
    var dt = data.door_trend || [];
    var ht = data.hos_violations || [];
    var hd = data.hos_by_driver || [];

    var fleetSize = (data.util_by_vehicle || data.fuel_by_vehicle || []).length;
    var totalFuel = sum(ft, 'total_fuel');
    var totalDist = sum(ft, 'distance');
    var avgMpg = avg(ft, 'avg_mpg');
    var totalIdle = sum(ft, 'idle_hours');
    var underutilized = (data.util_by_vehicle || []).filter(function (v) { return v.total_records < 10; }).length;
    var jobCount = data.job_count || 0;
    var delayedJobs = 0; // placeholder — no delay column in gold data
    var totalEvents = sum(st, 'total_events');
    var majorEvents = sum(st, 'major_events');
    var coachable = sum(st, 'coachable');
    var safetyAvg = avg(data.safety_scores || [], 'safety_score');
    var hosViolations = ht.reduce(function (s, r) { return s + (r.violation_count || 0); }, 0);
    var totalBreaches = sum(ct, 'high_breaches') + sum(ct, 'low_breaches');
    var doorEvents = sum(dt, 'door_events');
    var openIssues = sum(at, 'open_issues');
    var completedIssues = sum(at, 'completed');
    var alertCount = data.alert_count || 0;
    var unresolvedAlerts = alertCount; // approximation

    var html = '';

    /* Insight */
    var worstVehicle = (data.fuel_by_vehicle || [])[0];
    if (worstVehicle) {
      html += insightBar('&#128161;',
        '<strong>AI Insight:</strong> Vehicle <strong>' + worstVehicle.vehicle_id +
        '</strong> consumed the most fuel (' + fmtK(worstVehicle.total_fuel) +
        ' gal) with ' + fmt(worstVehicle.idle_hours, 0) +
        ' idle hours. Recommend route optimization to improve efficiency by an estimated 12%.');
    }

    /* KPIs — Full 18-metric shortlist */
    html += '<div class="rpt-kpi-grid">';
    html += kpiCard('&#9981;',  C.amberA,  fmtK(totalFuel),                'Total Fuel Gallons',     trendHTML(trendPct(ft, 'total_fuel'), true));
    html += kpiCard('&#128739;', C.blueA,   fmt(avgMpg),                    'Average MPG',            trendHTML(trendPct(ft, 'avg_mpg')));
    html += kpiCard('&#9200;',  C.purpleA, fmtK(totalIdle),                'Idle Hours',             trendHTML(trendPct(ft, 'idle_hours'), true));
    html += kpiCard('&#128666;', C.indigoA, fmtInt(fleetSize),              'Active Vehicles',        '');
    html += kpiCard('&#128683;', underutilized > 2 ? C.redA : C.grayA, fmtInt(underutilized), 'Underutilized Vehicles', '');
    html += kpiCard('&#128203;', C.greenA,  fmtK(jobCount),                 'Jobs Completed',         '');
    html += kpiCard('&#9888;',  C.redA,    fmtK(totalEvents),              'Total Safety Events',    trendHTML(trendPct(st, 'total_events'), true));
    html += kpiCard('&#128680;', C.redA,    fmtK(majorEvents),              'Major/Severe Events',    '');
    html += kpiCard('&#128218;', C.amberA,  fmtK(coachable),               'Coachable Events',       '');
    html += kpiCard('&#127942;', safetyAvg >= 70 ? C.greenA : C.redA, fmt(safetyAvg, 0), 'Safety Score', '');
    html += kpiCard('&#9201;',  hosViolations > 50 ? C.redA : C.amberA, fmtInt(hosViolations), 'HOS Violations', '');
    html += kpiCard('&#10052;', totalBreaches > 50 ? C.redA : C.cyanA, fmtInt(totalBreaches), 'Temp Breach Count',     '');
    html += kpiCard('&#128682;', C.cyanA,   fmtK(doorEvents),              'Door-Open Events',       '');
    html += kpiCard('&#128295;', openIssues > 100 ? C.redA : C.amberA, fmtInt(openIssues), 'Open Maint. Issues', '');
    html += kpiCard('&#9989;',  C.greenA,  fmtInt(completedIssues),        'Completed Maint.',       '');
    html += kpiCard('&#128276;', alertCount > 100 ? C.redA : C.amberA, fmtK(alertCount), 'Alert Volume', '');
    html += kpiCard('&#128176;', C.greenA,  '$' + fmtK(totalFuel * 3.45),  'Est. Fuel Cost',         trendHTML(trendPct(ft, 'total_fuel'), true));
    html += kpiCard('&#128200;', C.blueA,   fmtK(totalDist),               'Total Miles',            trendHTML(trendPct(ft, 'distance')));
    html += '</div>';

    /* Charts */
    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-exec-fuel', 'Fleet Fuel Consumption & Efficiency', 'Daily trend across all vehicles', true);
    html += chartCard('ch-exec-safety', 'Safety Events', 'Major vs coachable incidents');
    html += chartCard('ch-exec-dist', 'Event Distribution', 'Across categories');
    html += '</div>';

    el('rpt-content').innerHTML = html;

    /* Fuel + MPG dual axis */
    var labels = ft.map(function (r) { return shortDate(r.event_date); });
    lineChart('ch-exec-fuel', labels, [
      { label: 'Fuel (gal)', data: ft.map(function (r) { return r.total_fuel; }), borderColor: C.amber, backgroundColor: C.amberA, fill: true },
      { label: 'Distance (mi)', data: ft.map(function (r) { return r.distance; }), borderColor: C.blue, backgroundColor: C.blueA, fill: true },
      { label: 'Avg MPG', data: ft.map(function (r) { return r.avg_mpg; }), borderColor: C.green, backgroundColor: 'transparent', borderDash: [5, 3] }
    ]);

    /* Safety stacked */
    var sLabels = st.map(function (r) { return shortDate(r.event_date); });
    barChart('ch-exec-safety', sLabels, [
      { label: 'Major/Severe', data: st.map(function (r) { return r.major_events; }), backgroundColor: C.red, borderRadius: 3 },
      { label: 'Coachable', data: st.map(function (r) { return r.coachable; }), backgroundColor: C.amber, borderRadius: 3 }
    ], { stacked: true });

    /* Doughnut */
    doughnutChart('ch-exec-dist', ['Fuel Events', 'Safety Events', 'Cold Chain', 'Asset DVIRs', 'HOS Violations'],
      [ft.length, totalEvents, ct.length, sum(data.asset_health_trend || [], 'dvirs'), (data.hos_violations || []).length],
      [C.amber, C.red, C.cyan, C.purple, C.pink]);
  }

  /* ── View: Fuel & Idling ────────────────────────────────────────── */
  function renderFuel() {
    var ft = data.fuel_trend || [];
    var fv = data.fuel_by_vehicle || [];
    var totalFuel = sum(ft, 'total_fuel');
    var totalDist = sum(ft, 'distance');
    var totalIdle = sum(ft, 'idle_hours');
    var avgMpg = avg(ft, 'avg_mpg');

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#9981;', C.amberA, fmt(avgMpg), 'Avg Fleet MPG', trendHTML(trendPct(ft, 'avg_mpg')));
    html += kpiCard('&#128167;', C.blueA, fmtK(totalFuel), 'Total Fuel (gal)', trendHTML(trendPct(ft, 'total_fuel'), true));
    html += kpiCard('&#128336;', C.redA, fmtK(totalIdle), 'Total Idle Hours', trendHTML(trendPct(ft, 'idle_hours'), true));
    html += kpiCard('&#128176;', C.greenA, '$' + fmtK(totalFuel * 3.45), 'Est. Fuel Cost', '');
    html += kpiCard('&#128739;', C.purpleA, fmtK(totalDist), 'Total Distance (mi)', trendHTML(trendPct(ft, 'distance')));
    html += kpiCard('&#128200;', C.cyanA, fmt(totalDist / (totalFuel || 1)), 'Overall MPG', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-fuel-trend', 'Daily Fuel Efficiency', 'Avg MPG across fleet', true);
    html += chartCard('ch-fuel-idle', 'Idle Hours Trend', 'Daily total fleet idle hours');
    html += chartCard('ch-fuel-top', 'Top Vehicles by Fuel Usage', 'Highest fuel consumers');
    html += '</div>';

    /* Table */
    var th = '<th>Vehicle</th><th class="num">Total Fuel</th><th class="num">Avg MPG</th><th class="num">Idle Hrs</th><th class="bar-cell">Usage</th>';
    var maxFuel = fv.length ? fv[0].total_fuel : 1;
    var tb = fv.map(function (r) {
      var pctW = Math.round(100 * r.total_fuel / maxFuel);
      return '<tr><td>' + r.vehicle_id + '</td><td class="num">' + fmtInt(r.total_fuel) + '</td><td class="num">' +
        fmt(r.avg_mpg) + '</td><td class="num">' + fmtInt(r.idle_hours) + '</td>' +
        '<td class="bar-cell"><div class="mini-bar"><div class="mini-bar-fill" style="width:' + pctW + '%;background:' + C.amber + '"></div></div></td></tr>';
    }).join('');
    html += tableWrap('Vehicle Fuel Detail', th, tb);

    el('rpt-content').innerHTML = html;

    var labels = ft.map(function (r) { return shortDate(r.event_date); });
    lineChart('ch-fuel-trend', labels, [
      { label: 'Avg MPG', data: ft.map(function (r) { return r.avg_mpg; }), borderColor: C.green, backgroundColor: C.greenA, fill: true }
    ], { yLabel: 'MPG' });

    lineChart('ch-fuel-idle', labels, [
      { label: 'Idle Hours', data: ft.map(function (r) { return r.idle_hours; }), borderColor: C.red, backgroundColor: C.redA, fill: true }
    ], { yLabel: 'Hours', noLegend: true });

    barChart('ch-fuel-top', fv.map(function (r) { return r.vehicle_id; }),
      [{ label: 'Fuel (gal)', data: fv.map(function (r) { return r.total_fuel; }), backgroundColor: C.amber, borderRadius: 4 }],
      { horizontal: true, noLegend: true });
  }

  /* ── View: Fleet Utilization ────────────────────────────────────── */
  function renderUtilization() {
    var ut = data.utilization_trend || [];
    var avgSpeed = avg(ut, 'avg_speed');
    var avgFuelPct = avg(ut, 'avg_fuel_pct');
    var totalRecs = sum(ut, 'total_records');
    var ft = data.fuel_trend || [];
    var totalDist = sum(ft, 'distance');

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128668;', C.blueA, fmt(avgSpeed), 'Avg Speed (mph)', trendHTML(trendPct(ut, 'avg_speed')));
    html += kpiCard('&#128200;', C.greenA, fmt(avgFuelPct, 0) + '%', 'Avg Fuel Level', trendHTML(trendPct(ut, 'avg_fuel_pct')));
    html += kpiCard('&#128225;', C.purpleA, fmtK(totalRecs), 'Telemetry Records', '');
    html += kpiCard('&#128739;', C.amberA, fmtK(totalDist), 'Total Miles Driven', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-util-speed', 'Average Fleet Speed', 'Daily average mph', true);
    html += chartCard('ch-util-records', 'Daily Telemetry Volume', 'Records received per day');
    html += chartCard('ch-util-fuel', 'Fleet Fuel Level', 'Average fuel percentage');
    html += '</div>';

    el('rpt-content').innerHTML = html;

    var labels = ut.map(function (r) { return shortDate(r.event_date); });
    lineChart('ch-util-speed', labels, [
      { label: 'Avg Speed (mph)', data: ut.map(function (r) { return r.avg_speed; }), borderColor: C.blue, backgroundColor: C.blueA, fill: true }
    ], { yLabel: 'mph' });

    lineChart('ch-util-records', labels, [
      { label: 'Records', data: ut.map(function (r) { return r.total_records; }), borderColor: C.purple, backgroundColor: C.purpleA, fill: true }
    ], { noLegend: true });

    lineChart('ch-util-fuel', labels, [
      { label: 'Fuel %', data: ut.map(function (r) { return r.avg_fuel_pct; }), borderColor: C.green, backgroundColor: C.greenA, fill: true }
    ], { noLegend: true, yLabel: '%' });
  }

  /* ── View: Driver Safety ────────────────────────────────────────── */
  function renderSafety() {
    var st = data.safety_trend || [];
    var sd = data.safety_by_driver || [];
    var ss = data.safety_scores || [];
    var totalEvents = sum(st, 'total_events');
    var majorEvents = sum(st, 'major_events');
    var coachable = sum(st, 'coachable');
    var safetyAvg = avg(ss, 'safety_score');

    var html = '';
    if (safetyAvg < 75) {
      html += insightBar('&#9888;',
        '<strong>Safety Alert:</strong> Fleet average safety score is <strong>' + fmt(safetyAvg, 0) +
        '</strong> — below the 75-point target. <strong>' + fmtInt(majorEvents) +
        '</strong> major/severe events detected. Consider targeted coaching for bottom-quartile drivers.');
    }

    html += '<div class="rpt-kpi-grid">';
    html += kpiCard('&#9888;', C.redA, fmtK(totalEvents), 'Total Safety Events', trendHTML(trendPct(st, 'total_events'), true));
    html += kpiCard('&#128680;', C.redA, fmtK(majorEvents), 'Major / Severe', '');
    html += kpiCard('&#128218;', C.amberA, fmtK(coachable), 'Coachable Events', '');
    html += kpiCard('&#127942;', safetyAvg >= 75 ? C.greenA : C.redA, fmt(safetyAvg, 0), 'Avg Safety Score', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-safety-trend', 'Daily Safety Events', 'Major vs Coachable', true);
    html += chartCard('ch-safety-drivers', 'Driver Event Rankings', 'Top 15 by total events');
    html += chartCard('ch-safety-dist', 'Event Breakdown', 'Major vs Coachable ratio');
    html += '</div>';

    /* Driver table */
    var th = '<th>Driver</th><th class="num">Total</th><th class="num">Major</th><th class="num">Coachable</th><th class="bar-cell">Events</th>';
    var maxEvt = sd.length ? sd[0].total_events : 1;
    var tb = sd.map(function (r) {
      var w = Math.round(100 * r.total_events / maxEvt);
      return '<tr><td>' + r.driver_id + '</td><td class="num">' + fmtInt(r.total_events) + '</td><td class="num">' +
        fmtInt(r.major_events) + '</td><td class="num">' + fmtInt(r.coachable) + '</td>' +
        '<td class="bar-cell"><div class="mini-bar"><div class="mini-bar-fill" style="width:' + w + '%;background:' + C.red + '"></div></div></td></tr>';
    }).join('');
    html += tableWrap('Driver Safety Detail', th, tb);

    el('rpt-content').innerHTML = html;

    var labels = st.map(function (r) { return shortDate(r.event_date); });
    barChart('ch-safety-trend', labels, [
      { label: 'Major/Severe', data: st.map(function (r) { return r.major_events; }), backgroundColor: C.red, borderRadius: 2 },
      { label: 'Coachable', data: st.map(function (r) { return r.coachable; }), backgroundColor: C.amber, borderRadius: 2 }
    ], { stacked: true });

    barChart('ch-safety-drivers', sd.map(function (r) { return r.driver_id; }),
      [{ label: 'Total Events', data: sd.map(function (r) { return r.total_events; }), backgroundColor: C.red, borderRadius: 4 }],
      { horizontal: true, noLegend: true });

    doughnutChart('ch-safety-dist', ['Major/Severe', 'Coachable'], [majorEvents, coachable], [C.red, C.amber]);
  }

  /* ── View: Cold Chain ───────────────────────────────────────────── */
  function renderColdChain() {
    var ct = data.cold_chain_trend || [];
    var avgAmbient = avg(ct, 'avg_ambient');
    var avgProbe = avg(ct, 'avg_probe');
    var highBreaches = sum(ct, 'high_breaches');
    var lowBreaches = sum(ct, 'low_breaches');
    var daysWithBreach = ct.filter(function (r) { return (r.high_breaches || 0) + (r.low_breaches || 0) > 0; }).length;
    var complianceRate = ct.length ? (ct.length - daysWithBreach) / ct.length * 100 : 100;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#127777;', C.cyanA, fmt(avgProbe) + '&deg;C', 'Avg Probe Temp', trendHTML(trendPct(ct, 'avg_probe'), true));
    html += kpiCard('&#127780;', C.amberA, fmt(avgAmbient) + '&deg;C', 'Avg Ambient Temp', '');
    html += kpiCard('&#128293;', C.redA, fmtInt(highBreaches), 'High Temp Breaches', trendHTML(trendPct(ct, 'high_breaches'), true));
    html += kpiCard('&#10052;', C.blueA, fmtInt(lowBreaches), 'Low Temp Breaches', trendHTML(trendPct(ct, 'low_breaches'), true));
    html += kpiCard('&#9989;', complianceRate >= 95 ? C.greenA : C.redA, fmt(complianceRate, 1) + '%', 'Compliance Rate', '');
    html += kpiCard('&#128197;', C.purpleA, fmtInt(ct.length), 'Days Monitored', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-cold-temp', 'Temperature Trend', 'Probe vs Ambient with thresholds', true);
    html += chartCard('ch-cold-breach', 'Daily Breach Count', 'High and low temperature violations');
    html += chartCard('ch-cold-comp', 'Compliance Breakdown', 'Compliant vs breach days');
    html += '</div>';

    el('rpt-content').innerHTML = html;

    var labels = ct.map(function (r) { return shortDate(r.event_date); });
    lineChart('ch-cold-temp', labels, [
      { label: 'Probe Temp (°C)', data: ct.map(function (r) { return r.avg_probe; }), borderColor: C.cyan, backgroundColor: C.cyanA, fill: true },
      { label: 'Ambient Temp (°C)', data: ct.map(function (r) { return r.avg_ambient; }), borderColor: C.amber, backgroundColor: 'transparent' },
      { label: 'High Threshold (8°C)', data: ct.map(function () { return 8; }), borderColor: C.red, borderDash: [6, 4], borderWidth: 1.5, pointRadius: 0 },
      { label: 'Low Threshold (-25°C)', data: ct.map(function () { return -25; }), borderColor: C.blue, borderDash: [6, 4], borderWidth: 1.5, pointRadius: 0 }
    ], { yLabel: '°C' });

    barChart('ch-cold-breach', labels, [
      { label: 'High Temp', data: ct.map(function (r) { return r.high_breaches; }), backgroundColor: C.red, borderRadius: 2 },
      { label: 'Low Temp', data: ct.map(function (r) { return r.low_breaches; }), backgroundColor: C.blue, borderRadius: 2 }
    ], { stacked: true });

    doughnutChart('ch-cold-comp', ['Compliant Days', 'Breach Days'],
      [ct.length - daysWithBreach, daysWithBreach], [C.green, C.red]);
  }

  /* ── View: Asset Health ─────────────────────────────────────────── */
  function renderAsset() {
    var at = data.asset_health_trend || [];
    var totalDvirs = sum(at, 'dvirs');
    var openIssues = sum(at, 'open_issues');
    var completed = sum(at, 'completed');
    var resRate = (openIssues + completed) ? completed / (openIssues + completed) * 100 : 100;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128203;', C.purpleA, fmtK(totalDvirs), 'Total DVIRs', trendHTML(trendPct(at, 'dvirs')));
    html += kpiCard('&#128308;', C.redA, fmtK(openIssues), 'Open Issues', trendHTML(trendPct(at, 'open_issues'), true));
    html += kpiCard('&#9989;', C.greenA, fmtK(completed), 'Completed Issues', trendHTML(trendPct(at, 'completed')));
    html += kpiCard('&#128736;', resRate >= 80 ? C.greenA : C.amberA, fmt(resRate, 1) + '%', 'Resolution Rate', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-asset-dvir', 'Daily DVIR Count', 'Vehicle inspection reports', true);
    html += chartCard('ch-asset-issues', 'Open vs Completed Issues', 'Issue resolution over time');
    html += chartCard('ch-asset-dist', 'Issue Resolution', 'Overall breakdown');
    html += '</div>';

    el('rpt-content').innerHTML = html;

    var labels = at.map(function (r) { return shortDate(r.event_date); });
    lineChart('ch-asset-dvir', labels, [
      { label: 'DVIRs', data: at.map(function (r) { return r.dvirs; }), borderColor: C.purple, backgroundColor: C.purpleA, fill: true }
    ], { noLegend: true, yLabel: 'DVIRs' });

    barChart('ch-asset-issues', labels, [
      { label: 'Open', data: at.map(function (r) { return r.open_issues; }), backgroundColor: C.red, borderRadius: 2 },
      { label: 'Completed', data: at.map(function (r) { return r.completed; }), backgroundColor: C.green, borderRadius: 2 }
    ], { stacked: true });

    doughnutChart('ch-asset-dist', ['Completed', 'Open'], [completed, openIssues], [C.green, C.red]);
  }

  /* ── View: HOS Violations ───────────────────────────────────────── */
  function renderHOS() {
    var hos = data.hos_violations || [];
    var totalViolations = sum(hos, 'violation_count');
    var totalDurationMs = sum(hos, 'total_duration_ms');
    var totalDurationHrs = totalDurationMs / 3600000;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#9201;', C.pinkA, fmtK(totalViolations), 'Total HOS Violations', trendHTML(trendPct(hos, 'violation_count'), true));
    html += kpiCard('&#128336;', C.redA, fmt(totalDurationHrs, 1) + 'h', 'Total Violation Duration', '');
    html += kpiCard('&#128197;', C.amberA, fmtInt(hos.length), 'Days with Violations', '');
    html += kpiCard('&#128200;', C.blueA, fmt(totalViolations / (hos.length || 1), 1), 'Avg Violations/Day', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-hos-trend', 'Daily HOS Violations', 'Count of violations per day', true);
    html += chartCard('ch-hos-dur', 'Violation Duration', 'Total duration (hours) per day');
    html += chartCard('ch-hos-ratio', 'Violation Intensity', 'Avg duration per violation');
    html += '</div>';

    el('rpt-content').innerHTML = html;

    var labels = hos.map(function (r) { return shortDate(r.event_date); });
    barChart('ch-hos-trend', labels, [
      { label: 'Violations', data: hos.map(function (r) { return r.violation_count; }), backgroundColor: C.pink, borderRadius: 3 }
    ], { noLegend: true });

    lineChart('ch-hos-dur', labels, [
      { label: 'Duration (hrs)', data: hos.map(function (r) { return (r.total_duration_ms || 0) / 3600000; }), borderColor: C.red, backgroundColor: C.redA, fill: true }
    ], { noLegend: true, yLabel: 'Hours' });

    lineChart('ch-hos-ratio', labels, [
      { label: 'Avg Duration/Violation (min)', data: hos.map(function (r) { return r.violation_count ? (r.total_duration_ms / r.violation_count) / 60000 : 0; }),
        borderColor: C.amber, backgroundColor: C.amberA, fill: true }
    ], { noLegend: true, yLabel: 'Minutes' });
  }

  /* ── View: IFTA Summary ─────────────────────────────────────────── */
  function renderIFTA() {
    var ifta = data.ifta_summary || [];
    var totalTaxGal = sum(ifta, 'taxable_gallons');
    var totalTaxMi = sum(ifta, 'taxable_miles');
    var totalGal = sum(ifta, 'total_gallons');
    var totalMi = sum(ifta, 'total_miles');

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#9651;', C.greenA, fmtK(totalTaxMi), 'Taxable Miles', '');
    html += kpiCard('&#9981;', C.amberA, fmtK(totalTaxGal), 'Taxable Gallons', '');
    html += kpiCard('&#128739;', C.blueA, fmtK(totalMi), 'Total Miles', '');
    html += kpiCard('&#128167;', C.purpleA, fmtK(totalGal), 'Total Gallons', '');
    html += kpiCard('&#128200;', C.cyanA, fmt(totalTaxMi / (totalTaxGal || 1)), 'Tax MPG', '');
    html += kpiCard('&#128176;', C.redA, '$' + fmtK(totalTaxGal * 0.244), 'Est. IFTA Tax', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-ifta-miles', 'Taxable vs Total Miles', 'IFTA reporting period', true);
    html += chartCard('ch-ifta-gallons', 'Taxable vs Total Gallons', 'Fuel tax allocation');
    html += chartCard('ch-ifta-ratio', 'Tax Ratio', 'Taxable percentage');
    html += '</div>';

    el('rpt-content').innerHTML = html;

    var labels = ifta.map(function (r) { return shortDate(r.event_date); });
    lineChart('ch-ifta-miles', labels, [
      { label: 'Total Miles', data: ifta.map(function (r) { return r.total_miles; }), borderColor: C.blue, backgroundColor: C.blueA, fill: true },
      { label: 'Taxable Miles', data: ifta.map(function (r) { return r.taxable_miles; }), borderColor: C.green, backgroundColor: C.greenA, fill: true }
    ]);

    lineChart('ch-ifta-gallons', labels, [
      { label: 'Total Gallons', data: ifta.map(function (r) { return r.total_gallons; }), borderColor: C.purple, backgroundColor: C.purpleA, fill: true },
      { label: 'Taxable Gallons', data: ifta.map(function (r) { return r.taxable_gallons; }), borderColor: C.amber, backgroundColor: C.amberA, fill: true }
    ]);

    doughnutChart('ch-ifta-ratio', ['Taxable', 'Non-Taxable'],
      [totalTaxMi, totalMi - totalTaxMi], [C.green, C.gray]);
  }

  /* ── View: Fleet Utilization Detail ──────────────────────────────── */
  function renderFleetUtil() {
    var ubv = data.util_by_vehicle || [];
    var ut = data.utilization_trend || [];
    var ids = {};
    ubv.forEach(function (r) { ids[r.vehicle_id] = true; });
    var activeVehicles = Object.keys(ids).length;
    var avgSpd = avg(ubv, 'avg_speed');
    var totalRecs = sum(ubv, 'total_records');

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128666;', C.indigoA, fmtInt(activeVehicles), 'Active Vehicles', '');
    html += kpiCard('&#128668;', C.blueA, fmt(avgSpd), 'Avg Speed (mph)', trendHTML(trendPct(ut, 'avg_speed')));
    html += kpiCard('&#128225;', C.purpleA, fmtK(totalRecs), 'Total Records', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-futil-speed', 'Daily Average Speed', 'Fleet-wide daily avg speed', true);
    html += '</div>';

    var th = '<th>Vehicle</th><th class="num">Avg Speed</th><th class="num">Records</th><th class="num">Max Odometer</th>';
    var tb = ubv.map(function (r) {
      return '<tr><td>' + r.vehicle_id + '</td><td class="num">' + fmt(r.avg_speed) + '</td><td class="num">' +
        fmtInt(r.total_records) + '</td><td class="num">' + fmtK(r.max_odometer) + '</td></tr>';
    }).join('');
    html += tableWrap('Vehicle Utilization Detail', th, tb);

    el('rpt-content').innerHTML = html;

    var labels = ut.map(function (r) { return shortDate(r.event_date); });
    lineChart('ch-futil-speed', labels, [
      { label: 'Avg Speed (mph)', data: ut.map(function (r) { return r.avg_speed; }), borderColor: C.blue, backgroundColor: C.blueA, fill: true }
    ], { yLabel: 'mph' });
  }

  /* ── View: Vehicle Activity ────────────────────────────────────── */
  function renderVehicleActivity() {
    var ubv = data.util_by_vehicle || [];
    var ubvd = data.util_by_vehicle_daily || [];
    var ut = data.utilization_trend || [];
    var totalRecs = sum(ubv, 'total_records');
    var avgSpd = avg(ubv, 'avg_speed');
    var maxOdo = ubv.reduce(function (m, r) { return Math.max(m, r.max_odometer || 0); }, 0);

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128225;', C.purpleA, fmtK(totalRecs), 'Total Records', '');
    html += kpiCard('&#128668;', C.blueA, fmt(avgSpd), 'Avg Speed (mph)', '');
    html += kpiCard('&#128739;', C.greenA, fmtK(maxOdo), 'Max Odometer (mi)', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-vact-recs', 'Daily Record Count', 'Telemetry volume per day', true);
    html += '</div>';

    var recent = ubvd.slice(-50);
    var th = '<th>Date</th><th>Vehicle</th><th class="num">Avg Speed</th><th class="num">Records</th>';
    var tb = recent.map(function (r) {
      return '<tr><td>' + shortDate(r.event_date) + '</td><td>' + r.vehicle_id + '</td><td class="num">' +
        fmt(r.avg_speed) + '</td><td class="num">' + fmtInt(r.total_records) + '</td></tr>';
    }).join('');
    html += tableWrap('Recent Vehicle Activity (last 50)', th, tb);

    el('rpt-content').innerHTML = html;

    var labels = ut.map(function (r) { return shortDate(r.event_date); });
    barChart('ch-vact-recs', labels, [
      { label: 'Records', data: ut.map(function (r) { return r.total_records; }), backgroundColor: C.purple, borderRadius: 3 }
    ], { noLegend: true });
  }

  /* ── View: Route Performance ───────────────────────────────────── */
  function renderRoutePerf() {
    var rt = data.route_trend || [];
    var totalRoutes = sum(rt, 'routes');
    var avgPerDay = rt.length ? totalRoutes / rt.length : 0;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128205;', C.greenA, fmtK(totalRoutes), 'Total Routes', trendHTML(trendPct(rt, 'routes')));
    html += kpiCard('&#128200;', C.blueA, fmt(avgPerDay, 1), 'Avg Routes/Day', '');
    html += kpiCard('&#128197;', C.purpleA, fmtInt(rt.length), 'Days Tracked', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-route-trend', 'Daily Routes', 'Route volume over time', true);
    html += '</div>';

    var th = '<th>Date</th><th class="num">Routes</th>';
    var tb = rt.map(function (r) {
      return '<tr><td>' + shortDate(r.event_date) + '</td><td class="num">' + fmtInt(r.routes) + '</td></tr>';
    }).join('');
    html += tableWrap('Route Trend Detail', th, tb);

    el('rpt-content').innerHTML = html;

    lineChart('ch-route-trend', rt.map(function (r) { return shortDate(r.event_date); }), [
      { label: 'Routes', data: rt.map(function (r) { return r.routes; }), borderColor: C.green, backgroundColor: C.greenA, fill: true }
    ], { yLabel: 'Routes' });
  }

  /* ── View: Dispatch ────────────────────────────────────────────── */
  function renderDispatch() {
    var dt = data.dispatch_trend || [];
    var totalJobs = data.job_count || sum(dt, 'jobs');
    var avgPerDay = dt.length ? totalJobs / dt.length : 0;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128203;', C.indigoA, fmtK(totalJobs), 'Total Jobs', trendHTML(trendPct(dt, 'jobs')));
    html += kpiCard('&#128200;', C.blueA, fmt(avgPerDay, 1), 'Avg Jobs/Day', '');
    html += kpiCard('&#128197;', C.amberA, fmtInt(dt.length), 'Days Tracked', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-dispatch-trend', 'Daily Dispatch Volume', 'Jobs dispatched per day', true);
    html += '</div>';

    var th = '<th>Date</th><th class="num">Jobs</th>';
    var tb = dt.map(function (r) {
      return '<tr><td>' + shortDate(r.event_date) + '</td><td class="num">' + fmtInt(r.jobs) + '</td></tr>';
    }).join('');
    html += tableWrap('Dispatch Trend Detail', th, tb);

    el('rpt-content').innerHTML = html;

    barChart('ch-dispatch-trend', dt.map(function (r) { return shortDate(r.event_date); }), [
      { label: 'Jobs', data: dt.map(function (r) { return r.jobs; }), backgroundColor: C.indigo, borderRadius: 3 }
    ], { noLegend: true });
  }

  /* ── View: Vehicle Master ──────────────────────────────────────── */
  function renderVehicleMaster() {
    var vf = data.vehicle_fleet || [];
    var ids = {};
    vf.forEach(function (r) { ids[r.vehicle_id] = true; });
    var totalVehicles = Object.keys(ids).length;

    var yearBuckets = {};
    vf.forEach(function (r) {
      var y = r.year || 'Unknown';
      yearBuckets[y] = (yearBuckets[y] || 0) + 1;
    });
    var yearLabels = Object.keys(yearBuckets).sort();
    var yearValues = yearLabels.map(function (y) { return yearBuckets[y]; });
    var yearColors = [C.indigo, C.blue, C.green, C.amber, C.red, C.purple, C.cyan, C.pink];

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128666;', C.indigoA, fmtInt(totalVehicles), 'Total Vehicles', '');
    html += kpiCard('&#128203;', C.blueA, fmtInt(vf.length), 'Fleet Records', '');
    html += kpiCard('&#128197;', C.greenA, fmtInt(yearLabels.length), 'Model Years', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-vmast-year', 'Vehicles by Model Year', 'Fleet age distribution');
    html += '</div>';

    var th = '<th>Vehicle ID</th><th>Year</th><th>Make</th><th>Model</th>';
    var tb = vf.map(function (r) {
      return '<tr><td>' + (r.vehicle_id || '') + '</td><td>' + (r.year || '') + '</td><td>' +
        (r.make || '') + '</td><td>' + (r.model || '') + '</td></tr>';
    }).join('');
    html += tableWrap('Vehicle Fleet Master', th, tb);

    el('rpt-content').innerHTML = html;

    doughnutChart('ch-vmast-year', yearLabels, yearValues,
      yearLabels.map(function (_, i) { return yearColors[i % yearColors.length]; }));
  }

  /* ── View: Fuel Efficiency ─────────────────────────────────────── */
  function renderFuelEff() {
    var ft = data.fuel_trend || [];
    var fv = data.fuel_by_vehicle || [];
    var totalFuel = sum(ft, 'total_fuel');
    var totalDist = sum(ft, 'distance');
    var avgMpg = avg(ft, 'avg_mpg');
    var fuelPerMile = totalDist ? totalFuel / totalDist : 0;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128167;', C.amberA, fmtK(totalFuel), 'Total Fuel (gal)', trendHTML(trendPct(ft, 'total_fuel'), true));
    html += kpiCard('&#128739;', C.blueA, fmtK(totalDist), 'Total Distance (mi)', trendHTML(trendPct(ft, 'distance')));
    html += kpiCard('&#9981;', C.greenA, fmt(avgMpg), 'Avg MPG', trendHTML(trendPct(ft, 'avg_mpg')));
    html += kpiCard('&#128200;', C.purpleA, fmt(fuelPerMile, 3), 'Fuel per Mile (gal)', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-feff-mpg', 'Daily MPG Trend', 'Fleet avg fuel efficiency', true);
    html += chartCard('ch-feff-fuel', 'Daily Fuel Consumption', 'Gallons per day');
    html += '</div>';

    var th = '<th>Vehicle</th><th class="num">Fuel (gal)</th><th class="num">Avg MPG</th><th class="num">Idle Hrs</th>';
    var tb = fv.map(function (r) {
      return '<tr><td>' + r.vehicle_id + '</td><td class="num">' + fmtInt(r.total_fuel) + '</td><td class="num">' +
        fmt(r.avg_mpg) + '</td><td class="num">' + fmtInt(r.idle_hours) + '</td></tr>';
    }).join('');
    html += tableWrap('Vehicle Fuel Breakdown', th, tb);

    el('rpt-content').innerHTML = html;

    var labels = ft.map(function (r) { return shortDate(r.event_date); });
    lineChart('ch-feff-mpg', labels, [
      { label: 'Avg MPG', data: ft.map(function (r) { return r.avg_mpg; }), borderColor: C.green, backgroundColor: C.greenA, fill: true }
    ], { yLabel: 'MPG' });

    barChart('ch-feff-fuel', labels, [
      { label: 'Fuel (gal)', data: ft.map(function (r) { return r.total_fuel; }), backgroundColor: C.amber, borderRadius: 3 }
    ], { noLegend: true });
  }

  /* ── View: Idling Analysis ─────────────────────────────────────── */
  function renderIdling() {
    var ft = data.fuel_trend || [];
    var et = data.emissions_trend || [];
    var fv = data.fuel_by_vehicle || [];
    var totalIdleHrs = sum(ft, 'idle_hours');
    var totalIdleFuel = sum(et, 'idle_fuel');
    var idlePerVehicle = fv.length ? totalIdleHrs / fv.length : 0;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128336;', C.redA, fmtK(totalIdleHrs), 'Total Idle Hours', trendHTML(trendPct(ft, 'idle_hours'), true));
    html += kpiCard('&#128167;', C.amberA, fmtK(totalIdleFuel), 'Total Idle Fuel (gal)', '');
    html += kpiCard('&#128666;', C.blueA, fmt(idlePerVehicle, 1), 'Idle Hrs/Vehicle', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-idle-trend', 'Daily Idle Hours', 'Fleet idle hours over time', true);
    html += '</div>';

    var sorted = fv.slice().sort(function (a, b) { return (b.idle_hours || 0) - (a.idle_hours || 0); });
    var th = '<th>Vehicle</th><th class="num">Idle Hours</th><th class="num">Total Fuel</th><th class="num">Avg MPG</th>';
    var tb = sorted.map(function (r) {
      return '<tr><td>' + r.vehicle_id + '</td><td class="num">' + fmtInt(r.idle_hours) + '</td><td class="num">' +
        fmtInt(r.total_fuel) + '</td><td class="num">' + fmt(r.avg_mpg) + '</td></tr>';
    }).join('');
    html += tableWrap('Vehicles by Idle Hours (DESC)', th, tb);

    el('rpt-content').innerHTML = html;

    var labels = ft.map(function (r) { return shortDate(r.event_date); });
    lineChart('ch-idle-trend', labels, [
      { label: 'Idle Hours', data: ft.map(function (r) { return r.idle_hours; }), borderColor: C.red, backgroundColor: C.redA, fill: true }
    ], { yLabel: 'Hours' });
  }

  /* ── View: Fuel Cost Analysis ──────────────────────────────────── */
  function renderFuelCost() {
    var fv = data.fuel_by_vehicle || [];
    var totalFuel = sum(fv, 'total_fuel');
    var top10 = fv.slice(0, 10);
    var worstVehicle = fv.length ? fv[0].vehicle_id : '—';
    var savingsPotential = totalFuel * 0.10;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128680;', C.redA, worstVehicle, 'Top Fuel Consumer', '');
    html += kpiCard('&#128167;', C.amberA, fmtK(totalFuel), 'Total Fuel (gal)', '');
    html += kpiCard('&#128176;', C.greenA, fmtK(savingsPotential) + ' gal', '10% Savings Potential', '');
    html += kpiCard('&#128176;', C.blueA, '$' + fmtK(totalFuel * 3.45), 'Est. Fleet Fuel Cost', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-fcost-top', 'Top 10 Vehicles by Fuel', 'Highest consumers', true);
    html += '</div>';

    var th = '<th>Vehicle</th><th class="num">Total Fuel</th><th class="num">Distance</th><th class="num">Fuel/Mile</th><th class="num">Avg MPG</th>';
    var tb = fv.map(function (r) {
      var fpm = r.distance ? (r.total_fuel / r.distance) : 0;
      return '<tr><td>' + r.vehicle_id + '</td><td class="num">' + fmtInt(r.total_fuel) + '</td><td class="num">' +
        fmtK(r.distance) + '</td><td class="num">' + fmt(fpm, 3) + '</td><td class="num">' + fmt(r.avg_mpg) + '</td></tr>';
    }).join('');
    html += tableWrap('Vehicle Fuel Cost Detail', th, tb);

    el('rpt-content').innerHTML = html;

    barChart('ch-fcost-top', top10.map(function (r) { return r.vehicle_id; }), [
      { label: 'Fuel (gal)', data: top10.map(function (r) { return r.total_fuel; }), backgroundColor: C.amber, borderRadius: 4 }
    ], { horizontal: true, noLegend: true });
  }

  /* ── View: Emissions ───────────────────────────────────────────── */
  function renderEmissions() {
    var et = data.emissions_trend || [];
    var totalCO2 = sum(et, 'co2_kg');
    var totalDist = sum(et, 'distance') || sum(data.fuel_trend || [], 'distance');
    var fv = data.fuel_by_vehicle || [];
    var co2PerMile = totalDist ? totalCO2 / totalDist : 0;
    var co2PerVehicle = fv.length ? totalCO2 / fv.length : 0;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#127811;', C.greenA, fmtK(totalCO2), 'Total CO2 (kg)', trendHTML(trendPct(et, 'co2_kg'), true));
    html += kpiCard('&#128200;', C.blueA, fmt(co2PerMile, 2), 'CO2 per Mile (kg)', '');
    html += kpiCard('&#128666;', C.purpleA, fmtK(co2PerVehicle), 'CO2 per Vehicle (kg)', '');
    html += kpiCard('&#128197;', C.amberA, fmtInt(et.length), 'Days Tracked', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-emiss-co2', 'Daily CO2 Emissions', 'Fleet carbon output trend', true);
    html += '</div>';

    var th = '<th>Date</th><th class="num">CO2 (kg)</th><th class="num">Idle Fuel</th>';
    var tb = et.map(function (r) {
      return '<tr><td>' + shortDate(r.event_date) + '</td><td class="num">' + fmt(r.co2_kg, 1) + '</td><td class="num">' +
        fmt(r.idle_fuel, 1) + '</td></tr>';
    }).join('');
    html += tableWrap('Emissions Trend Detail', th, tb);

    el('rpt-content').innerHTML = html;

    lineChart('ch-emiss-co2', et.map(function (r) { return shortDate(r.event_date); }), [
      { label: 'CO2 (kg)', data: et.map(function (r) { return r.co2_kg; }), borderColor: C.green, backgroundColor: C.greenA, fill: true }
    ], { yLabel: 'kg CO2' });
  }

  /* ── View: Safety Dashboard ────────────────────────────────────── */
  function renderSafetyDash() {
    var st = data.safety_trend || [];
    var sd = data.safety_by_driver || [];
    var totalEvents = sum(st, 'total_events');
    var majorEvents = sum(st, 'major_events');
    var coachable = sum(st, 'coachable');
    var eventsPerDriver = sd.length ? totalEvents / sd.length : 0;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#9888;', C.redA, fmtK(totalEvents), 'Total Safety Events', trendHTML(trendPct(st, 'total_events'), true));
    html += kpiCard('&#128680;', C.redA, fmtK(majorEvents), 'Major Events', '');
    html += kpiCard('&#128218;', C.amberA, fmtK(coachable), 'Coachable', '');
    html += kpiCard('&#128100;', C.blueA, fmt(eventsPerDriver, 1), 'Events/Driver', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-sdash-trend', 'Daily Safety Events', 'Event volume over time', true);
    html += '</div>';

    var th = '<th>Driver</th><th class="num">Total</th><th class="num">Major</th><th class="num">Coachable</th>';
    var tb = sd.map(function (r) {
      return '<tr><td>' + r.driver_id + '</td><td class="num">' + fmtInt(r.total_events) + '</td><td class="num">' +
        fmtInt(r.major_events) + '</td><td class="num">' + fmtInt(r.coachable) + '</td></tr>';
    }).join('');
    html += tableWrap('Driver Safety Summary', th, tb);

    el('rpt-content').innerHTML = html;

    lineChart('ch-sdash-trend', st.map(function (r) { return shortDate(r.event_date); }), [
      { label: 'Total Events', data: st.map(function (r) { return r.total_events; }), borderColor: C.red, backgroundColor: C.redA, fill: true }
    ], { yLabel: 'Events' });
  }

  /* ── View: Driver Scorecard ────────────────────────────────────── */
  function renderDriverScorecard() {
    var ss = data.safety_scores || [];
    var sd = data.safety_by_driver || [];
    var avgScore = avg(ss, 'safety_score');
    var totalEvents = sum(sd, 'total_events');

    var buckets = { 'Excellent (90+)': 0, 'Good (75-89)': 0, 'Fair (60-74)': 0, 'Poor (<60)': 0 };
    ss.forEach(function (r) {
      var s = r.safety_score || 0;
      if (s >= 90) buckets['Excellent (90+)']++;
      else if (s >= 75) buckets['Good (75-89)']++;
      else if (s >= 60) buckets['Fair (60-74)']++;
      else buckets['Poor (<60)']++;
    });

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#127942;', avgScore >= 75 ? C.greenA : C.redA, fmt(avgScore, 0), 'Avg Safety Score', '');
    html += kpiCard('&#9888;', C.redA, fmtK(totalEvents), 'Total Events', '');
    html += kpiCard('&#128100;', C.blueA, fmtInt(ss.length), 'Drivers Scored', '');
    html += kpiCard('&#128308;', C.amberA, fmtInt(buckets['Poor (<60)']), 'Poor Score Drivers', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-dsc-dist', 'Score Distribution', 'Driver safety score brackets');
    html += '</div>';

    var th = '<th>Driver</th><th class="num">Safety Score</th><th class="num">Total Events</th><th class="num">Major</th>';
    var rows = ss.map(function (s) {
      var dr = sd.find(function (d) { return d.driver_id === s.driver_id; }) || {};
      return { driver_id: s.driver_id, safety_score: s.safety_score, total_events: dr.total_events || 0, major_events: dr.major_events || 0 };
    });
    var tb = rows.map(function (r) {
      return '<tr><td>' + r.driver_id + '</td><td class="num">' + fmt(r.safety_score, 0) + '</td><td class="num">' +
        fmtInt(r.total_events) + '</td><td class="num">' + fmtInt(r.major_events) + '</td></tr>';
    }).join('');
    html += tableWrap('Driver Scorecard Detail', th, tb);

    el('rpt-content').innerHTML = html;

    var bLabels = Object.keys(buckets);
    doughnutChart('ch-dsc-dist', bLabels, bLabels.map(function (k) { return buckets[k]; }),
      [C.green, C.blue, C.amber, C.red]);
  }

  /* ── View: Coaching Opportunities ──────────────────────────────── */
  function renderCoaching() {
    var sd = data.safety_by_driver || [];
    var needsReview = sd.filter(function (r) { return (r.major_events || 0) > 5; }).length;
    var totalCoachable = sum(sd, 'coachable');
    var coachDrivers = sd.filter(function (r) { return (r.coachable || 0) > 0; });

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128214;', C.redA, fmtInt(needsReview), 'Drivers Need Review (Major>5)', '');
    html += kpiCard('&#128218;', C.amberA, fmtK(totalCoachable), 'Total Coachable Events', '');
    html += kpiCard('&#128100;', C.blueA, fmtInt(coachDrivers.length), 'Drivers with Coachable', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-coach-compare', 'Coachable vs Major by Driver', 'Top drivers by combined events', true);
    html += '</div>';

    var th = '<th>Driver</th><th class="num">Coachable</th><th class="num">Major</th><th class="num">Total</th>';
    var tb = coachDrivers.map(function (r) {
      return '<tr><td>' + r.driver_id + '</td><td class="num">' + fmtInt(r.coachable) + '</td><td class="num">' +
        fmtInt(r.major_events) + '</td><td class="num">' + fmtInt(r.total_events) + '</td></tr>';
    }).join('');
    html += tableWrap('Coachable Drivers', th, tb);

    el('rpt-content').innerHTML = html;

    var top15 = sd.slice(0, 15);
    barChart('ch-coach-compare', top15.map(function (r) { return r.driver_id; }), [
      { label: 'Coachable', data: top15.map(function (r) { return r.coachable; }), backgroundColor: C.amber, borderRadius: 3 },
      { label: 'Major', data: top15.map(function (r) { return r.major_events; }), backgroundColor: C.red, borderRadius: 3 }
    ], { stacked: true, horizontal: true });
  }

  /* ── View: Safety Trend ────────────────────────────────────────── */
  function renderSafetyTrend() {
    var st = data.safety_trend || [];
    var mid = Math.floor(st.length / 2);
    var firstHalf = st.slice(0, mid);
    var secondHalf = st.slice(mid);
    var thisMonthEvts = sum(secondHalf, 'total_events');
    var lastMonthEvts = sum(firstHalf, 'total_events');

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128200;', C.blueA, fmtK(thisMonthEvts), 'Recent Period Events', '');
    html += kpiCard('&#128201;', C.grayA, fmtK(lastMonthEvts), 'Prior Period Events', '');
    html += kpiCard('&#128197;', C.purpleA, fmtInt(st.length), 'Days Tracked', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-strend-stack', 'Daily Events Breakdown', 'Total, Major, and Coachable events', true);
    html += '</div>';

    var th = '<th>Date</th><th class="num">Total</th><th class="num">Major</th><th class="num">Coachable</th>';
    var tb = st.map(function (r) {
      return '<tr><td>' + shortDate(r.event_date) + '</td><td class="num">' + fmtInt(r.total_events) + '</td><td class="num">' +
        fmtInt(r.major_events) + '</td><td class="num">' + fmtInt(r.coachable) + '</td></tr>';
    }).join('');
    html += tableWrap('Safety Trend Detail', th, tb);

    el('rpt-content').innerHTML = html;

    var labels = st.map(function (r) { return shortDate(r.event_date); });
    lineChart('ch-strend-stack', labels, [
      { label: 'Total Events', data: st.map(function (r) { return r.total_events; }), borderColor: C.indigo, backgroundColor: C.indigoA, fill: true },
      { label: 'Major', data: st.map(function (r) { return r.major_events; }), borderColor: C.red, backgroundColor: C.redA, fill: true },
      { label: 'Coachable', data: st.map(function (r) { return r.coachable; }), borderColor: C.amber, backgroundColor: C.amberA, fill: true }
    ], { stacked: true });
  }

  /* ── View: Driver Compliance ───────────────────────────────────── */
  function renderDriverCompliance() {
    var hbd = data.hos_by_driver || [];
    var totalViolators = hbd.length;
    var repeatViolators = hbd.filter(function (r) { return (r.violation_count || 0) > 3; }).length;
    var avgViolations = totalViolators ? sum(hbd, 'violation_count') / totalViolators : 0;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128100;', C.pinkA, fmtInt(totalViolators), 'Total Violators', '');
    html += kpiCard('&#128680;', C.redA, fmtInt(repeatViolators), 'Repeat Violators (>3)', '');
    html += kpiCard('&#128200;', C.amberA, fmt(avgViolations, 1), 'Avg Violations/Driver', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-dcomp-bar', 'Violations by Driver', 'HOS violation counts', true);
    html += '</div>';

    var th = '<th>Driver</th><th class="num">Violations</th><th class="num">Duration (hrs)</th>';
    var tb = hbd.map(function (r) {
      return '<tr><td>' + r.driver_id + '</td><td class="num">' + fmtInt(r.violation_count) + '</td><td class="num">' +
        fmt((r.total_duration_ms || 0) / 3600000, 1) + '</td></tr>';
    }).join('');
    html += tableWrap('HOS Violations by Driver', th, tb);

    el('rpt-content').innerHTML = html;

    barChart('ch-dcomp-bar', hbd.map(function (r) { return r.driver_id; }), [
      { label: 'Violations', data: hbd.map(function (r) { return r.violation_count; }), backgroundColor: C.pink, borderRadius: 4 }
    ], { horizontal: true, noLegend: true });
  }

  /* ── View: Compliance Trend ────────────────────────────────────── */
  function renderComplianceTrend() {
    var hos = data.hos_violations || [];
    var ifta = data.ifta_summary || [];
    var violTrend = trendPct(hos, 'violation_count');
    var milesTrend = trendPct(ifta, 'taxable_miles');

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#9201;', C.pinkA, fmtK(sum(hos, 'violation_count')), 'Total Violations', trendHTML(violTrend, true));
    html += kpiCard('&#128739;', C.greenA, fmtK(sum(ifta, 'taxable_miles')), 'Taxable Miles', trendHTML(milesTrend));
    html += kpiCard('&#128197;', C.blueA, fmtInt(Math.max(hos.length, ifta.length)), 'Days Tracked', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-ctrend-viol', 'HOS Violations Over Time', 'Daily violation count');
    html += chartCard('ch-ctrend-miles', 'IFTA Taxable Miles', 'Reporting period trend');
    html += '</div>';

    var th = '<th>Date</th><th class="num">HOS Violations</th><th class="num">Taxable Miles</th>';
    var maxLen = Math.max(hos.length, ifta.length);
    var tb = '';
    for (var i = 0; i < Math.min(maxLen, 50); i++) {
      var h = hos[i] || {};
      var f = ifta[i] || {};
      var date = h.event_date || f.event_date || '';
      tb += '<tr><td>' + shortDate(date) + '</td><td class="num">' + fmtInt(h.violation_count) + '</td><td class="num">' + fmtK(f.taxable_miles) + '</td></tr>';
    }
    html += tableWrap('Compliance Combined Trend', th, tb);

    el('rpt-content').innerHTML = html;

    barChart('ch-ctrend-viol', hos.map(function (r) { return shortDate(r.event_date); }), [
      { label: 'Violations', data: hos.map(function (r) { return r.violation_count; }), backgroundColor: C.pink, borderRadius: 3 }
    ], { noLegend: true });

    lineChart('ch-ctrend-miles', ifta.map(function (r) { return shortDate(r.event_date); }), [
      { label: 'Taxable Miles', data: ifta.map(function (r) { return r.taxable_miles; }), borderColor: C.green, backgroundColor: C.greenA, fill: true }
    ], { yLabel: 'Miles' });
  }

  /* ── View: Temperature Excursion ───────────────────────────────── */
  function renderTempExcursion() {
    var cbs = data.cold_by_sensor || [];
    var cbsd = data.cold_by_sensor_daily || [];
    var totalBreaches = sum(cbs, 'high_breaches') + sum(cbs, 'low_breaches');
    var sensorsWithBreaches = cbs.filter(function (r) { return (r.high_breaches || 0) + (r.low_breaches || 0) > 0; }).length;
    var worstSensor = cbs.length ? cbs.reduce(function (w, r) {
      var b = (r.high_breaches || 0) + (r.low_breaches || 0);
      return b > w.b ? { id: r.sensor_id || r.vehicle_id, b: b } : w;
    }, { id: '—', b: 0 }).id : '—';

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128293;', C.redA, fmtInt(totalBreaches), 'Total Breaches', '');
    html += kpiCard('&#128225;', C.amberA, fmtInt(sensorsWithBreaches), 'Sensors with Breaches', '');
    html += kpiCard('&#128680;', C.pinkA, worstSensor, 'Worst Sensor', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-texc-sensor', 'Breaches per Sensor', 'High + Low combined', true);
    html += '</div>';

    var filtered = cbsd.filter(function (r) { return (r.high_breaches || 0) + (r.low_breaches || 0) > 0; });
    var th = '<th>Date</th><th>Sensor</th><th class="num">High</th><th class="num">Low</th>';
    var tb = filtered.slice(0, 50).map(function (r) {
      return '<tr><td>' + shortDate(r.event_date) + '</td><td>' + (r.sensor_id || r.vehicle_id || '') + '</td><td class="num">' +
        fmtInt(r.high_breaches) + '</td><td class="num">' + fmtInt(r.low_breaches) + '</td></tr>';
    }).join('');
    html += tableWrap('Daily Breach Events', th, tb);

    el('rpt-content').innerHTML = html;

    barChart('ch-texc-sensor', cbs.map(function (r) { return r.sensor_id || r.vehicle_id || ''; }), [
      { label: 'High Breaches', data: cbs.map(function (r) { return r.high_breaches || 0; }), backgroundColor: C.red, borderRadius: 3 },
      { label: 'Low Breaches', data: cbs.map(function (r) { return r.low_breaches || 0; }), backgroundColor: C.blue, borderRadius: 3 }
    ], { stacked: true, horizontal: true });
  }

  /* ── View: Reefer / Trailer ────────────────────────────────────── */
  function renderReefer() {
    var ts = data.trailer_stats || [];
    var totalTrailers = ts.length;
    var avgEngineHrs = avg(ts, 'engine_hours');
    var maxOdo = ts.reduce(function (m, r) { return Math.max(m, r.odometer || 0); }, 0);

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128667;', C.cyanA, fmtInt(totalTrailers), 'Total Trailers', '');
    html += kpiCard('&#9881;', C.blueA, fmt(avgEngineHrs, 0), 'Avg Engine Hours', '');
    html += kpiCard('&#128739;', C.greenA, fmtK(maxOdo), 'Max Odometer', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-reefer-eng', 'Trailer Engine Hours', 'Per trailer comparison', true);
    html += '</div>';

    var th = '<th>Trailer</th><th class="num">Engine Hours</th><th class="num">Odometer</th>';
    var tb = ts.map(function (r) {
      return '<tr><td>' + (r.trailer_id || r.vehicle_id || '') + '</td><td class="num">' + fmtInt(r.engine_hours) + '</td><td class="num">' +
        fmtK(r.odometer) + '</td></tr>';
    }).join('');
    html += tableWrap('Trailer Stats', th, tb);

    el('rpt-content').innerHTML = html;

    barChart('ch-reefer-eng', ts.map(function (r) { return r.trailer_id || r.vehicle_id || ''; }), [
      { label: 'Engine Hours', data: ts.map(function (r) { return r.engine_hours || 0; }), backgroundColor: C.cyan, borderRadius: 4 }
    ], { horizontal: true, noLegend: true });
  }

  /* ── View: Door Monitor ────────────────────────────────────────── */
  function renderDoorMonitor() {
    var dt = data.door_trend || [];
    var totalEvents = sum(dt, 'door_events');
    var activeSensors = dt.length;
    var avgPerDay = dt.length ? totalEvents / dt.length : 0;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128682;', C.amberA, fmtK(totalEvents), 'Total Door Events', trendHTML(trendPct(dt, 'door_events')));
    html += kpiCard('&#128225;', C.blueA, fmtInt(activeSensors), 'Days Monitored', '');
    html += kpiCard('&#128200;', C.greenA, fmt(avgPerDay, 1), 'Avg Events/Day', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-door-trend', 'Daily Door Events', 'Door open/close activity', true);
    html += '</div>';

    var th = '<th>Date</th><th class="num">Door Events</th>';
    var tb = dt.map(function (r) {
      return '<tr><td>' + shortDate(r.event_date) + '</td><td class="num">' + fmtInt(r.door_events) + '</td></tr>';
    }).join('');
    html += tableWrap('Door Event Trend', th, tb);

    el('rpt-content').innerHTML = html;

    lineChart('ch-door-trend', dt.map(function (r) { return shortDate(r.event_date); }), [
      { label: 'Door Events', data: dt.map(function (r) { return r.door_events; }), borderColor: C.amber, backgroundColor: C.amberA, fill: true }
    ], { yLabel: 'Events' });
  }

  /* ── View: Asset Health Dashboard ──────────────────────────────── */
  function renderAssetHealth() {
    var at = data.asset_health_trend || [];
    var abv = data.asset_by_vehicle || [];
    var totalDvirs = sum(at, 'dvirs');
    var openIssues = sum(abv, 'open_issues');
    var completed = sum(abv, 'completed');
    var resRate = (openIssues + completed) ? completed / (openIssues + completed) * 100 : 100;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128203;', C.purpleA, fmtK(totalDvirs), 'Total DVIRs', trendHTML(trendPct(at, 'dvirs')));
    html += kpiCard('&#128308;', C.redA, fmtK(openIssues), 'Open Issues', '');
    html += kpiCard('&#9989;', C.greenA, fmtK(completed), 'Completed', '');
    html += kpiCard('&#128736;', resRate >= 80 ? C.greenA : C.amberA, fmt(resRate, 1) + '%', 'Completion Rate', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-ahealth-trend', 'Daily Asset Health', 'DVIRs and issues over time', true);
    html += '</div>';

    var th = '<th>Vehicle</th><th class="num">Open Issues</th><th class="num">Completed</th><th class="num">DVIRs</th>';
    var tb = abv.map(function (r) {
      return '<tr><td>' + (r.vehicle_id || '') + '</td><td class="num">' + fmtInt(r.open_issues) + '</td><td class="num">' +
        fmtInt(r.completed) + '</td><td class="num">' + fmtInt(r.dvirs) + '</td></tr>';
    }).join('');
    html += tableWrap('Asset Health by Vehicle', th, tb);

    el('rpt-content').innerHTML = html;

    var labels = at.map(function (r) { return shortDate(r.event_date); });
    lineChart('ch-ahealth-trend', labels, [
      { label: 'DVIRs', data: at.map(function (r) { return r.dvirs; }), borderColor: C.purple, backgroundColor: C.purpleA, fill: true },
      { label: 'Open Issues', data: at.map(function (r) { return r.open_issues; }), borderColor: C.red, backgroundColor: 'transparent' },
      { label: 'Completed', data: at.map(function (r) { return r.completed; }), borderColor: C.green, backgroundColor: 'transparent' }
    ]);
  }

  /* ── View: Maintenance Queue ───────────────────────────────────── */
  function renderMaintQueue() {
    var abv = data.asset_by_vehicle || [];
    var unresolved = sum(abv, 'open_issues');
    var needsAttention = abv.filter(function (r) { return (r.open_issues || 0) > 0; }).length;
    var totalIssues = unresolved + sum(abv, 'completed');
    var avgAge = totalIssues ? unresolved / (needsAttention || 1) : 0;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128308;', C.redA, fmtInt(unresolved), 'Unresolved Issues', '');
    html += kpiCard('&#128666;', C.amberA, fmtInt(needsAttention), 'Assets Need Attention', '');
    html += kpiCard('&#128200;', C.blueA, fmt(avgAge, 1), 'Avg Open/Asset', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-maint-stack', 'Open vs Completed by Vehicle', 'Issue resolution status', true);
    html += '</div>';

    var sorted = abv.slice().sort(function (a, b) { return (b.open_issues || 0) - (a.open_issues || 0); });
    var th = '<th>Vehicle</th><th class="num">Open</th><th class="num">Completed</th><th class="num">DVIRs</th>';
    var tb = sorted.map(function (r) {
      return '<tr><td>' + (r.vehicle_id || '') + '</td><td class="num">' + fmtInt(r.open_issues) + '</td><td class="num">' +
        fmtInt(r.completed) + '</td><td class="num">' + fmtInt(r.dvirs) + '</td></tr>';
    }).join('');
    html += tableWrap('Maintenance Queue (by open issues)', th, tb);

    el('rpt-content').innerHTML = html;

    var top20 = sorted.slice(0, 20);
    barChart('ch-maint-stack', top20.map(function (r) { return r.vehicle_id || ''; }), [
      { label: 'Open', data: top20.map(function (r) { return r.open_issues || 0; }), backgroundColor: C.red, borderRadius: 3 },
      { label: 'Completed', data: top20.map(function (r) { return r.completed || 0; }), backgroundColor: C.green, borderRadius: 3 }
    ], { stacked: true, horizontal: true });
  }

  /* ── View: DVIR Detail ─────────────────────────────────────────── */
  function renderDVIR() {
    var at = data.asset_health_trend || [];
    var totalDvirs = sum(at, 'dvirs');
    var defects = sum(at, 'open_issues');
    var corrected = sum(at, 'completed');

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128203;', C.purpleA, fmtK(totalDvirs), 'Total Inspections', '');
    html += kpiCard('&#128308;', C.redA, fmtK(defects), 'Defects Found', '');
    html += kpiCard('&#9989;', C.greenA, fmtK(corrected), 'Corrected', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-dvir-daily', 'Daily DVIRs', 'Inspection volume over time', true);
    html += '</div>';

    var th = '<th>Date</th><th class="num">DVIRs</th><th class="num">Open Issues</th><th class="num">Completed</th>';
    var tb = at.map(function (r) {
      return '<tr><td>' + shortDate(r.event_date) + '</td><td class="num">' + fmtInt(r.dvirs) + '</td><td class="num">' +
        fmtInt(r.open_issues) + '</td><td class="num">' + fmtInt(r.completed) + '</td></tr>';
    }).join('');
    html += tableWrap('DVIR Trend Detail', th, tb);

    el('rpt-content').innerHTML = html;

    barChart('ch-dvir-daily', at.map(function (r) { return shortDate(r.event_date); }), [
      { label: 'DVIRs', data: at.map(function (r) { return r.dvirs; }), backgroundColor: C.purple, borderRadius: 3 }
    ], { noLegend: true, yLabel: 'DVIRs' });
  }

  /* ── View: Trailer Health ──────────────────────────────────────── */
  function renderTrailerHealth() {
    var ts = data.trailer_stats || [];
    var trailerCount = ts.length;
    var avgEngHrs = avg(ts, 'engine_hours');
    var totalOdo = sum(ts, 'odometer');

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128667;', C.cyanA, fmtInt(trailerCount), 'Trailer Count', '');
    html += kpiCard('&#9881;', C.blueA, fmt(avgEngHrs, 0), 'Avg Engine Hours', '');
    html += kpiCard('&#128739;', C.greenA, fmtK(totalOdo), 'Total Odometer', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-theal-eng', 'Engine Hours by Trailer', 'Trailer wear comparison', true);
    html += '</div>';

    var th = '<th>Trailer</th><th class="num">Engine Hours</th><th class="num">Odometer</th>';
    var tb = ts.map(function (r) {
      return '<tr><td>' + (r.trailer_id || r.vehicle_id || '') + '</td><td class="num">' + fmtInt(r.engine_hours) + '</td><td class="num">' +
        fmtK(r.odometer) + '</td></tr>';
    }).join('');
    html += tableWrap('Trailer Health Detail', th, tb);

    el('rpt-content').innerHTML = html;

    barChart('ch-theal-eng', ts.map(function (r) { return r.trailer_id || r.vehicle_id || ''; }), [
      { label: 'Engine Hours', data: ts.map(function (r) { return r.engine_hours || 0; }), backgroundColor: C.cyan, borderRadius: 4 }
    ], { horizontal: true, noLegend: true });
  }

  /* ── View: Equipment Health ────────────────────────────────────── */
  function renderEquipHealth() {
    var es = data.equipment_stats || [];
    var equipCount = es.length;
    var avgFuelPct = avg(es, 'fuel_pct');
    var avgEngHrs = avg(es, 'engine_hours');

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128736;', C.purpleA, fmtInt(equipCount), 'Equipment Count', '');
    html += kpiCard('&#128167;', C.amberA, fmt(avgFuelPct, 0) + '%', 'Avg Fuel Level', '');
    html += kpiCard('&#9881;', C.blueA, fmt(avgEngHrs, 0), 'Avg Engine Hours', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-equip-fuel', 'Equipment Fuel Level', 'Fuel % by equipment', true);
    html += '</div>';

    var th = '<th>Equipment</th><th class="num">Fuel %</th><th class="num">Engine Hours</th>';
    var tb = es.map(function (r) {
      return '<tr><td>' + (r.equipment_id || r.vehicle_id || '') + '</td><td class="num">' + fmt(r.fuel_pct, 0) + '%</td><td class="num">' +
        fmtInt(r.engine_hours) + '</td></tr>';
    }).join('');
    html += tableWrap('Equipment Stats', th, tb);

    el('rpt-content').innerHTML = html;

    barChart('ch-equip-fuel', es.map(function (r) { return r.equipment_id || r.vehicle_id || ''; }), [
      { label: 'Fuel %', data: es.map(function (r) { return r.fuel_pct || 0; }), backgroundColor: C.amber, borderRadius: 4 }
    ], { horizontal: true, noLegend: true });
  }

  /* ── View: Alerts Overview ─────────────────────────────────────── */
  function renderAlerts() {
    var at = data.alert_trend || [];
    var totalAlerts = data.alert_count || sum(at, 'alerts');
    var avgPerDay = at.length ? totalAlerts / at.length : 0;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128276;', C.redA, fmtK(totalAlerts), 'Total Alerts', trendHTML(trendPct(at, 'alerts'), true));
    html += kpiCard('&#128200;', C.amberA, fmt(avgPerDay, 1), 'Avg Alerts/Day', '');
    html += kpiCard('&#128197;', C.blueA, fmtInt(at.length), 'Days Tracked', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-alerts-trend', 'Daily Alerts', 'Alert volume over time', true);
    html += '</div>';

    var th = '<th>Date</th><th class="num">Alerts</th>';
    var tb = at.map(function (r) {
      return '<tr><td>' + shortDate(r.event_date) + '</td><td class="num">' + fmtInt(r.alerts) + '</td></tr>';
    }).join('');
    html += tableWrap('Alert Trend Detail', th, tb);

    el('rpt-content').innerHTML = html;

    lineChart('ch-alerts-trend', at.map(function (r) { return shortDate(r.event_date); }), [
      { label: 'Alerts', data: at.map(function (r) { return r.alerts; }), borderColor: C.red, backgroundColor: C.redA, fill: true }
    ], { yLabel: 'Alerts' });
  }

  /* ── View: Exceptions ──────────────────────────────────────────── */
  function renderExceptions() {
    var at = data.alert_trend || [];
    var totalAlerts = sum(at, 'alerts');
    var peakDay = at.reduce(function (best, r) { return (r.alerts || 0) > best.v ? { d: r.event_date, v: r.alerts } : best; }, { d: '—', v: 0 });

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128276;', C.redA, fmtK(totalAlerts), 'Total Alerts', '');
    html += kpiCard('&#128293;', C.amberA, shortDate(peakDay.d) + ' (' + fmtInt(peakDay.v) + ')', 'Peak Day', '');
    html += kpiCard('&#128197;', C.blueA, fmtInt(at.length), 'Days Tracked', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-exc-bar', 'Alert Volume by Day', 'Exception counts', true);
    html += '</div>';

    var sorted = at.slice().sort(function (a, b) { return (b.alerts || 0) - (a.alerts || 0); });
    var th = '<th>Date</th><th class="num">Alerts</th>';
    var tb = sorted.map(function (r) {
      return '<tr><td>' + shortDate(r.event_date) + '</td><td class="num">' + fmtInt(r.alerts) + '</td></tr>';
    }).join('');
    html += tableWrap('Alerts Sorted by Volume (DESC)', th, tb);

    el('rpt-content').innerHTML = html;

    barChart('ch-exc-bar', at.map(function (r) { return shortDate(r.event_date); }), [
      { label: 'Alerts', data: at.map(function (r) { return r.alerts; }), backgroundColor: C.amber, borderRadius: 3 }
    ], { noLegend: true });
  }

  /* ── View: Incidents ───────────────────────────────────────────── */
  function renderIncidents() {
    var at = data.alert_trend || [];
    var totalIncidents = sum(at, 'alerts');
    var peakDay = at.reduce(function (mx, r) { return (r.alerts || 0) > (mx.alerts || 0) ? r : mx; }, { alerts: 0 });
    var assetsWithIncidents = (data.asset_by_vehicle || []).filter(function (v) { return (v.open_issues || 0) > 0; }).length;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128680;', C.redA, fmtK(totalIncidents), 'Total Incidents', trendHTML(trendPct(at, 'alerts'), true));
    html += kpiCard('&#128666;', C.amberA, fmtInt(assetsWithIncidents), 'Assets with Incidents', '');
    html += kpiCard('&#128197;', C.purpleA, fmtInt(at.length), 'Days Tracked', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-inc-trend', 'Daily Incidents', 'Incident volume over time', true);
    html += '</div>';

    var th = '<th>Date</th><th class="num">Incidents</th>';
    var tb = at.map(function (r) {
      return '<tr><td>' + shortDate(r.event_date) + '</td><td class="num">' + fmtInt(r.alerts) + '</td></tr>';
    }).join('');
    html += tableWrap('Incident Trend', th, tb);

    el('rpt-content').innerHTML = html;

    lineChart('ch-inc-trend', at.map(function (r) { return shortDate(r.event_date); }), [
      { label: 'Incidents', data: at.map(function (r) { return r.alerts; }), borderColor: C.pink, backgroundColor: C.pinkA, fill: true }
    ], { yLabel: 'Incidents' });
  }

  /* ── View: Resolution Tracking ─────────────────────────────────── */
  function renderResolution() {
    var at = data.alert_trend || [];
    var totalAlerts = sum(at, 'alerts');
    var trendDir = trendPct(at, 'alerts');

    var cumulative = [];
    var running = 0;
    at.forEach(function (r) { running += (r.alerts || 0); cumulative.push(running); });

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128276;', C.redA, fmtK(totalAlerts), 'Total Alerts', trendHTML(trendDir, true));
    html += kpiCard('&#128200;', trendDir && trendDir < 0 ? C.greenA : C.amberA, trendDir != null ? fmt(Math.abs(trendDir), 1) + '%' : '—', 'Trend Direction', '');
    html += kpiCard('&#128197;', C.blueA, fmtInt(at.length), 'Days Tracked', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-res-cum', 'Cumulative Alerts', 'Running total over time', true);
    html += '</div>';

    var th = '<th>Date</th><th class="num">Daily Alerts</th><th class="num">Cumulative</th>';
    var run2 = 0;
    var tb = at.map(function (r) {
      run2 += (r.alerts || 0);
      return '<tr><td>' + shortDate(r.event_date) + '</td><td class="num">' + fmtInt(r.alerts) + '</td><td class="num">' + fmtK(run2) + '</td></tr>';
    }).join('');
    html += tableWrap('Alert Resolution Timeline', th, tb);

    el('rpt-content').innerHTML = html;

    lineChart('ch-res-cum', at.map(function (r) { return shortDate(r.event_date); }), [
      { label: 'Cumulative Alerts', data: cumulative, borderColor: C.indigo, backgroundColor: C.indigoA, fill: true }
    ], { yLabel: 'Total Alerts' });
  }

  /* ── View: Sensor Monitor ──────────────────────────────────────── */
  function renderSensorMonitor() {
    var dt = data.door_trend || [];
    var cbs = data.cold_by_sensor || [];
    var activeSensors = cbs.length || dt.length;
    var totalDoorEvents = sum(dt, 'door_events');

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128225;', C.cyanA, fmtInt(activeSensors), 'Active Sensors', '');
    html += kpiCard('&#128682;', C.amberA, fmtK(totalDoorEvents), 'Total Door Events', trendHTML(trendPct(dt, 'door_events')));
    html += kpiCard('&#128197;', C.blueA, fmtInt(dt.length), 'Days Monitored', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-sensor-door', 'Daily Door Events', 'Sensor activity over time', true);
    html += '</div>';

    var th = '<th>Date</th><th class="num">Door Events</th>';
    var tb = dt.map(function (r) {
      return '<tr><td>' + shortDate(r.event_date) + '</td><td class="num">' + fmtInt(r.door_events) + '</td></tr>';
    }).join('');
    html += tableWrap('Sensor Event Trend', th, tb);

    el('rpt-content').innerHTML = html;

    lineChart('ch-sensor-door', dt.map(function (r) { return shortDate(r.event_date); }), [
      { label: 'Door Events', data: dt.map(function (r) { return r.door_events; }), borderColor: C.cyan, backgroundColor: C.cyanA, fill: true }
    ], { yLabel: 'Events' });
  }

  /* ── View: Industrial ──────────────────────────────────────────── */
  function renderIndustrial() {
    var it = data.industrial_trend || [];
    var totalValue = sum(it, 'total_value');
    var ids = {};
    it.forEach(function (r) { if (r.asset_id) ids[r.asset_id] = true; });
    var activeAssets = Object.keys(ids).length || it.length;
    var avgPerDay = it.length ? totalValue / it.length : 0;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#127981;', C.indigoA, '$' + fmtK(totalValue), 'Total Value', trendHTML(trendPct(it, 'total_value')));
    html += kpiCard('&#128736;', C.blueA, fmtInt(activeAssets), 'Active Assets', '');
    html += kpiCard('&#128200;', C.greenA, '$' + fmtK(avgPerDay), 'Avg Value/Day', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-ind-value', 'Daily Total Value', 'Industrial asset output', true);
    html += '</div>';

    var th = '<th>Date</th><th class="num">Total Value</th>';
    var tb = it.map(function (r) {
      return '<tr><td>' + shortDate(r.event_date) + '</td><td class="num">$' + fmtK(r.total_value) + '</td></tr>';
    }).join('');
    html += tableWrap('Industrial Trend Detail', th, tb);

    el('rpt-content').innerHTML = html;

    lineChart('ch-ind-value', it.map(function (r) { return shortDate(r.event_date); }), [
      { label: 'Total Value ($)', data: it.map(function (r) { return r.total_value; }), borderColor: C.indigo, backgroundColor: C.indigoA, fill: true }
    ], { yLabel: 'Value ($)' });
  }

  /* ── View: Facility Sensor ─────────────────────────────────────── */
  function renderFacilitySensor() {
    var dt = data.door_trend || [];
    var it = data.industrial_trend || [];
    var totalDoor = sum(dt, 'door_events');
    var totalValue = sum(it, 'total_value');
    var totalDays = Math.max(dt.length, it.length);

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128682;', C.amberA, fmtK(totalDoor), 'Total Door Events', '');
    html += kpiCard('&#127981;', C.indigoA, '$' + fmtK(totalValue), 'Industrial Value', '');
    html += kpiCard('&#128197;', C.blueA, fmtInt(totalDays), 'Days Monitored', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-fac-door', 'Door Events', 'Facility access activity');
    html += chartCard('ch-fac-ind', 'Industrial Value', 'Asset output trend');
    html += '</div>';

    var th = '<th>Date</th><th class="num">Door Events</th><th class="num">Industrial Value</th>';
    var maxLen = Math.max(dt.length, it.length);
    var tb = '';
    for (var i = 0; i < Math.min(maxLen, 50); i++) {
      var d = dt[i] || {};
      var ind = it[i] || {};
      var date = d.event_date || ind.event_date || '';
      tb += '<tr><td>' + shortDate(date) + '</td><td class="num">' + fmtInt(d.door_events) + '</td><td class="num">$' + fmtK(ind.total_value) + '</td></tr>';
    }
    html += tableWrap('Facility Sensor Combined', th, tb);

    el('rpt-content').innerHTML = html;

    lineChart('ch-fac-door', dt.map(function (r) { return shortDate(r.event_date); }), [
      { label: 'Door Events', data: dt.map(function (r) { return r.door_events; }), borderColor: C.amber, backgroundColor: C.amberA, fill: true }
    ], { yLabel: 'Events' });

    lineChart('ch-fac-ind', it.map(function (r) { return shortDate(r.event_date); }), [
      { label: 'Value ($)', data: it.map(function (r) { return r.total_value; }), borderColor: C.indigo, backgroundColor: C.indigoA, fill: true }
    ], { yLabel: 'Value ($)' });
  }

  /* ── View: Fleet Reference ─────────────────────────────────────── */
  function renderFleetRef() {
    var vf = data.vehicle_fleet || [];
    var ids = {};
    vf.forEach(function (r) { ids[r.vehicle_id] = true; });
    var totalVehicles = Object.keys(ids).length;

    var yearBuckets = {};
    vf.forEach(function (r) {
      var y = r.year || 'Unknown';
      yearBuckets[y] = (yearBuckets[y] || 0) + 1;
    });
    var yearLabels = Object.keys(yearBuckets).sort();
    var yearValues = yearLabels.map(function (y) { return yearBuckets[y]; });
    var yearColors = [C.indigo, C.blue, C.green, C.amber, C.red, C.purple, C.cyan, C.pink];

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128666;', C.indigoA, fmtInt(totalVehicles), 'Vehicle Count', '');
    html += kpiCard('&#128197;', C.blueA, fmtInt(yearLabels.length), 'Year Distribution', '');
    html += '</div>';

    html += '<div class="rpt-chart-grid">';
    html += chartCard('ch-fref-year', 'Vehicles by Year', 'Fleet age breakdown');
    html += '</div>';

    var th = '<th>Vehicle ID</th><th>Year</th><th>Make</th><th>Model</th>';
    var tb = vf.map(function (r) {
      return '<tr><td>' + (r.vehicle_id || '') + '</td><td>' + (r.year || '') + '</td><td>' +
        (r.make || '') + '</td><td>' + (r.model || '') + '</td></tr>';
    }).join('');
    html += tableWrap('Fleet Reference', th, tb);

    el('rpt-content').innerHTML = html;

    doughnutChart('ch-fref-year', yearLabels, yearValues,
      yearLabels.map(function (_, i) { return yearColors[i % yearColors.length]; }));
  }

  /* ── View: Coverage ────────────────────────────────────────────── */
  function renderCoverage() {
    var vf = data.vehicle_fleet || [];
    var sd = data.safety_by_driver || [];
    var cbs = data.cold_by_sensor || [];
    var vIds = {};
    vf.forEach(function (r) { vIds[r.vehicle_id] = true; });
    var vehicleCount = Object.keys(vIds).length;
    var driverCount = sd.length;
    var sensorCount = cbs.length || (data.door_trend || []).length;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128666;', C.indigoA, fmtInt(vehicleCount), 'Vehicles', '');
    html += kpiCard('&#128100;', C.blueA, fmtInt(driverCount), 'Drivers', '');
    html += kpiCard('&#128225;', C.cyanA, fmtInt(sensorCount), 'Sensors', '');
    html += '</div>';

    var th = '<th>Category</th><th class="num">Count</th>';
    var tb = '<tr><td>Vehicles</td><td class="num">' + fmtInt(vehicleCount) + '</td></tr>' +
      '<tr><td>Drivers</td><td class="num">' + fmtInt(driverCount) + '</td></tr>' +
      '<tr><td>Cold Chain Sensors</td><td class="num">' + fmtInt(cbs.length) + '</td></tr>' +
      '<tr><td>Door Sensors</td><td class="num">' + fmtInt((data.door_trend || []).length) + '</td></tr>' +
      '<tr><td>Equipment</td><td class="num">' + fmtInt((data.equipment_stats || []).length) + '</td></tr>' +
      '<tr><td>Trailers</td><td class="num">' + fmtInt((data.trailer_stats || []).length) + '</td></tr>';
    html += tableWrap('Coverage Summary', th, tb);

    el('rpt-content').innerHTML = html;
  }

  /* ── View: Documentation Coverage ──────────────────────────────── */
  function renderDocCoverage() {
    var vf = data.vehicle_fleet || [];
    var ids = {};
    vf.forEach(function (r) { ids[r.vehicle_id] = true; });
    var vehicleCount = Object.keys(ids).length;
    var withYear = vf.filter(function (r) { return r.year && r.year > 0; }).length;
    var withData = vf.filter(function (r) { return r.row_count && r.row_count > 0; }).length;

    var html = '<div class="rpt-kpi-grid">';
    html += kpiCard('&#128666;', C.indigoA, fmtInt(vehicleCount), 'Total Vehicles', '');
    html += kpiCard('&#128197;', C.blueA, fmtInt(withYear), 'With Year Data', '');
    html += kpiCard('&#128203;', withData === vf.length ? C.greenA : C.amberA, fmtInt(withData), 'With Activity Data', '');
    html += kpiCard('&#128276;', C.redA, fmtInt(vf.length - withData), 'Missing Data', '');
    html += '</div>';

    var th = '<th>Vehicle ID</th><th class="num">Year</th><th>Date</th><th class="num">Records</th><th>Status</th>';
    var tb = vf.map(function (r) {
      var complete = r.year && r.row_count > 0;
      var cls = complete ? 'color:' + C.green : 'color:' + C.red;
      return '<tr><td>' + (r.vehicle_id || '') + '</td><td class="num">' + (r.year || '—') +
        '</td><td>' + (r.event_date ? shortDate(r.event_date) : '—') +
        '</td><td class="num">' + fmtInt(r.row_count) +
        '</td><td style="' + cls + '">' + (complete ? 'Complete' : 'Incomplete') + '</td></tr>';
    }).join('');
    html += tableWrap('Fleet Documentation Status', th, tb);

    el('rpt-content').innerHTML = html;
  }

  /* ── View Router ────────────────────────────────────────────────── */
  var views = {
    'executive':          { title: 'Executive Summary',          render: renderExecutive },
    'fleet-util':         { title: 'Fleet Utilization',          render: renderFleetUtil },
    'vehicle-activity':   { title: 'Vehicle Activity',           render: renderVehicleActivity },
    'route-perf':         { title: 'Route Performance',          render: renderRoutePerf },
    'dispatch':           { title: 'Dispatch Throughput',        render: renderDispatch },
    'vehicle-master':     { title: 'Vehicle Master Overview',    render: renderVehicleMaster },
    'fuel-eff':           { title: 'Fuel Efficiency',            render: renderFuelEff },
    'idling':             { title: 'Idling Analysis',            render: renderIdling },
    'fuel-cost':          { title: 'Fuel Cost Opportunity',      render: renderFuelCost },
    'emissions':          { title: 'Emissions',                  render: renderEmissions },
    'safety-dash':        { title: 'Driver Safety Dashboard',    render: renderSafetyDash },
    'driver-scorecard':   { title: 'Driver Scorecard',           render: renderDriverScorecard },
    'coaching':           { title: 'Coaching Priority',          render: renderCoaching },
    'safety-trend':       { title: 'Safety Trend',               render: renderSafetyTrend },
    'hos':                { title: 'HOS Compliance',             render: renderHOS },
    'ifta':               { title: 'IFTA Summary',               render: renderIFTA },
    'driver-compliance':  { title: 'Driver Compliance',          render: renderDriverCompliance },
    'compliance-trend':   { title: 'Compliance Trend',           render: renderComplianceTrend },
    'cold-chain':         { title: 'Cold Chain Compliance',      render: renderColdChain },
    'temp-excursion':     { title: 'Temperature Excursion',      render: renderTempExcursion },
    'reefer':             { title: 'Reefer Performance',         render: renderReefer },
    'door-monitor':       { title: 'Door Monitoring',            render: renderDoorMonitor },
    'asset-health':       { title: 'Asset Health',               render: renderAssetHealth },
    'maint-queue':        { title: 'Maintenance Queue',          render: renderMaintQueue },
    'dvir':               { title: 'DVIR Dashboard',             render: renderDVIR },
    'trailer-health':     { title: 'Trailer Health',             render: renderTrailerHealth },
    'equip-health':       { title: 'Equipment Health',           render: renderEquipHealth },
    'alerts':             { title: 'Alerts Dashboard',           render: renderAlerts },
    'exceptions':         { title: 'Exception Management',       render: renderExceptions },
    'incidents':          { title: 'Operational Incidents',       render: renderIncidents },
    'resolution':         { title: 'Resolution Performance',     render: renderResolution },
    'sensor-monitor':     { title: 'Sensor Monitoring',          render: renderSensorMonitor },
    'industrial':         { title: 'Industrial Telemetry',       render: renderIndustrial },
    'facility-sensor':    { title: 'Facility Sensor Report',     render: renderFacilitySensor },
    'fleet-ref':          { title: 'Fleet Reference',            render: renderFleetRef },
    'coverage':           { title: 'Coverage Report',            render: renderCoverage },
    'doc-coverage':       { title: 'Document Coverage',          render: renderDocCoverage }
  };

  function switchView(view) {
    if (!views[view]) return;
    activeView = view;
    destroyCharts();
    el('rpt-title').textContent = views[view].title;
    $$('.rpt-nav-item').forEach(function (a) {
      a.classList.toggle('active', a.getAttribute('data-view') === view);
    });
    views[view].render();
    el('rpt-content').scrollTop = 0;
  }

  /* ── Navigation ─────────────────────────────────────────────────── */
  function setupNav() {
    $$('.rpt-nav-item').forEach(function (a) {
      a.addEventListener('click', function (e) {
        e.preventDefault();
        switchView(this.getAttribute('data-view'));
      });
    });

    /* Collapsible nav sections */
    $$('.rpt-nav-section').forEach(function (sec) {
      sec.addEventListener('click', function () {
        var cat = this.getAttribute('data-cat');
        this.classList.toggle('collapsed');
        var group = document.querySelector('.rpt-nav-group[data-cat="' + cat + '"]');
        if (group) group.classList.toggle('collapsed');
      });
    });
  }

  /* ── Date Range ─────────────────────────────────────────────────── */
  /* ── Time Picker (Splunk-style) ───────────────────────────────── */
  var timeRange = { label: 'Last 90 days', from: null, to: null };

  function setDateRange() {
    var ft = data.fuel_trend || [];
    if (ft.length < 2) return;
    var first = ft[0].event_date;
    var last = ft[ft.length - 1].event_date;
    /* Set default custom inputs */
    var fromInput = el('rpt-time-from');
    var toInput = el('rpt-time-to');
    if (fromInput && first) fromInput.value = first + 'T00:00';
    if (toInput && last) toInput.value = last + 'T23:59';
  }

  function setupTimePicker() {
    var btn = el('rpt-time-btn');
    var dropdown = el('rpt-time-dropdown');
    if (!btn || !dropdown) return;

    /* Toggle dropdown */
    btn.addEventListener('click', function (e) {
      e.stopPropagation();
      dropdown.classList.toggle('open');
    });
    document.addEventListener('click', function () {
      dropdown.classList.remove('open');
    });
    dropdown.addEventListener('click', function (e) { e.stopPropagation(); });

    /* Preset options */
    $$('.rpt-time-opt').forEach(function (opt) {
      opt.addEventListener('click', function () {
        $$('.rpt-time-opt').forEach(function (o) { o.classList.remove('active'); });
        opt.classList.add('active');
        var range = opt.getAttribute('data-range');
        var label = opt.textContent;
        el('rpt-time-label').textContent = label;
        timeRange.label = label;
        timeRange.from = computeRangeStart(range);
        timeRange.to = new Date().toISOString().slice(0, 10);
        dropdown.classList.remove('open');
        /* Re-render current view with new range */
        if (views[activeView]) views[activeView].render();
      });
    });

    /* Custom apply */
    var applyBtn = el('rpt-time-apply');
    if (applyBtn) {
      applyBtn.addEventListener('click', function () {
        var from = el('rpt-time-from').value;
        var to = el('rpt-time-to').value;
        if (from && to) {
          $$('.rpt-time-opt').forEach(function (o) { o.classList.remove('active'); });
          var fromShort = shortDate(from.slice(0, 10));
          var toShort = shortDate(to.slice(0, 10));
          var label = fromShort + ' – ' + toShort;
          el('rpt-time-label').textContent = label;
          timeRange.label = label;
          timeRange.from = from.slice(0, 10);
          timeRange.to = to.slice(0, 10);
          dropdown.classList.remove('open');
          if (views[activeView]) views[activeView].render();
        }
      });
    }
  }

  function computeRangeStart(range) {
    var now = new Date();
    switch (range) {
      case '1h':  return new Date(now - 3600000).toISOString().slice(0, 10);
      case '4h':  return new Date(now - 4 * 3600000).toISOString().slice(0, 10);
      case '12h': return new Date(now - 12 * 3600000).toISOString().slice(0, 10);
      case '24h': return new Date(now - 86400000).toISOString().slice(0, 10);
      case '7d':  return new Date(now - 7 * 86400000).toISOString().slice(0, 10);
      case '30d': return new Date(now - 30 * 86400000).toISOString().slice(0, 10);
      case '90d': return new Date(now - 90 * 86400000).toISOString().slice(0, 10);
      case 'ytd': return now.getFullYear() + '-01-01';
      case 'all': return '2020-01-01';
      default:    return new Date(now - 90 * 86400000).toISOString().slice(0, 10);
    }
  }

  /* Filter array by event_date within timeRange */
  function filterByTime(arr) {
    if (!timeRange.from || !arr) return arr || [];
    var from = timeRange.from;
    var to = timeRange.to || '9999-12-31';
    return arr.filter(function (r) {
      var d = r.event_date;
      return d && d >= from && d <= to;
    });
  }

  /* ══════════════════════════════════════════════════════════════════
     AI Chat — NL → SQL → Dynamic Report
     ══════════════════════════════════════════════════════════════════ */

  var aiPanel, aiMessages, aiInput, aiSend, aiSuggestions, rptConnSelect, rptRefreshSchema, rptSchemaMeta, rptSchemaBadges;
  var rptScopeList, rptScopeSummary, rptScopeSuggest, rptScopeAll, rptScopeClear, rptScopeEmpty;
  var rptSavedList, rptSavedEmpty, rptSavedSummary;
  var rptTrainSection, rptTrainBtn, rptTrainStatus;
  var aiChartInstance = null;
  var lastAIResult = null;
  var lastAIIntent = '';
  var activeChartPreference = 'auto';
  var savedReports = [];
  var activeReportingConnId = '';
  var activeSchemaInfo = null;
  var activeScopeTables = [];
  var REPORTING_DB_TYPES = {
    postgresql: true, mysql: true, oracle: true, sqlserver: true,
    db2: true, snowflake: true, databricks: true
  };

  function setupAI() {
    aiPanel = el('rpt-ai-panel');
    aiMessages = el('rpt-ai-messages');
    aiInput = el('rpt-ai-input');
    aiSend = el('rpt-ai-send');
    aiSuggestions = el('rpt-ai-suggestions');
    rptConnSelect = el('rpt-conn-select');
    rptRefreshSchema = el('rpt-refresh-schema');
    rptSchemaMeta = el('rpt-schema-meta');
    rptSchemaBadges = el('rpt-schema-badges');
    rptScopeList = el('rpt-scope-list');
    rptScopeSummary = el('rpt-scope-summary');
    rptScopeSuggest = el('rpt-scope-suggest');
    rptScopeAll = el('rpt-scope-all');
    rptScopeClear = el('rpt-scope-clear');
    rptScopeEmpty = el('rpt-scope-empty');
    rptSavedList = el('rpt-saved-list');
    rptSavedEmpty = el('rpt-saved-empty');
    rptSavedSummary = el('rpt-saved-summary');
    rptTrainSection = el('rpt-train-section');
    rptTrainBtn = el('rpt-train-btn');
    rptTrainStatus = el('rpt-train-status');

    var toggleBtn = el('rpt-ai-toggle');
    if (toggleBtn) {
      toggleBtn.addEventListener('click', function () {
        aiPanel.classList.toggle('open');
      });
    }
    var closeBtn = el('rpt-ai-close');
    if (closeBtn) {
      closeBtn.addEventListener('click', function () {
        aiPanel.classList.remove('open');
      });
    }

    aiSend.addEventListener('click', function () { aiAsk(); });
    aiInput.addEventListener('keydown', function (e) {
      if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); aiAsk(); }
    });

    if (rptConnSelect) {
      rptConnSelect.addEventListener('change', function () {
        activeReportingConnId = this.value || '';
        loadReportingSchema(false);
        loadSavedReports();
      });
    }
    if (rptRefreshSchema) {
      rptRefreshSchema.addEventListener('click', function () {
        loadReportingSchema(true);
      });
    }
    if (rptTrainBtn) {
      rptTrainBtn.addEventListener('click', function () { trainSchema(); });
    }
    if (rptScopeSuggest) rptScopeSuggest.addEventListener('click', function () { applySuggestedScope(); });
    if (rptScopeAll) rptScopeAll.addEventListener('click', function () {
      var tables = (activeSchemaInfo && activeSchemaInfo.tables) || [];
      if (tables.length > 50) {
        window.alert('Schema has ' + tables.length + ' tables. AI reporting supports up to 50 at a time. Use "Suggested" or select tables manually.');
        return;
      }
      setScopeTables(tables);
    });
    if (rptScopeClear) rptScopeClear.addEventListener('click', function () { setScopeTables([]); });

    /* Suggestion chips */
    $$('.rpt-ai-chip').forEach(function (chip) {
      chip.addEventListener('click', function () {
        aiInput.value = this.getAttribute('data-q');
        aiAsk();
      });
    });

    loadReportingConnections();
  }

  function aiAddMsg(cls, html) {
    var div = document.createElement('div');
    div.className = 'rpt-ai-msg ' + cls;
    div.innerHTML = html;
    aiMessages.appendChild(div);
    aiMessages.scrollTop = aiMessages.scrollHeight;
    return div;
  }

  function aiShowThinking() {
    var div = document.createElement('div');
    div.className = 'rpt-ai-thinking';
    div.id = 'rpt-ai-thinking';
    div.innerHTML = '<div class="dot-pulse"><span></span><span></span><span></span></div> Analyzing your question...';
    aiMessages.appendChild(div);
    aiMessages.scrollTop = aiMessages.scrollHeight;
  }

  function aiHideThinking() {
    var t = el('rpt-ai-thinking');
    if (t) t.remove();
  }

  function escapeHtml(s) {
    var d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  }

  function inferChartState(columns, rows) {
    var dateCol = null, catCol = null, numCols = [];
    (columns || []).forEach(function (c) {
      var sample = rows && rows.length ? rows[0][c] : null;
      if (/date|period/i.test(c) && typeof sample === 'string') dateCol = c;
      else if (typeof sample === 'number') numCols.push(c);
      else if (typeof sample === 'string' && !catCol) catCol = c;
    });
    var labelCol = dateCol || catCol;
    return {
      dateCol: dateCol,
      categoryCol: catCol,
      numericCols: numCols,
      labelCol: labelCol,
      isChartable: Boolean(labelCol && numCols.length > 0 && rows && rows.length >= 2)
    };
  }

  function normalizeChartPreference(value) {
    var v = String(value || 'auto').toLowerCase();
    if (v === 'line' || v === 'bar' || v === 'doughnut' || v === 'table' || v === 'auto') return v;
    if (v === 'pie' || v === 'donut') return 'doughnut';
    return 'auto';
  }

  function preferredChartType(pref, chartState) {
    var preference = normalizeChartPreference(pref);
    if (preference === 'table') return 'table';
    if (!chartState || !chartState.isChartable) return 'table';
    if (preference !== 'auto') return preference;
    return /date|period/i.test(chartState.labelCol) ? 'line' : 'bar';
  }

  function exportRowsToCsv(filename, columns, rows) {
    var cols = columns || [];
    var dataRows = rows || [];
    function esc(v) {
      var s = v == null ? '' : String(v);
      if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
      return s;
    }
    var csv = [cols.map(esc).join(',')];
    dataRows.forEach(function (row) {
      csv.push(cols.map(function (col) { return esc(row[col]); }).join(','));
    });
    var blob = new Blob([csv.join('\n')], { type: 'text/csv;charset=utf-8' });
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a');
    a.href = url;
    a.download = filename || 'bitool-report.csv';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  function parseChartCommand(text) {
    var lower = String(text || '').toLowerCase();
    if (!lower) return null;
    if (/(make|show|switch|turn|render).*\bbar (chart|graph)\b|\bbar (chart|graph)\b/.test(lower)) return 'bar';
    if (/(make|show|switch|turn|render).*\bline (chart|graph)\b|\bline (chart|graph)\b/.test(lower)) return 'line';
    if (/(make|show|switch|turn|render).*\b(pie|donut|doughnut) chart\b|\bpie chart\b|\bdonut chart\b|\bdoughnut chart\b/.test(lower)) return 'doughnut';
    if (/(show|switch|turn|render).*(table)|\btable only\b|\bshow table\b/.test(lower)) return 'table';
    if (/(auto chart|auto visualize|automatic chart|best chart)/.test(lower)) return 'auto';
    return null;
  }

  function handleLocalChartCommand(q) {
    var nextPref = parseChartCommand(q);
    if (!nextPref || !lastAIResult) return false;
    activeChartPreference = nextPref;
    var chartLabel = nextPref === 'auto' ? 'automatic chart selection' : (nextPref === 'doughnut' ? 'doughnut chart' : nextPref + ' chart');
    aiAddMsg('ai', 'Updated the current report to <b>' + escapeHtml(chartLabel) + '</b>.');
    renderAIReport(lastAIResult, lastAIIntent || ((lastAIResult.isl && lastAIResult.isl.intent) || 'AI Generated Report'));
    return true;
  }

  function reportingDbLabel(conn) {
    var dbtype = conn && conn.dbtype ? String(conn.dbtype).toUpperCase() : 'DB';
    return (conn.label || ('Connection ' + conn.conn_id)) + ' (' + dbtype + ')';
  }

  function initialAIMessagesHtml() {
    return '<div class="rpt-ai-msg ai">' +
      'Ask me anything about the selected dataset. I use an ISL pipeline to safely translate your question into a validated query — no hallucination, no raw SQL generation. Results open as a full report in the dashboard.' +
      '</div>';
  }

  function resetAIConversation() {
    if (!aiMessages) return;
    aiMessages.innerHTML = initialAIMessagesHtml();
  }

  function defaultScopeTables(info) {
    if (!info || !info.tables) return [];
    if (info.connection == null) return [];
    var tables = info.tables.slice();
    if (tables.length <= 20) return tables;
    var scored = tables.filter(function (tbl) {
      var cols = (info.columns && info.columns[tbl]) || [];
      return cols.some(function (c) { return c.type === 'date' || c.type === 'timestamp'; });
    });
    var chosen = (scored.length ? scored : tables).slice(0, Math.min(20, tables.length));
    return chosen;
  }

  function updateScopeSummary() {
    if (!rptScopeSummary || !activeSchemaInfo) return;
    var total = (activeSchemaInfo.tables || []).length;
    if (!activeSchemaInfo.connection) {
      rptScopeSummary.textContent = 'Demo tables';
      return;
    }
    if (!activeScopeTables.length) {
      rptScopeSummary.textContent = total > 50 ? 'Pick tables' : 'All tables';
      return;
    }
    rptScopeSummary.textContent = activeScopeTables.length + ' of ' + total + ' tables';
  }

  function renderScopeList() {
    if (!rptScopeList || !rptScopeEmpty) return;
    var info = activeSchemaInfo;
    var tables = (info && info.tables) || [];
    rptScopeList.innerHTML = '';
    if (!tables.length || !info.connection) {
      rptScopeEmpty.style.display = info && info.connection ? 'block' : 'none';
      updateScopeSummary();
      return;
    }
    rptScopeEmpty.style.display = 'none';
    tables.forEach(function (tbl) {
      var label = document.createElement('label');
      label.className = 'rpt-ai-scope-item';
      var checkbox = document.createElement('input');
      checkbox.type = 'checkbox';
      checkbox.checked = activeScopeTables.indexOf(tbl) >= 0;
      checkbox.addEventListener('change', function () {
        var next = activeScopeTables.slice();
        var idx = next.indexOf(tbl);
        if (checkbox.checked && idx < 0) next.push(tbl);
        if (!checkbox.checked && idx >= 0) next.splice(idx, 1);
        setScopeTables(next);
      });
      var cols = (info.columns && info.columns[tbl]) || [];
      var meta = document.createElement('span');
      meta.textContent = tbl + ' (' + cols.length + ' cols)';
      label.appendChild(checkbox);
      label.appendChild(meta);
      rptScopeList.appendChild(label);
    });
    updateScopeSummary();
  }

  function setScopeTables(tables) {
    activeScopeTables = Array.from(new Set((tables || []).slice())).sort();
    renderScopeList();
    resetAIConversation();
  }

  function applySuggestedScope() {
    setScopeTables(defaultScopeTables(activeSchemaInfo));
  }

  function renderSchemaMeta(info, err) {
    if (!rptSchemaMeta) return;
    if (err) {
      rptSchemaMeta.textContent = err;
      return;
    }
    if (!info) {
      rptSchemaMeta.textContent = 'Schema details unavailable.';
      return;
    }
    if (!info.connection) {
      rptSchemaMeta.textContent = 'Using the built-in Sheetz fleet demo tables.';
      return;
    }
    var sampleTables = (info.tables || []).slice(0, 4).join(', ');
    rptSchemaMeta.textContent =
      info.table_count + ' tables available' +
      (activeScopeTables.length ? ' · scope ' + activeScopeTables.length + ' table(s)' : '') +
      (sampleTables ? ' — ' + sampleTables + ((info.tables || []).length > 4 ? ', ...' : '') : '') +
      '.';
  }

  function renderDetectedSourceBadges(info) {
    if (!rptSchemaBadges) return;
    rptSchemaBadges.innerHTML = '';
    var detected = (info && info.detected_sources) || [];
    if (!detected.length) {
      rptSchemaBadges.style.display = 'none';
      return;
    }
    detected.forEach(function (entry) {
      var badge = document.createElement('div');
      badge.className = 'rpt-ai-source-badge';
      var label = document.createElement('span');
      label.textContent = entry.source || entry.key || 'Detected';
      badge.appendChild(label);
      if (entry.confidence != null) {
        var conf = document.createElement('span');
        conf.className = 'rpt-ai-source-badge-sub';
        conf.textContent = Math.round(Number(entry.confidence || 0) * 100) + '%';
        badge.appendChild(conf);
      }
      rptSchemaBadges.appendChild(badge);
    });
    rptSchemaBadges.style.display = 'flex';
  }

  function updateSavedSummary() {
    if (!rptSavedSummary) return;
    rptSavedSummary.textContent = savedReports.length + ' report' + (savedReports.length === 1 ? '' : 's');
  }

  function defaultSavedReportName(result) {
    var intent = result && result.isl && result.isl.intent ? String(result.isl.intent) : 'AI Report';
    return intent.length > 80 ? intent.slice(0, 80) : intent;
  }

  function renderSavedReports() {
    if (!rptSavedList || !rptSavedEmpty) return;
    rptSavedList.innerHTML = '';
    updateSavedSummary();
    rptSavedEmpty.textContent = 'No saved reports yet.';
    if (!savedReports.length) {
      rptSavedEmpty.style.display = 'block';
      return;
    }
    rptSavedEmpty.style.display = 'none';
    savedReports.forEach(function (report) {
      var item = document.createElement('div');
      item.className = 'rpt-ai-saved-item';

      var title = document.createElement('div');
      title.className = 'rpt-ai-saved-title';
      title.textContent = report.name || ('Report ' + report.report_id);
      item.appendChild(title);

      var meta = document.createElement('div');
      meta.className = 'rpt-ai-saved-meta';
      meta.textContent = (report.scope_tables && report.scope_tables.length
        ? report.scope_tables.length + ' table scope'
        : 'All tables') +
        (report.schema_name ? ' · ' + report.schema_name : '');
      item.appendChild(meta);

      var actions = document.createElement('div');
      actions.className = 'rpt-ai-saved-actions';

      var runBtn = document.createElement('button');
      runBtn.type = 'button';
      runBtn.className = 'rpt-ai-saved-btn';
      runBtn.textContent = 'Run';
      runBtn.addEventListener('click', function () { runSavedReport(report.report_id); });
      actions.appendChild(runBtn);

      var deleteBtn = document.createElement('button');
      deleteBtn.type = 'button';
      deleteBtn.className = 'rpt-ai-saved-btn delete';
      deleteBtn.textContent = 'Delete';
      deleteBtn.addEventListener('click', function () { deleteSavedReport(report.report_id); });
      actions.appendChild(deleteBtn);

      item.appendChild(actions);
      rptSavedList.appendChild(item);
    });
  }

  async function loadSavedReports() {
    var url = '/api/reporting/saved';
    if (activeReportingConnId) {
      url += '?conn_id=' + encodeURIComponent(activeReportingConnId);
    }
    try {
      var resp = await fetch(url);
      var payload = await resp.json();
      if (!resp.ok || payload.error) throw new Error(payload.error || ('HTTP ' + resp.status));
      savedReports = payload.reports || [];
    } catch (e) {
      savedReports = [];
      if (rptSavedEmpty) rptSavedEmpty.textContent = 'Saved reports unavailable: ' + e.message;
    }
    renderSavedReports();
  }

  async function saveCurrentReport() {
    if (!lastAIResult || !lastAIResult.isl) {
      aiAddMsg('ai', '<span style="color:#dc2626">Run a report before saving it.</span>');
      return;
    }
    var suggestedName = defaultSavedReportName(lastAIResult);
    var name = window.prompt('Save report as:', suggestedName);
    if (name == null) return;
    name = name.trim();
    if (!name) return;
    try {
      var resp = await fetch('/api/reporting/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: name,
          conn_id: lastAIResult.connection || activeReportingConnId || undefined,
          schema_name: lastAIResult.schema || undefined,
          tables: lastAIResult.scope_tables && lastAIResult.scope_tables.length ? lastAIResult.scope_tables : undefined,
          isl: lastAIResult.isl
        })
      });
      var payload = await resp.json();
      if (!resp.ok || payload.error) throw new Error(payload.error || ('HTTP ' + resp.status));
      await loadSavedReports();
      aiAddMsg('ai', 'Saved report <b>' + escapeHtml(payload.report.name) + '</b>.');
    } catch (e) {
      aiAddMsg('ai', '<span style="color:#dc2626">Save failed: ' + escapeHtml(e.message) + '</span>');
    }
  }

  async function runSavedReport(reportId) {
    try {
      var resp = await fetch('/api/reporting/run-saved', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ report_id: reportId })
      });
      var d = await resp.json();
      if (!resp.ok || d.error) throw new Error(d.error || ('HTTP ' + resp.status));
      lastAIResult = d;
      lastAIIntent = (d.saved_report && d.saved_report.name) || (d.isl && d.isl.intent) || 'Saved Report';
      activeChartPreference = 'auto';
      renderAIReport(d, lastAIIntent);
      aiPanel.classList.remove('open');
    } catch (e) {
      aiAddMsg('ai', '<span style="color:#dc2626">Saved report failed: ' + escapeHtml(e.message) + '</span>');
    }
  }

  async function deleteSavedReport(reportId) {
    if (!window.confirm('Delete this saved report?')) return;
    try {
      var resp = await fetch('/api/reporting/saved/' + encodeURIComponent(reportId), { method: 'DELETE' });
      var payload = await resp.json();
      if (!resp.ok || payload.error) throw new Error(payload.error || ('HTTP ' + resp.status));
      await loadSavedReports();
    } catch (e) {
      aiAddMsg('ai', '<span style="color:#dc2626">Delete failed: ' + escapeHtml(e.message) + '</span>');
    }
  }

  function updateTrainUI(info) {
    if (!rptTrainSection) return;
    if (!info || !info.connection) {
      rptTrainSection.style.display = 'none';
      return;
    }
    rptTrainSection.style.display = 'flex';
    if (info.trained) {
      rptTrainBtn.classList.add('trained');
      rptTrainBtn.innerHTML = '&#10003; Trained';
      rptTrainStatus.textContent = info.context_count + ' description' + (info.context_count === 1 ? '' : 's') + ' loaded';
    } else {
      rptTrainBtn.classList.remove('trained');
      rptTrainBtn.innerHTML = '&#9889; Train AI';
      rptTrainStatus.textContent = 'Teach AI what your tables mean';
    }
  }

  async function trainSchema() {
    if (!activeReportingConnId) return;
    rptTrainBtn.disabled = true;
    rptTrainBtn.innerHTML = '&#8987; Training...';
    rptTrainStatus.textContent = 'Sampling data and generating descriptions...';
    try {
      var body = { conn_id: activeReportingConnId };
      if (activeScopeTables.length) body.tables = activeScopeTables;
      var resp = await fetch('/api/reporting/auto-train', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      var payload = await resp.json();
      if (!resp.ok || payload.error) throw new Error(payload.error || ('HTTP ' + resp.status));
      var count = (payload.trained || []).length;
      rptTrainBtn.disabled = false;
      rptTrainBtn.classList.add('trained');
      rptTrainBtn.innerHTML = '&#10003; Trained';
      rptTrainStatus.textContent = count + ' table' + (count === 1 ? '' : 's') + ' learned' +
        (payload.errors && payload.errors.length ? ' (' + payload.errors.length + ' skipped)' : '');
      /* Refresh schema info to pick up new context_count */
      await loadReportingSchema(false);
    } catch (e) {
      rptTrainBtn.disabled = false;
      rptTrainBtn.innerHTML = '&#9889; Train AI';
      rptTrainStatus.textContent = 'Training failed: ' + e.message;
    }
  }

  async function loadReportingConnections() {
    if (!rptConnSelect) return;
    try {
      var resp = await fetch('/listConnections');
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      var payload = await resp.json();
      var connections = (payload.connections || []).filter(function (conn) {
        return conn && conn.dbtype && REPORTING_DB_TYPES[String(conn.dbtype).toLowerCase()];
      });
      rptConnSelect.innerHTML = '<option value="">Default demo dataset</option>';
      connections.forEach(function (conn) {
        var opt = document.createElement('option');
        opt.value = conn.conn_id;
        opt.textContent = reportingDbLabel(conn);
        rptConnSelect.appendChild(opt);
      });
      await loadReportingSchema(false);
      await loadSavedReports();
    } catch (e) {
      renderSchemaMeta(null, 'Failed to load connections: ' + e.message);
    }
  }

  async function loadReportingSchema(forceRefresh) {
    var url = '/api/reporting/schema';
    var qs = [];
    activeReportingConnId = rptConnSelect && rptConnSelect.value ? rptConnSelect.value : '';
    if (activeReportingConnId) qs.push('conn_id=' + encodeURIComponent(activeReportingConnId));
    if (forceRefresh) qs.push('force_refresh=true');
    if (qs.length) url += '?' + qs.join('&');
    renderSchemaMeta(null, 'Loading schema...');
    try {
      var resp = await fetch(url);
      var info = await resp.json();
      if (!resp.ok || info.error) throw new Error(info.error || ('HTTP ' + resp.status));
      activeSchemaInfo = info;
      activeScopeTables = defaultScopeTables(info);
      renderScopeList();
      renderSchemaMeta(info);
      renderDetectedSourceBadges(info);
      updateTrainUI(info);
      resetAIConversation();
      /* Show demo nav only when no connection selected */
      showDemoNav(!info.connection);
      if (info.connection) {
        renderWelcome();
        el('rpt-title').textContent = info.connection_label || 'AI Reporting';
      }
    } catch (e) {
      activeSchemaInfo = null;
      activeScopeTables = [];
      renderScopeList();
      renderSchemaMeta(null, 'Schema unavailable: ' + e.message);
      renderDetectedSourceBadges(null);
      updateTrainUI(null);
    }
  }

  function aiResultTable(columns, rows) {
    if (!rows || !rows.length) return '<div style="color:#8b91a3;font-size:12px;padding:8px">No results found.</div>';
    var html = '<div class="rpt-ai-results"><table><thead><tr>';
    columns.forEach(function (c) { html += '<th>' + escapeHtml(c) + '</th>'; });
    html += '</tr></thead><tbody>';
    var limit = Math.min(rows.length, 50);
    for (var i = 0; i < limit; i++) {
      html += '<tr>';
      columns.forEach(function (c) {
        var v = rows[i][c];
        var display = v == null ? '' : typeof v === 'number' ? fmt(v, 2) : String(v);
        html += '<td>' + escapeHtml(display) + '</td>';
      });
      html += '</tr>';
    }
    html += '</tbody></table></div>';
    if (rows.length > limit) html += '<div style="font-size:11px;color:#8b91a3;padding:4px 0">Showing ' + limit + ' of ' + rows.length + ' rows</div>';
    return html;
  }

  function aiAutoChart(columns, rows, msgDiv) {
    /* Detect if results are chartable: need a category/date column + at least one numeric column */
    if (!rows || rows.length < 2 || rows.length > 100) return;

    var chartState = inferChartState(columns, rows);
    var labelCol = chartState.labelCol;
    var numCols = chartState.numericCols;
    if (!chartState.isChartable) return;

    /* Add chart button */
    var btnId = 'ai-chart-btn-' + Date.now();
    var btn = document.createElement('span');
    btn.className = 'rpt-ai-chart-btn';
    btn.innerHTML = '&#128200; Visualize as chart';
    btn.addEventListener('click', function () {
      renderAIChart(labelCol, numCols, rows, msgDiv);
      btn.remove();
    });
    msgDiv.appendChild(btn);
  }

  function renderAIChart(labelCol, numCols, rows, msgDiv) {
    var palette = [C.indigo, C.green, C.amber, C.red, C.cyan, C.purple, C.blue, C.pink];
    var labels = rows.map(function (r) {
      var v = r[labelCol];
      return /^\d{4}-\d{2}-\d{2}/.test(v) ? shortDate(v) : String(v);
    });

    var wrap = document.createElement('div');
    wrap.style.cssText = 'margin:10px 0;height:220px;position:relative';
    var canvas = document.createElement('canvas');
    wrap.appendChild(canvas);
    msgDiv.appendChild(wrap);
    aiMessages.scrollTop = aiMessages.scrollHeight;

    var isTimeSeries = /date|period/i.test(labelCol);
    var chartType = isTimeSeries ? 'line' : 'bar';

    var datasets = numCols.map(function (col, i) {
      var color = palette[i % palette.length];
      var ds = {
        label: col,
        data: rows.map(function (r) { return r[col]; }),
        borderColor: color,
        backgroundColor: isTimeSeries ? color.replace(')', ',0.12)').replace('rgb', 'rgba') : color,
        borderWidth: 2,
        borderRadius: isTimeSeries ? 0 : 4,
        tension: 0.4,
        fill: isTimeSeries,
        pointRadius: isTimeSeries ? 0 : undefined
      };
      return ds;
    });

    var opts = JSON.parse(JSON.stringify(baseLineOpts));
    opts.plugins.legend.labels.font = { size: 10, family: "'DM Sans'" };
    if (numCols.length === 1) opts.plugins.legend.display = false;

    if (aiChartInstance) { aiChartInstance.destroy(); aiChartInstance = null; }
    aiChartInstance = new Chart(canvas, { type: chartType, data: { labels: labels, datasets: datasets }, options: opts });
  }

  function drawMainReportChart(canvas, chartState, rows, preference) {
    if (!canvas || !chartState || !chartState.isChartable) return;
    var chartType = preferredChartType(preference, chartState);
    if (chartType === 'table') return;
    var palette = [C.indigo, C.green, C.amber, C.red, C.cyan, C.purple, C.blue, C.pink];
    if (chartType === 'doughnut') {
      var totals = chartState.numericCols.map(function (col) {
        return rows.reduce(function (sum, row) { return sum + (Number(row[col]) || 0); }, 0);
      });
      charts.push(new Chart(canvas, {
        type: 'doughnut',
        data: {
          labels: chartState.numericCols.map(function (col) { return col.replace(/_/g, ' '); }),
          datasets: [{ data: totals, backgroundColor: palette.slice(0, chartState.numericCols.length) }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { position: 'bottom' } }
        }
      }));
      return;
    }

    var isTimeSeries = chartType === 'line';
    var labels = rows.map(function (r) {
      var v = r[chartState.labelCol];
      return /^\d{4}-\d{2}-\d{2}/.test(v) ? shortDate(v) : String(v);
    });
    var datasets = chartState.numericCols.map(function (col, i) {
      var color = palette[i % palette.length];
      return {
        label: col.replace(/_/g, ' '),
        data: rows.map(function (r) { return r[col]; }),
        borderColor: color,
        backgroundColor: isTimeSeries ? color.replace(')', ',0.12)').replace('rgb', 'rgba') : color,
        borderWidth: 2,
        borderRadius: isTimeSeries ? 0 : 4,
        tension: 0.4,
        fill: isTimeSeries,
        pointRadius: isTimeSeries ? 2 : undefined
      };
    });
    var opts = JSON.parse(JSON.stringify(baseLineOpts));
    if (chartState.numericCols.length === 1) opts.plugins.legend.display = false;
    charts.push(new Chart(canvas, { type: chartType, data: { labels: labels, datasets: datasets }, options: opts }));
  }

  /* ── Render AI Report in Main Dashboard ────────────────────────── */
  function renderAIReport(d, intent) {
    destroyCharts();
    activeView = 'ai-report';
    /* Update nav — deselect all sidebar items */
    $$('.rpt-nav-item').forEach(function (a) { a.classList.remove('active'); });
    el('rpt-title').textContent = intent || 'AI Generated Report';

    var content = el('rpt-content');
    var html = '';
    var cols = d.columns || [];
    var rows = d.rows || [];
    var numCols = cols.filter(function (c) { return rows.length && typeof rows[0][c] === 'number'; });
    var chartState = inferChartState(cols, rows);
    var selectedChartType = preferredChartType(activeChartPreference, chartState);
    var selectedChartLabel = selectedChartType === 'auto' ? 'Auto' :
      (selectedChartType === 'doughnut' ? 'Doughnut' :
        (selectedChartType.charAt(0).toUpperCase() + selectedChartType.slice(1)));

    /* Pipeline + ISL header card */
    html += '<div class="rpt-card" style="margin-bottom:20px">';
    html += '<div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">';
    if (d.pipeline) {
      html += '<span style="display:inline-block;background:linear-gradient(135deg,#6366f1,#7c5cfc);color:#fff;font-size:11px;font-weight:600;padding:4px 14px;border-radius:16px;letter-spacing:0.5px">' + escapeHtml(d.pipeline) + '</span>';
    }
    if (d.connection_label) {
      html += '<span style="background:#ecfeff;color:#0f766e;padding:4px 12px;border-radius:12px;font-size:11px;font-weight:600">' + escapeHtml(d.connection_label) + '</span>';
    }
    if (d.saved_report && d.saved_report.name) {
      html += '<span style="background:#f5f3ff;color:#6d28d9;padding:4px 12px;border-radius:12px;font-size:11px;font-weight:600">' + escapeHtml(d.saved_report.name) + '</span>';
    }
    if (d.isl && d.isl.table) {
      html += '<span style="background:#eef2ff;color:#6366f1;padding:4px 12px;border-radius:12px;font-size:11px;font-weight:600">' + escapeHtml(d.isl.table) + '</span>';
    }
    html += '<span style="color:#6b7280;font-size:12px">' + (d.count || 0) + ' rows</span>';
    html += '<div class="rpt-report-toolbar">';
    html += '<span class="rpt-report-toolbar-label">View</span>';
    html += '<select id="ai-rpt-chart-type" class="rpt-report-toolbar-select">';
    html += '<option value="auto"' + (activeChartPreference === 'auto' ? ' selected' : '') + '>Auto</option>';
    html += '<option value="line"' + (activeChartPreference === 'line' ? ' selected' : '') + (chartState.isChartable ? '' : ' disabled') + '>Line</option>';
    html += '<option value="bar"' + (activeChartPreference === 'bar' ? ' selected' : '') + (chartState.isChartable ? '' : ' disabled') + '>Bar</option>';
    html += '<option value="doughnut"' + (activeChartPreference === 'doughnut' ? ' selected' : '') + (chartState.isChartable ? '' : ' disabled') + '>Doughnut</option>';
    html += '<option value="table"' + (activeChartPreference === 'table' ? ' selected' : '') + '>Table only</option>';
    html += '</select>';
    html += '<button type="button" id="ai-rpt-export-csv" class="rpt-report-toolbar-btn">Export CSV</button>';
    html += '<button type="button" id="ai-rpt-save-main" class="rpt-report-toolbar-btn">Save report</button>';
    html += '</div>';
    html += '</div>';
    if (chartState.isChartable) {
      html += '<div style="margin-top:10px;font-size:11px;color:#6b7280">Chart view: <b style="color:#312e81">' + escapeHtml(selectedChartLabel) + '</b></div>';
    }
    /* SQL */
    html += '<pre style="background:#1e1e2e;color:#a5b4fc;padding:12px 16px;border-radius:8px;font-size:11px;margin-top:12px;overflow-x:auto;white-space:pre-wrap;word-break:break-all">' + escapeHtml(d.sql) + '</pre>';
    /* ISL collapsible */
    if (d.isl) {
      html += '<details style="margin-top:8px"><summary style="cursor:pointer;font-size:11px;color:#7c5cfc;font-weight:600">View ISL Document</summary>';
      html += '<pre style="background:#f5f3ff;color:#4c1d95;padding:10px 14px;border-radius:8px;font-size:10px;margin-top:4px;overflow-x:auto;max-height:240px;overflow-y:auto">' + escapeHtml(JSON.stringify(d.isl, null, 2)) + '</pre></details>';
    }
    html += '</div>';

    /* KPI cards for numeric totals */
    if (numCols.length > 0 && numCols.length <= 6) {
      html += '<div class="rpt-kpi-grid" style="margin-bottom:20px">';
      numCols.forEach(function (c) {
        var total = rows.reduce(function (s, r) { return s + (r[c] || 0); }, 0);
        var avg = rows.length ? total / rows.length : 0;
        html += '<div class="rpt-kpi-card">';
        html += '<div class="rpt-kpi-label">' + escapeHtml(c.replace(/_/g, ' ')) + '</div>';
        html += '<div class="rpt-kpi-value">' + fmtSmart(total, c) + '</div>';
        html += '<div class="rpt-kpi-label">Avg: ' + fmtSmart(avg, c) + '</div>';
        html += '</div>';
      });
      html += '</div>';
    }

    /* Chart */
    if (chartState.isChartable && selectedChartType !== 'table' && rows.length <= 200) {
      html += '<div class="rpt-card" style="margin-bottom:20px"><canvas id="ai-rpt-chart" height="280"></canvas></div>';
    }

    /* Data table */
    html += '<div class="rpt-card">';
    html += '<div style="overflow-x:auto">';
    html += '<table class="rpt-table"><thead><tr>';
    cols.forEach(function (c) { html += '<th>' + escapeHtml(c) + '</th>'; });
    html += '</tr></thead><tbody>';
    var limit = Math.min(rows.length, 200);
    for (var i = 0; i < limit; i++) {
      html += '<tr>';
      cols.forEach(function (c) {
        var v = rows[i][c];
        var display = v == null ? '' : typeof v === 'number' ? fmtSmart(v, c) : String(v);
        html += '<td>' + escapeHtml(display) + '</td>';
      });
      html += '</tr>';
    }
    html += '</tbody></table></div>';
    if (rows.length > limit) html += '<div style="font-size:11px;color:#6b7280;padding:8px 0">Showing ' + limit + ' of ' + rows.length + ' rows</div>';
    html += '</div>';

    content.innerHTML = html;
    content.scrollTop = 0;

    var saveMainBtn = el('ai-rpt-save-main');
    if (saveMainBtn) {
      saveMainBtn.addEventListener('click', function () {
        saveCurrentReport();
      });
    }
    var exportCsvBtn = el('ai-rpt-export-csv');
    if (exportCsvBtn) {
      exportCsvBtn.addEventListener('click', function () {
        var filename = ((d.saved_report && d.saved_report.name) || intent || 'ai-report')
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, '-')
          .replace(/^-+|-+$/g, '') || 'ai-report';
        exportRowsToCsv(filename + '.csv', cols, rows);
      });
    }
    var chartTypeSelect = el('ai-rpt-chart-type');
    if (chartTypeSelect) {
      chartTypeSelect.addEventListener('change', function () {
        activeChartPreference = normalizeChartPreference(chartTypeSelect.value);
        renderAIReport(d, intent);
      });
    }

    /* Render chart if we have the canvas */
    var chartCanvas = el('ai-rpt-chart');
    drawMainReportChart(chartCanvas, chartState, rows, activeChartPreference);
  }

  function showThinkingInMain() {
    var content = el('rpt-content');
    var existing = content.querySelector('.rpt-main-thinking');
    if (existing) existing.remove();
    var div = document.createElement('div');
    div.className = 'rpt-main-thinking';
    div.innerHTML = '<div style="display:flex;align-items:center;gap:10px;padding:40px 0;justify-content:center;color:#8b91a3">' +
      '<div class="rpt-spinner"></div><span>Analyzing your question...</span></div>';
    content.prepend(div);
  }

  function hideThinkingInMain() {
    var t = document.querySelector('.rpt-main-thinking');
    if (t) t.remove();
  }

  async function aiAsk() {
    var q = aiInput.value.trim();
    if (!q) return;
    aiInput.value = '';
    aiAddMsg('user', escapeHtml(q));
    if (handleLocalChartCommand(q)) {
      aiInput.focus();
      return;
    }
    aiSend.disabled = true;
    showThinkingInMain();

    try {
      var resp = await fetch('/api/reporting/ask', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          question: q,
          conn_id: activeReportingConnId || undefined,
          tables: activeScopeTables.length ? activeScopeTables : undefined
        })
      });
      hideThinkingInMain();

      var d = await resp.json();
      if (d.error) {
        var errHtml = '<span style="color:#dc2626">Error: ' + escapeHtml(d.error) + '</span>';
        if (d.details) errHtml += '<div style="color:#dc2626;font-size:12px;margin-top:4px">' + d.details.map(escapeHtml).join('<br>') + '</div>';
        aiAddMsg('ai', errHtml);
        /* Show error in main content */
        var content = el('rpt-content');
        content.innerHTML = '<div class="rpt-card" style="margin:24px 0;border-color:#fecaca"><div style="color:#dc2626;font-weight:600;margin-bottom:8px">Query failed</div>' +
          '<div style="font-size:13px;color:#6b7280">' + escapeHtml(d.error) + '</div>' +
          (d.details ? '<div style="font-size:12px;color:#9ca3af;margin-top:8px">' + d.details.map(escapeHtml).join('<br>') + '</div>' : '') +
          '</div>';
      } else {
        lastAIResult = d;
        lastAIIntent = (d.isl && d.isl.intent) ? d.isl.intent : q;
        activeChartPreference = 'auto';
        var intent = (d.isl && d.isl.intent) ? d.isl.intent : q;
        /* Log to conversation history */
        var msgHtml = '<div style="font-size:13px">' + escapeHtml(d.count + ' rows') + ' from <b>' + escapeHtml((d.isl && d.isl.table) || 'query') + '</b></div>';
        if (d.follow_up_used) {
          msgHtml += '<div class="ai-meta">Used your previous report context for this follow-up.</div>';
        }
        aiAddMsg('ai', msgHtml);
        /* Render directly in main area */
        renderAIReport(d, intent);
      }
    } catch (e) {
      hideThinkingInMain();
      aiAddMsg('ai', '<span style="color:#dc2626">Request failed: ' + escapeHtml(e.message) + '</span>');
    }
    aiSend.disabled = false;
    aiInput.focus();
  }

  function renderWelcome() {
    var content = el('rpt-content');
    content.innerHTML =
      '<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:60vh;text-align:center;padding:40px">' +
      '<div style="width:60px;height:60px;background:linear-gradient(135deg,#6366f1,#8b5cf6);border-radius:16px;display:flex;align-items:center;justify-content:center;font-size:28px;color:#fff;margin-bottom:20px">B</div>' +
      '<h2 style="font-size:24px;font-weight:700;margin-bottom:8px;letter-spacing:-0.5px">Ask anything about your data</h2>' +
      '<p style="color:#6b7280;font-size:15px;max-width:480px;margin-bottom:24px">' +
      'Type a question below. Bitool translates it into a validated query using the ISL pipeline — no SQL, no hallucination. Results appear here as charts and tables you can save and share.</p>' +
      '<div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:center">' +
      '<div style="background:#f8f9fb;border:1px solid #e2e5eb;border-radius:10px;padding:12px 16px;font-size:13px;color:#475569;max-width:200px">' +
      '<div style="font-weight:700;color:#1e293b;margin-bottom:2px">1. Connect</div>Pick a data source in the sidebar</div>' +
      '<div style="background:#f8f9fb;border:1px solid #e2e5eb;border-radius:10px;padding:12px 16px;font-size:13px;color:#475569;max-width:200px">' +
      '<div style="font-weight:700;color:#1e293b;margin-bottom:2px">2. Train</div>Click Train AI to teach it your schema</div>' +
      '<div style="background:#f8f9fb;border:1px solid #e2e5eb;border-radius:10px;padding:12px 16px;font-size:13px;color:#475569;max-width:200px">' +
      '<div style="font-weight:700;color:#1e293b;margin-bottom:2px">3. Ask</div>Type a question and get instant reports</div>' +
      '</div></div>';
  }

  function showDemoNav(show) {
    var nav = el('rpt-demo-nav');
    if (nav) nav.style.display = show ? 'block' : 'none';
  }

  /* ── Init ───────────────────────────────────────────────────────── */
  async function init() {
    setupTimePicker();
    setupAI();

    /* Try to load demo data for fallback canned views */
    try {
      var resp = await fetch('/api/reporting/data');
      if (resp.ok) {
        data = await resp.json();
        setupNav();
        setDateRange();
        showDemoNav(true);
        renderExecutive();
      } else {
        showDemoNav(false);
        renderWelcome();
      }
    } catch (e) {
      showDemoNav(false);
      renderWelcome();
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
