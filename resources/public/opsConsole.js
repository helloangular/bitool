/* ── Ops Console — Interaction Layer ── */

// ─── Screen Navigation ───
const navItems = document.querySelectorAll('.ops-nav-item[data-screen]');
const screens = document.querySelectorAll('.ops-screen');
const screenTitle = document.getElementById('screenTitle');
const screenIcon = document.getElementById('screenIcon');

const screenMeta = {
  'pipeline-overview':   { title: 'Pipeline Overview',       icon: '\u25EF' },
  'queue-workers':       { title: 'Queue & Workers',         icon: '\u2699' },
  'source-health':       { title: 'Source Health',            icon: '\u26A1' },
  'batches-manifests':   { title: 'Batches & Manifests',     icon: '\u2610' },
  'checkpoints-replay':  { title: 'Checkpoints & Replay',    icon: '\u21BA' },
  'bad-records':         { title: 'Bad Records',             icon: '\u26A0' },
  'schema-medallion':    { title: 'Schema & Medallion',      icon: '\u2605' },
  'admin-policies':      { title: 'Admin & Policies',        icon: '\u2692' },
};

function switchScreen(screenId) {
  // Update nav
  navItems.forEach(n => n.classList.toggle('active', n.dataset.screen === screenId));
  // Update content
  screens.forEach(s => s.classList.toggle('active', s.id === 'screen-' + screenId));
  // Update topbar
  const meta = screenMeta[screenId] || { title: screenId, icon: '' };
  screenTitle.textContent = meta.title;
  screenIcon.textContent = meta.icon;
}

navItems.forEach(item => {
  item.addEventListener('click', () => switchScreen(item.dataset.screen));
});

// ─── Refresh ───
function refreshAll() {
  const el = document.getElementById('lastRefresh');
  const now = new Date();
  el.textContent = now.toLocaleTimeString();

  // Visual feedback
  document.querySelectorAll('.ops-metric-card').forEach(card => {
    card.style.transition = 'opacity 0.2s';
    card.style.opacity = '0.5';
    setTimeout(() => card.style.opacity = '1', 300);
  });
}

// Set initial refresh time
document.getElementById('lastRefresh').textContent = new Date().toLocaleTimeString();

// ─── Select-all checkboxes ───
document.getElementById('selectAllQueue')?.addEventListener('change', function() {
  const table = this.closest('table');
  table.querySelectorAll('tbody input[type="checkbox"]').forEach(cb => {
    cb.checked = this.checked;
  });
});

// ─── Queue Status Filter ───
document.getElementById('queueStatusFilter')?.addEventListener('change', function() {
  const filter = this.value.toLowerCase();
  const rows = document.querySelectorAll('#queueTable tbody tr');
  rows.forEach(row => {
    if (!filter) { row.style.display = ''; return; }
    const badge = row.querySelector('.badge');
    const status = badge ? badge.textContent.toLowerCase().trim() : '';
    row.style.display = status.includes(filter) ? '' : 'none';
  });
});

// ─── Search (generic — works on any table within same panel) ───
document.querySelectorAll('.ops-search').forEach(input => {
  input.addEventListener('input', function() {
    const query = this.value.toLowerCase();
    const panel = this.closest('.ops-panel');
    if (!panel) return;
    const rows = panel.querySelectorAll('tbody tr');
    rows.forEach(row => {
      const text = row.textContent.toLowerCase();
      row.style.display = text.includes(query) ? '' : 'none';
    });
  });
});

// ─── Manifest Inspect ───
document.querySelectorAll('#screen-batches-manifests .ops-table .ops-btn').forEach(btn => {
  if (btn.textContent.trim() === 'Inspect') {
    btn.addEventListener('click', () => {
      document.getElementById('manifestDetail').style.display = 'block';
      document.getElementById('manifestDetail').scrollIntoView({ behavior: 'smooth' });
    });
  }
});

// ─── Bad Record Detail ───
function showBadRecordDetail() {
  document.getElementById('badRecordDetail').style.display = 'block';
  document.getElementById('badRecordDetail').scrollIntoView({ behavior: 'smooth' });
}

// ─── Action Button Confirmations ───
document.querySelectorAll('.row-actions .ops-btn').forEach(btn => {
  const text = btn.textContent.trim();
  if (['Cancel', 'Rollback', 'Drain', 'Force Release', 'Reset'].includes(text)) {
    btn.addEventListener('click', (e) => {
      if (!confirm(`Are you sure you want to ${text.toLowerCase()} this item?`)) {
        e.stopPropagation();
      }
    });
  }
  if (['Retry', 'Requeue', 'Replay'].includes(text)) {
    btn.addEventListener('click', () => {
      btn.textContent = '...';
      btn.disabled = true;
      setTimeout(() => {
        btn.textContent = '\u2713 Done';
        btn.style.color = 'var(--green)';
        btn.style.borderColor = 'rgba(52,211,153,0.3)';
        setTimeout(() => {
          btn.textContent = text;
          btn.disabled = false;
          btn.style.color = '';
          btn.style.borderColor = '';
        }, 2000);
      }, 800);
    });
  }
});

// ─── Bulk Actions ───
document.querySelectorAll('.ops-panel > div:last-child .ops-btn').forEach(btn => {
  const text = btn.textContent.trim();
  if (text.includes('Selected')) {
    btn.addEventListener('click', () => {
      const panel = btn.closest('.ops-panel');
      const checked = panel.querySelectorAll('tbody input[type="checkbox"]:checked');
      if (checked.length === 0) {
        alert('No items selected.');
        return;
      }
      if (confirm(`${text} for ${checked.length} item(s)?`)) {
        checked.forEach(cb => {
          const row = cb.closest('tr');
          row.style.opacity = '0.4';
          cb.checked = false;
        });
      }
    });
  }
});

// ─── Auto-refresh simulation ───
let autoRefreshInterval = null;

function startAutoRefresh(intervalMs) {
  stopAutoRefresh();
  autoRefreshInterval = setInterval(() => {
    refreshAll();
  }, intervalMs);
}

function stopAutoRefresh() {
  if (autoRefreshInterval) {
    clearInterval(autoRefreshInterval);
    autoRefreshInterval = null;
  }
}

// ─── Keyboard shortcuts ───
document.addEventListener('keydown', (e) => {
  // Alt+1 through Alt+8 for screen switching
  if (e.altKey && e.key >= '1' && e.key <= '8') {
    e.preventDefault();
    const screenIds = Object.keys(screenMeta);
    const idx = parseInt(e.key) - 1;
    if (idx < screenIds.length) {
      switchScreen(screenIds[idx]);
    }
  }
  // Alt+R for refresh
  if (e.altKey && e.key === 'r') {
    e.preventDefault();
    refreshAll();
  }
});

// ─── Tooltip: show screen keyboard shortcuts ───
navItems.forEach((item, i) => {
  item.title = `Alt+${i + 1}`;
});

// ─── Pipeline selector changes context ───
document.getElementById('pipelineSelect')?.addEventListener('change', function() {
  // In real app, this would filter all data to selected pipeline
  refreshAll();
});

// ─── Time range change ───
document.getElementById('timeRange')?.addEventListener('change', function() {
  refreshAll();
});

console.log('Bitool Operations Console loaded. Use Alt+1..8 to switch screens, Alt+R to refresh.');
