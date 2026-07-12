// ── Theme ────────────────────────────────────────────────────────────────────
function toggleTheme() {
  const light = document.body.classList.toggle('light');
  document.getElementById('theme-toggle').textContent = light ? '☀️' : '🌙';
  localStorage.setItem('theme', light ? 'light' : 'dark');
}
(function () {
  if (localStorage.getItem('theme') === 'light') {
    document.body.classList.add('light');
    document.addEventListener('DOMContentLoaded', () => {
      const btn = document.getElementById('theme-toggle');
      if (btn) btn.textContent = '☀️';
    });
  }
})();

// ── Navigation ──────────────────────────────────────────────────────────────
const _NAV_PAGE_MAP = {
  dashboard: 'dashboard', capture: 'capture golden', compare: 'compare',
  watch: 'watch (live)', goldens: 'golden snapshots', config: 'config',
};
function showPage(name) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById('page-' + name).classList.add('active');
  const label = _NAV_PAGE_MAP[name] || name;
  document.querySelectorAll('.nav-item').forEach(n => {
    if (n.querySelector('.nav-label')?.textContent.toLowerCase().trim() === label)
      n.classList.add('active');
  });
  if (name === 'goldens' || name === 'dashboard') loadGoldens();
  if (name === 'config') loadConfig();
  if (name === 'capture') refreshCaptureProject();
  if (name === 'dashboard') { loadReports(); checkAllureStatus(); }
}

let allureStatusChecked = false;
async function checkAllureStatus() {
  if (allureStatusChecked) return;        // one-time per session
  allureStatusChecked = true;
  try {
    const s = await (await fetch('/api/allure/status')).json();
    const warn = document.getElementById('allure-cli-warn');
    if (warn) warn.style.display = s.html_capable ? 'none' : 'block';
  } catch (e) {}
}

// ── Helpers ──────────────────────────────────────────────────────────────────
function datetimeLocalToISO(val) {
  if (!val) return null;
  return val.replace('T', ' ') + ':00';
}

function statusBadge(s) {
  const map = {PASS:'pass', FAIL:'fail', 'NO GOLDEN':'warn', 'NO BASELINE':'warn', ERROR:'error'};
  const icon = {PASS:'✅', FAIL:'❌', 'NO GOLDEN':'⚠️', 'NO BASELINE':'⚠️', ERROR:'🔥'};
  return `<span class="badge badge-${map[s]||'info'}">${icon[s]||''} ${s}</span>`;
}

// Registry for the "mark value-diffs as pass" feature (client-side, this view only).
window.__rowReg = window.__rowReg || {};
window.__rowUid = window.__rowUid || 0;

// A finding is "value-only" when the attribute/path is identical and only the
// value (or its type) differs — i.e. DeepDiff 'values changed' / 'type changes'.
function isValueOnly(f) {
  return f.type === 'values changed' || f.type === 'type changes';
}

function renderResultRow(r, tbodyId) {
  const tbody = document.getElementById(tbodyId);
  const uid = ++window.__rowUid;
  const rowId = 'row-' + uid;
  window.__rowReg[uid] = { findings: r.findings || [], status: r.status, marks: new Set(), tbodyId };

  const valueDiffCount = (r.findings || []).filter(isValueOnly).length;

  const tr = document.createElement('tr');
  tr.className = 'expandable';
  tr.innerHTML = `
    <td>${r.db_id}</td>
    <td style="white-space:nowrap;font-size:11px">${r.create_time}</td>
    <td style="font-family:monospace;font-size:11px;color:#64748b">${r.ext_id || '—'}</td>
    <td style="font-family:monospace;font-size:12px">${r.key}</td>
    <td id="${rowId}-status">${statusBadge(r.status)}</td>
    <td id="${rowId}-count">${r.findings.length}</td>
  `;
  tbody.appendChild(tr);

  const markAllBtn = valueDiffCount > 0 ? `
        <span style="font-weight:400;color:#475569;margin-left:8px">
          — ${valueDiffCount} value-only
          <button class="mark-pass-btn" onclick="event.stopPropagation(); markAllValueDiffs(${uid}, true)">✓ mark all pass</button>
          <button class="mark-pass-btn mark-pass-reset" onclick="event.stopPropagation(); markAllValueDiffs(${uid}, false)">↺ reset</button>
        </span>` : '';

  const diffBlock = r.findings.length > 0 ? `
        <div id="${rowId}-diffhdr" style="font-size:11px;font-weight:700;color:#64748b;margin:0 0 6px">
          DIFFERENCES (${r.findings.length})${markAllBtn}
        </div>
        ${r.findings.map((f, i) => `
          <div class="diff-row diff-row-fail" id="${rowId}-diff-${i}">
            <span class="diff-type">${f.type}</span>
            <span class="diff-path">${f.path}</span>
            <span class="diff-detail-text">${f.detail}</span>
            ${isValueOnly(f) ? `<button class="mark-pass-btn" id="${rowId}-mark-${i}" onclick="event.stopPropagation(); toggleMarkPass(${uid}, ${i})">✓ mark pass</button>` : ''}
          </div>`).join('')}` : '';

  const jsonBlock = r.payload ? `
        <div style="font-size:11px;font-weight:700;color:#64748b;margin:${diffBlock ? '12px' : '0'} 0 6px">
          PAYLOAD JSON <span style="font-weight:400;color:#475569">— <span style="color:var(--log-pass,#86efac)">green = matches golden</span>, <span style="color:var(--log-fail,#fca5a5)">red = schema mismatch</span></span>
        </div>
        <pre class="payload-json">${colorJsonLines(r.payload, r.findings)}</pre>` : '';

  if (diffBlock || jsonBlock) {
    const detail = document.createElement('tr');
    detail.innerHTML = `<td colspan="6">
      <div class="diff-detail" id="${rowId}-detail">
        ${diffBlock}${jsonBlock}
      </div>
    </td>`;
    tbody.appendChild(detail);
    tr.onclick = () => {
      const d = document.getElementById(rowId + '-detail');
      d.style.display = d.style.display === 'block' ? 'none' : 'block';
    };
  }
}

// Toggle a single value-only finding between dismissed (marked pass) and active.
function toggleMarkPass(uid, idx) {
  const reg = window.__rowReg[uid];
  if (!reg) return;
  const row = document.getElementById('row-' + uid + '-diff-' + idx);
  const btn = document.getElementById('row-' + uid + '-mark-' + idx);
  if (reg.marks.has(idx)) {
    reg.marks.delete(idx);
    if (row) row.classList.remove('diff-row-dismissed');
    if (btn) btn.textContent = '✓ mark pass';
  } else {
    reg.marks.add(idx);
    if (row) row.classList.add('diff-row-dismissed');
    if (btn) btn.textContent = '↺ undo';
  }
  recomputeRow(uid);
}

function markAllValueDiffs(uid, mark) {
  const reg = window.__rowReg[uid];
  if (!reg) return;
  reg.findings.forEach((f, i) => {
    if (!isValueOnly(f)) return;
    const has = reg.marks.has(i);
    if (mark && !has) toggleMarkPass(uid, i);
    else if (!mark && has) toggleMarkPass(uid, i);
  });
}

// Recompute status + counts after marking. Status becomes PASS only when every
// remaining (undismissed) difference has been marked away.
function recomputeRow(uid) {
  const reg = window.__rowReg[uid];
  if (!reg) return;
  const remaining = reg.findings.length - reg.marks.size;
  const newStatus = remaining === 0 ? 'PASS' : reg.status;
  const marked = reg.marks.size;

  const statusTd = document.getElementById('row-' + uid + '-status');
  if (statusTd) {
    statusTd.innerHTML = statusBadge(newStatus) +
      (marked > 0 && remaining === 0 ? ' <span style="font-size:10px;color:#fbbf24">(value-diffs accepted)</span>' : '');
  }
  const countTd = document.getElementById('row-' + uid + '-count');
  if (countTd) countTd.textContent = marked > 0 ? `${remaining} (+${marked} accepted)` : `${reg.findings.length}`;

  const hdr = document.getElementById('row-' + uid + '-diffhdr');
  if (hdr) {
    const valueDiffCount = reg.findings.filter(isValueOnly).length;
    hdr.childNodes[0].nodeValue = `DIFFERENCES (${remaining}${marked ? ` active, ${marked} accepted` : ''}) `;
  }

  // For the direct JSON/XML comparators there is a single row + summary strip.
  const prefix = reg.tbodyId === 'json-result-body' ? 'json'
               : reg.tbodyId === 'xml-result-body'  ? 'xml' : null;
  if (prefix) {
    const v = document.getElementById(prefix + '-verdict');
    if (v) {
      v.textContent = newStatus === 'PASS' ? '✅ MATCH' : '❌ MISMATCH';
      v.style.color = newStatus === 'PASS' ? '#86efac' : '#fca5a5';
    }
    const d = document.getElementById(prefix + '-diffs');
    if (d) d.textContent = remaining;
  }
}

function escapeHtml(s) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// Convert a DeepDiff path (root['a']['b'][0]) into a dot-path (a.b.0).
function parseDiffPaths(findings) {
  const set = new Set();
  (findings || []).forEach(f => {
    const segs = (f.path || '').match(/\[['"]?([^\]'"]+)['"]?\]/g) || [];
    const path = segs.map(s => s.replace(/^\[['"]?/, '').replace(/['"]?\]$/, '')).join('.');
    if (path) set.add(path);
  });
  return set;
}

// A line is "bad" only if it IS a changed node or sits INSIDE an added/removed
// subtree — never just because it's an ancestor on the way to a deep change.
function pathIsBad(path, diffPaths) {
  if (!path) return false;
  if (diffPaths.has(path)) return true;
  for (const d of diffPaths) if (path.startsWith(d + '.')) return true;
  return false;
}

// Render payload JSON line-by-line: green = matches golden, red = exact mismatch.
function colorJsonLines(payload, findings) {
  const diffPaths = parseDiffPaths(findings);
  const lines = [];
  function walk(node, path, indent, keyLabel, comma) {
    const pad = '  '.repeat(indent);
    const bad = pathIsBad(path, diffPaths);
    const prefix = keyLabel !== null ? '"' + keyLabel + '": ' : '';
    if (node !== null && typeof node === 'object') {
      const isArr = Array.isArray(node);
      lines.push({t: pad + prefix + (isArr ? '[' : '{'), bad});
      const entries = isArr ? node.map((v, i) => [i, v]) : Object.entries(node);
      entries.forEach((kv, i) => {
        const childPath = path ? path + '.' + kv[0] : String(kv[0]);
        walk(kv[1], childPath, indent + 1, isArr ? null : kv[0], i < entries.length - 1);
      });
      lines.push({t: pad + (isArr ? ']' : '}') + (comma ? ',' : ''), bad});
    } else {
      lines.push({t: pad + prefix + JSON.stringify(node) + (comma ? ',' : ''), bad});
    }
  }
  walk(payload, '', 0, null, false);
  return lines.map(l =>
    '<span class="' + (l.bad ? 'jl-bad' : 'jl-ok') + '">' + escapeHtml(l.t) + '</span>'
  ).join('\n');
}

function updateWatchCounters(results) {
  const pass = results.filter(r=>r.status==='PASS').length;
  const fail = results.filter(r=>r.status==='FAIL').length;
  const nog  = results.filter(r=>r.status==='NO GOLDEN').length;
  document.getElementById('w-total').textContent = results.length;
  document.getElementById('w-pass').textContent  = pass;
  document.getElementById('w-fail').textContent  = fail;
  document.getElementById('w-nogolden').textContent = nog;
  document.getElementById('watch-summary').style.display = 'block';
  document.getElementById('watch-results-card').style.display = 'block';
}

// ── Full Run (live, all flows) ────────────────────────────────────────────────
let fullRunResults = [];
let fullRunSSE = null;
let fullRunGolden = 'db';
let fullRunIsdData = 'db';

function setFullRunGolden(src) {
  fullRunGolden = src;
  ['db','isd','kowl'].forEach(s =>
    document.getElementById('fullrun-gs-' + s).classList.toggle('active', s === src));
  document.getElementById('fullrun-isd-data').style.display = src === 'isd' ? 'flex' : 'none';
}
function setFullRunIsdData(src) {
  fullRunIsdData = src;
  document.getElementById('fullrun-isd-db').classList.toggle('active', src === 'db');
  document.getElementById('fullrun-isd-kowl').classList.toggle('active', src === 'kowl');
}

// Generic CSS-donut painter
function paintDonut(donutId, pctId, pass, fail, other) {
  const total = pass + fail + other;
  if (!total) return;
  const pPass = pass / total * 100, pFail = pPass + fail / total * 100;
  document.getElementById(donutId).style.background =
    `conic-gradient(#22c55e 0 ${pPass}%, #ef4444 ${pPass}% ${pFail}%, #eab308 ${pFail}% 100%)`;
  document.getElementById(pctId).textContent = Math.round(pass / total * 100) + '%';
}

function updateFullRunCounters(results) {
  const pass = results.filter(r=>r.status==='PASS').length;
  const fail = results.filter(r=>r.status==='FAIL').length;
  const nog  = results.filter(r=>r.status!=='PASS'&&r.status!=='FAIL').length;
  document.getElementById('fr-total').textContent = results.length;
  document.getElementById('fr-pass').textContent  = pass;
  document.getElementById('fr-fail').textContent  = fail;
  document.getElementById('fr-nogolden').textContent = nog;
  document.getElementById('fr-leg-pass').textContent = pass;
  document.getElementById('fr-leg-fail').textContent = fail;
  document.getElementById('fr-leg-other').textContent = nog;
  paintDonut('fr-donut', 'fr-donut-pct', pass, fail, nog);
  document.getElementById('fullrun-summary').style.display = 'block';
  document.getElementById('fullrun-chart-card').style.display = 'block';
  document.getElementById('fullrun-results-card').style.display = 'block';
}

async function startFullRun() {
  const interval = document.getElementById('fullrun-interval').value;
  let data;
  try {
    const res = await fetch('/api/full-run/start', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ interval, mode: modeState.fullrun, golden_source: fullRunGolden,
                             data_source: fullRunGolden === 'isd' ? fullRunIsdData : undefined })
    });
    data = await res.json();
  } catch (e) { alert('Full Run failed to start: ' + e); return; }
  if (!data.ok) { alert(data.error); return; }

  fullRunResults = [];
  document.getElementById('fullrun-results-body').innerHTML = '';
  document.getElementById('fullrun-log').innerHTML = '';
  document.getElementById('fullrun-log-card').style.display = 'block';

  document.getElementById('fullrun-start-btn').disabled = true;
  document.getElementById('fullrun-stop-btn').disabled  = false;
  setFullRunModeLocked(true);
  document.getElementById('fullrun-status-dot').innerHTML = '<span class="pulse"></span>';
  document.getElementById('fullrun-status-text').textContent = 'Watching all flows...';

  fullRunSSE = new EventSource('/api/full-run/stream');
  fullRunSSE.onmessage = (e) => {
    const item = JSON.parse(e.data);
    if (item.type === 'ping') return;

    const log = document.getElementById('fullrun-log');
    const line = document.createElement('div');
    line.className = 'log-line log-' + item.type;
    line.textContent = new Date().toLocaleTimeString() + '  ' + item.msg;
    log.appendChild(line);
    log.scrollTop = log.scrollHeight;

    if (item.result) {
      const r = item.result;
      if (r.flow) r.create_time = r.flow + ' · ' + r.create_time;
      fullRunResults.push(r);
      renderResultRow(r, 'fullrun-results-body');
      updateFullRunCounters(fullRunResults);
    }

    if (item.type === 'done') {
      fullRunSSE.close();
      document.getElementById('fullrun-start-btn').disabled = false;
      document.getElementById('fullrun-stop-btn').disabled  = true;
      setFullRunModeLocked(false);
      document.getElementById('fullrun-status-dot').innerHTML = '';
      document.getElementById('fullrun-status-text').textContent = 'Idle';
      if (item.report) {
        const dl = document.getElementById('fullrun-report-dl');
        dl.href = '/api/report/' + encodeURIComponent(item.report) + '?download=1';
        dl.style.display = 'inline-flex';
      }
      showAllure(item.allure_zip, item.allure_html);
      loadReports();  // new report now appears under Past Reports
    }
  };
}

function showAllure(zip, html) {
  const card = document.getElementById('fullrun-allure-card');
  const body = document.getElementById('fullrun-allure-body');
  if (!zip && !html) { card.style.display = 'none'; return; }
  card.style.display = 'block';
  let h = '';
  if (html) {
    h += `<div style="margin-bottom:8px"><a class="btn btn-success" href="/api/allure-html/${html.replace('-html','')}/" target="_blank">📊 Open Allure Report</a></div>`;
  }
  if (zip) {
    h += `<div style="margin-bottom:8px"><a class="btn btn-primary" href="/api/allure/${encodeURIComponent(zip)}" download>⬇ Download allure-results (.zip)</a></div>`;
    h += `<div style="font-size:12px;color:#64748b">No Allure HTML on the server (allure CLI/Java not installed). Unzip the file and view it with:
            <pre class="payload-json" style="margin-top:6px">unzip ${zip} -d allure-results
allure serve allure-results</pre></div>`;
  }
  body.innerHTML = h;
}

async function stopFullRun() {
  await fetch('/api/full-run/stop', {method:'POST'});
  document.getElementById('fullrun-status-text').textContent = 'Stopping — saving report...';
  // The thread emits a 'done' event after building the report; SSE handler finalizes UI.
}

// ── Dashboard ─────────────────────────────────────────────────────────────────
async function loadGoldens() {
  const el = document.getElementById('goldens-list');
  if (!el) return;
  const keys = await (await fetch('/api/goldens')).json();
  if (!keys.length) {
    el.innerHTML = '<div class="no-results">No golden snapshots yet. Use Capture to create them.</div>';
    return;
  }
  el.innerHTML =
    `<div style="font-size:11px;color:#64748b;margin-bottom:12px">${keys.length} snapshot(s) — click a folder to expand, then “View” to see the JSON</div>` +
    renderGoldenNode(buildGoldenTree(keys), true, '');
  if (goldenSelectMode) el.classList.add('select-on');
  updateGoldenSelCount();
}

// Build a nested tree from relative paths like EL_Columbus/db/PUT/PUT__created__SUCCESS
function buildGoldenTree(keys) {
  const root = {};
  keys.forEach(k => {
    const parts = k.split('/');
    let node = root;
    parts.forEach((p, i) => {
      if (i === parts.length - 1) {
        (node.__leaves = node.__leaves || []).push({name: p, key: k});
      } else {
        node[p] = node[p] || {};
        node = node[p];
      }
    });
  });
  return root;
}

function countLeaves(node) {
  let c = (node.__leaves || []).length;
  for (const k in node) if (k !== '__leaves') c += countLeaves(node[k]);
  return c;
}

const FOLDER_ICON = {db: '🗄', kowl: '🧬', isd: '📄', PUT: '📦', PICK: '🛒', AUDIT: '🔎', SR: '🔁'};

function renderGoldenNode(node, topLevel, prefix) {
  let html = '';
  Object.keys(node).filter(k => k !== '__leaves').sort().forEach(name => {
    const child = node[name];
    const icon = FOLDER_ICON[name] || '📁';
    const childPrefix = prefix ? prefix + '/' + name : name;
    html += `<details class="tree-group"${topLevel ? ' open' : ''}>
      <summary>${icon} <b>${name}</b> <span class="tree-count">${countLeaves(child)}</span>
        <span class="folder-del" title="Delete this folder"
              onclick="event.preventDefault();event.stopPropagation();deleteGoldenFolder('${childPrefix}',${countLeaves(child)})">🗑</span>
      </summary>
      <div class="tree-children">${renderGoldenNode(child, false, childPrefix)}</div>
    </details>`;
  });
  (node.__leaves || []).sort((a, b) => a.name.localeCompare(b.name)).forEach(leaf => {
    const id = 'g-' + btoa(unescape(encodeURIComponent(leaf.key))).replace(/[^a-zA-Z0-9]/g, '');
    html += `<div class="tree-leaf">
      <div class="tree-leaf-row">
        <span class="tree-leaf-name">
          <input type="checkbox" class="g-check" value="${leaf.key}" onchange="updateGoldenSelCount()">
          📄 ${leaf.name}
        </span>
        <div class="golden-actions">
          <button class="btn-xs btn-xs-view" onclick="toggleGoldenJson('${id}','${leaf.key}',this)">View</button>
          <button class="btn-xs btn-xs-del"  onclick="deleteGolden('${leaf.key}')">Delete</button>
        </div>
      </div>
      <div class="tree-json" id="${id}" style="display:none"></div>
    </div>`;
  });
  return html;
}

let goldenSelectMode = false;
function toggleGoldenSelect() {
  goldenSelectMode = !goldenSelectMode;
  document.getElementById('goldens-list').classList.toggle('select-on', goldenSelectMode);
  document.getElementById('g-select-btn').classList.toggle('btn-primary', goldenSelectMode);
  document.getElementById('g-del-sel-btn').style.display = goldenSelectMode ? 'inline-flex' : 'none';
  updateGoldenSelCount();
}

function selectedGoldenKeys() {
  return Array.from(document.querySelectorAll('#goldens-list .g-check:checked')).map(c => c.value);
}
function updateGoldenSelCount() {
  const el = document.getElementById('g-sel-count');
  if (el) el.textContent = selectedGoldenKeys().length;
}

async function postGoldenDelete(body, confirmMsg) {
  if (confirmMsg && !confirm(confirmMsg)) return;
  const res = await fetch('/api/goldens/delete', {
    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)
  });
  const data = await res.json();
  loadGoldens();
  return data;
}

function deleteSelectedGoldens() {
  const keys = selectedGoldenKeys();
  if (!keys.length) { alert('No snapshots selected.'); return; }
  postGoldenDelete({keys}, `Delete ${keys.length} selected snapshot(s)?`);
}

function deleteAllGoldens() {
  postGoldenDelete({all: true}, '⚠️ Delete ALL golden snapshots? This cannot be undone.');
}

function deleteGoldenFolder(prefix, count) {
  postGoldenDelete({prefix}, `Delete folder "${prefix}" and its ${count} snapshot(s)?`);
}

async function toggleGoldenJson(id, key, btn) {
  const box = document.getElementById(id);
  if (box.style.display === 'block') { box.style.display = 'none'; btn.textContent = 'View'; return; }
  box.style.display = 'block';
  btn.textContent = 'Hide';
  if (!box.dataset.loaded) {
    box.innerHTML = '<div style="padding:8px;color:#64748b;font-size:12px">Loading…</div>';
    try {
      const data = await (await fetch(goldenUrl(key))).json();
      box.innerHTML = '<pre class="payload-json">' + escapeHtml(JSON.stringify(data, null, 2)) + '</pre>';
      box.dataset.loaded = '1';
    } catch (e) {
      box.innerHTML = '<div style="padding:8px;color:var(--log-fail,#fca5a5);font-size:12px">Failed to load.</div>';
      box.dataset.loaded = '1'; // prevent infinite retry on error
    }
  }
}

function goldenUrl(key) {
  // encode each path segment separately so slashes are preserved in the URL
  return '/api/golden/' + key.split('/').map(encodeURIComponent).join('/');
}

async function viewGolden(key) {
  const res = await fetch(goldenUrl(key));
  const data = await res.json();
  document.getElementById('modal-title').textContent = key;
  document.getElementById('modal-body').textContent = JSON.stringify(data, null, 2);
  document.getElementById('modal').classList.add('open');
}

async function deleteGolden(key) {
  if (!confirm('Delete golden snapshot: ' + key + '?')) return;
  await fetch(goldenUrl(key), {method:'DELETE'});
  loadGoldens();
}

function closeModal() {
  document.getElementById('modal').classList.remove('open');
}

// ── Capture ───────────────────────────────────────────────────────────────────
function switchCapTab(tab) {
  document.getElementById('cap-tab-time').classList.toggle('active', tab === 'time');
  document.getElementById('cap-tab-live').classList.toggle('active', tab === 'live');
  document.getElementById('cap-panel-time').style.display = tab === 'time' ? 'block' : 'none';
  document.getElementById('cap-panel-live').style.display = tab === 'live' ? 'block' : 'none';
}

function switchCapSource(src) {
  ['db','kowl','isd'].forEach(s => {
    document.getElementById('cap-src-' + s).style.display = s === src ? 'block' : 'none';
    document.getElementById('cap-src-tab-' + s).classList.toggle('active', s === src);
  });
  if (src === 'kowl') {
    initTopics();
    const lbl = document.getElementById('cap-kowl-project');
    if (lbl) refreshCaptureProject().then(() => {
      const p = document.getElementById('cap-project-label').textContent;
      lbl.textContent = p;
    });
  }
}

function renderPatternChecks(containerId, patterns) {
  const el = document.getElementById(containerId);
  if (!el) return;
  el.innerHTML = patterns.length
    ? patterns.map((p, i) => `
        <label style="display:flex;align-items:center;gap:6px;padding:2px 0;cursor:pointer">
          <input type="checkbox" class="${containerId}-item" value="${p.pattern}" checked>
          <span><b>${p.label}</b> — ${p.pattern}</span>
        </label>`).join('')
    : '<span style="color:var(--log-fail,#fca5a5)">No patterns configured. Add them on the Config tab.</span>';
}

function checkedPatterns(containerId) {
  return Array.from(document.querySelectorAll('.' + containerId + '-item:checked')).map(c => c.value);
}

async function refreshCaptureProject() {
  try {
    const cfg = await (await fetch('/api/config')).json();
    const p = cfg.project || '(none)';
    document.getElementById('cap-project-label').textContent = p;
    document.getElementById('cap-isd-project').textContent   = p;
    renderPatternChecks('cap-pattern-checks', cfg.patterns || []);
    renderPatternChecks('cap-live-pattern-checks', cfg.patterns || []);
  } catch (e) {}
}

async function uploadISD() {
  const btn = document.getElementById('cap-isd-btn');
  const fileEl = document.getElementById('cap-isd-file');
  if (!fileEl.files.length) { alert('Choose an ISD PDF first.'); return; }
  const fd = new FormData();
  fd.append('file', fileEl.files[0]);
  btn.disabled = true; btn.textContent = '⏳ Reading ISD...';
  try {
    const res = await fetch('/api/golden/from-isd', {method:'POST', body: fd});
    const data = await res.json();
    const card = document.getElementById('cap-isd-result');
    const body = document.getElementById('cap-isd-result-body');
    card.style.display = 'block';
    if (data.error) {
      body.innerHTML = '<div style="color:var(--log-fail,#fca5a5)">❌ ' + data.error + '</div>';
    } else {
      const unparse = data.blocks_unparseable || 0;
      body.innerHTML =
        `<div style="font-size:12px;color:var(--log-pass,#86efac);margin-bottom:6px">✅ Read ${data.pages} page(s); saved ${data.keys} golden(s) under project "${data.project || '(none)'}".</div>` +
        `<div style="font-size:11px;color:#64748b;margin-bottom:10px">JSON blocks found: ${data.blocks_seen} · parsed: ${data.blocks_parsed}${unparse ? ` · <span style="color:#fcd34d">unparseable: ${unparse}</span>` : ''}</div>` +
        (data.saved.length
          ? data.saved.map(s => `<div class="golden-item"><span class="golden-name">${s.key}</span></div>`).join('')
          : '<div style="color:#fcd34d;font-size:12px">No payloads auto-extracted.</div>') +
        (unparse ? `<div style="font-size:11px;color:#fcd34d;margin-top:8px">⚠️ ${unparse} payload block(s) couldn't be parsed (the PDF's JSON is malformed — smart quotes / wrapped tokens). Paste those below to capture them.</div>` : '');
      loadGoldens();
    }
  } catch (e) { alert('ISD upload error: ' + e); }
  btn.disabled = false; btn.textContent = '📄 Read ISD & Capture Golden';
}

async function saveIsdPaste() {
  const btn = document.getElementById('isd-paste-btn');
  const status = document.getElementById('isd-paste-status');
  const text = document.getElementById('isd-paste').value.trim();
  if (!text) { status.textContent = 'Paste a payload first.'; status.style.color = '#fca5a5'; return; }
  btn.disabled = true; btn.textContent = '⏳ Saving...'; status.textContent = '';
  try {
    const res = await fetch('/api/golden/from-json', {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({text})
    });
    const data = await res.json();
    const result = document.getElementById('isd-paste-result');
    if (data.error) {
      status.textContent = '❌ ' + data.error; status.style.color = '#fca5a5';
    } else {
      status.textContent = `✅ Saved ${data.keys} golden(s) from ${data.objects} object(s)`;
      status.style.color = '#86efac';
      result.innerHTML = data.saved.map(s => `<div class="golden-item"><span class="golden-name">${s.key}</span></div>`).join('');
      loadGoldens();
    }
  } catch (e) { status.textContent = 'Error: ' + e; status.style.color = '#fca5a5'; }
  btn.disabled = false; btn.textContent = '💾 Save pasted as golden';
}

let capFetchMode     = 'time';
let capLiveFetchMode = 'time';
let watchFetchMode   = 'time';

function switchCapFetchTab(tab) {
  capFetchMode = tab;
  document.getElementById('cap-fetch-tab-time').classList.toggle('active',  tab === 'time');
  document.getElementById('cap-fetch-tab-extid').classList.toggle('active', tab === 'extid');
  document.getElementById('cap-fetch-panel-time').style.display  = tab === 'time'  ? 'block' : 'none';
  document.getElementById('cap-fetch-panel-extid').style.display = tab === 'extid' ? 'block' : 'none';
}

function switchCapLiveFetchTab(tab) {
  capLiveFetchMode = tab;
  document.getElementById('cap-live-fetch-tab-time').classList.toggle('active',  tab === 'time');
  document.getElementById('cap-live-fetch-tab-extid').classList.toggle('active', tab === 'extid');
  document.getElementById('cap-live-fetch-panel-extid').style.display = tab === 'extid' ? 'block' : 'none';
}

function switchWatchFetchTab(tab) {
  watchFetchMode = tab;
  document.getElementById('watch-fetch-tab-time').classList.toggle('active',  tab === 'time');
  document.getElementById('watch-fetch-tab-extid').classList.toggle('active', tab === 'extid');
  document.getElementById('watch-fetch-panel-extid').style.display = tab === 'extid' ? 'block' : 'none';
}

async function doCapture() {
  const btn = document.getElementById('cap-btn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> Connecting...';

  const patterns = checkedPatterns('cap-pattern-checks');
  const since  = capFetchMode === 'time'  ? datetimeLocalToISO(document.getElementById('cap-since').value) : null;
  const ext_id = capFetchMode === 'extid' ? document.getElementById('cap-extid').value.trim() : null;

  if (!patterns.length) {
    alert('Select at least one pattern (add them on the Config tab if none are listed).');
    btn.disabled = false; btn.innerHTML = '📸 Capture'; return;
  }
  if (capFetchMode === 'extid' && !ext_id) {
    alert('Please enter an External Request ID.');
    btn.disabled = false; btn.innerHTML = '📸 Capture'; return;
  }

  try {
    const res = await fetch('/api/capture', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({patterns, since, ext_id})
    });
    const data = await res.json();
    const el   = document.getElementById('cap-result');
    const body = document.getElementById('cap-result-body');
    el.style.display = 'block';
    if (data.ok) {
      const errs = (data.errors || []).map(e => `<p style="color:#fbbf24;font-size:12px">⚠️ ${e}</p>`).join('');
      body.innerHTML = `
        ${errs}
        <p style="color:var(--log-pass,#86efac);margin-bottom:10px">✅ Captured ${data.saved.length} golden snapshot(s) from ${data.total_fetched} notifications.</p>
        ${data.saved.map(k=>`<div style="font-family:monospace;font-size:12px;color:#a5b4fc;padding:2px 0">${k}</div>`).join('')}
      `;
    } else {
      body.innerHTML = `<p style="color:var(--log-fail,#fca5a5)">❌ ${data.error}</p>`;
    }
  } catch(e) {
    alert('Error: ' + e.message);
  }
  btn.disabled = false;
  btn.innerHTML = '📸 Capture';
}

// ── Live Capture ──────────────────────────────────────────────────────────────
let liveCapSSE = null;

async function startLiveCapture() {
  const patterns = checkedPatterns('cap-live-pattern-checks');
  const interval   = document.getElementById('cap-live-interval').value;
  const ext_id     = capLiveFetchMode === 'extid' ? document.getElementById('cap-live-extid').value.trim() : null;

  if (!patterns.length) { alert('Select at least one pattern (add them on the Config tab if none are listed).'); return; }
  if (capLiveFetchMode === 'extid' && !ext_id) {
    alert('Please enter an External Request ID.'); return;
  }

  const res = await fetch('/api/capture/live/start', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({patterns, interval, ext_id})
  });
  const data = await res.json();
  if (!data.ok) { alert(data.error); return; }

  document.getElementById('cap-live-log').innerHTML = '';
  document.getElementById('cap-live-result').style.display = 'none';
  document.getElementById('cap-live-start-btn').disabled = true;
  document.getElementById('cap-live-stop-btn').disabled  = false;
  document.getElementById('cap-live-dot').innerHTML = '<span class="pulse"></span>';
  document.getElementById('cap-live-status').textContent = 'Polling — trigger your flow now...';

  liveCapSSE = new EventSource('/api/capture/live/stream');
  liveCapSSE.onmessage = (e) => {
    const item = JSON.parse(e.data);
    if (item.type === 'ping') return;

    const log  = document.getElementById('cap-live-log');
    const line = document.createElement('div');
    line.className = 'log-line log-' + item.type;
    line.textContent = new Date().toLocaleTimeString() + '  ' + item.msg;
    log.appendChild(line);
    log.scrollTop = log.scrollHeight;

    if (item.type === 'done') {
      liveCapSSE.close();
      document.getElementById('cap-live-start-btn').disabled = false;
      document.getElementById('cap-live-stop-btn').disabled  = true;
      document.getElementById('cap-live-dot').innerHTML = '';
      document.getElementById('cap-live-status').textContent = 'Done.';
      if (item.saved && item.saved.length > 0) {
        const el = document.getElementById('cap-live-result');
        el.style.display = 'block';
        document.getElementById('cap-live-result-body').innerHTML =
          `<p style="color:var(--log-pass,#86efac);margin-bottom:10px">✅ ${item.saved.length} golden snapshot(s) saved.</p>` +
          item.saved.map(k=>`<div style="font-family:monospace;font-size:12px;color:#a5b4fc;padding:2px 0">${k}</div>`).join('');
      }
    }
  };
}

async function stopLiveCapture() {
  await fetch('/api/capture/live/stop', {method:'POST'});
  if (liveCapSSE) { liveCapSSE.close(); liveCapSSE = null; }
  document.getElementById('cap-live-start-btn').disabled = false;
  document.getElementById('cap-live-stop-btn').disabled  = true;
  document.getElementById('cap-live-dot').innerHTML = '';
  document.getElementById('cap-live-status').textContent = 'Stopped.';
}

// ── Compare ───────────────────────────────────────────────────────────────────
let cmpFetchMode = 'time';  // 'time' or 'extid'
let cmpGoldenSource = 'db'; // 'db' | 'isd' | 'kowl'

let isdDataSource = 'db';   // when golden=isd: validate DB-data or Kowl-data
let topicGoldenSource = 'kowl';  // what golden the kowl panel compares against

// Pick comparison mode: db/isd golden, kowl golden, or standalone direct-JSON
function setCompareGolden(src) {
  ['db','isd','kowl','json','xml'].forEach(s =>
    document.getElementById('cmp-gs-' + s).classList.toggle('active', s === src));
  document.getElementById('cmp-isd-data').style.display = src === 'isd' ? 'flex' : 'none';
  if (src === 'kowl') {
    cmpGoldenSource = 'kowl'; topicGoldenSource = 'kowl';
    showComparePanel('kowl');
  } else if (src === 'json') {
    showComparePanel('json');
  } else if (src === 'xml') {
    showComparePanel('xml');
  } else if (src === 'db') {
    cmpGoldenSource = 'db'; showComparePanel('notif');
  } else if (src === 'isd') {
    cmpGoldenSource = 'isd';                 // golden source = isd
    setIsdDataSource(isdDataSource);         // data origin DB or Kowl
  }
}

// For ISD golden: choose whether live data comes from DB or Kowl
function setIsdDataSource(src) {
  isdDataSource = src;
  document.getElementById('cmp-isd-db').classList.toggle('active', src === 'db');
  document.getElementById('cmp-isd-kowl').classList.toggle('active', src === 'kowl');
  if (src === 'kowl') {
    topicGoldenSource = 'isd';               // kowl panel compares against ISD golden
    showComparePanel('kowl');
    initTopics();
  } else {
    showComparePanel('notif');               // DB-fetched notifications vs ISD golden
  }
}

// Toggle which compare panel is visible
function showComparePanel(which) {
  const isKowl = which === 'kowl', isJson = which === 'json', isXml = which === 'xml';
  const isDirect = isKowl || isJson || isXml;
  document.getElementById('cmp-tabs-row').style.display = isDirect ? 'none' : 'flex';
  document.getElementById('cmp-src-kowl').style.display = isKowl ? 'block' : 'none';
  document.getElementById('cmp-src-json').style.display = isJson ? 'block' : 'none';
  document.getElementById('cmp-src-xml').style.display  = isXml  ? 'block' : 'none';
  document.getElementById('cmp-src-notif').style.display = isDirect ? 'none' : 'block';
  if (which === 'notif') switchCmpTab(cmpFetchMode === 'extid' ? 'extid' : 'time');
  if (isKowl) initTopics();
}

function switchCmpTab(tab) {
  document.getElementById('cmp-tab-time').classList.toggle('active',  tab === 'time');
  document.getElementById('cmp-tab-extid').classList.toggle('active', tab === 'extid');
  cmpFetchMode = tab;
  document.getElementById('cmp-panel-time').style.display  = tab === 'time'  ? 'block' : 'none';
  document.getElementById('cmp-panel-extid').style.display = tab === 'extid' ? 'block' : 'none';
}

// ── Direct JSON compare ─────────────────────────────────────────────────────────
let jsonMode = 'full';
function setJsonMode(m) {
  jsonMode = m;
  document.getElementById('json-mode-full').classList.toggle('active',   m === 'full');
  document.getElementById('json-mode-schema').classList.toggle('active', m === 'schema');
}

function loadJsonFile(targetId, input) {
  if (!input.files.length) return;
  const reader = new FileReader();
  reader.onload = e => { document.getElementById(targetId).value = e.target.result; beautifyJson(targetId); };
  reader.readAsText(input.files[0]);
}

function jsonStatus(id, msg, ok) {
  const el = document.getElementById(id + '-status');
  if (el) { el.textContent = msg; el.style.color = ok ? '#86efac' : '#fca5a5'; }
}

function beautifyJson(id) {
  const ta = document.getElementById(id);
  const raw = ta.value.trim();
  if (!raw) return;
  try {
    const parsed = JSON.parse(raw);
    ta.value = JSON.stringify(parsed, null, 2);
    jsonStatus(id, '✓ valid JSON, beautified', true);
    // Render interactive tree
    const treeBox = document.getElementById(id + '-tree');
    if (treeBox) {
      treeBox.innerHTML = '';
      treeBox.appendChild(buildJsonTree(parsed));
      treeBox.style.display = 'block';
      ta.style.display = 'none';
    }
  } catch (e) {
    jsonStatus(id, '✗ invalid JSON: ' + e.message, false);
  }
}

function collapseJsonTree(id) {
  const treeBox = document.getElementById(id + '-tree');
  const ta = document.getElementById(id);
  if (treeBox) treeBox.style.display = 'none';
  if (ta) ta.style.display = '';
}

function buildJsonTree(value, key) {
  const wrap = document.createElement('div');
  wrap.className = 'jt-node';

  if (value === null || typeof value !== 'object') {
    // Leaf
    const leaf = document.createElement('span');
    leaf.className = 'jt-leaf';
    if (key !== undefined) {
      const k = document.createElement('span'); k.className = 'jt-key'; k.textContent = JSON.stringify(key) + ': ';
      leaf.appendChild(k);
    }
    const v = document.createElement('span');
    v.className = typeof value === 'string' ? 'jt-str' : typeof value === 'number' ? 'jt-num' : typeof value === 'boolean' ? 'jt-bool' : 'jt-null';
    v.textContent = JSON.stringify(value);
    leaf.appendChild(v);
    wrap.appendChild(leaf);
    return wrap;
  }

  const isArr = Array.isArray(value);
  const entries = isArr ? value.map((v, i) => [i, v]) : Object.entries(value);
  const count = entries.length;
  const open = count <= 5; // auto-expand small objects

  // Toggle button
  const toggle = document.createElement('button');
  toggle.className = 'jt-toggle';
  toggle.textContent = open ? '−' : '+';

  // Header line
  const header = document.createElement('div');
  header.className = 'jt-header';
  header.appendChild(toggle);

  if (key !== undefined) {
    const k = document.createElement('span'); k.className = 'jt-key'; k.textContent = JSON.stringify(key) + ': ';
    header.appendChild(k);
  }
  const bracket = document.createElement('span');
  bracket.className = 'jt-bracket';
  bracket.textContent = isArr ? '[' : '{';
  header.appendChild(bracket);

  const summary = document.createElement('span');
  summary.className = 'jt-summary';
  summary.textContent = ` ${count} ${isArr ? 'item' : 'key'}${count !== 1 ? 's' : ''} `;
  header.appendChild(summary);

  const closeBracket = document.createElement('span');
  closeBracket.className = 'jt-bracket';
  closeBracket.textContent = isArr ? ']' : '}';
  header.appendChild(closeBracket);

  wrap.appendChild(header);

  // Children container
  const children = document.createElement('div');
  children.className = 'jt-children';
  children.style.display = open ? '' : 'none';
  summary.style.display = open ? 'none' : '';
  closeBracket.style.display = open ? 'none' : '';

  for (const [k, v] of entries) {
    children.appendChild(buildJsonTree(v, isArr ? undefined : k));
  }

  // Closing bracket on its own line
  const close = document.createElement('div');
  close.className = 'jt-close';
  close.textContent = isArr ? ']' : '}';
  children.appendChild(close);

  wrap.appendChild(children);

  toggle.addEventListener('click', () => {
    const expanded = children.style.display !== 'none';
    children.style.display = expanded ? 'none' : '';
    toggle.textContent = expanded ? '+' : '−';
    summary.style.display = expanded ? '' : 'none';
    closeBracket.style.display = expanded ? '' : 'none';
  });

  return wrap;
}

function minifyJson(id) {
  const ta = document.getElementById(id);
  const raw = ta.value.trim();
  if (!raw) return;
  try {
    ta.value = JSON.stringify(JSON.parse(raw));
    jsonStatus(id, '✓ minified', true);
  } catch (e) {
    jsonStatus(id, '✗ invalid JSON: ' + e.message, false);
  }
}

async function doJsonCompare() {
  const btn = document.getElementById('json-cmp-btn');
  const err = document.getElementById('json-cmp-err');
  err.textContent = '';
  const a = document.getElementById('json-a').value.trim();
  const b = document.getElementById('json-b').value.trim();
  if (!a || !b) { err.textContent = 'Paste or upload JSON in both A and B.'; return; }
  btn.disabled = true; btn.textContent = '⏳ Comparing...';
  try {
    const res = await fetch('/api/compare/json', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({a, b, mode: jsonMode,
                            ignore_dynamic: document.getElementById('json-ignore-dyn').checked})
    });
    const data = await res.json();
    if (data.error) { err.textContent = data.error; }
    else { renderJsonCompare(data); }
  } catch (e) { err.textContent = 'Compare error: ' + e; }
  btn.disabled = false; btn.textContent = '🧩 Compare JSON';
}

function renderJsonCompare(data) {
  document.getElementById('json-summary').style.display = 'flex';
  const v = document.getElementById('json-verdict');
  v.textContent = data.status === 'PASS' ? '✅ MATCH' : '❌ MISMATCH';
  v.style.color = data.status === 'PASS' ? '#86efac' : '#fca5a5';
  document.getElementById('json-diffs').textContent = data.count;
  document.getElementById('json-result-card').style.display = 'block';
  const body = document.getElementById('json-result-body');
  body.innerHTML = '';
  renderResultRow({
    db_id: 'A↔B', create_time: 'direct', ext_id: '',
    key: jsonMode === 'schema' ? 'schema compare' : 'full compare',
    status: data.status, findings: data.findings, payload: data.payload
  }, 'json-result-body');
}

// ── Direct XML compare ──────────────────────────────────────────────────────────
let xmlMode = 'full';
function setXmlMode(m) {
  xmlMode = m;
  document.getElementById('xml-mode-full').classList.toggle('active',   m === 'full');
  document.getElementById('xml-mode-schema').classList.toggle('active', m === 'schema');
}

function loadXmlFile(targetId, input) {
  if (!input.files.length) return;
  const reader = new FileReader();
  reader.onload = e => {
    document.getElementById(targetId).value = e.target.result;
    jsonStatus(targetId, '✓ loaded ' + input.files[0].name, true);
  };
  reader.readAsText(input.files[0]);
}

async function doXmlCompare() {
  const btn = document.getElementById('xml-cmp-btn');
  const err = document.getElementById('xml-cmp-err');
  err.textContent = '';
  const a = document.getElementById('xml-a').value.trim();
  const b = document.getElementById('xml-b').value.trim();
  if (!a || !b) { err.textContent = 'Paste or upload XML in both A and B.'; return; }
  btn.disabled = true; btn.textContent = '⏳ Comparing...';
  try {
    const res = await fetch('/api/compare/xml', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({a, b, mode: xmlMode,
                            ignore_dynamic: document.getElementById('xml-ignore-dyn').checked})
    });
    const data = await res.json();
    if (data.error) { err.textContent = data.error; }
    else { renderXmlCompare(data); }
  } catch (e) { err.textContent = 'Compare error: ' + e; }
  btn.disabled = false; btn.textContent = '📰 Compare XML';
}

function renderXmlCompare(data) {
  document.getElementById('xml-summary').style.display = 'flex';
  const v = document.getElementById('xml-verdict');
  v.textContent = data.status === 'PASS' ? '✅ MATCH' : '❌ MISMATCH';
  v.style.color = data.status === 'PASS' ? '#86efac' : '#fca5a5';
  document.getElementById('xml-diffs').textContent = data.count;
  document.getElementById('xml-result-card').style.display = 'block';
  const body = document.getElementById('xml-result-body');
  body.innerHTML = '';
  renderResultRow({
    db_id: 'A↔B', create_time: 'direct', ext_id: '',
    key: xmlMode === 'schema' ? 'schema compare' : 'full compare',
    status: data.status, findings: data.findings, payload: data.payload
  }, 'xml-result-body');
}

async function doCompare() {
  const btn = document.getElementById('cmp-btn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> Comparing...';

  const pattern = document.getElementById('cmp-pattern').value.trim();
  const since  = cmpFetchMode === 'time'  ? datetimeLocalToISO(document.getElementById('cmp-since').value) : null;
  const ext_id = cmpFetchMode === 'extid' ? document.getElementById('cmp-extid').value.trim() : null;

  if (!pattern) {
    alert('Please enter a pattern.');
    btn.disabled = false;
    btn.innerHTML = '🔍 Compare';
    return;
  }
  if (cmpFetchMode === 'time' && !since) {
    alert('Please set a Since time or use By Request ID mode.');
    btn.disabled = false;
    btn.innerHTML = '🔍 Compare';
    return;
  }
  if (cmpFetchMode === 'extid' && !ext_id) {
    alert('Please enter an External Request ID.');
    btn.disabled = false;
    btn.innerHTML = '🔍 Compare';
    return;
  }

  try {
    const res = await fetch('/api/compare', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({pattern, since, ext_id, mode: modeState.cmp, golden_source: cmpGoldenSource})
    });
    const data = await res.json();

    if (!data.ok) { alert('Error: ' + data.error); return; }

    const pass = data.results.filter(r=>r.status==='PASS').length;
    const fail = data.results.filter(r=>r.status==='FAIL').length;
    const nog  = data.results.filter(r=>r.status==='NO GOLDEN').length;

    document.getElementById('cmp-total').textContent = data.total;
    document.getElementById('cmp-pass').textContent  = pass;
    document.getElementById('cmp-fail').textContent  = fail;
    document.getElementById('cmp-nogolden').textContent = nog;
    document.getElementById('cmp-summary').style.display = 'block';
    document.getElementById('cmp-results-card').style.display = 'block';

    const tbody = document.getElementById('cmp-results-body');
    tbody.innerHTML = '';
    if (data.results.length === 0) {
      tbody.innerHTML = '<tr><td colspan="5" class="no-results">No notifications found for this time range.</td></tr>';
    } else {
      data.results.forEach(r => renderResultRow(r, 'cmp-results-body'));
    }
  } catch(e) {
    alert('Error: ' + e.message);
  }

  btn.disabled = false;
  btn.innerHTML = '🔍 Compare';
}

// ── Watch ─────────────────────────────────────────────────────────────────────
let watchResults = [];
let watchSSE = null;

let watchGolden = 'db';
let watchIsdData = 'db';
function watchDataOrigin() {
  if (watchGolden === 'kowl') return 'kowl';
  if (watchGolden === 'isd' && watchIsdData === 'kowl') return 'kowl';
  return 'db';
}
// Kowl watching is topic-based — hide DB-only Flow/Subscriber/fetch controls.
function updateWatchControls() {
  const kowl = watchDataOrigin() === 'kowl';
  document.getElementById('watch-fetch-tabs').style.display = kowl ? 'none' : 'flex';
  document.getElementById('watch-sub-wrap').style.display   = kowl ? 'none' : 'block';
  document.getElementById('watch-kowl-note').style.display  = kowl ? 'block' : 'none';
  if (kowl && !document.getElementById('watch-fetch-panel-extid').style.display)
    document.getElementById('watch-fetch-panel-extid').style.display = 'none';
  if (kowl) {
    const names = (topicCfg.topics || []).map(t => t.label).join(', ') || 'none configured';
    document.getElementById('watch-kowl-topics').textContent = names;
  }
}
function setWatchGolden(src) {
  watchGolden = src;
  ['db','isd','kowl'].forEach(s =>
    document.getElementById('watch-gs-' + s).classList.toggle('active', s === src));
  document.getElementById('watch-isd-data').style.display = src === 'isd' ? 'flex' : 'none';
  updateWatchControls();
}
function setWatchIsdData(src) {
  watchIsdData = src;
  document.getElementById('watch-isd-db').classList.toggle('active', src === 'db');
  document.getElementById('watch-isd-kowl').classList.toggle('active', src === 'kowl');
  updateWatchControls();
}

async function startWatch() {
  const pattern = document.getElementById('watch-pattern').value.trim();
  const interval   = document.getElementById('watch-interval').value;

  let data;
  try {
    const res = await fetch('/api/watch/start', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({
        pattern, interval, mode: modeState.watch, golden_source: watchGolden,
        data_source: watchGolden === 'isd' ? watchIsdData : undefined,
        ext_id: watchFetchMode === 'extid' ? document.getElementById('watch-extid').value.trim() : null
      })
    });
    data = await res.json();
  } catch (e) { alert('Watch failed to start: ' + e); return; }
  if (!data.ok) { alert(data.error); return; }

  watchResults = [];
  document.getElementById('watch-results-body').innerHTML = '';
  document.getElementById('watch-log').innerHTML = '';

  document.getElementById('watch-start-btn').disabled = true;
  document.getElementById('watch-stop-btn').disabled  = false;
  setWatchModeLocked(true);
  document.getElementById('watch-status-dot').innerHTML = '<span class="pulse"></span>';
  document.getElementById('watch-status-text').textContent = 'Watching...';

  watchSSE = new EventSource('/api/watch/stream');
  watchSSE.onmessage = (e) => {
    const item = JSON.parse(e.data);
    if (item.type === 'ping') return;

    const log = document.getElementById('watch-log');

    // Scanning progress: update a sticky status line instead of spamming the log
    if (item.type === 'scanning') {
      let scanEl = document.getElementById('watch-scan-status');
      if (!scanEl) {
        scanEl = document.createElement('div');
        scanEl.id = 'watch-scan-status';
        scanEl.style.cssText = 'font-size:11px;color:#64748b;padding:2px 0;font-style:italic';
        log.appendChild(scanEl);
      }
      scanEl.textContent = item.msg;
      log.scrollTop = log.scrollHeight;
      return;
    }

    // Remove scan status line once a real event arrives
    const scanEl = document.getElementById('watch-scan-status');
    if (scanEl) scanEl.remove();

    const line = document.createElement('div');
    line.className = 'log-line log-' + item.type;
    line.textContent = new Date().toLocaleTimeString() + '  ' + item.msg;
    log.appendChild(line);
    log.scrollTop = log.scrollHeight;

    if (item.result) {
      watchResults.push(item.result);
      renderResultRow(item.result, 'watch-results-body');
      updateWatchCounters(watchResults);
    }

    if (item.type === 'done') {
      watchSSE.close();
      document.getElementById('watch-start-btn').disabled = false;
      document.getElementById('watch-stop-btn').disabled  = true;
      setWatchModeLocked(false);
      document.getElementById('watch-status-dot').innerHTML = '';
      document.getElementById('watch-status-text').textContent = 'Idle';
    }
  };
}

async function stopWatch() {
  await fetch('/api/watch/stop', {method:'POST'});
  if (watchSSE) { watchSSE.close(); watchSSE = null; }
  document.getElementById('watch-start-btn').disabled = false;
  document.getElementById('watch-stop-btn').disabled  = true;
  setWatchModeLocked(false);
  document.getElementById('watch-status-dot').innerHTML = '';
  document.getElementById('watch-status-text').textContent = 'Stopped';
}

// ── Mode Toggle ──────────────────────────────────────────────────────────────
const modeState = { cmp: 'full', watch: 'full', fullrun: 'full' };
let watchModeLocked = false;  // mode can't change mid-run — the watch thread is pinned to its start-time mode
let fullRunModeLocked = false;

function setWatchModeLocked(locked) {
  watchModeLocked = locked;
  const wrap = document.getElementById('watch-mode-wrap');
  const hint = document.getElementById('watch-mode-hint');
  wrap.classList.toggle('disabled', locked);
  if (locked) {
    hint.textContent = 'Locked while watching — stop the run to change comparison mode';
  } else {
    // restore the hint for the current mode
    hint.textContent = modeState.watch === 'schema'
      ? 'Only checks for missing or extra keys — ignores value changes'
      : 'Compares keys, values, and types';
  }
}

function setFullRunModeLocked(locked) {
  fullRunModeLocked = locked;
  const wrap = document.getElementById('fullrun-mode-wrap');
  const hint = document.getElementById('fullrun-mode-hint');
  wrap.classList.toggle('disabled', locked);
  if (locked) {
    hint.textContent = 'Locked while running — stop the Full Run to change comparison mode';
  } else {
    hint.textContent = modeState.fullrun === 'schema'
      ? 'Only checks for missing or extra keys — ignores value changes'
      : 'Compares keys, values, and types';
  }
}

function toggleMode(prefix) {
  if (prefix === 'watch' && watchModeLocked) return;     // ignore clicks during a live run
  if (prefix === 'fullrun' && fullRunModeLocked) return; // ignore clicks during a full run
  const isSchema = modeState[prefix] === 'full';  // about to flip to schema
  modeState[prefix] = isSchema ? 'schema' : 'full';

  const wrap  = document.getElementById(prefix + '-mode-wrap');
  const label = document.getElementById(prefix + '-mode-label');
  const hint  = document.getElementById(prefix + '-mode-hint');

  if (isSchema) {
    wrap.classList.add('on');
    label.textContent = 'Schema Only';
    label.style.color = '#818cf8';
    hint.textContent  = 'Only checks for missing or extra keys — ignores value changes';
  } else {
    wrap.classList.remove('on');
    label.textContent = 'Full Compare';
    label.style.color = '#94a3b8';
    hint.textContent  = 'Compares keys, values, and types';
  }
}

function parsePatternsTextarea(text) {
  return (text || '').split('\n').map(l => l.trim()).filter(Boolean).map(line => {
    const i = line.indexOf('=');
    if (i === -1) return {label: line, pattern: line};
    return {label: line.slice(0, i).trim() || line.slice(i + 1).trim(), pattern: line.slice(i + 1).trim()};
  }).filter(p => p.pattern);
}

function refreshPatternsDatalist(patterns) {
  const dl = document.getElementById('cfg-patterns-datalist');
  if (!dl) return;
  dl.innerHTML = (patterns || []).map(p => `<option value="${p.pattern}">${p.label}</option>`).join('');
}

// ── Config ───────────────────────────────────────────────────────────────────
async function loadConfig() {
  let cfg;
  try {
    const res = await fetch('/api/config');
    cfg = await res.json();
  } catch (e) { console.error('Failed to load config:', e); return; }
  // non-secret fields from disk
  document.getElementById('cfg-ssh-host').value  = cfg.ssh_host  || '';
  document.getElementById('cfg-ssh-host-b').value = cfg.ssh_host_b || '';
  document.getElementById('cfg-ssh-port').value  = cfg.ssh_port  || 22;
  document.getElementById('cfg-ssh-user').value  = cfg.ssh_user  || '';
  document.getElementById('cfg-db-host').value   = cfg.db_host   || '';
  document.getElementById('cfg-db-host-b').value = cfg.db_host_b || '';
  document.getElementById('cfg-db-port').value   = cfg.db_port   || 5432;
  document.getElementById('cfg-db-name').value   = cfg.db_name   || '';
  document.getElementById('cfg-db-table').value  = cfg.db_table  || '';
  document.getElementById('cfg-db-user').value   = cfg.db_user   || '';
  document.getElementById('cfg-poll').value      = cfg.poll_interval || 3;
  document.getElementById('cfg-project').value   = cfg.project || '';
  // project autocomplete from existing golden project folders
  try {
    const pj = await (await fetch('/api/projects')).json();
    document.getElementById('cfg-project-list').innerHTML =
      (pj.projects || []).map(p => `<option value="${p}">`).join('');
  } catch (e) {}
  // topic compare settings
  document.getElementById('cfg-topic-host').value     = cfg.topic_host     || '';
  document.getElementById('cfg-topic-host-b').value   = cfg.topic_host_b   || '';
  document.getElementById('cfg-topic-prefix').value   = cfg.topic_prefix   || '';
  document.getElementById('cfg-topic-prefix-b').value = cfg.topic_prefix_b || '';
  document.getElementById('cfg-topic-count').value    = cfg.topic_count    || 50;
  document.getElementById('cfg-topics').value =
    (cfg.topics || []).map(t => `${t.label} = ${t.topic}`).join('\n');
  updateTopicsCount();
  topicCfg = {
    host:     cfg.topic_host     || '',
    host_b:   cfg.topic_host_b   || '',
    prefix:   cfg.topic_prefix   || '',
    prefix_b: cfg.topic_prefix_b || '',
    count:    cfg.topic_count    || 50,
    topics:   cfg.topics || [],
  };
  // notification patterns
  document.getElementById('cfg-patterns').value =
    (cfg.patterns || []).map(p => `${p.label} = ${p.pattern}`).join('\n');
  refreshPatternsDatalist(cfg.patterns || []);
  // secrets — never pre-filled, always blank on load
  document.getElementById('cfg-ssh-key').value  = '';
  document.getElementById('cfg-db-pass').value  = '';
  document.getElementById('cfg-db-pass-b').value = '';
  // show banner if secrets not yet set
  document.getElementById('cfg-secrets-banner').style.display = cfg.secrets_ready ? 'none' : 'block';
  // check if secrets were auto-loaded from .secrets file
  checkSavedSecretsStatus();
}

function toggleVisible(inputId, btnId) {
  const inp  = document.getElementById(inputId);
  const btn  = typeof btnId === 'string' ? document.getElementById(btnId) : btnId;
  const show = inp.type === 'password';
  inp.type = show ? 'text' : 'password';
  btn.textContent = show ? '🙈 Hide' : '👁 Show';
}

async function checkSavedSecretsStatus() {
  const res  = await fetch('/api/secrets/saved');
  const data = await res.json();
  const banner = document.getElementById('cfg-secrets-loaded-banner');
  if (banner) banner.style.display = data.saved ? 'flex' : 'none';
}

async function saveSecrets() {
  const btn       = document.getElementById('cfg-secrets-btn');
  const status    = document.getElementById('cfg-secrets-status');
  const ssh_key   = document.getElementById('cfg-ssh-key').value.trim();
  const db_pass   = document.getElementById('cfg-db-pass').value;
  const db_pass_b = document.getElementById('cfg-db-pass-b').value;
  const saveDisk  = document.getElementById('cfg-save-disk').checked;

  if (!ssh_key && !db_pass && !db_pass_b) {
    status.textContent = '❌ Enter a password and/or SSH key';
    status.style.color = '#fca5a5';
    return;
  }
  btn.disabled = true;
  const res  = await fetch('/api/secrets', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ssh_key, db_pass, db_pass_b, save_to_disk: saveDisk})
  });
  const data = await res.json();
  if (data.ok) {
    const msg = saveDisk ? '✅ Secrets set & saved to disk' : '✅ Secrets set for this session';
    status.textContent = msg;
    status.style.color = '#86efac';
    document.getElementById('cfg-secrets-banner').style.display = 'none';
    document.getElementById('cfg-ssh-key').value  = '';
    document.getElementById('cfg-db-pass').value  = '';
    document.getElementById('cfg-db-pass-b').value = '';
    document.getElementById('cfg-save-disk').checked = false;
    checkSavedSecretsStatus();
  } else {
    status.textContent = '❌ Failed';
    status.style.color = '#fca5a5';
  }
  btn.disabled = false;
  setTimeout(() => status.textContent = '', 4000);
}

async function clearSavedSecrets() {
  await fetch('/api/secrets/clear', {method:'POST'});
  document.getElementById('cfg-secrets-loaded-banner').style.display = 'none';
}

// Returns true if saved successfully, false if validation failed or error.
// Pass silent=true to suppress the status message (used by testConnection).
async function saveConfig(silent = false) {
  const btn    = document.getElementById('cfg-save-btn');
  const status = document.getElementById('cfg-status');
  const errEl  = document.getElementById('cfg-pattern-error');

  const patterns = parsePatternsTextarea(document.getElementById('cfg-patterns').value);

  // At least one pattern required
  if (!patterns.length) {
    errEl.style.display = 'block';
    if (!silent) {
      status.textContent = '❌ At least one pattern is required';
      status.style.color = '#fca5a5';
    }
    return false;
  }
  errEl.style.display = 'none';

  btn.disabled = true;
  const payload = {
    ssh_host:          document.getElementById('cfg-ssh-host').value,
    ssh_host_b:        document.getElementById('cfg-ssh-host-b').value,
    ssh_port:          document.getElementById('cfg-ssh-port').value,
    ssh_user:          document.getElementById('cfg-ssh-user').value,
    db_host:           document.getElementById('cfg-db-host').value,
    db_host_b:         document.getElementById('cfg-db-host-b').value,
    db_port:           document.getElementById('cfg-db-port').value,
    db_name:           document.getElementById('cfg-db-name').value,
    db_table:          document.getElementById('cfg-db-table').value,
    db_user:           document.getElementById('cfg-db-user').value,
    patterns:          patterns,
    poll_interval:     document.getElementById('cfg-poll').value,
    project:           document.getElementById('cfg-project').value.trim(),
    topic_host:        document.getElementById('cfg-topic-host').value.trim(),
    topic_host_b:      document.getElementById('cfg-topic-host-b').value.trim(),
    topic_prefix:      document.getElementById('cfg-topic-prefix').value.trim(),
    topic_prefix_b:    document.getElementById('cfg-topic-prefix-b').value.trim(),
    topic_count:       document.getElementById('cfg-topic-count').value || 50,
    topics:            parseTopicsTextarea(document.getElementById('cfg-topics').value),
  };
  const res = await fetch('/api/config', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  });
  const data = await res.json();
  btn.disabled = false;
  if (!silent) {
    status.textContent = data.ok ? '✅ Saved!' : '❌ ' + data.error;
    status.style.color = data.ok ? '#86efac' : '#fca5a5';
    setTimeout(() => status.textContent = '', 3000);
  }
  if (data.ok) {
    refreshPatternsDatalist(patterns);
    document.getElementById('watch-interval').value = payload.poll_interval;
  }
  return data.ok;
}

async function testConnection(target = false) {
  const btn    = document.getElementById(target ? 'cfg-test-btn-b' : 'cfg-test-btn');
  const status = document.getElementById('cfg-status');
  // Save first (with validation) — if save fails, abort
  const saved = await saveConfig(true);
  if (!saved) return;
  btn.disabled = true;
  status.textContent = '🔄 Testing...';
  status.style.color = '#93c5fd';
  const res  = await fetch('/api/config/test', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({target})
  });
  const data = await res.json();
  status.textContent = data.msg;
  status.style.color = data.ok ? '#86efac' : '#fca5a5';
  btn.disabled = false;
}

async function saveProjectDefaults() {
  const btn     = document.getElementById('cfg-project-save-btn');
  const input   = document.getElementById('cfg-project');
  const project = input.value.trim();
  if (!project) {
    input.focus();
    input.style.borderColor = '#f43f5e';
    input.style.boxShadow   = '0 0 0 3px rgba(244,63,94,.2)';
    btn.textContent = '❌ Project name is required';
    setTimeout(() => {
      input.style.borderColor = '';
      input.style.boxShadow   = '';
      btn.textContent = '💾 Save';
    }, 2500);
    return;
  }
  btn.disabled = true;
  btn.textContent = 'Saving…';
  try {
    const res  = await fetch('/api/config');
    const cfg  = await res.json();
    cfg.project      = project;
    cfg.poll_interval = parseInt(document.getElementById('cfg-poll').value) || cfg.poll_interval;
    const r = await fetch('/api/config', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(cfg) });
    const d = await r.json();
    btn.textContent = d.ok ? '✓ Saved' : '❌ Failed';
    setTimeout(() => { btn.textContent = '💾 Save'; btn.disabled = false; }, 2000);
  } catch(e) {
    btn.textContent = '❌ Error';
    setTimeout(() => { btn.textContent = '💾 Save'; btn.disabled = false; }, 2000);
  }
}

async function saveKowlConfig() {
  const btn    = document.getElementById('cfg-kowl-save-btn');
  const status = document.getElementById('cfg-kowl-status');
  btn.disabled = true;
  const payload = {
    topic_host:     document.getElementById('cfg-topic-host').value.trim(),
    topic_host_b:   document.getElementById('cfg-topic-host-b').value.trim(),
    topic_prefix:   document.getElementById('cfg-topic-prefix').value.trim(),
    topic_prefix_b: document.getElementById('cfg-topic-prefix-b').value.trim(),
    topic_count:    document.getElementById('cfg-topic-count').value || 50,
    topics:         parseTopicsTextarea(document.getElementById('cfg-topics').value),
  };
  const res  = await fetch('/api/config', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  });
  const data = await res.json();
  btn.disabled = false;
  status.textContent = data.ok ? '✅ Saved!' : '❌ ' + data.error;
  status.style.color = data.ok ? '#86efac' : '#fca5a5';
  setTimeout(() => status.textContent = '', 3000);
  if (data.ok) {
    // Keep in-memory topicCfg in sync so compare/capture pick up changes immediately
    topicCfg = {
      host:     payload.topic_host,
      host_b:   payload.topic_host_b,
      prefix:   payload.topic_prefix,
      prefix_b: payload.topic_prefix_b,
      count:    parseInt(payload.topic_count) || 50,
      topics:   payload.topics,
    };
  }
  return data.ok;
}

async function testKowlConnection() {
  const btn    = document.getElementById('cfg-kowl-test-btn');
  const status = document.getElementById('cfg-kowl-status');
  const saved  = await saveKowlConfig();
  if (!saved) return;
  btn.disabled = true;
  status.textContent = '🔄 Testing Kowl...';
  status.style.color = '#93c5fd';
  const res  = await fetch('/api/config/test-kowl', {method:'POST'});
  const data = await res.json();
  status.textContent = data.msg;
  status.style.color = data.ok ? '#86efac' : '#fca5a5';
  btn.disabled = false;
}

// ── Init ─────────────────────────────────────────────────────────────────────
loadGoldens();
loadConfig();
loadReports();
checkAllureStatus();   // dashboard is the default page — warn early if allure CLI is missing
// Set default "since" to 1 hour ago
const now = new Date(); now.setHours(now.getHours() - 1);
const iso = now.toISOString().slice(0,16);
document.getElementById('cmp-since').value = iso;
// cap-since intentionally left blank (fetch last 100 by default)

// ── Topic Compare ──────────────────────────────────────────────────────────────
let topicCfg  = {host:'', host_b:'', count:50, topics:[]};
let topicMode = 'full';
let topicsInited = false;

function parseTopicsTextarea(text) {
  return (text || '').split('\n').map(l => l.trim()).filter(Boolean).map(line => {
    const i = line.indexOf('=');
    if (i === -1) return {label: 'TOPIC', topic: line};
    return {label: line.slice(0, i).trim() || 'TOPIC', topic: line.slice(i + 1).trim()};
  }).filter(t => t.topic);
}

function updateTopicsCount() {
  const el = document.getElementById('cfg-topics-count');
  if (el) el.textContent = parseTopicsTextarea(document.getElementById('cfg-topics').value).length;
}

function setTopicMode(m) {
  topicMode = m;
  document.getElementById('tc-mode-full').classList.toggle('active',   m === 'full');
  document.getElementById('tc-mode-schema').classList.toggle('active', m === 'schema');
}

async function initTopics() {
  // pull latest config so hosts/topics reflect the Config tab
  try {
    const cfg = await (await fetch('/api/config')).json();
    topicCfg = {host: cfg.topic_host || '', host_b: cfg.topic_host_b || '',
                prefix: cfg.topic_prefix || '', prefix_b: cfg.topic_prefix_b || '',
                count: cfg.topic_count || 50, topics: cfg.topics || []};
  } catch (e) {}
  if (!document.getElementById('tc-cap-host').value) document.getElementById('tc-cap-host').value = topicCfg.host;
  if (!document.getElementById('tc-cap-count').value) document.getElementById('tc-cap-count').value = topicCfg.count;
  if (!document.getElementById('tc-cmp-host').value) document.getElementById('tc-cmp-host').value = topicCfg.host_b || topicCfg.host;
  if (!document.getElementById('tc-cmp-count').value) document.getElementById('tc-cmp-count').value = topicCfg.count;
  document.getElementById('tc-cap-topics').innerHTML =
    topicCfg.topics.length
      ? topicCfg.topics.map(t => `<div style="padding:3px 0">• <b>${t.label}</b> — ${t.topic}</div>`).join('')
      : '<span style="color:var(--log-fail,#fca5a5)">No topics configured. Add them on the Config tab.</span>';
  loadTopicBaselines();
  topicsInited = true;
}

async function loadTopicBaselines() {
  const res  = await fetch('/api/topics/baselines');
  const keys = await res.json();
  document.getElementById('tc-baseline-count').textContent = keys.length;
  const el = document.getElementById('tc-baseline-list');
  if (!keys.length) { el.innerHTML = '<div class="no-results">No baseline captured yet.</div>'; return; }
  el.innerHTML = keys.map(k => `
    <div class="golden-item">
      <span class="golden-name">${k}</span>
      <div class="golden-actions">
        <button class="btn-xs btn-xs-view" onclick="viewTopicBaseline('${k}')">View</button>
        <button class="btn-xs btn-xs-del"  onclick="deleteTopicBaseline('${k}')">Delete</button>
      </div>
    </div>`).join('');
}

async function viewTopicBaseline(key) {
  const res  = await fetch('/api/topics/baseline/' + encodeURIComponent(key));
  const data = await res.json();
  document.getElementById('modal-title').textContent = key;
  document.getElementById('modal-body').textContent  = JSON.stringify(data, null, 2);
  document.getElementById('modal').classList.add('open');
}

async function deleteTopicBaseline(key) {
  if (!confirm('Delete baseline ' + key + '?')) return;
  await fetch('/api/topics/baseline/' + encodeURIComponent(key), {method:'DELETE'});
  loadTopicBaselines();
}

let kowlCapSSE = null;
async function startKowlCapture() {
  const host = document.getElementById('tc-cap-host').value.trim();
  const interval = document.getElementById('kc-interval').value;
  if (!host) { alert('Enter the Kowl host:port above'); return; }
  if (!topicCfg.topics.length) { alert('No topics configured. Add them on the Config tab.'); return; }
  const res = await fetch('/api/kowl-capture/start', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({host, interval})
  });
  const data = await res.json();
  if (!data.ok) { alert(data.error); return; }
  document.getElementById('kc-start-btn').disabled = true;
  document.getElementById('kc-stop-btn').disabled  = false;
  document.getElementById('kc-dot').innerHTML = '<span class="pulse"></span>';
  document.getElementById('kc-status').textContent = 'Capturing — run your flow...';
  const log = document.getElementById('kc-log');
  log.style.display = 'block'; log.innerHTML = '';
  kowlCapSSE = new EventSource('/api/kowl-capture/stream');
  kowlCapSSE.onmessage = (e) => {
    const item = JSON.parse(e.data);
    if (item.type === 'ping') return;
    const line = document.createElement('div');
    line.className = 'log-line log-' + item.type;
    line.textContent = new Date().toLocaleTimeString() + '  ' + item.msg;
    log.appendChild(line); log.scrollTop = log.scrollHeight;
    if (item.type === 'done') {
      kowlCapSSE.close();
      document.getElementById('kc-start-btn').disabled = false;
      document.getElementById('kc-stop-btn').disabled  = true;
      document.getElementById('kc-dot').innerHTML = '';
      document.getElementById('kc-status').textContent = 'Idle';
      loadTopicBaselines();
    }
  };
}
async function stopKowlCapture() {
  await fetch('/api/kowl-capture/stop', {method:'POST'});
  document.getElementById('kc-status').textContent = 'Stopping...';
}

let _tcCaptureSSE = null;

function stopCaptureTopics() {
  if (_tcCaptureSSE) { _tcCaptureSSE.close(); _tcCaptureSSE = null; }
  fetch('/api/topics/capture/stop', { method: 'POST' }).catch(() => {});
  document.getElementById('tc-cap-btn').disabled = false;
  document.getElementById('tc-cap-btn').textContent = '📥 Capture Baseline (snapshot)';
  document.getElementById('tc-cap-stop-btn').style.display = 'none';
}

async function captureTopics() {
  const btn  = document.getElementById('tc-cap-btn');
  const stopBtn = document.getElementById('tc-cap-stop-btn');
  const host = document.getElementById('tc-cap-host').value.trim();
  const count = parseInt(document.getElementById('tc-cap-count').value) || 50;
  if (!host) { alert('Enter the baseline Kowl host:port'); return; }
  if (!topicCfg.topics.length) { alert('No topics configured. Add them on the Config tab.'); return; }
  btn.disabled = true; btn.textContent = '⏳ Capturing...';
  stopBtn.style.display = 'inline-flex';

  const card = document.getElementById('tc-cap-result');
  const body = document.getElementById('tc-cap-result-body');
  card.style.display = 'block';
  body.innerHTML = '<div style="font-size:12px;color:var(--text-muted)">Starting capture...</div>';

  try {
    const res = await fetch('/api/topics/capture/start', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({host, count, prefix: topicCfg.prefix, topics: topicCfg.topics})
    });
    const start = await res.json();
    if (start.error) { alert('Capture failed: ' + start.error); btn.disabled = false; btn.textContent = '📥 Capture Baseline (snapshot)'; stopBtn.style.display = 'none'; return; }

    const progressLines = {};  // topic -> element id
    const progressBar = document.createElement('div');
    progressBar.style.cssText = 'font-size:12px;color:var(--text-muted);margin-bottom:8px';
    body.innerHTML = '';
    body.appendChild(progressBar);

    await new Promise((resolve) => {
      const sse = new EventSource('/api/topics/capture/stream');
      _tcCaptureSSE = sse;
      sse.onmessage = (e) => {
        const item = JSON.parse(e.data);
        if (item.type === 'ping') return;

        if (item.type === 'progress') {
          progressBar.textContent = `⏳ [${item.current}/${item.total}] Fetching: ${item.topic}`;
        } else if (item.type === 'ok') {
          const div = document.createElement('div');
          div.style.cssText = 'font-family:monospace;font-size:11px;color:var(--log-pass,#86efac);padding:1px 0';
          div.textContent = item.msg;
          body.insertBefore(div, progressBar);
        } else if (item.type === 'topic_error') {
          const div = document.createElement('div');
          div.style.cssText = 'font-family:monospace;font-size:11px;color:var(--log-fail,#fca5a5);padding:1px 0';
          div.textContent = item.msg;
          body.insertBefore(div, progressBar);
        } else if (item.type === 'done') {
          sse.close();
          _tcCaptureSSE = null;
          stopBtn.style.display = 'none';
          progressBar.remove();
          // Summary
          const summary = document.createElement('div');
          summary.style.cssText = 'font-size:12px;margin-top:8px;padding-top:8px;border-top:1px solid #334155';
          summary.innerHTML = item.keys
            ? `<span style="color:var(--log-pass,#86efac)">✅ Done — ${item.keys} key(s) from ${item.messages} message(s)</span>`
            : `<span style="color:#fbbf24">⚠️ No messages captured — topics may be empty</span>`;
          body.appendChild(summary);
          loadTopicBaselines();
          resolve();
        } else if (item.type === 'error') {
          sse.close();
          _tcCaptureSSE = null;
          stopBtn.style.display = 'none';
          const summary = document.createElement('div');
          summary.style.cssText = 'font-size:12px;margin-top:8px;padding-top:8px;border-top:1px solid #334155;color:#fbbf24';
          summary.textContent = '⏹ ' + (item.msg || 'Stopped.');
          body.appendChild(summary);
          resolve();
        }
      };
      sse.onerror = () => { sse.close(); resolve(); };
    });
  } catch (e) { alert('Capture error: ' + e); }
  btn.disabled = false; btn.textContent = '📥 Capture Baseline';
}

let _tcCompareSSE = null;

function stopCompareTopics() {
  if (_tcCompareSSE) { _tcCompareSSE.close(); _tcCompareSSE = null; }
  fetch('/api/topics/compare/stop', { method: 'POST' }).catch(() => {});
  document.getElementById('tc-cmp-btn').disabled = false;
  document.getElementById('tc-cmp-btn').textContent = '🔍 Compare';
  document.getElementById('tc-stop-btn').style.display = 'none';
  const progressEl = document.getElementById('tc-cmp-progress');
  if (progressEl) progressEl.textContent = '⏹ Stopped by user.';
}

async function compareTopics() {
  const btn      = document.getElementById('tc-cmp-btn');
  const stopBtn  = document.getElementById('tc-stop-btn');
  const host = document.getElementById('tc-cmp-host').value.trim();
  const count = parseInt(document.getElementById('tc-cmp-count').value) || 50;
  if (!host) { alert('Enter the target Kowl host:port'); return; }
  if (!topicCfg.topics.length) { alert('No topics configured. Add them on the Config tab.'); return; }
  btn.disabled = true; btn.textContent = '⏳ Comparing...';
  stopBtn.style.display = 'inline-flex';

  // Show progress area
  const resultCard = document.getElementById('tc-cmp-result');
  const body = document.getElementById('tc-results-body');
  resultCard.style.display = 'block';
  document.getElementById('tc-summary').style.display = 'none';
  body.innerHTML = '<tr><td colspan="6"><div style="font-size:12px;padding:8px">Starting compare...</div></td></tr>';

  try {
    const res = await fetch('/api/topics/compare/start', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({host, count, mode: topicMode, topics: topicCfg.topics,
                            prefix: topicCfg.prefix_b, golden_source: topicGoldenSource})
    });
    const start = await res.json();
    if (start.error) { alert('Compare failed: ' + start.error); btn.disabled = false; btn.textContent = '🔍 Compare'; stopBtn.style.display = 'none'; return; }

    const progressRow = document.createElement('tr');
    progressRow.innerHTML = '<td colspan="6"><div id="tc-cmp-progress" style="font-size:12px;padding:8px">Fetching topics...</div></td>';
    body.innerHTML = '';
    body.appendChild(progressRow);
    const progressEl = document.getElementById('tc-cmp-progress');

    await new Promise((resolve) => {
      const sse = new EventSource('/api/topics/compare/stream');
      _tcCompareSSE = sse;
      sse.onmessage = (e) => {
        const item = JSON.parse(e.data);
        if (item.type === 'ping') return;

        if (item.type === 'progress') {
          progressEl.textContent = `⏳ [${item.current}/${item.total}] Comparing: ${item.topic}`;
        } else if (item.type === 'ok') {
          const info = document.createElement('tr');
          info.innerHTML = `<td colspan="6"><div style="font-family:monospace;font-size:11px;color:var(--log-pass,#22c55e);padding:2px 8px">${item.msg}</div></td>`;
          body.insertBefore(info, progressRow);
        } else if (item.type === 'topic_error') {
          const info = document.createElement('tr');
          info.innerHTML = `<td colspan="6"><div style="font-family:monospace;font-size:11px;color:var(--log-fail,#f87171);padding:2px 8px">${item.msg}</div></td>`;
          body.insertBefore(info, progressRow);
        } else if (item.type === 'done') {
          sse.close(); _tcCompareSSE = null;
          progressRow.remove();
          renderTopicResults(item.results || []);
          const dl = document.getElementById('tc-report-dl');
          if (item.report) {
            dl.href = '/api/report/' + encodeURIComponent(item.report) + '?download=1';
            dl.style.display = 'inline-flex';
          } else {
            dl.style.display = 'none';
          }
          if (typeof loadReports === 'function') loadReports();
          resolve();
        } else if (item.type === 'error') {
          sse.close(); _tcCompareSSE = null;
          alert('Compare error: ' + item.msg);
          resolve();
        }
      };
      sse.onerror = () => { sse.close(); _tcCompareSSE = null; resolve(); };
    });
  } catch (e) { alert('Compare error: ' + e); }
  btn.disabled = false; btn.textContent = '🔍 Compare';
  stopBtn.style.display = 'none';
}

function renderTopicResults(results) {
  const body = document.getElementById('tc-results-body');
  body.innerHTML = '';
  document.getElementById('tc-cmp-result').style.display = 'block';
  document.getElementById('tc-summary').style.display = 'flex';
  document.getElementById('tc-total').textContent = results.length;
  document.getElementById('tc-pass').textContent  = results.filter(r => r.status === 'PASS').length;
  document.getElementById('tc-fail').textContent  = results.filter(r => r.status === 'FAIL').length;
  document.getElementById('tc-nob').textContent   = results.filter(r => r.status !== 'PASS' && r.status !== 'FAIL').length;
  if (!results.length) {
    body.innerHTML = '<tr><td colspan="6"><div class="no-results">No messages returned from the target topics.</div></td></tr>';
    return;
  }
  results.forEach(r => renderResultRow(r, 'tc-results-body'));
}

// ── Dashboard: Run All + Reports ───────────────────────────────────────────────
let runAllMode = 'full';
let runAllGolden = 'db';
function setRunAllMode(m) {
  runAllMode = m;
  document.getElementById('runall-mode-full').classList.toggle('active',   m === 'full');
  document.getElementById('runall-mode-schema').classList.toggle('active', m === 'schema');
}
function setRunAllGolden(src) {
  runAllGolden = src;
  ['db','isd','kowl'].forEach(s =>
    document.getElementById('runall-gs-' + s).classList.toggle('active', s === src));
  // 'since' only applies to DB-fetched flows, not the kowl topic sweep
  document.getElementById('runall-since-wrap').style.display = src === 'kowl' ? 'none' : 'block';
}

async function runAll() {
  const btn = document.getElementById('runall-btn');
  const since = datetimeLocalToISO(document.getElementById('runall-since').value);
  btn.disabled = true; btn.textContent = '⏳ Running…';
  try {
    const res = await fetch('/api/run-all', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({since, mode: runAllMode, source: runAllGolden})
    });
    const data = await res.json();
    if (data.error) { alert('Run All failed: ' + data.error); }
    else { renderRunAll(data); loadReports(); }
  } catch (e) { alert('Run All error: ' + e); }
  btn.disabled = false; btn.textContent = '🚀 Run All';
}

function drawDonut(pass, fail, other) {
  const total = pass + fail + other;
  const card = document.getElementById('runall-chart-card');
  card.style.display = total ? 'block' : 'none';
  if (!total) return;
  const pPass = pass / total * 100;
  const pFail = pPass + fail / total * 100;
  document.getElementById('ra-donut').style.background =
    `conic-gradient(#22c55e 0 ${pPass}%, #ef4444 ${pPass}% ${pFail}%, #eab308 ${pFail}% 100%)`;
  document.getElementById('ra-donut-pct').textContent = Math.round(pass / total * 100) + '%';
  document.getElementById('ra-leg-pass').textContent  = pass;
  document.getElementById('ra-leg-fail').textContent  = fail;
  document.getElementById('ra-leg-other').textContent = other;
}

function renderRunAll(data) {
  const results = data.results || [];
  document.getElementById('runall-summary').style.display = 'flex';
  document.getElementById('runall-result').style.display  = 'block';
  const np = results.filter(r=>r.status==='PASS').length;
  const nf = results.filter(r=>r.status==='FAIL').length;
  const no = results.length - np - nf;
  document.getElementById('ra-total').textContent = results.length;
  document.getElementById('ra-pass').textContent  = np;
  document.getElementById('ra-fail').textContent  = nf;
  document.getElementById('ra-other').textContent = no;
  drawDonut(np, nf, no);

  const pf = data.per_flow || {};
  document.getElementById('runall-perflow').innerHTML = Object.keys(pf).length
    ? Object.entries(pf).map(([flow,s]) =>
        `<span class="badge badge-info" style="margin-right:8px">${flow}: ${s.pass}/${s.total} pass${s.fail?`, ${s.fail} fail`:''}</span>`).join('')
    : '<span style="color:#fcd34d;font-size:12px">No patterns configured — set them on the Config tab.</span>';

  if (data.report) {
    const dl = document.getElementById('runall-download');
    dl.href = '/api/report/' + encodeURIComponent(data.report) + '?download=1';
  }

  const body = document.getElementById('runall-body');
  body.innerHTML = '';
  if (!results.length) {
    body.innerHTML = '<tr><td colspan="6"><div class="no-results">No notifications found for the configured flows.</div></td></tr>';
    return;
  }
  results.forEach(r => {
    if (r.flow) r.create_time = r.flow + ' · ' + r.create_time;
    renderResultRow(r, 'runall-body');
  });
}

let _allReports = [];

async function loadReports() {
  try {
    _allReports = await (await fetch('/api/reports')).json();
    renderReports();
  } catch (e) {}
}

function renderReports() {
  const el = document.getElementById('dash-reports');
  if (!el) return;
  const searchEl = document.getElementById('rep-search');
  const sortEl   = document.getElementById('rep-sort');
  const q    = (searchEl ? searchEl.value : '').trim().toLowerCase();
  const sort = sortEl ? sortEl.value : 'time-desc';

  let reports = _allReports.filter(rep =>
    !q || rep.name.toLowerCase().includes(q) || (rep.project || '').toLowerCase().includes(q));

  reports = reports.slice().sort((a, b) => {
    if (sort === 'name-asc')  return a.name.localeCompare(b.name);
    if (sort === 'name-desc') return b.name.localeCompare(a.name);
    if (sort === 'time-asc')  return (a.id || 0) - (b.id || 0);
    return (b.id || 0) - (a.id || 0);  // time-desc (default, newest first)
  });

  el.innerHTML = reports.length
    ? reports.map(rep => {
        const n = rep.name;
        const hasMeta = (rep.total !== undefined);
        const kindIcon = rep.kind === 'full_run' ? '▶' : (rep.kind === 'run_all' ? '📅' : '⇄');
        const idBadge = `<span class="badge badge-info" style="margin-right:8px">#${rep.id ?? '?'}</span>`;
        const title = hasMeta
          ? `<b>${rep.project || '(none)'}</b> · ${rep.created || ''}`
          : `${n} <span style="color:#475569">· ${rep.created || ''}</span>`;
        const counts = hasMeta
          ? `<span style="margin-left:10px;font-size:11px">
               <span style="color:var(--log-pass,#86efac)">✅ ${rep.pass}</span>
               <span style="color:var(--log-fail,#fca5a5);margin-left:6px">❌ ${rep.fail}</span>
               <span style="color:var(--text-muted);margin-left:6px">/ ${rep.total}</span>
               ${rep.mode ? `<span class="badge badge-info" style="margin-left:8px">${rep.mode}</span>` : ''}
             </span>`
          : '';
        const allureBtns =
          (rep.allure_html ? `<a class="btn-xs btn-xs-view" style="background:#14532d;color:var(--log-pass,#86efac)" href="/api/allure-html/${rep.allure_html.replace('-html','')}/" target="_blank" title="Open Allure HTML report">📊 Allure</a>` : '') +
          (rep.allure_zip ? `<a class="btn-xs btn-xs-view" href="/api/allure/${encodeURIComponent(rep.allure_zip)}" download title="Download allure-results (.zip)">📦 .zip</a>` : '');
        return `
        <div class="golden-item">
          <span class="golden-name"><input type="checkbox" class="rep-check" value="${n}" onchange="updateReportSelCount()">${idBadge}${kindIcon} ${title} ${counts}</span>
          <div class="golden-actions">
            <a class="btn-xs btn-xs-view" href="/api/report/${encodeURIComponent(n)}" target="_blank">Open</a>
            <a class="btn-xs btn-xs-view" href="/api/report/${encodeURIComponent(n)}?download=1" download>Download</a>
            ${allureBtns}
            <button class="btn-xs btn-xs-del" onclick="deleteReport('${n}')">Delete</button>
          </div>
        </div>`;
      }).join('')
    : (_allReports.length ? 'No reports match your search.' : 'No reports yet.');
  updateReportSelCount();
}

function selectedReportNames() {
  return Array.from(document.querySelectorAll('#dash-reports .rep-check:checked')).map(c => c.value);
}
function updateReportSelCount() {
  const el = document.getElementById('rep-sel-count');
  if (el) el.textContent = selectedReportNames().length;
}
async function deleteReport(name) {
  if (!confirm('Delete report ' + name + '?')) return;
  await fetch('/api/report/' + encodeURIComponent(name), {method: 'DELETE'});
  loadReports();
}
async function deleteSelectedReports() {
  const names = selectedReportNames();
  if (!names.length) { alert('No reports selected.'); return; }
  if (!confirm(`Delete ${names.length} selected report(s)?`)) return;
  await fetch('/api/reports/delete', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({names})});
  loadReports();
}
async function deleteAllReports() {
  if (!confirm('⚠️ Delete ALL reports? This cannot be undone.')) return;
  await fetch('/api/reports/delete', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({all: true})});
  loadReports();
}

// ── Resume in-progress jobs after a browser refresh ────────────────────────
// Backend threads keep running independently of the browser tab; without this,
// a refresh mid-run just shows nothing happening until the job silently finishes.
function resumeFullRun() {
  document.getElementById('fullrun-log-card').style.display = 'block';
  document.getElementById('fullrun-start-btn').disabled = true;
  document.getElementById('fullrun-stop-btn').disabled  = false;
  setFullRunModeLocked(true);
  document.getElementById('fullrun-status-dot').innerHTML = '<span class="pulse"></span>';
  document.getElementById('fullrun-status-text').textContent = 'Watching all flows...';
  fullRunSSE = new EventSource('/api/full-run/stream');
  fullRunSSE.onmessage = (e) => {
    const item = JSON.parse(e.data);
    if (item.type === 'ping') return;
    const log = document.getElementById('fullrun-log');
    const line = document.createElement('div');
    line.className = 'log-line log-' + item.type;
    line.textContent = new Date().toLocaleTimeString() + '  ' + item.msg;
    log.appendChild(line);
    log.scrollTop = log.scrollHeight;
    if (item.result) {
      const r = item.result;
      if (r.flow) r.create_time = r.flow + ' · ' + r.create_time;
      fullRunResults.push(r);
      renderResultRow(r, 'fullrun-results-body');
      updateFullRunCounters(fullRunResults);
    }
    if (item.type === 'done') {
      fullRunSSE.close();
      document.getElementById('fullrun-start-btn').disabled = false;
      document.getElementById('fullrun-stop-btn').disabled  = true;
      setFullRunModeLocked(false);
      document.getElementById('fullrun-status-dot').innerHTML = '';
      document.getElementById('fullrun-status-text').textContent = 'Idle';
      if (item.report) {
        const dl = document.getElementById('fullrun-report-dl');
        dl.href = '/api/report/' + encodeURIComponent(item.report) + '?download=1';
        dl.style.display = 'inline-flex';
      }
      showAllure(item.allure_zip, item.allure_html);
      loadReports();
    }
  };
}

function resumeWatch() {
  document.getElementById('watch-start-btn').disabled = true;
  document.getElementById('watch-stop-btn').disabled  = false;
  setWatchModeLocked(true);
  document.getElementById('watch-status-dot').innerHTML = '<span class="pulse"></span>';
  document.getElementById('watch-status-text').textContent = 'Watching...';
  watchSSE = new EventSource('/api/watch/stream');
  watchSSE.onmessage = (e) => {
    const item = JSON.parse(e.data);
    if (item.type === 'ping') return;
    const log = document.getElementById('watch-log');
    if (item.type === 'scanning') {
      let scanEl = document.getElementById('watch-scan-status');
      if (!scanEl) {
        scanEl = document.createElement('div');
        scanEl.id = 'watch-scan-status';
        scanEl.style.cssText = 'font-size:11px;color:#64748b;padding:2px 0;font-style:italic';
        log.appendChild(scanEl);
      }
      scanEl.textContent = item.msg;
      log.scrollTop = log.scrollHeight;
      return;
    }
    const scanEl = document.getElementById('watch-scan-status');
    if (scanEl) scanEl.remove();
    const line = document.createElement('div');
    line.className = 'log-line log-' + item.type;
    line.textContent = new Date().toLocaleTimeString() + '  ' + item.msg;
    log.appendChild(line);
    log.scrollTop = log.scrollHeight;
    if (item.result) {
      watchResults.push(item.result);
      renderResultRow(item.result, 'watch-results-body');
      updateWatchCounters(watchResults);
    }
    if (item.type === 'done') {
      watchSSE.close();
      document.getElementById('watch-start-btn').disabled = false;
      document.getElementById('watch-stop-btn').disabled  = true;
      setWatchModeLocked(false);
      document.getElementById('watch-status-dot').innerHTML = '';
      document.getElementById('watch-status-text').textContent = 'Idle';
    }
  };
}

function resumeLiveCapture() {
  document.getElementById('cap-live-start-btn').disabled = true;
  document.getElementById('cap-live-stop-btn').disabled  = false;
  document.getElementById('cap-live-dot').innerHTML = '<span class="pulse"></span>';
  document.getElementById('cap-live-status').textContent = 'Polling — trigger your flow now...';
  liveCapSSE = new EventSource('/api/capture/live/stream');
  liveCapSSE.onmessage = (e) => {
    const item = JSON.parse(e.data);
    if (item.type === 'ping') return;
    const log  = document.getElementById('cap-live-log');
    const line = document.createElement('div');
    line.className = 'log-line log-' + item.type;
    line.textContent = new Date().toLocaleTimeString() + '  ' + item.msg;
    log.appendChild(line);
    log.scrollTop = log.scrollHeight;
    if (item.type === 'done') {
      liveCapSSE.close();
      document.getElementById('cap-live-start-btn').disabled = false;
      document.getElementById('cap-live-stop-btn').disabled  = true;
      document.getElementById('cap-live-dot').innerHTML = '';
      document.getElementById('cap-live-status').textContent = 'Done.';
      if (item.saved && item.saved.length > 0) {
        const el = document.getElementById('cap-live-result');
        el.style.display = 'block';
        document.getElementById('cap-live-result-body').innerHTML =
          `<p style="color:var(--log-pass,#86efac);margin-bottom:10px">✅ ${item.saved.length} golden snapshot(s) saved.</p>` +
          item.saved.map(k=>`<div style="font-family:monospace;font-size:12px;color:#a5b4fc;padding:2px 0">${k}</div>`).join('');
      }
    }
  };
}

function resumeKowlCapture() {
  document.getElementById('kc-start-btn').disabled = true;
  document.getElementById('kc-stop-btn').disabled  = false;
  document.getElementById('kc-dot').innerHTML = '<span class="pulse"></span>';
  document.getElementById('kc-status').textContent = 'Capturing — run your flow...';
  const log = document.getElementById('kc-log');
  log.style.display = 'block';
  kowlCapSSE = new EventSource('/api/kowl-capture/stream');
  kowlCapSSE.onmessage = (e) => {
    const item = JSON.parse(e.data);
    if (item.type === 'ping') return;
    const line = document.createElement('div');
    line.className = 'log-line log-' + item.type;
    line.textContent = new Date().toLocaleTimeString() + '  ' + item.msg;
    log.appendChild(line); log.scrollTop = log.scrollHeight;
    if (item.type === 'done') {
      kowlCapSSE.close();
      document.getElementById('kc-start-btn').disabled = false;
      document.getElementById('kc-stop-btn').disabled  = true;
      document.getElementById('kc-dot').innerHTML = '';
      document.getElementById('kc-status').textContent = 'Idle';
      loadTopicBaselines();
    }
  };
}

function resumeTopicCapture() {
  const btn = document.getElementById('tc-cap-btn');
  const stopBtn = document.getElementById('tc-cap-stop-btn');
  btn.disabled = true; btn.textContent = '⏳ Capturing...';
  stopBtn.style.display = 'inline-flex';
  const card = document.getElementById('tc-cap-result');
  const body = document.getElementById('tc-cap-result-body');
  card.style.display = 'block';
  const progressBar = document.createElement('div');
  progressBar.style.cssText = 'font-size:12px;color:var(--text-muted);margin-bottom:8px';
  body.innerHTML = '';
  body.appendChild(progressBar);
  const sse = new EventSource('/api/topics/capture/stream');
  _tcCaptureSSE = sse;
  sse.onmessage = (e) => {
    const item = JSON.parse(e.data);
    if (item.type === 'ping') return;
    if (item.type === 'progress') {
      progressBar.textContent = `⏳ [${item.current}/${item.total}] Fetching: ${item.topic}`;
    } else if (item.type === 'ok') {
      const div = document.createElement('div');
      div.style.cssText = 'font-family:monospace;font-size:11px;color:var(--log-pass,#86efac);padding:1px 0';
      div.textContent = item.msg;
      body.insertBefore(div, progressBar);
    } else if (item.type === 'topic_error') {
      const div = document.createElement('div');
      div.style.cssText = 'font-family:monospace;font-size:11px;color:var(--log-fail,#fca5a5);padding:1px 0';
      div.textContent = item.msg;
      body.insertBefore(div, progressBar);
    } else if (item.type === 'done') {
      sse.close();
      _tcCaptureSSE = null;
      stopBtn.style.display = 'none';
      progressBar.remove();
      const summary = document.createElement('div');
      summary.style.cssText = 'font-size:12px;margin-top:8px;padding-top:8px;border-top:1px solid #334155';
      summary.innerHTML = item.keys
        ? `<span style="color:var(--log-pass,#86efac)">✅ Done — ${item.keys} key(s) from ${item.messages} message(s)</span>`
        : `<span style="color:#fbbf24">⚠️ No messages captured — topics may be empty</span>`;
      body.appendChild(summary);
      loadTopicBaselines();
      btn.disabled = false; btn.textContent = '📥 Capture Baseline (snapshot)';
    } else if (item.type === 'error') {
      sse.close();
      _tcCaptureSSE = null;
      stopBtn.style.display = 'none';
      const summary = document.createElement('div');
      summary.style.cssText = 'font-size:12px;margin-top:8px;padding-top:8px;border-top:1px solid #334155;color:#fbbf24';
      summary.textContent = '⏹ ' + (item.msg || 'Stopped.');
      body.appendChild(summary);
      btn.disabled = false; btn.textContent = '📥 Capture Baseline (snapshot)';
    }
  };
}

function resumeTopicCompare() {
  const btn = document.getElementById('tc-cmp-btn');
  const stopBtn = document.getElementById('tc-stop-btn');
  btn.disabled = true; btn.textContent = '⏳ Comparing...';
  stopBtn.style.display = 'inline-flex';
  const resultCard = document.getElementById('tc-cmp-result');
  const body = document.getElementById('tc-results-body');
  resultCard.style.display = 'block';
  document.getElementById('tc-summary').style.display = 'none';
  const progressRow = document.createElement('tr');
  progressRow.innerHTML = '<td colspan="6"><div id="tc-cmp-progress" style="font-size:12px;padding:8px">Resuming compare...</div></td>';
  body.innerHTML = '';
  body.appendChild(progressRow);
  const progressEl = document.getElementById('tc-cmp-progress');
  const sse = new EventSource('/api/topics/compare/stream');
  _tcCompareSSE = sse;
  sse.onmessage = (e) => {
    const item = JSON.parse(e.data);
    if (item.type === 'ping') return;
    if (item.type === 'progress') {
      progressEl.textContent = `⏳ [${item.current}/${item.total}] Comparing: ${item.topic}`;
    } else if (item.type === 'ok') {
      const info = document.createElement('tr');
      info.innerHTML = `<td colspan="6"><div style="font-family:monospace;font-size:11px;color:var(--log-pass,#22c55e);padding:2px 8px">${item.msg}</div></td>`;
      body.insertBefore(info, progressRow);
    } else if (item.type === 'topic_error') {
      const info = document.createElement('tr');
      info.innerHTML = `<td colspan="6"><div style="font-family:monospace;font-size:11px;color:var(--log-fail,#f87171);padding:2px 8px">${item.msg}</div></td>`;
      body.insertBefore(info, progressRow);
    } else if (item.type === 'done') {
      sse.close(); _tcCompareSSE = null;
      progressRow.remove();
      renderTopicResults(item.results || []);
      const dl = document.getElementById('tc-report-dl');
      if (item.report) {
        dl.href = '/api/report/' + encodeURIComponent(item.report) + '?download=1';
        dl.style.display = 'inline-flex';
      } else {
        dl.style.display = 'none';
      }
      if (typeof loadReports === 'function') loadReports();
      btn.disabled = false; btn.textContent = '🔍 Compare';
      stopBtn.style.display = 'none';
    } else if (item.type === 'error') {
      sse.close(); _tcCompareSSE = null;
      if (progressEl) progressEl.textContent = '⏹ ' + (item.msg || 'Stopped.');
      btn.disabled = false; btn.textContent = '🔍 Compare';
      stopBtn.style.display = 'none';
    }
  };
}

async function resumeRunningJobs() {
  try {
    const status = await (await fetch('/api/runtime/status')).json();
    if (status.full_watch)    resumeFullRun();
    if (status.watch)         resumeWatch();
    if (status.capture)       resumeLiveCapture();
    if (status.kowl_capture)  resumeKowlCapture();
    if (status.topic_capture) resumeTopicCapture();
    if (status.topic_compare) resumeTopicCompare();
  } catch (e) {}
}
document.addEventListener('DOMContentLoaded', resumeRunningJobs);
