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

// MATCH/MISMATCH text+color for the single-verdict JSON/XML compare views.
// hasWarnings notes a PASS that still has value-only findings (shown separately below).
function verdictLabel(status, hasWarnings) {
  if (status === 'PASS') {
    return hasWarnings
      ? {text: '✅ MATCH — with value warning(s)', color: '#86efac'}
      : {text: '✅ MATCH', color: '#86efac'};
  }
  return {text: '❌ MISMATCH', color: '#fca5a5'};
}

// Registry for the "mark value-diffs as pass" feature (client-side, this view only).
window.__rowReg = window.__rowReg || {};
window.__rowUid = window.__rowUid || 0;

// A finding is a "warning" (value-only drift, not a schema break) only when
// it's a plain value change — same field, same type, different value. A type
// change (str -> int) is a real schema break and stays a hard difference, not
// something markable-as-pass — mirrors status_from_findings() on the backend.
function isValueOnly(f) {
  return f.type === 'values changed';
}

function renderResultRow(r, tbodyId) {
  const tbody = document.getElementById(tbodyId);
  const uid = ++window.__rowUid;
  const rowId = 'row-' + uid;
  window.__rowReg[uid] = { findings: r.findings || [], status: r.status, marks: new Set(), tbodyId };

  // Split findings: real schema breaks (missing/extra field, type change) stay
  // red "differences"; plain value-only drift renders as a separate yellow
  // "warnings" section, not counted among the real differences.
  const indexed = (r.findings || []).map((f, i) => ({f, i}));
  const failFindings = indexed.filter(x => !isValueOnly(x.f));
  const warnFindings = indexed.filter(x => isValueOnly(x.f));

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

  const markAllBtn = warnFindings.length > 0 ? `
        <button class="mark-pass-btn" onclick="event.stopPropagation(); markAllValueDiffs(${uid}, true)">✓ mark all pass</button>
        <button class="mark-pass-btn mark-pass-reset" onclick="event.stopPropagation(); markAllValueDiffs(${uid}, false)">↺ reset</button>` : '';

  const failBlock = failFindings.length > 0 ? `
        <div id="${rowId}-diffhdr" style="font-size:11px;font-weight:700;color:#64748b;margin:0 0 6px">
          DIFFERENCES (${failFindings.length})
        </div>
        ${failFindings.map(({f, i}) => `
          <div class="diff-row diff-row-fail" id="${rowId}-diff-${i}">
            <span class="diff-type">${f.type}</span>
            <span class="diff-path">${f.path}</span>
            <span class="diff-detail-text">${f.detail}</span>
          </div>`).join('')}` : '';

  const warnBlock = warnFindings.length > 0 ? `
        <div id="${rowId}-warnhdr" style="font-size:11px;font-weight:700;color:#fbbf24;margin:${failBlock ? '12px' : '0'} 0 6px">
          WARNINGS (${warnFindings.length}) — value mismatch only${markAllBtn}
        </div>
        ${warnFindings.map(({f, i}) => `
          <div class="diff-row diff-row-warn" id="${rowId}-diff-${i}">
            <span class="diff-type">${f.type}</span>
            <span class="diff-path">${f.path}</span>
            <span class="diff-detail-text">${f.detail}</span>
            <button class="mark-pass-btn" id="${rowId}-mark-${i}" onclick="event.stopPropagation(); toggleMarkPass(${uid}, ${i})">✓ mark pass</button>
          </div>`).join('')}` : '';

  const diffBlock = failBlock + warnBlock;

  const jsonBlock = r.payload ? `
        <div style="font-size:11px;font-weight:700;color:#64748b;margin:${diffBlock ? '12px' : '0'} 0 6px">
          PAYLOAD JSON <span style="font-weight:400;color:var(--text-dim)">— <span style="color:var(--log-pass,#86efac)">green = matches golden</span>, <span style="color:#fbbf24">yellow = value-only drift</span>, <span style="color:var(--log-fail,#fca5a5)">red = schema mismatch</span></span>
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
  const failCount = reg.findings.filter(f => !isValueOnly(f)).length;
  const warnCount = reg.findings.filter(isValueOnly).length;
  const marked = reg.marks.size;
  const warnRemaining = warnCount - marked;
  const remaining = failCount + warnRemaining;
  const newStatus = remaining === 0 ? 'PASS' : reg.status;

  const statusTd = document.getElementById('row-' + uid + '-status');
  if (statusTd) {
    statusTd.innerHTML = statusBadge(newStatus) +
      (marked > 0 && remaining === 0 ? ' <span style="font-size:10px;color:#fbbf24">(value-diffs accepted)</span>' : '');
  }
  const countTd = document.getElementById('row-' + uid + '-count');
  if (countTd) countTd.textContent = marked > 0 ? `${remaining} (+${marked} accepted)` : `${reg.findings.length}`;

  // DIFFERENCES header (real schema breaks) never changes via marking — only
  // value-only warnings are markable. WARNINGS header reflects the mark state.
  const warnHdr = document.getElementById('row-' + uid + '-warnhdr');
  if (warnHdr) {
    warnHdr.childNodes[0].nodeValue =
      `WARNINGS (${warnRemaining}${marked ? ` active, ${marked} accepted` : ''}) — value mismatch only `;
  }

  // For the direct JSON/XML comparators there is a single row + summary strip.
  const prefix = reg.tbodyId === 'json-result-body' ? 'json'
               : reg.tbodyId === 'xml-result-body'  ? 'xml' : null;
  if (prefix) {
    const v = document.getElementById(prefix + '-verdict');
    if (v) {
      const vl = verdictLabel(newStatus, warnRemaining > 0);
      v.textContent = vl.text;
      v.style.color = vl.color;
    }
    const d = document.getElementById(prefix + '-diffs');
    if (d) d.textContent = remaining;
  }
}

function escapeHtml(s) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// Convert a DeepDiff path (root['a']['b'][0]) into a dot-path (a.b.0).
// Splits findings into two path sets so the payload view can tell a real
// schema break (red) apart from a plain value-only drift (yellow) — mirrors
// isValueOnly()'s red-vs-yellow split in the findings list below the payload.
function parseDiffPaths(findings) {
  const failSet = new Set(), warnSet = new Set();
  (findings || []).forEach(f => {
    const segs = (f.path || '').match(/\[['"]?([^\]'"]+)['"]?\]/g) || [];
    const path = segs.map(s => s.replace(/^\[['"]?/, '').replace(/['"]?\]$/, '')).join('.');
    if (!path) return;
    (isValueOnly(f) ? warnSet : failSet).add(path);
  });
  return {failSet, warnSet};
}

// A line is "bad"/"warn" only if it IS a changed node or sits INSIDE an
// added/removed subtree — never just because it's an ancestor on the way to
// a deep change. Real schema breaks (failSet) take priority over value-only
// drift (warnSet) if a path somehow lands in both.
function pathStatus(path, diffPaths) {
  if (!path) return 'ok';
  const inSet = (set) => set.has(path) || [...set].some(d => path.startsWith(d + '.'));
  if (inSet(diffPaths.failSet)) return 'bad';
  if (inSet(diffPaths.warnSet)) return 'warn';
  return 'ok';
}

// Render payload JSON line-by-line: green = matches golden, yellow = value-only
// drift (markable pass), red = real schema mismatch (missing/extra field, type change).
function colorJsonLines(payload, findings) {
  const diffPaths = parseDiffPaths(findings);
  const lines = [];
  function walk(node, path, indent, keyLabel, comma) {
    const pad = '  '.repeat(indent);
    const status = pathStatus(path, diffPaths);
    const prefix = keyLabel !== null ? '"' + keyLabel + '": ' : '';
    if (node !== null && typeof node === 'object') {
      const isArr = Array.isArray(node);
      lines.push({t: pad + prefix + (isArr ? '[' : '{'), status});
      const entries = isArr ? node.map((v, i) => [i, v]) : Object.entries(node);
      entries.forEach((kv, i) => {
        const childPath = path ? path + '.' + kv[0] : String(kv[0]);
        walk(kv[1], childPath, indent + 1, isArr ? null : kv[0], i < entries.length - 1);
      });
      lines.push({t: pad + (isArr ? ']' : '}') + (comma ? ',' : ''), status});
    } else {
      lines.push({t: pad + prefix + JSON.stringify(node) + (comma ? ',' : ''), status});
    }
  }
  walk(payload, '', 0, null, false);
  const cls = {ok: 'jl-ok', warn: 'jl-warn', bad: 'jl-bad'};
  return lines.map(l =>
    '<span class="' + cls[l.status] + '">' + escapeHtml(l.t) + '</span>'
  ).join('\n');
}

// A row that passed but still has a value-only finding — surfaced separately
// in summary tiles so a data-drift warning isn't invisible, without failing it.
function passedWithWarning(r) {
  return r.status === 'PASS' && (r.findings || []).some(isValueOnly);
}

function updateWatchCounters(results) {
  const pass = results.filter(r=>r.status==='PASS').length;
  const warn = results.filter(passedWithWarning).length;
  const fail = results.filter(r=>r.status==='FAIL').length;
  const nog  = results.filter(r=>r.status==='NO GOLDEN').length;
  document.getElementById('w-total').textContent = results.length;
  document.getElementById('w-pass').textContent  = pass;
  document.getElementById('w-warning').textContent = warn;
  document.getElementById('w-fail').textContent  = fail;
  document.getElementById('w-nogolden').textContent = nog;
  document.getElementById('watch-summary').style.display = 'block';
  document.getElementById('watch-results-card').style.display = 'block';
}

// ── Full Run (live, all flows) ────────────────────────────────────────────────
let fullRunResults = [];
let fullRunSSE = null;
let fullRunGolden = 'db';

function setFullRunGolden(src) {
  fullRunGolden = src;
  ['db','kowl'].forEach(s =>
    document.getElementById('fullrun-gs-' + s).classList.toggle('active', s === src));
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
  const warn = results.filter(passedWithWarning).length;
  const fail = results.filter(r=>r.status==='FAIL').length;
  const nog  = results.filter(r=>!['PASS','FAIL'].includes(r.status)).length;
  document.getElementById('fr-total').textContent = results.length;
  document.getElementById('fr-pass').textContent  = pass;
  document.getElementById('fr-warning').textContent = warn;
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
      body: JSON.stringify({ interval, mode: modeState.fullrun, golden_source: fullRunGolden })
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
  ['db','kowl','isd','subscriber'].forEach(s => {
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
  } else if (src === 'subscriber') {
    const lbl = document.getElementById('cap-subscriber-project');
    if (lbl) refreshCaptureProject().then(() => {
      lbl.textContent = document.getElementById('cap-project-label').textContent;
    });
  }
}

// ── Subscriber snapshot (Capture + Compare) ────────────────────────────────
async function doCaptureSubscriber() {
  const btn = document.getElementById('cap-subscriber-btn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> Connecting...';
  try {
    const res = await fetch('/api/subscriber/capture', {method: 'POST'});
    const data = await res.json();
    const el   = document.getElementById('cap-subscriber-result');
    const body = document.getElementById('cap-subscriber-result-body');
    el.style.display = 'block';
    if (data.ok) {
      const errs = (data.errors || []).map(e => `<p style="color:#fbbf24;font-size:12px">⚠️ ${e}</p>`).join('');
      body.innerHTML = `
        ${errs}
        <p style="color:var(--log-pass,#86efac);margin-bottom:10px">✅ Captured ${data.saved.length} subscriber snapshot(s).</p>
        ${data.saved.map(k=>`<div style="font-family:monospace;font-size:12px;color:#a5b4fc;padding:2px 0">${k}</div>`).join('')}
      `;
    } else {
      body.innerHTML = `<p style="color:var(--log-fail,#fca5a5)">❌ ${data.error}</p>`;
    }
  } catch(e) {
    alert('Error: ' + e.message);
  }
  btn.disabled = false;
  btn.innerHTML = '📸 Capture Subscriber Snapshot';
}

async function doCompareSubscriber() {
  const btn = document.getElementById('cmp-subscriber-btn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> Connecting...';
  try {
    const res = await fetch('/api/subscriber/compare', {method: 'POST'});
    const data = await res.json();
    const el   = document.getElementById('cmp-subscriber-result');
    const body = document.getElementById('cmp-subscriber-result-body');
    const reportBar = document.getElementById('cmp-subscriber-report-bar');
    el.style.display = 'block';
    if (data.ok) {
      body.innerHTML = data.results.map((r, i) => {
        const color = r.status === 'PASS' ? 'var(--log-pass,#86efac)'
                    : r.status === 'FAIL' ? 'var(--log-fail,#fca5a5)' : '#fbbf24';
        const findings = r.findings || [];
        const fields = r.fields || [];
        // Side-by-side field table: baseline (left) vs target (right), every
        // field shown — 'same' fields in the default color, 'warn' (expected
        // drift: env/time/ip/ids — doesn't fail) in yellow, 'fail' (schema
        // break) in red.
        const FIELD_COLOR = {same: 'var(--text-muted)', warn: '#fbbf24', fail: 'var(--log-fail,#fca5a5)'};
        let detail;
        if (fields.length) {
          detail = `<table style="width:100%;border-collapse:collapse;font-size:12px">
            <thead><tr style="color:var(--text-dim);font-size:10px;text-align:left">
              <th style="padding:3px 8px">FIELD</th><th style="padding:3px 8px">BASELINE</th><th style="padding:3px 8px">TARGET</th>
            </tr></thead>
            <tbody>${fields.map(f => `<tr>
              <td style="padding:3px 8px;font-family:monospace;font-size:11px;color:var(--text-dim)">${escapeHtml(f.path)}</td>
              <td style="padding:3px 8px;font-family:monospace;color:${FIELD_COLOR[f.status]}">${escapeHtml(String(f.baseline))}</td>
              <td style="padding:3px 8px;font-family:monospace;color:${FIELD_COLOR[f.status]}">${escapeHtml(String(f.target))}</td>
            </tr>`).join('')}</tbody>
          </table>`;
        } else {
          detail = findings.map(f => {
            const isWarn = f.type === 'values changed';
            return `<div style="font-size:12px;color:${isWarn ? '#fbbf24' : 'var(--log-fail,#fca5a5)'};padding:2px 0">
              <b>${escapeHtml(f.type)}</b> <code>${escapeHtml(f.path)}</code> — ${escapeHtml(f.detail)}</div>`;
          }).join('') || '<div style="font-size:12px;color:var(--log-pass,#86efac)">✓ matches golden</div>';
        }
        const rid = `sub-diff-${i}`;
        return `<tr style="cursor:pointer" onclick="document.getElementById('${rid}').classList.toggle('hidden');
                 this.querySelector('.sub-arrow').textContent = document.getElementById('${rid}').classList.contains('hidden') ? '▸' : '▾'">
          <td><span class="sub-arrow">▸</span> ${escapeHtml(r.label)}</td>
          <td><code>${escapeHtml(r.pattern)}</code></td>
          <td style="color:${color}">${r.status}</td>
          <td>${findings.length} finding(s)</td>
        </tr>
        <tr id="${rid}" class="hidden"><td></td><td colspan="3">${detail}</td></tr>`;
      }).join('');
      if (reportBar) {
        reportBar.style.display = data.report ? 'flex' : 'none';
        reportBar.innerHTML = data.report ? `
          <a class="btn btn-ghost" style="padding:6px 12px;font-size:12px" href="/api/report/${data.report}" target="_blank">📄 View Report</a>
          <a class="btn btn-ghost" style="padding:6px 12px;font-size:12px" href="/api/report/${data.report}?download=1">⬇ Download Report</a>` : '';
      }
    } else {
      body.innerHTML = `<tr><td colspan="4" style="color:var(--log-fail,#fca5a5)">❌ ${data.error}</td></tr>`;
      if (reportBar) reportBar.style.display = 'none';
    }
  } catch(e) {
    alert('Error: ' + e.message);
  }
  btn.disabled = false;
  btn.innerHTML = '🔍 Compare Subscribers';
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
    const dbNameEl = document.getElementById('isd-target-db-name');
    const kowlNameEl = document.getElementById('isd-target-kowl-name');
    if (dbNameEl) dbNameEl.textContent = cfg.project || 'none';
    if (kowlNameEl) kowlNameEl.textContent = cfg.kowl_project || 'none';
    renderPatternChecks('cap-pattern-checks', cfg.patterns || []);
    renderPatternChecks('cap-live-pattern-checks', cfg.patterns || []);
  } catch (e) {}
}

// Which project ISD-captured goldens are filed under — 'db' (default) or 'kowl'.
// One global picker (above both ISD capture methods: PDF upload + paste).
let isdTargetProject = 'db';
function setIsdTargetProject(kind) {
  isdTargetProject = kind;
  document.getElementById('isd-target-db').classList.toggle('active', kind === 'db');
  document.getElementById('isd-target-kowl').classList.toggle('active', kind === 'kowl');
}

async function uploadISD() {
  const btn = document.getElementById('cap-isd-btn');
  const fileEl = document.getElementById('cap-isd-file');
  if (!fileEl.files.length) { alert('Choose an ISD PDF first.'); return; }
  const fd = new FormData();
  fd.append('file', fileEl.files[0]);
  fd.append('project_kind', isdTargetProject);
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
      window.__isdFailedBlocks = data.failed_blocks || [];
      const failedList = window.__isdFailedBlocks.length
        ? `<div style="margin-top:8px">` +
          window.__isdFailedBlocks.map((fb, idx) => `
            <div class="golden-item" style="align-items:flex-start">
              <span class="golden-name" style="font-family:monospace;font-size:11px;color:#fcd34d">
                Page ${fb.page}: ${escapeHtml(fb.preview)}${fb.preview.length >= 100 ? '…' : ''}
              </span>
              <button class="btn-xs btn-xs-view" onclick="loadFailedIsdBlock(${idx})">📋 Load into paste box</button>
            </div>`).join('') +
          `</div>`
        : '';
      const scopeNote = data.scoped
        ? `<div style="font-size:11px;color:var(--log-pass,#86efac);margin-bottom:6px">🎯 Scoped to your configured patterns/topics — unrelated payloads elsewhere in the doc were skipped.</div>`
        : `<div style="font-size:11px;color:#fcd34d;margin-bottom:6px">⚠️ No configured pattern/topic name matched a "Kafka Topic" heading in this doc — fell back to scanning the whole document for any JSON block (may include unrelated payloads).</div>`;
      body.innerHTML = scopeNote +
        `<div style="font-size:12px;color:var(--log-pass,#86efac);margin-bottom:6px">✅ Read ${data.pages} page(s); saved ${data.keys} golden(s) under project "${data.project || '(none)'}".</div>` +
        `<div style="font-size:11px;color:#64748b;margin-bottom:10px">JSON blocks found: ${data.blocks_seen} · parsed: ${data.blocks_parsed}${unparse ? ` · <span style="color:#fcd34d">unparseable: ${unparse}</span>` : ''}</div>` +
        (data.saved.length
          ? data.saved.map(s => `<div class="golden-item"><span class="golden-name">${s.key}</span></div>`).join('')
          : '<div style="color:#fcd34d;font-size:12px">No payloads auto-extracted.</div>') +
        (unparse ? `<div style="font-size:11px;color:#fcd34d;margin-top:8px">⚠️ ${unparse} payload block(s) couldn't be parsed (the PDF's JSON is malformed — smart quotes / wrapped tokens). Found on the page(s) below — click to load the raw text into the paste box, fix it, then save it there.</div>${failedList}` : '');
      loadGoldens();
    }
  } catch (e) { alert('ISD upload error: ' + e); }
  btn.disabled = false; btn.textContent = '📄 Read ISD & Capture Golden';
}

function loadFailedIsdBlock(idx) {
  const fb = (window.__isdFailedBlocks || [])[idx];
  if (!fb) return;
  const box = document.getElementById('isd-paste');
  box.value = fb.raw;
  box.scrollIntoView({behavior: 'smooth', block: 'center'});
  box.focus();
}

async function saveIsdPaste() {
  const btn = document.getElementById('isd-paste-btn');
  const status = document.getElementById('isd-paste-status');
  const text = document.getElementById('isd-paste').value.trim();
  if (!text) { status.textContent = 'Paste a payload first.'; status.style.color = '#fca5a5'; return; }
  btn.disabled = true; btn.textContent = '⏳ Saving...'; status.textContent = '';
  try {
    const res = await fetch('/api/golden/from-json', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({text, project_kind: isdTargetProject})
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
let cmpGoldenSource = 'db'; // 'db' | 'kowl'

let topicGoldenSource = 'kowl';  // what golden the kowl panel compares against

// Pick comparison mode: db golden, kowl golden, or standalone direct-JSON
function setCompareGolden(src) {
  ['db','kowl','json','xml','subscriber'].forEach(s =>
    document.getElementById('cmp-gs-' + s).classList.toggle('active', s === src));
  if (src === 'kowl') {
    cmpGoldenSource = 'kowl'; topicGoldenSource = 'kowl';
    showComparePanel('kowl');
  } else if (src === 'json') {
    showComparePanel('json');
  } else if (src === 'xml') {
    showComparePanel('xml');
  } else if (src === 'subscriber') {
    showComparePanel('subscriber');
  } else if (src === 'db') {
    cmpGoldenSource = 'db'; showComparePanel('notif');
  }
}

// Toggle which compare panel is visible
function showComparePanel(which) {
  const isKowl = which === 'kowl', isJson = which === 'json', isXml = which === 'xml';
  const isSubscriber = which === 'subscriber';
  const isDirect = isKowl || isJson || isXml || isSubscriber;
  document.getElementById('cmp-tabs-row').style.display = isDirect ? 'none' : 'flex';
  document.getElementById('cmp-src-kowl').style.display = isKowl ? 'block' : 'none';
  document.getElementById('cmp-src-json').style.display = isJson ? 'block' : 'none';
  document.getElementById('cmp-src-xml').style.display  = isXml  ? 'block' : 'none';
  document.getElementById('cmp-src-subscriber').style.display = isSubscriber ? 'block' : 'none';
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
  const vl = verdictLabel(data.status, (data.findings || []).some(isValueOnly));
  v.textContent = vl.text;
  v.style.color = vl.color;
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
  const vl = verdictLabel(data.status, (data.findings || []).some(isValueOnly));
  v.textContent = vl.text;
  v.style.color = vl.color;
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

// Multi-pattern chip picker for Compare — lets the user queue up several
// patterns and run them together in one compare instead of one at a time.
let cmpPatterns = [];

function addCmpPattern() {
  const input = document.getElementById('cmp-pattern');
  const val = input.value.trim();
  if (val && !cmpPatterns.includes(val)) {
    cmpPatterns.push(val);
    renderCmpPatternChips();
  }
  input.value = '';
  input.focus();
}

function removeCmpPattern(p) {
  cmpPatterns = cmpPatterns.filter(x => x !== p);
  renderCmpPatternChips();
}

function renderCmpPatternChips() {
  const box = document.getElementById('cmp-pattern-chips');
  if (!box) return;
  box.innerHTML = cmpPatterns.map(p => `
    <span class="flow-pill active pill-other" style="cursor:default">
      ${p}<span style="cursor:pointer;margin-left:6px" onclick="removeCmpPattern('${p.replace(/'/g, "\\'")}')">✕</span>
    </span>`).join('');
}

async function doCompare() {
  const btn = document.getElementById('cmp-btn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> Comparing...';

  const typed = document.getElementById('cmp-pattern').value.trim();
  if (typed && !cmpPatterns.includes(typed)) { cmpPatterns.push(typed); renderCmpPatternChips(); }
  const patterns = cmpPatterns.slice();
  const since  = cmpFetchMode === 'time'  ? datetimeLocalToISO(document.getElementById('cmp-since').value) : null;
  const ext_id = cmpFetchMode === 'extid' ? document.getElementById('cmp-extid').value.trim() : null;

  if (patterns.length === 0) {
    alert('Please add at least one pattern.');
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
      body: JSON.stringify({patterns, since, ext_id, mode: modeState.cmp, golden_source: cmpGoldenSource})
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

    const notes = [];
    if (data.skipped_repeats > 0)
      notes.push(`${data.skipped_repeats} additional notification(s) with an already-seen key were skipped`);
    if (data.missing_patterns && data.missing_patterns.length)
      notes.push(`no subscriber found for: ${data.missing_patterns.join(', ')}`);
    const skippedNote = document.getElementById('cmp-skipped-note');
    if (notes.length) {
      skippedNote.textContent = 'ℹ️ ' + notes.join(' — ');
      skippedNote.style.display = 'block';
    } else {
      skippedNote.style.display = 'none';
    }

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
function watchDataOrigin() {
  return watchGolden === 'kowl' ? 'kowl' : 'db';
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
  ['db','kowl'].forEach(s =>
    document.getElementById('watch-gs-' + s).classList.toggle('active', s === src));
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
  document.getElementById('cfg-ssh-user').value  = cfg.ssh_user  || '';
  document.getElementById('cfg-db-host').value   = cfg.db_host   || '';
  document.getElementById('cfg-db-host-b').value = cfg.db_host_b || '';
  document.getElementById('cfg-db-name').value   = cfg.db_name   || '';
  document.getElementById('cfg-db-table').value  = cfg.db_table  || '';
  document.getElementById('cfg-poll').value      = cfg.poll_interval || 3;
  document.getElementById('cfg-project').value   = cfg.project || '';
  document.getElementById('cfg-project-kowl').value = cfg.kowl_project || '';
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
  // ssh_key is just a file path, not a secret — prefilled like any other field
  document.getElementById('cfg-ssh-key').value  = cfg.ssh_key || '';
  // DB passwords now prefill too, so they don't vanish on Save/refresh
  document.getElementById('cfg-db-pass').value  = cfg.db_pass || '';
  document.getElementById('cfg-db-pass-b').value = cfg.db_pass_b || '';
  // show banner if secrets not yet set
  document.getElementById('cfg-secrets-banner').style.display = cfg.secrets_ready ? 'none' : 'block';
  // check if secrets were auto-loaded from .secrets file
  checkSavedSecretsStatus();
}

// Generic export/import used by both the DB config and Kowl config buttons —
// each just points at its own pair of endpoints, filename, and status/file-input ids.
async function _exportConfigSubset(endpoint, filePrefix, statusId) {
  const status = document.getElementById(statusId);
  try {
    const cfg = await (await fetch(endpoint)).json();
    const blob = new Blob([JSON.stringify(cfg, null, 2)], {type: 'application/json'});
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    const stamp = (cfg.project || 'comparator').replace(/[^a-z0-9_-]+/gi, '_');
    a.href = url;
    a.download = `${filePrefix}-${stamp}.json`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
    status.textContent = '✅ Exported';
    status.style.color = '#86efac';
  } catch (e) {
    status.textContent = '❌ ' + e.message;
    status.style.color = '#fca5a5';
  }
  setTimeout(() => status.textContent = '', 3000);
}

async function _importConfigSubset(event, endpoint, statusId, confirmMsg) {
  const file   = event.target.files[0];
  const status = document.getElementById(statusId);
  event.target.value = '';  // allow re-selecting the same file later
  if (!file) return;
  if (!confirm(confirmMsg)) return;

  try {
    const text = await file.text();
    const parsed = JSON.parse(text);
    const res = await fetch(endpoint, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(parsed)
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || 'Import failed');
    status.textContent = '✅ Imported — reloading settings...';
    status.style.color = '#86efac';
    await loadConfig();
    status.textContent = '✅ Imported';
  } catch (e) {
    status.textContent = '❌ ' + e.message;
    status.style.color = '#fca5a5';
  }
  setTimeout(() => status.textContent = '', 4000);
}

function exportDbConfig() {
  return _exportConfigSubset('/api/config/export/db', 'db_config', 'cfg-db-import-status');
}
function importDbConfig(event) {
  return _importConfigSubset(event, '/api/config/import/db', 'cfg-db-import-status',
    `Import "${event.target.files[0]?.name}"? This replaces the current DB/SSH, patterns, and project settings (Kowl settings and secrets are untouched).`);
}
function exportKowlConfig() {
  return _exportConfigSubset('/api/config/export/kowl', 'kowl_config', 'cfg-kowl-import-status');
}
function importKowlConfig(event) {
  return _importConfigSubset(event, '/api/config/import/kowl', 'cfg-kowl-import-status',
    `Import "${event.target.files[0]?.name}"? This replaces the current Kowl/Kafka topic settings (DB/SSH settings are untouched).`);
}

async function checkSavedSecretsStatus() {
  const res  = await fetch('/api/secrets/saved');
  const data = await res.json();
  const banner = document.getElementById('cfg-secrets-loaded-banner');
  if (banner) banner.style.display = data.saved ? 'flex' : 'none';
}

// Fire-and-forget: pushes whatever's in the DB password fields to the
// in-memory secret store, always persisted (encrypted) into config.json.
// Called from saveConfig() so there's a single Save action instead of a
// separate secrets step — failures here are silent and never block the main
// config save. (ssh_key isn't a secret — it's just part of the main payload.)
async function pushSecretsIfPresent() {
  const db_pass   = document.getElementById('cfg-db-pass').value;
  const db_pass_b = document.getElementById('cfg-db-pass-b').value;
  if (!db_pass && !db_pass_b) return;
  try {
    const res = await fetch('/api/secrets', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({db_pass, db_pass_b, save_to_disk: true})
    });
    const data = await res.json();
    if (data.ok) {
      document.getElementById('cfg-secrets-banner').style.display = 'none';
      checkSavedSecretsStatus();
    }
  } catch (e) {}
}

function toggleVisible(inputId, btnId) {
  const inp  = document.getElementById(inputId);
  const btn  = document.getElementById(btnId);
  const show = inp.type === 'password';
  inp.type = show ? 'text' : 'password';
  btn.textContent = show ? '🙈 Hide' : '👁 Show';
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
  const projectInput = document.getElementById('cfg-project');

  const patterns = parsePatternsTextarea(document.getElementById('cfg-patterns').value);

  // Project is required — golden data is grouped under this name
  if (!projectInput.value.trim()) {
    projectInput.focus();
    projectInput.style.borderColor = '#f43f5e';
    projectInput.style.boxShadow   = '0 0 0 3px rgba(244,63,94,.2)';
    if (!silent) {
      status.textContent = '❌ Project name is required';
      status.style.color = '#fca5a5';
    }
    setTimeout(() => { projectInput.style.borderColor = ''; projectInput.style.boxShadow = ''; }, 2500);
    return false;
  }

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
    ssh_user:          document.getElementById('cfg-ssh-user').value,
    ssh_key:           document.getElementById('cfg-ssh-key').value.trim(),
    db_host:           document.getElementById('cfg-db-host').value,
    db_host_b:         document.getElementById('cfg-db-host-b').value,
    db_name:           document.getElementById('cfg-db-name').value,
    db_table:          document.getElementById('cfg-db-table').value,
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
  await pushSecretsIfPresent();
  if (!silent) {
    const originalLabel = btn.textContent;
    btn.textContent = data.ok ? '✓ Saved' : '❌ Failed';
    status.textContent = data.ok ? '' : '❌ ' + data.error;
    status.style.color = '#fca5a5';
    setTimeout(() => { btn.textContent = originalLabel; status.textContent = ''; }, 2000);
  }
  btn.disabled = false;
  if (data.ok) {
    refreshPatternsDatalist(patterns);
    document.getElementById('watch-interval').value = payload.poll_interval;
  }
  return data.ok;
}

async function testConnection(target = false) {
  const btn    = document.getElementById(target ? 'cfg-test-btn-b' : 'cfg-test-btn');
  const status = document.getElementById('cfg-status');
  const originalLabel = btn.textContent;
  // Save first (with validation) — if save fails, abort
  const saved = await saveConfig(true);
  if (!saved) return;
  btn.disabled = true;
  btn.textContent = '🔄 Testing...';
  status.textContent = '';
  const res  = await fetch('/api/config/test', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({target})
  });
  const data = await res.json();
  btn.textContent = data.ok ? '✓ OK' : '❌ Failed';
  status.textContent = data.msg;
  status.style.color = data.ok ? '#86efac' : '#fca5a5';
  setTimeout(() => { btn.textContent = originalLabel; btn.disabled = false; }, 2500);
}

async function saveKowlConfig() {
  const btn    = document.getElementById('cfg-kowl-save-btn');
  const status = document.getElementById('cfg-kowl-status');
  const projectInput = document.getElementById('cfg-project-kowl');
  const originalLabel = btn.textContent;

  // Project is required — golden data is grouped under this name
  if (!projectInput.value.trim()) {
    projectInput.focus();
    projectInput.style.borderColor = '#f43f5e';
    projectInput.style.boxShadow   = '0 0 0 3px rgba(244,63,94,.2)';
    status.textContent = '❌ Project name is required';
    status.style.color = '#fca5a5';
    setTimeout(() => {
      projectInput.style.borderColor = '';
      projectInput.style.boxShadow   = '';
      status.textContent = '';
    }, 2500);
    return false;
  }

  btn.disabled = true;
  const payload = {
    kowl_project:   projectInput.value.trim(),
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
  btn.textContent = data.ok ? '✓ Saved' : '❌ Failed';
  status.textContent = data.ok ? '' : '❌ ' + data.error;
  status.style.color = '#fca5a5';
  setTimeout(() => { btn.textContent = originalLabel; btn.disabled = false; status.textContent = ''; }, 2000);
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

async function testKowlConnection(target = false) {
  const btn    = document.getElementById(target ? 'cfg-kowl-test-btn-b' : 'cfg-kowl-test-btn');
  const status = document.getElementById('cfg-kowl-status');
  const originalLabel = btn.textContent;
  const saved  = await saveKowlConfig();
  if (!saved) return;
  btn.disabled = true;
  btn.textContent = '🔄 Testing...';
  status.textContent = '';
  const res  = await fetch('/api/config/test-kowl', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({target})
  });
  const data = await res.json();
  btn.textContent = data.ok ? '✓ OK' : '❌ Failed';
  status.textContent = data.msg;
  status.style.color = data.ok ? '#86efac' : '#fca5a5';
  setTimeout(() => { btn.textContent = originalLabel; btn.disabled = false; }, 2500);
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
  document.getElementById('tc-warn').textContent  = results.filter(passedWithWarning).length;
  document.getElementById('tc-fail').textContent  = results.filter(r => r.status === 'FAIL').length;
  document.getElementById('tc-nob').textContent   = results.filter(r => !['PASS', 'FAIL'].includes(r.status)).length;
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
    renderSubscriberReports();
  } catch (e) {}
}

function toggleReportsSection(wrapId, btnId) {
  const wrap = document.getElementById(wrapId);
  const btn  = document.getElementById(btnId);
  if (!wrap) return;
  const opening = wrap.classList.contains('hidden');
  wrap.classList.toggle('hidden');
  if (btn) btn.textContent = opening ? '⤡ Minimize' : '⤢ Expand';
}

function renderReports() {
  const el = document.getElementById('dash-reports');
  if (!el) return;
  const searchEl = document.getElementById('rep-search');
  const sortEl   = document.getElementById('rep-sort');
  const q    = (searchEl ? searchEl.value : '').trim().toLowerCase();
  const sort = sortEl ? sortEl.value : 'time-desc';

  // Subscriber compare reports get their own section below — keep them out of
  // the main Past Reports list so the two report kinds don't get mixed up.
  const byKind = _allReports.filter(rep => rep.kind !== 'subscriber_compare');
  const countEl = document.getElementById('rep-toggle-count');
  if (countEl) countEl.textContent = byKind.length ? `(${byKind.length})` : '';
  let reports = byKind.filter(rep =>
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
          : `${n} <span style="color:var(--text-dim)">· ${rep.created || ''}</span>`;
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
        const rid = `rep-detail-${cssEscapeId(n)}`;
        const flowRows = Object.entries(rep.per_flow || {});
        const detail = flowRows.length
          ? `<table style="width:100%;border-collapse:collapse;font-size:12px;margin-top:6px">
              <thead><tr style="color:var(--text-dim);font-size:10px;text-align:left">
                <th style="padding:3px 8px">FLOW</th><th style="padding:3px 8px">PASS</th><th style="padding:3px 8px">FAIL</th><th style="padding:3px 8px">TOTAL</th>
              </tr></thead>
              <tbody>${flowRows.map(([flow, s]) => `<tr>
                <td style="padding:3px 8px">${escapeHtml(flow)}</td>
                <td style="padding:3px 8px;color:var(--log-pass,#86efac)">${s.pass}</td>
                <td style="padding:3px 8px;color:var(--log-fail,#fca5a5)">${s.fail}</td>
                <td style="padding:3px 8px;color:var(--text-muted)">${s.total}</td>
              </tr>`).join('')}</tbody>
            </table>`
          : '<div style="color:var(--text-dim);font-size:12px;padding:6px 0">No per-flow breakdown stored for this report.</div>';
        return `
        <div class="report-entry">
          <div class="golden-item" style="cursor:pointer" onclick="toggleReportDetail(event, '${rid}')">
            <span class="golden-name"><input type="checkbox" class="rep-check" value="${n}" onclick="event.stopPropagation()" onchange="updateReportSelCount()">
              <span class="rep-arrow">▸</span> ${idBadge}${kindIcon} ${title} ${counts}</span>
            <div class="golden-actions" onclick="event.stopPropagation()">
              <a class="btn-xs btn-xs-view" href="/api/report/${encodeURIComponent(n)}" target="_blank">Open</a>
              <a class="btn-xs btn-xs-view" href="/api/report/${encodeURIComponent(n)}?download=1" download>Download</a>
              ${allureBtns}
              <button class="btn-xs btn-xs-del" onclick="deleteReport('${n}')">Delete</button>
            </div>
          </div>
          <div id="${rid}" class="report-detail hidden">${detail}</div>
        </div>`;
      }).join('')
    : (_allReports.length ? 'No reports match your search.' : 'No reports yet.');
  updateReportSelCount();
}

function cssEscapeId(s) {
  return String(s).replace(/[^a-zA-Z0-9_-]/g, '_');
}

function toggleReportDetail(evt, rid) {
  const detail = document.getElementById(rid);
  if (!detail) return;
  const arrow = evt.currentTarget.querySelector('.rep-arrow');
  const open  = !detail.classList.contains('hidden');
  detail.classList.toggle('hidden');
  if (arrow) arrow.textContent = open ? '▸' : '▾';
}

function renderSubscriberReports() {
  const el = document.getElementById('dash-subscriber-reports');
  if (!el) return;
  const reports = _allReports.filter(rep => rep.kind === 'subscriber_compare')
    .slice().sort((a, b) => (b.id || 0) - (a.id || 0));
  const countEl = document.getElementById('sub-rep-toggle-count');
  if (countEl) countEl.textContent = reports.length ? `(${reports.length})` : '';
  el.innerHTML = reports.length
    ? reports.map(rep => {
        const n = rep.name;
        const title = `<b>${rep.project || '(none)'}</b> · ${rep.created || ''}`;
        const counts = rep.total !== undefined
          ? `<span style="margin-left:10px;font-size:11px">
               <span style="color:var(--log-pass,#86efac)">✅ ${rep.pass}</span>
               <span style="color:var(--log-fail,#fca5a5);margin-left:6px">❌ ${rep.fail}</span>
               <span style="color:var(--text-muted);margin-left:6px">/ ${rep.total}</span>
             </span>`
          : '';
        const rid = `rep-detail-${cssEscapeId(n)}`;
        const items = rep.items || [];
        const detail = items.length
          ? `<table style="width:100%;border-collapse:collapse;font-size:12px;margin-top:6px">
              <thead><tr style="color:var(--text-dim);font-size:10px;text-align:left">
                <th style="padding:3px 8px">LABEL</th><th style="padding:3px 8px">PATTERN</th><th style="padding:3px 8px">STATUS</th>
              </tr></thead>
              <tbody>${items.map(it => {
                const c = it.status === 'PASS' ? 'var(--log-pass,#86efac)' : it.status === 'FAIL' ? 'var(--log-fail,#fca5a5)' : '#fbbf24';
                return `<tr>
                <td style="padding:3px 8px">${escapeHtml(it.label)}</td>
                <td style="padding:3px 8px;font-family:monospace">${escapeHtml(it.pattern)}</td>
                <td style="padding:3px 8px;color:${c}">${escapeHtml(it.status)}</td>
              </tr>`;
              }).join('')}</tbody>
            </table>`
          : '<div style="color:var(--text-dim);font-size:12px;padding:6px 0">No per-pattern breakdown stored for this report.</div>';
        return `
        <div class="report-entry">
          <div class="golden-item" style="cursor:pointer" onclick="toggleReportDetail(event, '${rid}')">
            <span class="golden-name"><input type="checkbox" class="rep-check" value="${n}" onclick="event.stopPropagation()" onchange="updateSubscriberReportSelCount()">
              <span class="rep-arrow">▸</span> 👤 ${title} ${counts}</span>
            <div class="golden-actions" onclick="event.stopPropagation()">
              <a class="btn-xs btn-xs-view" href="/api/report/${encodeURIComponent(n)}" target="_blank">Open</a>
              <a class="btn-xs btn-xs-view" href="/api/report/${encodeURIComponent(n)}?download=1" download>Download</a>
              <button class="btn-xs btn-xs-del" onclick="deleteReport('${n}')">Delete</button>
            </div>
          </div>
          <div id="${rid}" class="report-detail hidden">${detail}</div>
        </div>`;
      }).join('')
    : 'No subscriber compare reports yet.';
  updateSubscriberReportSelCount();
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
  if (!confirm('⚠️ Delete ALL past reports? This cannot be undone. (Subscriber Compare Reports are untouched.)')) return;
  await fetch('/api/reports/delete', {method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({all: true, exclude_kind: 'subscriber_compare'})});
  loadReports();
}

// Subscriber Compare Reports has its own independent selection/delete —
// deleting here (or in Past Reports above) never touches the other section.
function selectedSubscriberReportNames() {
  return Array.from(document.querySelectorAll('#dash-subscriber-reports .rep-check:checked')).map(c => c.value);
}
function updateSubscriberReportSelCount() {
  const el = document.getElementById('sub-rep-sel-count');
  if (el) el.textContent = selectedSubscriberReportNames().length;
}
async function deleteSelectedSubscriberReports() {
  const names = selectedSubscriberReportNames();
  if (!names.length) { alert('No reports selected.'); return; }
  if (!confirm(`Delete ${names.length} selected subscriber compare report(s)?`)) return;
  await fetch('/api/reports/delete', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({names})});
  loadReports();
}
async function deleteAllSubscriberReports() {
  if (!confirm('⚠️ Delete ALL subscriber compare reports? This cannot be undone. (Past Reports are untouched.)')) return;
  await fetch('/api/reports/delete', {method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({all: true, kind: 'subscriber_compare'})});
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
