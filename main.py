"""
Query Explorer — Step 5
검색 필터: user / table / database / 상태 / 시간 범위
"""

from asyncio import get_event_loop
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

from config import CM_CLUSTERS, APP_PORT
from cm_client import build_filter, resolve_time_range, fetch_queries, fetch_all_clusters

app = FastAPI(title="Query Explorer")


# ── API ───────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/clusters")
async def list_clusters():
    return {"clusters": [c["id"] for c in CM_CLUSTERS]}


@app.get("/api/queries")
async def get_queries(
    user:        Optional[str] = Query(None),
    table:       Optional[str] = Query(None),
    database:    Optional[str] = Query(None),
    query_state: Optional[str] = Query(None),
    hours:       Optional[int] = Query(None),
    from_time:   Optional[str] = Query(None),
    to_time:     Optional[str] = Query(None),
    limit:       int           = Query(100, ge=1, le=1000),
):
    filter_str          = build_filter(user, table, database, query_state)
    from_iso, to_iso    = resolve_time_range(hours, from_time, to_time)

    params = {"limit": limit}
    if filter_str:
        params["filter"] = filter_str
    if from_iso:
        params["from"] = from_iso
    if to_iso:
        params["to"] = to_iso

    loop   = get_event_loop()
    result = await loop.run_in_executor(None, fetch_all_clusters, params)
    result["filter_applied"] = filter_str
    return result


@app.get("/api/test/all")
async def test_all_clusters():
    loop   = get_event_loop()
    result = await loop.run_in_executor(None, fetch_all_clusters, {"limit": 5})
    return {
        "total":           result["total"],
        "cluster_results": result["cluster_results"],
        "sample":          result["queries"][:2],
    }


@app.get("/api/test/{cluster_id}")
async def test_cluster(cluster_id: str):
    cluster = next((c for c in CM_CLUSTERS if c["id"] == cluster_id), None)
    if not cluster:
        return {"error": f"cluster '{cluster_id}' not found"}
    result = fetch_queries(cluster, params={"limit": 5})
    return {
        "cluster": result["cluster"],
        "error":   result["error"],
        "count":   len(result["queries"]),
        "sample":  result["queries"][:2],
    }


# ── UI ────────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML


HTML = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<title>Query Explorer</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', sans-serif; background: #0f1117; color: #e0e0e0; font-size: 13px; }

  header {
    background: #1a1d2e; border-bottom: 1px solid #2a2d3e;
    padding: 12px 20px;
  }
  header h1 { font-size: 16px; color: #7eb8f7; }

  /* 필터 영역 */
  .filters {
    background: #13161f; border-bottom: 1px solid #2a2d3e;
    padding: 12px 20px; display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end;
  }
  .fg { display: flex; flex-direction: column; gap: 4px; }
  .fg label { font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 0.4px; }
  .fg input, .fg select {
    background: #1e2130; border: 1px solid #2e3148; color: #e0e0e0;
    padding: 5px 9px; border-radius: 4px; font-size: 13px;
  }
  .fg input:focus, .fg select:focus { outline: none; border-color: #5b7fe0; }
  .fg.wide input { min-width: 180px; }

  /* 시간 직접 입력 토글 */
  #custom-time { display: none; }

  .btn {
    padding: 6px 18px; border: none; border-radius: 4px;
    cursor: pointer; font-size: 13px; font-weight: 600;
  }
  .btn-primary { background: #3d5afe; color: #fff; }
  .btn-primary:hover { background: #536dfe; }
  .btn-secondary { background: #2a2d3e; color: #aaa; }
  .btn-secondary:hover { background: #363a52; }
  .btn:disabled { opacity: 0.5; cursor: not-allowed; }

  /* 상태 바 */
  .status-bar {
    background: #0d0f16; border-bottom: 1px solid #1e2130;
    padding: 6px 20px; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; min-height: 32px;
  }
  .cluster-badge { padding: 2px 10px; border-radius: 10px; font-size: 11px; font-weight: 600; }
  .badge-ok  { background: #1b5e2066; color: #66bb6a; border: 1px solid #2e7d32; }
  .badge-err { background: #b71c1c66; color: #ef9a9a; border: 1px solid #c62828; }
  .summary   { margin-left: auto; font-size: 11px; color: #888; }
  .filter-tag { font-family: monospace; font-size: 11px; color: #ffa726; }

  /* 테이블 */
  .table-wrap { overflow: auto; height: calc(100vh - 200px); }
  table { width: 100%; border-collapse: collapse; }
  thead th {
    background: #13161f; color: #888; text-align: left;
    padding: 8px 12px; border-bottom: 2px solid #2a2d3e;
    position: sticky; top: 0; white-space: nowrap;
  }
  tbody tr { border-bottom: 1px solid #1a1d2e; }
  tbody tr:hover { background: #1a1d2e; }
  tbody td { padding: 7px 12px; }

  .c-cluster { color: #7eb8f7; font-weight: 500; }
  .c-user    { color: #ce93d8; }
  .c-db      { color: #80cbc4; }
  .c-stmt    { max-width: 420px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; font-family: monospace; color: #ccc; }
  .c-dur     { text-align: right; white-space: nowrap; }
  .c-time    { white-space: nowrap; color: #888; }

  .s { display: inline-block; padding: 2px 7px; border-radius: 10px; font-size: 10px; font-weight: 700; }
  .s-FINISHED  { background: #1b5e2066; color: #66bb6a; border: 1px solid #2e7d32; }
  .s-EXCEPTION { background: #b71c1c66; color: #ef9a9a; border: 1px solid #c62828; }
  .s-RUNNING   { background: #0d47a166; color: #90caf9; border: 1px solid #1565c0; }
  .s-QUEUED    { background: #e65100aa; color: #ffcc02; border: 1px solid #e65100; }

  .empty   { text-align: center; padding: 60px; color: #555; }
  .loading { text-align: center; padding: 60px; color: #7eb8f7; }
</style>
</head>
<body>

<header><h1>⚡ Query Explorer</h1></header>

<div class="filters">
  <div class="fg wide">
    <label>User</label>
    <input id="f-user" type="text" placeholder="username">
  </div>
  <div class="fg wide">
    <label>Table</label>
    <input id="f-table" type="text" placeholder="table_name">
  </div>
  <div class="fg wide">
    <label>Database</label>
    <input id="f-database" type="text" placeholder="db_name">
  </div>
  <div class="fg">
    <label>상태</label>
    <select id="f-state">
      <option value="">전체</option>
      <option value="FINISHED">FINISHED</option>
      <option value="EXCEPTION">EXCEPTION</option>
      <option value="RUNNING">RUNNING</option>
      <option value="QUEUED">QUEUED</option>
    </select>
  </div>
  <div class="fg">
    <label>조회 범위</label>
    <select id="f-hours" onchange="toggleCustomTime()">
      <option value="1">최근 1시간</option>
      <option value="6">최근 6시간</option>
      <option value="24" selected>최근 24시간</option>
      <option value="72">최근 3일</option>
      <option value="168">최근 7일</option>
      <option value="custom">직접 입력</option>
    </select>
  </div>
  <div id="custom-time" style="display:flex; gap:8px;">
    <div class="fg">
      <label>시작</label>
      <input id="f-from" type="datetime-local">
    </div>
    <div class="fg">
      <label>종료</label>
      <input id="f-to" type="datetime-local">
    </div>
  </div>
  <div class="fg">
    <label>결과 수</label>
    <select id="f-limit">
      <option value="50">50건</option>
      <option value="100" selected>100건</option>
      <option value="200">200건</option>
      <option value="500">500건</option>
    </select>
  </div>
  <div class="fg" style="justify-content:flex-end">
    <label>&nbsp;</label>
    <div style="display:flex; gap:6px;">
      <button class="btn btn-primary"   id="btn-search" onclick="search()">검색</button>
      <button class="btn btn-secondary" onclick="reset()">초기화</button>
    </div>
  </div>
</div>

<div class="status-bar" id="status-bar">
  <span style="color:#555">검색 조건을 입력하고 검색하세요</span>
</div>

<div class="table-wrap" id="table-wrap">
  <div class="empty">검색 조건을 입력하고 검색하세요</div>
</div>

<script>
function toggleCustomTime() {
  const custom = document.getElementById('f-hours').value === 'custom';
  document.getElementById('custom-time').style.display = custom ? 'flex' : 'none';
}

async function search() {
  const btn = document.getElementById('btn-search');
  btn.disabled = true;
  btn.textContent = '조회 중...';
  document.getElementById('table-wrap').innerHTML = '<div class="loading">⏳ 클러스터 조회 중...</div>';

  const params = new URLSearchParams();
  const v = id => document.getElementById(id).value.trim();

  if (v('f-user'))     params.set('user',        v('f-user'));
  if (v('f-table'))    params.set('table',        v('f-table'));
  if (v('f-database')) params.set('database',     v('f-database'));
  if (v('f-state'))    params.set('query_state',  v('f-state'));
  params.set('limit', v('f-limit') || '100');

  if (v('f-hours') === 'custom') {
    if (v('f-from')) params.set('from_time', new Date(v('f-from')).toISOString());
    if (v('f-to'))   params.set('to_time',   new Date(v('f-to')).toISOString());
  } else {
    params.set('hours', v('f-hours'));
  }

  try {
    const res  = await fetch(`/api/queries?${params}`);
    const data = await res.json();
    renderStatus(data);
    renderTable(data.queries || []);
  } catch(e) {
    document.getElementById('table-wrap').innerHTML = `<div class="empty">오류: ${e.message}</div>`;
  } finally {
    btn.disabled = false;
    btn.textContent = '검색';
  }
}

function reset() {
  ['f-user','f-table','f-database','f-from','f-to'].forEach(id => document.getElementById(id).value = '');
  document.getElementById('f-state').value = '';
  document.getElementById('f-hours').value = '24';
  document.getElementById('f-limit').value = '100';
  document.getElementById('custom-time').style.display = 'none';
  document.getElementById('status-bar').innerHTML = '<span style="color:#555">검색 조건을 입력하고 검색하세요</span>';
  document.getElementById('table-wrap').innerHTML  = '<div class="empty">검색 조건을 입력하고 검색하세요</div>';
}

function renderStatus(data) {
  const badges = (data.cluster_results || []).map(c => {
    const cls = c.error ? 'badge-err' : 'badge-ok';
    const tip = c.error ? c.error : `${c.count}건`;
    return `<span class="cluster-badge ${cls}">${c.cluster} (${tip})</span>`;
  }).join('');
  const tag = data.filter_applied
    ? `<span class="filter-tag">filter: ${data.filter_applied}</span>` : '';
  document.getElementById('status-bar').innerHTML =
    badges + tag + `<span class="summary">총 ${data.total}건 / 표시 ${(data.queries||[]).length}건</span>`;
}

function renderTable(rows) {
  if (!rows.length) {
    document.getElementById('table-wrap').innerHTML = '<div class="empty">결과가 없습니다</div>';
    return;
  }
  const thead = `<thead><tr>
    <th>클러스터</th><th>사용자</th><th>DB</th><th>상태</th>
    <th>Statement</th><th>실행시간</th><th>시작시간</th>
  </tr></thead>`;

  const tbody = rows.map(q => `<tr>
    <td class="c-cluster">${q._cluster ?? ''}</td>
    <td class="c-user">${q.user ?? ''}</td>
    <td class="c-db">${q.database ?? ''}</td>
    <td><span class="s s-${q.queryState}">${q.queryState ?? ''}</span></td>
    <td class="c-stmt" title="${esc(q.statement ?? '')}">${esc(q.statement ?? '')}</td>
    <td class="c-dur">${fmtDur(q.durationMillis)}</td>
    <td class="c-time">${fmtTime(q.startTime)}</td>
  </tr>`).join('');

  document.getElementById('table-wrap').innerHTML =
    `<table>${thead}<tbody>${tbody}</tbody></table>`;
}

function fmtDur(ms) {
  if (ms == null) return '-';
  if (ms < 1000)  return ms + 'ms';
  if (ms < 60000) return (ms / 1000).toFixed(1) + 's';
  return Math.floor(ms / 60000) + 'm ' + Math.floor((ms % 60000) / 1000) + 's';
}
function fmtTime(iso) {
  if (!iso) return '-';
  return new Date(iso).toLocaleString('ko-KR', { hour12: false });
}
function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

document.addEventListener('keydown', e => {
  if (e.key === 'Enter') search();
});
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=APP_PORT, reload=False)
