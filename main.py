"""
Query Explorer — Step 4
기본 검색 UI: 쿼리 결과 테이블 + 클러스터 상태 표시
"""

from asyncio import get_event_loop

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from config import CM_CLUSTERS, APP_PORT
from cm_client import fetch_queries, fetch_all_clusters

app = FastAPI(title="Query Explorer")


# ── API ──────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/clusters")
async def list_clusters():
    return {"clusters": [c["id"] for c in CM_CLUSTERS]}


@app.get("/api/test/all")
async def test_all_clusters():
    loop = get_event_loop()
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


@app.get("/api/queries")
async def get_queries(limit: int = 100):
    """전체 클러스터 쿼리 조회 (UI용)"""
    loop = get_event_loop()
    result = await loop.run_in_executor(None, fetch_all_clusters, {"limit": limit})
    return result


# ── UI ───────────────────────────────────────────────────────────────────────

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
    padding: 12px 20px; display: flex; align-items: center; gap: 12px;
  }
  header h1 { font-size: 16px; color: #7eb8f7; }

  .toolbar {
    background: #13161f; border-bottom: 1px solid #2a2d3e;
    padding: 10px 20px; display: flex; align-items: center; gap: 10px;
  }
  .btn {
    padding: 6px 16px; border: none; border-radius: 4px;
    cursor: pointer; font-size: 13px; font-weight: 500;
  }
  .btn-primary { background: #3d5afe; color: #fff; }
  .btn-primary:hover { background: #536dfe; }
  .btn:disabled { opacity: 0.5; cursor: not-allowed; }

  select {
    background: #1e2130; border: 1px solid #2e3148; color: #e0e0e0;
    padding: 5px 10px; border-radius: 4px; font-size: 13px;
  }

  /* 클러스터 상태 바 */
  .status-bar {
    background: #0d0f16; border-bottom: 1px solid #1e2130;
    padding: 6px 20px; display: flex; gap: 10px; align-items: center; flex-wrap: wrap;
  }
  .cluster-badge {
    padding: 2px 10px; border-radius: 10px; font-size: 11px; font-weight: 600;
  }
  .badge-ok  { background: #1b5e2066; color: #66bb6a; border: 1px solid #2e7d32; }
  .badge-err { background: #b71c1c66; color: #ef9a9a; border: 1px solid #c62828; }
  .summary { margin-left: auto; font-size: 11px; color: #888; }

  /* 테이블 */
  .table-wrap { overflow: auto; height: calc(100vh - 160px); }
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
  .c-stmt    { max-width: 400px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; font-family: monospace; color: #ccc; }
  .c-dur     { text-align: right; white-space: nowrap; }
  .c-time    { white-space: nowrap; color: #888; }

  .badge-state { display: inline-block; padding: 2px 7px; border-radius: 10px; font-size: 10px; font-weight: 700; }
  .s-FINISHED  { background: #1b5e2066; color: #66bb6a; border: 1px solid #2e7d32; }
  .s-EXCEPTION { background: #b71c1c66; color: #ef9a9a; border: 1px solid #c62828; }
  .s-RUNNING   { background: #0d47a166; color: #90caf9; border: 1px solid #1565c0; }
  .s-QUEUED    { background: #e65100aa; color: #ffcc02; border: 1px solid #e65100; }

  .empty { text-align: center; padding: 60px; color: #555; }
  .loading { text-align: center; padding: 60px; color: #7eb8f7; }
</style>
</head>
<body>

<header>
  <h1>⚡ Query Explorer</h1>
</header>

<div class="toolbar">
  <select id="sel-limit">
    <option value="50">50건</option>
    <option value="100" selected>100건</option>
    <option value="200">200건</option>
    <option value="500">500건</option>
  </select>
  <button class="btn btn-primary" id="btn-search" onclick="search()">조회</button>
</div>

<div class="status-bar" id="status-bar">
  <span style="color:#555">조회 버튼을 눌러 시작하세요</span>
</div>

<div class="table-wrap" id="table-wrap">
  <div class="empty">조회 버튼을 눌러 시작하세요</div>
</div>

<script>
async function search() {
  const btn   = document.getElementById('btn-search');
  const limit = document.getElementById('sel-limit').value;

  btn.disabled = true;
  btn.textContent = '조회 중...';
  document.getElementById('table-wrap').innerHTML = '<div class="loading">⏳ 클러스터 조회 중...</div>';
  document.getElementById('status-bar').innerHTML = '';

  try {
    const res  = await fetch(`/api/queries?limit=${limit}`);
    const data = await res.json();
    renderStatus(data);
    renderTable(data.queries || []);
  } catch(e) {
    document.getElementById('table-wrap').innerHTML = `<div class="empty">오류: ${e.message}</div>`;
  } finally {
    btn.disabled = false;
    btn.textContent = '조회';
  }
}

function renderStatus(data) {
  const bar = document.getElementById('status-bar');
  const badges = (data.cluster_results || []).map(c => {
    const cls  = c.error ? 'badge-err' : 'badge-ok';
    const icon = c.error ? '✗' : '✓';
    const tip  = c.error ? c.error : `${c.count}건`;
    return `<span class="cluster-badge ${cls}">${icon} ${c.cluster} (${tip})</span>`;
  }).join('');
  bar.innerHTML = badges + `<span class="summary">총 ${data.total}건 표시 ${(data.queries||[]).length}건</span>`;
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
    <td><span class="badge-state s-${q.queryState}">${q.queryState ?? ''}</span></td>
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
  if (ms < 60000) return (ms/1000).toFixed(1) + 's';
  return Math.floor(ms/60000) + 'm ' + Math.floor((ms%60000)/1000) + 's';
}
function fmtTime(iso) {
  if (!iso) return '-';
  return new Date(iso).toLocaleString('ko-KR', {hour12: false});
}
function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=APP_PORT, reload=False)
