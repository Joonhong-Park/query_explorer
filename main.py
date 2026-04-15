"""
Query Explorer — Step 5
검색 필터: user / table / database / 상태 / 시간 범위
"""

from asyncio import get_event_loop
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

from config import CM_CLUSTERS, APP_PORT
from cm_client import build_filter, resolve_time_range, fetch_queries, fetch_all_clusters

app = FastAPI(title="Query Explorer")

TEMPLATE = Path(__file__).parent / "templates" / "index.html"


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
    keyword:     Optional[str] = Query(None),
    query_state: Optional[str] = Query(None),
    hours:       Optional[int] = Query(None),
    from_time:   Optional[str] = Query(None),
    to_time:     Optional[str] = Query(None),
    limit:       int           = Query(100, ge=1, le=1000),
    clusters:    Optional[str] = Query(None),  # 쉼표 구분 cluster ID
):
    filter_str       = build_filter(user, keyword, query_state)
    from_iso, to_iso = resolve_time_range(hours, from_time, to_time)

    params = {"limit": limit}
    if filter_str: params["filter"] = filter_str
    if from_iso:   params["from"]   = from_iso
    if to_iso:     params["to"]     = to_iso

    cluster_ids = [c.strip() for c in clusters.split(",")] if clusters else None

    loop   = get_event_loop()
    result = await loop.run_in_executor(None, fetch_all_clusters, params, cluster_ids)
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
    return TEMPLATE.read_text(encoding="utf-8")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=APP_PORT, reload=False)
