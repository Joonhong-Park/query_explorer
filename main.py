"""
Query Explorer — Step 5
검색 필터: user / table / database / 상태 / 시간 범위
"""

from asyncio import get_event_loop
from pathlib import Path
from typing import Optional

import requests as _requests
from requests.auth import HTTPBasicAuth

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, PlainTextResponse

from config import CM_CLUSTERS, CM_CLUSTER_NAME, CM_USERNAME, CM_PASSWORD, REQUEST_TIMEOUT, APP_PORT
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


@app.get("/api/profile/{cluster_id}/{query_id}", response_class=HTMLResponse)
async def get_query_profile(cluster_id: str, query_id: str):
    """CM에서 쿼리 프로파일을 가져와 브라우저에 표시"""
    cluster = next((c for c in CM_CLUSTERS if c["id"] == cluster_id), None)
    if not cluster:
        return HTMLResponse("<pre>cluster not found</pre>", status_code=404)

    url = (
        f"https://{cluster['host']}:{cluster['port']}"
        f"/api/{cluster['api_version']}"
        f"/clusters/{CM_CLUSTER_NAME}/services/impala"
        f"/impalaQueries/{query_id}/queryProfile"
    )

    try:
        resp = _requests.get(
            url,
            auth=HTTPBasicAuth(CM_USERNAME, CM_PASSWORD),
            verify=False,
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 404:
            return HTMLResponse(
                "<html><body style='background:#0f1117;color:#ef9a9a;font-family:monospace;padding:40px'>"
                "<h2>Profile Not Found</h2>"
                "<p>보관 기간이 지났거나 아직 생성되지 않은 프로파일입니다.</p>"
                "</body></html>",
                status_code=404,
            )
        resp.raise_for_status()
        profile_text = resp.json().get("profile", resp.text)
        safe = profile_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return HTMLResponse(
            f"<html><head><meta charset='UTF-8'><title>Query Profile</title>"
            f"<style>body{{background:#0d0f16;color:#ccc;font-family:monospace;font-size:12px;"
            f"padding:20px;white-space:pre-wrap;word-break:break-all;line-height:1.6}}</style></head>"
            f"<body>{safe}</body></html>"
        )
    except Exception as e:
        return HTMLResponse(
            f"<html><body style='background:#0f1117;color:#ef9a9a;font-family:monospace;padding:40px'>"
            f"<h2>Error</h2><pre>{e}</pre></body></html>",
            status_code=500,
        )


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
