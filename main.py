"""
Query Explorer — Step 3
5개 클러스터 병렬 조회 + 결과 병합
"""

from fastapi import FastAPI
from config import CM_CLUSTERS, APP_PORT
from cm_client import fetch_queries, fetch_all_clusters

app = FastAPI(title="Query Explorer")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/clusters")
async def list_clusters():
    return {"clusters": [c["id"] for c in CM_CLUSTERS]}


@app.get("/api/test/{cluster_id}")
async def test_cluster(cluster_id: str):
    """단일 클러스터 API 호출 테스트 (최근 1시간, 5건)"""
    cluster = next((c for c in CM_CLUSTERS if c["id"] == cluster_id), None)
    if not cluster:
        return {"error": f"cluster '{cluster_id}' not found"}

    result = fetch_queries(cluster, params={"limit": 5})
    return {
        "cluster": result["cluster"],
        "error": result["error"],
        "count": len(result["queries"]),
        "sample": result["queries"][:2],  # 응답 구조 확인용 2건만
    }


@app.get("/api/test/all")
async def test_all_clusters():
    """전체 클러스터 병렬 조회 테스트 (클러스터당 5건)"""
    from asyncio import get_event_loop
    loop = get_event_loop()
    result = await loop.run_in_executor(
        None, fetch_all_clusters, {"limit": 5}
    )
    return {
        "total":           result["total"],
        "cluster_results": result["cluster_results"],
        "sample":          result["queries"][:2],
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=APP_PORT, reload=False)
