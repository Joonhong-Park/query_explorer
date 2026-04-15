"""
Query Explorer — Step 2
단일 클러스터 CM API 호출 확인
"""

from fastapi import FastAPI
from config import CM_CLUSTERS, APP_PORT
from cm_client import fetch_queries

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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=APP_PORT, reload=False)
