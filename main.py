"""
Query Explorer — Step 1
최소 실행 확인용: 앱 기동 + 클러스터 목록 API
"""

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from config import CM_CLUSTERS, APP_PORT

app = FastAPI(title="Query Explorer")


@app.get("/api/clusters")
async def list_clusters():
    """설정된 클러스터 목록 반환"""
    return {"clusters": [c["id"] for c in CM_CLUSTERS]}


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=APP_PORT, reload=False)
