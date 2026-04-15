import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from requests.auth import HTTPBasicAuth

from config import CM_CLUSTERS, CM_CLUSTER_NAME, CM_USERNAME, CM_PASSWORD, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

requests.packages.urllib3.disable_warnings()  # self-signed cert 경고 억제


def build_filter(
    user: Optional[str] = None,
    keyword: Optional[str] = None,
    query_state: Optional[str] = None,  # 쉼표 구분 다중 상태 가능 (예: "FINISHED,EXCEPTION")
    query_type: Optional[str] = None,   # QUERY / SET / DDL / N/A
) -> str:
    """
    CM impalaQueries filter 표현식 조립.

    keyword 검색 예시:
        "mytable"     → statement에 mytable 포함
        "mydb.mytable"→ statement에 mydb.mytable 포함
    """
    parts = []

    if query_type:
        parts.append(f'queryType = "{query_type}"')
    if user:
        parts.append(f'user = "{user}"')
    if keyword:
        parts.append(f'statement rlike "(?i).*{keyword}.*"')
    if query_state:
        states = [s.strip() for s in query_state.split(",") if s.strip()]
        if len(states) == 1:
            parts.append(f'queryState = "{states[0]}"')
        elif len(states) > 1:
            parts.append(f'queryState rlike "({"|".join(states)})"')

    return " AND ".join(parts)


def resolve_time_range(
    hours: Optional[int] = None,
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
) -> tuple[Optional[str], Optional[str]]:
    """
    시간 범위를 ISO8601 문자열로 반환.
    hours가 있으면 최근 N시간, 없으면 from_time/to_time 그대로 사용.
    둘 다 없으면 최근 24시간 기본값.
    """
    if from_time and to_time:
        return from_time, to_time

    now = datetime.now(timezone.utc)
    h   = hours if hours else 24
    return (now - timedelta(hours=h)).isoformat(), now.isoformat()


def fetch_queries(cluster: dict, params: dict) -> dict:
    """단일 클러스터에서 impalaQueries API를 호출합니다."""
    url = (
        f"https://{cluster['host']}:{cluster['port']}"
        f"/api/{cluster['api_version']}"
        f"/clusters/{CM_CLUSTER_NAME}/services/impala/impalaQueries"
    )

    try:
        resp = requests.get(
            url,
            params=params,
            auth=HTTPBasicAuth(CM_USERNAME, CM_PASSWORD),
            verify=False,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        queries = resp.json().get("queries", [])
        for q in queries:
            q["_cluster"] = cluster["id"]
        return {"cluster": cluster["id"], "queries": queries, "error": None}

    except requests.exceptions.Timeout:
        logger.warning("[%s] timeout", cluster["id"])
        return {"cluster": cluster["id"], "queries": [], "error": "timeout"}

    except requests.exceptions.HTTPError as e:
        logger.warning("[%s] HTTP %s", cluster["id"], e.response.status_code)
        return {"cluster": cluster["id"], "queries": [], "error": f"HTTP {e.response.status_code}"}

    except Exception as e:
        logger.error("[%s] %s", cluster["id"], e)
        return {"cluster": cluster["id"], "queries": [], "error": str(e)}


def fetch_all_clusters(params: dict, cluster_ids: list = None) -> dict:
    """전체(또는 선택된) 클러스터에서 병렬로 쿼리를 조회합니다."""
    targets = CM_CLUSTERS
    if cluster_ids:
        targets = [c for c in CM_CLUSTERS if c["id"] in cluster_ids]

    all_queries = []
    cluster_results = []

    with ThreadPoolExecutor(max_workers=max(1, len(targets))) as executor:
        futures = {executor.submit(fetch_queries, c, params): c for c in targets}
        for future in as_completed(futures):
            result = future.result()
            all_queries.extend(result["queries"])
            cluster_results.append({
                "cluster": result["cluster"],
                "count":   len(result["queries"]),
                "error":   result["error"],
            })

    all_queries.sort(key=lambda q: q.get("startTime", ""), reverse=True)

    limit = params.get("limit", len(all_queries))
    return {
        "queries":         all_queries[:limit],
        "cluster_results": cluster_results,
        "total":           len(all_queries),
    }
