import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from requests.auth import HTTPBasicAuth

from config import CM_CLUSTERS, CM_CLUSTER_NAME, CM_USERNAME, CM_PASSWORD, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

requests.packages.urllib3.disable_warnings()  # self-signed cert 경고 억제


def build_filter(
    query_type: Optional[str] = None,   # QUERY / SET / DDL / N/A
    query_state: Optional[str] = None,  # 쉼표 구분 다중 상태 (예: "FINISHED,EXCEPTION")
    conditions: list = None,            # [{"field": "user"|"keyword", "value": "..."}]
) -> str:
    """
    CM impalaQueries filter 표현식 조립.
    conditions 예시:
        [{"field": "user",    "value": "alice"}]   → user = "alice"
        [{"field": "keyword", "value": "mytable"}] → statement rlike "(?i).*mytable.*"
    여러 조건은 모두 AND로 연결됨.
    """
    parts = []

    if query_type:
        parts.append(f'queryType = "{query_type}"')
    for cond in (conditions or []):
        field = cond.get("field", "")
        value = (cond.get("value") or "").strip()
        if not value:
            continue
        if field == "user":
            parts.append(f'user = "{value}"')
        elif field == "keyword":
            parts.append(f'statement rlike "(?i).*{re.escape(value)}.*"')
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


def _matches_conditions(q: dict, query_type: Optional[str], conditions: list) -> bool:
    """Python 사이드 필터 — CM 스캔 한도 우회용."""
    if query_type and q.get("queryType") != query_type:
        return False
    for cond in conditions:
        field = cond.get("field", "")
        value = (cond.get("value") or "").strip()
        if not value:
            continue
        if field == "user":
            if q.get("user", "") != value:
                return False
        elif field == "keyword":
            if value.lower() not in q.get("statement", "").lower():
                return False
    return True


def fetch_all_clusters(
    params: dict,
    cluster_ids: list = None,
    query_type: Optional[str] = None,
    conditions: list = None,
) -> dict:
    """전체(또는 선택된) 클러스터에서 병렬로 쿼리를 조회합니다.

    query_type / conditions 가 있으면 CM 에 필터를 보내지 않고 Python 에서 직접 필터링.
    CM 은 필터 적용 시 내부 스캔 한도(scan limit)에 걸려 극소수만 반환하는 문제가 있음.
    """
    targets = CM_CLUSTERS
    if cluster_ids:
        targets = [c for c in CM_CLUSTERS if c["id"] in cluster_ids]

    user_limit   = params.get("limit", 100)
    has_cond     = query_type or any((c.get("value") or "").strip() for c in (conditions or []))

    if has_cond:
        # CM 에 필터·limit 없이 요청 → 시간 범위 내 전체 수신 후 Python 필터링
        cm_params = {k: v for k, v in params.items() if k not in ("filter", "limit")}
    else:
        cm_params = params

    all_queries = []
    cluster_results = []

    with ThreadPoolExecutor(max_workers=max(1, len(targets))) as executor:
        futures = {executor.submit(fetch_queries, c, cm_params): c for c in targets}
        for future in as_completed(futures):
            result = future.result()
            all_queries.extend(result["queries"])
            cluster_results.append({
                "cluster": result["cluster"],
                "count":   len(result["queries"]),
                "error":   result["error"],
            })

    all_queries.sort(key=lambda q: q.get("startTime", ""), reverse=True)

    if has_cond:
        all_queries = [q for q in all_queries if _matches_conditions(q, query_type, conditions or [])]

    return {
        "queries":         all_queries[:user_limit],
        "cluster_results": cluster_results,
        "total":           len(all_queries),
    }
