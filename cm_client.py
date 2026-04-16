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

# 커서 페이지네이션 설정
CURSOR_CHUNK_HOURS = 6    # 커서 방식에서 한 번에 요청하는 시간 범위 (시간)
CURSOR_CHUNK_LIMIT = 1000 # 커서 방식에서 CM에 보내는 limit


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


def _parse_dt(s: str) -> datetime:
    """ISO8601 문자열 → timezone-aware datetime."""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def _matches_conditions(q: dict, query_type: Optional[str], conditions: list) -> bool:
    """Python 사이드 필터 매칭."""
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


def _fetch_parallel(targets: list, params: dict) -> list[dict]:
    """여러 클러스터에 동일한 params로 병렬 요청."""
    results = []
    with ThreadPoolExecutor(max_workers=max(1, len(targets))) as executor:
        futures = {executor.submit(fetch_queries, c, params): c for c in targets}
        for future in as_completed(futures):
            results.append(future.result())
    return results


def fetch_all_clusters(
    params: dict,
    cluster_ids: list = None,
    query_type: Optional[str] = None,
    conditions: list = None,
) -> dict:
    """전체(또는 선택된) 클러스터에서 쿼리를 조회합니다.

    조건(query_type / conditions)이 없으면 CM에 그대로 요청.
    조건이 있으면 시간 커서 페이지네이션으로 CM 스캔 한도를 우회:
      - 전체 시간 범위를 CURSOR_CHUNK_HOURS 단위로 최신 → 과거 순으로 분할 요청
      - 각 청크에 CM 필터 적용 (좁은 범위라 스캔 한도에 걸리지 않음)
      - Python에서 재검증 후 user_limit 건수가 모이면 중단
    """
    targets = CM_CLUSTERS
    if cluster_ids:
        targets = [c for c in CM_CLUSTERS if c["id"] in cluster_ids]

    user_limit = params.get("limit", 100)
    has_cond   = query_type or any((c.get("value") or "").strip() for c in (conditions or []))

    # ── 조건 없음: 기존 단순 요청 ────────────────────────────────────────────
    if not has_cond:
        all_queries    = []
        cluster_results = []
        for res in _fetch_parallel(targets, params):
            all_queries.extend(res["queries"])
            cluster_results.append({
                "cluster": res["cluster"],
                "count":   len(res["queries"]),
                "error":   res["error"],
            })
        all_queries.sort(key=lambda q: q.get("startTime", ""), reverse=True)
        return {
            "queries":         all_queries[:user_limit],
            "cluster_results": cluster_results,
            "total":           len(all_queries),
        }

    # ── 조건 있음: 시간 커서 페이지네이션 ────────────────────────────────────
    now      = datetime.now(timezone.utc)
    from_dt  = _parse_dt(params["from"]) if params.get("from") else now - timedelta(hours=24)
    to_dt    = _parse_dt(params["to"])   if params.get("to")   else now
    filter_str = build_filter(query_type, None, conditions)

    collected      = []
    seen_ids       = set()
    cluster_counts = {t["id"]: 0 for t in targets}
    cluster_errors = {t["id"]: None for t in targets}
    cursor_to      = to_dt

    while len(collected) < user_limit and cursor_to > from_dt:
        chunk_from = max(from_dt, cursor_to - timedelta(hours=CURSOR_CHUNK_HOURS))

        chunk_params = {
            "limit": CURSOR_CHUNK_LIMIT,
            "from":  chunk_from.isoformat(),
            "to":    cursor_to.isoformat(),
        }
        if filter_str:
            chunk_params["filter"] = filter_str

        for res in _fetch_parallel(targets, chunk_params):
            if res["error"]:
                cluster_errors[res["cluster"]] = res["error"]
            for q in res["queries"]:
                qid = q.get("queryId")
                if qid and qid in seen_ids:
                    continue
                if qid:
                    seen_ids.add(qid)
                if _matches_conditions(q, query_type, conditions):
                    collected.append(q)
                    cluster_counts[res["cluster"]] += 1

        logger.debug(
            "cursor chunk %s ~ %s  collected=%d/%d",
            chunk_from.isoformat(), cursor_to.isoformat(),
            len(collected), user_limit,
        )
        cursor_to = chunk_from

    collected.sort(key=lambda q: q.get("startTime", ""), reverse=True)

    cluster_results = [
        {"cluster": cid, "count": cluster_counts[cid], "error": cluster_errors[cid]}
        for cid in cluster_counts
    ]

    return {
        "queries":         collected[:user_limit],
        "cluster_results": cluster_results,
        "total":           len(collected),
    }
