import logging
import math
import queue
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Generator, Optional

import requests
from requests.auth import HTTPBasicAuth

from config import CM_CLUSTERS, CM_CLUSTER_NAME, CM_USERNAME, CM_PASSWORD, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

requests.packages.urllib3.disable_warnings()  # self-signed cert 경고 억제

# 커서 페이지네이션 설정
CURSOR_CHUNK_HOURS  = 3 / 60  # 한 번에 요청하는 시간 범위 (3분)
CURSOR_CHUNK_LIMIT  = 1000    # CM에 보내는 limit


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


def fetch_all_clusters_stream(
    params: dict,
    cluster_ids: list = None,
    query_type: Optional[str] = None,
    conditions: list = None,
) -> Generator[dict, None, None]:
    """진행도를 dict로 yield하는 제너레이터.

    {"type":"progress", "chunk":n, "total":N, "collected":n}  — 청크 진행도
    {"type":"done", "queries":[...], "cluster_results":[...], "total":n}  — 완료
    """
    targets = CM_CLUSTERS
    if cluster_ids:
        targets = [c for c in CM_CLUSTERS if c["id"] in cluster_ids]

    # user / keyword 조건이 있을 때만 청크 조회 (query_type만 있는 경우 단순 요청)
    has_cond = any((c.get("value") or "").strip() for c in (conditions or []))

    logger.info("[fetch] query_type=%r has_cond=%s", query_type, bool(has_cond))

    # ── 조건 없음 (query_type만 있어도 해당): 단순 요청 ──────────────────────
    if not has_cond:
        yield {"type": "progress", "chunk": 0, "total": 0, "collected": 0}
        all_queries    = []
        cluster_results = []
        for res in _fetch_parallel(targets, {**params, "limit": CURSOR_CHUNK_LIMIT}):
            filtered = [q for q in res["queries"] if _matches_conditions(q, query_type, [])]
            all_queries.extend(filtered)
            cluster_results.append({"cluster": res["cluster"], "count": len(filtered), "error": res["error"]})
        all_queries.sort(key=lambda q: q.get("startTime", ""), reverse=True)
        yield {
            "type":            "done",
            "queries":         all_queries,
            "cluster_results": cluster_results,
            "total":           len(all_queries),
        }
        return

    # ── 조건 있음: 클러스터 간 병렬 + 클러스터 내 순차 청크 조회 ──────────────
    now     = datetime.now(timezone.utc)
    from_dt = _parse_dt(params["from"]) if params.get("from") else now - timedelta(hours=24)
    to_dt   = _parse_dt(params["to"])   if params.get("to")   else now

    # 청크 목록 사전 계산 (최신 → 과거 순)
    chunks    = []
    cursor_to = to_dt
    while cursor_to > from_dt:
        chunk_from = max(from_dt, cursor_to - timedelta(hours=CURSOR_CHUNK_HOURS))
        chunks.append((chunk_from, cursor_to))
        cursor_to = chunk_from

    total_tasks    = len(chunks) * len(targets)
    collected      = []
    seen_ids       = set()
    cluster_counts = {t["id"]: 0 for t in targets}
    cluster_errors = {t["id"]: 0 for t in targets}   # 오류 청크 수
    result_q       = queue.Queue()                    # 워커 → 제너레이터 전달용

    logger.info("[fetch] chunks=%d  targets=%d", len(chunks), len(targets))

    def _run_cluster(target: dict) -> None:
        """클러스터 1개의 전체 청크를 순차적으로 조회."""
        cid = target["id"]
        for cf, ct in chunks:
            p = {"limit": CURSOR_CHUNK_LIMIT, "from": cf.isoformat(), "to": ct.isoformat()}
            result_q.put((cid, cf, ct, fetch_queries(target, p)))

    # 클러스터별 스레드를 띄워 병렬 실행
    cluster_threads = [threading.Thread(target=_run_cluster, args=(t,), daemon=True) for t in targets]
    for th in cluster_threads:
        th.start()

    # 완료 감지용 스레드: 모든 클러스터 스레드가 끝나면 sentinel 투입
    def _sentinel():
        for th in cluster_threads:
            th.join()
        result_q.put(None)
    threading.Thread(target=_sentinel, daemon=True).start()

    completed = 0
    while True:
        item = result_q.get()
        if item is None:
            break

        cluster_id, chunk_from, chunk_to, res = item
        if res["error"]:
            cluster_errors[cluster_id] += 1
            logger.warning("chunk error  cluster=%s  %s ~ %s  error=%s",
                           cluster_id, chunk_from.isoformat(), chunk_to.isoformat(), res["error"])

        prev_count = len(collected)
        for q in res["queries"]:
            qid = q.get("queryId")
            if qid and qid in seen_ids:
                continue
            if qid:
                seen_ids.add(qid)
            if _matches_conditions(q, query_type, conditions):
                collected.append(q)
                cluster_counts[cluster_id] += 1

        completed += 1
        new_queries = collected[prev_count:]
        logger.info("task %d/%d  cluster=%s  %s ~ %s  new=%d  collected=%d",
                    completed, total_tasks, cluster_id,
                    chunk_from.isoformat(), chunk_to.isoformat(),
                    len(new_queries), len(collected))

        yield {
            "type":        "progress",
            "chunk":       completed,
            "total":       total_tasks,
            "collected":   len(collected),
            "chunk_from":  chunk_from.isoformat(),
            "chunk_to":    chunk_to.isoformat(),
            "new_queries": new_queries,
        }

    collected.sort(key=lambda q: q.get("startTime", ""), reverse=True)
    cluster_results = [
        {"cluster": cid, "count": cluster_counts[cid],
         "error": f"chunk errors: {cluster_errors[cid]}" if cluster_errors[cid] else None}
        for cid in cluster_counts
    ]
    yield {
        "type":            "done",
        "queries":         collected,
        "cluster_results": cluster_results,
        "total":           len(collected),
    }


def fetch_all_clusters(
    params: dict,
    cluster_ids: list = None,
    query_type: Optional[str] = None,
    conditions: list = None,
) -> dict:
    """fetch_all_clusters_stream의 블로킹 래퍼 (test 엔드포인트용)."""
    for event in fetch_all_clusters_stream(params, cluster_ids, query_type, conditions):
        if event["type"] == "done":
            return {k: v for k, v in event.items() if k != "type"}
    return {"queries": [], "cluster_results": [], "total": 0}
