import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.auth import HTTPBasicAuth

from config import CM_CLUSTERS, CM_CLUSTER_NAME, CM_USERNAME, CM_PASSWORD, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

requests.packages.urllib3.disable_warnings()  # self-signed cert 경고 억제


def fetch_queries(cluster: dict, params: dict) -> dict:
    """
    단일 클러스터에서 impalaQueries API를 호출합니다.

    Returns:
        {"cluster": str, "queries": list, "error": str|None}
    """
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
    """
    전체(또는 선택된) 클러스터에서 병렬로 쿼리를 조회합니다.

    Returns:
        {
            "queries": [...startTime 내림차순 정렬...],
            "cluster_results": [{"cluster", "count", "error"}, ...],
        }
    """
    targets = CM_CLUSTERS
    if cluster_ids:
        targets = [c for c in CM_CLUSTERS if c["id"] in cluster_ids]

    all_queries = []
    cluster_results = []

    with ThreadPoolExecutor(max_workers=len(targets)) as executor:
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
