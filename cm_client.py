import logging
import requests
from requests.auth import HTTPBasicAuth

from config import CM_CLUSTER_NAME, CM_USERNAME, CM_PASSWORD, REQUEST_TIMEOUT

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
