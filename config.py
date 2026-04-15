CM_CLUSTERS = [
    {"id": "cluster1", "host": "cm1", "port": 7183, "api_version": "v57"},
    {"id": "cluster2", "host": "cm2", "port": 7183, "api_version": "v57"},
    {"id": "cluster3", "host": "cm3", "port": 7183, "api_version": "v54"},
    {"id": "cluster4", "host": "cm4", "port": 7183, "api_version": "v57"},
]

CM_CLUSTER_NAME = "CDP-Base"
CM_USERNAME     = "user"   # 실제 CM 계정으로 변경
CM_PASSWORD     = "pw"     # 실제 CM 비밀번호로 변경

APP_PORT        = 9090
REQUEST_TIMEOUT = 120      # 쿼리가 많을 경우 응답이 느릴 수 있으므로 여유있게 설정
DEFAULT_LIMIT   = 100
MAX_LIMIT       = 1000
