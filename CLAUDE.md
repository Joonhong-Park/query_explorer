# Query Explorer — 프로젝트 가이드

## 개요

Cloudera Manager(CM) API를 통해 여러 Impala 클러스터의 쿼리 이력을 조회·검색하는 웹 애플리케이션.
- **서버**: node1 (GUI 없는 리눅스 서버)에서 FastAPI + uvicorn으로 실행
- **접근**: 로컬 PC → 터널링 서버 → node1 SSH 포트 포워딩(9090)
- **런처**: Windows용 `.exe` (paramiko SSH 자동 연결 + tkinter GUI)

---

## 파일 구조

```
query_explorer/
├── main.py          # FastAPI 앱, API 엔드포인트
├── cm_client.py     # CM API 호출, 필터 조립, 병렬 조회
├── config.py        # 클러스터 목록, 인증 정보, 포트 설정
├── launcher.py      # Windows 런처 (SSH 터널 + tkinter GUI)
└── templates/
    └── index.html   # 단일 파일 프론트엔드 (HTML + CSS + JS)
```

---

## 환경 및 의존성

### 서버 (node1) — 폐쇄망, pip install 불가
사전 설치된 패키지만 사용:
- `fastapi 0.128.0`
- `uvicorn 0.39.0`
- `requests 2.32.5`
- **httpx 없음** → 반드시 `requests` + `ThreadPoolExecutor` 사용

### 런처 (Windows)
```
pip install paramiko pyinstaller cryptography
pyinstaller --onefile --noconsole --name QueryExplorer launcher.py
```

---

## 실행

```bash
# node1에서
python main.py
# 또는
uvicorn main:app --host 0.0.0.0 --port 9090 --reload
```

앱 포트: **9090** (8888은 사용 중)

---

## 클러스터 설정 (`config.py`)

```python
CM_CLUSTERS = [
    {"id": "cluster1", "host": "cm1", "port": 7183, "api_version": "v57"},
    {"id": "cluster2", "host": "cm2", "port": 7183, "api_version": "v57"},
    {"id": "cluster3", "host": "cm3", "port": 7183, "api_version": "v54"},  # v54 주의
    {"id": "cluster4", "host": "cm4", "port": 7183, "api_version": "v57"},
]
CM_CLUSTER_NAME = "CDP-Base"
REQUEST_TIMEOUT = 120  # 쿼리 수가 많으면 응답이 느릴 수 있음
```

- cluster3만 api_version `v54`, 나머지는 `v57`
- `fetch_all_clusters`는 ThreadPoolExecutor로 병렬 조회

---

## CM API 주요 사항

### impalaQueries 엔드포인트
```
GET /api/{api_version}/clusters/{CM_CLUSTER_NAME}/services/impala/impalaQueries
```

### 필터 문법 (`cm_client.py:build_filter`)
```
user = "username"
statement rlike "(?i).*keyword.*"
queryState rlike "(FINISHED|EXCEPTION)"   # 다중 상태: 괄호+파이프, 공백 없이
```
> **주의**: 다중 상태 rlike에서 `"FINISHED| RUNNING"` 처럼 공백이 들어가면 매칭 실패.
> 반드시 `"|".join(states)` (공백 없이) 사용.

### 쿼리 프로파일
```
GET /api/{api_version}/clusters/{CM_CLUSTER_NAME}/services/impala/impalaQueries/{queryId}/queryDetails
```
보관 기간이 짧아 404가 자주 발생 → friendly 메세지로 처리됨.

---

## 프론트엔드 구조 (`templates/index.html`)

단일 HTML 파일 (Jinja2 미사용, `Path.read_text()`로 서빙).

### 핵심 상태 변수
| 변수 | 설명 |
|------|------|
| `_allRows` | API에서 받은 전체 쿼리 목록 |
| `_rows` | 현재 탭 필터 적용된 표시 목록 |
| `_activeCluster` | 현재 선택된 클러스터 탭 (`""` = 전체) |
| `_openRows` | 펼쳐진 행의 queryId Set (정렬/탭 전환 후 복원용) |

### 동작 흐름
1. 검색 버튼 → `/api/queries` 호출 → `_allRows` 저장
2. 드롭다운 선택 클러스터 탭 자동 활성화
3. 탭 클릭 → `_allRows` 클라이언트 필터링 → `_rows` 갱신 (API 재호출 없음)
4. 헤더 클릭 → `_rows` 정렬 → `_openRows` 기준으로 펼침 상태 복원

### 클러스터 이중 구조
- **드롭다운** (검색 조건 2행): 선택 후 검색 → API에 `clusters=cluster1` 전달 (서버 필터)
- **탭** (검색 결과 상단): 클릭 시 즉시 클라이언트 필터링

### Enter 키 범위
`.filters` 내부 요소 포커스 중일 때만 검색 실행.

---

## 런처 (`launcher.py`)

### SSH 터널 경로
```
로컬:9090 → (paramiko) → TUNNEL_HOST:22 → NODE_HOST:22 → localhost:9090
```

### 비밀번호 암호화 저장
- 저장 위치: `%APPDATA%\QueryExplorer\credentials.dat`
- 암호화: `cryptography.Fernet` + SHA-256(COMPUTERNAME + USERNAME) 파생 키
- 다른 PC나 다른 사용자 계정에서는 복호화 불가

### SSH 세션 모니터링
- 연결 후 10초마다 `transport.is_active()` 확인
- 끊김 감지 시 GUI 상태 "연결이 끊어졌습니다" 업데이트
- keepalive: 30초마다 패킷 전송으로 세션 유지

---

## API 엔드포인트

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/` | 프론트엔드 HTML |
| GET | `/health` | 헬스체크 |
| GET | `/api/clusters` | 클러스터 ID 목록 |
| GET | `/api/queries` | 쿼리 목록 조회 |
| GET | `/api/profile/{cluster_id}/{query_id}` | 쿼리 프로파일 (HTML 프록시) |
| GET | `/api/test/all` | 전체 클러스터 연결 테스트 |
| GET | `/api/test/{cluster_id}` | 단일 클러스터 연결 테스트 |

> `/api/profile`과 `/api/test/all`은 반드시 `/{cluster_id}` 라우트보다 **앞에** 선언해야 함.

### `/api/queries` 파라미터
| 파라미터 | 타입 | 설명 |
|----------|------|------|
| `user` | str | 사용자명 필터 |
| `keyword` | str | statement rlike 검색 |
| `query_state` | str | 쉼표 구분 상태 (예: `FINISHED,EXCEPTION`) |
| `hours` | int | 최근 N시간 (from/to 없을 때) |
| `from_time` | str | ISO8601 시작 시각 |
| `to_time` | str | ISO8601 종료 시각 |
| `limit` | int | 결과 수 (기본 100, 최대 1000) |
| `clusters` | str | 쉼표 구분 클러스터 ID |

---

## 알려진 제약사항

- CM 응답 속도가 느릴 수 있음 (REQUEST_TIMEOUT=120s)
- 쿼리 프로파일 보관 기간이 짧아 404 빈발
- `queryStatus` 필드: EXCEPTION 쿼리의 오류 메세지. CM 버전에 따라 내용이 다를 수 있음
- self-signed 인증서 → `verify=False`, urllib3 경고 억제 처리됨
