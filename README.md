# Query Explorer

Cloudera Manager(CM) API를 통해 여러 Impala 클러스터의 쿼리 이력을 조회·검색하는 웹 애플리케이션.

---

## 주요 기능

- 다중 Impala 클러스터 동시 조회 (ThreadPoolExecutor 병렬 처리)
- 사용자 / 키워드(SQL rlike) 조건 검색
- 검색 결과 SSE 스트리밍 (진행 상황 실시간 표시)
- 상태(FINISHED / RUNNING / EXCEPTION) × 클러스터 탭 교차 필터
- 쿼리 SQL 및 오류 메시지 행 펼침 (lazy 렌더링)
- 컬럼 정렬 (클러스터 / 사용자 / 상태 / 시작시간 / 종료시간)
- Windows 런처 `.exe`: SSH 터널 자동 연결 + 비밀번호 암호화 저장

---

## 파일 구조

```
query_explorer/
├── main.py          # FastAPI 앱, API 엔드포인트
├── cm_client.py     # CM API 호출, 필터 조립, 병렬/스트리밍 조회
├── config.py        # 클러스터 목록, 인증 정보, 포트 설정
├── launcher.py      # Windows 런처 (SSH 터널 + tkinter GUI)
└── templates/
    └── index.html   # 단일 파일 프론트엔드 (HTML + CSS + JS)
```

---

## 빠른 시작

### 서버 실행 (node1)

```bash
python main.py
# 또는
uvicorn main:app --host 0.0.0.0 --port 9090 --reload
```

브라우저에서 `http://localhost:9090` 접속.

### Windows 런처 빌드

```bash
pip install paramiko pyinstaller cryptography
pyinstaller --onefile --noconsole --name QueryExplorer launcher.py
```

생성된 `dist/QueryExplorer.exe` 실행 → SSH 터널 자동 연결 후 브라우저 열림.

---

## 환경 및 의존성

### 서버 (node1 — 폐쇄망)

| 패키지 | 버전 |
|--------|------|
| fastapi | 0.128.0 |
| uvicorn | 0.39.0 |
| requests | 2.32.5 |

> `httpx` 미설치 환경 → `requests` + `ThreadPoolExecutor` 사용.

### 런처 (Windows)

```
paramiko, cryptography, pyinstaller
```

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
REQUEST_TIMEOUT = 120
```

---

## 검색 동작 방식

CM impalaQueries API는 filter 파라미터 포함 시 내부 scan limit으로 결과가 극히 적음.
이를 우회하기 위해 **커서 페이지네이션** 방식으로 동작:

1. 조회 시간 범위를 **3분(CURSOR_CHUNK_HOURS)** 단위 청크로 분할
2. 각 청크를 filter 없이 `limit=1000(CURSOR_CHUNK_LIMIT)`으로 CM 요청
3. 수신된 결과에 Python 사이드 필터 적용 (user 일치, keyword rlike 매칭)
4. `seen_ids` set으로 청크 간 중복 제거
5. SSE `progress` 이벤트로 UI에 실시간 스트리밍

조건이 없는 경우에는 청크 분할 없이 단일 요청.

---

## API 엔드포인트

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/` | 프론트엔드 HTML |
| GET | `/health` | 헬스체크 |
| GET | `/api/clusters` | 클러스터 ID 목록 |
| GET | `/api/queries` | 쿼리 목록 조회 (SSE 스트리밍) |
| GET | `/api/profile/{cluster_id}/{query_id}` | 쿼리 상세 (queryDetails HTML 프록시) |
| GET | `/api/test/all` | 전체 클러스터 연결 테스트 |
| GET | `/api/test/{cluster_id}` | 단일 클러스터 연결 테스트 |

### `/api/queries` 주요 파라미터

| 파라미터 | 설명 |
|----------|------|
| `conditions` | JSON 배열: `[{"field":"user"\|"keyword","value":"..."}]` |
| `query_type` | QUERY / SET / DDL / N/A |
| `hours` | 최근 N시간 (기본 24h) |
| `from_time` / `to_time` | ISO8601 시간 범위 |
| `clusters` | 쉼표 구분 클러스터 ID |

---

## 런처 동작 (`launcher.py`)

```
로컬:9090 → (paramiko) → TUNNEL_HOST:22 → NODE_HOST:22 → localhost:9090
```

- 비밀번호 암호화 저장: `%APPDATA%\QueryExplorer\credentials.dat`
  - `cryptography.Fernet` + SHA-256(COMPUTERNAME + USERNAME) 파생 키
- SSH keepalive: 30초마다 패킷 전송
- 연결 상태 감지: 10초마다 `transport.is_active()` 확인

---

## 알려진 제약사항

- CM 응답 속도가 느릴 수 있음 (REQUEST_TIMEOUT=120s)
- 쿼리 프로파일(queryDetails) 보관 기간이 짧아 404 빈발
- self-signed 인증서 → `verify=False` (urllib3 경고 억제)
- cluster3은 api_version `v54` (나머지 v57)
