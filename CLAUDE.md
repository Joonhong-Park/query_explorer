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
DEFAULT_LIMIT   = 100
MAX_LIMIT       = 1000
```

- cluster3만 api_version `v54`, 나머지는 `v57`
- `fetch_all_clusters`는 `ThreadPoolExecutor(max_workers=max(1, len(targets)))`로 병렬 조회

---

## CM API 주요 사항

### impalaQueries 엔드포인트
```
GET /api/{api_version}/clusters/{CM_CLUSTER_NAME}/services/impala/impalaQueries
```

### 필터 문법 (`cm_client.py:build_filter`)

`build_filter(query_type, query_state, conditions)` 함수가 CM filter 표현식을 조립함.

- `conditions`: `[{"field": "user"|"keyword", "value": "..."}]` 형태의 리스트
  - `field="user"` → `user = "value"`
  - `field="keyword"` → `statement rlike "(?i).*value.*"`
  - 여러 조건은 모두 AND로 연결

생성되는 표현식 예시:
```
queryType = "QUERY"                          # query_type 파라미터
user = "alice"                               # conditions field=user
statement rlike "(?i).*mytable.*"            # conditions field=keyword
queryState rlike "(FINISHED|EXCEPTION)"     # 다중 상태: 괄호+파이프, 공백 없이
```
> **주의**: 다중 상태 rlike에서 `"FINISHED| RUNNING"` 처럼 공백이 들어가면 매칭 실패.
> 반드시 `"|".join(states)` (공백 없이) 사용.

> **queryState 필터**: 서버 코드에 `query_state` 파라미터가 존재하나, 프론트엔드는
> CM이 limit 적용 후 필터링하는 문제로 인해 이 파라미터를 전송하지 않음.
> 상태 필터는 클라이언트 사이드(상태 탭)에서 처리.

### 쿼리 상세 / 프로파일
```
GET /api/{api_version}/clusters/{CM_CLUSTER_NAME}/services/impala/impalaQueries/{queryId}/queryDetails
```
보관 기간이 짧아 404가 자주 발생 → friendly 메세지로 처리됨.

### CM 응답 필드 주요 매핑
| CM 필드 | 설명 |
|---------|------|
| `queryId` | 쿼리 고유 ID (전체 표시, 말줄임 없음) |
| `user` | 실행 사용자 |
| `queryState` | FINISHED / EXCEPTION / RUNNING / QUEUED |
| `queryType` | QUERY / SET / DDL / N/A |
| `statement` | SQL 전문 |
| `durationMillis` | 실행 시간(ms) |
| `rowsProduced` | 생성된 행 수 |
| `startTime` / `endTime` | ISO8601 시각 |
| `attributes.connected_user` | 연결 사용자 |
| `attributes.query_status` | 오류 메세지 (EXCEPTION 시 내용 있음) |

---

## API 엔드포인트

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/` | 프론트엔드 HTML |
| GET | `/health` | 헬스체크 |
| GET | `/api/clusters` | 클러스터 ID 목록 |
| GET | `/api/queries` | 쿼리 목록 조회 |
| GET | `/api/profile/{cluster_id}/{query_id}` | 쿼리 상세 (queryDetails, HTML 프록시) |
| GET | `/api/test/all` | 전체 클러스터 연결 테스트 |
| GET | `/api/test/{cluster_id}` | 단일 클러스터 연결 테스트 |

> `/api/profile`과 `/api/test/all`은 반드시 `/{cluster_id}` 라우트보다 **앞에** 선언해야 함.

### `/api/queries` 파라미터
| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `conditions` | str (JSON) | - | 조건 배열: `[{"field":"user"\|"keyword","value":"..."}]` |
| `query_type` | str | - | QUERY / SET / DDL / N/A |
| `query_state` | str | - | 쉼표 구분 상태 — 서버에 파라미터 존재하나 **프론트에서 미사용** (클라이언트 필터) |
| `hours` | int | - | 최근 N시간 (from/to 없을 때) |
| `from_time` | str | - | ISO8601 시작 시각 |
| `to_time` | str | - | ISO8601 종료 시각 |
| `limit` | int | 100 | 결과 수 (최대 1000) |
| `clusters` | str | - | 쉼표 구분 클러스터 ID |

> `from_time`/`to_time`, `hours` 모두 없으면 기본값 최근 **24시간** 적용.

---

## 프론트엔드 구조 (`templates/index.html`)

단일 HTML 파일 (Jinja2 미사용, `Path.read_text()`로 서빙).

### 검색 조건 (2행 구성)
**1행**: 사용자(고정) / 검색 조건(키워드 rlike 빌더) / 빠른 범위 / 시작일시 / 종료일시
**2행**: 클러스터 드롭다운 / Query Type / 결과 수 / 검색·초기화 버튼

- **사용자**: 고정 텍스트 입력 (조건 빌더 외부), `conditions` JSON의 `field=user`로 전송
- **검색 조건 빌더**: `+ 추가`로 키워드 rlike 조건 블록을 가로로 추가. 기본 1개 활성화. 첫 번째 블록은 AND 레이블 숨김. `conditions` JSON의 `field=keyword`로 전송
- Query Type 기본값: **QUERY** (초기화 시에도 QUERY로 복원)
- 빠른 범위 기본값: **1h**
- 결과 수 기본값: **100건**
- 빠른 범위 버튼: 1h / 6h / 12h / 24h / 3d / 7d / 15d / 30d

### 테이블 컬럼
| 컬럼 | 소스 필드 | 정렬 |
|------|----------|------|
| ▶ (펼침) | - | - |
| 클러스터 | `_cluster` | ✓ |
| Query ID | `queryId` | - |
| 사용자 | `user` | ✓ |
| Connected User | `attributes.connected_user` | - |
| 상태 | `queryState` | ✓ |
| Statement | `statement` | - |
| 실행시간 | `durationMillis` | - |
| Rows | `rowsProduced` | - |
| 시작시간 | `startTime` | ✓ |
| 종료시간 | `endTime` | ✓ |
| queryStatus | `attributes.query_status` | - |

- **정렬 가능 컬럼**: ↕ 아이콘 표시(비활성), ▲▼ 파란색(활성)
- **정렬 불가 컬럼**: 어두운 색, 커서 변화 없음

### 행 펼침 (lazy 렌더링)
- **초기 렌더링 시 SQL을 DOM에 포함하지 않음** → 대량 데이터 렌더링 성능 확보
- ▶ 클릭 시 해당 행만 SQL + `attributes.query_status` 삽입
- 한 번 열린 행은 이후 토글에서 재생성 없이 display만 전환
- 정렬·탭 전환 후 `_openRows`(queryId Set) 기준으로 펼침 상태 복원

### 탭 구조 (결과 상단 2개 탭 바)

| 탭 바 | 위치 | 옵션 | 역할 |
|-------|------|------|------|
| 상태 탭 | 위 | 전체 / FINISHED / RUNNING / EXCEPTION | 클라이언트 상태 필터링 |
| 클러스터 탭 | 아래 | 전체 / cluster1 / cluster2 … | 클라이언트 클러스터 필터링 |

- 두 탭은 **독립적으로 조합** 가능 (예: EXCEPTION + cluster1 → cluster1의 EXCEPTION만)
- **교차 건수 갱신**: 상태 탭 선택 시 클러스터 탭 건수가 해당 상태 기준으로 갱신, 반대도 동일
- 클러스터 드롭다운(검색 조건 2행)은 서버 요청 시 `clusters=` 전달용 (API 재호출 필요)
- 탭 우측 요약: `EXCEPTION / cluster1: 37건 / 전체 142건`

### 핵심 상태 변수
| 변수 | 설명 |
|------|------|
| `_allRows` | API에서 받은 전체 쿼리 목록 |
| `_rows` | 현재 탭 필터 적용된 표시 목록 |
| `_activeCluster` | 현재 선택된 클러스터 탭 (`""` = 전체) |
| `_activeState` | 현재 선택된 상태 탭 (`""` = 전체) |
| `_openRows` | 펼쳐진 행의 queryId Set (정렬/탭 전환 후 복원용) |

### query_status 색상
- 값이 `'OK'` → 녹색 블록 (`.ok-block`)
- 그 외 → 빨간 블록 (`.err-block`)

### Enter 키 범위
`.filters` 내부 요소 포커스 중일 때만 검색 실행 (다른 곳에서 Enter 입력 시 무시).

---

## 런처 (`launcher.py`)

### SSH 터널 경로
```
로컬:9090 → (paramiko) → TUNNEL_HOST:22 → NODE_HOST:22 → localhost:9090
```

### 비밀번호 암호화 저장
- 저장 위치: `%APPDATA%\QueryExplorer\credentials.dat`
- 암호화: `cryptography.Fernet` + SHA-256(COMPUTERNAME + USERNAME) 파생 키
- 다른 PC나 다른 사용자 계정에서는 복호화 불가 (기존 `.json` 평문 방식에서 변경)

### SSH 세션 모니터링
- 연결 후 10초마다 `transport.is_active()` 확인
- 끊김 감지 시 GUI 상태 "연결이 끊어졌습니다" 업데이트
- keepalive: 30초마다 패킷 전송으로 세션 유지

---

## 알려진 제약사항

- CM 응답 속도가 느릴 수 있음 (REQUEST_TIMEOUT=120s)
- 쿼리 프로파일(queryDetails) 보관 기간이 짧아 404 빈발
- `attributes.query_status`: EXCEPTION 쿼리의 오류 메세지. CM 버전에 따라 내용이 다를 수 있음
- self-signed 인증서 → `verify=False`, urllib3 경고 억제 처리됨
- CM API limit은 클러스터별로 적용 후 전체 합산에서 다시 limit 적용 (예: 클러스터 4개 × 100건 = 최대 400건 수신 후 시작시간 기준 상위 100건 반환)
