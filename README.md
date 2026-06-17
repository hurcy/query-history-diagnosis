# Query History Diagnosis

Azure Databricks **SQL Warehouse** 의 시스템 테이블(`system.query.history` 등)을 읽어
**쿼리 속도 개선 / 쿼리 튜닝 / 파일 레이아웃 최적화** 후보를 도출하고, 결과를 Delta 테이블에
저장한 뒤 Lakeview 대시보드로 보여주는 Databricks Asset Bundle.

규칙 기반(LLM 미사용) · 수동 실행 · 비-Serverless 스팟 잡 클러스터로 동작한다.

## 설계 전제 (고객 환경)

- **Serverless Job 불가** → 소형 비-Serverless **스팟** Job Cluster 에서 실행
- **비용 민감** → 수동 실행, 스팟 컴퓨트, 시간/웨어하우스 필터 푸시다운, LLM 미사용
- **운영 구조** → 잡 등록 → 결과를 Delta 에 저장 → 대시보드 연결

`system.query.history` 는 쿼리 단위 집계 지표만 제공(연산자 트리 없음)하므로 본 도구는
**플릿 트리아지** 범위다. 연산자 수준 심층 튜닝은 개별 쿼리 Query Profile JSON 분석이 별도로 필요.

## Quick Start

```bash
# 0) databricks CLI 로그인
databricks auth login --host https://<your-workspace>

# 1) 설정
cp local-overrides.yml.sample local-overrides.yml
#   local-overrides.yml 에서 warehouse_id / qh_catalog / qh_schema 등 수정

# 2) 배포
databricks bundle deploy -t dev

# 3) 수동 실행 (analyze → dashboard)
databricks bundle run query_history_analysis -t dev
```

마지막에 분석 노트북 로그와 대시보드 URL 이 출력된다. 이후에는 Workflows UI 에서 **Run now** 로
수동 실행해도 된다.

## 구성

```
databricks.yml                         # DABs 번들 (잡 + 변수 + 타깃)
local-overrides.yml.sample             # 환경별 설정 템플릿
notebooks/
  01_query_history_analysis.py         # 시스템 테이블 → Gold Delta
  02_query_history_dashboard.py        # Gold → Lakeview 대시보드 (규칙 기반)
```

### 잡 흐름

```
[수동 실행] query_history_analysis (소형 스팟 Job Cluster, SINGLE_USER)
   ├─ task: analyze   →  01_query_history_analysis.py
   │     system.query.history (+billing.usage/list_prices, compute.warehouse_events,
   │     access.table_lineage, DESCRIBE DETAIL/HISTORY)  →  QH_* Gold Delta
   └─ task: dashboard →  02_query_history_dashboard.py  (analyze 성공 후)
         QH_* Gold  →  Lakeview 대시보드 생성/PATCH/공개
```

### 생성되는 Gold 테이블 (기본 prefix `QH_`)

| 테이블 | 키 | 내용 |
|---|---|---|
| `silver_query_history`      | `statement_id` | 분석 윈도우 내 정제 쿼리 행 |
| `gold_query_summary`        | `warehouse_id, fingerprint` | 정규화 쿼리 단위 집계 + 비용 귀속 |
| `gold_query_alerts`         | `warehouse_id, fingerprint, alert_id` | 규칙 기반 진단 |
| `gold_query_regression`     | `warehouse_id, fingerprint` | 최근 vs 과거 p50 악화 |
| `gold_warehouse_cost_daily` | `usage_date, warehouse_id, sku_name` | 일별 실비용(DBU·$) |
| `gold_warehouse_events`     | `warehouse_id, event_hour, event_type` | 시간대별 스케일/동시성 |
| `gold_table_health`         | `table_full_name` | 파일 수/평균 크기/OPTIMIZE 필요 |
| `gold_run_log`              | `etl_run_id` | 실행 메타데이터 |

모든 쓰기는 **idempotent MERGE** — 재실행해도 중복이 생기지 않는다.

### 진단(alert) 규칙

| alert_id | category | 신호 |
|---|---|---|
| `disk_spill`           | memory       | 평균 스필 ≥ 1GB (심각 ≥ 10GB) |
| `low_cache`            | cache        | 평균 캐시율 < 30% & read 규모 충분 |
| `low_file_pruning`     | file_layout  | 파일 프루닝율 < 50% & 파일 수 충분 → **파일 레이아웃** |
| `compilation_overhead` | compilation  | 컴파일 비중 ≥ 30% & 반복 실행 |
| `queue_pressure`       | concurrency  | 평균 큐/용량 대기 ≥ 5s (환경 요인) |
| `row_explosion`        | sql_pattern  | produced/read ≥ 10x |
| `frequent_slow`        | priority     | p95 ≥ 60s & 실행 ≥ 20회 (누적 영향 큼) |

임계값은 `notebooks/01_query_history_analysis.py` 의 `TH` dict 에서 조정.

## 사전 준비 — 권한 (가장 중요)

### 1) 시스템 스키마 활성화 (메타스토어 관리자 1회)

```bash
databricks system-schemas list <metastore-id>
databricks system-schemas enable <metastore-id> query      # 필수
databricks system-schemas enable <metastore-id> billing    # 비용(선택)
databricks system-schemas enable <metastore-id> compute    # 동시성(선택)
databricks system-schemas enable <metastore-id> access     # 테이블헬스 대상 식별(선택)
```

선택 스키마가 없으면 노트북이 해당 단계를 **건너뛰고 계속 진행**한다.

### 2) 잡 실행 주체(run-as)의 권한

- `SELECT` on `system.query.history` (및 활성화한 `system.billing.*` / `system.compute.*` / `system.access.table_lineage`)
- 출력 카탈로그/스키마에 `USE CATALOG` / `USE SCHEMA` / `CREATE TABLE` / `MODIFY`
- 테이블 헬스: 점검 대상 테이블에 `SELECT` (권한 없으면 자동 skip)
- 대시보드용 SQL Warehouse 에 `CAN_USE`

> dev 모드는 배포한 사용자가 run-as. 운영에서는 위 권한을 가진 **서비스 프린시펄**로
> `run_as` 지정을 권장.

## 대시보드 읽는 법

| 페이지 | 본다 |
|---|---|
| 개요 | KPI + 상위 진단 요약(증상→추천, 규칙 기반) |
| 느린·비싼 쿼리 | 실행빈도 × p95 산점도(우상단=최우선) + 누적 실행시간 순 표 |
| 진단(알림) | 카테고리×severity 막대 + 알림 상세 |
| 비용·동시성 | 일별 비용 추이 + 시간대별 최대 클러스터 수 |
| 회귀(악화) | 최근 p50 / 과거 p50 악화 쿼리 |
| 테이블 헬스 | 파일 수·평균 크기·OPTIMIZE 경과일·small files (파일 레이아웃) |

## 비용 메모

- 컴퓨트: 소형 스팟 Job Cluster, 잡 실행 중에만 과금. 소형 플릿은 `qh_num_workers: 0`(single-node)로 더 절감.
- 권장 주기: 주 1회/월 1회 수동 실행. 대시보드는 PATCH 로 같은 URL 유지.
- **비용 귀속은 근사**: per-query DBU 가 시스템 테이블에 없어 웨어하우스 실비용을
  `execution_duration_ms` 점유율로 분배. 절대값보다 **상대 순위**로 해석.

## 한계 / 확장

- 쿼리 단위 집계 기반 트리아지(연산자 트리 없음). shuffle/skew 등 연산자 수준은 제외.
- 더 깊이: 상위 문제 쿼리의 Query Profile JSON 을 받아 심층 분석기로 분석 가능(별도 작업).
- 컬럼 드리프트: 노트북은 `col_or_null` 로 런타임 스키마에 견고. 누락 컬럼은 NULL 처리되어
  해당 알림이 발화하지 않을 뿐 실패하지 않는다. 첫 dev 실행 후 `DESCRIBE system.query.history`
  로 컬럼명을 확인하고, 다르면 노트북의 `col_or_null(...)` 인자만 맞추면 된다.

## 라이선스 / 출처

Apache License 2.0 (`LICENSE`). 본 프로젝트는
[databricks-perf-toolkit](https://github.com/akuwano-db/databricks-perf-toolkit)(Apache-2.0)의
구조·컨벤션·일부 코드를 기반/파생했다. `NOTICE` 참조.
