# Databricks notebook source
# MAGIC %md
# MAGIC # Query History Analysis — SQL Warehouse 시스템 테이블 기반 플릿 트리아지
# MAGIC
# MAGIC `system.query.history` 등 시스템 테이블을 읽어 **쿼리 속도 개선 / 쿼리 튜닝 / 파일 레이아웃 최적화**
# MAGIC 후보를 도출하고, 결과를 Gold Delta 테이블로 저장합니다. (LLM 미사용 — 전부 규칙 기반)
# MAGIC
# MAGIC **설계 전제 (고객 환경):**
# MAGIC - Serverless Job 사용 불가 → 본 노트북은 소형 비-Serverless Job Cluster(스팟)에서 수동 실행
# MAGIC - 비용 민감 → 시간 필터/웨어하우스 필터를 푸시다운, 필요한 컬럼만 select
# MAGIC - `system.query.history` 는 쿼리 단위 **집계 지표만** 제공(연산자 트리 없음) → "플릿 트리아지" 범위
# MAGIC
# MAGIC **읽는 시스템 테이블:**
# MAGIC - `system.query.history`            (필수) — 쿼리 단위 메트릭
# MAGIC - `system.billing.usage` + `list_prices` (선택) — 실비용(DBU·금액)
# MAGIC - `system.compute.warehouse_events`  (선택) — 스케일/큐/재시작(동시성)
# MAGIC - `system.access.table_lineage`      (선택) — 상위 쿼리가 건드린 테이블 식별
# MAGIC - `DESCRIBE DETAIL/HISTORY`          (선택) — 테이블 헬스(파일 레이아웃)
# MAGIC
# MAGIC **생성하는 Gold 테이블 (TABLE_PREFIX 기본 `QH_`):**
# MAGIC | 테이블 | 키 | 내용 |
# MAGIC |---|---|---|
# MAGIC | `silver_query_history`     | statement_id            | 분석 윈도우 내 정제된 쿼리 행 (추적용) |
# MAGIC | `gold_query_summary`       | warehouse_id, fingerprint | 정규화 쿼리(핑거프린트) 단위 집계 |
# MAGIC | `gold_query_alerts`        | warehouse_id, fingerprint, alert_id | 규칙 기반 진단(ActionCard 유사) |
# MAGIC | `gold_query_regression`    | warehouse_id, fingerprint | 최근 vs 과거 악화 탐지 |
# MAGIC | `gold_warehouse_cost_daily`| usage_date, warehouse_id, sku_name | 일별 실비용 |
# MAGIC | `gold_warehouse_events`    | warehouse_id, event_hour, event_type | 시간대별 스케일/동시성 |
# MAGIC | `gold_table_health`        | table_full_name         | 파일 수/크기/OPTIMIZE 필요 여부 |
# MAGIC | `gold_run_log`             | etl_run_id              | 실행 메타데이터(감사) |
# MAGIC
# MAGIC 진단 임계값과 비용 모델은 `dabs/app/core/constants.py` / `dbsql_cost.py` / `fingerprint.py` 의
# MAGIC 로직을 미러링했습니다(노트북 자체완결 컨벤션 — `01_Spark Perf Pipeline` 와 동일). 변경 시 함께 동기화하세요.

# COMMAND ----------

# ┌─────────────────────────────────────────────────────────────────────────┐
# │  CONFIGURATION — ウィジェット(위젯)으로 동적 변경 가능                      │
# └─────────────────────────────────────────────────────────────────────────┘
dbutils.widgets.text("catalog",       "main",     "Catalog (출력 Gold 테이블)")
dbutils.widgets.text("schema",        "default",  "Schema (출력 Gold 테이블)")
dbutils.widgets.text("table_prefix",  "QH_",      "Table Name Prefix")
dbutils.widgets.text("warehouse_ids", "",         "Warehouse ID 필터 (콤마구분, 비우면 전체)")
dbutils.widgets.text("lookback_days", "30",       "분석 기간(일)")
dbutils.widgets.text("recent_days",   "7",        "회귀 탐지: 최근 구간(일)")
dbutils.widgets.text("top_n_tables",  "50",       "테이블 헬스 점검 대상 상위 N개")
dbutils.widgets.dropdown("enable_cost",             "true", ["true", "false"], "비용(billing) 포함")
dbutils.widgets.dropdown("enable_warehouse_events", "true", ["true", "false"], "warehouse_events 포함")
dbutils.widgets.dropdown("enable_table_health",     "true", ["true", "false"], "테이블 헬스 포함")

CATALOG       = dbutils.widgets.get("catalog").strip()
_SCHEMA       = dbutils.widgets.get("schema").strip()
SCHEMA        = f"{CATALOG}.{_SCHEMA}"
TABLE_PREFIX  = dbutils.widgets.get("table_prefix").strip()
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
RECENT_DAYS   = int(dbutils.widgets.get("recent_days"))
TOP_N_TABLES  = int(dbutils.widgets.get("top_n_tables"))
ENABLE_COST   = dbutils.widgets.get("enable_cost") == "true"
ENABLE_WHEV   = dbutils.widgets.get("enable_warehouse_events") == "true"
ENABLE_HEALTH = dbutils.widgets.get("enable_table_health") == "true"

_wh_raw = dbutils.widgets.get("warehouse_ids").strip()
WAREHOUSE_IDS = [w.strip() for w in _wh_raw.split(",") if w.strip()] if _wh_raw else []

# 입력 가드 — 흔한 실수(미치환 placeholder)를 조기 실패시킨다
import re as _re
assert _re.fullmatch(r"[A-Za-z0-9_]+", CATALOG), f"catalog 가 비정상입니다: {CATALOG!r}"
assert _re.fullmatch(r"[A-Za-z0-9_]+", _SCHEMA),  f"schema 가 비정상입니다: {_SCHEMA!r}"
assert 1 <= LOOKBACK_DAYS <= 365, "lookback_days 는 1~365 범위여야 합니다"
assert 1 <= RECENT_DAYS < LOOKBACK_DAYS, "recent_days 는 1 이상, lookback_days 미만이어야 합니다"

print(f"출력      : {SCHEMA}.{TABLE_PREFIX}*")
print(f"분석기간  : 최근 {LOOKBACK_DAYS}일 (회귀: 최근 {RECENT_DAYS}일 vs 그 이전)")
print(f"WH 필터   : {WAREHOUSE_IDS or '전체'}")
print(f"옵션      : cost={ENABLE_COST}  warehouse_events={ENABLE_WHEV}  table_health={ENABLE_HEALTH}")

# COMMAND ----------

# ── Imports · 상수 · 헬퍼 (외부 의존 없음, 자체완결) ──────────────────────────
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

RUN_TS = datetime.now(timezone.utc)
RUN_ID = f"run_{RUN_TS:%Y%m%d_%H%M%S}"
ETL_VERSION = "qh-v1.0.0"

# 진단 임계값 — core/constants.py(분석기 임계값) + dbsql_cost.py 를 미러링.
# 쿼리 단위 집계 지표만으로 판정 가능한 항목으로 한정(연산자 트리 불요).
TH = dict(
    spill_warn_gb       = 1.0,    # 디스크 스필 경고 (쿼리당 평균)
    spill_crit_gb       = 10.0,   # 디스크 스필 심각
    cache_low_pct       = 30.0,   # 캐시 적중률 낮음 (read_io_cache_percent)
    cache_min_read_gb   = 1.0,    # 캐시 알림은 read 규모가 충분할 때만(노이즈 억제)
    pruning_low_ratio   = 0.5,    # 파일 프루닝 비율 낮음
    pruning_min_files   = 1000,   # 프루닝 알림은 파일 수가 충분할 때만
    compile_ratio_warn  = 0.30,   # 컴파일 시간 비중 높음
    compile_min_runs    = 5,      # 반복 실행될 때만 컴파일 오버헤드가 유의
    queue_warn_ms       = 5000,   # 큐/용량 대기 평균 5s+
    row_amp_warn        = 10.0,   # 행 증폭(produced/read)
    slow_p95_ms         = 60000,  # 느린 쿼리 p95 60s+
    frequent_runs       = 20,     # 빈번 실행
    regression_ratio    = 1.5,    # 회귀: 최근 p50 / 과거 p50
    regression_min_ms   = 5000,   # 회귀 판정 최소 절대 시간(노이즈 억제)
    regression_min_runs = 3,      # 양 구간 최소 실행 수
    small_file_mb       = 32.0,   # 테이블 헬스: 평균 파일 크기 < 32MB → small files
    small_file_min_n    = 100,    # small files 알림 최소 파일 수
    stale_optimize_days = 30,     # OPTIMIZE 미수행 경과일
)
GB = 1024.0 ** 3
MB = 1024.0 ** 2


def col_or_null(df: DataFrame, name: str, cast: str | None = None):
    """존재하면 컬럼, 없으면 NULL. 런타임별 system table 스키마 드리프트에 견고.
    'struct.field' 형태는 최상위 struct 존재만 확인한다."""
    top = name.split(".")[0]
    c = F.col(name) if top in df.columns else F.lit(None)
    return c.cast(cast) if cast else c


def table_readable(name: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {name}")
        return True
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠ {name} 접근 불가 — 건너뜀 ({str(e).splitlines()[0][:120]})")
        return False


def fingerprint_expr(text_col):
    """SQL 정규화 후 sha2 해시. core/fingerprint.py 의 정규화 아이디어를 SQL 로 구현
    (대량 처리용). 리터럴/숫자/주석/공백을 제거해 '같은 모양' 쿼리를 묶는다."""
    t = F.lower(text_col)
    t = F.regexp_replace(t, r"--[^\n]*", " ")     # 라인 주석
    t = F.regexp_replace(t, r"/\*.*?\*/", " ")    # 블록 주석
    t = F.regexp_replace(t, r"'[^']*'", "?")      # 문자열 리터럴
    t = F.regexp_replace(t, r"\b\d+\b", "?")      # 숫자
    t = F.regexp_replace(t, r"\s+", " ")          # 공백
    t = F.trim(t)
    return F.sha2(t, 256)


def _save(df: DataFrame, table_name: str, merge_keys=None, partition_by=None) -> str:
    """idempotent 저장. merge_keys 있으면 MERGE(upsert), 없으면 overwrite.
    01_Spark Perf Pipeline 의 _save 컨벤션과 동일(재실행 시 중복 없음)."""
    full = f"{SCHEMA}.{TABLE_PREFIX}{table_name}"
    if "etl_run_id" not in df.columns:
        df = df.withColumn("etl_run_id", F.lit(RUN_ID))
    if "etl_run_ts" not in df.columns:
        df = df.withColumn("etl_run_ts", F.lit(RUN_TS))
    df = df.withColumn("etl_version", F.lit(ETL_VERSION))

    exists = spark.catalog.tableExists(full)
    if merge_keys and exists:
        df = df.dropDuplicates(merge_keys)
        cond = " AND ".join([f"t.`{k}` = s.`{k}`" for k in merge_keys])
        (DeltaTable.forName(spark, full).alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        tag = "  [MERGE]"
    else:
        if merge_keys:
            df = df.dropDuplicates(merge_keys)
        w = df.write.mode("overwrite").option("overwriteSchema", "true")
        if partition_by:
            w = w.partitionBy(*partition_by)
        w.saveAsTable(full)
        tag = "  [OVERWRITE]"
    n = spark.read.table(full).count()
    print(f"  ✓ {full}  ({n:,} rows){tag}")
    return full


# 출력 스키마 보장
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
_RUN_NOTES = []  # gold_run_log 용 진행 메모

# COMMAND ----------

# MAGIC %md ## Step 1 — system.query.history → silver_query_history

# COMMAND ----------

QH = "system.query.history"
assert table_readable(QH), (
    f"{QH} 를 읽을 수 없습니다. 시스템 스키마 'query' 활성화 + SELECT 권한을 확인하세요.\n"
    "활성화: 계정 콘솔/메타스토어 관리자가 system schema 'query' 를 enable 해야 합니다."
)

qh_raw = spark.read.table(QH)

# 시간 필터(푸시다운) + 컬럼 선택(런타임 스키마에 견고)
silver = (
    qh_raw
    .where(F.col("start_time") >= F.expr(f"current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS"))
    .select(
        col_or_null(qh_raw, "statement_id").alias("statement_id"),
        col_or_null(qh_raw, "compute.warehouse_id").alias("warehouse_id"),
        col_or_null(qh_raw, "compute.type").alias("compute_type"),
        col_or_null(qh_raw, "statement_type").alias("statement_type"),
        col_or_null(qh_raw, "execution_status").alias("execution_status"),
        col_or_null(qh_raw, "executed_by").alias("executed_by"),
        col_or_null(qh_raw, "client_application").alias("client_application"),
        col_or_null(qh_raw, "statement_text").alias("statement_text"),
        col_or_null(qh_raw, "start_time").alias("start_time"),
        col_or_null(qh_raw, "end_time").alias("end_time"),
        col_or_null(qh_raw, "total_duration_ms", "long").alias("total_duration_ms"),
        col_or_null(qh_raw, "execution_duration_ms", "long").alias("execution_duration_ms"),
        col_or_null(qh_raw, "compilation_duration_ms", "long").alias("compilation_duration_ms"),
        col_or_null(qh_raw, "waiting_for_compute_duration_ms", "long").alias("waiting_for_compute_ms"),
        col_or_null(qh_raw, "waiting_at_capacity_duration_ms", "long").alias("waiting_at_capacity_ms"),
        col_or_null(qh_raw, "total_task_duration_ms", "long").alias("total_task_duration_ms"),
        col_or_null(qh_raw, "result_fetch_duration_ms", "long").alias("result_fetch_ms"),
        col_or_null(qh_raw, "read_bytes", "long").alias("read_bytes"),
        col_or_null(qh_raw, "read_io_cache_percent", "double").alias("read_io_cache_percent"),
        col_or_null(qh_raw, "spilled_local_bytes", "long").alias("spilled_local_bytes"),
        col_or_null(qh_raw, "shuffle_read_bytes", "long").alias("shuffle_read_bytes"),
        col_or_null(qh_raw, "read_files", "long").alias("read_files"),
        col_or_null(qh_raw, "pruned_files", "long").alias("pruned_files"),
        col_or_null(qh_raw, "read_partitions", "long").alias("read_partitions"),
        col_or_null(qh_raw, "read_rows", "long").alias("read_rows"),
        col_or_null(qh_raw, "produced_rows", "long").alias("produced_rows"),
        col_or_null(qh_raw, "written_bytes", "long").alias("written_bytes"),
        col_or_null(qh_raw, "from_result_cache", "boolean").alias("from_result_cache"),
    )
    # SQL Warehouse 쿼리만 (warehouse_id 존재)
    .where(F.col("warehouse_id").isNotNull())
)

if WAREHOUSE_IDS:
    silver = silver.where(F.col("warehouse_id").isin(WAREHOUSE_IDS))

# 핑거프린트(정규화 쿼리 묶음) + 캐시 결과 플래그
silver = silver.withColumn(
    "fingerprint",
    F.when(F.col("statement_text").isNotNull(), fingerprint_expr(F.col("statement_text")))
     .otherwise(F.lit("unknown")),
).withColumn(
    "from_result_cache", F.coalesce(F.col("from_result_cache"), F.lit(False))
)

silver = silver.cache()
_total_rows = silver.count()
_finished = silver.where(F.col("execution_status") == "FINISHED")
print(f"분석 윈도우 행수: {_total_rows:,} (FINISHED: {_finished.count():,})")
_RUN_NOTES.append(f"query.history rows={_total_rows}")

_save(silver, "silver_query_history", merge_keys=["statement_id"])

# COMMAND ----------

# MAGIC %md ## Step 2 — gold_query_summary (핑거프린트 단위 집계 + 비용 귀속)

# COMMAND ----------

# 성능 통계는 완료된 쿼리만(실패/취소 제외). 캐시 결과는 실행시간≈0이라 통계 왜곡 없음.
fin = _finished

summary = (
    fin.groupBy("warehouse_id", "fingerprint")
    .agg(
        F.max("statement_text").alias("sample_statement_text"),
        F.first("statement_type", ignorenulls=True).alias("statement_type"),
        F.count("*").alias("run_count"),
        F.expr("percentile_approx(execution_duration_ms, 0.5)").alias("p50_exec_ms"),
        F.expr("percentile_approx(execution_duration_ms, 0.95)").alias("p95_exec_ms"),
        F.max("execution_duration_ms").alias("max_exec_ms"),
        F.avg("execution_duration_ms").alias("avg_exec_ms"),
        F.sum("execution_duration_ms").alias("total_exec_ms"),
        F.avg("compilation_duration_ms").alias("avg_compile_ms"),
        F.avg("waiting_at_capacity_ms").alias("avg_wait_capacity_ms"),
        F.avg("waiting_for_compute_ms").alias("avg_wait_compute_ms"),
        F.sum("read_bytes").alias("total_read_bytes"),
        F.avg("read_bytes").alias("avg_read_bytes"),
        F.avg("read_io_cache_percent").alias("avg_cache_percent"),
        F.sum("spilled_local_bytes").alias("total_spill_bytes"),
        F.avg("spilled_local_bytes").alias("avg_spill_bytes"),
        F.sum(F.when(F.col("spilled_local_bytes") > 0, 1).otherwise(0)).alias("queries_with_spill"),
        F.sum("read_files").alias("total_read_files"),
        F.sum("pruned_files").alias("total_pruned_files"),
        F.sum("read_rows").alias("total_read_rows"),
        F.sum("produced_rows").alias("total_produced_rows"),
        F.sum(F.when(F.col("from_result_cache"), 1).otherwise(0)).alias("result_cache_hits"),
        F.min("start_time").alias("first_seen"),
        F.max("start_time").alias("last_seen"),
    )
    .withColumn(
        "file_pruning_ratio",
        F.when((F.col("total_read_files") + F.col("total_pruned_files")) > 0,
               F.col("total_pruned_files") / (F.col("total_read_files") + F.col("total_pruned_files"))),
    )
    .withColumn(
        "compile_ratio",
        F.when(F.col("avg_exec_ms") + F.col("avg_compile_ms") > 0,
               F.col("avg_compile_ms") / (F.col("avg_exec_ms") + F.col("avg_compile_ms"))),
    )
    .withColumn(
        "row_amplification",
        F.when(F.col("total_read_rows") > 0, F.col("total_produced_rows") / F.col("total_read_rows")),
    )
    .withColumn("avg_read_gb", F.col("avg_read_bytes") / GB)
    .withColumn("total_read_gb", F.col("total_read_bytes") / GB)
    .withColumn("total_spill_gb", F.col("total_spill_bytes") / GB)
    .withColumn("avg_spill_gb", F.col("avg_spill_bytes") / GB)
)

# ── 비용 귀속(근사): 윈도우 내 웨어하우스 실비용을 실행시간 점유율로 분배 ──────────
#    per-query DBU 는 시스템 테이블에 없으므로, billing 실비용을 execution_duration 비중으로 분배한다.
wh_cost_window = None
if ENABLE_COST and table_readable("system.billing.usage") and table_readable("system.billing.list_prices"):
    try:
        usage = spark.read.table("system.billing.usage")
        prices = spark.read.table("system.billing.list_prices")
        u = (
            usage
            .where(F.col("usage_start_time") >= F.expr(f"current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS"))
            .where(col_or_null(usage, "usage_metadata.warehouse_id").isNotNull())
            .select(
                col_or_null(usage, "usage_metadata.warehouse_id").alias("warehouse_id"),
                F.col("sku_name"),
                F.to_date("usage_start_time").alias("usage_date"),
                F.col("usage_quantity").cast("double").alias("dbu"),
            )
        )
        p = prices.select(
            F.col("sku_name"),
            col_or_null(prices, "pricing.default", "double").alias("unit_price"),
            F.col("price_start_time"),
            F.coalesce(F.col("price_end_time"), F.current_timestamp()).alias("price_end_time"),
        )
        # 가격 유효구간 매칭(간이): sku 일치 + 가장 최근 유효가
        p_latest = (
            p.groupBy("sku_name").agg(F.max("price_start_time").alias("ps"))
            .join(p, ["sku_name"]).where(F.col("price_start_time") == F.col("ps"))
            .select("sku_name", "unit_price")
        )
        priced = u.join(p_latest, ["sku_name"], "left").withColumn(
            "usd", F.col("dbu") * F.coalesce(F.col("unit_price"), F.lit(0.0))
        )
        if WAREHOUSE_IDS:
            priced = priced.where(F.col("warehouse_id").isin(WAREHOUSE_IDS))
        priced = priced.cache()

        cost_daily = priced.groupBy("usage_date", "warehouse_id", "sku_name").agg(
            F.sum("dbu").alias("total_dbu"), F.sum("usd").alias("total_usd")
        )
        _save(cost_daily, "gold_warehouse_cost_daily",
              merge_keys=["usage_date", "warehouse_id", "sku_name"])

        wh_cost_window = priced.groupBy("warehouse_id").agg(
            F.sum("usd").alias("wh_window_usd"), F.sum("dbu").alias("wh_window_dbu")
        )
        _RUN_NOTES.append("cost: OK")
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠ 비용 계산 실패 — 건너뜀: {str(e).splitlines()[0][:160]}")
        _RUN_NOTES.append("cost: FAILED")
else:
    print("  · 비용 단계 비활성 또는 billing 미가용")
    _RUN_NOTES.append("cost: skipped")

# 웨어하우스 윈도우 실행시간 합 → 점유율 분배
wh_exec_window = fin.groupBy("warehouse_id").agg(F.sum("execution_duration_ms").alias("wh_window_exec_ms"))
summary = summary.join(wh_exec_window, ["warehouse_id"], "left")
if wh_cost_window is not None:
    summary = summary.join(wh_cost_window, ["warehouse_id"], "left").withColumn(
        "est_total_usd",
        F.when(F.col("wh_window_exec_ms") > 0,
               F.col("wh_window_usd") * F.col("total_exec_ms") / F.col("wh_window_exec_ms")),
    ).withColumn(
        "est_total_dbu",
        F.when(F.col("wh_window_exec_ms") > 0,
               F.col("wh_window_dbu") * F.col("total_exec_ms") / F.col("wh_window_exec_ms")),
    ).drop("wh_window_usd", "wh_window_dbu")
else:
    summary = summary.withColumn("est_total_usd", F.lit(None).cast("double")) \
                     .withColumn("est_total_dbu", F.lit(None).cast("double"))

summary = summary.drop("wh_window_exec_ms").cache()
_save(summary, "gold_query_summary", merge_keys=["warehouse_id", "fingerprint"])

# COMMAND ----------

# MAGIC %md ## Step 3 — gold_query_alerts (규칙 기반 진단, ActionCard 유사)

# COMMAND ----------

# 각 규칙: summary 를 필터 → 공통 알림 스키마로 select. severity 는 규모로 단계화.
# problem/recommendation 문구는 core/analyzers/recommendations_registry.py 의
# ActionCard(disk_spill / low_cache / low_file_pruning / compilation_overhead 등)를
# 쿼리 단위 집계 지표로 재구성한 것.

_ALERT_COLS = ["warehouse_id", "fingerprint", "alert_id", "category", "severity",
               "problem", "evidence", "recommendation", "priority",
               "run_count", "p95_exec_ms", "total_exec_ms", "est_total_usd",
               "sample_statement_text"]


def mk_alert(df_f, alert_id, category, severity_col, problem, recommendation, priority, evidence_col):
    return df_f.select(
        "warehouse_id", "fingerprint",
        F.lit(alert_id).alias("alert_id"),
        F.lit(category).alias("category"),
        severity_col.alias("severity"),
        F.lit(problem).alias("problem"),
        evidence_col.alias("evidence"),
        F.lit(recommendation).alias("recommendation"),
        F.lit(priority).alias("priority"),
        "run_count", "p95_exec_ms", "total_exec_ms", "est_total_usd",
        "sample_statement_text",
    )


_alerts = []

# 1) 디스크 스필 — 메모리 부족 신호 (속도/튜닝)
_f = summary.where(F.col("avg_spill_gb") >= TH["spill_warn_gb"])
_alerts.append(mk_alert(
    _f, "disk_spill", "memory",
    F.when(F.col("avg_spill_gb") >= TH["spill_crit_gb"], "HIGH").otherwise("MEDIUM"),
    "디스크 스필 발생 (메모리 부족)",
    "Warehouse 사이즈 상향 또는 스필 유발 조인/집계/정렬 최적화. 스필은 작업 메모리 초과 신호 — "
    "BROADCAST 힌트·필터 선행·중간 결과 축소를 검토.",
    100,
    F.concat(F.lit("평균 스필 "), F.round("avg_spill_gb", 2), F.lit("GB / 스필쿼리 "),
             F.col("queries_with_spill"), F.lit("회")),
))

# 2) 낮은 캐시 적중률 (속도)
_f = summary.where((F.col("avg_cache_percent") < TH["cache_low_pct"]) &
                   (F.col("avg_read_gb") >= TH["cache_min_read_gb"]))
_alerts.append(mk_alert(
    _f, "low_cache", "cache",
    F.when(F.col("avg_cache_percent") < TH["cache_low_pct"] / 2, "MEDIUM").otherwise("LOW"),
    "디스크/IO 캐시 적중률 낮음",
    "반복 쿼리는 디스크 캐시 워밍·결과 캐시 활용을 검토. 스캔 대상 축소(프루닝)도 캐시 효율을 높임.",
    75,
    F.concat(F.lit("평균 캐시 "), F.round("avg_cache_percent", 1), F.lit("% / 평균 read "),
             F.round("avg_read_gb", 1), F.lit("GB")),
))

# 3) 낮은 파일 프루닝 — 파일 레이아웃 최적화 (핵심)
_f = summary.where((F.col("file_pruning_ratio") < TH["pruning_low_ratio"]) &
                   ((F.col("total_read_files") + F.col("total_pruned_files")) >= TH["pruning_min_files"]))
_alerts.append(mk_alert(
    _f, "low_file_pruning", "file_layout",
    F.when(F.col("file_pruning_ratio") < TH["pruning_low_ratio"] / 2, "HIGH").otherwise("MEDIUM"),
    "파일 프루닝 비효율 (파일 레이아웃)",
    "필터 컬럼 기준 Liquid Clustering(권장) 또는 파티셔닝 재설계. 데이터 스키핑이 동작하도록 "
    "필터 컬럼 통계/레이아웃 정렬. gold_table_health 의 대상 테이블과 교차 확인.",
    90,
    F.concat(F.lit("프루닝율 "), F.round(F.col("file_pruning_ratio") * 100, 1),
             F.lit("% / 스캔파일 "), F.col("total_read_files")),
))

# 4) 컴파일 오버헤드 (튜닝)
_f = summary.where((F.col("compile_ratio") >= TH["compile_ratio_warn"]) &
                   (F.col("run_count") >= TH["compile_min_runs"]))
_alerts.append(mk_alert(
    _f, "compilation_overhead", "compilation",
    F.when(F.col("compile_ratio") >= 0.5, "MEDIUM").otherwise("LOW"),
    "컴파일 오버헤드 비중 높음",
    "쿼리 복잡도/뷰 중첩 축소, 파라미터화로 플랜 캐시 재사용. 잦은 DDL/스키마 변경이 컴파일을 유발하는지 점검.",
    60,
    F.concat(F.lit("컴파일 비중 "), F.round(F.col("compile_ratio") * 100, 1),
             F.lit("% / 실행 "), F.col("run_count"), F.lit("회")),
))

# 5) 큐/용량 대기 — 환경(동시성) 요인
_f = summary.where((F.coalesce(F.col("avg_wait_capacity_ms"), F.lit(0)) +
                    F.coalesce(F.col("avg_wait_compute_ms"), F.lit(0))) >= TH["queue_warn_ms"])
_alerts.append(mk_alert(
    _f, "queue_pressure", "concurrency",
    F.when((F.coalesce(F.col("avg_wait_capacity_ms"), F.lit(0)) +
            F.coalesce(F.col("avg_wait_compute_ms"), F.lit(0))) >= TH["queue_warn_ms"] * 4,
           "MEDIUM").otherwise("LOW"),
    "큐/용량 대기 (환경 요인)",
    "Warehouse 최대 클러스터 수 상향(오토스케일)·동시성 분산. 쿼리 자체가 아닌 환경 병목 — "
    "gold_warehouse_events 의 스케일/큐 추이와 교차 확인.",
    50,
    F.concat(F.lit("평균 대기 capacity="), F.round(F.coalesce(F.col("avg_wait_capacity_ms"), F.lit(0)), 0),
             F.lit("ms compute="), F.round(F.coalesce(F.col("avg_wait_compute_ms"), F.lit(0)), 0), F.lit("ms")),
))

# 6) 행 증폭 (튜닝)
_f = summary.where(F.col("row_amplification") >= TH["row_amp_warn"])
_alerts.append(mk_alert(
    _f, "row_explosion", "sql_pattern",
    F.when(F.col("row_amplification") >= TH["row_amp_warn"] * 5, "MEDIUM").otherwise("LOW"),
    "행 증폭 (조인/집계 의심)",
    "조인 조건·중복 키 점검, GROUP BY 누락 확인. produced/read 비가 큼 — 카티전/팬아웃 조인 가능성.",
    55,
    F.concat(F.lit("행 증폭 "), F.round("row_amplification", 1), F.lit("x")),
))

# 7) 느리고 빈번 — 집계 영향 큰 우선순위 (속도)
_f = summary.where((F.col("p95_exec_ms") >= TH["slow_p95_ms"]) & (F.col("run_count") >= TH["frequent_runs"]))
_alerts.append(mk_alert(
    _f, "frequent_slow", "priority",
    F.when(F.col("p95_exec_ms") >= TH["slow_p95_ms"] * 5, "HIGH").otherwise("MEDIUM"),
    "느리고 빈번한 쿼리 (높은 누적 영향)",
    "최우선 튜닝 대상. 스캔 축소(프루닝/클러스터링)·캐시·결과 재사용을 종합 적용. "
    "동일 핑거프린트의 다른 알림(스필/프루닝/컴파일)을 함께 해소.",
    95,
    F.concat(F.lit("p95 "), F.round(F.col("p95_exec_ms") / 1000.0, 1), F.lit("s × "),
             F.col("run_count"), F.lit("회")),
))

alerts = _alerts[0]
for a in _alerts[1:]:
    alerts = alerts.unionByName(a)

# alert_id 는 핑거프린트당 규칙당 1개 → (wh, fingerprint, alert_id) 유니크
_save(alerts, "gold_query_alerts", merge_keys=["warehouse_id", "fingerprint", "alert_id"])

_alert_counts = alerts.groupBy("alert_id", "severity").count().orderBy(F.desc("count"))
print("규칙별 알림 수:")
for r in _alert_counts.collect():
    print(f"  {r['alert_id']:<22} {r['severity']:<7} {r['count']:,}")
_RUN_NOTES.append(f"alerts={alerts.count()}")

# COMMAND ----------

# MAGIC %md ## Step 4 — gold_query_regression (최근 vs 과거 악화)

# COMMAND ----------

_cut = F.expr(f"current_timestamp() - INTERVAL {RECENT_DAYS} DAYS")
_recent = fin.where(F.col("start_time") >= _cut)
_base = fin.where(F.col("start_time") < _cut)

_rg = _recent.groupBy("warehouse_id", "fingerprint").agg(
    F.expr("percentile_approx(execution_duration_ms, 0.5)").alias("recent_p50_ms"),
    F.count("*").alias("recent_runs"),
)
_bg = _base.groupBy("warehouse_id", "fingerprint").agg(
    F.expr("percentile_approx(execution_duration_ms, 0.5)").alias("baseline_p50_ms"),
    F.count("*").alias("baseline_runs"),
)
regression = (
    _rg.join(_bg, ["warehouse_id", "fingerprint"], "inner")
    .withColumn("regression_ratio",
                F.when(F.col("baseline_p50_ms") > 0, F.col("recent_p50_ms") / F.col("baseline_p50_ms")))
    .withColumn(
        "is_regression",
        (F.col("regression_ratio") >= TH["regression_ratio"]) &
        (F.col("recent_p50_ms") >= TH["regression_min_ms"]) &
        (F.col("recent_runs") >= TH["regression_min_runs"]) &
        (F.col("baseline_runs") >= TH["regression_min_runs"]),
    )
    .join(summary.select("warehouse_id", "fingerprint", "sample_statement_text", "statement_type"),
          ["warehouse_id", "fingerprint"], "left")
)
_save(regression, "gold_query_regression", merge_keys=["warehouse_id", "fingerprint"])
_n_reg = regression.where(F.col("is_regression")).count()
print(f"회귀(악화) 탐지: {_n_reg:,} 핑거프린트")
_RUN_NOTES.append(f"regressions={_n_reg}")

# COMMAND ----------

# MAGIC %md ## Step 5 — gold_warehouse_events (시간대별 스케일/동시성)

# COMMAND ----------

if ENABLE_WHEV and table_readable("system.compute.warehouse_events"):
    try:
        we = spark.read.table("system.compute.warehouse_events")
        we = we.where(F.col("event_time") >= F.expr(f"current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS"))
        if WAREHOUSE_IDS:
            we = we.where(F.col("warehouse_id").isin(WAREHOUSE_IDS))
        we_hourly = (
            we.withColumn("event_hour", F.date_trunc("hour", F.col("event_time")))
            .groupBy("warehouse_id", "event_hour", "event_type")
            .agg(
                F.count("*").alias("event_count"),
                F.max(col_or_null(we, "cluster_count", "int")).alias("max_cluster_count"),
                F.avg(col_or_null(we, "cluster_count", "int")).alias("avg_cluster_count"),
            )
        )
        _save(we_hourly, "gold_warehouse_events",
              merge_keys=["warehouse_id", "event_hour", "event_type"])
        _RUN_NOTES.append("warehouse_events: OK")
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠ warehouse_events 실패 — 건너뜀: {str(e).splitlines()[0][:160]}")
        _RUN_NOTES.append("warehouse_events: FAILED")
else:
    print("  · warehouse_events 단계 비활성 또는 미가용")
    _RUN_NOTES.append("warehouse_events: skipped")

# COMMAND ----------

# MAGIC %md ## Step 6 — gold_table_health (파일 레이아웃: DESCRIBE DETAIL/HISTORY)

# COMMAND ----------

# 상위 쿼리(읽기 규모/스필 기준)가 건드린 테이블을 table_lineage 로 식별 →
# DESCRIBE DETAIL/HISTORY 로 파일 수·평균 파일 크기·마지막 OPTIMIZE 점검.
def run_table_health():
    LIN = "system.access.table_lineage"
    if not table_readable(LIN):
        print("  · table_lineage 미가용 — 테이블 헬스 건너뜀")
        _RUN_NOTES.append("table_health: no_lineage")
        return

    # 점검 우선순위: 읽기 규모가 큰 상위 쿼리의 statement_id
    top_stmts = (
        fin.select("statement_id", "read_bytes", "spilled_local_bytes")
        .orderBy(F.desc(F.coalesce(F.col("read_bytes"), F.lit(0))))
        .limit(2000)
        .select("statement_id")
    )
    lin = spark.read.table(LIN)
    lin = lin.where(F.col("event_time") >= F.expr(f"current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS"))
    src = (
        lin.select(
            col_or_null(lin, "source_table_full_name").alias("table_full_name"),
            col_or_null(lin, "statement_id").alias("statement_id"),
        )
        .where(F.col("table_full_name").isNotNull())
        .join(top_stmts, ["statement_id"], "inner")
        .groupBy("table_full_name").agg(F.count("*").alias("ref_count"))
        .orderBy(F.desc("ref_count"))
        .limit(TOP_N_TABLES)
    )
    tables = [r["table_full_name"] for r in src.collect()]
    print(f"  점검 대상 테이블: {len(tables)}개")
    if not tables:
        _RUN_NOTES.append("table_health: 0_tables")
        return

    rows = []
    for t in tables:
        try:
            d = spark.sql(f"DESCRIBE DETAIL {t}").collect()[0].asDict()
            num_files = d.get("numFiles")
            size_bytes = d.get("sizeInBytes")
            avg_mb = (size_bytes / num_files / MB) if (num_files and size_bytes) else None
            part_cols = d.get("partitionColumns") or []
            clust_cols = d.get("clusteringColumns") or []
            last_opt = None
            try:
                h = (spark.sql(f"DESCRIBE HISTORY {t}")
                     .where(F.col("operation").isin("OPTIMIZE", "CLUSTERING"))
                     .agg(F.max("timestamp").alias("last_opt")).collect())
                last_opt = h[0]["last_opt"] if h else None
            except Exception:  # noqa: BLE001
                pass
            small_files = bool(avg_mb is not None and avg_mb < TH["small_file_mb"]
                               and (num_files or 0) >= TH["small_file_min_n"])
            recs = []
            if small_files:
                recs.append(f"small files (평균 {avg_mb:.1f}MB, {num_files}개) → OPTIMIZE/Liquid Clustering")
            if not clust_cols and not part_cols:
                recs.append("클러스터링/파티셔닝 미설정 → 필터 컬럼 기준 CLUSTER BY 검토")
            rows.append({
                "table_full_name": t,
                "format": d.get("format"),
                "num_files": int(num_files) if num_files is not None else None,
                "size_bytes": int(size_bytes) if size_bytes is not None else None,
                "avg_file_mb": round(avg_mb, 2) if avg_mb is not None else None,
                "partition_columns": ",".join(part_cols),
                "clustering_columns": ",".join(clust_cols),
                "last_optimize_ts": last_opt,
                "is_small_files": small_files,
                "recommendation": " | ".join(recs) if recs else "OK",
            })
        except Exception as e:  # noqa: BLE001
            print(f"    ⚠ {t}: {str(e).splitlines()[0][:100]}")

    if not rows:
        _RUN_NOTES.append("table_health: all_failed")
        return
    from pyspark.sql.types import (StructType, StructField, StringType,
                                   LongType, DoubleType, BooleanType, TimestampType)
    schema = StructType([
        StructField("table_full_name", StringType()),
        StructField("format", StringType()),
        StructField("num_files", LongType()),
        StructField("size_bytes", LongType()),
        StructField("avg_file_mb", DoubleType()),
        StructField("partition_columns", StringType()),
        StructField("clustering_columns", StringType()),
        StructField("last_optimize_ts", TimestampType()),
        StructField("is_small_files", BooleanType()),
        StructField("recommendation", StringType()),
    ])
    th_df = spark.createDataFrame(rows, schema=schema).withColumn(
        "days_since_optimize",
        F.when(F.col("last_optimize_ts").isNotNull(),
               F.datediff(F.current_timestamp(), F.col("last_optimize_ts"))),
    )
    _save(th_df, "gold_table_health", merge_keys=["table_full_name"])
    _RUN_NOTES.append(f"table_health={len(rows)}")


if ENABLE_HEALTH:
    run_table_health()
else:
    print("  · 테이블 헬스 단계 비활성")
    _RUN_NOTES.append("table_health: skipped")

# COMMAND ----------

# MAGIC %md ## Step 7 — gold_run_log (실행 메타데이터)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

_log_schema = StructType([
    StructField("etl_run_id", StringType()),
    StructField("etl_run_ts", TimestampType()),
    StructField("etl_version", StringType()),
    StructField("catalog", StringType()),
    StructField("schema", StringType()),
    StructField("lookback_days", IntegerType()),
    StructField("recent_days", IntegerType()),
    StructField("warehouse_filter", StringType()),
    StructField("notes", StringType()),
])
_log_row = [{
    "etl_run_id": RUN_ID,
    "etl_run_ts": RUN_TS,
    "etl_version": ETL_VERSION,
    "catalog": CATALOG,
    "schema": _SCHEMA,
    "lookback_days": LOOKBACK_DAYS,
    "recent_days": RECENT_DAYS,
    "warehouse_filter": ",".join(WAREHOUSE_IDS) if WAREHOUSE_IDS else "ALL",
    "notes": " | ".join(_RUN_NOTES),
}]
_save(spark.createDataFrame(_log_row, schema=_log_schema), "gold_run_log", merge_keys=["etl_run_id"])

print("\n" + "=" * 60)
print(f"완료: {RUN_ID}")
print(" | ".join(_RUN_NOTES))
print(f"다음 단계: 02_query_history_dashboard 를 실행해 대시보드를 생성/갱신하세요.")
print("=" * 60)
