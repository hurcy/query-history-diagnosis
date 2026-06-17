# Databricks notebook source
# MAGIC %md
# MAGIC # Query History Analysis — Lakeview 대시보드 생성/갱신
# MAGIC
# MAGIC `01_query_history_analysis` 가 만든 Gold 테이블 위에 **규칙 기반(LLM 미사용)** 대시보드를 생성/공개합니다.
# MAGIC
# MAGIC **手順(절차):**
# MAGIC 1. 아래 CONFIGURATION 위젯을 환경에 맞게 변경
# MAGIC 2. 「Run All」 로 전체 실행
# MAGIC
# MAGIC **갱신 시:** `existing_dashboard_id` 는 보통 비워두면 됩니다. 동명 대시보드를 자동 검색해 PATCH 로
# MAGIC 덮어쓰므로 URL 이 바뀌지 않습니다. (notebook 03 과 동일 패턴)

# COMMAND ----------

dbutils.widgets.text("catalog",      "main",                          "Catalog")
dbutils.widgets.text("schema",       "default",                       "Schema")
dbutils.widgets.text("table_prefix", "QH_",                           "Table Name Prefix (04 와 동일하게)")
dbutils.widgets.text("warehouse_id", "",                              "SQL Warehouse ID (대시보드 쿼리 실행용)")
dbutils.widgets.text("parent_path",  "/Shared/query-history-analysis", "Dashboard Parent Path")
dbutils.widgets.text("dash_name",    "Query History Analysis",        "Dashboard Display Name")
dbutils.widgets.text("existing_dashboard_id", "",                     "Existing Dashboard ID (optional)")

CATALOG       = dbutils.widgets.get("catalog").strip()
_SCHEMA       = dbutils.widgets.get("schema").strip()
SCHEMA        = f"{CATALOG}.{_SCHEMA}"
TABLE_PREFIX  = dbutils.widgets.get("table_prefix").strip()
WAREHOUSE_ID  = dbutils.widgets.get("warehouse_id").strip()
PARENT_PATH   = dbutils.widgets.get("parent_path").strip()
DASH_NAME     = dbutils.widgets.get("dash_name").strip()
_existing_id  = dbutils.widgets.get("existing_dashboard_id").strip()
EXISTING_DASHBOARD_ID = _existing_id if _existing_id else None

assert WAREHOUSE_ID, "warehouse_id 는 필수입니다 (대시보드 데이터셋이 SQL Warehouse 에서 실행됨)"


def tbl(name: str) -> str:
    return f"{SCHEMA}.{TABLE_PREFIX}{name}"


def tbl_exists(name: str) -> bool:
    try:
        return spark.catalog.tableExists(tbl(name))
    except Exception:  # noqa: BLE001
        return False


assert tbl_exists("gold_query_summary"), (
    f"{tbl('gold_query_summary')} 가 없습니다. 먼저 01_query_history_analysis 를 실행하세요."
)
print(f"소스 테이블: {SCHEMA}.{TABLE_PREFIX}*")
print(f"대시보드   : {DASH_NAME}  @ {PARENT_PATH}")

# COMMAND ----------

# ── LakeviewDashboard 클래스 + 위젯 헬퍼 (notebook 03 에서 그대로 재사용, 외부 의존 없음) ──
import json, uuid, requests
from typing import Optional, List, Dict, Any


class LakeviewDashboard:
    def __init__(self, name: str = "New Dashboard"):
        self.name = name
        self.datasets: List[Dict] = []
        self.pages: List[Dict] = []
        self._current_page: Optional[Dict] = None
        self.add_page("Overview")

    @staticmethod
    def _generate_id() -> str:
        return uuid.uuid4().hex[:8]

    def add_dataset(self, name, display_name, query):
        self.datasets.append({"name": name, "displayName": display_name, "queryLines": [query]})
        return name

    def add_page(self, display_name):
        page_id = self._generate_id()
        page = {"name": page_id, "displayName": display_name, "pageType": "PAGE_TYPE_CANVAS", "layout": []}
        self.pages.append(page)
        self._current_page = page
        return page_id

    def to_dict(self):
        return {"datasets": self.datasets, "pages": self.pages,
                "uiSettings": {"theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"}, "applyModeEnabled": False}}

    def to_json(self):
        return json.dumps(self.to_dict())

    def get_api_payload(self, warehouse_id, parent_path):
        return {"display_name": self.name, "warehouse_id": warehouse_id,
                "parent_path": parent_path, "serialized_dashboard": self.to_json()}


def uid():
    return uuid.uuid4().hex[:8]


def add_widget(db, page_idx, widget, x, y, w, h):
    db.pages[page_idx]["layout"].append({"widget": widget, "position": {"x": x, "y": y, "width": w, "height": h}})


def counter(ds_name, value_field, title, agg="SUM"):
    if agg == "COUNT":
        vname, vexpr = "count(*)", "COUNT(`*`)"
    else:
        vname, vexpr = f"{agg.lower()}({value_field})", f"{agg}(`{value_field}`)"
    return {
        "name": uid(),
        "queries": [{"name": "main_query", "query": {
            "datasetName": ds_name,
            "fields": [{"name": vname, "expression": vexpr}],
            "disaggregated": True}}],
        "spec": {"version": 2, "widgetType": "counter",
                 "encodings": {"value": {"fieldName": vname, "displayName": title}},
                 "frame": {"showTitle": True, "title": title}},
    }


def agg_bar(ds_name, x_f, y_f, y_agg, title, color_f=None, sort_x=None):
    y_name = f"{y_agg.lower()}({y_f})"
    x_scale = {"type": "categorical"}
    if sort_x:
        x_scale["sort"] = {"by": sort_x}
    enc = {"x": {"fieldName": x_f, "scale": x_scale, "displayName": x_f},
           "y": {"fieldName": y_name, "scale": {"type": "quantitative"}, "displayName": y_f},
           "label": {"show": True}}
    fields = [{"name": x_f, "expression": f"`{x_f}`"}, {"name": y_name, "expression": f"{y_agg}(`{y_f}`)"}]
    if color_f:
        enc["color"] = {"fieldName": color_f, "scale": {"type": "categorical"}, "displayName": color_f}
        fields.append({"name": color_f, "expression": f"`{color_f}`"})
    return {"name": uid(),
            "queries": [{"name": "main_query", "query": {"datasetName": ds_name, "fields": fields, "disaggregated": False}}],
            "spec": {"version": 3, "widgetType": "bar", "encodings": enc, "frame": {"showTitle": True, "title": title}}}


def scatter(ds_name, x_f, x_label, y_f, y_label, color_f, title):
    return {"name": uid(),
            "queries": [{"name": "main_query", "query": {"datasetName": ds_name, "fields": [
                {"name": x_f, "expression": f"`{x_f}`"},
                {"name": y_f, "expression": f"`{y_f}`"},
                {"name": color_f, "expression": f"`{color_f}`"}], "disaggregated": True}}],
            "spec": {"version": 3, "widgetType": "scatter",
                     "encodings": {"x": {"fieldName": x_f, "scale": {"type": "quantitative"}, "displayName": x_label},
                                   "y": {"fieldName": y_f, "scale": {"type": "quantitative"}, "displayName": y_label},
                                   "color": {"fieldName": color_f, "scale": {"type": "categorical"}, "displayName": color_f}},
                     "frame": {"showTitle": True, "title": title}}}


def table(ds_name, field_names, title, max_rows=1000):
    return {"name": uid(),
            "queries": [{"name": "main_query", "query": {"datasetName": ds_name,
                "fields": [{"name": f, "expression": f"`{f}`"} for f in field_names], "disaggregated": True}}],
            "spec": {"version": 2, "widgetType": "table",
                     "encodings": {"columns": [{"fieldName": f, "displayName": f} for f in field_names]},
                     "frame": {"showTitle": True, "title": title}},
            "overrides": {"queries": [{"query": {"limit": max_rows}}]}}


def text(md):
    return {"name": uid(), "textbox_spec": md}

# COMMAND ----------

# ── 규칙 기반 요약 텍스트(LLM 미사용): 상위 알림을 우선순위로 정렬해 마크다운 생성 ──
def build_summary_md() -> str:
    lines = [f"# Query History Analysis", "", f"소스: `{SCHEMA}.{TABLE_PREFIX}*` · 규칙 기반 진단(LLM 미사용)", ""]
    try:
        run = spark.sql(f"SELECT * FROM {tbl('gold_run_log')} ORDER BY etl_run_ts DESC LIMIT 1").collect()
        if run:
            r = run[0]
            lines.append(f"**최근 실행:** {r['etl_run_id']} · 기간 {r['lookback_days']}일 · WH `{r['warehouse_filter']}`")
            lines.append("")
    except Exception:  # noqa: BLE001
        pass

    lines.append("## 상위 진단 (severity · 누적영향 순)")
    lines.append("")
    try:
        top = spark.sql(f"""
            SELECT alert_id, severity, problem, recommendation, evidence,
                   run_count, ROUND(total_exec_ms/1000.0, 0) AS total_exec_s, ROUND(est_total_usd, 2) AS usd
            FROM {tbl('gold_query_alerts')}
            ORDER BY CASE severity WHEN 'HIGH' THEN 3 WHEN 'MEDIUM' THEN 2 ELSE 1 END DESC,
                     priority DESC, total_exec_ms DESC
            LIMIT 12
        """).collect()
        if not top:
            lines.append("_탐지된 알림이 없습니다._")
        for i, a in enumerate(top, 1):
            usd = f" · ~${a['usd']}" if a["usd"] is not None else ""
            lines.append(f"**{i}. [{a['severity']}] {a['problem']}** ({a['alert_id']})")
            lines.append(f"- 증상: {a['evidence']} · 실행 {a['run_count']}회 · 누적 {a['total_exec_s']}s{usd}")
            lines.append(f"- 추천: {a['recommendation']}")
            lines.append("")
    except Exception as e:  # noqa: BLE001
        lines.append(f"_요약 생성 실패: {e}_")
    return "\n".join(lines)


SUMMARY_MD = build_summary_md()
print(SUMMARY_MD[:1500])

# COMMAND ----------

# ── 대시보드 & 데이터셋 ───────────────────────────────────────────────────────
db = LakeviewDashboard(DASH_NAME)

# KPI (단일행 다중컬럼)
_cost_kpi = (f"(SELECT ROUND(SUM(est_total_usd),2) FROM {tbl('gold_query_summary')}) AS est_total_usd,"
             if tbl_exists("gold_query_summary") else "CAST(NULL AS DOUBLE) AS est_total_usd,")
db.add_dataset("kpi_ds", "KPIs", f"""
    SELECT
      (SELECT COUNT(*) FROM {tbl('silver_query_history')}) AS total_queries,
      (SELECT COUNT(*) FROM {tbl('gold_query_summary')}) AS distinct_queries,
      (SELECT COUNT(*) FROM {tbl('gold_query_alerts')} WHERE severity='HIGH') AS high_alerts,
      (SELECT COUNT(*) FROM {tbl('gold_query_alerts')}) AS total_alerts,
      (SELECT ROUND(SUM(total_spill_gb),1) FROM {tbl('gold_query_summary')}) AS total_spill_gb,
      {_cost_kpi.rstrip(',')}
""")

db.add_dataset("slow_ds", "Slow / Expensive Queries", f"""
    SELECT warehouse_id, fingerprint,
           ROUND(p95_exec_ms/1000.0,1) AS p95_s, ROUND(avg_exec_ms/1000.0,1) AS avg_s,
           run_count, ROUND(total_exec_ms/1000.0,0) AS total_exec_s,
           ROUND(total_read_gb,1) AS read_gb, ROUND(total_spill_gb,2) AS spill_gb,
           ROUND(file_pruning_ratio*100,1) AS pruning_pct, ROUND(avg_cache_percent,1) AS cache_pct,
           ROUND(est_total_usd,2) AS est_usd, statement_type,
           SUBSTRING(sample_statement_text,1,200) AS sample_sql
    FROM {tbl('gold_query_summary')}
    ORDER BY total_exec_ms DESC NULLS LAST
""")

db.add_dataset("alerts_ds", "Alerts", f"""
    SELECT severity, category, alert_id, problem, evidence, recommendation,
           run_count, ROUND(p95_exec_ms/1000.0,1) AS p95_s, ROUND(total_exec_ms/1000.0,0) AS total_exec_s,
           ROUND(est_total_usd,2) AS est_usd, warehouse_id, fingerprint
    FROM {tbl('gold_query_alerts')}
    ORDER BY CASE severity WHEN 'HIGH' THEN 3 WHEN 'MEDIUM' THEN 2 ELSE 1 END DESC, priority DESC, total_exec_ms DESC
""")

db.add_dataset("alertcat_ds", "Alerts by category", f"""
    SELECT category, severity, COUNT(*) AS cnt FROM {tbl('gold_query_alerts')}
    GROUP BY category, severity
""")

db.add_dataset("reg_ds", "Regressions", f"""
    SELECT warehouse_id, fingerprint, ROUND(recent_p50_ms/1000.0,1) AS recent_p50_s,
           ROUND(baseline_p50_ms/1000.0,1) AS baseline_p50_s, ROUND(regression_ratio,2) AS ratio,
           recent_runs, baseline_runs, statement_type, SUBSTRING(sample_statement_text,1,200) AS sample_sql
    FROM {tbl('gold_query_regression')}
    WHERE is_regression = true
    ORDER BY regression_ratio DESC
""") if tbl_exists("gold_query_regression") else None

if tbl_exists("gold_warehouse_cost_daily"):
    db.add_dataset("cost_ds", "Daily Cost", f"""
        SELECT usage_date, warehouse_id, ROUND(SUM(total_usd),2) AS usd, ROUND(SUM(total_dbu),1) AS dbu
        FROM {tbl('gold_warehouse_cost_daily')} GROUP BY usage_date, warehouse_id ORDER BY usage_date
    """)

if tbl_exists("gold_warehouse_events"):
    db.add_dataset("whev_ds", "Warehouse Events", f"""
        SELECT event_hour, warehouse_id, event_type, SUM(event_count) AS events, MAX(max_cluster_count) AS max_clusters
        FROM {tbl('gold_warehouse_events')} GROUP BY event_hour, warehouse_id, event_type ORDER BY event_hour
    """)

if tbl_exists("gold_table_health"):
    db.add_dataset("health_ds", "Table Health", f"""
        SELECT table_full_name, num_files, ROUND(size_bytes/1024.0/1024.0/1024.0,2) AS size_gb,
               avg_file_mb, partition_columns, clustering_columns, days_since_optimize,
               is_small_files, recommendation
        FROM {tbl('gold_table_health')}
        ORDER BY is_small_files DESC, num_files DESC NULLS LAST
    """)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1: 개요 (KPI + 규칙 기반 요약)
# ══════════════════════════════════════════════════════════════════════════════
db.pages[0]["displayName"] = "개요"
add_widget(db, 0, counter("kpi_ds", "total_queries",    "총 쿼리(윈도우)",     "SUM"), 0, 0, 1, 2)
add_widget(db, 0, counter("kpi_ds", "distinct_queries", "고유 쿼리(핑거프린트)", "SUM"), 1, 0, 1, 2)
add_widget(db, 0, counter("kpi_ds", "high_alerts",      "HIGH 알림",          "SUM"), 2, 0, 1, 2)
add_widget(db, 0, counter("kpi_ds", "total_alerts",     "전체 알림",          "SUM"), 3, 0, 1, 2)
add_widget(db, 0, counter("kpi_ds", "total_spill_gb",   "총 스필(GB)",        "SUM"), 4, 0, 1, 2)
add_widget(db, 0, counter("kpi_ds", "est_total_usd",    "추정 비용($)",       "SUM"), 5, 0, 1, 2)
add_widget(db, 0, text(SUMMARY_MD), 0, 2, 6, 22)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2: 느린/비싼 쿼리
# ══════════════════════════════════════════════════════════════════════════════
db.add_page("느린·비싼 쿼리")
add_widget(db, 1, scatter("slow_ds", "run_count", "실행 횟수", "p95_s", "p95(초)", "statement_type",
                          "실행 빈도 × p95 (우상단 = 최우선)"), 0, 0, 6, 8)
add_widget(db, 1, table("slow_ds",
    ["warehouse_id", "p95_s", "avg_s", "run_count", "total_exec_s", "read_gb", "spill_gb",
     "pruning_pct", "cache_pct", "est_usd", "statement_type", "sample_sql"],
    "쿼리 요약 (누적 실행시간 순)"), 0, 8, 6, 14)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3: 진단(알림)
# ══════════════════════════════════════════════════════════════════════════════
db.add_page("진단(알림)")
add_widget(db, 2, agg_bar("alertcat_ds", "category", "cnt", "SUM", "카테고리별 알림 수", color_f="severity"),
           0, 0, 6, 8)
add_widget(db, 2, table("alerts_ds",
    ["severity", "category", "alert_id", "problem", "evidence", "recommendation",
     "run_count", "p95_s", "total_exec_s", "est_usd", "warehouse_id"],
    "알림 상세 (severity·우선순위 순)"), 0, 8, 6, 14)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4: 비용 · 동시성
# ══════════════════════════════════════════════════════════════════════════════
_p = db.add_page("비용·동시성")
_pi = len(db.pages) - 1
_y = 0
if tbl_exists("gold_warehouse_cost_daily"):
    add_widget(db, _pi, agg_bar("cost_ds", "usage_date", "usd", "SUM", "일별 비용($)", color_f="warehouse_id"),
               0, _y, 6, 8); _y += 8
if tbl_exists("gold_warehouse_events"):
    add_widget(db, _pi, agg_bar("whev_ds", "event_hour", "max_clusters", "MAX", "시간대별 최대 클러스터 수",
                                color_f="warehouse_id"), 0, _y, 6, 8); _y += 8
if _y == 0:
    add_widget(db, _pi, text("_billing / warehouse_events 데이터셋이 없습니다 (04 에서 비활성 또는 미가용)._"),
               0, 0, 6, 4)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5: 회귀(악화)
# ══════════════════════════════════════════════════════════════════════════════
if tbl_exists("gold_query_regression"):
    db.add_page("회귀(악화)")
    _ri = len(db.pages) - 1
    add_widget(db, _ri, table("reg_ds",
        ["warehouse_id", "recent_p50_s", "baseline_p50_s", "ratio", "recent_runs", "baseline_runs",
         "statement_type", "sample_sql"],
        "최근 악화 쿼리 (recent p50 / baseline p50)"), 0, 0, 6, 20)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 6: 테이블 헬스 (파일 레이아웃)
# ══════════════════════════════════════════════════════════════════════════════
if tbl_exists("gold_table_health"):
    db.add_page("테이블 헬스")
    _hi = len(db.pages) - 1
    add_widget(db, _hi, table("health_ds",
        ["table_full_name", "num_files", "size_gb", "avg_file_mb", "partition_columns",
         "clustering_columns", "days_since_optimize", "is_small_files", "recommendation"],
        "테이블 파일 레이아웃 (small files 우선)"), 0, 0, 6, 20)

print(f"페이지 {len(db.pages)}개, 데이터셋 {len(db.datasets)}개 구성 완료")

# COMMAND ----------

# MAGIC %md ## 작성 & 공개 (notebook 03 과 동일 패턴: 동명 검색 → PATCH, 없으면 생성)

# COMMAND ----------

ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
token = ctx.apiToken().get()
host  = ctx.apiUrl().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

target_id = EXISTING_DASHBOARD_ID
if not target_id:
    dashboard_path = f"{PARENT_PATH}/{DASH_NAME}.lvdash.json"
    r = requests.get(f"{host}/api/2.0/workspace/get-status", headers=headers, params={"path": dashboard_path})
    if r.status_code == 200 and r.json().get("object_type") == "DASHBOARD":
        target_id = r.json().get("resource_id")
        print(f"기존 대시보드 발견: {target_id}")

# parent_path 폴더 보장
_pp = requests.get(f"{host}/api/2.0/workspace/get-status", headers=headers, params={"path": PARENT_PATH})
if _pp.status_code != 200:
    _mk = requests.post(f"{host}/api/2.0/workspace/mkdirs", headers=headers, json={"path": PARENT_PATH})
    assert _mk.status_code == 200, f"MKDIRS ERROR: {_mk.status_code} {_mk.text}"
    print(f"폴더 생성: {PARENT_PATH}")

payload = db.get_api_payload(WAREHOUSE_ID, PARENT_PATH)
payload["display_name"] = DASH_NAME

if target_id:
    r = requests.patch(f"{host}/api/2.0/lakeview/dashboards/{target_id}", headers=headers, json=payload)
    assert r.status_code == 200, f"UPDATE ERROR: {r.status_code} {r.text}"
    dashboard_id = target_id
    print(f"갱신: {dashboard_id}")
else:
    r = requests.post(f"{host}/api/2.0/lakeview/dashboards", headers=headers, json=payload)
    assert r.status_code == 200, f"CREATE ERROR: {r.status_code} {r.text}"
    dashboard_id = r.json()["dashboard_id"]
    print(f"생성: {dashboard_id}")

r = requests.post(f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}/published",
                  headers=headers, json={"warehouse_id": WAREHOUSE_ID})
assert r.status_code == 200, f"PUBLISH ERROR: {r.status_code} {r.text}"
print("공개 완료!")
print(f"URL: {host.rstrip('/')}/dashboardsv3/{dashboard_id}/published")
