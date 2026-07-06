import ast
import io
import os
import re
import time
import warnings
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import quote_plus

import boto3
import s3fs
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pydantic import SecretStr
from sqlalchemy import create_engine, text
from sqlalchemy import exc as sa_exc
from sqlalchemy.dialects.postgresql.psycopg2 import dialect as pg_dialect
from sqlalchemy.sql.sqltypes import CHAR

# ──────────────────────────────────────────────────────────────
# SUPPRESS NOISE
# ──────────────────────────────────────────────────────────────
warnings.filterwarnings("ignore", category=sa_exc.SAWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

# ──────────────────────────────────────────────────────────────
# PAGE CONFIG — must be first Streamlit call
# ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Telemetry Data", layout="wide")

# ──────────────────────────────────────────────────────────────
# ░░░  CREDENTIALS — hardcoded for internal use  ░░░
# ──────────────────────────────────────────────────────────────
DDL_USERNAME = "kalyan_thummalapalli"
DDL_PASSWORD = "Musigma@70322"

DDL_HOSTNAME     = "ddlgpmprd11.us.dell.com"
DDL_PORT         = 6420
DDL_DB           = "gp_ns_ddl_prod"
DDL_SCHEMA       = "ws_svc_gpe_ds"
LINE_OF_BUSINESS = "poweredge"

S3_ACCESS_KEY = "digital-resolution-isg"
S3_SECRET_KEY = "uVG/j6IK32lm2gI7BoYvSLEBZTklCwAX+oTPLurs"
S3_ENDPOINT   = "http://cloudstorage.onefs.dell.com"
S3_BUCKET     = "nba-raw-data"
LCLOGS_PREFIX = "telemetry-isg/poweredge/syslogs/data/lclogs/"
CONFIG_PREFIX = "telemetry-isg/poweredge/config/data/"

LOG_WINDOW_DAYS  = 14
S3_THREADS       = 32

LCLOGS_KEEP_COLS = ["case_nbr", "messageid", "severity", "message", "timestamp"]
CONFIG_KEEP_COLS = [
    "case_nbr", "objectclass", "objectid",
    "properties", "collectiontimestamp",
    "system_id", "payload_type", "case_id",
]

# ──────────────────────────────────────────────────────────────
# HEALTH-CHECK THRESHOLDS
# ──────────────────────────────────────────────────────────────
BAD_PRIMARY_STATUS  = {"2", "3", "Degraded", "Error", "Critical", "Failed", "Unknown"}
BAD_PREDICTIVE_FAIL = {"1", "Smart Alert Present", "Predictive Failure"}
BAD_REDUNDANCY      = {"4", "Lost"}

DCIM_CLASSES = [
    "DCIM_PhysicalDiskView", "DCIM_MemoryView", "DCIM_PowerSupplyView",
    "DCIM_FanView", "DCIM_ControllerBatteryView",
]
CLASS_LABELS = {
    "DCIM_PhysicalDiskView":      "Physical Disk",
    "DCIM_MemoryView":            "Memory",
    "DCIM_PowerSupplyView":       "Power Supply",
    "DCIM_FanView":               "Fan",
    "DCIM_ControllerBatteryView": "Controller Battery",
}

# ──────────────────────────────────────────────────────────────
# GLOBAL CSS
# ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
  html, body, [class*="css"] { font-family: Calibri, 'Segoe UI', sans-serif; }
  [data-testid="stAppViewContainer"] { background:#F4F6F9; }
  .block-container {
    padding-top:1rem !important;
    max-width:100% !important;
    padding-left:1rem !important;
    padding-right:1rem !important;
  }
  [data-testid="stMarkdownContainer"] * {
    cursor: default !important;
    user-select: none !important;
    -webkit-user-select: none !important;
  }
  [data-testid="stSidebar"] { background:#1F3864; }
  [data-testid="stSidebar"] * { color:#D0DCF0 !important; }
  [data-testid="stSidebar"] .stMultiSelect [data-baseweb="tag"] { background:#2C7C7E !important; }
  [data-testid="stSidebar"] label {
    color:#A8C0D8 !important; font-size:11px !important;
    font-weight:700 !important; text-transform:uppercase; letter-spacing:0.6px;
  }
  [data-testid="stSidebar"] [data-testid="stSelectbox"] div {
    background:#162d52 !important; border-color:#2C7C7E !important;
  }
  [data-testid="metric-container"] {
    background:#ffffff; border:1px solid #D9DCE0; border-radius:8px;
    padding:16px 18px 12px 18px; box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
  [data-testid="metric-container"] label {
    font-size:10px !important; font-weight:700 !important;
    text-transform:uppercase; letter-spacing:0.7px;
    color:#5A6070 !important; font-family:Calibri,sans-serif !important;
  }
  [data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size:28px !important; font-weight:700 !important;
    color:#1F3864 !important; font-family:Calibri,sans-serif !important;
    line-height:1.1;
  }
  [data-testid="metric-container"] [data-testid="stMetricDelta"] {
    font-size:11px !important; color:#2C7C7E !important;
  }
  [data-testid="stExpander"] {
    background:#ffffff !important; border:1px solid #D9DCE0 !important;
    border-radius:8px !important; margin-bottom:6px;
    box-shadow:0 1px 3px rgba(0,0,0,0.04);
  }
  [data-testid="stExpander"] summary {
    font-size:13px !important; font-weight:700 !important;
    color:#1F3864 !important; padding:12px 16px !important;
  }
  [data-testid="stDataFrame"] {
    border:1px solid #D9DCE0 !important; border-radius:8px !important; overflow:hidden;
  }
  .phase-pill {
    display:inline-block; font-size:10px; font-weight:700;
    letter-spacing:0.6px; text-transform:uppercase;
    padding:3px 10px; border-radius:12px; margin-right:8px;
  }
  .phase-pill-done    { background:#EAF7EC; color:#27ae60; }
  .phase-pill-loading { background:#EAF4FB; color:#1565a0; }
  .phase-pill-pending { background:#F0F1F3; color:#9aa0a8; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# DB + S3 INIT
# ══════════════════════════════════════════════════════════════
@st.cache_resource
def _get_engine():
    def _fix_bpchar():
        pg_dialect.ischema_names["bpchar"] = CHAR
    _fix_bpchar()

    return create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host":     DDL_HOSTNAME,
            "port":     DDL_PORT,
            "dbname":   DDL_DB,
            "user":     DDL_USERNAME,
            "password": DDL_PASSWORD,
        },
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
    )


@st.cache_resource
def _get_s3():
    from botocore.config import Config as BotocoreConfig
    session = boto3.session.Session()
    return session.client(
        service_name="s3",
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        endpoint_url=S3_ENDPOINT,
        config=BotocoreConfig(max_pool_connections=max(S3_THREADS, 32)),
    )


@st.cache_resource
def _get_s3fs():
    return s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        endpoint_url=S3_ENDPOINT,
        config_kwargs={
            "max_pool_connections": max(S3_THREADS, 32),
        },
    )


# ══════════════════════════════════════════════════════════════
# DDL QUERIES
# ══════════════════════════════════════════════════════════════
def _query_case_metadata(case_numbers: list[str]) -> pd.DataFrame:
    base_query = """
        SELECT
            case_nbr,
            case_crt_dt,
            subj_desc          AS case_subject,
            case_smry_desc     AS case_description
        FROM {table}
        WHERE case_nbr = ANY(:cases)
    """
    last_err = None
    for table in (f"{DDL_SCHEMA}.sfdc_case_dtl", "public.sfdc_case_dtl", "sfdc_case_dtl"):
        try:
            query = text(base_query.format(table=table))
            with _get_engine().connect() as conn:
                df = pd.read_sql(query, conn, params={"cases": case_numbers})
            df["case_crt_dt"] = pd.to_datetime(df["case_crt_dt"], errors="coerce")
            df["case_nbr"]    = df["case_nbr"].astype(str).str.strip()
            return df
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"DDL connection failed (metadata query): {last_err}") from last_err


def _query_parts(case_numbers: list[str]) -> pd.DataFrame:
    placeholders = ", ".join(f"'{c}'" for c in case_numbers)
    query = text(f"""
        SELECT
            cf.case_nbr,
            sgc.comdty_nm    AS part_comdty_nm,
            wis.itm_qty
        FROM
            {DDL_SCHEMA}.usdm_wo_itm_shipd_fact  wis
            INNER JOIN {DDL_SCHEMA}.usdm_wo_fact  wf
                ON  wis.wo_nbr   = wf.wo_nbr
                AND wis.wo_bu_id = wf.wo_bu_id
            INNER JOIN {DDL_SCHEMA}.usdm_case_fact  cf
                ON  wf.case_id = cf.case_id
            INNER JOIN {DDL_SCHEMA}.usdm_case_fact_x  cfx
                ON  wf.case_id = cfx.case_id
            INNER JOIN {DDL_SCHEMA}.usdm_prod_hier  ph
                ON  cf.asst_prod_hier_key = ph.prod_key
            INNER JOIN {DDL_SCHEMA}.usdm_asst_fact  af
                ON  cf.src_cust_prod_id = af.src_cust_prod_id
            LEFT  JOIN {DDL_SCHEMA}.usdm_prod_hier  phi
                ON  wis.itm_nbr = phi.itm_nbr
            LEFT  JOIN {DDL_SCHEMA}.sp_gbl_comdty  sgc
                ON  wis.itm_comdty_id = sgc.comdty_id
            LEFT  JOIN {DDL_SCHEMA}.sp_part  sp
                ON  wis.itm_nbr       = sp.part_nbr
                AND wis.itm_comdty_id = sp.comdty_id
        WHERE
            ph.prod_bu_type IN (
                'Enterprise Solution Group PBU',
                'Infrastructure Solutions PBU'
            )
            AND ph.prod_lob_cd IN ('4SV', '4ES', '4CL')
            AND wf.svc_type  != 'Labor Only'
            AND wf.wo_type    = 'Break Fix'
            AND (
                wf.curr_stat ILIKE '%close%'
                OR wf.curr_stat ILIKE '%complete%'
                OR wf.curr_stat ILIKE '%done%'
                OR wf.curr_stat ILIKE '%deliver%'
                OR wf.curr_stat ILIKE '%ship%'
                OR wf.curr_stat ILIKE '%receive%'
            )
            AND wis.itm_qty > 0
            AND wis.part_ord_stat_cd NOT IN (
                'LOCATED','Rejected','SUBSTITUTED',
                'CANCELLED','BACKLOG','IN STOCK'
            )
            AND cf.case_nbr IN ({placeholders})
    """)
    with _get_engine().connect() as conn:
        df = pd.read_sql(query, conn)
    return df


def _aggregate_parts(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw.empty:
        return pd.DataFrame(columns=["case_nbr", "parts", "qty", "resolution"])

    df_raw = df_raw.copy()
    df_raw["case_nbr"]       = df_raw["case_nbr"].astype(str).str.strip()
    df_raw["part_comdty_nm"] = df_raw["part_comdty_nm"].astype(str).str.strip().str.upper()
    df_raw["itm_qty"]        = pd.to_numeric(df_raw["itm_qty"], errors="coerce").fillna(0).astype(int)
    df_raw = df_raw[
        df_raw["part_comdty_nm"].notna() &
        (df_raw["part_comdty_nm"].str.strip() != "") &
        (df_raw["part_comdty_nm"] != "NAN")
    ]
    if df_raw.empty:
        return pd.DataFrame(columns=["case_nbr", "parts", "qty", "resolution"])

    grouped = (
        df_raw
        .groupby(["case_nbr", "part_comdty_nm"], as_index=False)["itm_qty"]
        .sum()
    )
    result = (
        grouped
        .groupby("case_nbr")
        .apply(
            lambda g: pd.Series({
                "parts": g["part_comdty_nm"].tolist(),
                "qty":   g["itm_qty"].tolist(),
            }),
            include_groups=False,
        )
        .reset_index()
    )
    result["resolution"] = result.apply(
        lambda r: ", ".join(
            f"{p} ×{q}" if q > 1 else p
            for p, q in zip(r["parts"], r["qty"])
            if str(p).strip()
        ),
        axis=1,
    )
    return result


# ══════════════════════════════════════════════════════════════
# CASE NUMBER NORMALIZATION
# ══════════════════════════════════════════════════════════════
def _normalize_case_nbr(val) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass

    s = str(val).strip()
    if re.fullmatch(r"-?\d+\.0+", s):
        s = s.split(".")[0]
    return s


def _normalize_case_set(case_numbers) -> set:
    return {_normalize_case_nbr(c) for c in case_numbers if _normalize_case_nbr(c)}


# ══════════════════════════════════════════════════════════════
# S3 HELPERS — single folder per case (not a 14-day scan)
# ══════════════════════════════════════════════════════════════
def _list_one_folder(prefix_str: str) -> list[tuple]:
    s3        = _get_s3()
    paginator = s3.get_paginator("list_objects_v2")
    items     = []
    try:
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix_str):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if not k.endswith("/") and k.endswith(".parquet"):
                    items.append((k, obj.get("Size", 0)))
    except Exception:
        pass
    return items


def _get_keys_for_case_date(prefix: str, case_crt_date) -> list[tuple]:
    date_str    = case_crt_date.strftime("%Y-%m-%d")
    folder_path = f"{prefix}case_crt_dt={date_str}/"
    return _list_one_folder(folder_path)


def _read_parquet_file_filtered(
    key: str,
    case_set: set,
    keep_columns: list,
    objectclass_filter: list,
    diag: dict,
) -> "pa.Table | None":
    try:
        fs = _get_s3fs()
        s3_path = f"{S3_BUCKET}/{key}"

        schema = pq.read_schema(s3_path, filesystem=fs)
        avail_cols = [c for c in keep_columns if c in schema.names]

        read_cols = avail_cols if "case_nbr" in avail_cols else (avail_cols + ["case_nbr"])
        if "objectclass" in schema.names and objectclass_filter and "objectclass" not in read_cols:
            read_cols = read_cols + ["objectclass"]

        pa_filter = [("case_nbr", "in", list(case_set))]
        try:
            table = pq.read_table(s3_path, filesystem=fs, columns=read_cols, filters=pa_filter)
        except Exception:
            table = pq.read_table(s3_path, filesystem=fs, columns=read_cols)

        return _apply_arrow_filters(table, case_set, objectclass_filter, diag, key)

    except Exception as e:
        diag.setdefault("s3fs_errors", []).append(f"{key}: {e}")

    try:
        obj = _get_s3().get_object(Bucket=S3_BUCKET, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        table = pq.read_table(buf)
        return _apply_arrow_filters(table, case_set, objectclass_filter, diag, key)
    except Exception as e:
        diag.setdefault("download_errors", []).append(f"{key}: {e}")
        return None


def _apply_arrow_filters(
    table: "pa.Table",
    case_set: set,
    objectclass_filter: list,
    diag: dict,
    key: str,
) -> "pa.Table | None":
    if table.num_rows == 0:
        return None

    case_col = "case_nbr" if "case_nbr" in table.column_names else None
    if case_col is None:
        normalized_lookup = {re.sub(r"[\s_]", "", c).lower(): c for c in table.column_names}
        case_col = normalized_lookup.get("casenbr") or normalized_lookup.get("casenumber")

    if case_col is None:
        diag.setdefault("missing_case_col_files", []).append(
            f"{key}: columns seen = {table.column_names}"
        )
        return None

    case_nbr_series = table.column(case_col).to_pandas().apply(_normalize_case_nbr)

    if "sample_raw_case_nbr_values" not in diag:
        diag["sample_raw_case_nbr_values"] = (
            table.column(case_col).to_pandas().dropna().astype(str).unique()[:10].tolist()
        )
        diag["case_col_dtype"] = str(table.column(case_col).type)
        diag["case_col_name_found"] = case_col

    case_mask = pa.array(case_nbr_series.isin(case_set))

    if objectclass_filter and "objectclass" in table.column_names:
        oc_mask = pc.is_in(table.column("objectclass"), value_set=pa.array(objectclass_filter))
        combined_mask = pc.and_(case_mask, oc_mask)
    else:
        combined_mask = case_mask

    diag["total_rows_seen"] = diag.get("total_rows_seen", 0) + table.num_rows
    filtered = table.filter(combined_mask)
    diag["total_rows_matched"] = diag.get("total_rows_matched", 0) + filtered.num_rows

    if filtered.num_rows == 0:
        return None

    if case_col != "case_nbr":
        filtered = filtered.rename_columns(
            [("case_nbr" if c == case_col else c) for c in filtered.column_names]
        )
    return filtered


def _download_keys_parallel(
    key_size_pairs: list,
    case_set: set,
    keep_columns: list,
    diag: dict,
    label: str,
    objectclass_filter: list = None,
) -> pd.DataFrame:
    if not key_size_pairs:
        diag["no_keys_found"] = True
        return pd.DataFrame()

    diag["file_count"] = len(key_size_pairs)
    diag["total_bytes"] = sum(size for _, size in key_size_pairs)
    diag["file_sizes_mb"] = sorted(
        [round(size / (1024 * 1024), 1) for _, size in key_size_pairs], reverse=True
    )[:10]

    tables = []
    with ThreadPoolExecutor(max_workers=S3_THREADS) as ex:
        futures = {
            ex.submit(_read_parquet_file_filtered, k, case_set, keep_columns, objectclass_filter, diag): k
            for k, _size in key_size_pairs
        }
        for future in as_completed(futures):
            res = future.result()
            if res is not None:
                tables.append(res)

    if not tables:
        return pd.DataFrame()

    combined = pa.concat_tables(tables, promote_options="default")
    return combined.to_pandas()


def _apply_time_filter(df: pd.DataFrame, crt_date_map: dict) -> pd.DataFrame:
    if df.empty or "timestamp" not in df.columns:
        return df
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["case_nbr"]  = df["case_nbr"].apply(_normalize_case_nbr)

    meta = pd.DataFrame(
        [(k, v) for k, v in crt_date_map.items()],
        columns=["case_nbr", "crt_dt"]
    )
    meta["case_nbr"] = meta["case_nbr"].apply(_normalize_case_nbr)
    meta["crt_dt"]   = pd.to_datetime(meta["crt_dt"], errors="coerce")

    merged = df.merge(meta, on="case_nbr", how="inner")
    window = pd.Timedelta(days=LOG_WINDOW_DAYS)
    mask   = (
        (merged["timestamp"] >= merged["crt_dt"] - window) &
        (merged["timestamp"] <= merged["crt_dt"])
    )
    return merged.loc[mask].drop(columns=["crt_dt"]).reset_index(drop=True)


# ══════════════════════════════════════════════════════════════
# PROGRESSIVE FETCH — split into 3 independently cached phases
# ══════════════════════════════════════════════════════════════
# Original fetch_all() blocked until BOTH lclogs AND config finished
# before rendering ANYTHING. Splitting into phases lets the page show
# case subject/description/resolution within seconds (Phase 1), then
# LC Logs shortly after (Phase 2, ~30s — small, fully relevant file),
# then Config last (Phase 3, the slow ~120-150s phase — confirmed by
# direct S3 inspection to be reading ~180MB of global, unsorted,
# all-customer data to extract a single case's ~28KB of useful rows).
#
# TRADEOFF: lclogs and config now fetch SEQUENTIALLY instead of in
# parallel, so total time may increase slightly (losing the overlap
# between the two S3 fetches — at most ~30s, the size of the smaller
# phase). In exchange, the user sees real content starting in seconds
# instead of staring at a blank page for the full duration.

@st.cache_data(show_spinner=False)
def fetch_metadata(case_numbers_tuple: tuple) -> dict:
    """PHASE 1 — DDL only (metadata + parts). Fast: seconds, not minutes."""
    timings      = {}
    case_numbers = [str(c).strip() for c in case_numbers_tuple]
    case_set     = _normalize_case_set(case_numbers)

    _t = time.time()
    with ThreadPoolExecutor(max_workers=2) as ex:
        fut_meta  = ex.submit(_query_case_metadata, case_numbers)
        fut_parts = ex.submit(_query_parts,         case_numbers)
        df_meta      = fut_meta.result()
        df_raw_parts = fut_parts.result()
    timings["db_query_s"] = round(time.time() - _t, 2)

    _t = time.time()
    df_parts = _aggregate_parts(df_raw_parts)
    timings["parts_processing_s"] = round(time.time() - _t, 2)

    return {
        "metadata": df_meta,
        "parts":    df_parts,
        "case_set": case_set,
        "timings":  timings,
        "diagnostics": {
            "case_set_searched": sorted(case_set),
            "metadata_rows": len(df_meta),
            "reason": "No case_crt_dt found in metadata — cannot determine which S3 folder to open."
                       if (df_meta.empty or df_meta["case_crt_dt"].isna().all()) else None,
        },
    }


@st.cache_data(show_spinner=False)
def fetch_lclogs(case_set_tuple: tuple, crt_dates_tuple: tuple) -> dict:
    """PHASE 2 — LC Logs only. Small, fast, fully relevant per JHub diagnostics."""
    timings      = {}
    diag_lclogs  = {}
    case_set     = set(case_set_tuple)
    crt_date_map = dict(crt_dates_tuple)
    unique_dates = sorted({d for d in crt_date_map.values() if pd.notna(d)})

    _t = time.time()
    with ThreadPoolExecutor(max_workers=min(16, max(1, len(unique_dates)))) as ex:
        lclog_futures = {ex.submit(_get_keys_for_case_date, LCLOGS_PREFIX, d): d for d in unique_dates}
        lclogs_keys = []
        for fut in as_completed(lclog_futures):
            lclogs_keys.extend(fut.result())
    timings["s3_key_listing_s"]      = round(time.time() - _t, 2)
    timings["s3_lclogs_keys"]        = len(lclogs_keys)
    timings["unique_folders_opened"] = len(unique_dates)

    _t = time.time()
    raw_logs = _download_keys_parallel(
        lclogs_keys, case_set, LCLOGS_KEEP_COLS, diag_lclogs, "lclogs",
        None,
    )
    timings["s3_transfer_s"] = round(time.time() - _t, 2)
    timings["raw_logs_rows"] = len(raw_logs)

    _t = time.time()
    if raw_logs.empty:
        df_lclogs = pd.DataFrame(columns=LCLOGS_KEEP_COLS)
    else:
        df_lclogs = _apply_time_filter(raw_logs, crt_date_map)
        keep      = [c for c in LCLOGS_KEEP_COLS if c in df_lclogs.columns]
        df_lclogs = df_lclogs[keep].reset_index(drop=True) if keep else df_lclogs.reset_index(drop=True)
    timings["data_processing_s"] = round(time.time() - _t, 2)

    timings["total_fetch_s"] = round(
        timings.get("s3_key_listing_s", 0)
        + timings.get("s3_transfer_s", 0)
        + timings.get("data_processing_s", 0), 2,
    )

    return {"lclogs": df_lclogs, "timings": timings, "diagnostics": diag_lclogs}


@st.cache_data(show_spinner=False)
def fetch_config(case_set_tuple: tuple, crt_dates_tuple: tuple) -> dict:
    """PHASE 3 — Config only. The slow phase — see module docstring above."""
    timings      = {}
    diag_config  = {}
    case_set     = set(case_set_tuple)
    crt_date_map = dict(crt_dates_tuple)
    unique_dates = sorted({d for d in crt_date_map.values() if pd.notna(d)})

    _t = time.time()
    with ThreadPoolExecutor(max_workers=min(16, max(1, len(unique_dates)))) as ex:
        config_futures = {ex.submit(_get_keys_for_case_date, CONFIG_PREFIX, d): d for d in unique_dates}
        config_keys = []
        for fut in as_completed(config_futures):
            config_keys.extend(fut.result())
    timings["s3_key_listing_s"]      = round(time.time() - _t, 2)
    timings["s3_config_keys"]        = len(config_keys)
    timings["unique_folders_opened"] = len(unique_dates)

    _t = time.time()
    raw_config = _download_keys_parallel(
        config_keys, case_set, CONFIG_KEEP_COLS, diag_config, "config",
        DCIM_CLASSES,
    )
    timings["s3_transfer_s"]   = round(time.time() - _t, 2)
    timings["raw_config_rows"] = len(raw_config)

    _t = time.time()
    if raw_config.empty:
        df_config = pd.DataFrame(columns=CONFIG_KEEP_COLS)
    else:
        dedup_cols = [c for c in ["case_nbr", "objectid", "objectclass", "collectiontimestamp"]
                      if c in raw_config.columns]
        if dedup_cols:
            raw_config = raw_config.drop_duplicates(subset=dedup_cols)
        for col in ["collectiontimestamp", "case_crt_dts", "debi_etl_inst_time"]:
            if col in raw_config.columns:
                raw_config[col] = pd.to_datetime(raw_config[col], errors="coerce")
        keep      = [c for c in CONFIG_KEEP_COLS if c in raw_config.columns]
        df_config = raw_config[keep].reset_index(drop=True) if keep else raw_config.reset_index(drop=True)
    timings["data_processing_s"] = round(time.time() - _t, 2)

    timings["total_fetch_s"] = round(
        timings.get("s3_key_listing_s", 0)
        + timings.get("s3_transfer_s", 0)
        + timings.get("data_processing_s", 0), 2,
    )

    return {"config": df_config, "timings": timings, "diagnostics": diag_config}


# ══════════════════════════════════════════════════════════════
# FEATURE ENGINEERING
# ══════════════════════════════════════════════════════════════
def parse_props(props):
    if props is None:
        return {}
    if isinstance(props, dict):
        return props
    try:
        return dict(ast.literal_eval(str(props)))
    except Exception:
        return {}


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    records = []
    for _, row in df.iterrows():
        props  = parse_props(row.get("properties"))
        objcls = row.get("objectclass", "?")
        ps     = str(props.get("PrimaryStatus",          "?"))
        pf     = str(props.get("PredictiveFailureState", "?"))
        red    = str(props.get("RedundancyStatus",       "?"))
        if objcls in DCIM_CLASSES:
            ps_bad  = ps  in BAD_PRIMARY_STATUS
            pf_bad  = pf  in BAD_PREDICTIVE_FAIL
            red_bad = red in BAD_REDUNDANCY
            ok_bad  = "BAD" if (ps_bad or pf_bad or red_bad) else "OK"
        else:
            ok_bad = "N/A"
        records.append({
            "ok_bad":                 ok_bad,
            "component":              CLASS_LABELS.get(objcls, objcls),
            "objectclass":            objcls,
            "system_id":              row.get("system_id",  ""),
            "FQDD":                   props.get("FQDD",         "?"),
            "SerialNumber":           props.get("SerialNumber", "?"),
            "PrimaryStatus":          ps,
            "PredictiveFailureState": pf,
            "RedundancyStatus":       red,
            "objectid":               row.get("objectid",   ""),
            "collectiontimestamp":    row.get("collectiontimestamp", ""),
            "payload_type":           row.get("payload_type", ""),
            "case_nbr":               row.get("case_nbr", ""),
            "case_id":                row.get("case_id", ""),
        })
    return pd.DataFrame(records)


def build_parts_resolution(case_nbr: str, df_parts: pd.DataFrame) -> str:
    row = df_parts[df_parts["case_nbr"] == case_nbr]
    if row.empty:
        return "—"
    try:
        parts = row.iloc[0]["parts"]
        qtys  = row.iloc[0]["qty"]
        if isinstance(parts, str):
            parts = re.findall(r"'([^']+)'", parts)
        if isinstance(qtys, str):
            qtys = [int(x) for x in re.findall(r'\d+', qtys)]
        parts = list(parts)
        qtys  = [int(q) for q in qtys]
        labels = []
        for part, qty in zip(parts, qtys):
            part = part.strip()
            if not part:
                continue
            labels.append(f"{part} ×{qty}" if qty > 1 else part)
        return ", ".join(labels) if labels else "—"
    except Exception:
        return "—"


# ══════════════════════════════════════════════════════════════
# UI HELPERS
# ══════════════════════════════════════════════════════════════
SEV_COLOR = {
    "Critical":      "#C0392B",
    "Warning":       "#C18A3D",
    "Informational": "#1F3864",
    "Other":         "#7F8C8D",
}


def _normalise_sev(s: str) -> str:
    t = s.strip().title()
    return t if t in SEV_COLOR else "Other"


def section_label(title: str):
    st.markdown(f"""
    <div style="
        font-size:11px;font-weight:700;text-transform:uppercase;
        letter-spacing:0.8px;color:#2C7C7E;
        padding:20px 0 8px 0;border-bottom:2px solid #2C7C7E;
        margin-bottom:16px;user-select:none;
    ">{title}</div>
    """, unsafe_allow_html=True)


def kpi_card(label, count, unique_ids, sub_label, accent_color, bg_color):
    return f"""
    <div style="
        background:{bg_color}; border:1px solid #D9DCE0;
        border-top:4px solid {accent_color}; border-radius:8px;
        padding:16px 18px 14px 18px; box-shadow:0 1px 4px rgba(0,0,0,0.06);
        user-select:none;
    ">
        <div style="font-size:10px;font-weight:700;text-transform:uppercase;
            letter-spacing:0.7px;color:#5A6070;margin-bottom:8px;">{label}</div>
        <div style="font-size:32px;font-weight:700;color:{accent_color};
            line-height:1;">{count}</div>
        <div style="font-size:11px;color:#5A6070;margin-top:6px;">
            <span style="font-weight:700;color:{accent_color};">{unique_ids}</span>
            &nbsp;{sub_label}
        </div>
    </div>"""


def phase_status_bar(meta_done: bool, logs_done: bool, config_done: bool):
    def pill(label, done, active):
        if done:
            cls, icon = "phase-pill-done", "✓"
        elif active:
            cls, icon = "phase-pill-loading", "⏳"
        else:
            cls, icon = "phase-pill-pending", "○"
        return f'<span class="phase-pill {cls}">{icon} {label}</span>'

    html = (
        pill("1 · Case Info", meta_done, not meta_done)
        + pill("2 · LC Logs", logs_done, meta_done and not logs_done)
        + pill("3 · Config Data", config_done, logs_done and not config_done)
    )
    st.markdown(f"<div style='margin:6px 0 14px 0;'>{html}</div>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# SESSION STATE INIT
# ══════════════════════════════════════════════════════════════
for key, default in [
    ("drill_day",        None),
    ("fetched_cases",    None),
    ("meta_data",         None),
    ("lclogs_data",       None),
    ("config_data",       None),
    ("active_case_tuple", None),
]:
    if key not in st.session_state:
        st.session_state[key] = default


# ══════════════════════════════════════════════════════════════
# ░░░  HEADER  ░░░
# ══════════════════════════════════════════════════════════════
st.markdown("""
<div style="
    background:linear-gradient(135deg,#1A2F5A 0%,#1F3864 60%,#24527A 100%);
    padding:18px 28px; margin-top:35px; border-radius:10px;
    border-bottom:3px solid #2C7C7E; display:flex; align-items:center;
    gap:16px; box-shadow:0 2px 8px rgba(31,56,100,0.18);
">
    <div style="width:5px;height:44px;background:linear-gradient(180deg,#2C7C7E,#7ECFD0);
        border-radius:3px;flex-shrink:0;"></div>
    <div>
        <div style="color:#FFFFFF;font-size:26px;font-weight:800;
            font-family:'Segoe UI',Calibri,sans-serif;letter-spacing:0.5px;line-height:1.15;">
            Telemetry Data</div>
        <div style="color:#7ECFD0;font-size:11px;font-family:'Segoe UI',Calibri,sans-serif;
            font-weight:600;letter-spacing:1.2px;text-transform:uppercase;margin-top:4px;">
            DCIM Property View &nbsp;·&nbsp; LC Event Log</div>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
# ░░░  CASE SEARCH / FETCH PANEL  ░░░
# ══════════════════════════════════════════════════════════════
search_col, btn_col, status_col = st.columns([2, 1, 3])

with search_col:
    st.markdown("""
    <div style="font-size:10px;font-weight:700;text-transform:uppercase;
        letter-spacing:0.7px;color:#1F3864;margin-bottom:4px;">
        Enter Case Number(s)
    </div>
    """, unsafe_allow_html=True)
    raw_input = st.text_input(
        "Case Numbers",
        placeholder="e.g. 209301297  or  209301297, 209468636",
        label_visibility="collapsed",
        key="case_input",
    )

with btn_col:
    st.markdown("<div style='height:22px'></div>", unsafe_allow_html=True)
    fetch_clicked = st.button("🔍 Fetch Data", use_container_width=True, type="primary")

with status_col:
    status_placeholder = st.empty()


def _parse_cases(raw: str) -> list[str]:
    tokens = re.split(r"[\s,;]+", raw.strip())
    return [t.strip() for t in tokens if t.strip()]


parsed_cases = _parse_cases(raw_input)

if fetch_clicked and parsed_cases:
    cases_tuple = tuple(sorted(parsed_cases))
    st.session_state["drill_day"]         = None
    st.session_state["active_case_tuple"] = cases_tuple
    st.session_state["meta_data"]   = None
    st.session_state["lclogs_data"] = None
    st.session_state["config_data"] = None
    status_placeholder.empty()

elif fetch_clicked and not parsed_cases:
    status_placeholder.warning("Please enter at least one case number.")

cases_tuple = st.session_state.get("active_case_tuple")

if cases_tuple is None:
    st.markdown("""
    <div style="
        text-align:center;padding:60px 20px;color:#5A6070;
        background:#fff;border:1px dashed #D9DCE0;border-radius:10px;
        margin-top:24px;
    ">
        <div style="font-size:36px;margin-bottom:12px;">🔍</div>
        <div style="font-size:16px;font-weight:700;color:#1F3864;margin-bottom:6px;">
            Enter a case number and click Fetch Data
        </div>
        <div style="font-size:12px;">
            Supports single or multiple cases (comma-separated)
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ══════════════════════════════════════════════════════════════
# PROGRESSIVE FETCH + RENDER ORCHESTRATION
# ══════════════════════════════════════════════════════════════
# Each phase fetches, is stored in session_state, and its UI section
# is rendered IMMEDIATELY — before the NEXT phase's (blocking) fetch
# call is reached. This is what makes case info appear first, LC Logs
# appear second, and Config appear last, instead of everything
# appearing at once after the slowest fetch finishes.

# ── PHASE 1: metadata — fetch (if needed) ───────────────────────
if st.session_state["meta_data"] is None:
    try:
        with status_placeholder:
            with st.spinner("Loading case information…"):
                t0 = time.time()
                meta_result = fetch_metadata(cases_tuple)
                meta_elapsed = time.time() - t0
        meta_result["_elapsed"] = round(meta_elapsed, 2)
        st.session_state["meta_data"] = meta_result

        if meta_result["metadata"].empty or meta_result["metadata"]["case_crt_dt"].isna().all():
            status_placeholder.error(
                meta_result["diagnostics"].get("reason")
                or "No case data found for the entered case number(s)."
            )
            st.stop()
    except Exception as e:
        status_placeholder.markdown(f"""
        <div style="background:#FDECEA;border:1px solid #f1b0b7;border-left:4px solid #C0392B;
            border-radius:8px;padding:10px 14px;margin-top:20px;font-size:12px;
            font-weight:600;color:#7a1f1f;">
            ✗ Fetch failed: {str(e)[:400]}
        </div>
        """, unsafe_allow_html=True)
        st.stop()

meta_data    = st.session_state["meta_data"]
df_metadata  = meta_data["metadata"]
df_parts     = meta_data["parts"]
case_set     = meta_data["case_set"]
crt_date_map = dict(zip(
    df_metadata["case_nbr"].apply(_normalize_case_nbr),
    df_metadata["case_crt_dt"],
))
case_set_tuple  = tuple(sorted(case_set))
crt_dates_tuple = tuple(sorted(crt_date_map.items()))

# ── RENDER NOW: phase pills (reflect true state of each phase) ──
phase_status_bar(
    meta_done=True,
    logs_done=(st.session_state["lclogs_data"] is not None),
    config_done=(st.session_state["config_data"] is not None),
)

# ── RENDER NOW: case selector + subject/description/resolution ──
# Uses ONLY Phase 1 data — renders immediately, before LC Logs or
# Config are ready. Case list comes from metadata directly.
metadata_cases = sorted(df_metadata["case_nbr"].dropna().unique())

if not metadata_cases:
    st.warning("No case data found for the entered case number(s).")
    st.stop()

info_case_col, info_res_col = st.columns([1, 2])

with info_case_col:
    st.markdown("""
    <div style="font-size:10px;font-weight:700;text-transform:uppercase;
        letter-spacing:0.7px;color:#1F3864;margin-bottom:4px;">
        Select Case Number
    </div>
    """, unsafe_allow_html=True)
    selected_case = st.selectbox(
        "Case Number", options=metadata_cases, key="global_case",
        label_visibility="collapsed"
    )

meta_row         = df_metadata[df_metadata["case_nbr"] == selected_case]
case_subject     = meta_row["case_subject"].values[0]     if (not meta_row.empty and "case_subject"     in meta_row.columns) else "—"
case_description = meta_row["case_description"].values[0] if (not meta_row.empty and "case_description" in meta_row.columns) else "—"
case_resolution  = build_parts_resolution(selected_case, df_parts)

with info_res_col:
    st.markdown("""
    <div style="font-size:10px;font-weight:700;text-transform:uppercase;
        letter-spacing:0.7px;color:#1F3864;margin-bottom:4px;">
        Case Resolution
    </div>
    """, unsafe_allow_html=True)
    st.markdown(f"""
    <div style="
        background:#fff5f5;border:1px solid #f5c6c6;border-left:4px solid #C0392B;
        border-radius:8px;padding:8px 14px;box-shadow:0 1px 3px rgba(0,0,0,0.04);
        display:flex;align-items:center;min-height:38px;
    ">
        <div style="font-size:14px;font-weight:700;color:#C0392B;
            cursor:text;user-select:text;letter-spacing:0.5px;">
            {case_resolution}
        </div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

subj_col, desc_col = st.columns([1, 2])

with subj_col:
    st.markdown(f"""
    <div style="
        background:#ffffff;border:1px solid #D9DCE0;border-left:4px solid #2C7C7E;
        border-radius:8px;padding:14px 18px;height:110px;overflow-y:auto;
        box-shadow:0 1px 3px rgba(0,0,0,0.05);
    ">
        <div style="font-size:10px;font-weight:700;text-transform:uppercase;
            letter-spacing:0.7px;color:#2C7C7E;margin-bottom:7px;">Subject</div>
        <div style="font-size:13px;font-weight:700;color:#1F3864;
            line-height:1.45;cursor:text;user-select:text;">{case_subject}</div>
    </div>
    """, unsafe_allow_html=True)

with desc_col:
    st.markdown(f"""
    <div style="
        background:#ffffff;border:1px solid #D9DCE0;border-left:4px solid #C18A3D;
        border-radius:8px;padding:14px 18px;height:110px;overflow-y:auto;
        box-shadow:0 1px 3px rgba(0,0,0,0.05);
    ">
        <div style="font-size:10px;font-weight:700;text-transform:uppercase;
            letter-spacing:0.7px;color:#C18A3D;margin-bottom:7px;">Description</div>
        <div style="font-size:13px;color:#333;line-height:1.55;
            cursor:text;user-select:text;">{case_description}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

# ── PHASE 2: LC Logs — fetch (if needed), AFTER case info already shown ──
if st.session_state["lclogs_data"] is None:
    with status_placeholder:
        with st.spinner("Loading LC Logs…"):
            t0 = time.time()
            logs_result = fetch_lclogs(case_set_tuple, crt_dates_tuple)
            logs_elapsed = time.time() - t0
    logs_result["_elapsed"] = round(logs_elapsed, 2)
    st.session_state["lclogs_data"] = logs_result
    st.rerun()

lclogs_data   = st.session_state["lclogs_data"]
df_logs       = lclogs_data["lclogs"]
logs_for_case = df_logs[df_logs["case_nbr"] == selected_case].copy()

# ── Sidebar: LC Log filters (need logs_for_case, so placed here) ──
with st.sidebar:
    st.markdown("""
    <div style="padding:16px 4px 8px 4px;user-select:none;">
      <div style="font-size:11px;font-weight:700;text-transform:uppercase;
          letter-spacing:0.7px;color:#7ECFD0;border-bottom:1px solid #2C4872;
          padding-bottom:6px;margin-bottom:12px;">LC Log Filters</div>
    </div>
    """, unsafe_allow_html=True)

    prev_sev = st.session_state.get("log_severity",   [])
    prev_mid = st.session_state.get("log_messageids", [])

    sev_opts = sorted(
        (logs_for_case[logs_for_case["messageid"].isin(prev_mid)]
         if prev_mid else logs_for_case)["severity"].dropna().unique().tolist()
    ) if "severity" in logs_for_case.columns else []
    mid_opts = sorted(
        (logs_for_case[logs_for_case["severity"].isin(prev_sev)]
         if prev_sev else logs_for_case)["messageid"].dropna().unique().tolist()
    ) if "messageid" in logs_for_case.columns else []

    selected_severity = st.multiselect(
        "Severity", options=sev_opts,
        default=[s for s in prev_sev if s in sev_opts], key="log_severity"
    )
    selected_msgids = st.multiselect(
        "Message ID", options=mid_opts,
        default=[m for m in prev_mid if m in mid_opts], key="log_messageids"
    )

final_logs = logs_for_case.copy()
if selected_severity and "severity" in final_logs.columns:
    final_logs = final_logs[final_logs["severity"].isin(selected_severity)]
if selected_msgids and "messageid" in final_logs.columns:
    final_logs = final_logs[final_logs["messageid"].isin(selected_msgids)]
final_logs = final_logs.sort_values("timestamp", ascending=True) if "timestamp" in final_logs.columns else final_logs

# ── RENDER NOW: LC Logs KPI cards — appears right after Phase 2 ──
section_label("LC Logs")

sev_norm     = final_logs["severity"].str.strip().str.title() if (not final_logs.empty and "severity" in final_logs.columns) else pd.Series([], dtype=str)
total_events = len(final_logs)
info_count   = int((sev_norm == "Informational").sum())
warn_count   = int((sev_norm == "Warning").sum())
crit_count   = int((sev_norm == "Critical").sum())
info_ids     = final_logs[sev_norm == "Informational"]["messageid"].nunique() if (not final_logs.empty and "messageid" in final_logs.columns) else 0
warn_ids     = final_logs[sev_norm == "Warning"]["messageid"].nunique()       if (not final_logs.empty and "messageid" in final_logs.columns) else 0
crit_ids     = final_logs[sev_norm == "Critical"]["messageid"].nunique()      if (not final_logs.empty and "messageid" in final_logs.columns) else 0

k1, k2, k3, k4 = st.columns(4)

with k1:
    st.markdown(f"""
    <div style="
        background:#EAF4FB;border:1px solid #D9DCE0;border-top:4px solid #1565a0;
        border-radius:8px;padding:16px 18px 14px 18px;
        box-shadow:0 1px 4px rgba(0,0,0,0.06);user-select:none;
    ">
        <div style="font-size:10px;font-weight:700;text-transform:uppercase;
            letter-spacing:0.7px;color:#5A6070;margin-bottom:8px;">Total Events</div>
        <div style="font-size:32px;font-weight:700;color:#1F3864;line-height:1;">
            {total_events}</div>
        <div style="font-size:11px;color:#5A6070;margin-top:6px;">
            <span style="font-weight:700;color:#1565a0;">
                {final_logs['messageid'].nunique() if (not final_logs.empty and 'messageid' in final_logs.columns) else 0}
            </span>&nbsp;unique msg IDs
        </div>
    </div>""", unsafe_allow_html=True)

with k2:
    st.markdown(kpi_card("Informational", info_count, info_ids, "unique msg IDs",
                         "#1565a0", "#EAF4FB"), unsafe_allow_html=True)
with k3:
    st.markdown(kpi_card("Warning", warn_count, warn_ids, "unique msg IDs",
                         "#8a5a00", "#FEF6E7"), unsafe_allow_html=True)
with k4:
    st.markdown(kpi_card("Critical", crit_count, crit_ids, "unique msg IDs",
                         "#C0392B", "#FDECEA"), unsafe_allow_html=True)

# ── RENDER NOW: Log Event Timeline ───────────────────────────────
section_label("Log Event Timeline")

if final_logs.empty or "timestamp" not in final_logs.columns:
    st.info("No log events to chart for this case and filter combination.")
else:
    chart_logs = final_logs.copy()
    chart_logs = chart_logs[chart_logs["timestamp"].notna()]
    chart_logs["sev_norm"] = chart_logs["severity"].apply(_normalise_sev) if "severity" in chart_logs.columns else "Other"
    chart_logs["date"]     = chart_logs["timestamp"].dt.date

    drill_day = st.session_state.get("drill_day")

    if drill_day is not None:
        day_logs = chart_logs[chart_logs["timestamp"].dt.date == drill_day].copy()
        day_logs["hour"] = day_logs["timestamp"].dt.hour

        col_back, col_title = st.columns([1, 6])
        with col_back:
            if st.button("← All Days", key="back_to_days"):
                st.session_state["drill_day"] = None
                st.rerun()
        with col_title:
            st.markdown(
                f"<div style='font-size:13px;font-weight:700;color:#1F3864;"
                f"padding-top:6px;'>Hourly breakdown — "
                f"<span style='color:#2C7C7E;'>{drill_day.strftime('%d %b %Y')}</span></div>",
                unsafe_allow_html=True,
            )

        hour_fig  = go.Figure()
        all_hours = list(range(24))

        for sev in ["Critical", "Warning", "Informational", "Other"]:
            sev_data = day_logs[day_logs["sev_norm"] == sev]
            if sev_data.empty:
                continue
            counts = sev_data.groupby("hour").size().reindex(all_hours, fill_value=0)

            hover_texts = []
            for h in all_hours:
                hr_sev = sev_data[sev_data["hour"] == h]
                if hr_sev.empty:
                    hover_texts.append(f"<b>{sev}</b><br>No events at {h:02d}:00")
                    continue
                top_ids  = hr_sev["messageid"].value_counts().head(5) if "messageid" in hr_sev.columns else pd.Series([])
                id_lines = "<br>".join(
                    f"&nbsp;&nbsp;{mid} ×{cnt}" for mid, cnt in top_ids.items()
                )
                hover_texts.append(
                    f"<b>{sev}</b><br>Events: <b>{len(hr_sev)}</b><br>"
                    f"──────────────<br><b>Message IDs:</b><br>{id_lines}"
                )

            hour_fig.add_trace(go.Bar(
                x=[f"{h:02d}:00" for h in all_hours],
                y=counts.values,
                name=sev,
                marker_color=SEV_COLOR[sev],
                marker_line_width=0,
                text=hover_texts,
                textposition="none",
                hovertemplate="%{text}<extra></extra>",
            ))

        total_day = len(day_logs)
        peak_hour = int(day_logs.groupby("hour").size().idxmax()) if not day_logs.empty else 0

        hour_fig.update_layout(
            barmode="stack", height=320,
            margin=dict(l=0, r=0, t=44, b=40),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#F8FAFC",
            font=dict(family="Calibri, Segoe UI, sans-serif", size=12, color="#1F3864"),
            hoverlabel=dict(bgcolor="#1F3864", font=dict(size=11, color="#FFFFFF"),
                            bordercolor="#2C7C7E", namelength=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                        font=dict(size=11), bgcolor="rgba(0,0,0,0)"),
            xaxis=dict(showgrid=False, tickfont=dict(size=10, color="#5A6070"), linecolor="#D9DCE0"),
            yaxis=dict(gridcolor="#EEF0F3", tickfont=dict(size=10, color="#5A6070"),
                       title=dict(text="Events", font=dict(size=10, color="#5A6070")),
                       linecolor="#D9DCE0"),
            bargap=0.18,
            annotations=[dict(
                text=(f"<b>{total_day}</b> events on {drill_day.strftime('%d %b')}  ·  "
                      f"Peak hour: <b>{peak_hour:02d}:00</b>"),
                xref="paper", yref="paper", x=0, y=1.13, showarrow=False,
                font=dict(size=11, color="#2C7C7E"), align="left",
            )],
        )
        st.plotly_chart(hour_fig, use_container_width=True, config={"displayModeBar": False})

    else:
        all_dates = sorted(chart_logs["date"].unique())
        day_fig   = go.Figure()

        for sev in ["Critical", "Warning", "Informational", "Other"]:
            sev_data = chart_logs[chart_logs["sev_norm"] == sev]
            if sev_data.empty:
                continue
            counts = sev_data.groupby("date").size().reindex(all_dates, fill_value=0)
            day_fig.add_trace(go.Bar(
                x=[d.strftime("%d %b") for d in all_dates],
                y=counts.values,
                name=sev,
                marker_color=SEV_COLOR[sev],
                marker_line_width=0,
                hovertemplate=(
                    f"<b>{sev}</b><br>Date: %{{x}}<br>Events: %{{y}}<br>"
                    "<i>Select day below to drill into hours</i><extra></extra>"
                ),
            ))

        daily_totals = chart_logs.groupby("date").size()
        peak_day     = daily_totals.idxmax() if not daily_totals.empty else None
        peak_label   = peak_day.strftime("%d %b") if peak_day else ""
        peak_count   = int(daily_totals.max()) if not daily_totals.empty else 0

        day_fig.update_layout(
            barmode="stack", height=320,
            margin=dict(l=0, r=0, t=44, b=40),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#F8FAFC",
            font=dict(family="Calibri, Segoe UI, sans-serif", size=12, color="#1F3864"),
            legend=dict(orientation="h", yanchor="bottom", y=1.06, xanchor="right", x=1,
                        font=dict(size=11), bgcolor="rgba(0,0,0,0)"),
            xaxis=dict(showgrid=False, tickfont=dict(size=10, color="#5A6070"), linecolor="#D9DCE0"),
            yaxis=dict(gridcolor="#EEF0F3", tickfont=dict(size=10, color="#5A6070"),
                       title=dict(text="Events", font=dict(size=10, color="#5A6070")),
                       linecolor="#D9DCE0"),
            bargap=0.25,
            annotations=[dict(
                text=(f"Peak day: <b>{peak_label}</b> ({peak_count} events)  ·  "
                      f"Select a day below to drill into hourly view"),
                xref="paper", yref="paper", x=0, y=1.16, showarrow=False,
                font=dict(size=11, color="#2C7C7E"), align="left",
            )],
        )
        st.plotly_chart(day_fig, use_container_width=True, config={"displayModeBar": False})

        st.markdown(
            "<div style='font-size:11px;color:#5A6070;margin-top:-8px;margin-bottom:8px;"
            "user-select:none;'>Select a day below to drill into hourly view</div>",
            unsafe_allow_html=True,
        )

        date_options = [d.strftime("%d %b %Y") for d in all_dates]
        date_map     = {d.strftime("%d %b %Y"): d for d in all_dates}

        selected_date_str = st.selectbox(
            "Drill into day",
            options=["— select a day —"] + date_options,
            key="day_drill_select",
            label_visibility="collapsed",
        )
        if selected_date_str != "— select a day —":
            st.session_state["drill_day"] = date_map[selected_date_str]
            st.rerun()

# ── RENDER NOW: Log Events Table ─────────────────────────────────
section_label("Log Events Table")

if final_logs.empty:
    st.info("No log events found for this case and filter combination.")
else:
    display_cols = [c for c in ["messageid", "severity", "message", "timestamp"]
                    if c in final_logs.columns]
    display_logs = final_logs[display_cols].reset_index(drop=True)

    def _colour_log_row(row):
        sev = str(row["severity"]).strip().title() if "severity" in row else ""
        if sev == "Critical":
            bg = "#FDECEA"
        elif sev == "Warning":
            bg = "#FEF6E7"
        elif sev == "Informational":
            bg = "#EAF4FB"
        else:
            bg = "#FFFFFF"
        return [f"background-color:{bg}"] * len(row)

    st.dataframe(
        display_logs.style.apply(_colour_log_row, axis=1),
        use_container_width=True,
        height=520,
        hide_index=True,
    )

# ── PHASE 3: Config Data — fetch (if needed), AFTER logs already shown ──
if st.session_state["config_data"] is None:
    with status_placeholder:
        with st.spinner("Loading Config Data — this is the larger fetch and may take longer…"):
            t0 = time.time()
            config_result = fetch_config(case_set_tuple, crt_dates_tuple)
            config_elapsed = time.time() - t0
    config_result["_elapsed"] = round(config_elapsed, 2)
    st.session_state["config_data"] = config_result
    st.rerun()

config_data     = st.session_state["config_data"]
df_config       = config_data["config"]
config_for_case = df_config[df_config["case_nbr"] == selected_case].copy()

# ── All three phases complete — final status summary + pills ──
total_elapsed = round(
    meta_data.get("_elapsed", 0) + lclogs_data.get("_elapsed", 0) + config_data.get("_elapsed", 0), 2
)
n_logs   = len(df_logs)
n_config = len(df_config)
status_placeholder.markdown(f"""
<div style="
    background:#EAF7EC;border:1px solid #c3e6cb;border-left:4px solid #27ae60;
    border-radius:8px;padding:8px 14px;margin-top:20px;
    font-size:12px;font-weight:600;color:#1e3a2f;
">
    ✓ All phases complete in {total_elapsed:.1f}s total &nbsp;·&nbsp;
    Logs: {n_logs} rows &nbsp;·&nbsp; Config: {n_config} rows
</div>
""", unsafe_allow_html=True)

phase_status_bar(meta_done=True, logs_done=True, config_done=True)

# ── Sidebar: Config filters + actions (config now available) ──
with st.sidebar:
    st.markdown("""
    <div style="padding:16px 4px 8px 4px;user-select:none;">
      <div style="font-size:11px;font-weight:700;text-transform:uppercase;
          letter-spacing:0.7px;color:#7ECFD0;border-bottom:1px solid #2C4872;
          padding-bottom:6px;margin-bottom:12px;">Config Filters</div>
    </div>
    """, unsafe_allow_html=True)

    obj_opts = sorted([
        cls for cls in DCIM_CLASSES
        if "objectclass" in config_for_case.columns
        and cls in config_for_case["objectclass"].dropna().unique().tolist()
    ])
    selected_classes = st.multiselect(
        "objectclass", options=obj_opts, default=[], key="config_obj_classes"
    )
    ok_bad_filter = st.selectbox(
        "Status", options=["All", "OK", "BAD", "N/A"], index=0, key="ok_bad_filter"
    )

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
    st.markdown("""
    <div style="font-size:11px;font-weight:700;text-transform:uppercase;
        letter-spacing:0.7px;color:#7ECFD0;border-bottom:1px solid #2C4872;
        padding-bottom:6px;margin-bottom:12px;">Actions</div>
    """, unsafe_allow_html=True)
    if st.button("🔄 Clear Cache & Re-fetch", use_container_width=True):
        fetch_metadata.clear()
        fetch_lclogs.clear()
        fetch_config.clear()
        st.session_state["meta_data"]         = None
        st.session_state["lclogs_data"]       = None
        st.session_state["config_data"]       = None
        st.session_state["active_case_tuple"] = None
        st.rerun()

case_config = config_for_case.copy()
if selected_classes and "objectclass" in case_config.columns:
    case_config = case_config[case_config["objectclass"].isin(selected_classes)]

full_result = engineer_features(case_config) if not case_config.empty else pd.DataFrame()
result_df   = (
    full_result[full_result["ok_bad"] == ok_bad_filter].reset_index(drop=True)
    if (not full_result.empty and ok_bad_filter != "All") else full_result.copy()
)

# ── RENDER NOW: Config Data section — appears LAST, once Phase 3 is done ──
section_label("Config Data")

if full_result.empty:
    st.info("No config data available for this case.")
else:
    present_classes = [cls for cls in DCIM_CLASSES if cls in full_result["objectclass"].values]

    if present_classes:
        dcim_only  = full_result[full_result["objectclass"].isin(DCIM_CLASSES)]
        total_comp = len(dcim_only)
        total_ok   = int((dcim_only["ok_bad"] == "OK").sum())
        total_bad  = int((dcim_only["ok_bad"] == "BAD").sum())
        ok_fqdds   = dcim_only[dcim_only["ok_bad"] == "OK"]["FQDD"].nunique()
        bad_fqdds  = dcim_only[dcim_only["ok_bad"] == "BAD"]["FQDD"].nunique()

        cc1, cc2, cc3 = st.columns(3)
        with cc1:
            st.markdown(f"""
            <div style="
                background:#E5EBF5;border:1px solid #D9DCE0;border-top:4px solid #1F3864;
                border-radius:8px;padding:16px 18px 14px 18px;
                box-shadow:0 1px 4px rgba(0,0,0,0.06);user-select:none;
            ">
                <div style="font-size:10px;font-weight:700;text-transform:uppercase;
                    letter-spacing:0.7px;color:#5A6070;margin-bottom:8px;">Total Components</div>
                <div style="font-size:32px;font-weight:700;color:#1F3864;line-height:1;">
                    {total_comp}</div>
                <div style="font-size:11px;color:#5A6070;margin-top:6px;">
                    across {len(present_classes)} classes
                </div>
            </div>""", unsafe_allow_html=True)

        with cc2:
            st.markdown(f"""
            <div style="
                background:#EAF7EC;border:1px solid #D9DCE0;border-top:4px solid #27ae60;
                border-radius:8px;padding:16px 18px 14px 18px;
                box-shadow:0 1px 4px rgba(0,0,0,0.06);user-select:none;
            ">
                <div style="font-size:10px;font-weight:700;text-transform:uppercase;
                    letter-spacing:0.7px;color:#5A6070;margin-bottom:8px;">Total OK</div>
                <div style="font-size:32px;font-weight:700;color:#27ae60;line-height:1;">
                    {total_ok}</div>
                <div style="font-size:11px;color:#5A6070;margin-top:6px;">
                    <span style="font-weight:700;color:#27ae60;">{ok_fqdds}</span>
                    &nbsp;unique FQDDs
                </div>
            </div>""", unsafe_allow_html=True)

        with cc3:
            st.markdown(f"""
            <div style="
                background:#FDECEA;border:1px solid #D9DCE0;border-top:4px solid #C0392B;
                border-radius:8px;padding:16px 18px 14px 18px;
                box-shadow:0 1px 4px rgba(0,0,0,0.06);user-select:none;
            ">
                <div style="font-size:10px;font-weight:700;text-transform:uppercase;
                    letter-spacing:0.7px;color:#5A6070;margin-bottom:8px;">Total BAD</div>
                <div style="font-size:32px;font-weight:700;color:#C0392B;line-height:1;">
                    {total_bad}</div>
                <div style="font-size:11px;color:#5A6070;margin-top:6px;">
                    <span style="font-weight:700;color:#C0392B;">{bad_fqdds}</span>
                    &nbsp;unique FQDDs
                </div>
            </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

        bad_rows = result_df[result_df["ok_bad"] == "BAD"].reset_index(drop=True)

        if not bad_rows.empty:
            st.markdown("""
            <div style="font-size:11px;font-weight:700;text-transform:uppercase;
                letter-spacing:0.8px;color:#C0392B;
                padding-bottom:6px;border-bottom:1px solid #e8d0cf;
                margin-bottom:14px;user-select:none;">
                DCIM Bad Components
            </div>""", unsafe_allow_html=True)

            for i in range(0, len(bad_rows), 2):
                cols = st.columns(2)
                for j, col in enumerate(cols):
                    idx = i + j
                    if idx >= len(bad_rows):
                        break
                    row = bad_rows.iloc[idx]
                    fqdd   = row.get("FQDD",         "?")
                    objcls = row.get("objectclass",  "?")
                    ps     = row.get("PrimaryStatus", "?")
                    pf     = row.get("PredictiveFailureState", "?")
                    red    = row.get("RedundancyStatus", "?")
                    sn     = row.get("SerialNumber",  "?")

                    ps_color  = "#C0392B" if ps  in BAD_PRIMARY_STATUS  else "#1F3864"
                    pf_color  = "#C18A3D" if pf  in BAD_PREDICTIVE_FAIL else "#1F3864"
                    red_color = "#C0392B" if red in BAD_REDUNDANCY       else "#1F3864"

                    with col:
                        st.markdown(f"""
                        <div style="
                            background:#ffffff;border:1px solid #D9DCE0;
                            border-left:4px solid #C0392B;border-radius:8px;
                            padding:16px 20px 14px 16px;margin-bottom:10px;
                            box-shadow:0 1px 4px rgba(0,0,0,0.07);
                        ">
                            <div style="margin-bottom:8px;">
                                <span style="
                                    background:#C0392B;color:#fff;font-size:10px;
                                    font-weight:700;padding:2px 9px;border-radius:3px;
                                    letter-spacing:0.8px;text-transform:uppercase;
                                ">BAD</span>
                            </div>
                            <div style="
                                font-family:'Courier New',monospace;font-size:12px;
                                font-weight:700;color:#1F3864;margin-bottom:4px;
                                word-break:break-all;
                            ">{fqdd}</div>
                            <div style="font-size:12px;color:#2C7C7E;
                                margin-bottom:14px;font-weight:600;">{objcls}</div>
                            <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px 20px;">
                                <div>
                                    <div style="font-size:11px;color:#5A6070;margin-bottom:2px;">Primary Status</div>
                                    <div style="font-weight:700;color:{ps_color};font-family:'Courier New',monospace;">{ps}</div>
                                </div>
                                <div>
                                    <div style="font-size:11px;color:#5A6070;margin-bottom:2px;">Predictive Failure</div>
                                    <div style="font-weight:700;color:{pf_color};font-family:'Courier New',monospace;">{pf}</div>
                                </div>
                                <div>
                                    <div style="font-size:11px;color:#5A6070;margin-bottom:2px;">Redundancy Status</div>
                                    <div style="font-weight:700;color:{red_color};font-family:'Courier New',monospace;">{red}</div>
                                </div>
                                <div>
                                    <div style="font-size:11px;color:#5A6070;margin-bottom:2px;">Serial Number</div>
                                    <div style="font-weight:700;font-family:'Courier New',monospace;">{sn}</div>
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        for cls in present_classes:
            subset_detail = result_df[result_df["objectclass"] == cls]
            if subset_detail.empty:
                continue
            label = CLASS_LABELS[cls]
            bad_n = int((full_result[full_result["objectclass"] == cls]["ok_bad"] == "BAD").sum())
            ok_n  = int((full_result[full_result["objectclass"] == cls]["ok_bad"] == "OK").sum())

            with st.expander(f"{label}  —  {ok_n} OK  |  {bad_n} BAD", expanded=False):
                st.markdown(
                    f"<span style='color:#27ae60;font-weight:700;font-size:13px'>{ok_n} OK</span>"
                    f"<span style='color:#bdc3c7;margin:0 8px'>|</span>"
                    f"<span style='color:#C0392B;font-weight:700;font-size:13px'>{bad_n} BAD</span>",
                    unsafe_allow_html=True
                )
                st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

                dcols = [c for c in [
                    "ok_bad", "FQDD", "SerialNumber",
                    "PrimaryStatus", "PredictiveFailureState",
                    "RedundancyStatus", "system_id", "collectiontimestamp"
                ] if c in subset_detail.columns]

                def _hl(row):
                    bg = "#FDECEA" if row["ok_bad"] == "BAD" else "#EAF7EC"
                    return [f"background-color:{bg}"] * len(row)

                st.dataframe(
                    subset_detail[dcols].reset_index(drop=True).style.apply(_hl, axis=1),
                    use_container_width=True, hide_index=True
                )

