import re
import numpy as np
import pandas as pd
import snowflake.connector
from dataclasses import dataclass
from typing import Dict, Optional

# =========================
# Global parameters
# =========================
RAW_POS_PATH = "WG.xlsx"   # Default POS path (for testing)
RAW_POS_HEADER_ROW = 1
WEEKS_PM = 4.3
CUTOFF = pd.to_datetime("2025-08-01").to_period("M").to_timestamp(how="start")  # Cutoff month (month start)

# Brand ↔ CUSTOMER_KEY mapping (extend as needed)
CUSTOMER_MAP: Dict[str, str] = {
    "WG": "0010002016",  # Walgreens
    "WM": "0010004314",  # Walmart (example)
    "CVS": "0010002009", # CVS (example)
    "DG": "0010007686",  # Dollar General (example)
    "ULTA": "0010008732",
    "FD": "0010007573",
    "TG": "0010003336"
}

# =========================
# Customer-specific Week → Month mapping definition
# =========================
WEEK_DATES_A = [  # CVS, FD, MJ, TG, ULTA, WG
    "01/04","01/11","01/18","01/25","02/01","02/08","02/15","02/22",
    "03/01","03/08","03/15","03/22","03/29","04/05","04/12","04/19",
    "04/26","05/03","05/10","05/17","05/24","05/31","06/07","06/14",
    "06/21","06/28","07/05","07/12","07/19","07/26","08/02","08/09",
    "08/16","08/23","08/30","09/06","09/13","09/20","09/27","10/04",
    "10/11","10/18","10/25","11/01","11/08","11/15","11/22","11/29",
    "12/06","12/13","12/20","12/27"
]
WEEK_DATES_B = [  # WM, DG
    "01-03","01-10","01-17","01-24","01-31","02-07","02-14","02-21","02-28",
    "03-07","03-14","03-21","03-28","04-04","04-11","04-18","04-25","05-02",
    "05-09","05-16","05-23","05-30","06-06","06-13","06-20","06-27","07-04",
    "07-11","07-18","07-25","08-01","08-08","08-15","08-22","08-29","09-05",
    "09-12","09-19","09-26","10-03","10-10","10-17","10-24","10-31","11-07",
    "11-14","11-21","11-28","12-05","12-12","12-19","12-26"
]
WEEK_DATES_BY_BRAND = {
    # Type A
    "CVS": WEEK_DATES_A, "FD": WEEK_DATES_A, "MJ": WEEK_DATES_A,
    "TG": WEEK_DATES_A, "ULTA": WEEK_DATES_A, "WG": WEEK_DATES_A,
    # Type B
    "WM": WEEK_DATES_B, "DG": WEEK_DATES_B,
}

def _build_week_to_month_map_from_brand(brand: str, year_anchor: int = 2022) -> Dict[int, int]:
    dates = WEEK_DATES_BY_BRAND.get(str(brand).upper(), WEEK_DATES_A)
    months = [pd.to_datetime(f"{year_anchor}-{d.replace('/', '-')}", errors="coerce").month for d in dates]
    # week index(1~53) → month(1~12)
    return {i + 1: m for i, m in enumerate(months)}

# =========================
# Snowflake: load forecast (convert MONTH_KEY → YearMonth)
# =========================
def load_fcst_from_snowflake(brand: str = "WG") -> pd.DataFrame:
    user = "JEJEON@KISSUSA.COM"
    account = "UKDVSEA-NPB82638"
    warehouse = "COMPUTE_WH"
    database = "KDB"
    schema = "SCP"

    conn = snowflake.connector.connect(
        user=user,
        account=account,
        authenticator="externalbrowser",
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    cur = conn.cursor()
    cur.execute(f'USE WAREHOUSE {warehouse}')
    cur.execute(f'USE DATABASE {database}')
    cur.execute(f'USE SCHEMA {schema}')

    customer_key = CUSTOMER_MAP.get(brand.upper())
    if not customer_key:
        raise ValueError(f"Unknown brand: {brand}")

    query = f"""
    SELECT "MATERIAL_KEY", "MONTH_KEY", "ESHIP", "ASHIP+Open" AS "ASHIP"
    FROM ZPPRFC01
    WHERE "PLANT_KEY" IN ('G100') AND "CUSTOMER_KEY" = '{customer_key}'
    """
    df_fcst = pd.read_sql(query, conn)

    # MONTH_KEY(YYYYMM) → YearMonth(datetime)
    df_fcst["MONTH_KEY"] = pd.to_datetime(df_fcst["MONTH_KEY"].astype(str), format="%Y%m", errors="coerce")
    df_fcst.rename(columns={"MONTH_KEY": "YearMonth"}, inplace=True)
    # Safely normalize to month start
    df_fcst["YearMonth"] = df_fcst["YearMonth"].dt.to_period("M").dt.to_timestamp(how="start")
    return df_fcst

# =========================
# Load POS & preprocess (aggregate weekly→monthly)
# =========================
def read_pos_raw(path: str, header_row: int = RAW_POS_HEADER_ROW) -> pd.DataFrame:
    return pd.read_excel(path, header=header_row)

def pos_preprocess(pos: pd.DataFrame, brand: str | None = None) -> pd.DataFrame:
    if str(brand).upper() == "TG" and "On-Counter Date" in pos.columns:
        pos = pos[pos["On-Counter Date"].astype(str).str.strip().str.upper() == "STORE"].copy()

    if str(brand).upper() == 'ULTA' and "On-Counter Date" in pos.columns:
        pos = pos[pos["On-Counter Date"].astype(str).str.strip().str.upper() == "IN-STORE"].copy()

    # 1) Remove PPK rows (only for existing status columns)
    def drop_ppk_rows(df: pd.DataFrame,
                      candidate_cols=("SS Status", "FW Status", "Status"),
                      suffix="PPK") -> pd.DataFrame:
        cols = [c for c in candidate_cols if c in df.columns]
        if not cols:
            return df.reset_index(drop=True)
        mask = np.ones(len(df), dtype=bool)
        for c in cols:
            s = df[c].astype(str).str.strip().str.upper()
            mask &= ~s.str.endswith(suffix.upper())
        return df[mask].reset_index(drop=True)

    pos = drop_ppk_rows(pos)

    # 2) Filter to Nail PU
    if "PU" in pos.columns:
        pos = pos[pos["PU"].astype(str).str.strip().str.upper() == "NAIL"]

    # 3) Standardize Segment (only if present)
    if "Segment" in pos.columns:
        pos["Segment"] = pos["Segment"].astype(str).str.strip()
        pos["Segment"].replace({
            "imPRESS ": "imPRESS",
            "Impress": "imPRESS",
            "Preglued Nails": "PreGlued Nails",
            "FRENCH NAILS": "French Nails",
            "Decorated nails": "Decorated Nails",
            "French nails": "French Nails",
            "Color nails": "Color Nails",
            "impress toe nail": "Toe Nails",
        }, inplace=True)
        pos = pos.dropna(subset=["Segment"]).reset_index(drop=True)

    # 4) Select required columns only
    units_cols   = [c for c in pos.columns if re.fullmatch(r"Units Wk\s*\d+", str(c))]
    door_cols    = [c for c in pos.columns if re.fullmatch(r"Door Wk\s*\d+", str(c))]
    instock_cols = [c for c in pos.columns if re.fullmatch(r"Instock % Wk\s*\d+", str(c))]
    base_cols    = [c for c in ["Material", "Segment", "YEAR"] if c in pos.columns]
    pos_sel = pos[base_cols + units_cols + door_cols + instock_cols].copy()

    # 5) Filter to recent years (optional)
    if "YEAR" in pos_sel.columns:
        pos_sel = pos_sel[pos_sel["YEAR"] > 2022]
    return pos_sel



def convert_weekly_to_weekly_long(
    df: pd.DataFrame,
    brand: str = "WG",
    week_to_month_map: Optional[Dict[int, int]] = None,
    instock_scale_if_fraction: float = 100.0
) -> pd.DataFrame:
    """
    Convert weekly (Units/Door/Instock) to WEEKLY-long format (NO monthly aggregation)
      - Keeps the exact same preprocessing as convert_weekly_to_monthly_long()
      - Computes weekly UPM (=Units/Door) as "UPM_week"
      - Returns weekly-level rows: Material/Segment/(Year)/WeekNum/(Month)/Units/Door/Instock/UPM_week/YearMonth
    """
    if week_to_month_map is None:
        week_to_month_map = _build_week_to_month_map_from_brand(brand)

    # 1) Find week columns that actually exist  (same as monthly fn)
    def cols_like(prefix: str):
        pat = re.compile(rf"^{re.escape(prefix)}\s*\d+$")
        return [c for c in df.columns if isinstance(c, str) and pat.match(c)]

    units_cols   = cols_like("Units Wk")
    door_cols    = cols_like("Door Wk")
    instock_cols = cols_like("Instock % Wk")

    # 2) id vars
    id_vars = ["Material", "Segment"]
    if "YEAR" in df.columns:
        id_vars += ["YEAR"]

    # 3) Melt + map week → month  (same as monthly fn)
    def melt_metric(metric_cols, value_name):
        if not metric_cols:
            return pd.DataFrame(columns=id_vars + ["WeekNum", "Month", value_name])

        long = df[id_vars + metric_cols].melt(
            id_vars=id_vars, var_name="Week", value_name=value_name
        )
        long["WeekNum"] = long["Week"].str.extract(r"(\d+)").astype("Int64")
        long["Month"]   = long["WeekNum"].map(week_to_month_map).astype("Int64")
        return long.drop(columns=["Week"])

    u_long = melt_metric(units_cols,   "Units")
    d_long = melt_metric(door_cols,    "Door")
    i_long = melt_metric(instock_cols, "Instock")

    # 4) Merge the three metrics  (same as monthly fn)
    key_cols = [c for c in ["Material", "Segment", "YEAR", "WeekNum", "Month"]
                if c in (u_long.columns.union(d_long.columns).union(i_long.columns))]
    merged = u_long.merge(d_long, on=key_cols, how="left").merge(i_long, on=key_cols, how="left")

    # 5) Convert to numeric & rescale Instock (same as monthly fn)
    for c in ["Units", "Door", "Instock"]:
        if c in merged.columns:
            merged[c] = pd.to_numeric(merged[c], errors="coerce")

    if "Instock" in merged.columns and merged["Instock"].notna().any():
        q95 = merged["Instock"].quantile(0.95)
        if pd.notna(q95) and q95 <= 1.5:
            merged["Instock"] = merged["Instock"] * instock_scale_if_fraction

    # 6) Weekly UPM (=Units/Door)  (same as monthly fn)
    merged["UPM_week"] = np.where(
        merged.get("Door", 0) > 0,
        merged.get("Units", np.nan) / merged.get("Door", np.nan),
        np.nan
    )

    # 7) Create Year/Month/YearMonth (keep same logic; but DO NOT aggregate monthly)
    if "YEAR" in merged.columns:
        merged = merged.rename(columns={"YEAR": "Year"})
    else:
        merged["Year"] = CUTOFF.year  # fallback (same as monthly fn)

    merged["Month"] = pd.to_numeric(merged["Month"], errors="coerce").astype("Int64")
    merged["Year"]  = pd.to_numeric(merged["Year"],  errors="coerce").astype("Int64")

    ym = pd.to_datetime(
        dict(
            year=merged["Year"].astype(int),
            month=merged["Month"].astype(int),
            day=1
        ),
        errors="coerce"
    )
    merged["YearMonth"] = ym.dt.to_period("M").dt.to_timestamp(how="start")

    # ✅ return WEEKLY rows (no groupby)
    # Optional: 정렬
    sort_cols = [c for c in ["Material", "Segment", "Year", "Month", "WeekNum"] if c in merged.columns]
    if sort_cols:
        merged = merged.sort_values(sort_cols).reset_index(drop=True)

    return merged
