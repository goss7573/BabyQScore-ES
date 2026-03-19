# risk_ptb.py
# Deterministic lookup for Preterm Birth (PTB) with standardized single-row headers.

from __future__ import annotations

import os
import logging
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd

log = logging.getLogger("risk_ptb")
if not log.handlers:
    logging.basicConfig(level=logging.INFO)

# ---------- Bands / anchors ----------
PTB_GLOBAL_AVG = 10.0           # %
PTB_AVG_LOW = 8.5               # %
PTB_AVG_HIGH = 11.5             # %
ARROW_VMAX = 20.0               # % → 100 scale

# ---------- File path ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_REL = os.path.join("data", "PTB_Version_fixed_Row_Restored.xlsx")
XLSX_PATH = os.environ.get("PTB_XLSX_PATH") or os.path.join(BASE_DIR, DEFAULT_REL)

# ---------- Expected columns ----------
EXPECTED = [
    "age_15_19", "age_20_34", "age_35_44",
    "preg_interval_4_11", "preg_interval_12_plus",
    "prior_ptb",
    "preg_type_single", "preg_type_multiple",
    "smoking_current",
    "insurance_medicaid", "insurance_private",
    "total_births", "total_ptb_births", "percent_ptb",
]

# ---------- Helpers ----------
def _y(val) -> bool:
    return str(val).strip().upper().startswith("Y")

def _to_percent(v) -> Optional[float]:
    try:
        s = str(v).strip()
        if s == "" or s.lower() == "nan":
            return None
        if s.endswith("%"):
            return float(s[:-1].replace(",", "").strip())
        f = float(s.replace(",", ""))
        return f * 100.0 if f <= 1.0 else f
    except Exception:
        return None

def _age_col_ptb(age: Optional[int]) -> Optional[str]:
    if age is None:
        return None
    a = int(age)
    if 15 <= a <= 19: return "age_15_19"
    if 20 <= a <= 34: return "age_20_34"
    if 35 <= a <= 44: return "age_35_44"
    return None

def _bucket_and_pos(value_percent: float) -> Tuple[str, float]:
    if value_percent < PTB_AVG_LOW:
        bucket = "below"
    elif value_percent <= PTB_AVG_HIGH:
        bucket = "average"
    else:
        bucket = "above"
    pos = max(0.0, min(100.0, (value_percent / ARROW_VMAX) * 100.0))
    return bucket, pos

def _is_one(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.lower()
    return s.isin({"1", "true", "t", "y", "yes"})

# ---------- Loader ----------
class _PTBTable:
    def __init__(self, path: str):
        self.path = path
        self.ok = False
        self.error: Optional[str] = None
        self.df: Optional[pd.DataFrame] = None
        self.headers: List[str] = []
        try:
            df = pd.read_excel(self.path)
            self.headers = list(df.columns)
            keep = [c for c in EXPECTED if c in df.columns]
            if keep:
                df = df[keep].copy()
            if "percent_ptb" not in df.columns:
                raise ValueError("Sheet is missing 'percent_ptb'.")
            self.df = df.reset_index(drop=True)
            self.ok = True
            log.info("[PTB] Loaded %s (rows=%d, cols=%d)", self.path, len(self.df), len(self.df.columns))
        except Exception as e:
            self.error = f"Failed to load PTB table from {self.path}: {e}"
            log.error(self.error)

_PTB = _PTBTable(XLSX_PATH)
log.info("[PTB DEBUG] BASE_DIR=%s", BASE_DIR)
log.info("[PTB DEBUG] XLSX_PATH=%s", XLSX_PATH)
log.info("[PTB DEBUG] EXISTS=%s", os.path.exists(XLSX_PATH))
log.info("[PTB DEBUG] TABLE_OK=%s ERROR=%s", _PTB.ok, _PTB.error)

# ---------- Public API ----------
def ptb_lookup(inputs: Dict[str, Any]) -> Dict[str, Any]:
    if not _PTB.ok or _PTB.df is None:
        return {"ok": False, "error": _PTB.error or "PTB table not available.", "percent": None,
                "bucket": "average", "position": 50.0, "matched_on": [], "matched_row": {},
                "available_fields": _PTB.headers if _PTB.headers else [], "risk_percent": None,
                "cohort_percent": PTB_GLOBAL_AVG}

    df = _PTB.df.copy()
    matched_on: List[str] = []

    # Age
    age_col = _age_col_ptb(inputs.get("age"))
    if age_col and age_col in df.columns:
        df = df[_is_one(df[age_col])]
        matched_on.append(f"Age={age_col}")

    # Prior PTB
    hx_ptb = str(inputs.get("history_ptb") or inputs.get("prior_ptb") or "No").strip().lower().startswith("y")
    if "prior_ptb" in df.columns:
        df = df[df["prior_ptb"].apply(_y) == hx_ptb]
        matched_on.append(f"HistoryPTB={'Yes' if hx_ptb else 'No'}")

    # Plurality
    preg_type = (inputs.get("pregnancy_type") or "").strip().lower()
    is_multiple = preg_type in ("twins", "triplets or more")
    if is_multiple and "preg_type_multiple" in df.columns:
        df = df[df["preg_type_multiple"].apply(_y)]
        matched_on.append("Plurality=Multiple")
    elif "preg_type_single" in df.columns:
        df = df[df["preg_type_single"].apply(_y)]
        matched_on.append("Plurality=Singleton")

    # Smoking
    smoking = (inputs.get("smoking_status") or "").strip().lower() == "current smoker"
    if "smoking_current" in df.columns:
        df = df[df["smoking_current"].apply(_y) == smoking]
        matched_on.append(f"Smoking={'Yes' if smoking else 'No'}")

    # Payer
    payer = (inputs.get("insurance_type") or "").strip().lower()
    if payer == "medicaid" and "insurance_medicaid" in df.columns:
        df = df[df["insurance_medicaid"].apply(_y)]
        matched_on.append("Payer=Medicaid")
    elif "insurance_private" in df.columns:
        df = df[df["insurance_private"].apply(_y)]
        matched_on.append("Payer=Private/Uninsured")

    # Inter-pregnancy interval
    interval = (inputs.get("preg_interval_code") or "").strip().lower()
    if interval == "4_11" and "preg_interval_4_11" in df.columns:
        df = df[df["preg_interval_4_11"].apply(_y)]
        matched_on.append("Interval=4_11")
    elif interval == "12_plus" and "preg_interval_12_plus" in df.columns:
        df = df[df["preg_interval_12_plus"].apply(_y)]
        matched_on.append("Interval=12_plus")

    if df.empty or "percent_ptb" not in df.columns:
        return {"ok": True, "error": None, "percent": None, "bucket": "average", "position": 50.0,
                "matched_on": matched_on, "matched_row": {}, "available_fields": _PTB.headers,
                "risk_percent": None, "cohort_percent": PTB_GLOBAL_AVG}

    chosen = df.iloc[0].to_dict()
    pct = _to_percent(chosen.get("percent_ptb"))
    if pct is None:
        return {"ok": True, "error": None, "percent": None, "bucket": "average", "position": 50.0,
                "matched_on": matched_on, "matched_row": chosen, "available_fields": _PTB.headers,
                "risk_percent": None, "cohort_percent": PTB_GLOBAL_AVG}

    bucket, position = _bucket_and_pos(float(pct))

    return {"ok": True, "error": None, "percent": round(float(pct), 2),
            "bucket": bucket, "position": round(float(position), 1),
            "matched_on": matched_on, "matched_row": chosen,
            "available_fields": _PTB.headers, "risk_percent": round(float(pct), 2),
            "cohort_percent": PTB_GLOBAL_AVG}

# Report status
if _PTB.ok:
    log.info("[PTB] READY: file=%s, rows=%d", XLSX_PATH, len(_PTB.df))
else:
    log.warning("[PTB] NOT READY: %s", _PTB.error)
