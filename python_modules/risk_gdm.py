# risk_gdm.py
# Deterministic lookup for Gestational Diabetes (GDM) against the
# single-row-header workbook with standardized column names.

from __future__ import annotations

import os
import logging
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd

log = logging.getLogger("risk_gdm")
if not log.handlers:
    logging.basicConfig(level=logging.INFO)

# ---------- Bands / anchors ----------
GDM_GLOBAL_AVG = 14.0           # %
GDM_AVG_LOW = 12.5               # %
GDM_AVG_HIGH = 15.5              # %
ARROW_VMAX = 20.0                # % → 100 scale

# ---------- File path ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_REL = os.path.join("data", "GDM_Version_fixed_Row_Restored.xlsx")
XLSX_PATH = os.environ.get("GDM_XLSX_PATH") or os.path.join(BASE_DIR, DEFAULT_REL)

# ---------- Expected columns in the standardized GDM sheet ----------
EXPECTED = [
    # BMI
    "bmi_lt_18_5", "bmi_18_5_29_9", "bmi_30_34_9", "bmi_ge_35",
    # Age
    "age_15_19", "age_20_24", "age_25_29", "age_30_34", "age_35_44",
    # Race
    "race_not_asian", "race_asian",
    # Medical/obstetric hx
    "chronic_htn", "prior_ptb",
    # Plurality
    "preg_type_single", "preg_type_multiple",
    # Payer
    "insurance_medicaid", "insurance_private",
    # Outputs
    "total_births", "total_gdm_births", "percent_gdm",
]

# ---------- Helpers ----------
def _y(val) -> bool:
    """Treat any string starting with 'Y' (case-insensitive) as True."""
    return str(val).strip().upper().startswith("Y")

def _to_percent(v) -> Optional[float]:
    """
    Convert '1.43%' or 0.0143 or 1.43 -> 1.43 (float).
    Returns None if cannot parse.
    """
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

def _bmi(weight_lb: Optional[float], h_ft: Optional[int], h_in: Optional[int]) -> Optional[float]:
    try:
        if weight_lb is None or h_ft is None or h_in is None:
            return None
        inches = int(h_ft) * 12 + int(h_in)
        if inches <= 0:
            return None
        return 703.0 * float(weight_lb) / (inches ** 2)
    except Exception:
        return None

def _which_bmi_col(bmi: Optional[float]) -> Optional[str]:
    """Map BMI value to exactly one BMI column name."""
    if bmi is None:
        return None
    if bmi < 18.5:
        return "bmi_lt_18_5"
    if 18.5 <= bmi <= 29.9:
        return "bmi_18_5_29_9"
    if 30.0 <= bmi <= 34.9:
        return "bmi_30_34_9"
    if bmi >= 35.0:
        return "bmi_ge_35"
    return None

def _which_age_col(age: Optional[int]) -> Optional[str]:
    if age is None:
        return None
    if 15 <= age <= 19:
        return "age_15_19"
    if 20 <= age <= 24:
        return "age_20_24"
    if 25 <= age <= 29:
        return "age_25_29"
    if 30 <= age <= 34:
        return "age_30_34"
    if 35 <= age <= 44:
        return "age_35_44"
    return None

def _bucket_and_pos(value_percent: float) -> Tuple[str, float]:
    if value_percent < GDM_AVG_LOW:
        bucket = "below"
    elif value_percent <= GDM_AVG_HIGH:
        bucket = "average"
    else:
        bucket = "above"
    pos = max(0.0, min(100.0, (value_percent / ARROW_VMAX) * 100.0))
    return bucket, pos

# ---------- Loader ----------
class _GDMTable:
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
            if "percent_gdm" not in df.columns:
                raise ValueError("Sheet is missing 'percent_gdm'.")
            self.df = df.reset_index(drop=True)
            self.ok = True
            log.info("[GDM] Loaded %s (rows=%d, cols=%d)", self.path, len(self.df), len(self.df.columns))
        except Exception as e:
            self.error = f"Failed to load GDM table from {self.path}: {e}"
            log.error(self.error)

_GDM = _GDMTable(XLSX_PATH)
log.info("[GDM DEBUG] BASE_DIR=%s", BASE_DIR)
log.info("[GDM DEBUG] XLSX_PATH=%s", XLSX_PATH)
log.info("[GDM DEBUG] EXISTS=%s", os.path.exists(XLSX_PATH))
log.info("[GDM DEBUG] TABLE_OK=%s ERROR=%s", _GDM.ok, _GDM.error)

# ---------- Public API ----------
def gdm_lookup(inputs: Dict[str, Any]) -> Dict[str, Any]:
    if not _GDM.ok or _GDM.df is None:
        return {
            "ok": False,
            "error": _GDM.error or "GDM table not available.",
            "percent": None,
            "bucket": "average",
            "position": 50.0,
            "matched_on": [],
            "matched_row": {},
            "available_fields": _GDM.headers if _GDM.headers else [],
            "risk_percent": None,
            "cohort_percent": GDM_GLOBAL_AVG,
        }

    df = _GDM.df.copy()
    matched_on: List[str] = []

    # BMI
    bmi_val = _bmi(inputs.get("weight_pre"), inputs.get("height_feet"), inputs.get("height_inches"))
    bmi_col = _which_bmi_col(bmi_val)
    if bmi_col and bmi_col in df.columns:
        df = df[df[bmi_col].apply(_y)]
        matched_on.append(f"BMI={bmi_col.replace('bmi_','').replace('_',' ')}")

    # Age
    try:
        age = int(inputs.get("age")) if inputs.get("age") not in (None, "") else None
    except Exception:
        age = None
    age_col = _which_age_col(age)
    if age_col and age_col in df.columns:
        df = df[df[age_col].apply(_y)]
        matched_on.append(f"Age={age_col.replace('age_','').replace('_','-')}")

    # Race (asian vs not)
    race_in = (inputs.get("race") or "").strip().lower()
    is_asian = (race_in == "asian")
    race_col = "race_asian" if is_asian else "race_not_asian"
    if race_col in df.columns:
        df = df[df[race_col].apply(_y)]
        matched_on.append("Race=Asian" if is_asian else "Race=not Asian")

    # Chronic HTN
    c_htn = str(inputs.get("chronic_htn") or "No").lower().startswith("y")
    if "chronic_htn" in df.columns:
        df = df[df["chronic_htn"].apply(_y) == c_htn]
        matched_on.append(f"ChronicHTN={'Yes' if c_htn else 'No'}")

    # History PTB
    hx_ptb = str(inputs.get("history_ptb") or "No").lower().startswith("y")
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

    # Payer
    payer = (inputs.get("insurance_type") or "").strip().lower()
    if payer == "medicaid" and "insurance_medicaid" in df.columns:
        df = df[df["insurance_medicaid"].apply(_y)]
        matched_on.append("Payer=Medicaid")
    elif "insurance_private" in df.columns:
        df = df[df["insurance_private"].apply(_y)]
        matched_on.append("Payer=Private/Uninsured")

    if df.empty or "percent_gdm" not in df.columns:
        return {
            "ok": True,
            "error": None,
            "percent": None,
            "bucket": "average",
            "position": 50.0,
            "matched_on": matched_on,
            "matched_row": {},
            "available_fields": _GDM.headers,
            "risk_percent": None,
            "cohort_percent": GDM_GLOBAL_AVG,
        }

    chosen = df.iloc[0].to_dict()
    pct = _to_percent(chosen.get("percent_gdm"))
    if pct is None:
        return {
            "ok": True,
            "error": None,
            "percent": None,
            "bucket": "average",
            "position": 50.0,
            "matched_on": matched_on,
            "matched_row": chosen,
            "available_fields": _GDM.headers,
            "risk_percent": None,
            "cohort_percent": GDM_GLOBAL_AVG,
        }

    bucket, position = _bucket_and_pos(float(pct))

    return {
        "ok": True,
        "error": None,
        "percent": round(float(pct), 2),
        "bucket": bucket,
        "position": round(float(position), 1),
        "matched_on": matched_on,
        "matched_row": chosen,
        "available_fields": _GDM.headers,
        "risk_percent": round(float(pct), 2),
        "cohort_percent": GDM_GLOBAL_AVG,
    }

# Report status at import
if _GDM.ok:
    log.info("[GDM] READY: file=%s, rows=%d", XLSX_PATH, len(_GDM.df))
else:
    log.warning("[GDM] NOT READY: %s", _GDM.error)
