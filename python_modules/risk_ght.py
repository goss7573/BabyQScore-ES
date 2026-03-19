# risk_ght.py
# Pregnancy-Related Hypertension (GHT) predictor
# Returns your group’s estimated risk (%), with cutoffs:
#   - Below average (green): <6%
#   - Average (yellow): 6–9%
#   - Above average (red): >9%

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple, Iterable

import pandas as pd

# Central resolver if present
try:
    import risk_columns as rc  # type: ignore
except Exception:
    rc = None

# ------------ Paths ------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_REL = os.path.join("data", "GHT_Version_fixed_Row_Restored.xlsx")
_XLSX_PATH = os.environ.get("GHT_XLSX_PATH") or os.path.join(BASE_DIR, DEFAULT_REL)

# Cache
_TABLE_DF: Optional[pd.DataFrame] = None

# ------------ IO ------------

def _read_table(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"GHT reference file not found: {path}")
    df = pd.read_excel(path)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _get_table() -> pd.DataFrame:
    global _TABLE_DF
    if _TABLE_DF is None:
        _TABLE_DF = _read_table(_XLSX_PATH)
        print("[GHT DEBUG] BASE_DIR =", BASE_DIR)
        print("[GHT DEBUG] XLSX_PATH =", _XLSX_PATH)
        print("[GHT DEBUG] EXISTS =", os.path.exists(_XLSX_PATH))
        print("[GHT DEBUG] TABLE_LOADED =", _TABLE_DF is not None)        
    return _TABLE_DF

# ------------ helpers ------------

def _first_matching(colnames: Iterable[str], *candidates: str) -> Optional[str]:
    lower = {c.lower(): c for c in colnames}
    for cand in candidates:
        k = cand.lower()
        if k in lower:
            return lower[k]
    for name in colnames:
        ln = name.lower()
        if any(c.lower() in ln for c in candidates):
            return name
    return None

def _to_yes_no(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip().lower()
    if s in {"1", "y", "yes", "true", "t"}: return "Yes"
    if s in {"0", "n", "no", "false", "f"}: return "No"
    return None

def _truthy(value: Any) -> bool:
    s = str(value).strip().lower()
    return s in {"1", "y", "yes", "true", "t"}

def _falsy(value: Any) -> bool:
    s = str(value).strip().lower()
    return s in {"0", "n", "no", "false", "f"}

def _numeric(x: Any) -> Optional[float]:
    try:
        if pd.isna(x):  # type: ignore
            return None
    except Exception:
        pass
    try:
        return float(str(x).replace("%", "").strip())
    except Exception:
        return None

def _pick_risk_column(df: pd.DataFrame) -> Optional[str]:
    candidates = [
        "percent_ght", "GHT Risk %", "Risk %", "Risk%", "risk_percent",
        "pregnancy-related hypertension risk %", "hypertension risk %", "risk"
    ]
    col = _first_matching(df.columns, *candidates)
    if col:
        return col
    for c in df.columns:
        if "risk" in c.lower() or "%" in c:
            vals = pd.to_numeric(df[c].astype(str).str.replace("%", "", regex=False), errors="coerce")
            if vals.notna().any():
                return c
    return None

# --- local band helpers ---
def _age_band(age: Optional[int]) -> Optional[str]:
    if rc and hasattr(rc, "age_band_gdm_ght"):
        return rc.age_band_gdm_ght(age)  # type: ignore
    if age is None: return None
    a = int(age)
    if 15 <= a <= 19: return "age_15_19"
    if 20 <= a <= 24: return "age_20_24"
    if 25 <= a <= 29: return "age_25_29"
    if 30 <= a <= 34: return "age_30_34"
    if 35 <= a <= 44: return "age_35_44"
    return None

def _bmi_band(bmi: Optional[float]) -> Optional[str]:
    if rc and hasattr(rc, "bmi_band_gdm_ght"):
        return rc.bmi_band_gdm_ght(bmi)  # type: ignore
    if bmi is None: return None
    try:
        b = float(bmi)
    except Exception:
        return None
    if b < 18.5: return "bmi_lt_18_5"
    if 18.5 <= b <= 29.9: return "bmi_18_5_29_9"
    if 30.0 <= b <= 34.9: return "bmi_30_34_9"
    return "bmi_ge_35"

def _race_column(race_input: Optional[str]) -> Optional[str]:
    if not race_input:
        return None
    if rc and hasattr(rc, "race_column_for"):
        return rc.race_column_for("ght", race_input)  # type: ignore
    r = str(race_input or "").strip().lower()
    if r == "black": return "race_black"
    if r == "asian": return "race_asian"
    return "race_white"

def _is_one(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.lower()
    return s.isin({"1", "true", "t", "y", "yes"})

def _is_zero(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.lower()
    return s.isin({"0", "false", "f", "n", "no"})

# ------------ Public API ------------

def ght_lookup(user_inputs: Dict[str, Any]) -> Dict[str, Any]:
    try:
        df = _get_table()
    except Exception as e:
        return {"ok": False, "risk_percent": None, "matched_on": [], "matched_row": {}, "available_fields": [], "error": str(e)}

    # ---------- Required backbone: BMI + AGE ----------
    age_val = user_inputs.get("age")
    bmi_val = user_inputs.get("bmi")
    race_val = user_inputs.get("race")
    ppd_val = user_inputs.get("pre_preg_diabetes")
    prior_val = user_inputs.get("prior_births")

    age_col = _age_band(age_val)
    bmi_col = _bmi_band(bmi_val)
    race_col = _race_column(race_val)

    matched_on: List[str] = []
    work = df.copy()

    if bmi_col and bmi_col in work.columns:
        work = work[_is_one(work[bmi_col])]
        matched_on.append(f"bmi_band={bmi_col}")
    if age_col and age_col in work.columns:
        work = work[_is_one(work[age_col])]
        matched_on.append(f"age_band={age_col}")

    if race_col and race_col in work.columns and not work.empty:
        narrowed = work[_is_one(work[race_col])]
        if not narrowed.empty:
            work = narrowed
            matched_on.append(f"race={race_col}")

    if _truthy(ppd_val) and ("pre_preg_diabetes" in work.columns) and not work.empty:
        narrowed = work[_is_one(work["pre_preg_diabetes"])]
        if not narrowed.empty:
            work = narrowed
            matched_on.append("pre_preg_diabetes=1")

    if "prior_births" in work.columns and prior_val is not None and not work.empty:
        if _truthy(prior_val):
            narrowed = work[_is_one(work["prior_births"])]
            if not narrowed.empty:
                work = narrowed
                matched_on.append("prior_births=1")
        elif _falsy(prior_val):
            narrowed = work[_is_zero(work["prior_births"])]
            if not narrowed.empty:
                work = narrowed
                matched_on.append("prior_births=0")

    if work.empty:
        if bmi_col and (bmi_col in df.columns):
            bmi_only = df[_is_one(df[bmi_col])]
            if not bmi_only.empty:
                work = bmi_only
                matched_on = [f"bmi_band={bmi_col}"]
        if work.empty and age_col and (age_col in df.columns):
            age_only = df[_is_one(df[age_col])]
            if not age_only.empty:
                work = age_only
                matched_on = [f"age_band={age_col}"]
        if work.empty:
            work = df.copy()
            matched_on = ["fallback=cohort_average"]

    matched_row = work.iloc[0].to_dict()

    # ---------- Risk extraction ----------
    risk_col = _pick_risk_column(df)
    err: Optional[str] = None
    risk_val: Optional[float] = None

    if risk_col:
        raw = matched_row.get(risk_col)
        v = _numeric(raw)
        if v is not None:
            had_percent = isinstance(raw, str) and "%" in raw
            if not had_percent and v <= 1:
                v = v * 100.0
            risk_val = round(float(v), 2)
        if risk_val is None:
            series = pd.to_numeric(df[risk_col].astype(str).str.replace("%", "", regex=False), errors="coerce")
            if series.notna().any():
                avg = float(series.mean())
                if (series.dropna() <= 1.0).mean() > 0.8:
                    avg *= 100.0
                risk_val = round(avg, 2)
            else:
                err = f"Risk column '{risk_col}' has no numeric values."
    else:
        err = "Could not identify a risk column in the GHT table."

    # ---------- Buckets & arrow position ----------
    bucket = "average"
    position = 50.0
    if risk_val is not None:
        if risk_val < 6.0:
            bucket = "below"
        elif risk_val <= 9.0:
            bucket = "average"
        else:
            bucket = "above"
        vmax = 20.0  # scaling assumption
        position = max(0.0, min(100.0, (risk_val / vmax) * 100.0))

    return {
        "ok": risk_val is not None,
        "risk_percent": risk_val,
        "bucket": bucket,
        "position": position,
        "matched_on": matched_on,
        "matched_row": matched_row,
        "available_fields": list(df.columns),
        "error": err,
        "notes": ""
    }

if __name__ == "__main__":
    demo = {
        "age": 39,
        "bmi": 37.5,
        "race": "Black",
        "pre_preg_diabetes": "Yes",
        "prior_births": 1,
    }
    print(ght_lookup(demo))
