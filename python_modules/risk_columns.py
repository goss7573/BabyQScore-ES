# risk_columns.py
# Centralized column resolver + input normalization helpers
# Works with the standardized single-row headers you just finalized for PTB, GDM, GHT.
#
# Design goals:
# 1) No hard-coded surprises in per-model scripts — resolve columns once, in one place.
# 2) Be tolerant of minor header quirks (e.g., pandas de-dup suffixes), but prefer exact matches.
# 3) Provide small helpers to map user inputs (age, BMI, race, payer, plurality, smoking) into
#    the specific column that should be filtered in each outcome table.
#
# This file DOES NOT change your routes or risk_* modules yet — it's Step 2 only.

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
import math
import re

import pandas as pd


# -------------------------------
# Outcome identifiers & percent columns
# -------------------------------

_OUTCOME_PERCENT_COL = {
    "ptb": "percent_ptb",
    "gdm": "percent_gdm",
    "ght": "percent_ght",
}

# If present, these are useful but optional totals
_OUTCOME_TOTAL_COLS = {
    "ptb": ("total_births", "total_ptb_births"),
    "gdm": ("total_births", "total_gdm_births"),
    "ght": ("total_births", "total_ght_births"),
}


# -------------------------------
# Canonical group specifications per outcome
# (ordered as they should be applied)
# -------------------------------

@dataclass(frozen=True)
class GroupSpec:
    name: str
    columns_ordered: Tuple[str, ...]
    # If True, we expect to pick exactly one column from this set (one-hot style).
    # If False and len==1, it’s a boolean Y/N column. If False and len>1, treat as optional multi.
    pick_one: bool = True
    # If optional, silently skip if none of the listed columns are present.
    optional: bool = False


def _spec_for(outcome: str) -> Tuple[GroupSpec, ...]:
    """Return the ordered group specs for a given outcome."""
    o = outcome.lower().strip()
    if o == "ptb":
        return (
            GroupSpec("age_band", ("age_15_19", "age_20_34", "age_35_44"), pick_one=True),
            GroupSpec("preg_interval", ("preg_interval_4_11", "preg_interval_12_plus"), pick_one=True, optional=True),
            GroupSpec("race", ("race_white", "race_black", "race_asian"), pick_one=True),
            GroupSpec("prior_ptb", ("prior_ptb",), pick_one=False),
            GroupSpec("plurality", ("preg_type_single", "preg_type_multiple"), pick_one=True),
            GroupSpec("smoking", ("smoking_current",), pick_one=False),
            GroupSpec("payer", ("insurance_medicaid", "insurance_private"), pick_one=True),
        )
    if o == "gdm":
        return (
            GroupSpec("bmi_band", ("bmi_lt_18_5", "bmi_18_5_29_9", "bmi_30_34_9", "bmi_ge_35"), pick_one=True, optional=True),
            GroupSpec("age_band", ("age_15_19", "age_20_24", "age_25_29", "age_30_34", "age_35_44"), pick_one=True),
            # Race schema for GDM is (asian vs not-asian) in your standardized data
            GroupSpec("race", ("race_not_asian", "race_asian"), pick_one=True),
            GroupSpec("plurality", ("preg_type_single", "preg_type_multiple"), pick_one=True),
            GroupSpec("payer", ("insurance_medicaid", "insurance_private"), pick_one=True),
            GroupSpec("chronic_htn", ("chronic_htn",), pick_one=False, optional=True),
            GroupSpec("history_ptb", ("prior_ptb",), pick_one=False, optional=True),
            GroupSpec("prior_gdm", ("prior_gdm",), pick_one=False, optional=True),
            GroupSpec("fam_hx_diabetes", ("fam_hx_diabetes",), pick_one=False, optional=True),
        )
    if o == "ght":
        return (
            GroupSpec("bmi_band", ("bmi_lt_18_5", "bmi_18_5_29_9", "bmi_30_34_9", "bmi_ge_35"), pick_one=True, optional=True),
            GroupSpec("age_band", ("age_15_19", "age_20_24", "age_25_29", "age_30_34", "age_35_44"), pick_one=True),
            GroupSpec("race", ("race_white", "race_black", "race_asian"), pick_one=True),
            GroupSpec("plurality", ("preg_type_single", "preg_type_multiple"), pick_one=True),
            # MINIMAL CHANGE: make payer optional so missing insurance columns don't crash GHT lookups.
            GroupSpec("payer", ("insurance_medicaid", "insurance_private"), pick_one=True, optional=True),
            # Optional boolean predictors some versions may include:
            GroupSpec("pre_preg_diabetes", ("pre_preg_diabetes",), pick_one=False, optional=True),
            GroupSpec("chronic_htn", ("chronic_htn",), pick_one=False, optional=True),
        )
    raise ValueError(f"Unknown outcome: {outcome!r}")


# -------------------------------
# Column resolution utilities
# -------------------------------

_DEDUP_SUFFIX = re.compile(r"\.\d+$")  # e.g., "race_white.1"


def _normalize_header(name: str) -> str:
    """Strip pandas-style de-dup suffixes; keep exact base header."""
    return _DEDUP_SUFFIX.sub("", name.strip())


def _headers_set(df: pd.DataFrame) -> Dict[str, str]:
    """
    Build a map of normalized_header -> actual_header (first occurrence wins).
    This lets us tolerate pandas de-dup like 'col', 'col.1', 'col.2' while preferring 'col'.
    """
    seen: Dict[str, str] = {}
    for c in df.columns:
        base = _normalize_header(str(c))
        if base not in seen:
            seen[base] = str(c)
    return seen


class ColumnResolver:
    """
    Resolve standardized headers in a given DataFrame for a specific outcome (ptb/gdm/ght).
    Provides:
      - groups: the ordered GroupSpec sequence for this outcome
      - headers: normalized->actual header mapping
      - percent_col(): the percent column name for this outcome if present
      - ensure_percent_columns(): computes __pct_frac (0..1) and __pct_float (0..100)
      - resolve_group(name): returns the existing columns (actual header names) for that group
    """
    def __init__(self, df: pd.DataFrame, outcome: Optional[str] = None):
        self.df = df
        self.headers = _headers_set(df)
        self.outcome = self._infer_outcome(df) if outcome is None else outcome.lower().strip()
        self.groups = _spec_for(self.outcome)

    # ---------- outcome detection ----------
    @staticmethod
    def _infer_outcome(df: pd.DataFrame) -> str:
        cols = { _normalize_header(c).lower() for c in df.columns }
        # Prefer explicit, outcome-specific percent column
        if "percent_ptb" in cols:
            return "ptb"
        if "percent_gdm" in cols:
            return "gdm"
        if "percent_ght" in cols:
            return "ght"
        raise ValueError("Cannot infer outcome — expected one of percent_ptb/percent_gdm/percent_ght headers.")

    # ---------- percent handling ----------
    def percent_col(self) -> Optional[str]:
        name = _OUTCOME_PERCENT_COL.get(self.outcome)
        if name and name in self.headers:
            return self.headers[name]
        return None

    def ensure_percent_columns(self) -> None:
        """
        Create:
          - __pct_frac  (0..1)
          - __pct_float (0..100)
        Accepts strings like '1.43%' or numbers like 0.0143 or 1.43.
        """
        pcol = self.percent_col()
        if not pcol:
            return
        series = self.df[pcol]

        def to_frac(x: Any) -> Optional[float]:
            if x is None:
                return None
            s = str(x).strip()
            # Handle "1.43%" style
            if s.endswith("%"):
                try:
                    return float(s[:-1]) / 100.0
                except Exception:
                    return None
            # Handle numeric
            try:
                v = float(s)
            except Exception:
                return None
            # Assume <=1.0 is already a fraction; >1.0 is a percent value
            if v <= 1.0:
                return v
            return v / 100.0

        frac = series.map(to_frac)
        self.df["__pct_frac"] = frac
        self.df["__pct_float"] = frac * 100.0

    # ---------- group resolution ----------
    def resolve_group(self, group_name: str) -> List[str]:
        """
        Return actual header names for the requested group, preserving the defined order,
        but only for columns that truly exist in the DataFrame.
        """
        spec = next((g for g in self.groups if g.name == group_name), None)
        if spec is None:
            raise KeyError(f"Unknown group {group_name!r} for outcome {self.outcome}")

        cols: List[str] = []
        for base in spec.columns_ordered:
            actual = self.headers.get(base)
            if actual in self.df.columns:
                cols.append(actual)

        if not cols and not spec.optional:
            # Provide a clear error for debugging header issues
            needed = ", ".join(spec.columns_ordered)
            present = ", ".join(self.df.columns.astype(str))
            raise KeyError(f"Required group {group_name!r} is missing all expected columns: [{needed}]. Present: [{present}]")

        return cols

    # Convenience: return (first_existing_column or None)
    def first_of(self, group_name: str) -> Optional[str]:
        cols = self.resolve_group(group_name)
        return cols[0] if cols else None


# -------------------------------
# Input normalization helpers
# (Use them in risk_* modules if helpful; light wrappers, no I/O)
# -------------------------------

# ---- BMI helpers ----
def bmi_from_imperial(weight_lb: Optional[float], height_feet: Optional[int], height_inches: Optional[int]) -> Optional[float]:
    try:
        if weight_lb is None or height_feet is None or height_inches is None:
            return None
        inches = int(height_feet) * 12 + int(height_inches)
        if inches <= 0:
            return None
        return 703.0 * float(weight_lb) / (inches ** 2)
    except Exception:
        return None


def bmi_band_gdm_ght(bmi: Optional[float]) -> Optional[str]:
    """Return the standardized BMI band label used to pick a GDM/GHT column."""
    if bmi is None:
        return None
    try:
        b = float(bmi)
    except Exception:
        return None
    if b < 18.5:
        return "bmi_lt_18_5"
    if 18.5 <= b <= 29.9:
        return "bmi_18_5_29_9"
    if 30.0 <= b <= 34.9:
        return "bmi_30_34_9"
    return "bmi_ge_35"


# ---- Age helpers ----
def age_band_ptb(age: Optional[int]) -> Optional[str]:
    if age is None:
        return None
    a = int(age)
    if 15 <= a <= 19:
        return "age_15_19"
    if 20 <= a <= 34:
        return "age_20_34"
    if 35 <= a <= 44:
        return "age_35_44"
    return None  # out of scope -> average fallback


def age_band_gdm_ght(age: Optional[int]) -> Optional[str]:
    if age is None:
        return None
    a = int(age)
    if 15 <= a <= 19:
        return "age_15_19"
    if 20 <= a <= 24:
        return "age_20_24"
    if 25 <= a <= 29:
        return "age_25_29"
    if 30 <= a <= 34:
        return "age_30_34"
    if 35 <= a <= 44:
        return "age_35_44"
    return None


# ---- Race helpers ----
def race_column_for(outcome: str, race_input: str) -> str:
    """
    Map user-entered race/ethnicity to the appropriate column name for the outcome.
    For PTB/GHT: white/black/asian (others -> white).
    For GDM: asian vs not-asian.
    """
    r = (race_input or "").strip().lower()
    if outcome.lower() == "gdm":
        return "race_asian" if r == "asian" else "race_not_asian"
    # PTB/GHT:
    if r == "black":
        return "race_black"
    if r == "asian":
        return "race_asian"
    # Hispanic, Native American, Other → treat as "non-Black, non-Asian" i.e., white
    return "race_white"


# ---- Payer helpers ----
def payer_columns_for(insurance_type: str) -> str:
    """
    Medicaid -> insurance_medicaid
    Private or Uninsured -> insurance_private
    """
    v = (insurance_type or "").strip().lower()
    return "insurance_medicaid" if v == "medicaid" else "insurance_private"


# ---- Plurality helpers ----
def plurality_columns_for(pregnancy_type: str) -> str:
    """
    Singleton -> preg_type_single
    Twins or Triplets+ -> preg_type_multiple
    """
    v = (pregnancy_type or "").strip().lower()
    if v in ("twins", "triplets or more"):
        return "preg_type_multiple"
    return "preg_type_single"


# ---- Smoking helper (PTB) ----
def smoking_current_flag(smoking_status: str) -> bool:
    """Return True if current smoker; False for non-smoker/former."""
    return (smoking_status or "").strip().lower() == "current smoker"


# ---- Interval helper (PTB) ----
def interval_column_for(code: Optional[str]) -> Optional[str]:
    """
    Form uses: '4_11' (Yes, within 13 months) or '12_plus' (No).
    Returns the PTB table column name to filter, or None if blank.
    """
    if not code:
        return None
    v = code.strip().lower()
    if v == "4_11":
        return "preg_interval_4_11"
    if v == "12_plus":
        return "preg_interval_12_plus"
    return None


# -------------------------------
# Small convenience for risk_* modules
# -------------------------------

def prepare_table(df: pd.DataFrame, outcome: str) -> ColumnResolver:
    """
    Wrap common prep:
      - Create resolver
      - Ensure percent columns (__pct_frac, __pct_float)
      - Return resolver ready for filtering/sorting
    """
    resolver = ColumnResolver(df, outcome=outcome)
    resolver.ensure_percent_columns()
    return resolver
