# -*- coding: utf-8 -*-

"""
cleaner.py — Python 3.8–compatible data cleaning pipeline for listings.csv

- Preserves description_fr.
- Zip code normalization to strict 5 digits.
- Nullable Int64 for: floor, room_count, balcony_count, terrace_count, build_year.
- Modular steps with timing/logging + fail-fast.
"""

import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Protocol

import numpy as np
import pandas as pd

# =========================
# Decorators
# =========================

def timeit(step_name: str):
    def deco(func):
        def wrapper(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
            t0 = time.perf_counter()
            out = func(self, df, ctx)
            ms = (time.perf_counter() - t0) * 1000
            print(f" --  {step_name} took {ms:.2f} ms")
            return out
        return wrapper
    return deco

def log_step(step_name: str):
    def deco(func):
        def wrapper(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
            before = df.shape
            out = func(self, df, ctx)
            if out is None:
                raise ValueError(f"{self.__class__.__name__}.{func.__name__} (step='{step_name}') returned None; expected DataFrame.")
            after = out.shape
            ctx.setdefault("log", []).append(f"{step_name}: {before} -> {after}")
            return out
        return wrapper
    return deco

def safe_step(func):
    def wrapper(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        try:
            return func(self, df, ctx)
        except Exception as e:
            ctx.setdefault("errors", []).append(f"{self.__class__.__name__}: {type(e).__name__}: {e}")
            raise
    return wrapper

def validate_columns(required: Iterable[str]):
    req = list(required)
    def deco(func):
        def wrapper(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
            missing = [c for c in req if c not in df.columns]
            if missing:
                raise ValueError(f"{self.__class__.__name__}: missing required columns: {missing}")
            return func(self, df, ctx)
        return wrapper
    return deco

# =========================
# Policy / Interfaces
# =========================

@dataclass
class CleaningPolicy:
    drop_ultra_sparse_threshold: float = 0.95
    preserve_identifiers: List[str] = field(default_factory=lambda: ["listing_id"])
    # Always keep these columns
    preserve_always: List[str] = field(default_factory=lambda: ["terrace_area", "description_fr"])
    enum_like_columns: List[str] = field(default_factory=lambda: ["transaction_type", "item_type", "item_subtype"])
    boolean_prefixes: List[str] = field(default_factory=lambda: ["is_", "has_"])
    non_negative_keywords: List[str] = field(default_factory=lambda: ["price", "area", "count", "room", "floor", "year", "terrace"])
    small_cardinality_cast_threshold: int = 10000
    # Do NOT drop description_fr
    drop_by_name: List[str] = field(default_factory=list)
    # Never impute these object columns
    no_impute_objects: List[str] = field(default_factory=lambda: ["description_fr"])

class CleanerStep(Protocol):
    name: str
    def apply(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame: ...

# =========================
# Helpers
# =========================

TRUTHY = {"true", "t", "yes", "y", "1", "vrai", "oui"}
FALSY  = {"false", "f", "no", "n", "0", "faux", "non"}

def normalize_bool_series(s: pd.Series) -> pd.Series:
    ss = s.astype(str).str.strip().str.lower()
    out = np.where(ss.isin(TRUTHY), True, np.where(ss.isin(FALSY), False, np.nan))
    return pd.Series(out, index=s.index, dtype="object")

def strip_namespace(x: Any) -> Any:
    if isinstance(x, str) and "." in x:
        return x.split(".")[-1].strip()
    return x

_NUMERIC_JUNK_RE = re.compile(r"[^0-9\.,]+")

def _to_num_generic(x: Any) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    if "," in s and "." in s:
        s = s.replace(",", "")  # drop thousands sep
    else:
        s = s.replace(",", ".")  # accept comma decimal
    try:
        return float(s)
    except Exception:
        return None

def _to_int_generic(x: Any) -> Optional[int]:
    v = _to_num_generic(x)
    if v is None:
        return None
    try:
        return int(round(v))
    except Exception:
        return None

def parse_area_like(x: Any) -> Optional[float]:
    if x is None:
        return None
    raw = str(x).strip()
    if raw == "":
        return None
    s_low = raw.lower().replace(" ", "")
    is_sqft = any(token in s_low for token in ("sqft", "sq.ft", "ft2", "ft^2"))
    num_str = _NUMERIC_JUNK_RE.sub("", raw)
    if num_str == "":
        return None
    if "," in num_str and "." in num_str:
        num_str = num_str.replace(",", "")
    else:
        num_str = num_str.replace(",", ".")
    try:
        val = float(num_str)
    except Exception:
        return None
    if is_sqft:
        return round(val * 0.092903, 6)
    return val

# ZIP normalization to strict 5 digits
_zip5 = re.compile(r"\d{5}")

def _normalize_zipcode_value(x: Any) -> Optional[str]:
    if pd.isna(x):
        return np.nan
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    m = _zip5.search(s)  # any 5-digit run
    if m:
        return m.group(0)
    digits = re.sub(r"\D", "", s)
    if len(digits) == 5:
        return digits
    if len(digits) == 4:
        return digits.zfill(5)  # 7501 -> 07501
    return np.nan

def normalize_zipcode_series(s: pd.Series) -> pd.Series:
    return s.apply(_normalize_zipcode_value).astype("object")

# =========================
# Steps
# =========================

class StandardizeColumns:
    name = "standardize_columns"

    @timeit(name)
    @log_step(name)
    @safe_step
    def apply(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        df = df.copy()
        df.columns = [c.strip() for c in df.columns]
        return df

class RemoveExactDuplicates:
    name = "remove_exact_duplicates"

    @timeit(name)
    @log_step(name)
    @safe_step
    def apply(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        dups = df.duplicated().sum()
        if dups:
            ctx.setdefault("details", {}).setdefault(self.name, {})["removed"] = int(dups)
            return df.drop_duplicates()
        return df

class DeduplicateBusinessKey:
    name = "deduplicate_business_key"

    def __init__(self, key: str = "listing_id"):
        self.key = key

    @timeit(name)
    @log_step(name)
    @safe_step
    def apply(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        if self.key not in df.columns:
            return df
        tmp = df.copy()
        if "change_date" in tmp.columns:
            tmp["__cdt"] = pd.to_datetime(tmp["change_date"], errors="coerce")
        else:
            tmp["__cdt"] = pd.NaT
        if "start_date" in tmp.columns:
            tmp["__sdt"] = pd.to_datetime(tmp["start_date"], errors="coerce")
        else:
            tmp["__sdt"] = pd.NaT
        before = tmp.shape[0]
        tmp = tmp.sort_values([self.key, "__cdt", "__sdt"]).drop_duplicates(subset=[self.key], keep="last")
        ctx.setdefault("details", {}).setdefault(self.name, {})["removed"] = int(before - tmp.shape[0])
        return tmp.drop(columns=["__cdt", "__sdt"], errors="ignore")

class ConvertDtypes:
    """Dates→datetime; zipcode→5-digit string; boolean-like→bool/NaN; enums strip namespaces."""
    name = "convert_dtypes"

    def __init__(self, policy: CleaningPolicy):
        self.policy = policy

    @timeit(name)
    @log_step(name)
    @safe_step
    def apply(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        df = df.copy()

        # Date-like columns
        for c in [col for col in df.columns if "date" in col.lower()]:
            df[c] = pd.to_datetime(df[c], errors="coerce")

        # zipcode -> strict 5 digits
        if "zipcode" in df.columns:
            df["zipcode"] = df["zipcode"].astype(str).str.strip()
            df["zipcode"] = normalize_zipcode_series(df["zipcode"])

        # boolean-like by prefixes
        for c in df.columns:
            if any(c.lower().startswith(p) for p in self.policy.boolean_prefixes):
                df[c] = normalize_bool_series(df[c])

        # enum-like columns
        for col in self.policy.enum_like_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().apply(strip_namespace)

        return df

class CastNumericLike:
    """
    Cast numeric-like columns.
    - terrace_area via unit-aware parse
    - enforce Int64 for: floor, room_count, balcony_count, terrace_count, build_year
    """
    name = "cast_numeric_like"

    INT_COLS = ("floor", "room_count", "balcony_count", "terrace_count", "build_year")

    def __init__(self, include: Optional[List[str]] = None):
        self.include = include or [
            "price", "area", "site_area", "terrace_area",
            "floor", "room_count", "balcony_count", "terrace_count",
            "build_year"
        ]

    @timeit(name)
    @log_step(name)
    @safe_step
    def apply(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        df = df.copy()

        if "terrace_area" in df.columns:
            df["terrace_area"] = df["terrace_area"].apply(parse_area_like)

        for col in self.include:
            if col not in df.columns or col == "terrace_area":
                continue
            if col in self.INT_COLS:
                df[col] = df[col].apply(_to_int_generic).astype("Int64")
            else:
                df[col] = df[col].apply(_to_num_generic)

        return df

class NumericSanity:
    """Non-negative and plausibility checks."""
    name = "numeric_sanity"

    def __init__(self, policy: CleaningPolicy):
        self.policy = policy

    @timeit(name)
    @log_step(name)
    @safe_step
    def apply(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        df = df.copy()

        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]) and any(k in col.lower() for k in self.policy.non_negative_keywords):
                neg = int(df[col].lt(0).sum())
                if neg:
                    df.loc[df[col] < 0, col] = pd.NA if pd.api.types.is_integer_dtype(df[col]) else np.nan
                    ctx.setdefault("details", {}).setdefault(self.name, {})[col] = f"negatives->NA: {neg}"

        if "build_year" in df.columns:
            mask_bad = (~df["build_year"].isna()) & ((df["build_year"] < 1800) | (df["build_year"] > 2100))
            bad = int(mask_bad.sum())
            if bad:
                df.loc[mask_bad, "build_year"] = pd.NA
                ctx.setdefault("details", {}).setdefault(self.name, {})["build_year"] = f"out_of_range->NA: {bad}"

        return df

class DropColumns:
    """Drop irrelevant columns and ultra-sparse ones (> threshold missingness)."""
    name = "drop_columns"

    def __init__(self, to_drop: Optional[List[str]] = None, drop_ultra_sparse_threshold: float = 0.95, preserve: Optional[List[str]] = None):
        self.to_drop = to_drop or []
        self.drop_ultra_sparse_threshold = drop_ultra_sparse_threshold
        self.preserve = set(preserve or [])

    @timeit(name)
    @log_step(name)
    @safe_step
    def apply(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        df = df.copy()
        na_pct = df.isna().mean()
        ultra_sparse = na_pct[na_pct > self.drop_ultra_sparse_threshold].index.tolist()
        final_drop = [c for c in set(self.to_drop + ultra_sparse) if c not in self.preserve]
        if final_drop:
            ctx.setdefault("details", {}).setdefault(self.name, {})["dropped"] = final_drop
            df = df.drop(columns=final_drop, errors="ignore")
        return df

class Impute:
    """Impute numerics; keep integer cols as Int64 after impute; NEVER impute description_fr."""
    name = "impute"

    def __init__(self, policy: CleaningPolicy):
        self.policy = policy
        self._int_cols = ("floor", "room_count", "balcony_count", "terrace_count", "build_year")

    @timeit(name)
    @log_step(name)
    @safe_step
    def apply(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        df = df.copy()

        # Integers: median (rounded) -> Int64
        for col in [c for c in self._int_cols if c in df.columns]:
            na = int(df[col].isna().sum())
            if na > 0:
                med_series = pd.to_numeric(df[col], errors="coerce").dropna()
                if not med_series.empty:
                    v = int(round(med_series.median()))
                    df[col] = df[col].fillna(v).astype("Int64")
                    ctx.setdefault("details", {}).setdefault(self.name, {})[col] = f"int_median={v} filled={na}"

        # Other numerics: median
        num_float_cols = [c for c in df.select_dtypes(include=[np.number]).columns if c not in self._int_cols]
        for col in num_float_cols:
            na = int(df[col].isna().sum())
            if na > 0:
                med = df[col].median()
                df[col] = df[col].fillna(med)
                ctx.setdefault("details", {}).setdefault(self.name, {})[col] = f"median={med} filled={na}"

        # Booleans: mode
        for col in [c for c in df.columns if c.startswith(("is_", "has_"))]:
            mode_val = df[col].mode(dropna=True)
            if not mode_val.empty:
                na = int(df[col].isna().sum())
                if na:
                    df[col] = df[col].fillna(mode_val.iloc[0])
                    ctx.setdefault("details", {}).setdefault(self.name, {})[col] = f"bool_mode={mode_val.iloc[0]} filled={na}"

        # Objects: mode, but skip any in policy.no_impute_objects (e.g., description_fr)
        for col in df.select_dtypes(include=["object"]).columns:
            if col in self.policy.no_impute_objects:
                continue
            mode_val = df[col].mode(dropna=True)
            if not mode_val.empty:
                na = int(df[col].isna().sum())
                if na:
                    df[col] = df[col].fillna(mode_val.iloc[0])
                    ctx.setdefault("details", {}).setdefault(self.name, {})[col] = f"obj_mode='{mode_val.iloc[0]}' filled={na}"

        return df

class TightenTypes:
    """Cast booleans & small-cardinality categoricals for memory/speed."""
    name = "tighten_types"

    def __init__(self, policy: CleaningPolicy):
        self.policy = policy

    @timeit(name)
    @log_step(name)
    @safe_step
    def apply(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        df = df.copy()

        for col in [c for c in df.columns if any(c.lower().startswith(p) for p in self.policy.boolean_prefixes)]:
            if col in df.columns:
                if df[col].isna().sum() == 0:
                    df[col] = df[col].astype(bool)
                else:
                    df[col] = df[col].astype("boolean")

        for col in ["transaction_type", "item_type", "item_subtype", "city"]:
            if col in df.columns and df[col].nunique(dropna=True) < self.policy.small_cardinality_cast_threshold:
                df[col] = df[col].astype("category")

        # Reassert Int64 for integer fields
        for col in ("floor", "room_count", "balcony_count", "terrace_count", "build_year"):
            if col in df.columns:
                df[col] = df[col].astype("Int64")

        return df

# =========================
# Pipeline
# =========================

@dataclass
class Paths:
    raw_path: Path
    out_dir: Path = Path("./clean_artifacts_py")
    cleaned_filename: str = "listings_cleaned.csv"
    changelog_filename: str = "cleaning_changelog.txt"

    @property
    def cleaned_path(self) -> Path:
        return self.out_dir / self.cleaned_filename

    @property
    def changelog_path(self) -> Path:
        return self.out_dir / self.changelog_filename

class CleaningPipeline:
    def __init__(self, steps: List[CleanerStep], paths: Paths):
        self.steps = steps
        self.paths = paths
        self.ctx: Dict[str, Any] = {}

    def run(self) -> pd.DataFrame:
        self.paths.out_dir.mkdir(parents=True, exist_ok=True)
        df = pd.read_csv(self.paths.raw_path)
        self.ctx.setdefault("log", []).append(f"Loaded {self.paths.raw_path} shape={df.shape}")

        for step in self.steps:
            df = step.apply(df, self.ctx)

        df.to_csv(self.paths.cleaned_path, index=False)
        with open(self.paths.changelog_path, "w", encoding="utf-8") as f:
            f.write("\n".join(self.ctx.get("log", [])))
            f.write("\n\nDetails:\n")
            for step_name, details in (self.ctx.get("details") or {}).items():
                f.write(f"- {step_name}: {details}\n")

        print(f" ---- Saved cleaned CSV → {self.paths.cleaned_path}")
        print(f" ---- Changelog         → {self.paths.changelog_path}")
        if self.ctx.get("errors"):
            print(" -- Step errors recorded:", self.ctx["errors"])
        return df

def build_default_pipeline(raw_path: str, out_dir: str = "./clean_artifacts_py") -> CleaningPipeline:
    policy = CleaningPolicy()
    paths = Paths(raw_path=Path(raw_path), out_dir=Path(out_dir))
    preserve_cols = list(set(policy.preserve_identifiers + policy.preserve_always))
    steps: List[CleanerStep] = [
        StandardizeColumns(),
        RemoveExactDuplicates(),
        DeduplicateBusinessKey(key="listing_id"),
        ConvertDtypes(policy),
        CastNumericLike(),      # Int64 for specified integer columns
        NumericSanity(policy),
        DropColumns(
            to_drop=policy.drop_by_name,
            drop_ultra_sparse_threshold=policy.drop_ultra_sparse_threshold,
            preserve=preserve_cols  # <- keeps description_fr and terrace_area
        ),
        Impute(policy),          # <- never imputes description_fr
        TightenTypes(policy),
    ]
    return CleaningPipeline(steps=steps, paths=paths)

# =========================
# CLI
# =========================

def _parse_args():
    import argparse
    p = argparse.ArgumentParser(description="Cleaning pipeline (Python 3.8 compatible)")
    p.add_argument("--raw", default="listings.csv", help="Path to raw CSV input")
    p.add_argument("--out", default="./clean_artifacts_py", help="Output directory")
    return p.parse_args()

if __name__ == "__main__":
    args = _parse_args()
    pipeline = build_default_pipeline(raw_path=args.raw, out_dir=args.out)
    pipeline.run()
