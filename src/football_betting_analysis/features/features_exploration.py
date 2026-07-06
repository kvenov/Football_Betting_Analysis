import pandas as pd

from scipy.stats import jarque_bera, skew, kurtosis


def safe_cols(df: pd.DataFrame, cols: list) -> list:
    """Return only columns that actually exist in df."""
    return [c for c in cols if c in df.columns]
 
 
def numeric_profile(series: pd.Series) -> dict:
    """Full statistical profile of a numeric series."""
    s = pd.to_numeric(series, errors="coerce").dropna()
    
    if len(s) == 0:
        return {}
    
    q1, q3 = s.quantile(0.25), s.quantile(0.75)
    iqr = q3 - q1
    lo_fence = q1 - 1.5 * iqr
    hi_fence = q3 + 1.5 * iqr
    n_outliers_iqr = int(((s < lo_fence) | (s > hi_fence)).sum())
 
    # Modified Z-score (Hampel)
    median = s.median()
    mad = (s - median).abs().median()
    if mad > 0:
        mz = 0.6745 * (s - median) / mad
        n_outliers_mz = int((mz.abs() > 3.5).sum())
    else:
        n_outliers_mz = 0
 
    sk = float(skew(s))
    kt = float(kurtosis(s))
 
    # Normality test - Jarque-Bera (faster than Shapiro for large n)
    if len(s) >= 8:
        jb_stat, jb_p = jarque_bera(s)
    else:
        jb_p = 1.0
 
    return {
        "n": len(s),
        "missing": int(series.isna().sum()),
        "missing_pct": round(series.isna().mean() * 100, 2),
        "mean": round(float(s.mean()), 4),
        "median": round(float(median), 4),
        "std": round(float(s.std()), 4),
        "min": round(float(s.min()), 4),
        "p01": round(float(s.quantile(0.01)), 4),
        "p05": round(float(s.quantile(0.05)), 4),
        "p25": round(float(q1), 4),
        "p75": round(float(q3), 4),
        "p95": round(float(s.quantile(0.95)), 4),
        "p99": round(float(s.quantile(0.99)), 4),
        "max": round(float(s.max()), 4),
        "skewness": round(sk, 3),
        "kurtosis": round(kt, 3),
        "iqr_outliers": n_outliers_iqr,
        "iqr_outlier_pct": round(n_outliers_iqr / len(s) * 100, 2),
        "mz_outliers": n_outliers_mz,
        "jb_p": round(jb_p, 4),
        "is_normal": jb_p > 0.05,
        "lo_fence": round(float(lo_fence), 4),
        "hi_fence": round(float(hi_fence), 4),
    }
 
def recommend_scaling(group_name: str, col: str, profile: dict) -> dict:
    """
    Rule-based recommendation engine.
    Returns dict with: transform, scaler, clip_lo, clip_hi, notes.
    """
    sk = profile.get("skewness", 0)
    mn = profile.get("min", 0)
    mx = profile.get("max", 1)
    lo = profile.get("lo_fence")
    hi = profile.get("hi_fence")
    out_pct = profile.get("iqr_outlier_pct", 0)
    is_normal = profile.get("is_normal", False)
    missing_pct = profile.get("missing_pct", 0)
 
    transform = "none"
    scaler = "none"
    clip_lo = None
    clip_hi = None
    notes = []
 
    # Missing value strategy
    if missing_pct > 50:
        notes.append(f"HIGH MISSING {missing_pct:.1f}% — consider dropping column")
    elif missing_pct > 10:
        notes.append(f"Missing {missing_pct:.1f}% — impute with median")
    elif missing_pct > 0:
        notes.append(f"Missing {missing_pct:.1f}% — impute with median")
 
    # Binary / flag groups - no scaling
    if "G1_binary" in group_name:
        scaler = "none"
        notes.append("Binary feature - no scaling needed")
        return {"transform": transform, "scaler": scaler,
                "clip_lo": clip_lo, "clip_hi": clip_hi, "notes": "; ".join(notes)}
 
    # Bounded [0,1] features - no scaling
    if "G4_bounded" in group_name:
        scaler = "none"
        if abs(sk) > 2:
            transform = "sqrt" if mn >= 0 else "none"
            notes.append(f"Skewed {sk:.2f} - consider sqrt transform to reduce pile-up")
        else:
            notes.append("Already in [0,1] - no scaling needed for tree models")
            
        notes.append("For linear models: leave as-is (already bounded)")
        return {"transform": transform, "scaler": scaler,
                "clip_lo": clip_lo, "clip_hi": clip_hi, "notes": "; ".join(notes)}
 
    # Odds - always log transform
    if "G7_odds" in group_name:
        transform = "log1p"
        scaler = "standard"
        if out_pct > 5:
            clip_lo = lo
            clip_hi = hi
            notes.append(f"Clip at IQR fences before log ({out_pct:.1f}% outliers)")
        notes.append("Odds are right-skewed; log1p normalises, then StandardScaler")
        return {"transform": transform, "scaler": scaler,
                "clip_lo": clip_lo, "clip_hi": clip_hi, "notes": "; ".join(notes)}
 
    # General numeric rules
 
    # Outlier clipping recommendation
    if out_pct > 10:
        clip_lo = lo
        clip_hi = hi
        notes.append(f"Strong outliers ({out_pct:.1f}% IQR) - clip at fences before scaling")
    elif out_pct > 3:
        notes.append(f"Moderate outliers ({out_pct:.1f}% IQR) - consider clipping at p1/p99")
 
    # Transform recommendation
    if mn >= 0 and sk > 2:
        transform = "log1p"
        notes.append(f"Right-skewed ({sk:.2f}) with non-negative values - log1p transform")
    elif mn >= 0 and 1 < sk <= 2:
        transform = "sqrt"
        notes.append(f"Moderately right-skewed ({sk:.2f}) - sqrt transform")
    elif abs(sk) <= 0.5 and is_normal:
        transform = "none"
        notes.append("Approximately normal - no transform needed")
    elif sk < -2:
        transform = "reflect_log1p"
        notes.append(f"Left-skewed ({sk:.2f}) - reflect then log1p")
    else:
        transform = "none"
        notes.append(f"Skewness {sk:.2f} - no transform needed for tree models")
 
    # Scaler recommendation
    if is_normal and abs(sk) <= 1:
        scaler = "standard"
        notes.append("Approximately normal -> StandardScaler")
    elif out_pct > 5:
        scaler = "robust"
        notes.append("Outliers present -> RobustScaler preferred over StandardScaler")
    else:
        scaler = "minmax" if (mn >= 0 and mx <= 200) else "standard"
        notes.append(f"{'MinMaxScaler' if scaler == 'minmax' else 'StandardScaler'}")
 
    return {
        "transform": transform,
        "scaler": scaler,
        "clip_lo": round(clip_lo, 4) if clip_lo is not None else None,
        "clip_hi": round(clip_hi, 4) if clip_hi is not None else None,
        "notes": "; ".join(notes),
    }
 
def profile_group(group_name: str, cols: list, df: pd.DataFrame) -> pd.DataFrame:
    """Profile all numeric columns in a group and return summary DataFrame."""
    existing = safe_cols(df, cols)
    rows = []
    for col in existing:
        try:
            p = numeric_profile(df[col])
            if not p:
                continue
            r = recommend_scaling(group_name, col, p)
            rows.append({
                "group": group_name,
                "feature": col,
                **p,
                "transform": r["transform"],
                "scaler": r["scaler"],
                "clip_lo": r["clip_lo"],
                "clip_hi": r["clip_hi"],
                "notes": r["notes"],
            })
        except Exception as e:
            rows.append({"group": group_name, "feature": col, "notes": f"ERROR: {e}"})
    return pd.DataFrame(rows)
 
def profile_categorical(cols: list, df: pd.DataFrame) -> pd.DataFrame:
    """Profile categorical columns."""
    existing = safe_cols(df, cols)
    rows = []
    for col in existing:
        s = df[col].astype(str)
        n_unique = s.nunique()
        top_val = s.mode().iloc[0] if len(s) > 0 else "N/A"
        top_freq = (s == top_val).mean()
        missing = df[col].isna().sum()
        missing_pct = round(df[col].isna().mean() * 100, 2)
 
        if n_unique == 2:
            enc = "binary (0/1)"
            note = "Two values - encode as binary"
        elif n_unique <= 20:
            enc = "one-hot"
            note = f"Low cardinality ({n_unique}) - one-hot encoding recommended"
        elif n_unique <= 100:
            enc = "target / ordinal"
            note = f"Medium cardinality ({n_unique}) - target encoding or frequency encoding"
        else:
            enc = "target / hash / drop"
            note = f"High cardinality ({n_unique}) - target encoding or consider dropping"
 
        rows.append({
            "group": "G2_categorical",
            "feature": col,
            "n_unique": n_unique,
            "top_value": top_val,
            "top_freq_pct":round(top_freq * 100, 1),
            "missing": missing,
            "missing_pct": missing_pct,
            "encoding": enc,
            "notes": note,
        })

    return pd.DataFrame(rows)
 
def profile_binary_flags(cols: list, df: pd.DataFrame) -> pd.DataFrame:
    """Profile binary flag features — check class balance."""
    existing = safe_cols(df, cols)
    rows = []
    for col in existing:
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        pos_rate  = float(s.mean())
        n = len(s)
        missing = int(df[col].isna().sum())
        imbalance = "severe" if pos_rate < 0.05 or pos_rate > 0.95 else \
                    "high" if pos_rate < 0.20 or pos_rate > 0.80 else \
                    "moderate" if pos_rate < 0.30 or pos_rate > 0.70 else "acceptable"
        rows.append({
            "group": "G1_binary_flags",
            "feature": col,
            "n": n,
            "pos_count": int(s.sum()),
            "pos_rate": round(pos_rate, 4),
            "neg_rate": round(1 - pos_rate, 4),
            "missing": missing,
            "imbalance": imbalance,
            "notes": f"Class balance: {pos_rate:.1%} positive. {imbalance} imbalance. No scaling needed.",
        })
    return pd.DataFrame(rows)
