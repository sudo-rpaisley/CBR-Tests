"""Read-only campaign metric response analysis, with capture-level units.

Input: exports/metric_results.csv from recursive GAN campaign.
Output: per-capture/seed curves and equal-weight held-out summaries.
Never edits authoritative outcome JSONs; does not invent realism labels.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from collections import defaultdict

import numpy as np
import pandas as pd


REQUIRED = {"group", "held_out", "seed", "perturbation_pct", "metric_id", "primary_value"}


def load_rows(input_path: Path) -> pd.DataFrame:
    df = pd.read_csv(input_path, dtype={"held_out":str, "group":str, "metric_id":str})
    missing = REQUIRED - set(df.columns)
    if missing:
        raise ValueError("Required columns missing: "+", ".join(sorted(missing)))
    if "execution_status" in df:
        df = df.loc[df.execution_status == "success"].copy()
    if "attempt_status" in df:
        df = df.loc[df.attempt_status == "completed"].copy()
    df["perturbation_pct"] = pd.to_numeric(df["perturbation_pct"], errors="coerce")
    df["primary_value"] = pd.to_numeric(df["primary_value"], errors="coerce")
    df = df.dropna(subset=["perturbation_pct", "primary_value"])
    keys = ["group","held_out","seed","metric_id","perturbation_pct"]
    if df.duplicated(keys).any():
        raise ValueError("Duplicate metric outcomes for same held-out/seed/level; resolve attempts explicitly")
    return df


def _spearman(x: np.ndarray, y: np.ndarray) -> float | None:
    if len(x) < 3 or np.std(y) == 0:
        return None
    xs = pd.Series(x).rank().to_numpy()
    ys = pd.Series(y).rank().to_numpy()
    if np.std(ys) == 0:
        return None
    return float(np.corrcoef(xs,ys)[0,1])


def curves(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (group, held_out, seed, metric), subset in df.groupby(
        ["group","held_out","seed","metric_id"],sort=True,dropna=False):
        subset=subset.sort_values("perturbation_pct")
        x=subset.perturbation_pct.to_numpy(dtype=float)
        y=subset.primary_value.to_numpy(dtype=float)
        start = float(y[0]) if len(x) and x[0] == 0 else None
        corr = _spearman(x,y)
        distinct = len(set(x))
        full = (len(x)==11 and np.array_equal(x,np.arange(0,101,10)))
        rows.append({
            "group":group,"held_out":held_out,"seed":seed,"metric_id":metric,
            "observed_levels":distinct,"complete_11_level_series":full,
            "baseline_value":start,
            "final_value":float(y[-1]) if len(y) else None,
            "last_level":float(x[-1]) if len(x) else None,
            "delta_final_from_zero":float(y[-1]-start) if start is not None else None,
            "monotonic_non_decreasing":bool(np.all(np.diff(y)>=-1e-8)) if len(y)>=2 else None,
            "spearman_rho":corr,
            "slope_per_percent":float(np.polyfit(x,y,1)[0]) if len(x)>=2 else None,
            "trapezoidal_auc":float(np.trapz(y,x)/(x[-1]-x[0])) if len(x)>=2 and x[-1]>x[0] else None,
            "maximum_value":float(max(y)) if len(y) else None,
            "level_at_maximum":float(x[np.argmax(y)]) if len(y) else None,
        })
    return pd.DataFrame(rows)


def _bootstrap_median(values: np.ndarray, group_labels: np.ndarray, seed: int=42,
                      iterations: int=2000) -> tuple[float,float]:
    """Cluster bootstrap by held-out capture, respecting seed reuse within capture."""
    groups = sorted(set(map(str,group_labels)))
    if len(groups) < 3:
        return float("nan"),float("nan")
    by_group = {
        g: np.asarray(values[np.asarray(group_labels,dtype=str)==g],dtype=float)
        for g in groups
    }
    rng=np.random.default_rng(seed)
    boot=[]
    for _ in range(iterations):
        sampled=rng.choice(groups,size=len(groups),replace=True)
        # Equal held-out weight; repeated seeds are averaged within capture.
        estimates=[np.median(by_group[g]) for g in sampled]
        boot.append(float(np.median(estimates)))
    return tuple(map(float,np.quantile(boot,[0.025,0.975])))


def summaries(curve_df: pd.DataFrame) -> pd.DataFrame:
    rows=[]
    for (group, metric), sub in curve_df.groupby(["group","metric_id"],sort=True):
        valid=sub.dropna(subset=["delta_final_from_zero"])
        if valid.empty:
            continue
        by_capture=valid.groupby("held_out",sort=True)["delta_final_from_zero"].median()
        vals=valid.delta_final_from_zero.to_numpy(dtype=float)
        labs=valid.held_out.to_numpy(dtype=str)
        low,high=_bootstrap_median(vals,labs)
        rows.append({
            "group":group,"metric_id":metric,
            "held_out_capture_count":len(by_capture),
            "seed_level_series_count":len(valid),
            "median_capture_level_delta":float(by_capture.median()),
            "bootstrap_capture_cluster_ci_2_5":low,
            "bootstrap_capture_cluster_ci_97_5":high,
            "median_spearman_rho":float(valid.spearman_rho.median()) if valid.spearman_rho.notna().any() else None,
            "all_capture_deltas_positive":bool((by_capture>0).all()),
            "fraction_monotonic_series":float(valid.monotonic_non_decreasing.astype(bool).mean()),
            "scope":"descriptive; no external realism labels",
        })
    return pd.DataFrame(rows)


def main(argv=None):
    parser=argparse.ArgumentParser()
    parser.add_argument("--metrics",required=True,type=Path)
    parser.add_argument("--out",required=True,type=Path)
    args=parser.parse_args(argv)
    if args.out.exists() and any(args.out.iterdir()):
        parser.error("--out must be a new or empty directory")
    rows=load_rows(args.metrics)
    if rows.empty:
        parser.error("No usable completed numerical rows")
    output=curves(rows)
    grouped=summaries(output)
    args.out.mkdir(parents=True,exist_ok=True)
    output.to_csv(args.out/"capture_seed_response_curves.csv",index=False)
    grouped.to_csv(args.out/"group_metric_cluster_bootstrap.csv",index=False)
    (args.out/"analysis_manifest.json").write_text(json.dumps({
        "source":str(args.metrics.resolve()),"row_count":len(rows),
        "complete_11_level_capture_seed_metric_series":int(output.complete_11_level_series.sum()),
        "held_out_count":int(output[["group","held_out"]].drop_duplicates().shape[0]),
        "statistical_unit":"held-out capture; multiple seeds clustered",
        "warning":"No external realism labels; rho/slope are sensitivity diagnostics, not accuracy.",
    },indent=2)+"\n")
    return 0


if __name__=="__main__":
    raise SystemExit(main())
