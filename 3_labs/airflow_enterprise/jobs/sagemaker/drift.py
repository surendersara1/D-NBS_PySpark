"""
drift.py — the container entrypoint for DAG 03's SageMakerProcessingOperator.

Runs as /opt/ml/code/drift.py inside the processing container. This is NOT a
Spark job: SageMaker Processing gives one box and pandas, which is right for a
statistical comparison of two feature snapshots.

    inputs   /opt/ml/processing/today      today's assembled features
             /opt/ml/processing/baseline   the snapshot the live model was trained on
    output   /opt/ml/processing/out/psi.json

WHAT IT COMPUTES

Population Stability Index per feature:

    PSI = sum over bins of  (actual% - expected%) * ln(actual% / expected%)

Rules of thumb the industry uses, and the reason DAG 03's threshold is 0.20:

    PSI < 0.10   no meaningful shift
    0.10 - 0.25  moderate shift, worth watching
    PSI > 0.25   significant shift, retrain

The DAG reads psi.json, compares the overall figure against
CHURN_DRIFT_PSI_THRESHOLD, and branches. Retraining nightly out of habit costs
roughly 240 GPU-hours a year and silently reshuffles the model under the
campaign team every night; this file is what makes that decision evidence-based.

BIN EDGES COME FROM THE BASELINE, NOT FROM TODAY
    Re-binning on today's data would move the goalposts with the distribution
    and report no drift no matter what happened. The edges are frozen from the
    baseline, which is the whole point of the measure.
"""
from __future__ import annotations

import glob
import json
import os

import numpy as np
import pandas as pd

TODAY_DIR = "/opt/ml/processing/today"
BASELINE_DIR = "/opt/ml/processing/baseline"
OUT_DIR = "/opt/ml/processing/out"

N_BINS = 10
EPSILON = 1e-6            # keeps ln() finite when a bin empties completely
PSI_ALERT = 0.25


def load(directory: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(directory, "**", "*.parquet"), recursive=True))
    if not files:
        files = sorted(glob.glob(os.path.join(directory, "**", "*.csv"), recursive=True))
        if not files:
            raise FileNotFoundError(f"no parquet or csv under {directory}")
        return pd.concat((pd.read_csv(f) for f in files), ignore_index=True)
    return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)


def psi_numeric(expected: pd.Series, actual: pd.Series) -> float:
    """PSI for a continuous feature, using baseline quantile edges."""
    expected = expected.dropna()
    actual = actual.dropna()
    if len(expected) < 100 or len(actual) < 100:
        return float("nan")

    # Edges frozen from the baseline. Duplicates dropped for spiky features
    # (many zeros), which would otherwise produce zero-width bins.
    edges = np.unique(np.quantile(expected, np.linspace(0, 1, N_BINS + 1)))
    if len(edges) < 3:
        return 0.0                      # effectively constant; nothing to measure
    edges[0], edges[-1] = -np.inf, np.inf

    e_pct = np.histogram(expected, bins=edges)[0] / len(expected)
    a_pct = np.histogram(actual, bins=edges)[0] / len(actual)
    e_pct = np.clip(e_pct, EPSILON, None)
    a_pct = np.clip(a_pct, EPSILON, None)
    return float(np.sum((a_pct - e_pct) * np.log(a_pct / e_pct)))


def psi_categorical(expected: pd.Series, actual: pd.Series) -> float:
    e = expected.astype("string").fillna("__NULL__").value_counts(normalize=True)
    a = actual.astype("string").fillna("__NULL__").value_counts(normalize=True)
    cats = e.index.union(a.index)
    e = e.reindex(cats, fill_value=0).clip(lower=EPSILON)
    a = a.reindex(cats, fill_value=0).clip(lower=EPSILON)
    return float(np.sum((a - e) * np.log(a / e)))


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    today = load(TODAY_DIR)
    baseline = load(BASELINE_DIR)
    print(f"today={today.shape} baseline={baseline.shape}")

    skip = {"subscriber_id", "msisdn_hash", "feature_date", "assembled_at",
            "label_churned_30d", "label_is_observable", "activation_date"}
    shared = [c for c in today.columns if c in baseline.columns and c not in skip]
    print(f"comparing {len(shared)} shared features")

    per_feature: dict[str, float] = {}
    for col in shared:
        try:
            if pd.api.types.is_numeric_dtype(baseline[col]):
                val = psi_numeric(baseline[col], today[col])
            else:
                val = psi_categorical(baseline[col], today[col])
            if not np.isnan(val):
                per_feature[col] = round(val, 6)
        except Exception as exc:                      # one bad column must not kill the check
            print(f"  skip {col}: {exc}")

    if not per_feature:
        raise RuntimeError("no features could be compared — the baseline is unusable")

    drifted = sorted([c for c, v in per_feature.items() if v > PSI_ALERT],
                     key=lambda c: -per_feature[c])
    values = np.array(list(per_feature.values()))

    # The headline number is the 90th percentile, not the mean. A mean over 120
    # features hides four badly drifted ones, which is exactly the case that
    # should trigger a retrain.
    overall = float(np.percentile(values, 90))

    report = {
        "population_stability_index": round(overall, 6),
        "mean_psi": round(float(values.mean()), 6),
        "max_psi": round(float(values.max()), 6),
        "features_compared": len(per_feature),
        "drifted_features": drifted[:25],
        "drifted_count": len(drifted),
        "per_feature_psi": per_feature,
        "rows_today": int(len(today)),
        "rows_baseline": int(len(baseline)),
    }

    out = os.path.join(OUT_DIR, "psi.json")
    with open(out, "w") as fh:
        json.dump(report, fh, indent=2)

    print(f"PSI(p90)={overall:.4f} mean={report['mean_psi']:.4f} "
          f"max={report['max_psi']:.4f} drifted={len(drifted)}")
    for c in drifted[:10]:
        print(f"  {c}: {per_feature[c]:.4f}")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
