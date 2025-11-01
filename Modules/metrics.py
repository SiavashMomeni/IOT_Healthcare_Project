import pandas as pd
import os
from collections import deque
import numpy as np

# Rolling windows to compute recent statistics
_recent_decisions = deque(maxlen=200)
_recent_delays = deque(maxlen=200)
_recent_utils = deque(maxlen=200)

def record_task(decision, total_latency_ms, path_util):
    """Record one task's outcome for online metric tracking."""
    _recent_decisions.append(decision)
    _recent_delays.append(total_latency_ms)
    _recent_utils.append(path_util)

def get_recent_offload_ratio():
    """Return the fraction of tasks that were offloaded recently."""
    if not _recent_decisions:
        return 0.5  # default neutral value
    offload_count = sum(1 for d in _recent_decisions if d == "offload")
    return offload_count / len(_recent_decisions)

def get_mean_latency():
    """Mean of recent total latencies."""
    return np.mean(_recent_delays) if _recent_delays else 0.0

def get_mean_path_utilization():
    """Mean of recent path utilizations (0–1 range)."""
    return np.mean(_recent_utils) if _recent_utils else 0.0

# ----------------------------------------------
# Logging / Saving (your original functionality)
# ----------------------------------------------
def save_logs_and_metrics(task_logs, weight_logs, outdir):
    df = pd.DataFrame(task_logs)
    
    expected_cols = [
        "total_latency_ms", "sla_violation", "decision", "queue_delay_ms",
        "proc_delay_ms", "tx_delay_ms", "energy_j", "status"
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    df.to_csv(os.path.join(outdir, "sim_task_logs.csv"), index=False)

    metrics = {
        "num_tasks": len(df),
        "avg_latency_ms": df["total_latency_ms"].mean(skipna=True),
        "std_latency_ms": df["total_latency_ms"].std(skipna=True),
        "sla_violation_rate": df["sla_violation"].mean(skipna=True) if "sla_violation" in df else 0.0,
        "drop_rate": (df["status"] == "drop").mean() if "status" in df else 0.0,
        "hit_rate": (df["status"] == "hit").mean() if "status" in df else 0.0,
        "avg_queue_delay_ms": df["queue_delay_ms"].mean(skipna=True),
        "avg_proc_delay_ms": df["proc_delay_ms"].mean(skipna=True),
        "avg_tx_delay_ms": df["tx_delay_ms"].mean(skipna=True),
        "avg_energy_j": df["energy_j"].mean(skipna=True),
        "recent_offload_ratio": get_recent_offload_ratio(),
        "mean_path_util": get_mean_path_utilization(),
        "mean_recent_latency": get_mean_latency(),
    }

    metrics_df = pd.DataFrame([metrics])
    metrics_df.to_csv(os.path.join(outdir, "sim_metrics_summary.csv"), index=False)

    weights_df = pd.DataFrame(weight_logs)
    weights_df.to_csv(os.path.join(outdir, "weights.csv"), index=False)

    return df, metrics_df, weights_df
