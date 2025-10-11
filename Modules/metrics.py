import pandas as pd
import os

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
    }

    metrics_df = pd.DataFrame([metrics])
    metrics_df.to_csv(os.path.join(outdir, "sim_metrics_summary.csv"), index=False)

    weights_df = pd.DataFrame(weight_logs)
    weights_df.to_csv(os.path.join(outdir, "weights.csv"), index=False)

    return df, metrics_df, weights_df
