import pandas as pd
import os

def save_logs_and_metrics(task_logs, weight_logs, out_dir):
    logs_path = os.path.join(out_dir, "sim_task_logs.csv")
    pd.DataFrame(task_logs).to_csv(logs_path, index=False)

    df = pd.DataFrame(task_logs)
    metrics = {
        "num_tasks": len(df),
        "avg_latency_ms": df.total_latency_ms.mean(),
        "std_latency_ms": df.total_latency_ms.std(),
        "sla_violation_rate": (df.status=="miss").mean(),
        "drop_rate": (df.status=="drop").mean(),
        "hit_rate": (df.status=="hit").mean(),
        "avg_queue_delay_ms": df.queue_delay_ms.mean(),
        "avg_proc_delay_ms": df.proc_delay_ms.mean(),
        "avg_tx_delay_ms": df.tx_delay_ms.mean(),
        "avg_energy_j": df.energy_j.mean()
    }
    metrics_path = os.path.join(out_dir, "sim_metrics_summary.csv")
    pd.DataFrame([metrics]).to_csv(metrics_path, index=False)

    weights_path = os.path.join(out_dir, "weights.csv")
    pd.DataFrame(weight_logs).to_csv(weights_path, index=False)

    return logs_path, metrics_path, weights_path
