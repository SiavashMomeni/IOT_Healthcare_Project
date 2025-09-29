import pandas as pd
import os


def save_logs_and_metrics(task_logs, out_dir):
    logs_path = os.path.join(out_dir, "sim_task_logs.csv")
    pd.DataFrame(task_logs).to_csv(logs_path, index=False)
    df = pd.DataFrame(task_logs)
    metrics = {
        "num_tasks": len(df),
        "avg_latency_ms": df.latency_ms.mean(),
        "std_latency_ms": df.latency_ms.std(),
        "sla_violation_rate": df.sla_violation.mean(),
        "avg_energy_j": df.energy_j.mean()
    }
    metrics_path = os.path.join(out_dir, "sim_metrics_summary.csv")
    pd.DataFrame([metrics]).to_csv(metrics_path, index=False)
    return logs_path, metrics_path