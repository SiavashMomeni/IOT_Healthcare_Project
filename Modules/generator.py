import json, random
import pandas as pd
from .utils import snap_time

def generate_tasks(config, out_path="Data/tasks.json"):
    random.seed(42)
    n_tasks = config["num_tasks"]
    num_devices = config["num_devices"]
    horizon = config["simulation_horizon_s"]

    avg_tasks_per_device = n_tasks / num_devices
    lambda_rate = avg_tasks_per_device / horizon

    tasks = []
    for dev_id in range(num_devices):
        t = 0.0
        count = 0
        while count < avg_tasks_per_device:
            t += random.expovariate(lambda_rate)
            t = snap_time(t)
            if t > horizon:
                break
            size_kb = max(10.0, random.gauss(config["mean_task_size_kb"], config["std_task_size_kb"]))
            deadline_ms = snap_time(random.uniform(1.0, 3.0) / 1000.0) * 1000.0
            priority = 1 if random.random() < 0.8 else 2
            tasks.append({
                "task_id": f"task_{dev_id}_{count}",
                "device_id": f"dev_{dev_id}",
                "creation_time_s": t,
                "size_kb": size_kb,
                "deadline_ms": deadline_ms,
                "priority": priority
            })
            count += 1

    tasks = sorted(tasks, key=lambda x: x["creation_time_s"])
    pd.DataFrame(tasks).to_json(out_path, orient="records", lines=True)
    return tasks

