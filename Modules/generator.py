import json, random
import pandas as pd


def generate_tasks(n_tasks=3000, num_devices=200, mean_size=450, std_size=50,
    deadline_min=1.0, deadline_max=3.0, out_path="Data/tasks.json"):
    random.seed(42)
    mean_interarrival_s = 0.1
    t = 0.0
    tasks = []
    for i in range(n_tasks):
        t += random.expovariate(1.0 / mean_interarrival_s)
        size_kb = max(10.0, random.gauss(mean_size, std_size))
        deadline_ms = random.uniform(deadline_min, deadline_max)
        device_id = f"dev_{random.randint(0, num_devices-1)}"
        priority = 1 if random.random() < 0.8 else 2
        tasks.append({
            "task_id": f"task_{i}",
            "device_id": device_id,
            "creation_time_s": t,
            "size_kb": size_kb,
            "deadline_ms": deadline_ms,
            "priority": priority
        })
    pd.DataFrame(tasks).to_json(out_path, orient="records", lines=True)
    return tasks