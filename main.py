import os, heapq, yaml
import pandas as pd
from Modules.generator import generate_tasks
from Modules.scheduler import Scheduler
from Modules.sdn_controller import SDNController
from Modules.maths import processing_time_ms
from Modules.network import Network
from Modules.metrics import save_logs_and_metrics, record_task, get_mean_path_utilization  # ✅ new import
from Modules.utils import snap_time

if __name__ == "__main__":
    # -----------------------------
    # Load config
    # -----------------------------
    with open("config.yml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Ensure numeric fields
    for k in [
        "device_cpu_hz", "fog_cpu_hz", "cloud_cpu_hz", "cycles_per_byte",
        "p_tx", "energy_per_cycle", "default_link_bw_bps", "device_access_bw_bytes_per_s"
    ]:
        if k in config:
            config[k] = float(config[k])

    os.makedirs("Data", exist_ok=True)
    os.makedirs("Results", exist_ok=True)

    # -----------------------------
    # Generate tasks and setup network
    # -----------------------------
    tasks = generate_tasks(config, out_path="Data/tasks.json")

    device_weights = {
        f"dev_{i}": {
            "w_local": config["initial_weights"]["w_local"],
            "w_offload": config["initial_weights"]["w_offload"]
        } for i in range(config["num_devices"])
    }

    network = Network(
        topology_path=config.get("topology_path"),
        default_link_bw_bps=config.get("default_link_bw_bps"),
        device_access_bw_bytes_per_s=config.get("device_access_bw_bytes_per_s"),
        default_rtt_s=config.get("default_rtt_s")
    )
    network.attach_devices(config["num_devices"])

    controller = SDNController(network, config)
    scheduler = Scheduler(network, controller, config)

    # -----------------------------
    # Simulation setup
    # -----------------------------
    events = []
    for i, task in enumerate(tasks):
        heapq.heappush(events, (snap_time(task["creation_time_s"]), "arrival", i, task))

    device_busy_until = {d: 0.0 for d in device_weights}
    fog_busy_until = [0.0] * config["fog_workers"]

    task_logs, weight_logs = [], []

    # -----------------------------
    # Main simulation loop
    # -----------------------------
    while events:
        time_now, ev_type, _, payload = heapq.heappop(events)
        time_now = snap_time(time_now)

        if ev_type == "arrival":
            task = payload
            decision, path_nodes, path_links, meta = scheduler.decide(task, time_now)

            # -----------------
            # LOCAL processing
            # -----------------
            if decision == "local":
                proc_time_ms, cycles = processing_time_ms(
                    task["size_kb"], config["device_cpu_hz"], config["cycles_per_byte"]
                )
                ready_time = device_busy_until[task["device_id"]]
                start = snap_time(max(time_now, ready_time))
                queue_delay = (start - task["creation_time_s"]) * 1000

                if (queue_delay + proc_time_ms) > task["deadline_ms"]:
                    log = {
                        "task_id": task["task_id"], "device_id": task["device_id"],
                        "decision": "local", "arrival_time_s": task["creation_time_s"],
                        "queue_enter_time_s": time_now, "start_time_s": None,
                        "end_time_s": None, "queue_delay_ms": queue_delay,
                        "proc_delay_ms": 0.0, "tx_delay_ms": 0.0,
                        "total_latency_ms": None, "energy_j": 0.0,
                        "deadline_ms": task["deadline_ms"], "status": "drop"
                    }
                    task_logs.append(log)
                    continue

                end = snap_time(start + proc_time_ms / 1000.0)
                device_busy_until[task["device_id"]] = end
                total_latency = (end - task["creation_time_s"]) * 1000
                energy = cycles * config["energy_per_cycle"]
                status = "hit" if total_latency <= task["deadline_ms"] else "miss"

                log = {
                    "task_id": task["task_id"], "device_id": task["device_id"],
                    "decision": "local", "arrival_time_s": task["creation_time_s"],
                    "queue_enter_time_s": time_now, "start_time_s": start,
                    "end_time_s": end, "queue_delay_ms": queue_delay,
                    "proc_delay_ms": proc_time_ms, "tx_delay_ms": 0.0,
                    "total_latency_ms": total_latency, "energy_j": energy,
                    "deadline_ms": task["deadline_ms"], "status": status
                }
                task_logs.append(log)

                # ✅ log metrics
                sla_vio = total_latency > task["deadline_ms"]

            # -----------------
            # OFFLOAD processing
            # -----------------
            elif decision == "offload":
                tx_ms = network.estimate_network_delay_ms(path_links, task["size_kb"])
                arrival_fog = snap_time(time_now + tx_ms / 1000.0)
                fog_time_ms, _ = processing_time_ms(task["size_kb"], config["fog_cpu_hz"], config["cycles_per_byte"])
                idx = min(range(len(fog_busy_until)), key=lambda i: fog_busy_until[i])
                start = snap_time(max(arrival_fog, fog_busy_until[idx]))
                end = snap_time(start + fog_time_ms / 1000.0)
                fog_busy_until[idx] = end

                total_latency = (end - task["creation_time_s"]) * 1000 + tx_ms
                energy = config["p_tx"] * (tx_ms / 1000.0)
                queue_delay = (start - task["creation_time_s"]) * 1000
                proc_delay = fog_time_ms
                status = "hit" if total_latency <= task["deadline_ms"] else "miss"

                log = {
                    "task_id": task["task_id"], "device_id": task["device_id"],
                    "decision": "offload", "arrival_time_s": task["creation_time_s"],
                    "queue_enter_time_s": time_now, "start_time_s": start,
                    "end_time_s": end, "queue_delay_ms": queue_delay,
                    "proc_delay_ms": proc_delay, "tx_delay_ms": tx_ms,
                    "total_latency_ms": total_latency, "energy_j": energy,
                    "deadline_ms": task["deadline_ms"], "status": status
                }
                task_logs.append(log)
                sla_vio = total_latency > task["deadline_ms"]
   

            # -----------------
            # DROP
            # -----------------
            elif decision == "drop_by_capacity":
                log = {
                    "task_id": task["task_id"], "device_id": task["device_id"],
                    "decision": "drop", "arrival_time_s": task["creation_time_s"],
                    "queue_enter_time_s": time_now, "start_time_s": None,
                    "end_time_s": None, "queue_delay_ms": None,
                    "proc_delay_ms": 0.0, "tx_delay_ms": None,
                    "total_latency_ms": None, "energy_j": 0.0,
                    "deadline_ms": task["deadline_ms"], "status": "drop"
                }
                task_logs.append(log)
               

    # -----------------------------
    # End of simulation
    # -----------------------------
    link_rows = network.snapshot_link_stats()
    pd.DataFrame(link_rows).to_csv(os.path.join("Results", "link_utilization.csv"), index=False)

    logs, metrics, weights = save_logs_and_metrics(task_logs, weight_logs, "Results")
    print("Simulation done.")
    print(metrics)
