# Updated IoT Healthcare Simulation with drop handling
# - Now tasks can be dropped if queue_delay > deadline.

# =============================
# File: main.py (excerpt with changes)
# =============================
import os, heapq, yaml
from Modules.generator import generate_tasks
from Modules.scheduler import Scheduler
from Modules.sdn_controller import SDNController
from Modules.maths import processing_time_ms
from Modules.network import transmission_time_ms
from Modules.metrics import save_logs_and_metrics
from Modules.utils import snap_time

if __name__ == "__main__":
    with open("config.yml","r") as f:
        config = yaml.safe_load(f)
    for k in ["device_cpu_hz","fog_cpu_hz","cloud_cpu_hz","cycles_per_byte","p_tx","energy_per_cycle"]:
        if k in config:
         config[k] = float(config[k])

    os.makedirs("Data", exist_ok=True)
    os.makedirs("Results", exist_ok=True)

    tasks = generate_tasks(config, out_path="Data/tasks.json")

    device_weights = {f"dev_{i}": {"w_local":config["initial_weights"]["w_local"], "w_offload":config["initial_weights"]["w_offload"]} for i in range(config["num_devices"])}
    scheduler = Scheduler(device_weights, config)
    controller = SDNController(device_weights, config)

    events = []
    counter = 0
    for task in tasks:
        heapq.heappush(events,(snap_time(task["creation_time_s"]),"arrival",counter,task))
        counter += 1

    device_busy_until = {d:0.0 for d in device_weights}
    fog_busy_until = [0.0]*config["fog_workers"]

    task_logs = []
    round_acc = []
    weight_logs = []

    while events:
        time_now, ev_type, _, payload = heapq.heappop(events)
        time_now = snap_time(time_now)

        if ev_type=="arrival":
            task = payload
            decision = scheduler.decide(task)

            if decision=="local":
                proc_time_ms, cycles = processing_time_ms(task["size_kb"], config["device_cpu_hz"], config["cycles_per_byte"])
                start = snap_time(max(time_now, device_busy_until[task["device_id"]]))
                queue_delay = (start - task["creation_time_s"]) * 1000

                # check for drop
                if queue_delay > task["deadline_ms"]:
                    log={
                        "task_id":task["task_id"],
                        "device_id":task["device_id"],
                        "decision":"local",
                        "arrival_time_s":task["creation_time_s"],
                        "queue_enter_time_s":time_now,
                        "start_time_s":None,
                        "end_time_s":None,
                        "queue_delay_ms":queue_delay,
                        "proc_delay_ms":0.0,
                        "tx_delay_ms":0.0,
                        "total_latency_ms":None,
                        "energy_j":0.0,
                        "deadline_ms":task["deadline_ms"],
                        "status":"drop"
                    }
                    task_logs.append(log)
                    round_acc.append(log)
                    continue

                end = snap_time(start+proc_time_ms/1000.0)
                proc_delay = proc_time_ms
                tx_delay = 0.0
                device_busy_until[task["device_id"]]=end

                total_latency = (end - task["creation_time_s"]) * 1000
                energy = cycles*config["energy_per_cycle"]
                status = "hit" if total_latency <= task["deadline_ms"] else "miss"

                log={
                    "task_id":task["task_id"],
                    "device_id":task["device_id"],
                    "decision":"local",
                    "arrival_time_s":task["creation_time_s"],
                    "queue_enter_time_s":time_now,
                    "start_time_s":start,
                    "end_time_s":end,
                    "queue_delay_ms":queue_delay,
                    "proc_delay_ms":proc_delay,
                    "tx_delay_ms":tx_delay,
                    "total_latency_ms":total_latency,
                    "energy_j":energy,
                    "deadline_ms":task["deadline_ms"],
                    "status":status
                }
                task_logs.append(log)
                round_acc.append(log)

            else:
                tx_ms=transmission_time_ms(task["size_kb"],"fog")
                fog_time_ms,_=processing_time_ms(task["size_kb"],config["fog_cpu_hz"],config["cycles_per_byte"])
                idx=min(range(len(fog_busy_until)),key=lambda i:fog_busy_until[i])
                arrival_fog=snap_time(time_now+tx_ms/1000.0)
                start=snap_time(max(arrival_fog,fog_busy_until[idx]))
                queue_delay = (start - task["creation_time_s"]) * 1000

                # check for drop
                if queue_delay > task["deadline_ms"]:
                    log={
                        "task_id":task["task_id"],
                        "device_id":task["device_id"],
                        "decision":"offload",
                        "arrival_time_s":task["creation_time_s"],
                        "queue_enter_time_s":time_now,
                        "start_time_s":None,
                        "end_time_s":None,
                        "queue_delay_ms":queue_delay,
                        "proc_delay_ms":0.0,
                        "tx_delay_ms":tx_ms,
                        "total_latency_ms":None,
                        "energy_j":0.0,
                        "deadline_ms":task["deadline_ms"],
                        "status":"drop"
                    }
                    task_logs.append(log)
                    round_acc.append(log)
                    continue

                end=snap_time(start+fog_time_ms/1000.0)
                fog_busy_until[idx]=end

                proc_delay = fog_time_ms
                total_latency = (end-task["creation_time_s"])*1000 + tx_ms
                energy = config["p_tx"]*(tx_ms/1000.0)
                status = "hit" if total_latency <= task["deadline_ms"] else "miss"

                log={
                    "task_id":task["task_id"],
                    "device_id":task["device_id"],
                    "decision":"offload",
                    "arrival_time_s":task["creation_time_s"],
                    "queue_enter_time_s":time_now,
                    "start_time_s":start,
                    "end_time_s":end,
                    "queue_delay_ms":queue_delay,
                    "proc_delay_ms":proc_delay,
                    "tx_delay_ms":tx_ms,
                    "total_latency_ms":total_latency,
                    "energy_j":energy,
                    "deadline_ms":task["deadline_ms"],
                    "status":status
                }
                task_logs.append(log)
                round_acc.append(log)

        if len(round_acc)>=config["round_size"]:
            controller.update_weights(round_acc)
            snapshot = {"round":len(weight_logs)+1}
            for dev, ws in device_weights.items():
                snapshot[f"{dev}_w_local"] = ws["w_local"]
                snapshot[f"{dev}_w_offload"] = ws["w_offload"]
            weight_logs.append(snapshot)
            round_acc=[]

    logs,metrics,weights=save_logs_and_metrics(task_logs, weight_logs,"Results")
    print("Simulation done. Logs:",logs)
    print("Metrics:",metrics)
    print("Weights:",weights)
