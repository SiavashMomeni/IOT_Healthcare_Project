import os, heapq, yaml
from Modules.generator import generate_tasks
from Modules.scheduler import Scheduler
from Modules.sdn_controller import SDNController
from Modules.maths import processing_time_ms
from Modules.network import transmission_time_ms
from Modules.metrics import save_logs_and_metrics

if __name__ == "__main__":
    with open("config.yml","r") as f:
        config = yaml.safe_load(f)
    for key in ["device_cpu_hz","fog_cpu_hz","cloud_cpu_hz","cycles_per_byte","p_tx","energy_per_cycle"]:
        if key in config:
            config[key] = float(config[key])  
              
    os.makedirs("Data", exist_ok=True)
    os.makedirs("Results", exist_ok=True)
    tasks = generate_tasks(config, out_path="Data/tasks.json")
    device_weights = {f"dev_{i}": {"w_local":config["initial_weights"]["w_local"], "w_offload":config["initial_weights"]["w_offload"]} for i in range(config["num_devices"])}
    scheduler = Scheduler(device_weights, config)
    controller = SDNController(device_weights, config)
    events = []
    for task in tasks:
        heapq.heappush(events,(task["creation_time_s"],"arrival",task))
    device_busy_until = {d:0.0 for d in device_weights}
    fog_busy_until = [0.0]*config["fog_workers"]
    task_logs = []
    round_acc = []
    while events:
        time_now, ev_type, payload = heapq.heappop(events)
        if ev_type=="arrival":
            task = payload
            decision = scheduler.decide(task)
            if decision=="local":
                local_time_ms, local_cycles = processing_time_ms(task["size_kb"], config["device_cpu_hz"], config["cycles_per_byte"])
                start = max(time_now, device_busy_until[task["device_id"]])
                end = start+local_time_ms/1000.0
                device_busy_until[task["device_id"]]=end
                latency_ms=(end-task["creation_time_s"])*1000
                energy=local_cycles*config["energy_per_cycle"]
                sla=0 if latency_ms<=task["deadline_ms"] else 1
                log={"task_id":task["task_id"],"device_id":task["device_id"],"decision":"local","latency_ms":latency_ms,"energy_j":energy,"sla_violation":sla}
                task_logs.append(log)
                round_acc.append(log)
            else:
                tx_ms=transmission_time_ms(task["size_kb"],"fog")
                fog_time_ms,_=processing_time_ms(task["size_kb"],config["fog_cpu_hz"],config["cycles_per_byte"])
                idx=min(range(len(fog_busy_until)),key=lambda i:fog_busy_until[i])
                arrival_fog=time_now+tx_ms/1000.0
                start=max(arrival_fog,fog_busy_until[idx])
                end=start+fog_time_ms/1000.0
                fog_busy_until[idx]=end
                latency_ms=(end-task["creation_time_s"])*1000+tx_ms
                energy=config["p_tx"]*(tx_ms/1000.0)
                sla=0 if latency_ms<=task["deadline_ms"] else 1
                log={"task_id":task["task_id"],"device_id":task["device_id"],"decision":"offload","latency_ms":latency_ms,"energy_j":energy,"sla_violation":sla}
                task_logs.append(log)
                round_acc.append(log)
        if len(round_acc)>=config["round_size"]:
            controller.update_weights(round_acc)
            round_acc=[]
    logs,metrics=save_logs_and_metrics(task_logs,"Results")
    print("Simulation done. Logs:",logs)
    print("Metrics:",metrics)
