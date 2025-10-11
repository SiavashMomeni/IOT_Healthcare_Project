import os, heapq, yaml
import pandas as pd, os
from Modules.generator import generate_tasks
from Modules.scheduler import Scheduler
from Modules.sdn_controller import SDNController
from Modules.maths import processing_time_ms
from Modules.network import Network
from Modules.metrics import save_logs_and_metrics
from Modules.utils import snap_time

if __name__ == "__main__":
    with open("config.yml","r", encoding='utf-8') as f:
        config = yaml.safe_load(f)
    for k in ["device_cpu_hz","fog_cpu_hz","cloud_cpu_hz","cycles_per_byte","p_tx","energy_per_cycle","default_link_bw_bps","device_access_bw_bytes_per_s"]:
        if k in config:
         config[k] = float(config[k])

    os.makedirs("Data", exist_ok=True)
    os.makedirs("Results", exist_ok=True)

    tasks = generate_tasks(config, out_path="Data/tasks.json")

    device_weights = {f"dev_{i}": {"w_local":config["initial_weights"]["w_local"], "w_offload":config["initial_weights"]["w_offload"]} for i in range(config["num_devices"])}
    network = Network(topology_path=config.get("topology_path"),
                  default_link_bw_bps=config.get("default_link_bw_bps"),
                  device_access_bw_bytes_per_s=config.get("device_access_bw_bytes_per_s"),
                  default_rtt_s=config.get("default_rtt_s"))
    network.attach_devices(config["num_devices"])
    scheduler = Scheduler(device_weights, config, network)
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
        dev_node = network.device_to_node_id(task["device_id"])
        dest = scheduler.pick_destination_server()  # keep using scheduler for destination selection
        path_nodes, path_links, total_rtt = network.shortest_path_with_links(dev_node, dest)
        state = controller.extract_state(task, network, time_now, path_links=path_links)
        action = controller.select_action(state)   # 0 local, 1 offload
        # map action to decision
        if action == 0:
            decision = "local"
            meta = {"rl_action": action}
        else:
            # before reserving, ask can_transmit
            size_bits = task["size_kb"] * 1024.0 * 8.0
            ok, blocking = network.can_transmit(path_links or [], size_bits, src_node=dev_node, now=time_now)
            if not ok:
                decision = "drop_by_capacity"
                meta = {"blocking": blocking, "rl_action": action}
            else:
                reservations = network.reserve_access_and_path(path_links, size_bits, src_node=dev_node, now=time_now, task_id=task["task_id"])
                decision = "offload"
                meta = {"reservations": reservations, "rl_action": action}
        

        if ev_type=="arrival":
            task = payload
            decision, path_nodes, path_links, meta = scheduler.decide(task, time_now)
                        # debug prints for first ~50 tasks
            # if len(task_logs) < 50:
                # print("DEBUG task:", task["task_id"],
                #       "dec:", decision,
                #       "dev_node:", network.device_to_node.get(task["device_id"]),
                #       "path_len:", len(path_links) if path_links else 0,
                #       "meta_keys:", list(meta.keys()) if isinstance(meta, dict) else meta)
            #     if decision == "offload":
            #         size_bits = task["size_kb"] * 1024.0 * 8.0
            #         ok, blocking = network.can_transmit(path_links if path_links else [], size_bits, src_node=network.device_to_node.get(task["device_id"]), now=time_now)
            #         print("  can_transmit check:", ok, "blocking:", blocking)
            #         # print reservations on involved links
            #         if path_links:
            #             for i,lnk in enumerate(path_links):
            #                 print(f"   link {i}: {lnk.u}->{lnk.v} active_res={len(lnk.reservations)} total_reserved={lnk.total_reserved_bits}")


            # local
            if decision=="local":
                proc_time_ms, cycles = processing_time_ms(task["size_kb"], config["device_cpu_hz"], config["cycles_per_byte"])
                ready_time = device_busy_until[task["device_id"]]
                start = snap_time(max(time_now, ready_time))
                queue_delay = (start - task["creation_time_s"]) * 1000

                if (queue_delay + proc_time_ms) > task["deadline_ms"]:
                    # drop because even with immediate start it won't meet deadline
                    log = {
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

                end = snap_time(start + proc_time_ms/1000.0)
                device_busy_until[task["device_id"]] = end
                total_latency = (end - task["creation_time_s"]) * 1000
                energy = cycles * config["energy_per_cycle"]
                status = "hit" if total_latency <= task["deadline_ms"] else "miss"

                log = {
                    "task_id":task["task_id"],
                    "device_id":task["device_id"],
                    "decision":"local",
                    "arrival_time_s":task["creation_time_s"],
                    "queue_enter_time_s":time_now,
                    "start_time_s":start,
                    "end_time_s":end,
                    "queue_delay_ms":queue_delay,
                    "proc_delay_ms":proc_time_ms,
                    "tx_delay_ms":0.0,
                    "total_latency_ms":total_latency,
                    "energy_j":energy,
                    "deadline_ms":task["deadline_ms"],
                    "status":status
                }
                task_logs.append(log)
                round_acc.append(log)

            # offload
            elif decision=="offload":
                print("offload")
                # meta contains reservations
                reservations = meta.get("reservations", [])
    
                tx_ms = network.estimate_network_delay_ms(path_links, task["size_kb"])
                arrival_fog = snap_time(time_now + tx_ms / 1000.0)

                # schedule on fog worker (same logic as before)
                fog_time_ms, _ = processing_time_ms(task["size_kb"], config["fog_cpu_hz"], config["cycles_per_byte"])
                idx = min(range(len(fog_busy_until)), key=lambda i: fog_busy_until[i])
                start = snap_time(max(arrival_fog, fog_busy_until[idx]))
                queue_delay = (start - task["creation_time_s"]) * 1000
                # print(f"(queue_delay({queue_delay}) + fog_time_ms({fog_time_ms}))={queue_delay + fog_time_ms} > task.deadline_ms={task['deadline_ms']}")
                # if (queue_delay + fog_time_ms) > task["deadline_ms"]:
                #     # drop because won't meet deadline
                #     # also record drop on links if desired
                #     # record drop on the blocking link(s) if present inside meta
                #     for r in reservations:
                #         if r.get("type")=="link":
                #             r["link"].record_drop(r["bits"])
                #     log = {
                #         "task_id":task["task_id"],
                #         "device_id":task["device_id"],
                #         "decision":"offload",
                #         "arrival_time_s":task["creation_time_s"],
                #         "queue_enter_time_s":time_now,
                #         "start_time_s":None,
                #         "end_time_s":None,
                #         "queue_delay_ms":queue_delay,
                #         "proc_delay_ms":0.0,
                #         "tx_delay_ms":tx_ms,
                #         "total_latency_ms":None,
                #         "energy_j":0.0,
                #         "deadline_ms":task["deadline_ms"],
                #         "status":"drop",
                #         "drop_reason":"deadline_miss"
                #     }
                #     task_logs.append(log)
                #     round_acc.append(log)
                #     continue

                end = snap_time(start + fog_time_ms/1000.0)
                fog_busy_until[idx] = end
                proc_delay = fog_time_ms
                total_latency = (end - task["creation_time_s"]) * 1000 + tx_ms
                energy = config["p_tx"] * (tx_ms/1000.0)
                status = "hit" if total_latency <= task["deadline_ms"] else "miss"

                log = {
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

            # drop_by_capacity (scheduler decided)
            elif decision=="drop_by_capacity":
                blocking = meta.get("blocking")
                # if blocking is ("access_link", node) or ("path_link", Link)
                if isinstance(blocking, tuple) and blocking[0]=="access_link":
                    node = blocking[1]
                    # optionally record drop stats for node access (not implemented)
                elif isinstance(blocking, tuple) and blocking[0]=="path_link":
                    link = blocking[1]
                    if hasattr(link, "record_drop"):
                        link.record_drop(task["size_kb"] * 1024.0 * 8.0)

                log = {
                    "task_id":task["task_id"],
                    "device_id":task["device_id"],
                    "decision":"drop",
                    "drop_reason":"link_capacity",
                    "arrival_time_s":task["creation_time_s"],
                    "queue_enter_time_s":time_now,
                    "start_time_s":None,
                    "end_time_s":None,
                    "queue_delay_ms":None,
                    "proc_delay_ms":0.0,
                    "tx_delay_ms":None,
                    "total_latency_ms":None,
                    "energy_j":0.0,
                    "deadline_ms":task["deadline_ms"],
                    "status":"drop"
                }
                task_logs.append(log)
                round_acc.append(log)
                    
            next_state = controller.extract_state(task, network, Q1A, path_links=None)
            latency = log["total_latency_ms"] if log["total_latency_ms"] is not None else (log.get("proc_delay_ms",0) + log.get("tx_delay_ms",0))
            # reward design:
            reward = - latency / max(1.0, task["deadline_ms"])    # negative: lower latency -> larger reward
            if log["status"] == "drop":
                reward -= 10.0
            controller.store_transition(state, action, reward, next_state, done=0.0)
            # optionally: train a bit each step/round
            loss = controller.train()

        if len(round_acc)>=config["round_size"]:
            controller.update_weights(round_acc)
            snapshot = {"round":len(weight_logs)+1}
            for dev, ws in device_weights.items():
                snapshot[f"{dev}_w_local"] = ws["w_local"]
                snapshot[f"{dev}_w_offload"] = ws["w_offload"]
            weight_logs.append(snapshot)
            round_acc=[]

    link_rows = network.snapshot_link_stats()
    pd.DataFrame(link_rows).to_csv(os.path.join("Results", "link_utilization.csv"), index=False)
    logs,metrics,weights=save_logs_and_metrics(task_logs, weight_logs,"Results")
    print("Simulation done. Logs:",logs)
    print("Metrics:",metrics)
    print("Weights:",weights)
