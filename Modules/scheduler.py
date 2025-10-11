# Modules/scheduler.py
from .maths import processing_time_ms
from .utils import snap_time

class Scheduler:
    def __init__(self, device_weights, config, network):
        self.device_weights = device_weights
        self.config = config
        self.network = network
        self.server_nodes = [nid for nid, n in network.nodes.items() if n.get("color") == "blue"]

    def pick_destination_server(self):
        best = None
        best_load = float("inf")
        for s in self.server_nodes:
            load = 0
            for nbr, link in self.network.adj[s]:
                load += len(link.reservations)
            load += len(self.network.access_reservations.get(s, []))
            if load < best_load:
                best_load = load
                best = s
        return best

    def decide(self, task, time_now):
        """
        Return (decision, path_nodes, path_links, meta)
        decision in {'local', 'offload', 'drop_by_capacity'}
        """
        dev = task["device_id"]
        w = self.device_weights.get(dev, self.config["initial_weights"])
        local_time_ms, _ = processing_time_ms(task["size_kb"], self.config["device_cpu_hz"], self.config["cycles_per_byte"])
        local_norm = local_time_ms / task["deadline_ms"] if task["deadline_ms"]>0 else float('inf')

        dest = self.pick_destination_server()
        if dest is None:
            return "local", None, None, {"reason":"no_server"}

        dev_node = self.network.device_to_node.get(dev)
        if dev_node is None:
            dev_node = int(dev.split("_")[1]) % self.network.node_count

        path_nodes, path_links = self.network.find_path(dev_node, dest, weight="rtt")
    
        if path_nodes is None:
            return "local", None, None, {"reason":"no_path"}

        # estimate offload time
        size_bytes = task["size_kb"] * 1024.0
        size_bits = size_bytes * 8.0
        tx_s = 0.0
        for link in path_links:
            tx_s += size_bits / link.bw_bps
        fog_proc_ms, _ = processing_time_ms(task["size_kb"], self.config["fog_cpu_hz"], self.config["cycles_per_byte"])
        offload_time_ms = (tx_s * 1000.0) + fog_proc_ms
        offload_norm = offload_time_ms / task["deadline_ms"] if task["deadline_ms"]>0 else float('inf')

        score_local = w["w_local"] * local_norm
        score_offload = w["w_offload"] * offload_norm

        if score_local <= score_offload:
            return "local", None, None, {"reason":"score_local"}
        
        # else try offload -> check capacity including device access
        ok, blocking = self.network.can_transmit(path_links, size_bits, src_node=dev_node, now=time_now, safety_factor=1)
        if ok:
            reservations = self.network.reserve_access_and_path(path_links, size_bits, src_node=dev_node, now=time_now, task_id=task["task_id"])
            print("dfas")
            return "offload", path_nodes, path_links, {"reservations": reservations}
        else:
            return "drop_by_capacity", None, [blocking], {"reason":"link_capacity","blocking":blocking}
