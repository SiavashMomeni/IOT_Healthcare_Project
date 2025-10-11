import json
import heapq
from collections import defaultdict

class Link:
    def __init__(self, u, v, bw_bps=1e9, rtt_s=0.005):
        self.u = int(u)
        self.v = int(v)
        self.bw_bps = float(bw_bps)   # bits per second
        self.rtt_s = float(rtt_s)     # seconds (prop/RTT contribution)
        self.reservations = []        # list of dict: {"start":, "finish":, "bits":, "task_id":}
        # stats
        self.total_reserved_bits = 0.0
        self.total_dropped_bits = 0.0
        self.drop_count = 0

    def cleanup(self, now):
        before = len(self.reservations)
        self.reservations = [r for r in self.reservations if r["finish"] > now]
        return before - len(self.reservations)

    def reserved_bits_in_window(self, window_start, window_end):
        s = 0.0
        for r in self.reservations:
            if r["finish"] <= window_start or r["start"] >= window_end:
                continue
            s += r["bits"]
        return s

    def add_reservation(self, start, finish, bits, task_id=None):
        self.reservations.append({"start": start, "finish": finish, "bits": bits, "task_id": task_id})
        self.total_reserved_bits += bits

    def record_drop(self, bits):
        self.total_dropped_bits += bits
        self.drop_count += 1


class Network:
    def __init__(self, topology_path, default_link_bw_bps, device_access_bw_bytes_per_s, default_rtt_s):
        """
        topology_path: json file path (nodes, links)
        default_link_bw_bps: bits/s for backbone links (1e9)
        device_access_bw_bytes_per_s: bytes/s for device->access link (e.g. 40e6 bytes/s)
        default_rtt_s: per-link rtt (seconds)
        """
        self.nodes = {}         
        self.adj = defaultdict(list)   # node -> list of (neighbor, link_obj)
        self.links = []        
        self.node_count = 0
        self.device_to_node = {} # dev_id (str) -> node_id (int)
        self.default_link_bw_bps = float(default_link_bw_bps)
   
        self.device_access_bw_bps = float(device_access_bw_bytes_per_s) * 8.0
        self.default_rtt_s = float(default_rtt_s)

 
        self.access_reservations = defaultdict(list)

        if topology_path:
            self.load_topology(topology_path)

    def load_topology(self, topology_path):
        with open(topology_path, "r") as f:
            topo = json.load(f)

        for n in topo.get("nodes", []):
            nid = int(n["id"])
            self.nodes[nid] = n
        self.node_count = len(self.nodes)

        for l in topo.get("links", []):
            u = int(l["source"])
            v = int(l["target"])
            link = Link(u, v, bw_bps=self.default_link_bw_bps, rtt_s=self.default_rtt_s)
            self.links.append(link)
            self.adj[u].append((v, link))
            self.adj[v].append((u, link))

    def attach_devices(self, num_devices):
        """
        Attach devices to nodes deterministically: dev_i -> node (i % node_count).
        """
        if self.node_count == 0:
            raise RuntimeError("Topology not loaded")
        for i in range(num_devices):
            self.device_to_node[f"dev_{i}"] = i % self.node_count

    def find_path(self, src_node, dst_node, weight="rtt"):
        """
        Dijkstra on nodes using link.rtt_s as weight (can be extended).
        Returns (path_nodes_list, path_links_list) or (None, None) if no path.
        """
        dist = {n: float("inf") for n in self.nodes}
        prev = {n: None for n in self.nodes}
        prev_link = {n: None for n in self.nodes}
        dist[src_node] = 0.0
        pq = [(0.0, src_node)]
        visited = set()
        while pq:
            d,u = heapq.heappop(pq)
            if u in visited:
                continue
            visited.add(u)
            if u == dst_node:
                break
            for v, link in self.adj[u]:
                w = link.rtt_s
                nd = d + w
                if nd < dist[v]:
                    dist[v] = nd
                    prev[v] = u
                    prev_link[v] = link
                    heapq.heappush(pq, (nd, v))
        if dist[dst_node] == float("inf"):
            return None, None

        path_nodes = []
        path_links = []
        cur = dst_node
        while cur is not None:
            path_nodes.append(cur)
            if prev_link[cur] is not None:
                path_links.append(prev_link[cur])
            cur = prev[cur]
        path_nodes.reverse()
        path_links.reverse()
        return path_nodes, path_links

    def clean_all_links(self, now):
        for link in self.links:
            link.cleanup(now)

    def can_reserve_on_path(self, path_links, size_kb, now, safety_factor=0.95):
        """
        window reservation check for each link: if reserved + this task bits > capacity_in_window * safety_factor -> fail
        size_kb in KB
        Returns (True, None) or (False, blocking_link)
        """
        size_bytes = size_kb * 1024.0
        size_bits = size_bytes * 8.0
        # cleanup old reservations
        for link in path_links:
            link.cleanup(now)

        for link in path_links:
            transfer_time = size_bits / link.bw_bps
            if transfer_time <= 0:
                return False, link
            window_start = now
            window_end = now + transfer_time
            reserved = link.reserved_bits_in_window(window_start, window_end)
            capacity_bits_in_window = link.bw_bps * transfer_time
            if (reserved + size_bits) > (capacity_bits_in_window * safety_factor):
                return False, link
        return True, None

    def reserve_on_path(self, path_links, size_kb, now, task_id=None):
        """
        Register reservations on each link and return reservations list.
        """
        size_bytes = size_kb * 1024.0
        size_bits = size_bytes * 8.0
        reservations = []
        for link in path_links:
            transfer_time = size_bits / link.bw_bps
            start = now
            finish = now + transfer_time
            link.add_reservation(start, finish, size_bits, task_id=task_id)
            reservations.append({"link": link, "start": start, "finish": finish, "bits": size_bits})
        return reservations

    def snapshot_link_stats(self):
        rows = []
        for i, link in enumerate(self.links):
            rows.append({
                "link_idx": i,
                "u": link.u,
                "v": link.v,
                "bw_bps": link.bw_bps,
                "active_reservations": len(link.reservations),
                "total_reserved_bits": link.total_reserved_bits,
                "total_dropped_bits": link.total_dropped_bits,
                "drop_count": link.drop_count
            })
        return rows

    def device_to_node_id(self, dev_id):
        return self.device_to_node.get(dev_id, None)

    def get_least_loaded_node(self, candidate_nodes):
        best = None
        best_load = float("inf")
        for n in candidate_nodes:
            load = 0
            for nbr, link in self.adj[n]:
                load += len(link.reservations)
            load += len(self.access_reservations.get(n, []))
            if load < best_load:
                best_load = load
                best = n
        return best

    def shortest_path_with_links(self, src_node, dst_node, weight="rtt"):
        path_nodes, path_links = self.find_path(src_node, dst_node, weight=weight)
        if path_nodes is None:
            return None, None, None
        total_rtt = sum(l.rtt_s for l in path_links)
        return path_nodes, path_links, total_rtt

    def access_cleanup(self, node_id, now):
        before = len(self.access_reservations[node_id])
        self.access_reservations[node_id] = [r for r in self.access_reservations[node_id] if r["finish"] > now]
        return before - len(self.access_reservations[node_id])

    def access_reserved_bits_in_window(self, node_id, window_start, window_end):
        s = 0.0
        for r in self.access_reservations[node_id]:
            if r["finish"] <= window_start or r["start"] >= window_end:
                continue
            s += r["bits"]
        return s

    def add_access_reservation(self, node_id, start, finish, bits, task_id=None):
        self.access_reservations[node_id].append({"start": start, "finish": finish, "bits": bits, "task_id": task_id})

    def can_transmit(self, path_links, size_bits, src_node=None, now=0.0, safety_factor=0.95): 
        if src_node is not None:
            self.access_cleanup(src_node, now)
            access_bw = self.device_access_bw_bps
            if access_bw <= 0:
                return False, ("access_link", src_node)
            
            access_transfer_time = size_bits / access_bw
            a_start = now
            a_end = now + access_transfer_time
            reserved_access_bits = self.access_reserved_bits_in_window(src_node, a_start, a_end)
            capacity_access_bits = access_bw * access_transfer_time
            if reserved_access_bits > (capacity_access_bits * safety_factor):
                return False, ("access_link", src_node)
                        
        # check path links
        size_bytes = size_bits / 8.0
        size_kb = size_bytes / 1024.0
        ok, blocking = self.can_reserve_on_path(path_links, size_kb, now, safety_factor=safety_factor)
        if not ok:
            return False, ("path_link", blocking)
        return True, None

    def reserve_access_and_path(self, path_links, size_bits, src_node=None, now=0.0, task_id=None):
        reservations = []
        if src_node is not None:
            access_bw = self.device_access_bw_bps
            transfer_time = size_bits / access_bw
            start = now
            finish = now + transfer_time
            self.add_access_reservation(src_node, start, finish, size_bits, task_id=task_id)
            reservations.append({"type": "access", "node": src_node, "start": start, "finish": finish, "bits": size_bits, "task_id": task_id})
        size_bytes = size_bits / 8.0
        size_kb = size_bytes / 1024.0
        path_res = self.reserve_on_path(path_links, size_kb, now, task_id=task_id)
        for r in path_res:
            reservations.append({"type":"link","link": r["link"], "start": r["start"], "finish": r["finish"], "bits": r["bits"], "task_id": task_id})
        return reservations
