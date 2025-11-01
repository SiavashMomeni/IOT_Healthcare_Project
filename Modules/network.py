import json
import heapq
from collections import defaultdict

class Link:
    def __init__(self, u, v, bw_bps=1e9, rtt_s=0.002):
        self.u = int(u)
        self.v = int(v)
        self.bw_bps = float(bw_bps)   # bits per second
        self.rtt_s = float(rtt_s)     # seconds (prop/RTT contribution)
        self.delay_ms = 1.0e9 / bw_bps
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
    
    def k_shortest_paths(self, src, dst, K=3):
        """
        Find up to K shortest paths using Yen's algorithm.
        Returns a list of (path_nodes, path_links, total_rtt)
        """
        # Step 1: shortest path
        path_nodes, path_links = self.find_path(src, dst)
        if not path_nodes:
            return []

        def path_total_rtt(p_links):
            return sum(l.rtt_s for l in p_links)

        A = [(path_nodes, path_links, path_total_rtt(path_links))]  # shortest paths found
        B = []  # candidate paths

        for k in range(1, K):
            for i in range(len(A[k-1][0]) - 1):
                spur_node = A[k-1][0][i]
                root_path_nodes = A[k-1][0][:i+1]
                root_path_links = A[k-1][1][:i]

                # Copy network and remove links in previous paths
                removed_links = []
                for p_nodes, p_links, _ in A:
                    if len(p_nodes) > i and p_nodes[:i+1] == root_path_nodes:
                        u = p_nodes[i]
                        v = p_nodes[i+1]
                        # remove link (u,v)
                        for nbr, link in self.adj[u]:
                            if nbr == v:
                                self.adj[u].remove((nbr, link))
                                removed_links.append((u, nbr, link))
                                break

                # Spur path from spur_node to dst
                spur_path_nodes, spur_path_links = self.find_path(spur_node, dst)
                if spur_path_nodes and len(spur_path_nodes) > 1:
                    total_path_nodes = root_path_nodes[:-1] + spur_path_nodes
                    total_path_links = root_path_links + spur_path_links
                    total_rtt = path_total_rtt(total_path_links)
                    if not any(pn == total_path_nodes for pn, _, _ in B):
                        B.append((total_path_nodes, total_path_links, total_rtt))

                # Restore removed links
                for (u, v, link) in removed_links:
                    self.adj[u].append((v, link))

            if not B:
                break

            B.sort(key=lambda x: x[2])
            A.append(B[0])
            B.pop(0)

        return A

    def clean_all_links(self, now):
        for link in self.links:
            link.cleanup(now)

    def can_reserve_on_path(self, path_links, size_kb, now, safety_factor=0.95):
        """
        Check if we can reserve enough bandwidth along a path for a given task size.
        size_kb: task size in KB
        safety_factor: fraction of link capacity considered usable (e.g. 0.95 means 95%)
        Returns (True, None) if successful, else (False, blocking_link)
        """
        size_bits = size_kb * 8 * 1024.0

        # Cleanup old reservations
        for link in path_links:
            link.cleanup(now)

        for link in path_links:
            # Define a reasonable time window (e.g. 50ms)
            window_dur = max(link.delay_ms / 1000.0, 0.05)  # seconds
            window_start = now
            window_end = now + window_dur

            # Current reserved bits in this time window
            reserved = link.reserved_bits_in_window(window_start, window_end)

            # Max capacity available in this window
            capacity_bits_in_window = link.bw_bps * window_dur * safety_factor

            ratio = (reserved + size_bits) / capacity_bits_in_window
            print(f"[DEBUG] can_reserve_on_path link=({link.u}->{link.v}), "
                f"reserved={reserved:.2f}, capacity={capacity_bits_in_window:.2f}, ratio={ratio:.3f}")

            # If reservation exceeds safe capacity, block it
            if (reserved + size_bits) > capacity_bits_in_window:
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
            print(f"[DEBUG] can_transmit reserved={reserved_access_bits:.2f}, capacity={capacity_access_bits:.2f}, ratio={reserved_access_bits / capacity_access_bits:.3f}")
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
    
    def estimate_network_delay_ms(self, path_links, size_kb):
        size_bits = size_kb * 1024 * 8
        access_bw = self.device_access_bw_bps
        total_delay_s = size_bits / access_bw
        for link in path_links:
            total_delay_s += (size_bits / link.bw_bps) 
        return total_delay_s * 1000  # ms
    

