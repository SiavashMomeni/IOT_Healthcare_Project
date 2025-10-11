import pandas as pd
from Controllers.DQL import DeepQLearner
class SDNController:
    def __init__(self, device_weights, config):
        self.device_weights = device_weights
        self.config = config
        self.rl_agent = DeepQLearner(state_dim=6, action_dim=2)
        # buffers to collect transitions within a round if needed
        self.pending = []

    def update_weights(self, recent_logs):
        df = pd.DataFrame(recent_logs)
        if df.empty:
            return


        df["sla_violation"] = df["status"].apply(lambda x: 0 if x=="hit" else 1)

        off_df = df[df.decision=="offload"]
        loc_df = df[df.decision=="local"]

        off_rate = off_df.sla_violation.mean() if not off_df.empty else 0.0
        loc_rate = loc_df.sla_violation.mean() if not loc_df.empty else 0.0

        for _, rec in df.iterrows():
            dev = rec.device_id
            if rec.decision == "offload" and off_rate > loc_rate:
                delta = 0.05
            elif rec.decision == "local" and loc_rate > off_rate:
                delta = -0.03
            else:
                delta = 0.0

            wold_local = self.device_weights[dev]["w_local"]
            wnew_local = min(1.0, max(0.0, wold_local + self.lr*delta))
            self.device_weights[dev]["w_local"] = wnew_local
            self.device_weights[dev]["w_offload"] = 1.0 - wnew_local
    def extract_state(self, task, network, time_now, path_links=None):
        # Build the 6-dim state vector described above
        mean_task_kb = self.config.get("mean_task_size_kb", 450.0)
        typical_deadline = self.config.get("typical_deadline_ms", 200.0)
        s1 = task["size_kb"] / max(1.0, mean_task_kb)
        s2 = task["deadline_ms"] / max(1.0, typical_deadline)
        # device queue length proxy: use device busy flag (0/1)
        dev_busy = 1.0 if network.device_to_node.get(task["device_id"], None) is None else 0.0
        # access utilization estimate at src node
        src_node = network.device_to_node_id(task["device_id"])
        access_reserved = 0.0
        if src_node is not None:
            # sum bits reserved overlapping small window
            access_reserved = network.access_reserved_bits_in_window(src_node, time_now, time_now + 0.01)
        access_util = access_reserved / max(1.0, network.device_access_bw_bps * 0.01)
        # path avg reservation density
        path_util = 0.0
        if path_links:
            vals = []
            for link in path_links:
                # reserved bits in next window (approx)
                vals.append(link.reserved_bits_in_window(time_now, time_now + 0.01) / max(1.0, link.bw_bps * 0.01))
            path_util = float(np.mean(vals))
        # recent latency stat (not always available) - use 0
        recent_latency = 0.0
        return np.array([s1, s2, dev_busy, access_util, path_util, recent_latency], dtype=np.float32)

    def select_action(self, state, eval_mode=False):
        return self.rl_agent.select_action(state, eval_mode=eval_mode)

    def store_transition(self, s,a,r,s2,done):
        self.rl_agent.store_transition(s,a,r,s2,done)

    def train(self):
        return self.rl_agent.train_step()