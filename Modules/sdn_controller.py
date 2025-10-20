import pandas as pd
import numpy as np
from Controllers.DQL import DQNRouter

class SDNController:
    def __init__(self, device_weights, network, config):
        self.device_weights = device_weights
        self.network = network
        self.config = config
        self.lr = 1e-2  # نرخ یادگیری برای وزن‌های دستگاه‌ها

        # Deep Q-Network router
        self.router_agent = DQNRouter(
            n_states=2,     # src, dst, u_local, mean_delay, std_delay
            n_actions=3,    # تعداد مسیرهای کاندید
            lr=1e-3,
            gamma=0.9,
            epsilon=0.1
        )

        # بافر برای اپیزودهای RL در صورت نیاز
        self.pending = []

    # ------------------------------------------------------------------
    def update_weights(self, recent_logs):
        df = pd.DataFrame(recent_logs)
        if df.empty:
            return

        df["sla_violation"] = df["status"].apply(lambda x: 0 if x == "hit" else 1)
        off_df = df[df.decision == "offload"]
        loc_df = df[df.decision == "local"]

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
            wnew_local = min(1.0, max(0.0, wold_local + self.lr * delta))
            self.device_weights[dev]["w_local"] = wnew_local
            self.device_weights[dev]["w_offload"] = 1.0 - wnew_local

    # ------------------------------------------------------------------
    def select_path(self, src, dst):
        """
        انتخاب مسیر با DQN بین k مسیر کوتاه‌ترین.
        """
        # گرفتن 5 مسیر کاندید از توپولوژی (باید در network پیاده‌سازی شده باشه)
        paths = self.network.k_shortest_paths(src, dst, k=5)
        if not paths:
            return None

        # تاخیر تخمینی هر مسیر
        delays = [self.network.estimate_network_delay_ms(p) for p in paths]
        mean_delay = np.mean(delays)
        std_delay = np.std(delays)

        # ضریب local/offload فعلی
        u_local = self.device_weights[src].get("w_local", 0.5)

        # ساخت state برای DQN
        state = np.array([src, dst, u_local, mean_delay, std_delay], dtype=np.float32)

        # انتخاب مسیر توسط DQN
        action = self.router_agent.select_action(state)
        chosen_path = paths[action % len(paths)]  # احتیاط در صورت کمتر بودن مسیرها

        # محاسبه پاداش (منفی تاخیر و انحراف معیار)
        reward = - (delays[action] + 0.3 * std_delay)

        # حالت بعدی (ساده‌سازی: همون state با delay مسیر انتخاب‌شده)
        next_state = np.array([src, dst, u_local, delays[action], std_delay], dtype=np.float32)

        # ذخیره در حافظه RL و آموزش مرحله‌ای
        self.router_agent.store((state, action, reward, next_state))
        self.router_agent.train_step()

        path_nodes = chosen_path
        path_links = [(chosen_path[i], chosen_path[i+1]) for i in range(len(chosen_path)-1)]

        return path_nodes, path_links, delays[action]

    # ------------------------------------------------------------------
    def get_device_weights(self, dev_id):
        """برای debug"""
        return self.device_weights.get(dev_id, {"w_local": 0.5, "w_offload": 0.5})
