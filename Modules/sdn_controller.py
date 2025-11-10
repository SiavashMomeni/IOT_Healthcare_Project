# Controllers/sdn_controller.py
"""
SDNController: رابط بین شبکه (Network) و عامل‌های RL.
این ماژول دو عامل DQN را نگهداری می‌کند:
 - decision_agent: تصمیم local (0) یا offload (1)
 - router_agent: انتخاب مسیر از بین کاندیدها (0..K-1)

توابع مهم:
 - select_decision(device_id, task, time_now)
 - select_path(src_node, dst_node, size_kb, time_now)
 - get_candidate_paths(...)  # wrapper روی network
"""
import numpy as np
import pandas as pd
from Controllers.DQL import DeepQLearner
from Modules.metrics import record_decision_reward, record_selection_reward, get_decision_reward_stats, get_selection_reward_stats

class SDNController:
    def __init__(self, network, config):
        self.network = network
        self.config = config

        # پارامترها
        self.k_paths = config.get("k_paths", 5)
        self.decision_state_dim = config.get("decision_state_dim", 2)  # تغییرپذیر
        self.router_state_dim = config.get("router_state_dim", 1)
        self.decision_action_dim = 2   # {0: local, 1: offload}
        self.router_action_dim = self.k_paths  # انتخاب یکی از k مسیر

        # عوامل DQN (از DeepQLearner موجود در Controllers/DQL.py استفاده می‌کنیم)
        self.decision_agent = DeepQLearner(
            state_dim=self.decision_state_dim,
            action_dim=self.decision_action_dim,
            lr=5e-4,                # ← learning rate کمتر برای پایداری بهتر
            gamma=0.95,             # ← تخفیف کمتر برای تمرکز روی پاداش‌های نزدیک‌تر
            epsilon=0.9             # ← شروع با exploration بالا
        )

        self.router_agent = DeepQLearner(
            state_dim=self.router_state_dim,
            action_dim=self.router_action_dim,
            lr=3e-4,                # ← یادگیری نرم‌تر برای جلوگیری از stuck شدن
            gamma=0.99,
            epsilon=0.9             # ← exploration بالا
        )

    # -----------------------
# --- درون SDNController ---

    def get_candidate_paths(self, src_node, dst_node):
        """
        برمی‌گرداند لیست تمام مسیرهای ثابت بین src و dst.
        اگر قبلاً در cache موجود است، از آن استفاده می‌کند.
        """
        key = (src_node, dst_node)
        if not hasattr(self, "_path_cache"):
            self._path_cache = {}

        if key not in self._path_cache:
            k = self.k_paths
            if hasattr(self.network, "k_shortest_paths"):
                paths = self.network.k_shortest_paths(src_node, dst_node, K=k)
                formatted = []
                for p in paths:
                    if isinstance(p, tuple) and len(p) >= 2:
                        formatted.append((p[0], p[1]))  # (nodes, links)
                    else:
                        formatted.append((p, None))
                self._path_cache[key] = formatted
            else:
                # فقط کوتاه‌ترین مسیر
                path_nodes, path_links = self.network.find_path(src_node, dst_node, weight="rtt")
                if path_nodes:
                    self._path_cache[key] = [(path_nodes, path_links)]
                else:
                    self._path_cache[key] = []
        return self._path_cache[key]

    # -------------------------------------------------------
    def select_path(self, src_node, dst_node, size_kb, time_now):
        """
        انتخاب مسیر بر اساس state (src,dst)
        - state_id عدد یکتا برای (src,dst)
        - اکشن = شماره مسیر
        - پاداش = -delay
        """
        candidates = self.get_candidate_paths(src_node, dst_node)
        if not candidates:
            return None, None, None

        # ساخت شناسه یکتا برای state
        state_id = self._get_state_id(src_node, dst_node)
        state = np.array([state_id], dtype=np.float32)

        # انتخاب اکشن از بین مسیرها
        action = int(self.router_agent.select_action(state))
        idx = action % len(candidates)
        path_nodes, path_links = candidates[idx]

        # محاسبه تأخیر مسیر انتخابی
        delay_ms = self.network.estimate_network_delay_ms(path_links, size_kb)
        reward = -float(delay_ms)
        decision_avg, decision_std = get_selection_reward_stats()
        reward_norm = (reward - decision_avg) / (decision_std + 1e-6)
        record_selection_reward(reward)

        # آموزش DQN روی همین state ثابت
        next_state = state.copy()
        try:
            transition = (state, action, reward_norm, next_state)
            self.router_agent.store(transition)
            self.router_agent.train_step()
        except Exception as e:
            print(f"[ERROR] router agent update failed: {e}")
            pass

        return path_nodes, path_links, delay_ms

    # -------------------------------------------------------
    def _get_state_id(self, src_node, dst_node):
        """برمی‌گرداند یک اندیس عددی یکتا برای جفت (src, dst)"""
        if not hasattr(self, "_state_index_map"):
            self._state_index_map = {}
            self._next_state_id = 0

        key = (src_node, dst_node)
        if key not in self._state_index_map:
            self._state_index_map[key] = self._next_state_id
            self._next_state_id += 1
        return self._state_index_map[key]


    # -----------------------
    def estimate_path_delay_ms(self, path_links, size_kb):
        """اگر لینک‌ها داده شدند، از network.estimate_network_delay_ms استفاده کن"""
        if path_links is None:
            # اگر فقط nodes داده شد، محاسبه را با find_path دوباره انجام بده
            return None
        return self.network.estimate_network_delay_ms(path_links, size_kb)

    # -----------------------
    def select_decision(self, device_id, task, time_now):
        """
        تصمیم‌گیری local vs offload با استفاده از decision_agent.
        state ساده: [device_load_fraction, recent_offload_ratio, mean_path_util]
        توجه: این state می‌توانید بنا بر نیاز تغییر بدی.
        خروجی: (decision, d_state, d_action)
        """
        # نمونه‌سازی state (ساده و قابل سفارشی‌سازی)
        # device load proxy:
        dev_node = self.network.device_to_node_id(device_id)
        if dev_node is None:
            dev_node = int(device_id.split("_")[1]) % self.network.node_count

        # load estimate: میانگین تعداد رزرو‌ها روی لینک‌های خروجی
        loads = []
        for nbr, link in self.network.adj[dev_node]:
            loads.append(len(link.reservations))
        device_load = float(np.mean(loads)) if loads else 0.0
        device_load_norm = device_load / max(1.0, self.config.get("load_norm_div", 10.0))

        # استخراج recent_offload_ratio از متریک‌ها یا لاگ‌ها
        recent_logs = self.metrics.get_recent_logs() if hasattr(self, "metrics") else []
        if recent_logs:
            df = pd.DataFrame(recent_logs)
            total = len(df)
            offloads = len(df[df["decision"] == "offload"])
            recent_offload_ratio = offloads / total if total > 0 else 0.5
        else:
            recent_offload_ratio = 0.5

        # محاسبه میانگین استفاده لینک‌ها از شبکه
        link_stats = self.network.snapshot_link_stats()
        if link_stats:
            utilizations = []
            for l in link_stats:
                bw = l["bw_bps"]
                used = l["total_reserved_bits"]
                util = used / (bw * self.config["monitor_window_s"]) if bw > 0 else 0
                utilizations.append(util)
            path_util = np.mean(utilizations) if utilizations else 0.0
        else:
            path_util = 0.0

        # ساخت state برای DQN
        state = np.array([recent_offload_ratio, path_util], dtype=np.float32)

        action = self.decision_agent.select_action(state)
        decision = "local" if int(action) == 0 else "offload"
        return decision, state, int(action), path_util


    # -----------------------
    def update_decision_agent(self, state, action, reward):
        """ذخیره و آموزش decision agent"""
        next_state = state.copy()
        try:
            transition = (state, action, reward, next_state)
            self.decision_agent.store(transition)
            record_decision_reward(reward)
            self.decision_agent.train_step()
        except Exception as e:
            print(f"[ERROR] decision agent update failed: {e}")
            pass

    # -----------------------
    def log_metrics_snapshot(self):
        """اختیاری: استخراج آمار لینک/مسیر برای logging"""
        return self.network.snapshot_link_stats()
