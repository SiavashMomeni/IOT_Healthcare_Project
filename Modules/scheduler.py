# Modules/scheduler.py
"""
Scheduler: منطق سطح تسک. این ماژول وظیفه دارد برای هر تسک:
 - تصمیم اولیه (local/offload/drop_by_capacity) با کمک SDNController بگیرد
 - اگر offload شد، مسیر مناسب را از SDNController بگیرد و رزرو انجام دهد
 - لاگ و متریک‌ها را ثبت کند

ورودی‌ها: network (Network instance), sdn_controller (SDNController instance), config
"""
import numpy as np

class Scheduler:
    def __init__(self, network, sdn_controller, metrics, config):
        self.network = network
        self.sdn = sdn_controller
        self.metrics = metrics
        self.config = config

    def decide(self, task, time_now):
        """
        task: dict with keys like:
          - "device_id", "size_kb", "deadline_ms", "task_id"
        Returns: (decision, path_nodes, path_links, meta)
        """
        dev = task["device_id"]
        size_kb = task.get("size_kb", self.config.get("default_task_kb", 100))
        # 1) اول از SDNController می‌پرسیم چه تصمیمی بهتره (local/offload)
        decision, d_state, d_action = self.sdn.select_decision(dev, task, time_now)

        if decision == "local":
            # اجرای محلی (ساده: محاسبه زمان پردازش محلی و لاگ)
            local_time_ms, _ = self._local_processing_time_ms(task)
            sla_hit = (local_time_ms <= task.get("deadline_ms", float('inf')))
            self.metrics.log_task(task["task_id"], dev, "local", local_time_ms, sla_hit)
            # به عامل تصمیم پاداش بده
            reward = -float(local_time_ms)
            self.sdn.update_decision_agent(d_state, d_action, reward)
            return "local", None, None, {"delay_ms": local_time_ms, "sla_hit": sla_hit}

        # اگر تصمیم offload
        # 2) مسیر مناسب را بگیر
        dev_node = self.network.device_to_node_id(dev)
        if dev_node is None:
            dev_node = int(dev.split("_")[1]) % self.network.node_count

        # انتخاب مقصد سرور توسط قبلی‌ها (مثلاً pick_destination_server)
        dest = self.sdn.network.get_least_loaded_node([n for n, nd in self.network.nodes.items() if nd.get("color") == "blue"]) \
               if hasattr(self.network, "get_least_loaded_node") else None
        if dest is None:
            # fallback: local
            return "local", None, None, {"reason": "no_server"}

        path_nodes, path_links, delay_ms = self.sdn.select_path(dev_node, dest, size_kb, time_now)

        if path_nodes is None:
            # اگر نتوان مسیر گرفت => local
            local_time_ms, _ = self._local_processing_time_ms(task)
            self.metrics.log_task(task["task_id"], dev, "local", local_time_ms, True)
            reward = -float(local_time_ms)
            self.sdn.update_decision_agent(d_state, d_action, reward)
            return "local", None, None, {"reason": "no_path"}

        # 3) چک ظرفیت و رزرو در network (با can_transmit / reserve_access_and_path)
        size_bytes = size_kb * 1024.0
        size_bits = size_bytes * 8.0
        ok, blocking = self.network.can_transmit(path_links, size_bits, src_node=dev_node, now=time_now, safety_factor=1.0)
        if not ok:
            # drop یا fallback
            self.metrics.log_task(task["task_id"], dev, "drop_by_capacity", float('inf'), False)
            # به decision agent پاداش منفی بده
            self.sdn.update_decision_agent(d_state, d_action, -100.0)
            return "drop_by_capacity", None, [blocking], {"reason":"link_capacity","blocking":blocking}

        # reserve and execute offload
        reservations = self.network.reserve_access_and_path(path_links, size_bits, src_node=dev_node, now=time_now, task_id=task["task_id"])
        sla_hit = (delay_ms <= task.get("deadline_ms", float('inf')))
        self.metrics.log_task(task["task_id"], dev, "offload", delay_ms, sla_hit)

        # به decision agent پاداش بده
        reward = -float(delay_ms)
        self.sdn.update_decision_agent(d_state, d_action, reward)

        return "offload", path_nodes, path_links, {"delay_ms": delay_ms, "reservations": reservations, "sla_hit": sla_hit}

    # -------------------------------------------------
    def _local_processing_time_ms(self, task):
        # از maths.processing_time_ms یا فرمول ساده استفاده کن
        # اینجا فرض ساده: cycles_per_byte و cpu_hz از config گرفته می‌شود
        cycles_per_byte = self.config.get("cycles_per_byte", 100)
        device_cpu_hz = self.config.get("device_cpu_hz", 1e9)
        size_bytes = task.get("size_kb", 100) * 1024.0
        cycles = size_bytes * cycles_per_byte
        sec = cycles / device_cpu_hz
        return sec * 1000.0, cycles
