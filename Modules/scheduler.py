from .maths import processing_time_ms
from .network import transmission_time_ms


class Scheduler:
    def __init__(self, device_weights, config):
        self.device_weights = device_weights
        self.config = config


    def decide(self, task):
        w = self.device_weights.get(task["device_id"], self.config["initial_weights"])
        local_time_ms, _ = processing_time_ms(task["size_kb"], self.config["device_cpu_hz"], self.config["cycles_per_byte"])
        local_norm = local_time_ms / task["deadline_ms"]
        tx_ms = transmission_time_ms(task["size_kb"], "fog")
        fog_proc_ms, _ = processing_time_ms(task["size_kb"], self.config["fog_cpu_hz"], self.config["cycles_per_byte"])
        offload_time_ms = tx_ms + fog_proc_ms
        offload_norm = offload_time_ms / task["deadline_ms"]
        score_local = w["w_local"] * local_norm
        score_offload = w["w_offload"] * offload_norm
        return "local" if score_local <= score_offload else "offload"