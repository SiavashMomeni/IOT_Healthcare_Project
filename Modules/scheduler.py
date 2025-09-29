from .maths import processing_time_ms
from .network import transmission_time_ms


DEVICE_CPU_HZ = 0.5e9
FOG_CPU_HZ = 5e9


class Scheduler:
    def __init__(self, device_weights):
        self.device_weights = device_weights


    def decide(self, task):
        w = self.device_weights.get(task["device_id"], {"w_local": 0.7, "w_offload": 0.3})
        local_time_ms, _ = processing_time_ms(task["size_kb"], DEVICE_CPU_HZ)
        local_norm = local_time_ms / task["deadline_ms"]
        tx_ms = transmission_time_ms(task["size_kb"], "fog")
        fog_proc_ms, _ = processing_time_ms(task["size_kb"], FOG_CPU_HZ)
        offload_time_ms = tx_ms + fog_proc_ms
        offload_norm = offload_time_ms / task["deadline_ms"]
        score_local = w["w_local"] * local_norm
        score_offload = w["w_offload"] * offload_norm
        return "local" if score_local <= score_offload else "offload"