import pandas as pd


class SDNController:
    def __init__(self, device_weights, config):
        self.device_weights = device_weights
        self.lr = config["learning_rate"]


    def update_weights(self, recent_logs):
        df = pd.DataFrame(recent_logs)
        off_rate = df[df.decision=="offload"].sla_violation.mean() if not df[df.decision=="offload"].empty else 0.0
        loc_rate = df[df.decision=="local"].sla_violation.mean() if not df[df.decision=="local"].empty else 0.0
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