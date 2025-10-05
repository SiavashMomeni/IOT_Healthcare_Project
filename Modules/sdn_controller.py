import pandas as pd

class SDNController:
    def __init__(self, device_weights, config):
        self.device_weights = device_weights
        self.lr = config["learning_rate"]

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
