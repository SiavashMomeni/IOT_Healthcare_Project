import pandas as pd
import os
from collections import deque
import numpy as np
import matplotlib.pyplot as plt

# Rolling windows to compute recent statistics
_recent_decisions = deque(maxlen=200)
_recent_delays = deque(maxlen=200)
_recent_utils = deque(maxlen=200)
decision_reward_history = [] 
selection_reward_history = [] 

def record_decision_reward(reward):
    decision_reward_history.append(reward)    
    
def record_selection_reward(reward):
    selection_reward_history.append(reward)
    
def get_reward_stats(reward_history):
    """Get average and standard deviation of reward history"""
    if len(reward_history) == 0:
        return 0, 0
    
    avg_reward = sum(reward_history) / len(reward_history)
    
    # Calculate standard deviation
    variance = sum((x - avg_reward) ** 2 for x in reward_history) / len(reward_history)
    std_reward = variance ** 0.5
    
    return avg_reward, std_reward

def get_decision_reward_stats():
    return get_reward_stats(decision_reward_history)

def get_selection_reward_stats():
    return get_reward_stats(selection_reward_history)

def get_recent_reward_stats(reward_history, window=100):
    """Get stats for recent episodes only"""
    if len(reward_history) == 0:
        return 0, 0
    
    recent_rewards = reward_history[-window:]
    return get_reward_stats(recent_rewards)

def record_task(decision, total_latency_ms, path_util):
    """Record one task's outcome for online metric tracking."""
    _recent_decisions.append(decision)
    _recent_delays.append(total_latency_ms)
    _recent_utils.append(path_util)
def get_recent_offload_ratio():
    """Return the fraction of tasks that were offloaded recently."""
    if not _recent_decisions:
        return 0.5  # default neutral value
    offload_count = sum(1 for d in _recent_decisions if d == "offload")
    return offload_count / len(_recent_decisions)

def get_mean_latency():
    """Mean of recent total latencies."""
    return np.mean(_recent_delays) if _recent_delays else 0.0

def get_mean_path_utilization():
    """Mean of recent path utilizations (0–1 range)."""
    return np.mean(_recent_utils) if _recent_utils else 0.0

# ----------------------------------------------
# Logging / Saving (your original functionality)
# ----------------------------------------------
decision_reward_history = [] 
selection_reward_history = [] 

def save_logs_and_metrics(task_logs, weight_logs, outdir):
    df = pd.DataFrame(task_logs)
    
    # Create DataFrames for rewards
    decision_reward_df = pd.DataFrame({
        "index": range(len(decision_reward_history)),
        "reward": decision_reward_history
    })
    selection_reward_df = pd.DataFrame({
        "index": range(len(selection_reward_history)),
        "reward": selection_reward_history
    })
    
    # Save to CSV
    decision_reward_df.to_csv(os.path.join(outdir, "decision_reward_log.csv"), index=False)
    selection_reward_df.to_csv(os.path.join(outdir, "selection_reward_log.csv"), index=False)
    
    # Calculate normalized rewards and statistics
    for reward_df, title_suffix in [(decision_reward_df, "decision"), (selection_reward_df, "selection")]:
        if len(reward_df) > 0:
            # Calculate normalized reward (reward / avg(reward))
            avg_reward = reward_df["reward"].mean()
            reward_df["normalized_reward"] = reward_df["reward"] / avg_reward if avg_reward != 0 else reward_df["reward"]
            
            # Calculate rolling statistics
            reward_df["rolling_avg"] = reward_df["reward"].rolling(window=100, min_periods=1).mean()
            reward_df["rolling_std"] = reward_df["reward"].rolling(window=100, min_periods=1).std()
            reward_df["rolling_normalized_avg"] = reward_df["normalized_reward"].rolling(window=100, min_periods=1).mean()
            
            # Create plots
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))
            
            # Plot 1: Original rewards with moving average and std
            color = 'tab:blue'
            ax1.set_xlabel("Episode")
            ax1.set_ylabel("Total Reward", color=color)
            ax1.plot(reward_df["index"], reward_df["reward"], label="Episode Reward", 
                    linewidth=1, alpha=0.6, color=color)
            ax1.plot(reward_df["index"], reward_df["rolling_avg"], 
                    label="Smoothed (100)", linewidth=2, color="orange")
            ax1.tick_params(axis='y', labelcolor=color)
            ax1.legend(loc='upper left')
            ax1.grid(True, alpha=0.3)
            ax1.set_title(f"{title_suffix.title()} DQN Learning Curve - Reward over Episodes")
            
            # Add standard deviation on second y-axis
            ax1_std = ax1.twinx()
            color = 'tab:gray'
            ax1_std.set_ylabel("Standard Deviation", color=color)
            ax1_std.plot(reward_df["index"], reward_df["rolling_std"], 
                        color=color, alpha=0.5, linewidth=1, label="Rolling Std (100)")
            ax1_std.tick_params(axis='y', labelcolor=color)
            ax1_std.legend(loc='upper right')
            
            # Plot 2: Normalized rewards
            color = 'tab:green'
            ax2.set_xlabel("Episode")
            ax2.set_ylabel("Normalized Reward", color=color)
            ax2.plot(reward_df["index"], reward_df["normalized_reward"], 
                    label="Normalized Reward", linewidth=1, alpha=0.6, color=color)
            ax2.plot(reward_df["index"], reward_df["rolling_normalized_avg"], 
                    label="Smoothed Normalized (100)", linewidth=2, color="red")
            ax2.axhline(y=1.0, color='black', linestyle='--', alpha=0.5, label='Average (1.0)')
            ax2.tick_params(axis='y', labelcolor=color)
            ax2.legend()
            ax2.grid(True, alpha=0.3)
            ax2.set_title(f"{title_suffix.title()} DQN Learning Curve - Normalized Reward over Episodes")
            
            plt.tight_layout()
            plt.savefig(os.path.join(outdir, f"{title_suffix}_reward_analysis.png"), dpi=300)
            plt.show()
            
            # Also create individual simplified plots for quick viewing
            plt.figure(figsize=(8, 5))
            plt.plot(reward_df["index"], reward_df["normalized_reward"], 
                    label="Normalized Reward", linewidth=1, alpha=0.7)
            plt.plot(reward_df["index"], reward_df["rolling_normalized_avg"], 
                    label="Smoothed (100)", linewidth=2, color="red")
            plt.axhline(y=1.0, color='black', linestyle='--', alpha=0.5, label='Average')
            plt.xlabel("Episode")
            plt.ylabel("Normalized Reward")
            plt.title(f"{title_suffix.title()} DQN - Normalized Reward (Reward/Avg Reward)")
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(os.path.join(outdir, f"{title_suffix}_normalized_reward_plot.png"))
            plt.show()

    expected_cols = [
        "total_latency_ms", "sla_violation", "decision", "queue_delay_ms",
        "proc_delay_ms", "tx_delay_ms", "energy_j", "status"
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    df.to_csv(os.path.join(outdir, "sim_task_logs.csv"), index=False)

    metrics = {
        "num_tasks": len(df),
        "avg_latency_ms": df["total_latency_ms"].mean(skipna=True),
        "std_latency_ms": df["total_latency_ms"].std(skipna=True),
        "sla_violation_rate": df["sla_violation"].mean(skipna=True) if "sla_violation" in df else 0.0,
        "drop_rate": (df["status"] == "drop").mean() if "status" in df else 0.0,
        "hit_rate": (df["status"] == "hit").mean() if "status" in df else 0.0,
        "avg_queue_delay_ms": df["queue_delay_ms"].mean(skipna=True),
        "avg_proc_delay_ms": df["proc_delay_ms"].mean(skipna=True),
        "avg_tx_delay_ms": df["tx_delay_ms"].mean(skipna=True),
        "avg_energy_j": df["energy_j"].mean(skipna=True),
        "recent_offload_ratio": get_recent_offload_ratio(),
        "mean_path_util": get_mean_path_utilization(),
        "mean_recent_latency": get_mean_latency(),
    }

    metrics_df = pd.DataFrame([metrics])
    metrics_df.to_csv(os.path.join(outdir, "sim_metrics_summary.csv"), index=False)

    weights_df = pd.DataFrame(weight_logs)
    weights_df.to_csv(os.path.join(outdir, "weights.csv"), index=False)

    return df, metrics_df, weights_df
