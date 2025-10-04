from .utils import snap_time

PATHS = {
    "fog": {"base_bw_kbps": 20000.0, "base_rtt_ms": 5.0},
    "cloud": {"base_bw_kbps": 5000.0, "base_rtt_ms": 30.0}
}

def transmission_time_ms(size_kb, path="fog"):
    bw = PATHS[path]["base_bw_kbps"]
    rtt = PATHS[path]["base_rtt_ms"]
    tx_ms = (size_kb * 8.0) / bw * 1000.0
    return snap_time(tx_ms + rtt)