RESOLUTION = 0.0001  # 0.1 ms

def snap_time(t: float) -> float:
    return round(t / RESOLUTION) * RESOLUTION