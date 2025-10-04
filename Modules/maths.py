import math


def cycles_required(size_kb, cycles_per_byte=100):
    return size_kb * 1024.0 * cycles_per_byte


def processing_time_ms(size_kb, cpu_hz, cycles_per_byte=100):
    cycles = cycles_required(size_kb, cycles_per_byte)
    time_s = cycles / cpu_hz
    return time_s * 1000.0, cycles