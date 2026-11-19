"""Latency statistics from --latency-log files (lines: symbol,window_end_ms,latency_us[,last_event_ms]).

    python3 scripts/latency_stats.py <log_prefix> [<log_prefix> ...]

Prints: results,median_ms,p95_ms,p99_ms,max_ms over all lines of all given prefixes
(prefix.0, prefix.1, ... = one file per window subtask). Percentiles: nearest rank.
"""
import glob
import math
import sys

vals = []
for prefix in sys.argv[1:]:
    for path in glob.glob(prefix + ".*"):
        with open(path) as f:
            for line in f:
                vals.append(int(line.split(",")[2]))
vals.sort()
n = len(vals)


def pct(q):
    return vals[max(0, math.ceil(q * n) - 1)] / 1000.0


print(f"{n},{pct(0.5):.1f},{pct(0.95):.1f},{pct(0.99):.1f},{vals[-1] / 1000.0:.1f}")
