"""Check a CSV for price events that are likely to arrive 'late'.

    python check_stale.py data/small/<file>.csv

stale  = price events whose Trading date differs from the system Date
         (an old trade being re-reported inside today's file)
behind = price events whose trading timestamp is more than 2 hours older
         than the newest trading timestamp already seen in the file
"""
import sys
from datetime import datetime

total = stale = behind = 0
newest = None
with open(sys.argv[1], "r", encoding="utf-8", errors="replace") as f:
    for line in f:
        if line.startswith("#") or line.startswith("ID,"):
            continue
        p = line.rstrip("\n").split(",")
        if len(p) < 27 or not p[21] or not p[23] or not p[26]:
            continue
        try:
            if float(p[21]) == 0.0:
                continue
            ts = datetime.strptime(f"{p[26]} {p[23]}", "%d-%m-%Y %H:%M:%S.%f")
        except ValueError:
            continue
        total += 1
        if p[26] != p[2]:
            stale += 1
        if newest is None or ts > newest:
            newest = ts
        elif (newest - ts).total_seconds() > 7200:
            behind += 1

print(f"price events: {total}")
print(f"trading date != system date: {stale}")
print(f"more than 2h behind newest seen: {behind}")
