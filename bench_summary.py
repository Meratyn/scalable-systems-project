import csv, sys, statistics as st
from collections import defaultdict
rows = defaultdict(list)
with open(sys.argv[1]) as f:
    for r in csv.DictReader(f):
        rows[(r["dataset"], int(r["parallelism"]))].append(float(r["seconds"]))
for ds in sorted({k[0] for k in rows}):
    base = st.median(rows[(ds, 1)]) if (ds, 1) in rows else None
    print(f"\n{ds}")
    print("parallelism  runs  median(s)  min-max(s)  speedup")
    for p in sorted(k[1] for k in rows if k[0] == ds):
        v = rows[(ds, p)]
        m = st.median(v)
        sp = f"{base/m:.2f}x" if base else "-"
        print(f"{p:>11}  {len(v):>4}  {m:>9.1f}  {min(v):.0f}-{max(v):.0f}".ljust(45) + f"  {sp}")
