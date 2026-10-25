"""Compare two job.py output folders, e.g. parallelism 1 vs parallelism 4.

    python compare_runs.py output/p1 output/p4
"""
import glob
import sys


def load(folder):
    rows = {}
    for path in glob.glob(f"{folder}/results/**/*.csv", recursive=True):
        with open(path) as f:
            for line in f:
                sym, ts, e38, e100, adv = line.rstrip("\n").split(",")
                rows[(sym, int(ts))] = (float(e38), float(e100), adv)
    late = 0
    for path in glob.glob(f"{folder}/late/**/*.csv", recursive=True):
        with open(path) as f:
            late += sum(1 for _ in f)
    return rows, late


a_dir, b_dir = sys.argv[1:3]
a, a_late = load(a_dir)
b, b_late = load(b_dir)


def crossovers(r):
    return sum(1 for v in r.values() if v[2])


print(f"{a_dir}: {len(a)} windows, {crossovers(a)} crossovers, {a_late} late events")
print(f"{b_dir}: {len(b)} windows, {crossovers(b)} crossovers, {b_late} late events")

if not a or not b:
    print("NO DATA: at least one output is empty - a job probably failed. "
          "Nothing was compared.")
    sys.exit(1)

only_a = a.keys() - b.keys()
only_b = b.keys() - a.keys()
diff = [k for k in a.keys() & b.keys()
        if abs(a[k][0] - b[k][0]) > 1e-9 or abs(a[k][1] - b[k][1]) > 1e-9
        or a[k][2] != b[k][2]]
print(f"windows only in {a_dir}: {len(only_a)}")
print(f"windows only in {b_dir}: {len(only_b)}")
print(f"windows in both with different values: {len(diff)}")
for label, keys in (("only A", only_a), ("only B", only_b), ("different", diff)):
    for k in sorted(keys)[:5]:
        print(f"  {label}: {k}")
print("IDENTICAL" if not (only_a or only_b or diff) else "RESULTS DIFFER")
