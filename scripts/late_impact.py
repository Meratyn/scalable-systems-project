"""Effect of late events on the results: reference run (no late events) vs a run with late events.

    python3 scripts/late_impact.py output/w2_p8 output/w2_p8_tol01

Prints:
  windows only in ref / only in run, symbols that lost windows
  (a) advisories: rows in both with a different advisory (BUY<->SELL, or present in one only),
      and advisories in windows that exist in only one run
  (b) differing rows (only in one run, or in both with |dEMA| > 1e-9 or another advisory),
      split into symbols that lost windows and the other symbols
  (c) max relative EMA38 / EMA100 difference over windows of symbols that lost no window
"""
import glob
import sys


def load(folder):
    rows = {}
    for path in glob.glob(f"{folder}/results/**/*.csv", recursive=True):
        if path.rsplit("/", 1)[-1].startswith("."):
            continue
        with open(path) as f:
            for line in f:
                sym, ts, e38, e100, adv = line.rstrip("\n").split(",")
                rows[(sym, int(ts))] = (float(e38), float(e100), adv)
    return rows


ref_dir, run_dir = sys.argv[1:3]
ref = load(ref_dir)
run = load(run_dir)
only_ref = ref.keys() - run.keys()
only_run = run.keys() - ref.keys()
both = ref.keys() & run.keys()
lost = {k[0] for k in only_ref} | {k[0] for k in only_run}
print(f"{ref_dir}: {len(ref)} rows, {sum(1 for v in ref.values() if v[2])} advisories")
print(f"{run_dir}: {len(run)} rows, {sum(1 for v in run.values() if v[2])} advisories")
print(f"windows only in ref: {len(only_ref)}, only in run: {len(only_run)}; symbols that lost windows: {len(lost)}")

# (a) advisories
flip = sum(1 for k in both if ref[k][2] and run[k][2] and ref[k][2] != run[k][2])
gone = sum(1 for k in both if ref[k][2] and not run[k][2])
new = sum(1 for k in both if run[k][2] and not ref[k][2])
adv_only_ref = sum(1 for k in only_ref if ref[k][2])
adv_only_run = sum(1 for k in only_run if run[k][2])
print(f"(a) rows in both with a different advisory: {flip + gone + new} "
      f"(BUY<->SELL {flip}, only in ref {gone}, only in run {new})")
print(f"    advisories in windows only in ref: {adv_only_ref}, only in run: {adv_only_run}")
for k in sorted(k for k in both if ref[k][2] != run[k][2])[:10]:
    print(f"      {k}: ref '{ref[k][2]}' run '{run[k][2]}'")

# (b) differing rows by symbol group
def differs(k):
    a, b = ref[k], run[k]
    return abs(a[0] - b[0]) > 1e-9 or abs(a[1] - b[1]) > 1e-9 or a[2] != b[2]


diff_both = [k for k in both if differs(k)]
total = len(only_ref) + len(only_run) + len(diff_both)
in_lost = len(only_ref) + len(only_run) + sum(1 for k in diff_both if k[0] in lost)
print(f"(b) differing rows: {total}; of symbols that lost windows: {in_lost}; other symbols: {total - in_lost}")

# (c) other symbols: max relative EMA differences
m38 = m100 = 0.0
for k in both:
    if k[0] in lost:
        continue
    a, b = ref[k], run[k]
    m38 = max(m38, abs(a[0] - b[0]) / abs(a[0]))
    m100 = max(m100, abs(a[1] - b[1]) / abs(a[1]))
print(f"(c) symbols without lost windows: max relative difference EMA38 {m38:.3e}, EMA100 {m100:.3e}")

# for the symbols that lost windows: max relative difference by window distance is not computed;
# their largest relative difference overall:
l38 = max((abs(ref[k][0] - run[k][0]) / abs(ref[k][0]) for k in both if k[0] in lost), default=0.0)
l100 = max((abs(ref[k][1] - run[k][1]) / abs(ref[k][1]) for k in both if k[0] in lost), default=0.0)
print(f"    symbols that lost windows: max relative difference EMA38 {l38:.3e}, EMA100 {l100:.3e}")
