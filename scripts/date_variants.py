"""Summary table for the Trading-date variants (called by scripts/date_variants.sh).

    python3 scripts/date_variants.py "name,results_dir,symbolstats_dir,seconds" ...

Columns: variant, price_events, symbols, result_rows, buy, sell, signals (= BUY + SELL, literal
rule), then per exchange (ETR, FR, NL): symbols, signals; then runtime_s (empty if not run here).
symbols = symbols with at least one price event (= symbols with at least one result row).
"""
import glob
import sys

EXCHANGES = ("ETR", "FR", "NL")


def exchange(sym):
    return sym.rsplit(".", 1)[-1]


def csv_lines(folder):
    for path in sorted(glob.glob(f"{folder}/**/*.csv", recursive=True)):
        if path.rsplit("/", 1)[-1].startswith("."):
            continue
        with open(path) as f:
            yield from f


cols = ["variant", "price_events", "symbols", "result_rows", "buy", "sell", "signals"]
for ex in EXCHANGES:
    cols += [f"{ex}_symbols", f"{ex}_signals"]
cols.append("runtime_s")
print(",".join(cols))

for arg in sys.argv[1:]:
    name, results, stats, secs = arg.split(",")
    events = 0
    for line in csv_lines(stats):
        events += int(line.rsplit(",", 1)[1])
    rows = 0
    adv = {"BUY": 0, "SELL": 0}
    syms = {ex: set() for ex in EXCHANGES}
    sig = {ex: 0 for ex in EXCHANGES}
    for line in csv_lines(results + "/results"):
        sym, _, _, _, a = line.rstrip("\n").split(",")
        rows += 1
        ex = exchange(sym)
        syms.setdefault(ex, set()).add(sym)
        if a:
            adv[a] += 1
            sig[ex] = sig.get(ex, 0) + 1
    nsym = sum(len(s) for s in syms.values())
    out = [name, events, nsym, rows, adv["BUY"], adv["SELL"], adv["BUY"] + adv["SELL"]]
    for ex in EXCHANGES:
        out += [len(syms[ex]), sig[ex]]
    out.append(f"{float(secs):.1f}" if secs else "")
    print(",".join(str(x) for x in out))
