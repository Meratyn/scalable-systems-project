"""Compare --ema-init first (default) and --ema-init zero from two result folders.

    python3 scripts/ema_init.py <first_dir> <zero_dir> > logs/ema_init.csv

Window price: the result files hold no price, so the price of each (symbol, window) is
recovered from the first-mode EMA38: P_0 = EMA38_0 (seed) and
P_k = (EMA38_k - (1 - a) * EMA38_{k-1}) / a with a = 2/39. Both modes see the same windows.

Metrics per mode (one CSV row each):
  advisories, buy, sell
  first_window_advisories     advisories in each symbol's first window (index 0)
  adv_first100 / adv_later    advisories in window index 0..99 / >= 100 of each symbol
  symbols, symbols_ge100      symbols, and symbols with at least 100 windows
  conv_median_windows         median over symbols of the first window index k (0-based) where
                              |EMA38_k - P_k| <= 1% of P_k, counted as k+1 windows; symbols that
                              never get there are left out (their number is conv_never)
  conv_p25 / conv_p75         quartiles of the same
  conv_never                  symbols whose EMA38 is never within 1% of the price
"""
import glob
import statistics
import sys

A38 = 2 / 39


def load(folder):
    by_sym = {}
    for path in glob.glob(f"{folder}/results/**/*.csv", recursive=True):
        if path.rsplit("/", 1)[-1].startswith("."):
            continue
        with open(path) as f:
            for line in f:
                sym, ts, e38, e100, adv = line.rstrip("\n").split(",")
                by_sym.setdefault(sym, []).append((int(ts), float(e38), adv))
    for rows in by_sym.values():
        rows.sort()
    return by_sym


def prices(rows_first):
    out = []
    prev = None
    for _, e38, _ in rows_first:
        out.append(e38 if prev is None else (e38 - (1 - A38) * prev) / A38)
        prev = e38
    return out


def quantile(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * (len(xs) - 1) + 0.5))]


first_dir, zero_dir = sys.argv[1:3]
first = load(first_dir)
zero = load(zero_dir)
assert first.keys() == zero.keys(), "different symbol sets"

cols = ["mode", "advisories", "buy", "sell", "first_window_advisories", "adv_first100", "adv_later",
        "symbols", "symbols_ge100", "conv_median_windows", "conv_p25", "conv_p75", "conv_never"]
print(",".join(cols))
price_of = {s: prices(first[s]) for s in first}
for mode, data in (("first", first), ("zero", zero)):
    buy = sell = first_win = early = late = 0
    conv = []
    never = 0
    ge100 = 0
    for sym, rows in data.items():
        assert [r[0] for r in rows] == [r[0] for r in first[sym]], f"windows differ for {sym}"
        if len(rows) >= 100:
            ge100 += 1
        for k, (_, _, adv) in enumerate(rows):
            if not adv:
                continue
            buy += adv == "BUY"
            sell += adv == "SELL"
            first_win += k == 0
            if k < 100:
                early += 1
            else:
                late += 1
        ps = price_of[sym]
        hit = next((k for k, (_, e38, _) in enumerate(rows) if abs(e38 - ps[k]) <= 0.01 * abs(ps[k])), None)
        if hit is None:
            never += 1
        else:
            conv.append(hit + 1)
    print(",".join(str(x) for x in [mode, buy + sell, buy, sell, first_win, early, late, len(data), ge100,
                                    statistics.median(conv), quantile(conv, 0.25), quantile(conv, 0.75), never]))
