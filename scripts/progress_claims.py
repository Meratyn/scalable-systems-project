"""Re-derive report claims from existing job outputs (no job re-run).

Usage: python3 -I scripts/progress_claims.py LITERAL_DIR STRICT_DIR ROUNDING_OUT FLAT_OUT
  LITERAL_DIR  e.g. output/w2_p8/results         (literal rule, full week, p8)
  STRICT_DIR   e.g. output/w2_strict_p8/results  (--strict-crossover)
Result rows: symbol,window_end_ms,ema38,ema100,signal

Rounding:
  * literal signal is a rounding artefact if |EMA38 - EMA100| < 1e-12 * P in its
    window, P = window price recovered from EMA38: P = (e38 - prev38*(1-a)) / a,
    a = 2/39;
  * strict signal comes from rounding drift if the previous EMAs differ but
    |prev38 - prev100| < 1e-12 * P.
Flat symbols (as VizData): (max EMA38 - min EMA38) / mean EMA38 < 0.005.
"""
import glob
import os
import sys
from collections import defaultdict

A38 = 2 / 39
TOL = 1e-12
FLAT = 0.005


def load(d):
    rows = defaultdict(list)
    for f in glob.glob(os.path.join(d, "**", "*"), recursive=True):
        if not os.path.isfile(f):
            continue
        with open(f) as fh:
            for line in fh:
                p = line.rstrip("\n").split(",")
                rows[p[0]].append((int(p[1]), float(p[2]), float(p[3]), p[4]))
    for v in rows.values():
        v.sort()
    return rows


def price(prev38, e38):
    return (e38 - prev38 * (1 - A38)) / A38


def main(lit_dir, strict_dir, rounding_out, flat_out):
    lit = load(lit_dir)
    strict = load(strict_dir)

    n_lit = n_eq = n_art = n_art_eq = 0
    art_split = defaultdict(int)
    eq_win1 = eq_later = 0
    for s, v in lit.items():
        for i in range(1, len(v)):
            _, e38, e100, sig = v[i]
            if not sig:
                continue
            n_lit += 1
            p38, p100 = v[i - 1][1], v[i - 1][2]
            eq = p38 == p100
            if eq:
                n_eq += 1
                if i == 1:
                    eq_win1 += 1
                else:
                    eq_later += 1
            P = price(p38, e38)
            if abs(e38 - e100) < TOL * abs(P):
                n_art += 1
                n_art_eq += eq
                art_split[("window 1" if i == 1 else "later windows", sig)] += 1

    n_strict = n_drift = 0
    for s, v in strict.items():
        for i in range(1, len(v)):
            _, e38, e100, sig = v[i]
            if not sig:
                continue
            n_strict += 1
            p38, p100 = v[i - 1][1], v[i - 1][2]
            P = price(p38, e38)
            if p38 != p100 and abs(p38 - p100) < TOL * abs(P):
                n_drift += 1

    with open(rounding_out, "w") as out:
        out.write(f"source: literal={lit_dir} strict={strict_dir}\n")
        out.write(f"rule: |diff| < {TOL:g} * P, P recovered from EMA38 (a = 2/39)\n")
        out.write(f"literal signals: {n_lit}\n")
        out.write(f"  equal-start (previous EMAs exactly equal): {n_eq} "
                  f"({100 * n_eq / n_lit:.2f} %); in window 1: {eq_win1}, later: {eq_later}\n")
        out.write(f"  rounding artefacts (|EMA38-EMA100| < tol*P): {n_art} "
                  f"({100 * n_art / n_lit:.2f} %), of which equal-start: {n_art_eq}\n")
        for k in sorted(art_split):
            out.write(f"    {k[0]} {k[1]}: {art_split[k]}\n")
        out.write(f"strict signals: {n_strict}\n")
        out.write(f"  previous EMAs differ only by rounding (0 < |diff| < tol*P): {n_drift} "
                  f"({100 * n_drift / n_strict:.2f} %)\n")

    move = {}
    for s, v in lit.items():
        e = [r[1] for r in v]
        mean = sum(e) / len(e)
        move[s] = (max(e) - min(e)) / mean if mean else 0.0
    flat = {s for s, m in move.items() if m < FLAT}
    counts = sorted(((sum(1 for r in v if r[3]), s) for s, v in lit.items()), reverse=True)
    with open(flat_out, "w") as out:
        out.write(f"source: {lit_dir}\n")
        out.write(f"movement = (max EMA38 - min EMA38) / mean EMA38; flat if < {FLAT}\n")
        out.write(f"symbols: {len(lit)}, flat: {len(flat)}\n")
        out.write("top 10 symbols by literal signal count (rank, symbol, signals, windows, movement, flat):\n")
        for rank, (c, s) in enumerate(counts[:10], 1):
            out.write(f"  {rank:2d} {s:14s} {c:5d} {len(lit[s]):5d} {move[s]:.6f} "
                      f"{'FLAT' if s in flat else ''}\n")


if __name__ == "__main__":
    main(*sys.argv[1:5])
