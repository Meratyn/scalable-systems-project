"""Cut a small multi-file test set: one slice of trading hours per day file.

    python make_small_set.py data/raw_test data/small 09:00:00 10:30:00

Keeps only rows whose system time (column 4) falls in the slice. The raw
files contain stray rows with out-of-sequence system times near the top
(e.g. 18:30 reset rows), so reading only stops after the slice has started
and 1000 consecutive rows fall beyond its end.
"""
import glob
import os
import sys

src_dir, dst_dir, start, end = sys.argv[1:5]
os.makedirs(dst_dir, exist_ok=True)
STOP_AFTER = 1000

for path in sorted(glob.glob(os.path.join(src_dir, "*.csv"))):
    dst = os.path.join(dst_dir, os.path.basename(path))
    kept = beyond = 0
    with open(path, "r", encoding="utf-8", errors="replace") as fin, \
         open(dst, "w", encoding="utf-8") as fout:
        for line in fin:
            if line.startswith("#") or line.startswith("ID,"):
                continue
            p = line.split(",", 5)
            if len(p) < 5:
                continue
            sys_time = p[3]  # system time HH:MM:SS.sss
            if start <= sys_time <= end:
                fout.write(line)
                kept += 1
                beyond = 0
            elif kept >= 100000 and sys_time > end:
                beyond += 1
                if beyond >= STOP_AFTER:
                    break
    print(f"{os.path.basename(path)}: kept {kept} rows -> {dst}")
