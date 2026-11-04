#!/bin/bash
# Ablation on the full week at parallelism 8: change one factor at a time from the
# baseline (fast parser, ParallelGC, FileSource, one file per day).
# Usage: ./ablation.sh   (~35 min)   -> logs/ablation_times.csv, logs/ablation.log
JAR=java-job/target/ema-crossover-1.0.jar
OUT=logs/ablation_times.csv
P=8
mkdir -p logs
echo "config,rep,seconds,cpu_percent,max_rss_kb,check" > $OUT

# name | JVM GC flag | input dir | extra job options
CONFIGS=(
  "baseline|-XX:+UseParallelGC|data/raw|"
  "reference-parser|-XX:+UseParallelGC|data/raw|--parser reference"
  "g1-gc|-XX:+UseG1GC|data/raw|"
  "legacy-source|-XX:+UseParallelGC|data/raw|--source legacy"
  "8-chunks-per-day|-XX:+UseParallelGC|data/rawS|"
  "object-reuse|-XX:+UseParallelGC|data/raw|--object-reuse"
)

for rep in 1 2 3; do
  for c in "${CONFIGS[@]}"; do
    IFS='|' read -r name gc input extra <<< "$c"
    tmp=output/abl_tmp
    rm -rf $tmp
    /usr/bin/time -f "%e,%P,%M" -o logs/.abl_time.txt \
      java -Xmx5g $gc -jar $JAR --input $input --output $tmp --parallelism $P $extra > /dev/null 2>&1
    rc=$?
    if [ $rc -ne 0 ]; then echo "FAILED: $name rep=$rep (exit $rc)"; rm -rf $tmp; continue; fi
    t=$(tail -1 logs/.abl_time.txt | sed 's/%//')
    check="-"
    if [ $rep -eq 1 ]; then
      # correctness: every config must give the same output as the baseline
      if [ "$name" = "baseline" ]; then rm -rf output/abl_base; mv $tmp output/abl_base; check="ref"
      else check=$(python3 compare_runs.py output/abl_base $tmp | tail -1); fi
    fi
    echo "$name,$rep,$t,$check" >> $OUT
    echo "$(date +%H:%M) $name rep=$rep $t $check"
    rm -rf $tmp
  done
done
rm -f logs/.abl_time.txt

python3 - <<'PY'
import csv, statistics as st
from collections import defaultdict
rows = defaultdict(list); checks = {}
for r in csv.DictReader(open("logs/ablation_times.csv")):
    rows[r["config"]].append((float(r["seconds"]), float(r["cpu_percent"]), float(r["max_rss_kb"]) / 1e6))
    if r["check"] != "-": checks[r["config"]] = r["check"]
base = st.median(x[0] for x in rows["baseline"])
print("\nABLATION, 7 days, parallelism 8 (medians of 3)")
print("config              median(s)  vs baseline  cpu%  peak GB  output")
for k, v in rows.items():
    m = st.median(x[0] for x in v)
    print(f"{k:<19} {m:9.1f} {m/base:11.2f}x {st.median(x[1] for x in v):5.0f} "
          f"{st.median(x[2] for x in v):8.2f}  {checks.get(k, '?')}")
PY
