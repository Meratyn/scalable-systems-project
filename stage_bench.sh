#!/bin/bash
JAR=java-job/target/ema-crossover-1.0.jar
OUT=logs/stage_times.csv
echo "stage,parallelism,rep,seconds" > $OUT
for rep in 1 2; do
  for stage in read parse; do
    for p in 1 2 4 8; do
      start=$(date +%s.%N)
      java -Xmx5g -jar $JAR --input data/raw --output output/stage_tmp --parallelism $p --stage $stage > /dev/null 2>&1
      rc=$?
      end=$(date +%s.%N)
      secs=$(echo "$end - $start" | bc)
      if [ $rc -ne 0 ]; then echo "FAILED: $stage p=$p rep=$rep (exit $rc)"; else
        echo "$stage,$p,$rep,$secs" >> $OUT
        echo "$(date +%H:%M) $stage p=$p rep=$rep ${secs}s"; fi
    done
  done
done
python3 - <<'PY'
import csv, statistics as st
from collections import defaultdict
r = defaultdict(list)
for row in csv.DictReader(open("logs/stage_times.csv")):
    r[(row["stage"], int(row["parallelism"]))].append(float(row["seconds"]))
for stage in ("read", "parse"):
    print(f"\n{stage}: parallelism, median(s), speedup")
    base = st.median(r[(stage, 1)])
    for p in (1, 2, 4, 8):
        m = st.median(r[(stage, p)])
        print(f"{p:>3}  {m:7.1f}  {base/m:.2f}x")
PY
