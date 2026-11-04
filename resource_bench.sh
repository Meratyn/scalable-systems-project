#!/bin/bash
JAR=java-job/target/ema-crossover-1.0.jar
OUT=logs/resource_times.csv
echo "experiment,dataset,parallelism,rep,seconds,cpu_percent,max_rss_kb" > $OUT
run() { # experiment dataset dir parallelism rep
  rm -rf output/res_tmp
  /usr/bin/time -f "%e,%P,%M" -o /tmp/res_time.txt java -Xmx5g -XX:+UseParallelGC -jar $JAR --input data/$3 --output output/res_tmp --parallelism $4 > /dev/null 2>&1
  rc=$?
  if [ $rc -ne 0 ]; then echo "FAILED: $1 $2 p=$4 rep=$5"; else
    t=$(tail -1 /tmp/res_time.txt | sed 's/%//')
    echo "$1,$2,$4,$5,$t" >> $OUT
    echo "$(date +%H:%M) $1 $2 p=$4 rep=$5 $t"
  fi
  rm -rf output/res_tmp
}
for rep in 1 2 3; do
  for ds in raw1:1day raw2:2day raw4:4day raw:7day; do
    run volume ${ds##*:} ${ds%%:*} 8 $rep
  done
done
for rep in 1 2; do
  for p in 1 2 4 8; do run resources 7day raw $p $rep; done
done
python3 - <<'PY'
import csv, statistics as st
from collections import defaultdict
events = {"1day": 64591857, "2day": 120139716, "4day": 235062795, "7day": 289116899}
vol = defaultdict(list); res = defaultdict(list)
for r in csv.DictReader(open("logs/resource_times.csv")):
    row = (float(r["seconds"]), float(r["cpu_percent"]), float(r["max_rss_kb"]) / 1e6)
    (vol if r["experiment"] == "volume" else res)[r["dataset"] if r["experiment"] == "volume" else int(r["parallelism"])].append(row)
print("\nVOLUME SCALING at parallelism 8")
print("dataset  events(M)  median(s)  M events/s  cpu%  peak GB")
for ds in ("1day", "2day", "4day", "7day"):
    v = vol[ds]; t = st.median(x[0] for x in v)
    print(f"{ds:<7} {events[ds]/1e6:9.1f} {t:10.1f} {events[ds]/1e6/t:11.2f} {st.median(x[1] for x in v):5.0f} {st.median(x[2] for x in v):8.2f}")
print("\nRESOURCES, 7 days")
print("parallelism  median(s)  cpu%  peak GB")
for p in sorted(res):
    v = res[p]
    print(f"{p:>11} {st.median(x[0] for x in v):10.1f} {st.median(x[1] for x in v):5.0f} {st.median(x[2] for x in v):8.2f}")
PY
