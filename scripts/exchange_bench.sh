#!/bin/bash
# Per-exchange scalability: full week (data/raw), --exchange FR|NL|ETR|ALL at parallelism 1/2/4/8,
# 2 repetitions, interleaved like benchmark.sh. Every run still reads and parses all 7 files;
# only the events of the chosen exchange reach the keyed window stage.
# Usage (from anywhere):  scripts/exchange_bench.sh      (~40 min)
# Output: logs/exchange_times.csv  (exchange,parallelism,rep,seconds,result_rows)
#         logs/exchange_events.csv (exchange,symbols,price_events; from logs/skew_report_v2.txt)
cd "$(dirname "$0")/.." || exit 1
JAR=java-job/target/ema-crossover-1.0.jar
OUT=logs/exchange_times.csv
REPS=2
mkdir -p logs

# Keep Windows awake (WSL only): dropped automatically when this script exits.
AW=""
if command -v powershell.exe >/dev/null 2>&1 && [ -d /mnt/c/Users/Public ]; then
  cp scripts/keep_awake.ps1 /mnt/c/Users/Public/ema_keep_awake.ps1
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File 'C:\Users\Public\ema_keep_awake.ps1' -Minutes 90 >/dev/null 2>&1 &
  AW=$!
fi
cleanup() { [ -n "$AW" ] && kill $AW 2>/dev/null; rm -f /mnt/c/Users/Public/ema_keep_awake.ps1; rm -rf output/exbench_tmp; }
trap cleanup EXIT

# Price events kept per exchange (already measured by --stage symbolstats; see logs/skew_report_v2.txt).
echo "exchange,symbols,price_events" > logs/exchange_events.csv
awk '$1 ~ /^(ETR|FR|NL)$/ && $3=="symbols" {print $1","$2","$4; s+=$2; e+=$4} END {print "ALL,"s","e}' \
    logs/skew_report_v2.txt >> logs/exchange_events.csv

echo "exchange,parallelism,rep,seconds,result_rows" > $OUT
for rep in $(seq 1 $REPS); do
  for ex in FR NL ETR ALL; do
    for p in 1 2 4 8; do
      tmp=output/exbench_tmp
      rm -rf $tmp
      start=$(date +%s.%N)
      java -Xmx5g -XX:+UseParallelGC -jar $JAR --input data/raw --output $tmp --parallelism $p --exchange $ex > /dev/null 2>&1
      rc=$?
      end=$(date +%s.%N)
      secs=$(echo "$end - $start" | bc)
      if [ $rc -ne 0 ]; then echo "FAILED: $ex p=$p rep=$rep (exit $rc)"; else
        rows=$(find $tmp/results -type f -name '*.csv' ! -name '.*' -exec cat {} + | wc -l)
        echo "$ex,$p,$rep,$secs,$rows" >> $OUT
        echo "$(date +%H:%M) $ex p=$p rep=$rep ${secs}s rows=$rows"; fi
      rm -rf $tmp
    done
  done
done

echo; echo "Median seconds (rows) per exchange and parallelism:"
awk -F, 'NR>1 {k=$1" p="$2; t[k]=t[k]" "$4; r[k]=$5} END {for (k in t) {n=split(t[k],a," "); asort(a); m=(n%2)?a[(n+1)/2]:(a[n/2]+a[n/2+1])/2; printf "%-10s %7.1f s  (%d rows)\n", k, m, r[k]}}' \
    $OUT | sort -k1,1 -k2,2V
