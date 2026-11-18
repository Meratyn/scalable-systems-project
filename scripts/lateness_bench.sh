#!/bin/bash
# Late events vs watermark tolerance: full week (data/raw, 7 days), parallelism 8 (as the main run), --late-output,
# --tolerance-hours 200 (reference, run first), 24, 6, 1, 0.1; 2 repetitions each.
# A smaller tolerance lets windows fire earlier, but events that arrive after their window has
# closed are late: they go to <output>/late instead of the results, so windows can change.
# With a small tolerance the result can also differ between runs (it depends on which reader gets
# which file and how fast each reads); the 2 repetitions show this.
# Usage (from anywhere):  scripts/lateness_bench.sh   (~15-20 min)
# Output: logs/lateness_times.csv
#   tolerance_hours,rep,seconds,late_events,result_rows,rows_differing
#   rows_differing = windows only in the reference + only in this run + in both with different values,
#   compared with the 200-hour run of rep 1 (compare_runs.py).
cd "$(dirname "$0")/.." || exit 1
JAR=java-job/target/ema-crossover-1.0.jar
OUT=logs/lateness_times.csv
REF=output/late_ref_200h
REPS=2
mkdir -p logs

AW=""
if command -v powershell.exe >/dev/null 2>&1 && [ -d /mnt/c/Users/Public ]; then
  cp scripts/keep_awake.ps1 /mnt/c/Users/Public/ema_keep_awake.ps1
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File 'C:\Users\Public\ema_keep_awake.ps1' -Minutes 60 >/dev/null 2>&1 &
  AW=$!
fi
cleanup() { [ -n "$AW" ] && kill $AW 2>/dev/null; rm -f /mnt/c/Users/Public/ema_keep_awake.ps1; rm -rf output/latebench_tmp; }
trap cleanup EXIT

count_lines() { if [ -d "$1" ]; then find "$1" -type f -name '*.csv' ! -name '.*' -exec cat {} + | wc -l; else echo 0; fi; }

echo "tolerance_hours,rep,seconds,late_events,result_rows,rows_differing" > $OUT
for rep in $(seq 1 $REPS); do
  for tol in 200 24 6 1 0.1; do
    tmp=output/latebench_tmp
    rm -rf $tmp
    start=$(date +%s.%N)
    java -Xmx5g -XX:+UseParallelGC -jar $JAR --input data/raw --output $tmp --parallelism 8 \
         --tolerance-hours $tol --late-output > /dev/null 2>&1
    rc=$?
    end=$(date +%s.%N)
    secs=$(echo "$end - $start" | bc)
    if [ $rc -ne 0 ]; then echo "FAILED: tol=$tol rep=$rep (exit $rc)"; rm -rf $tmp; continue; fi
    late=$(count_lines $tmp/late)
    rows=$(count_lines $tmp/results)
    if [ "$tol" = "200" ] && [ $rep -eq 1 ]; then rm -rf $REF; cp -r $tmp $REF; fi
    diff=$(python3 compare_runs.py $REF $tmp | awk -F': ' '/^windows only in|^windows in both with different values/ {s+=$2} END {print s+0}')
    echo "$tol,$rep,$secs,$late,$rows,$diff" >> $OUT
    echo "$(date +%H:%M) tol=${tol}h rep=$rep ${secs}s late=$late rows=$rows differing=$diff"
    rm -rf $tmp
  done
done
rm -rf $REF
echo; column -s, -t $OUT
