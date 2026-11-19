#!/bin/bash
# Clean overhead check, machine idle: main jar vs latency-probe jar, probe OFF (no --latency-log,
# no --replay-speed), 200 h tolerance, full week, parallelism 8, 3 alternating runs each.
# Output: <worktree>/logs/overhead_clean.log, overhead_clean_times.csv
W=$(cd "$(dirname "$0")/.." && pwd)
cd /home/asus/repo || exit 1
MAIN=output/jars/ema-main-206ffa1.jar
PROBE=output/jars/ema-latency-probe-pace.jar
OUT=$W/logs/overhead_clean_times.csv
LOG=$W/logs/overhead_clean.log
EVENTS=59391670
AW=""
if command -v powershell.exe >/dev/null 2>&1 && [ -d /mnt/c/Users/Public ]; then
  cp scripts/keep_awake.ps1 /mnt/c/Users/Public/ema_keep_awake.ps1
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File 'C:\Users\Public\ema_keep_awake.ps1' -Minutes 30 >/dev/null 2>&1 &
  AW=$!
fi
cleanup() { [ -n "$AW" ] && kill $AW 2>/dev/null; rm -f /mnt/c/Users/Public/ema_keep_awake.ps1; }
trap cleanup EXIT
echo "variant,jar,rep,seconds,events_per_s,result_rows,rc" > $OUT
echo "$(date '+%F %T') start" > $LOG
run() { # run <variant> <jar> <rep>
  local out=output/overhead/$1_r$3
  rm -rf $out; mkdir -p $out
  local start; start=$(date +%s.%N)
  java -Xmx5g -XX:+UseParallelGC -jar $2 --input data/raw --output $out --parallelism 8 --tolerance-hours 200 > /dev/null 2>&1
  local rc=$?
  local secs; secs=$(echo "$(date +%s.%N) - $start" | bc)
  local rows; rows=$(find $out/results -type f -name '*.csv' ! -name '.*' -exec cat {} + | wc -l)
  echo "$1,$(basename $2),$3,$secs,$(echo "$EVENTS / $secs" | bc),$rows,$rc" >> $OUT
  echo "$(date +%T) $1 rep=$3 rc=$rc ${secs}s rows=$rows" >> $LOG
}
for rep in 1 2 3; do
  run main $MAIN $rep
  run probe_off $PROBE $rep
done
echo "== main_r1 vs probe_off_r1" >> $LOG
python3 compare_runs.py output/overhead/main_r1 output/overhead/probe_off_r1 | tail -1 >> $LOG
rm -rf output/overhead
echo "$(date '+%F %T') done" >> $LOG
