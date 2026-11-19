#!/bin/bash
# Paced-replay speed sweep on ONE weekday without the 01:00 placeholder rows (11 Nov 2021).
# Tolerance 0.1 h, probe on, parallelism 8; speeds 60, 300, 1500 and one unpaced run.
# Runs back to back; analysis (ema.PaceDelay) only after the last run, so nothing else runs
# during the measurements.
# Run from anywhere:  scripts/pace_sweep.sh                 (speeds 60 300 1500 0; 0 = unpaced)
#                     TAG=_fast scripts/pace_sweep.sh 3000 6000   (extra speeds, separate log files)
# Output: logs/pace_sweep${TAG}.log, pace_sweep${TAG}_times.csv, pace_<name>.txt; latency logs in output/pace/
W=$(cd "$(dirname "$0")/.." && pwd)
cd /home/asus/repo || exit 1
JAR=$W/java-job/target/ema-crossover-1.0.jar
DAY=data/raw/debs2022-gc-trading-day-11-11-21.csv
TOL=0.1
EVENTS=11835403
SPEEDS=${*:-60 300 1500 0}
OUT=$W/logs/pace_sweep${TAG}_times.csv
LOG=$W/logs/pace_sweep${TAG}.log
mkdir -p output/pace

AW=""
if command -v powershell.exe >/dev/null 2>&1 && [ -d /mnt/c/Users/Public ]; then
  cp scripts/keep_awake.ps1 /mnt/c/Users/Public/ema_keep_awake.ps1
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File 'C:\Users\Public\ema_keep_awake.ps1' -Minutes 40 >/dev/null 2>&1 &
  AW=$!
fi
cleanup() { [ -n "$AW" ] && kill $AW 2>/dev/null; rm -f /mnt/c/Users/Public/ema_keep_awake.ps1; }
trap cleanup EXIT

echo "name,speed,tolerance_hours,seconds,events_per_s,result_rows,rc" > $OUT
echo "$(date '+%F %T') start" > $LOG
run() { # run <name> <speed|0>
  local name=$1 speed=$2 out=output/pace/$1
  rm -rf $out; mkdir -p $out
  local args="--input $DAY --output $out --parallelism 8 --tolerance-hours $TOL --latency-log $out/lat"
  [ "$speed" != "0" ] && args="$args --replay-speed $speed"
  echo "$(date +%T) begin $name" >> $LOG
  local start; start=$(date +%s.%N)
  java -Xmx5g -XX:+UseParallelGC -jar $JAR $args > $out/stdout.txt 2>&1
  local rc=$?
  local secs; secs=$(echo "$(date +%s.%N) - $start" | bc)
  local rows; rows=$(find $out/results -type f -name '*.csv' ! -name '.*' -exec cat {} + 2>/dev/null | wc -l)
  local eps; eps=$(echo "$EVENTS / $secs" | bc)
  echo "$name,$speed,$TOL,$secs,$eps,$rows,$rc" >> $OUT
  echo "$(date +%T) end $name rc=$rc ${secs}s rows=$rows events_per_s=$eps" >> $LOG
}
name_of() { [ "$1" = "0" ] && echo unpaced || echo s$1; }
for sp in $SPEEDS; do run $(name_of $sp) $sp; done
echo "$(date '+%F %T') runs done; analysis" >> $LOG

for sp in $SPEEDS; do
  name=$(name_of $sp); speed=$sp; [ "$sp" = "0" ] && speed=Infinity
  java -Xmx3g -cp $JAR ema.PaceDelay analyze $DAY $speed $TOL output/pace/$name/lat > $W/logs/pace_$name.txt
done
# results must not depend on the speed
for sp in $SPEEDS; do
  name=$(name_of $sp); [ "$name" = "s60" ] && continue
  echo "== s60 vs $name" >> $LOG
  python3 compare_runs.py output/pace/s60 output/pace/$name | tail -1 >> $LOG
done
echo "$(date '+%F %T') done" >> $LOG
