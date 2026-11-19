#!/bin/bash
# Latency probe and its overhead: full week (data/raw), parallelism 8, literal rule, 2 reps.
#   probe on : patched jar, --latency-log, tolerance 0.1 h and 6 h   (latency percentiles)
#   probe off: patched jar, same tolerances, no option               (overhead of logging)
#   default  : 200 h, unpatched jar (output/jars/ema-e3c5fe1.jar) vs patched jar, probe off
#              (cost of the extra Tuple5 field when the probe is off)
# 200 h is not measured with the probe: no window fires before the input ends.
# Run from the main checkout (data/, output/):  /home/asus/repo-latency/scripts/latency_bench.sh
# Output: <worktree>/logs/latency_times.csv, logs/latency_bench.log; latency logs kept in output/latency/
W=$(cd "$(dirname "$0")/.." && pwd)
cd /home/asus/repo || exit 1
OLD=output/jars/ema-e3c5fe1.jar
NEW=output/jars/ema-latency-probe.jar
OUT=$W/logs/latency_times.csv
LOG=$W/logs/latency_bench.log
EVENTS=59391670
mkdir -p output/latency

AW=""
if command -v powershell.exe >/dev/null 2>&1 && [ -d /mnt/c/Users/Public ]; then
  cp scripts/keep_awake.ps1 /mnt/c/Users/Public/ema_keep_awake.ps1
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File 'C:\Users\Public\ema_keep_awake.ps1' -Minutes 45 >/dev/null 2>&1 &
  AW=$!
fi
cleanup() { [ -n "$AW" ] && kill $AW 2>/dev/null; rm -f /mnt/c/Users/Public/ema_keep_awake.ps1; }
trap cleanup EXIT

echo "variant,jar,tolerance_hours,rep,seconds,events_per_s,result_rows,results_logged,median_ms,p95_ms,p99_ms,max_ms" > $OUT
echo "$(date '+%F %T') start" > $LOG
run() { # run <variant> <jar> <tol> <rep> [--latency-log]
  local variant=$1 jar=$2 tol=$3 rep=$4 probe=$5
  local out=output/latency/${variant}_t${tol}_r${rep}
  rm -rf $out; mkdir -p $out
  local args="--input data/raw --output $out --parallelism 8 --tolerance-hours $tol"
  [ -n "$probe" ] && args="$args --latency-log $out/lat"
  local start; start=$(date +%s.%N)
  java -Xmx5g -XX:+UseParallelGC -jar $jar $args > /dev/null 2>&1
  local rc=$?
  local secs; secs=$(echo "$(date +%s.%N) - $start" | bc)
  [ $rc -eq 0 ] || { echo "$(date +%T) FAILED $variant tol=$tol rep=$rep rc=$rc" >> $LOG; return; }
  local rows; rows=$(find $out/results -type f -name '*.csv' ! -name '.*' -exec cat {} + | wc -l)
  local stats=",,,,"
  [ -n "$probe" ] && stats=$(python3 $W/scripts/latency_stats.py $out/lat)
  local eps; eps=$(echo "$EVENTS / $secs" | bc)
  echo "$variant,$(basename $jar),$tol,$rep,$secs,$eps,$rows,$stats" >> $OUT
  echo "$(date +%T) $variant tol=$tol rep=$rep ${secs}s rows=$rows stats=$stats" >> $LOG
  [ -z "$probe" ] && [ "$variant" != "probe_off" ] && rm -rf $out
}
for rep in 1 2; do
  for tol in 0.1 6; do
    run probe_on $NEW $tol $rep --latency-log
    run probe_off $NEW $tol $rep
  done
  run default_old $OLD 200 $rep
  run default_new $NEW 200 $rep
done
# outputs with the probe must equal those without (rep 1)
for tol in 0.1 6; do
  echo "== probe_on vs probe_off, tol=$tol" >> $LOG
  python3 compare_runs.py output/latency/probe_off_t${tol}_r1 output/latency/probe_on_t${tol}_r1 | tail -1 >> $LOG
done
# pooled percentiles over both reps
for tol in 0.1 6; do
  echo "pooled tol=$tol: results,median_ms,p95_ms,p99_ms,max_ms = $(python3 $W/scripts/latency_stats.py output/latency/probe_on_t${tol}_r1/lat output/latency/probe_on_t${tol}_r2/lat)" >> $LOG
done
rm -rf output/latency/probe_off_*
echo "$(date '+%F %T') done" >> $LOG
cat $LOG
