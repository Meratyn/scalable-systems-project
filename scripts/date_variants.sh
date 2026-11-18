#!/bin/bash
# Trading-date variants on the full week (data/raw), parallelism 8, literal rule:
#   fallback = default: an empty Trading date takes the system date (results in output/w2_p8)
#   drop     = --require-trading-date: rows with an empty Trading date are dropped
#              (results in output/w2_p8_reqdate)
# The fallback job is run only if output/w2_p8 is missing. Price events per symbol come from
# one "--stage symbolstats" run per variant.
# Usage (from anywhere):  scripts/date_variants.sh   (~4 min)
# Output: logs/date_variants.csv (see scripts/date_variants.py for the columns)
cd "$(dirname "$0")/.." || exit 1
JAR=java-job/target/ema-crossover-1.0.jar
JAVA="java -Xmx5g -XX:+UseParallelGC -jar $JAR --input data/raw --parallelism 8"
LOG=logs/date_variants.log
mkdir -p logs

AW=""
if command -v powershell.exe >/dev/null 2>&1 && [ -d /mnt/c/Users/Public ]; then
  cp scripts/keep_awake.ps1 /mnt/c/Users/Public/ema_keep_awake.ps1
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File 'C:\Users\Public\ema_keep_awake.ps1' -Minutes 30 >/dev/null 2>&1 &
  AW=$!
fi
cleanup() { [ -n "$AW" ] && kill $AW 2>/dev/null; rm -f /mnt/c/Users/Public/ema_keep_awake.ps1; }
trap cleanup EXIT

# run <name> <args...>: runs the job, prints the wall time in seconds; fails the script on error
run() {
  local name=$1; shift
  local start end rc
  start=$(date +%s.%N)
  $JAVA "$@" > /dev/null 2>&1
  rc=$?
  end=$(date +%s.%N)
  if [ $rc -ne 0 ]; then echo "$(date +%T) FAILED: $name (exit $rc)" >> $LOG; exit 1; fi
  local secs; secs=$(echo "$end - $start" | bc)
  echo "$(date +%T) $name ${secs}s" >> $LOG
  echo "$secs"
}

echo "$(date '+%F %T') start" > $LOG
FALLBACK_SECS=""
if [ ! -d output/w2_p8/results ]; then
  FALLBACK_SECS=$(run fallback_full --output output/w2_p8)
fi
rm -rf output/w2_p8_reqdate
DROP_SECS=$(run drop_full --output output/w2_p8_reqdate --require-trading-date)
[ -n "$DROP_SECS" ] || { echo "drop run failed, see $LOG"; exit 1; }
rm -rf output/w2_p8_stats output/w2_p8_reqdate_stats
run fallback_symbolstats --output output/w2_p8_stats --stage symbolstats > /dev/null
run drop_symbolstats --output output/w2_p8_reqdate_stats --stage symbolstats --require-trading-date > /dev/null

python3 scripts/date_variants.py \
  "fallback,output/w2_p8,output/w2_p8_stats,$FALLBACK_SECS" \
  "drop,output/w2_p8_reqdate,output/w2_p8_reqdate_stats,$DROP_SECS" > logs/date_variants.csv
echo "$(date '+%F %T') done" >> $LOG
column -s, -t logs/date_variants.csv
