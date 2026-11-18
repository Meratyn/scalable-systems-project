#!/bin/bash
# EMA start value: full week (data/raw), parallelism 8, literal rule,
#   --ema-init first (default; seed both EMAs with the symbol's first price) -> output/w2_p8_emafirst
#   --ema-init zero  (spec: previous EMAs start at 0)                         -> output/w2_p8_emazero
# The first-mode run is compared with output/w2_p8 (must be IDENTICAL: default unchanged).
# Usage (from anywhere):  scripts/ema_init.sh   (~5 min)
# Output: logs/ema_init.csv (columns: see scripts/ema_init.py), logs/ema_init.log
cd "$(dirname "$0")/.." || exit 1
JAR=java-job/target/ema-crossover-1.0.jar
LOG=logs/ema_init.log
mkdir -p logs

AW=""
if command -v powershell.exe >/dev/null 2>&1 && [ -d /mnt/c/Users/Public ]; then
  cp scripts/keep_awake.ps1 /mnt/c/Users/Public/ema_keep_awake.ps1
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File 'C:\Users\Public\ema_keep_awake.ps1' -Minutes 30 >/dev/null 2>&1 &
  AW=$!
fi
cleanup() { [ -n "$AW" ] && kill $AW 2>/dev/null; rm -f /mnt/c/Users/Public/ema_keep_awake.ps1; }
trap cleanup EXIT

echo "$(date '+%F %T') start" > $LOG
for mode in first zero; do
  out=output/w2_p8_ema$mode
  rm -rf $out
  start=$(date +%s.%N)
  java -Xmx5g -XX:+UseParallelGC -jar $JAR --input data/raw --output $out --parallelism 8 --ema-init $mode > /dev/null 2>&1
  rc=$?
  secs=$(echo "$(date +%s.%N) - $start" | bc)
  echo "$(date +%T) $mode rc=$rc ${secs}s" >> $LOG
  [ $rc -eq 0 ] || { echo "FAILED: $mode, see $LOG"; exit 1; }
done
python3 compare_runs.py output/w2_p8 output/w2_p8_emafirst >> $LOG
python3 scripts/ema_init.py output/w2_p8_emafirst output/w2_p8_emazero > logs/ema_init.csv
echo "$(date '+%F %T') done" >> $LOG
cat $LOG; column -s, -t logs/ema_init.csv
