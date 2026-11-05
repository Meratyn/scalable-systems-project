#!/bin/bash
# Scaling with limited CPU resources: full week (data/raw) at parallelism 8, with taskset limiting the
# JVM to 1, 2, 4, 6 and 12 logical CPUs, 2 repetitions, interleaved.
# Usage (from anywhere):  scripts/cpu_limit_bench.sh  (~35 min)
# Output: logs/cpu_limit_times.csv (cpus,cpuset,rep,seconds,cpu_seconds,max_rss_kb)
#
# CPU sets: WSL shows 6 cores x 2 threads (siblings 0/1, 2/3, ... 10/11) and hides which host cores are
# performance or efficiency cores; Windows may also move WSL's virtual CPUs between host cores. So taskset
# limits HOW MANY virtual CPUs the job can use, not WHICH physical cores. One thread per virtual core first,
# then the siblings. The JVM sees the limit too (availableProcessors, GC threads follow the affinity mask).
cd "$(dirname "$0")/.." || exit 1
JAR=java-job/target/ema-crossover-1.0.jar
OUT=logs/cpu_limit_times.csv
REPS=2
LIMIT=900   # seconds; a run that takes longer is recorded as TIMEOUT and not retried
SETS=("1:0" "2:0,2" "4:0,2,4,6" "6:0,2,4,6,8,10" "12:0-11")
mkdir -p logs

AW=""
if command -v powershell.exe >/dev/null 2>&1 && [ -d /mnt/c/Users/Public ]; then
  cp scripts/keep_awake.ps1 /mnt/c/Users/Public/ema_keep_awake.ps1
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File 'C:\Users\Public\ema_keep_awake.ps1' -Minutes 90 >/dev/null 2>&1 &
  AW=$!
fi
TV=$(mktemp)
cleanup() { [ -n "$AW" ] && kill $AW 2>/dev/null; rm -f /mnt/c/Users/Public/ema_keep_awake.ps1 "$TV"; rm -rf output/cpubench_tmp; }
trap cleanup EXIT

echo "cpus,cpuset,rep,seconds,cpu_seconds,max_rss_kb" > $OUT
for rep in $(seq 1 $REPS); do
  for s in "${SETS[@]}"; do
    n=${s%%:*}; set_=${s#*:}; csvset=${set_//,/;}   # ';' in the CSV so the cpuset stays one field
    tmp=output/cpubench_tmp
    rm -rf $tmp
    start=$(date +%s.%N)
    timeout $LIMIT /usr/bin/time -v -o "$TV" taskset -c "$set_" \
      java -Xmx5g -XX:+UseParallelGC -jar $JAR --input data/raw --output $tmp --parallelism 8 > /dev/null 2>&1
    rc=$?
    end=$(date +%s.%N)
    secs=$(echo "$end - $start" | bc)
    if [ $rc -eq 124 ]; then echo "TIMEOUT (> ${LIMIT}s): cpus=$n rep=$rep"; echo "$n,$csvset,$rep,TIMEOUT,," >> $OUT
    elif [ $rc -ne 0 ]; then echo "FAILED: cpus=$n rep=$rep (exit $rc)"
    else
      cpu=$(awk -F': ' '/User time \(seconds\)/{u=$2} /System time \(seconds\)/{s=$2} END{printf "%.2f", u+s}' "$TV")
      rss=$(awk -F': ' '/Maximum resident set size/{print $2}' "$TV")
      echo "$n,$csvset,$rep,$secs,$cpu,$rss" >> $OUT
      echo "$(date +%H:%M) cpus=$n ($set_) rep=$rep ${secs}s cpu=${cpu}s rss=${rss}kB"
    fi
    rm -rf $tmp
  done
done

echo; echo "Medians per CPU count (wall s, CPU s, CPU s / wall s = average busy CPUs):"
awk -F, 'NR>1 && $4!="TIMEOUT" {w[$1]=w[$1]" "$4; c[$1]=c[$1]" "$5}
  function med(str,  a,n){n=split(str,a," "); asort(a); return (n%2)?a[(n+1)/2]:(a[n/2]+a[n/2+1])/2}
  END {for (k in w) printf "%3d CPUs  %7.1f s wall  %7.1f s CPU  %5.2f busy\n", k, med(w[k]), med(c[k]), med(c[k])/med(w[k])}' $OUT | sort -n
