#!/bin/bash
# Usage: ./benchmark.sh   (3 reps x 2 datasets x 4 parallelism levels)
JAR=java-job/target/ema-crossover-1.0.jar
OUT=logs/bench_times.csv
mkdir -p logs
echo "dataset,parallelism,rep,seconds" > $OUT
for rep in 1 2 3; do
  for ds in raw4:4day raw:7day; do
    dir=${ds%%:*}; name=${ds##*:}
    for p in 1 2 4 8; do
      tmp=output/bench_tmp
      rm -rf $tmp
      start=$(date +%s.%N)
      java -Xmx5g -jar $JAR --input data/$dir --output $tmp --parallelism $p > /dev/null 2>&1
      rc=$?
      end=$(date +%s.%N)
      secs=$(echo "$end - $start" | bc)
      if [ $rc -ne 0 ]; then echo "FAILED: $name p=$p rep=$rep (exit $rc)"; else
        echo "$name,$p,$rep,$secs" >> $OUT
        echo "$(date +%H:%M) $name p=$p rep=$rep ${secs}s"; fi
      rm -rf $tmp
    done
  done
done
python3 bench_summary.py $OUT
