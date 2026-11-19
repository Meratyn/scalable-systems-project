# scalable-systems-project

CS-E4780 Course Project — Detecting Trading Trends in Financial Tick Data
(DEBS 2022 Grand Challenge data).

For every symbol, the job computes EMA38 and EMA100 over 5-minute tumbling
event-time windows and emits a BUY or SELL advisory when the two EMAs cross.
It is written in Java on Apache Flink 1.20.1 (`java-job/`).

## Requirements

- Java 11 or newer, Maven 3
- Python 3 (only for helper scripts: `compare_runs.py`, `bench_summary.py`, `make_small_set.py`, `check_stale.py`)
- About 6 GB RAM for the full week

## Data

Download the 7 daily CSV files from https://doi.org/10.5281/zenodo.6382482 and
put them in `data/raw/`. The `data/` folder is gitignored.

Smaller subsets used during development (also gitignored):

| Folder | Contents |
|---|---|
| `data/raw1`, `data/raw2`, `data/raw4` | first 1, 2 and 4 day-files |
| `data/small` | 20-minute slice of each day, for quick correctness checks |

`data/small` was cut with `python3 make_small_set.py <in_dir> <out_dir> <start> <end>`.
`check_stale.py <file.csv>` counts price events that would arrive late (old trades re-reported).

## Build

```sh
./build.sh
```

This produces `java-job/target/ema-crossover-1.0.jar` (a fat jar that runs
Flink in local mode, so no Flink cluster is needed).

## Run

```sh
java -Xmx5g -XX:+UseParallelGC -jar java-job/target/ema-crossover-1.0.jar \
     --input data/raw --output output/x --parallelism 8
```
For ease of execution, this command can also be found as a script:

```sh
./run.sh
```


### Options

| Option | Default | Meaning |
|---|---|---|
| `--input <dir>` | `data/raw_test/` | folder with the CSV files |
| `--output <dir>` | `output` | results go to `<dir>/results/` |
| `--parallelism <n>` | `1` | number of parallel workers |
| `--strict-crossover` | off | require the previous EMAs to differ (see below) |
| `--tolerance-hours <h>` | `200` | watermark out-of-orderness bound |
| `--late-output` | off | write events that arrive after their window closed to `<dir>/late/` |
| `--source file\|legacy` | `file` | `file` = FileSource (one reader per file); `legacy` = splittable `readTextFile` |
| `--require-trading-date` | off | drop rows with an empty Trading date instead of using the system date |
| `--object-reuse` | off | Flink object reuse (performance experiment) |
| `--parser fast\|reference` | `fast` | `reference` = original split-based parser (ablation) |
| `--exchange FR\|NL\|ETR\|ALL` | `ALL` | keep only the events of one exchange (symbol suffix); all files are still read and parsed |
| `--stage <s>` | `full` | `read`, `parse`, `symbolstats` or `full` (see Experiments) |

### Output format

One line per (symbol, window):

```
symbol,window_end_ms,ema38,ema100,advisory
```

`advisory` is `BUY`, `SELL` or empty. `window_end_ms` is the window end in
epoch milliseconds (UTC).

## How it works

1. **Parse.** Each CSV row with a `Last` price and a `Trading time` is a price
   event. The event time is `Trading date` + `Trading time`. If `Trading date`
   is empty (index rows and Frankfurt/ETR rows), the system date (column 3) is
   used instead. This gives 59.4M price events for 4,970 symbols (FR, NL, ETR).
2. **Window.** Events are keyed by symbol and grouped into 5-minute tumbling
   event-time windows. Each window keeps only its latest price (incremental reduce).
3. **EMA and crossover.** When a window fires, keyed state updates
   EMA_j = Last·(2/(1+j)) + EMA_j,prev·(1 − 2/(1+j)) for j = 38 and 100.
   - BUY: EMA38 > EMA100 now, and EMA38 ≤ EMA100 in the previous window.
   - SELL: EMA38 < EMA100 now, and EMA38 ≥ EMA100 in the previous window.
   - Both EMAs are seeded with the symbol's first price, so the first window
     never has an advisory. The spec starts at 0; we seed with the first price so
     that the EMAs need no ramp-up and not every symbol gets a BUY in its first
     window (`--ema-init zero` gives the spec's start).
   - This literal rule (equality in the previous window allowed) follows the
     specification's equations literally and is the main result (24,836
     advisories for the full week).
   - With `--strict-crossover`, the previous comparison must be strict (`<` / `>`);
     this is an extra analysis only.
     This removes "crossovers" that start from equal EMAs, for example in a
     symbol's second window, right after seeding.

For a window without events we emit no result and let the EMAs continue from
their last values.

**Window price and ties.** The window price is the event with the latest
trading timestamp. On equal timestamps the later
event in input order wins: files sorted by name (the day), then line. In file
mode (`--source file`) each file has one reader and Flink's channels keep line
order, so the later line of the file wins at every parallelism (JUnit test
`equalTimestampsTakeTheLaterLinesPrice`). In the full week 92,233 of 1,857,457
windows (5.0 %) have a shared latest timestamp, 23,320 (1.26 %) with different
prices, all within a single file (`ema.TieReport`, `logs/tie_report.txt`).
`--source legacy` splits files across readers and is not covered by this rule.

**Determinism.** With the default 200 h tolerance, no event is ever late, and
all windows fire when the input ends. The output is therefore identical at every
parallelism level.

## Results (full week, 7 days)

| | value |
|---|---|
| windows | 1,857,457 |
| crossovers (default rule) | 24,836 |
| crossovers (`--strict-crossover`) | 19,989 |

## Checking correctness

Compare two output folders. The script reports window counts, crossovers and any
differing windows:

```sh
python3 compare_runs.py output/p1 output/p8
```

Results must be IDENTICAL across parallelism levels.

## Visualization

`java-job/viz/index.html` is a static page (no server, no dependencies) to explore the results:
EMA charts per symbol with zoom, BUY/SELL markers, a literal/strict toggle, a day × hour heat map and
the benchmark results. Generate its data from a results folder and the benchmark logs, then open the page:

```sh
java -cp java-job/target/ema-crossover-1.0.jar ema.VizData output/w2_p8/results java-job/viz/data.js --top 150
java -cp java-job/target/ema-crossover-1.0.jar ema.PerfData
```

For ease of execution, this command can also be found as a script:

```sh
./scripts/visualization.sh
```

See `java-job/viz/README.md` for the options.

## Experiments

All measurements, with the tables and longer explanations that do not fit the
report, are in [`docs/EXPERIMENTS.md`](docs/EXPERIMENTS.md).

| Script | What it measures | Output |
|---|---|---|
| `benchmark.sh` | runtime at parallelism 1/2/4/8, 4 and 7 days, 3 runs | `logs/bench_times.csv` (summarise with `python3 bench_summary.py logs/bench_times.csv`) |
| `stage_bench.sh` | runtime of only reading / reading+parsing | `logs/stage_times.csv` |
| `ablation.sh` | one-factor-at-a-time ablation: parser, GC, source, file splitting, object reuse (7 days, p=8) | `logs/ablation_times.csv`, `logs/ablation.log` |
| `scripts/exchange_bench.sh` | per exchange (FR, NL, ETR, ALL) at parallelism 1/2/4/8, 7 days, 2 runs | `logs/exchange_times.csv` |
| `scripts/cpu_limit_bench.sh` | 7 days at parallelism 8 limited with `taskset` to 1/2/4/6/12 CPUs, 2 runs | `logs/cpu_limit_times.csv` |
| `scripts/lateness_bench.sh` | late events vs `--tolerance-hours` 200/24/6/1/0.1, 7 days, parallelism 8, 2 runs | `logs/lateness_times.csv` |
| `resource_bench.sh` | volume scaling (1–7 days) and CPU % / peak memory | `logs/resource_times.csv` |

Key skew per worker (uses Flink's own key-group assignment):

```sh
java -jar java-job/target/ema-crossover-1.0.jar --input data/raw --output output/stats --stage symbolstats
java -cp java-job/target/ema-crossover-1.0.jar ema.SkewReport output/stats/symbolstats
```
For ease of execution, this command can also be found as a script:

```sh
./scripts/key_skew.sh
```


Timestamp ties per (symbol, window) (single pass, ~4.5 min, 2.1 GB):

```sh
java -Xmx3g -cp java-job/target/ema-crossover-1.0.jar ema.TieReport data/raw > logs/tie_report.txt
```

For ease of execution, this command can also be found as a script:

```sh
./scripts/timestamp_ties.sh
```

Median runtime for the full week (3 runs, 12-thread laptop CPU):
109 s (1 worker), 77 s (2), 66 s (4), 57 s (8).
