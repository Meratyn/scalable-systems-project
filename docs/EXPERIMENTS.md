# Experiments and details

This file holds the measurements and longer explanations that do not fit the
5-page report (submitted separately). Every number here comes from a log in
`logs/`; the script or tool that produced it is named in each section.
Sections follow the order of the report.

Unless stated otherwise: full week (`data/raw`, 7 files, 59,391,670 price
events), parallelism 8, literal crossover rule, first-price EMA seed, 200 h
watermark tolerance, `-Xmx5g -XX:+UseParallelGC`, one laptop (see the report's
Setup paragraph).

## 0. Baseline and ablation notes

An earlier PyFlink prototype of the same logic needed about 85 minutes for four
days of data; the Java port is more than 30× faster. The Java single-worker run
is the baseline for all measurements.

Ablation (table in the report): Parallel GC is 16 % faster than G1 and uses
1.7× less memory, because G1 grows the heap more eagerly.

## 1. Data cleaning

### 1.1 Why input lines are dropped

`ema.DropReport` (`logs/drop_reasons.csv`) counts every input line under the
first parser rule it fails. The table is also in the report.

| Reason | Lines | Share |
|---|---:|---:|
| Empty `Last` | 229,408,756 | 79.35 % |
| `Last` = 0 | 316,382 | 0.11 % |
| Comment or header line | 91 | 0.00 % |
| Other (short row, no `Trading time`, unparsable) | 0 | 0 % |
| Kept as price events | 59,391,670 | 20.54 % |
| Total | 289,116,899 | 100 % |

Of the kept price events, 54,479,627 (18.84 % of all lines) have an empty
`Trading date` and use the system-date fallback, and 5,506 are the 01:00
placeholder rows (section 1.4).

**Rows without a price.** Rows with an empty or zero `Last` contain no trade
price. In a sample (the first two million lines of one file), 64 % of the rows
with an empty `Last` carry only the symbol and system time; the others also
carry a bid or ask quote or a total volume.

**File metadata.** Each file starts with 13 non-data lines (an 11-line licence
comment, the column header and a column description), 91 in total.

### 1.2 Rows without a trading date (`--require-trading-date`)

The instructor allows rows with an empty `Trading date` to be ignored. Our main
run uses the system-date fallback; `--require-trading-date` drops these rows.
Source: `logs/date_variants.csv`, `logs/date_variants.log`.

| | Fallback (main run) | Drop | Dropped |
|---|---:|---:|---:|
| Price events | 59,391,670 | 4,912,043 | 91.7 % |
| Result rows | 1,857,457 | 156,903 | 91.6 % |
| Symbols | 4,970 | 950 | 80.9 % |
| &nbsp;&nbsp;ETR / FR / NL | 2,126 / 2,285 / 559 | 0 / 789 / 161 | |
| Signals (BUY+SELL) | 24,836 | 2,889 | 88.4 % |
| &nbsp;&nbsp;BUY / SELL | 12,585 / 12,251 | 1,425 / 1,464 | |
| &nbsp;&nbsp;ETR / FR / NL | 8,805 / 12,419 / 3,612 | 0 / 2,309 / 580 | |

"Dropped" is (fallback − drop) / fallback. ETR disappears completely, since no
Frankfurt row has a trading date.

### 1.3 Timestamp ties

`ema.TieReport` (`logs/tie_report.txt`). In 92,233 of the 1,857,457
(symbol, window) groups (5.0 %) the latest timestamp is shared by two or more
events, and in 23,320 (1.26 %) those events have different prices
(ETR 10,051, FR 10,995, NL 2,274). All of these ties lie within a single file,
so the later line wins at every parallelism (unit test in
`JobCorrectnessTest`).

### 1.4 Placeholder rows and late events

On 10 November, 5,506 index rows for 1,812 FR/NL indices were published at
18:56 with an empty `Trading date` and a placeholder `Trading time` of
01:00:00.000. The system-date fallback places them at 01:00 that morning, which
creates one window per index outside trading hours with 53 advisories under the
literal rule (51 under the strict rule). The main run keeps them;
`--drop-placeholder-time` drops exactly these rows (empty `Trading date` and
`Trading time` 01:00:00.000, nothing else; `PlaceholderDropTest`).

**Late events vs tolerance** (`scripts/lateness_bench.sh`,
`logs/lateness_times.csv`, `logs/lateness_bench.log`; 2 runs each). Tolerances
of 200 h and 24 h give no late events. Tolerances of 6 h, 1 h and 0.1 h all
give exactly 5,506 late events, the same in both runs. In the 0.1 h run these
are exactly the 5,506 placeholder rows (in the 10 November file about 18 h after
their event time); no other event is late.

**Effect of dropping them** (`logs/placeholder_impact.txt`; the 0.1 h run and
the `--drop-placeholder-time` run give the same result). Dropping them removes
the 01:05 window of each of these indices. Only these indices are affected: all
other symbols give bit-identical results. For 1,810 of them the dropped window
is not their first, so the seed is unchanged, but the EMAs miss one update; the
difference decays only by a factor of 37/39 (EMA38) and 99/101 (EMA100) per
window, so it persists in later windows (up to 1.6 % for EMA38 and 0.74 % for
EMA100). For the other two indices the dropped window was the first, so their
seed changes. In later windows of the affected indices 886 advisories disappear
and 903 appear (none flips between BUY and SELL), giving 24,800 advisories
instead of 24,836. Compared with the 200 h main run, a 6-minute tolerance
changes the total by 36 of 24,836 advisories (0.15 %; 1,842 individual
advisories differ, all in these indices).

### 1.5 Event-time order inside the files

**Within a symbol** (`ema.OrderReport`, `logs/order_report.txt`; placeholder
rows excluded). Each price row is compared with the previous price row of the
same symbol in the same file.

| Exchange | Symbols | Symbols with a backward step | Pairs compared | Backward steps |
|---|---:|---:|---:|---:|
| ETR | 2,126 | 0 | 31,985,543 | 0 |
| FR | 2,285 | 0 | 21,352,016 | 0 |
| NL | 559 | 0 | 6,024,388 | 0 |
| All | 4,970 | 0 | 59,361,947 | 0 |

**Across symbols** (`ema.DisorderReport`, `logs/disorder_report.txt`). Lag of a
price row = largest event time seen so far in the same file (all symbols) −
the row's event time. Placeholder rows excluded, as rows and from the running
maximum. Percentile: nearest rank.

| File | Price rows | Max lag (s) | p99.9 lag (s) | > 6 min | > 1 min | > 1 s |
|---|---:|---:|---:|---:|---:|---:|
| 08-11 | 11,794,520 | 64.517 | 0.334 | 0 | 4 | 1,147 |
| 09-11 | 11,900,121 | 63.760 | 0.407 | 0 | 2 | 1,088 |
| 10-11 | 12,062,767 | 64.788 | 0.486 | 0 | 1 | 1,072 |
| 11-11 | 11,835,403 | 64.739 | 0.388 | 0 | 2 | 1,095 |
| 12-11 | 11,793,353 | 64.758 | 0.364 | 0 | 2 | 1,040 |
| 13-11, 14-11 | 0 | – | – | – | – | – |
| All | 59,386,164 | 64.788 | 0.401 | 0 | 11 | 5,442 |

Watermarks are generated per reader, and at 8 workers each file has its own
reader, so a tolerance of about 65 s would have made no row late except the
placeholder rows. This is derived from the lags above, not run.

## 2. EMA starting value (`--ema-init`)

The specification starts both EMAs at 0. We seed both with the symbol's first
window price, which the instructor allows if the choice is explained.
`--ema-init zero` gives the specification's start (`EmaInitZeroTest` checks it
against a hand-computed example). Both modes see the same 1,857,457 windows of
4,970 symbols. Source: `logs/ema_init.csv`, `logs/ema_init.log`. Windows are
numbered from 0 per symbol.

| | First price (ours) | Zero (spec) |
|---|---:|---:|
| Advisories (BUY / SELL) | 24,836 (12,585 / 12,251) | 11,355 (7,772 / 3,583) |
| &nbsp;&nbsp;in window 0 | 0 | 4,970 |
| &nbsp;&nbsp;in windows 1–99 | 10,981 | 0 |
| &nbsp;&nbsp;from window 100 | 13,855 | 6,385 |
| Windows until EMA38 within 1 % of P (median) | 1 (by construction) | 88 |
| Symbols never within 1 % | 0 | 1,523 |

With a zero start, EMA38 rises faster than EMA100, so in window 0 the previous
EMAs are equal (both 0) and EMA38 is above EMA100: every symbol gets a BUY.
While both EMAs ramp up, EMA38 stays above EMA100, and no symbol has an
advisory in windows 1–99. EMA38 needs a median of 88 windows (about 7 h of
trading) to come within 1 % of the price, as (37/39)^n ≤ 0.01 predicts
(n ≈ 88), and 1,523 symbols never get there within the week. The median is
over the 3,447 symbols that do; the window price is recovered from the
first-mode EMA38, which a spot check on PHIA.NL (516 windows) matches to a
relative error below 2·10⁻¹⁵. EMA100 should need about 230 windows by the same
formula ((99/101)^n ≤ 0.01); we did not measure this. We chose the first-price
seed because it has no ramp-up, does not emit a BUY at every symbol's first
window, and gives meaningful EMAs for the 1,523 symbols that a zero start never
brings close to their price. Its cost is the 4,847 equal-start signals, which
the strict rule removes: 3,738 are in window 1 and 1,109 come later, after flat
stretches in which EMA38 and EMA100 stay exactly equal.

## 3. Why the speedup levels off

The speedup table (4 and 7 days, parallelism 1–8) stays in the report.

### 3.1 Key skew and the long tail

`ema.SkewReport` on the per-symbol event counts (`logs/skew_report_v2.txt`),
using Flink's own key-group assignment (128 key groups). The keyed window
stage can only be as fast as its busiest worker.

| Workers | Busiest worker | Ideal | Best possible speedup |
|---:|---:|---:|---:|
| 2 | 50.8 % | 50.0 % | 1.97× |
| 4 | 27.5 % | 25.0 % | 3.64× |
| 8 | 14.3 % | 12.5 % | 6.97× |
| 12 | 10.0 % | 8.3 % | 10.03× |

The load is not dominated by a few heavy symbols (long tail):

| Busiest k symbols | Share of all price events |
|---:|---:|
| 1 | 0.27 % |
| 10 | 2.71 % |
| 50 | 13.48 % |
| 100 | 26.90 % |
| 500 | 54.76 % |

306 symbols (6.16 %) are needed to cover half of all events. The ten busiest
symbols are all ETR, each with about 160,000 events.

### 3.2 Window and EMA work

Runs that stop after reading (`--stage read`) or after parsing
(`--stage parse`) show that reading and parsing take most of the runtime
(`stage_bench.sh`, `logs/stage_times.csv`). Reading alone took 61 s with one
worker and 26 s with eight (2.3×).

**Per exchange** (`scripts/exchange_bench.sh`, `logs/exchange_times.csv`;
7 days, median of 2 runs, measured in a separate session from the speedup
table). Runs restricted to one exchange (`--exchange`) still read and parse
every file.

| Exchange | Events | 1 worker | 8 workers | Speedup |
|---|---:|---:|---:|---:|
| NL | 6.0M | 72.2 s | 38.5 s | 1.87 |
| ETR | 32.0M | 77.3 s | 38.6 s | 2.00 |
| FR | 21.4M | 74.0 s | 43.3 s | 1.71 |
| All | 59.4M | 85.8 s | 46.2 s | 1.86 |

NL, with 10 % of the price events, takes 72 s with one worker against 86 s for
all exchanges, and 38.5 s against 46.2 s with eight. The window and EMA stage
for all 59.4M events therefore costs roughly 14 s and 8 s.

### 3.3 CPU

At 8 workers the process uses 7.6–7.8 cores on average (764–778 % CPU) out of
12 hardware threads. Only 8 of the 12 hardware threads belong to performance
cores; the other 4 run on slower efficiency cores.

### 3.4 One reader per file

`FileSource` with a text-line format does not split files: each file is read
and parsed by exactly one worker. The week has only five large files, so at
most five readers do real work, and the run cannot finish before the largest
file (5.5 GB) has been read by a single thread.

**Volume scaling at 8 workers** (`resource_bench.sh`,
`logs/resource_times.csv`; median of 3):

| Input | Files | Lines | Time | Lines/s | CPU |
|---|---:|---:|---:|---:|---:|
| 1 day | 1 | 64.6M | 31.1 s | 2.1M | 216 % |
| 2 days | 2 | 120.1M | 33.1 s | 3.6M | 365 % |
| 4 days | 4 | 235.1M | 45.0 s | 5.2M | 654 % |
| 7 days | 7 (5 large weekday files, 2 near-empty weekend files) | 289.1M | 53.2 s | 5.4M | 778 % |

Peak memory (RSS) 2.2–2.8 GB in all runs. One day (exactly the largest file)
takes 31 s and uses only 2.2 cores; seven days take only 1.7× longer for 4.5×
more data. Throughput grows with the number of files, not with the number of
workers.

### 3.5 Limiting the CPUs

The 8-worker job pinned to 1–12 logical CPUs with `taskset`
(`scripts/cpu_limit_bench.sh`, `logs/cpu_limit_times.csv`; 7 days, median of
2 runs):

| CPUs | Time | CPU time | Busy CPUs | Speedup |
|---:|---:|---:|---:|---:|
| 1 | 161.0 s | 159.7 s | 0.99 | 1.00 |
| 2 | 86.7 s | 168.5 s | 1.94 | 1.86 |
| 4 | 59.4 s | 221.1 s | 3.72 | 2.71 |
| 6 | 51.0 s | 268.3 s | 5.26 | 3.16 |
| 12 | 43.7 s | 349.6 s | 8.01 | 3.68 |

From 6 to 12 CPUs, the runtime drops 14 % for 30 % more CPU time, and even with
12 CPUs available only 8.0 are busy on average. Each added CPU therefore
contributes less. We did not test why. Plausible causes are coordination and
garbage-collection overhead, hyperthread siblings sharing a core, and slower
efficiency cores. WSL2 hides which host cores are performance cores, so our CPU
sets (one thread per virtual core first) cannot separate these effects.

### 3.6 Splitting the input

Splitting the input did not remove the plateau (ablation table in the report).
With each day cut into 8 files (56 files), the process used almost 10 cores
(986 % CPU) but finished in the same time as the baseline. The splittable
legacy source, which divides files into byte ranges, was 1.6× slower. Once the
reading is spread out, the job appears to be limited by the shared hardware:
parsing is mostly byte scanning and string allocation, and the readers compete
with the shuffle, the garbage collector and WSL2's file-system layer for the
same 4 performance cores and memory bandwidth. We did not measure this last
step directly. The legacy source is slower even though it splits files, and it
keeps the CPU less busy; we did not investigate why, because this API is
deprecated and removed in Flink 2.0.

## 4. Latency

### 4.1 Method

`--latency-log <prefix>`: the parser stamps each event with `System.nanoTime()`
when it enters the pipeline; for every result, the window function writes
`symbol,window_end_ms,latency_us,last_event_ms` to `<prefix>.<subtask>`, where
the latency runs from the stamp of the window's last event to the moment the
result is emitted. All tasks run in one JVM, so the stamps are comparable.

`--replay-speed S` (`Pacer`): right after parsing, each reader holds an event
until `start + V / S` of wall-clock time has passed, where V is the event time
elapsed so far (the largest event time seen, forward jumps over 10 minutes
counted as 10 minutes). Backward events do not move the clock. The arrival
stamp is renewed when the event is released, so the deliberate wait is not
counted as latency. Off by default (no extra operator).

**Excess** (`ema.PaceDelay`): measured latency − built-in wait. The built-in
wait is the virtual delay divided by S, with the virtual delay =
min(V(window end + tolerance), V_final) − V(last event time), V being the
reader's virtual clock rebuilt from the day file exactly as `Pacer` does. A
second variant ends the virtual delay at the event that actually advances the
watermark past window end + tolerance (the first event with event time ≥
end + tol, or the end of input); this removes the paced wait for that next
event, which matters at the end of the day when trading thins out.

### 4.2 Paced sweep, 11 November

11 November has no placeholder rows: 11,835,403 price events over 15.584 h of
event time, no forward gap over 10 minutes (`logs/pace_scan_11nov.txt`).
Tolerance 0.1 h, parallelism 8, probe on, 368,727 results per run, identical at
every speed. One day file is read by one reader, so the sweep measures the
pipeline behind one reader. Scripts: `scripts/pace_sweep.sh`; logs:
`logs/pace_sweep*.log`, `logs/pace_sweep*_times.csv`, `logs/pace_<run>.txt`,
`logs/pace_unpaced_recheck.txt`.

The runs at 3000 and 6000 and the unpaced re-check were made later, when the
machine was about 2.4× slower (unpaced 53.8 s instead of 22.7 s).

| Speed S | Machine state | Run time (s) | Price events/s |
|---|---|---|---|
| 60 | first | 938.7 | 12,608 |
| 300 | first | 190.7 | 62,074 |
| 1500 | first | 41.2 | 287,302 |
| unpaced | first | 22.7 | 520,926 |
| 3000 | later | 56.9 | 208,040 |
| 6000 | later | 58.6 | 201,958 |
| unpaced (re-check) | later | 53.8 | 219,974 |

**All 368,727 windows** (ms: median / p95 / p99 / max)

| Speed S | Measured | Excess (to end + tol) | Excess (to triggering event) |
|---|---|---|---|
| 60 | 6,397 / 8,972 / 11,051 / 14,987 | 145 / 257 / 286 / 4,055 | 145 / 257 / 286 / 709 |
| 300 | 1,381 / 1,886 / 2,347 / 3,003 | 120 / 219 / 239 / 816 | 120 / 219 / 239 / 259 |
| 1500 | 406 / 514 / 576 / 794 | 147 / 211 / 272 / 367 | 147 / 211 / 272 / 367 |
| unpaced | 313 / 466 / 614 / 810 | 313 / 466 / 614 / 810 | 313 / 466 / 614 / 810 |
| 3000 | 625 / 912 / 1,085 / 1,574 | 498 / 780 / 904 / 1,354 | 498 / 780 / 904 / 1,354 |
| 6000 | 652 / 1,020 / 1,384 / 1,974 | 588 / 951 / 1,320 / 1,864 | 588 / 951 / 1,320 / 1,864 |
| unpaced (re-check) | 593 / 921 / 1,459 / 2,143 | 593 / 921 / 1,459 / 2,143 | 593 / 921 / 1,459 / 2,143 |

**Windows ending in the last 30 min before the day's last event (686)** (ms: median / p95 / p99 / max)

| Speed S | Measured | Excess (to end + tol) | Excess (to triggering event) |
|---|---|---|---|
| 60 | 6,292 / 14,987 / 14,987 / 14,987 | 31 / 4,054 / 4,054 / 4,055 | 25 / 42 / 43 / 43 |
| 300 | 1,487 / 3,002 / 3,003 / 3,003 | 145 / 816 / 816 / 816 | 143 / 236 / 236 / 236 |
| 1500 | 366 / 602 / 602 / 602 | 116 / 165 / 165 / 165 | 115 / 116 / 116 / 116 |
| unpaced | 43 / 48 / 49 / 49 | 43 / 48 / 49 / 49 | 43 / 48 / 49 / 49 |
| 3000 | 23 / 31 / 31 / 32 | -107 / -94 / 16 / 24 | -107 / -94 / 16 / 24 |
| 6000 | 15 / 23 / 24 / 25 | -50 / -39 / 11 / 16 | -50 / -39 / 11 / 16 |
| unpaced (re-check) | 38 / 44 / 45 / 46 | 38 / 44 / 45 / 46 | 38 / 44 / 45 / 46 |

**All other windows (368,041)** (ms: median / p95 / p99 / max)

| Speed S | Measured | Excess (to end + tol) | Excess (to triggering event) |
|---|---|---|---|
| 60 | 6,397 / 8,938 / 11,050 / 11,288 | 145 / 257 / 286 / 709 | 145 / 257 / 286 / 709 |
| 300 | 1,380 / 1,879 / 2,346 / 2,439 | 120 / 219 / 239 / 259 | 120 / 219 / 239 / 259 |
| 1500 | 406 / 514 / 575 / 794 | 147 / 211 / 272 / 367 | 147 / 211 / 272 / 367 |
| unpaced | 313 / 466 / 614 / 810 | 313 / 466 / 614 / 810 | 313 / 466 / 614 / 810 |
| 3000 | 626 / 912 / 1,086 / 1,574 | 499 / 780 / 904 / 1,354 | 499 / 780 / 904 / 1,354 |
| 6000 | 652 / 1,020 / 1,384 / 1,974 | 588 / 951 / 1,320 / 1,864 | 588 / 951 / 1,320 / 1,864 |
| unpaced (re-check) | 593 / 921 / 1,463 / 2,143 | 593 / 921 / 1,463 / 2,143 | 593 / 921 / 1,463 / 2,143 |

**Reading the tables.** A window can fire only when event time has passed its
end plus the tolerance, so its last event waits between 6 and 11 minutes of
event time (0.1 h plus up to one 5-minute window), which at S = 60 is 6 to 11 s
of wall-clock time; the pipeline itself adds only the excess. Up to S = 1500
the excess stays at a median of 120–150 ms (consistent with Flink's 200 ms
periodic watermark interval), and every paced run ends about 3.7 s after its
built-in wait (935.0 s, 187.0 s and 37.4 s), so the pipeline keeps up. At 3000
and 6000 the runs take as long as an unpaced run made at the same time and the
excess grows: the pipeline saturates. Where it starts between 1500 and 3000 in
the faster machine state is not measured; from the faster unpaced run (22.7 s,
about 19 s of processing) we would expect it near S ≈ 3,000. Once saturated,
events also wait in the reader before they are stamped, so the excess then
understates the delay (the negative end-of-day excess at 3000 and 6000 shows
this). In the end-of-day windows at S = 60 and 300, the excess to end + tol
reaches about 244 s of event time divided by S (4,054 ms × 60, 816 ms × 300):
the wait for the next event after end + tol, which is still pacing.

### 4.3 Saturated replay (full week, unpaced)

`scripts/latency_bench.sh`, `logs/latency_bench.log`, `logs/latency_times.csv`
(18 November; pooled over 2 runs, 3,711,290 results per tolerance). The whole
week is read as fast as possible, so events queue in buffers and windows wait
for the watermark to sweep through the backlog; these numbers reflect the length
of the run, not the latency of the pipeline.

| Tolerance | Median | p95 | p99 | Max |
|---|---:|---:|---:|---:|
| 0.1 h | 39.9 s | 98.9 s | 105.6 s | 108.4 s |
| 6 h | 46.6 s | 89.3 s | 94.8 s | 97.9 s |

### 4.4 Cost of the probe code (probe off)

`scripts/overhead_clean.sh`, `logs/overhead_clean_times.csv` (19 November,
idle machine, 3 alternating runs each, full week, 200 h, parallelism 8, no
`--latency-log`, no `--replay-speed`). The probe build carries the extra
timestamp field through the pipeline even when the probe is off.

| Build | Run 1 | Run 2 | Run 3 | Mean |
|---|---:|---:|---:|---:|
| main before the probe (`206ffa1`) | 48.1 s | 51.5 s | 50.4 s | 50.0 s |
| with probe and pacing code, probe off | 52.7 s | 52.9 s | 52.3 s | 52.7 s (+5.3 %) |

The difference (2.7 s) is smaller than the spread of the main runs (3.5 s), but
all three probe runs were slower than all three main runs. The outputs are
identical. The throughput numbers in the report were measured before this code
was added. An earlier 2-run comparison on 18 November (`logs/latency_times.csv`)
varied by 16 % between repetitions and is not used.
