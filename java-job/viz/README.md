# EMA crossover explorer (optional visualization)

Static web page, no server and no dependencies (vanilla JS). Works by double-clicking `index.html`
(also from `file://`), in light and dark theme.

## Generate the data (from the repo root, after `cd java-job && mvn -q package -DskipTests && cd ..`)

1. Run the job (see main README) so you have a results folder, e.g. `output/w2_p8/results`.
2. Page data from the results:

       java -cp java-job/target/ema-crossover-1.0.jar ema.VizData output/w2_p8/results java-job/viz/data.js --top 150

   | Option | Default | Meaning |
   |---|---|---|
   | `--top N` | 60 | number of symbols with a full EMA series (drawn chart) |
   | `--min-windows N` | 300 | only symbols with at least N result windows are chosen by default |
   | `--per-exchange K` | 15 | at least K of the chosen symbols come from each exchange (ETR, FR, NL) |
   | `--min-move F` | 0.005 | skip "flat" symbols whose EMA38 moves less than F (0.5 %) of its mean over the week |
   | `--symbols A.FR,B.NL` | – | always include these symbols too (even if flat) |

   The drawn set is: the top-K crossers per exchange among non-flat symbols with at least N windows, filled up
   to `--top` with the next best crossers (real crossings). The chosen symbols are printed per exchange,
   with how many symbols were skipped as flat, then the totals line (always over all symbols).
   With `--top 150` the file is about 2.6 MB.
3. Performance data from the benchmark logs (optional):

       java -cp java-job/target/ema-crossover-1.0.jar ema.PerfData

   Reads `logs/bench_times.csv` (dataset,parallelism,rep,seconds) and `logs/ablation_times.csv`
   (config,rep,seconds,cpu_percent,max_rss_kb,check) and writes `java-job/viz/perf.js` with medians.
   Options: `--bench`, `--ablation`, `--out`, and `--exclude config:rep,...` (default `baseline:2`, a
   machine stall of 583.7 s that was re-run as rep 4). Without `perf.js` the page simply hides the
   Performance section.
4. Open `java-job/viz/index.html` in a browser.

## Features

- **Literal rule / Strict** toggle: literal = spec equations (24,836 signals, equal-start drawn hollow);
  strict = `--strict-crossover` (19,989 signals, equal-start hidden). Applies to KPIs, chart markers,
  signal feed, prev/next, list and table counts. The choice is remembered.
- Symbol list with search, sort, exchange filter, exchange tag, "flat" tag and BUY/SELL counts. The
  chart opens on the first drawn symbol of the list as filtered and sorted.
- EMA38/EMA100 chart with shaded band and markers, spread chart, shared crosshair and tooltip.
  Mouse wheel zooms around the cursor, drag zooms into a range, **Show all** resets. Day filter,
  previous/next signal, replay with live signal feed.
- Day × hour heat map of real crossings (hours 07–19 plus any hour with data), with colour legend.
- Per-exchange literal-vs-strict counts, most active symbols.
- Performance: runtime vs parallelism (4 and 7 days, with ideal speedup) and ablation bars.

All times are local exchange time (CET in November) of the window end.

**Equal-start signals:** a signal whose previous window had exactly equal EMAs (always on a symbol's
second window, since both EMAs start from the first price, and again after flat stretches). The literal
rule fires there although nothing crossed; `--strict-crossover` removes them.

**Flat symbols:** some stocks barely move (e.g. A1EXUJ.ETR at about 0.795 EUR, EMA38 range 0.004 % in
the week); their crossings are tick-level noise and their charts are unreadable. They still count in all
totals but are not drawn by default (`--min-move`), and the list tags them "flat".
