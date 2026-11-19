package ema;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * CS-E4780 course project: EMA38/EMA100 per symbol over 5-minute tumbling
 * event-time windows (Query 1) and BUY/SELL crossover advisories (Query 2).
 *
 * Java port of the PyFlink job.py. Same logic, same CSV output:
 *   symbol,window_end_ms,ema38,ema100,advisory
 *
 * Usage (all options optional):
 *   java -jar target/ema-crossover-1.0.jar --input data/small --output output/java_p1 --parallelism 1
 */
public class Job {

    // ---------- Step A: parse a raw CSV line into a clean price event ----------

    /** Event = (symbol, exchange, last_price, ts_millis). Null when the line is not a price event. See Parser. */
    public static Tuple4<String, String, Double, Long> parse(String line, boolean fallbackToSystemDate) {
        return Parser.parse(line, fallbackToSystemDate);
    }

    public static class ParseFn implements FlatMapFunction<String, Tuple5<String, String, Double, Long, Long>> {
        private final boolean fallbackToSystemDate;
        private final boolean reference;
        private final boolean dropPlaceholderTime;
        private final boolean stampArrival;

        public ParseFn(boolean fallbackToSystemDate) {
            this(fallbackToSystemDate, false);
        }

        /** reference=true uses the original split-based parser (for the ablation). */
        public ParseFn(boolean fallbackToSystemDate, boolean reference) {
            this(fallbackToSystemDate, reference, false);
        }

        /** dropPlaceholderTime=true drops rows with an empty Trading date and Trading time 01:00:00.000. */
        public ParseFn(boolean fallbackToSystemDate, boolean reference, boolean dropPlaceholderTime) {
            this(fallbackToSystemDate, reference, dropPlaceholderTime, false);
        }

        /**
         * stampArrival=true (--latency-log): f4 = System.nanoTime() when the event is emitted by the
         * parser; otherwise f4 = 0. Local mode runs all tasks in one JVM, so stamps are comparable.
         */
        public ParseFn(boolean fallbackToSystemDate, boolean reference, boolean dropPlaceholderTime,
                       boolean stampArrival) {
            this.fallbackToSystemDate = fallbackToSystemDate;
            this.reference = reference;
            this.dropPlaceholderTime = dropPlaceholderTime;
            this.stampArrival = stampArrival;
        }

        @Override
        public void flatMap(String line, Collector<Tuple5<String, String, Double, Long, Long>> out) {
            Tuple4<String, String, Double, Long> e = reference
                    ? Parser.parseReference(line, fallbackToSystemDate, dropPlaceholderTime)
                    : Parser.parse(line, fallbackToSystemDate, dropPlaceholderTime);
            if (e != null) {
                out.collect(Tuple5.of(e.f0, e.f1, e.f2, e.f3, stampArrival ? System.nanoTime() : 0L));
            }
        }
    }

    /** Keeps only events of one exchange (the symbol suffix after the last dot, e.g. FR, NL, ETR). */
    public static class ExchangeFilter implements FilterFunction<Tuple5<String, String, Double, Long, Long>> {
        private final String exchange;

        public ExchangeFilter(String exchange) {
            this.exchange = exchange;
        }

        @Override
        public boolean filter(Tuple5<String, String, Double, Long, Long> e) {
            return exchange.equals(e.f1);
        }
    }

    /**
     * Incremental reduce: keep only the event with the latest trading timestamp,
     * so each (symbol, window) holds ONE event instead of all of them.
     *
     * Tie rule: on equal timestamps the later event wins (b = the newer arrival). In file
     * mode (--source file) this means the later line of the same file: one reader per
     * file and FIFO channels keep the file's line order per key. Ties across files do not
     * occur in the data (0 of 92,233 tied windows, see TieReport / logs/tie_report.txt).
     * --source legacy (split files, several readers per file) is not covered by this rule.
     */
    public static class PickLatest implements ReduceFunction<Tuple5<String, String, Double, Long, Long>> {
        @Override
        public Tuple5<String, String, Double, Long, Long> reduce(
                Tuple5<String, String, Double, Long, Long> a, Tuple5<String, String, Double, Long, Long> b) {
            Tuple5<String, String, Double, Long, Long> w = b.f3 >= a.f3 ? b : a;
            // f4 = arrival stamp of the LAST event that entered the window (0 when not measured)
            w.f4 = Math.max(a.f4, b.f4);
            return w;
        }
    }

    // ---------- Step B: stateful EMA + crossover per symbol per window ----------

    /**
     * Runs once per (symbol, window) when the window closes.
     *  - Close = the price event with the LATEST trading timestamp in the window.
     *  - zeroInit=false (default, --ema-init first): both EMAs are seeded with the first
     *    price of a symbol, so its first window has no advisory (allowed by the
     *    instructor if the choice is explained; see the report, "EMA starting value").
     *    zeroInit=true (--ema-init zero): as in the spec, both previous EMAs start at 0; the
     *    first window then applies the formula and the crossover rule like any other window.
     *  - Windows with no price event for a symbol emit nothing; the EMAs continue from their
     *    last values (as discussed with the instructor, passing the state forward or dropping
     *    it are both acceptable).
     *  - strict=false: spec equations literally (previous EMAs may be equal).
     *    strict=true: a crossover needs the previous EMAs to differ.
     */
    public static class EmaCrossoverFunction extends ProcessWindowFunction<
            Tuple5<String, String, Double, Long, Long>,
            Tuple5<String, Long, Double, Double, String>,
            String, TimeWindow> {

        static final double ALPHA38 = 2.0 / (1 + 38);
        static final double ALPHA100 = 2.0 / (1 + 100);

        private final boolean strict;
        private final boolean zeroInit;
        private final String latencyLog; // null = off
        private transient BufferedWriter latencyOut;
        private transient ValueState<Double> ema38State;
        private transient ValueState<Double> ema100State;

        public EmaCrossoverFunction(boolean strict) {
            this(strict, false);
        }

        public EmaCrossoverFunction(boolean strict, boolean zeroInit) {
            this(strict, zeroInit, null);
        }

        /** latencyLog != null: each subtask i writes "symbol,window_end_ms,latency_us" to latencyLog.i */
        public EmaCrossoverFunction(boolean strict, boolean zeroInit, String latencyLog) {
            this.strict = strict;
            this.zeroInit = zeroInit;
            this.latencyLog = latencyLog;
        }

        @Override
        public void open(Configuration parameters) {
            ema38State = getRuntimeContext().getState(new ValueStateDescriptor<>("ema38", Types.DOUBLE));
            ema100State = getRuntimeContext().getState(new ValueStateDescriptor<>("ema100", Types.DOUBLE));
            if (latencyLog != null) {
                try {
                    latencyOut = new BufferedWriter(new FileWriter(
                            latencyLog + "." + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()), 1 << 20);
                } catch (IOException ex) {
                    throw new RuntimeException("cannot open latency log " + latencyLog, ex);
                }
            }
        }

        @Override
        public void close() throws Exception {
            if (latencyOut != null) {
                latencyOut.close();
            }
        }

        @Override
        public void process(String key, Context context,
                            Iterable<Tuple5<String, String, Double, Long, Long>> elements,
                            Collector<Tuple5<String, Long, Double, Double, String>> out) throws Exception {
            Tuple5<String, String, Double, Long, Long> close = null;
            for (Tuple5<String, String, Double, Long, Long> e : elements) {
                if (close == null || e.f3 >= close.f3) {
                    close = e;
                }
            }
            if (close == null) {
                return;
            }
            double price = close.f2;

            Double prev38 = ema38State.value();
            Double prev100 = ema100State.value();
            if (prev38 == null && zeroInit) {
                prev38 = 0.0; // spec: EMA_prev = 0 before the first window
                prev100 = 0.0;
            }

            String advisory = "";
            double ema38;
            double ema100;
            if (prev38 == null) {
                ema38 = price; // first window of this symbol: seed
                ema100 = price;
            } else {
                ema38 = price * ALPHA38 + prev38 * (1 - ALPHA38);
                ema100 = price * ALPHA100 + prev100 * (1 - ALPHA100);
                boolean wasBelow = strict ? prev38 < prev100 : prev38 <= prev100;
                boolean wasAbove = strict ? prev38 > prev100 : prev38 >= prev100;
                if (ema38 > ema100 && wasBelow) {
                    advisory = "BUY";
                } else if (ema38 < ema100 && wasAbove) {
                    advisory = "SELL";
                }
            }

            ema38State.update(ema38);
            ema100State.update(ema100);
            out.collect(Tuple5.of(key, context.window().getEnd(), ema38, ema100, advisory));
            if (latencyOut != null) {
                // processing-time delay: last event's parser emission -> this result handed downstream
                long us = (System.nanoTime() - close.f4) / 1000;
                latencyOut.write(key + "," + context.window().getEnd() + "," + us + "\n");
            }
        }
    }

    static FileSink<String> csvSink(String path) {
        return FileSink.forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(OutputFileConfig.builder().withPartSuffix(".csv").build())
                .build();
    }

    public static void main(String[] argv) throws Exception {
        String input = "data/raw_test/";
        String output = "output";
        int parallelism = 1;
        double toleranceHours = 200.0;
        boolean strict = false;
        boolean lateOutput = false;
        String stage = "full";
        boolean objectReuse = false;
        boolean requireTradingDate = false;
        boolean dropPlaceholderTime = false;
        String sourceType = "file";
        String parser = "fast";
        String exchange = "ALL";
        String emaInit = "first";
        String latencyLog = null;
        double replaySpeed = 0; // 0 = off (read as fast as possible)
        for (int i = 0; i < argv.length; i++) {
            switch (argv[i]) {
                case "--input": input = argv[++i]; break;
                case "--output": output = argv[++i]; break;
                case "--parallelism": parallelism = Integer.parseInt(argv[++i]); break;
                case "--tolerance-hours": toleranceHours = Double.parseDouble(argv[++i]); break;
                case "--strict-crossover": strict = true; break;
                case "--late-output": lateOutput = true; break;
                case "--stage": stage = argv[++i]; break;
                case "--object-reuse": objectReuse = true; break;
                case "--require-trading-date": requireTradingDate = true; break;
                case "--drop-placeholder-time": dropPlaceholderTime = true; break;
                case "--source": sourceType = argv[++i]; break;
                case "--parser": parser = argv[++i]; break;
                case "--exchange": exchange = argv[++i].toUpperCase(); break;
                case "--ema-init": emaInit = argv[++i].toLowerCase(); break;
                case "--latency-log": latencyLog = argv[++i]; break;
                case "--replay-speed": replaySpeed = Double.parseDouble(argv[++i]); break;
                default:
                    System.err.println("Unknown option: " + argv[i]);
                    System.err.println("Options: --input --output --parallelism --tolerance-hours "
                            + "--strict-crossover --late-output --stage --object-reuse --require-trading-date --drop-placeholder-time --source file|legacy --parser fast|reference --exchange FR|NL|ETR|ALL --ema-init first|zero --latency-log <file> --replay-speed S");
                    System.exit(2);
            }
        }
        if (!emaInit.equals("first") && !emaInit.equals("zero")) {
            System.err.println("--ema-init must be first or zero, got: " + emaInit);
            System.exit(2);
        }
        System.out.println("input=" + input + " output=" + output + " parallelism=" + parallelism
                + " tolerance_h=" + toleranceHours + " strict=" + strict + " stage=" + stage
                + " objectReuse=" + objectReuse
                + " requireTradingDate=" + requireTradingDate
                + " dropPlaceholderTime=" + dropPlaceholderTime
                + " source=" + sourceType + " parser=" + parser + " exchange=" + exchange
                + " emaInit=" + emaInit + " latencyLog=" + latencyLog
                + " replaySpeed=" + replaySpeed);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        if (stage.equals("symbolstats")) {
            // bounded input: emit one final count per symbol, not running totals
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }
        if (objectReuse) {
            // Skip the defensive record copies between chained operators. Safe here:
            // no function mutates its input records or the records it emits.
            env.getConfig().enableObjectReuse();
        }

        DataStream<String> rawLines;
        if (sourceType.equals("legacy")) {
            // Splittable source: big files are cut into byte ranges that different workers
            // read in parallel. Deprecated in Flink 1.20 (removed in 2.0) but it works here.
            rawLines = env.readTextFile(input);
        } else {
            // FileSource with TextLineInputFormat: simple, but one file is read by one worker.
            FileSource<String> source = FileSource
                    .forRecordStreamFormat(new TextLineInputFormat(), new Path(input)).build();
            rawLines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "csv-source");
        }

        // Diagnostic stages for experiments:
        //   read  = read the files only (results discarded)
        //   parse = read + parse (results discarded)
        //   symbolstats = price events per symbol, written to <output>/symbolstats
        //   full  = the real job (default)
        if (stage.equals("read")) {
            rawLines.addSink(new DiscardingSink<String>());
            env.execute("stage-read");
            return;
        }

        SingleOutputStreamOperator<Tuple5<String, String, Double, Long, Long>> events =
                rawLines.flatMap(new ParseFn(!requireTradingDate, parser.equals("reference"), dropPlaceholderTime,
                                latencyLog != null))
                        .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.DOUBLE, Types.LONG, Types.LONG));
        if (replaySpeed > 0) {
            // Paced replay: chained to the parser (same parallelism, forward), so one Pacer per
            // reader. Before the watermark assigner, so watermarks advance with the paced events.
            events = events.map(new Pacer(replaySpeed, latencyLog != null))
                    .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.DOUBLE, Types.LONG, Types.LONG));
        }
        if (!exchange.equals("ALL")) {
            // Per-exchange experiments: all files are still read and parsed; only the events
            // of one exchange go on to the keyed window stage. Default ALL adds no operator.
            events = events.filter(new ExchangeFilter(exchange));
        }

        if (stage.equals("symbolstats")) {
            // Price events per symbol -> <output>/symbolstats (symbol,count). Used for
            // long-tail and key-skew analysis (see SkewReport).
            events.map(new MapFunction<Tuple5<String, String, Double, Long, Long>, Tuple2<String, Long>>() {
                        @Override
                        public Tuple2<String, Long> map(Tuple5<String, String, Double, Long, Long> e) {
                            return Tuple2.of(e.f0, 1L);
                        }
                    })
                    .returns(Types.TUPLE(Types.STRING, Types.LONG))
                    .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                        @Override
                        public String getKey(Tuple2<String, Long> t) {
                            return t.f0;
                        }
                    })
                    .sum(1)
                    .map(new MapFunction<Tuple2<String, Long>, String>() {
                        @Override
                        public String map(Tuple2<String, Long> t) {
                            return t.f0 + "," + t.f1;
                        }
                    })
                    .sinkTo(csvSink(output + "/symbolstats"));
            env.execute("stage-symbolstats");
            return;
        }

        if (stage.equals("parse")) {
            events.addSink(new DiscardingSink<Tuple5<String, String, Double, Long, Long>>());
            env.execute("stage-parse");
            return;
        }

        // Tolerance larger than the dataset (default 200h): no event is ever late,
        // so results do not depend on file read order or parallelism.
        DataStream<Tuple5<String, String, Double, Long, Long>> watermarked = events.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple5<String, String, Double, Long, Long>>forBoundedOutOfOrderness(
                                Duration.ofSeconds((long) (toleranceHours * 3600)))
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<Tuple5<String, String, Double, Long, Long>>)
                                        (e, ts) -> e.f3));

        WindowedStream<Tuple5<String, String, Double, Long, Long>, String, TimeWindow> windowed = watermarked
                .keyBy(new KeySelector<Tuple5<String, String, Double, Long, Long>, String>() {
                    @Override
                    public String getKey(Tuple5<String, String, Double, Long, Long> e) {
                        return e.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(5)));

        OutputTag<Tuple5<String, String, Double, Long, Long>> lateTag = null;
        if (lateOutput) {
            lateTag = new OutputTag<Tuple5<String, String, Double, Long, Long>>("late-events",
                    Types.TUPLE(Types.STRING, Types.STRING, Types.DOUBLE, Types.LONG, Types.LONG)) { };
            windowed = windowed.sideOutputLateData(lateTag);
        }

        SingleOutputStreamOperator<Tuple5<String, Long, Double, Double, String>> result =
                windowed.reduce(new PickLatest(), new EmaCrossoverFunction(strict, emaInit.equals("zero"), latencyLog));

        result.map(new MapFunction<Tuple5<String, Long, Double, Double, String>, String>() {
            @Override
            public String map(Tuple5<String, Long, Double, Double, String> r) {
                return r.f0 + "," + r.f1 + "," + r.f2 + "," + r.f3 + "," + r.f4;
            }
        }).sinkTo(csvSink(output + "/results"));

        if (lateTag != null) {
            result.getSideOutput(lateTag).map(
                    new MapFunction<Tuple5<String, String, Double, Long, Long>, String>() {
                        @Override
                        public String map(Tuple5<String, String, Double, Long, Long> e) {
                            return e.f0 + "," + e.f3 + "," + e.f2;
                        }
                    }).sinkTo(csvSink(output + "/late"));
        }

        env.execute("ema-crossover");
    }
}
