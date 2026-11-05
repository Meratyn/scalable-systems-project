package ema;

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

    public static class ParseFn implements FlatMapFunction<String, Tuple4<String, String, Double, Long>> {
        private final boolean fallbackToSystemDate;
        private final boolean reference;

        public ParseFn(boolean fallbackToSystemDate) {
            this(fallbackToSystemDate, false);
        }

        /** reference=true uses the original split-based parser (for the ablation). */
        public ParseFn(boolean fallbackToSystemDate, boolean reference) {
            this.fallbackToSystemDate = fallbackToSystemDate;
            this.reference = reference;
        }

        @Override
        public void flatMap(String line, Collector<Tuple4<String, String, Double, Long>> out) {
            Tuple4<String, String, Double, Long> e = reference
                    ? Parser.parseReference(line, fallbackToSystemDate)
                    : parse(line, fallbackToSystemDate);
            if (e != null) {
                out.collect(e);
            }
        }
    }

    /** Keeps only events of one exchange (the symbol suffix after the last dot, e.g. FR, NL, ETR). */
    public static class ExchangeFilter implements FilterFunction<Tuple4<String, String, Double, Long>> {
        private final String exchange;

        public ExchangeFilter(String exchange) {
            this.exchange = exchange;
        }

        @Override
        public boolean filter(Tuple4<String, String, Double, Long> e) {
            return exchange.equals(e.f1);
        }
    }

    /**
     * Incremental reduce: keep only the event with the latest trading timestamp,
     * so each (symbol, window) holds ONE event instead of all of them. On equal
     * timestamps the later arrival wins.
     */
    public static class PickLatest implements ReduceFunction<Tuple4<String, String, Double, Long>> {
        @Override
        public Tuple4<String, String, Double, Long> reduce(
                Tuple4<String, String, Double, Long> a, Tuple4<String, String, Double, Long> b) {
            return b.f3 >= a.f3 ? b : a;
        }
    }

    // ---------- Step B: stateful EMA + crossover per symbol per window ----------

    /**
     * Runs once per (symbol, window) when the window closes.
     *  - Close = the price event with the LATEST trading timestamp in the window.
     *  - EMA is seeded with the first real price of a symbol (professor confirmed).
     *  - Windows with no price event for a symbol are skipped (stated assumption).
     *  - strict=false: spec equations literally (previous EMAs may be equal).
     *    strict=true: a crossover needs the previous EMAs to differ.
     */
    public static class EmaCrossoverFunction extends ProcessWindowFunction<
            Tuple4<String, String, Double, Long>,
            Tuple5<String, Long, Double, Double, String>,
            String, TimeWindow> {

        static final double ALPHA38 = 2.0 / (1 + 38);
        static final double ALPHA100 = 2.0 / (1 + 100);

        private final boolean strict;
        private transient ValueState<Double> ema38State;
        private transient ValueState<Double> ema100State;

        public EmaCrossoverFunction(boolean strict) {
            this.strict = strict;
        }

        @Override
        public void open(Configuration parameters) {
            ema38State = getRuntimeContext().getState(new ValueStateDescriptor<>("ema38", Types.DOUBLE));
            ema100State = getRuntimeContext().getState(new ValueStateDescriptor<>("ema100", Types.DOUBLE));
        }

        @Override
        public void process(String key, Context context,
                            Iterable<Tuple4<String, String, Double, Long>> elements,
                            Collector<Tuple5<String, Long, Double, Double, String>> out) throws Exception {
            Tuple4<String, String, Double, Long> close = null;
            for (Tuple4<String, String, Double, Long> e : elements) {
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
        String sourceType = "file";
        String parser = "fast";
        String exchange = "ALL";
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
                case "--source": sourceType = argv[++i]; break;
                case "--parser": parser = argv[++i]; break;
                case "--exchange": exchange = argv[++i].toUpperCase(); break;
                default:
                    System.err.println("Unknown option: " + argv[i]);
                    System.err.println("Options: --input --output --parallelism --tolerance-hours "
                            + "--strict-crossover --late-output --stage --object-reuse --require-trading-date --source file|legacy --parser fast|reference --exchange FR|NL|ETR|ALL");
                    System.exit(2);
            }
        }
        System.out.println("input=" + input + " output=" + output + " parallelism=" + parallelism
                + " tolerance_h=" + toleranceHours + " strict=" + strict + " stage=" + stage
                + " objectReuse=" + objectReuse
                + " requireTradingDate=" + requireTradingDate
                + " source=" + sourceType + " parser=" + parser + " exchange=" + exchange);

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

        SingleOutputStreamOperator<Tuple4<String, String, Double, Long>> events =
                rawLines.flatMap(new ParseFn(!requireTradingDate, parser.equals("reference")))
                        .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.DOUBLE, Types.LONG));
        if (!exchange.equals("ALL")) {
            // Per-exchange experiments: all files are still read and parsed; only the events
            // of one exchange go on to the keyed window stage. Default ALL adds no operator.
            events = events.filter(new ExchangeFilter(exchange));
        }

        if (stage.equals("symbolstats")) {
            // Price events per symbol -> <output>/symbolstats (symbol,count). Used for
            // long-tail and key-skew analysis (see SkewReport).
            events.map(new MapFunction<Tuple4<String, String, Double, Long>, Tuple2<String, Long>>() {
                        @Override
                        public Tuple2<String, Long> map(Tuple4<String, String, Double, Long> e) {
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
            events.addSink(new DiscardingSink<Tuple4<String, String, Double, Long>>());
            env.execute("stage-parse");
            return;
        }

        // Tolerance larger than the dataset (default 200h): no event is ever late,
        // so results do not depend on file read order or parallelism.
        DataStream<Tuple4<String, String, Double, Long>> watermarked = events.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple4<String, String, Double, Long>>forBoundedOutOfOrderness(
                                Duration.ofSeconds((long) (toleranceHours * 3600)))
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<Tuple4<String, String, Double, Long>>)
                                        (e, ts) -> e.f3));

        WindowedStream<Tuple4<String, String, Double, Long>, String, TimeWindow> windowed = watermarked
                .keyBy(new KeySelector<Tuple4<String, String, Double, Long>, String>() {
                    @Override
                    public String getKey(Tuple4<String, String, Double, Long> e) {
                        return e.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(5)));

        OutputTag<Tuple4<String, String, Double, Long>> lateTag = null;
        if (lateOutput) {
            lateTag = new OutputTag<Tuple4<String, String, Double, Long>>("late-events",
                    Types.TUPLE(Types.STRING, Types.STRING, Types.DOUBLE, Types.LONG)) { };
            windowed = windowed.sideOutputLateData(lateTag);
        }

        SingleOutputStreamOperator<Tuple5<String, Long, Double, Double, String>> result =
                windowed.reduce(new PickLatest(), new EmaCrossoverFunction(strict));

        result.map(new MapFunction<Tuple5<String, Long, Double, Double, String>, String>() {
            @Override
            public String map(Tuple5<String, Long, Double, Double, String> r) {
                return r.f0 + "," + r.f1 + "," + r.f2 + "," + r.f3 + "," + r.f4;
            }
        }).sinkTo(csvSink(output + "/results"));

        if (lateTag != null) {
            result.getSideOutput(lateTag).map(
                    new MapFunction<Tuple4<String, String, Double, Long>, String>() {
                        @Override
                        public String map(Tuple4<String, String, Double, Long> e) {
                            return e.f0 + "," + e.f3 + "," + e.f2;
                        }
                    }).sinkTo(csvSink(output + "/late"));
        }

        env.execute("ema-crossover");
    }
}
