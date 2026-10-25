package ema;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
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

    /** Event = (symbol, exchange, last_price, ts_millis). Null when the line is not a price event. */
    public static Tuple4<String, String, Double, Long> parse(String line) {
        if (line.startsWith("#") || line.startsWith("ID,")) {
            return null;
        }
        String[] parts = line.trim().split(",", -1);
        if (parts.length < 27) {
            return null;
        }
        String symbol = parts[0];
        String last = parts[21];
        String tradingTime = parts[23];
        String tradingDate = parts[26];
        if (last.isEmpty() || tradingTime.isEmpty() || tradingDate.isEmpty()) {
            return null;
        }
        try {
            double price = Double.parseDouble(last);
            if (price == 0.0) {
                return null;
            }
            long ts = parseMillis(tradingDate, tradingTime);
            int dot = symbol.lastIndexOf('.');
            String exchange = dot >= 0 ? symbol.substring(dot + 1) : symbol;
            return Tuple4.of(symbol, exchange, price, ts);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * DD-MM-YYYY + HH:MM:SS[.ffff] -> epoch millis. The CEST wall-clock time is
     * used as-is (treated as UTC, no timezone shift), so 5-minute windows line
     * up with local midnight as the spec requires.
     */
    static long parseMillis(String date, String time) {
        String[] d = date.split("-");
        int day = Integer.parseInt(d[0]);
        int month = Integer.parseInt(d[1]);
        int year = Integer.parseInt(d[2]);
        String hms = time;
        String frac = "";
        int dot = time.indexOf('.');
        if (dot >= 0) {
            hms = time.substring(0, dot);
            frac = time.substring(dot + 1);
        }
        String[] t = hms.split(":");
        int hh = Integer.parseInt(t[0]);
        int mm = Integer.parseInt(t[1]);
        int ss = Integer.parseInt(t[2]);
        long millis = 0;
        if (!frac.isEmpty()) {
            String f = (frac + "000").substring(0, 3);
            millis = Long.parseLong(f);
        }
        LocalDateTime dt = LocalDateTime.of(year, month, day, hh, mm, ss);
        return dt.toEpochSecond(ZoneOffset.UTC) * 1000L + millis;
    }

    public static class ParseFn implements FlatMapFunction<String, Tuple4<String, String, Double, Long>> {
        @Override
        public void flatMap(String line, Collector<Tuple4<String, String, Double, Long>> out) {
            Tuple4<String, String, Double, Long> e = parse(line);
            if (e != null) {
                out.collect(e);
            }
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
        for (int i = 0; i < argv.length; i++) {
            switch (argv[i]) {
                case "--input": input = argv[++i]; break;
                case "--output": output = argv[++i]; break;
                case "--parallelism": parallelism = Integer.parseInt(argv[++i]); break;
                case "--tolerance-hours": toleranceHours = Double.parseDouble(argv[++i]); break;
                case "--strict-crossover": strict = true; break;
                case "--late-output": lateOutput = true; break;
                default:
                    System.err.println("Unknown option: " + argv[i]);
                    System.err.println("Options: --input --output --parallelism --tolerance-hours "
                            + "--strict-crossover --late-output");
                    System.exit(2);
            }
        }
        System.out.println("input=" + input + " output=" + output + " parallelism=" + parallelism
                + " tolerance_h=" + toleranceHours + " strict=" + strict);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(input)).build();
        DataStream<String> rawLines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "csv-source");

        SingleOutputStreamOperator<Tuple4<String, String, Double, Long>> events =
                rawLines.flatMap(new ParseFn())
                        .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.DOUBLE, Types.LONG));

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
