package ema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end correctness test of the Flink job on a small synthetic CSV.
 *
 * The test builds the price events as Java objects, writes them in the DEBS CSV layout, runs
 * Job.main in-process (Flink local mini-cluster), and compares the result CSV with an independent
 * straightforward implementation of the spec computed from the same objects:
 *   window close = event with the latest trading timestamp in the 5-minute window,
 *   alpha = 2/(1+N), EMA = price*alpha + prev*(1-alpha), first window seeds both EMAs,
 *   BUY if ema38 > ema100 and prev38 <= prev100 (strict: prev38 < prev100), SELL symmetric.
 *
 * Symbols:
 *   OSC.FR   price steps 100 -> 90 -> 110 -> 90 -> 110 -> 90: 2 real BUY, 2 real SELL, plus one
 *            equal-start SELL in window 2 (literal rule only). Two events per window, the later one
 *            written first (tests "latest timestamp", not "last arrival"), and one out-of-order event
 *            for window 10 written at the very end of the file (16 h of event time later).
 *   FLAT.NL  constant price: EMAs stay exactly equal, no signal under either rule.
 *   ZERO.FR  odd windows contain only Last=0 / empty-Last rows: those windows must not exist.
 *   IDX.ETR  empty Trading date: event time must use the system date (a different day).
 *   TIE.NL   equal trading timestamps with different prices in one window: the later line of the
 *            file must win (documented tie rule), with an earlier-timestamp event written in between.
 * Plus comment/header lines and rows without Trading time, which must be ignored.
 */
class JobCorrectnessTest {

    static final long WINDOW = 300_000L;
    static final double A38 = 2.0 / (1 + 38);
    static final double A100 = 2.0 / (1 + 100);
    static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("dd-MM-yyyy");
    static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    static final LocalDateTime DAY1 = LocalDateTime.of(2021, 11, 8, 8, 0);
    static final LocalDateTime DAY2 = LocalDateTime.of(2021, 11, 9, 8, 0);

    /** A price event as the test designs it (price kept as the exact string written to the CSV). */
    static final class Ev {
        final String sym;
        final String price;
        final long ts;

        Ev(String sym, String price, long ts) {
            this.sym = sym;
            this.price = price;
            this.ts = ts;
        }
    }

    /** One result row: symbol, window end, EMA38, EMA100, advisory. */
    static final class Res {
        final double e38;
        final double e100;
        final String adv;

        Res(double e38, double e100, String adv) {
            this.e38 = e38;
            this.e100 = e100;
            this.adv = adv;
        }
    }

    @TempDir
    static Path tmp;

    static final List<Ev> events = new ArrayList<>();
    static long outOfOrderWindowEnd;
    static Map<String, Res> literalP1;
    static Map<String, Res> literalP2;
    static Map<String, Res> strictP1;
    static Path lateDir;

    // ---------------------------------------------------------------- test data

    static long millis(LocalDateTime t) {
        return t.toEpochSecond(ZoneOffset.UTC) * 1000L;
    }

    static String price(double p) {
        return String.format(Locale.ROOT, "%.6f", p);
    }

    /** A CSV row in the DEBS layout (39 columns; only the ones the parser reads are filled). */
    static String row(String sym, String sysDate, String last, String tradingTime, String tradingDate) {
        String[] f = new String[39];
        Arrays.fill(f, "");
        f[0] = sym;
        f[1] = "E";
        f[2] = sysDate;
        f[3] = "18:00:00.000";
        f[21] = last;
        f[23] = tradingTime;
        f[26] = tradingDate;
        return String.join(",", f);
    }

    static String priceRow(Ev e, boolean withTradingDate, String sysDate) {
        LocalDateTime t = LocalDateTime.ofEpochSecond(e.ts / 1000, (int) (e.ts % 1000) * 1_000_000, ZoneOffset.UTC);
        return row(e.sym, sysDate, e.price, t.format(TIME), withTradingDate ? t.format(DATE) : "");
    }

    static final int TIE_WINDOWS = 6;

    /** Price of the last line at the latest timestamp of TIE.NL window w. */
    static double tieWinner(int w) {
        return 30 + 2 * w;
    }

    static double oscPrice(int w) {
        if (w == 0) {
            return 100;
        }
        if (w <= 30) {
            return 90;
        }
        if (w <= 80) {
            return 110;
        }
        if (w <= 130) {
            return 90;
        }
        if (w <= 180) {
            return 110;
        }
        return 90;
    }

    static Path writeInput() throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("# synthetic test file");
        lines.add("ID,SecType,Date,Time,...");
        String d1 = DAY1.format(DATE);
        // OSC.FR: 200 windows, two events each; the later event is written first.
        for (int w = 0; w < 200; w++) {
            long start = millis(DAY1) + w * WINDOW;
            Ev later = new Ev("OSC.FR", price(oscPrice(w)), start + 240_000);
            Ev earlier = new Ev("OSC.FR", price(oscPrice(w) + 0.5), start + 60_000);
            events.add(later);
            events.add(earlier);
            lines.add(priceRow(later, true, d1));
            lines.add(priceRow(earlier, true, d1));
            if (w % 50 == 0) {
                lines.add(row("OSC.FR", d1, "", "", "")); // not a price event: no Trading time
            }
        }
        // FLAT.NL: constant price.
        for (int w = 0; w < 40; w++) {
            Ev e = new Ev("FLAT.NL", price(50), millis(DAY1) + w * WINDOW + 100_000);
            events.add(e);
            lines.add(priceRow(e, true, d1));
        }
        // ZERO.FR: even windows have a real price; odd windows only Last=0 or empty Last.
        for (int w = 0; w < 20; w++) {
            long ts = millis(DAY1) + w * WINDOW + 30_000;
            LocalDateTime t = LocalDateTime.ofEpochSecond(ts / 1000, 0, ZoneOffset.UTC);
            if (w % 2 == 0) {
                Ev e = new Ev("ZERO.FR", price(10 + w * 0.1), ts);
                events.add(e);
                lines.add(priceRow(e, true, d1));
            } else {
                lines.add(row("ZERO.FR", d1, "0.000000", t.format(TIME), t.format(DATE)));
                lines.add(row("ZERO.FR", d1, "", t.format(TIME), t.format(DATE)));
            }
        }
        // IDX.ETR: empty Trading date -> the system date (DAY2) must be used.
        for (int w = 0; w < 10; w++) {
            Ev e = new Ev("IDX.ETR", price(1000 + w), millis(DAY2) + w * WINDOW + 45_000);
            events.add(e);
            lines.add(priceRow(e, false, DAY2.format(DATE)));
        }
        // TIE.NL: per window, events at the same latest timestamp with different prices; the price of
        // the LAST such line must be the window price. An earlier-timestamp event sits in between.
        for (int w = 0; w < TIE_WINDOWS; w++) {
            long start = millis(DAY1) + w * WINDOW;
            List<Ev> evs = new ArrayList<>();
            evs.add(new Ev("TIE.NL", price(20 + w), start + 200_000));
            evs.add(new Ev("TIE.NL", price(99), start + 60_000));
            if (w % 2 == 1) {
                evs.add(new Ev("TIE.NL", price(40 + w), start + 200_000)); // three-way tie
            }
            evs.add(new Ev("TIE.NL", price(tieWinner(w)), start + 200_000));
            for (Ev e : evs) {
                events.add(e);
                lines.add(priceRow(e, true, d1));
            }
        }
        // Out-of-order event for OSC.FR window 10, written last: latest timestamp in its window.
        Ev late = new Ev("OSC.FR", price(95), millis(DAY1) + 10 * WINDOW + 290_000);
        events.add(late);
        lines.add(priceRow(late, true, d1));
        outOfOrderWindowEnd = millis(DAY1) + 11 * WINDOW;

        Path dir = tmp.resolve("input");
        Files.createDirectories(dir);
        Files.write(dir.resolve("test-day.csv"), lines, StandardCharsets.UTF_8);
        return dir;
    }

    // ---------------------------------------------------------------- reference implementation

    /** Independent, straightforward implementation of the spec, from the designed events. */
    static Map<String, Res> expected(boolean strict) {
        // symbol -> window end -> close event (latest timestamp)
        Map<String, TreeMap<Long, Ev>> close = new TreeMap<>();
        for (Ev e : events) {
            long end = Math.floorDiv(e.ts, WINDOW) * WINDOW + WINDOW;
            TreeMap<Long, Ev> m = close.computeIfAbsent(e.sym, k -> new TreeMap<>());
            Ev c = m.get(end);
            if (c == null || e.ts >= c.ts) {
                m.put(end, e);
            }
        }
        Map<String, Res> out = new HashMap<>();
        for (Map.Entry<String, TreeMap<Long, Ev>> s : close.entrySet()) {
            Double p38 = null;
            Double p100 = null;
            for (Map.Entry<Long, Ev> w : s.getValue().entrySet()) {
                double price = Double.parseDouble(w.getValue().price);
                double e38;
                double e100;
                String adv = "";
                if (p38 == null) {
                    e38 = price;
                    e100 = price;
                } else {
                    e38 = price * A38 + p38 * (1 - A38);
                    e100 = price * A100 + p100 * (1 - A100);
                    boolean below = strict ? p38 < p100 : p38 <= p100;
                    boolean above = strict ? p38 > p100 : p38 >= p100;
                    if (e38 > e100 && below) {
                        adv = "BUY";
                    } else if (e38 < e100 && above) {
                        adv = "SELL";
                    }
                }
                out.put(s.getKey() + "," + w.getKey(), new Res(e38, e100, adv));
                p38 = e38;
                p100 = e100;
            }
        }
        return out;
    }

    // ---------------------------------------------------------------- running the job

    static List<String> csvLines(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return new ArrayList<>();
        }
        List<String> all = new ArrayList<>();
        try (Stream<Path> s = Files.walk(dir)) {
            for (Path f : s.filter(Files::isRegularFile)
                    .filter(f -> !f.getFileName().toString().startsWith("."))
                    .collect(Collectors.toList())) {
                all.addAll(Files.readAllLines(f, StandardCharsets.UTF_8));
            }
        }
        return all;
    }

    static Map<String, Res> run(Path input, String name, String... extra) throws Exception {
        Path out = tmp.resolve(name);
        List<String> args = new ArrayList<>(Arrays.asList("--input", input.toString(), "--output", out.toString()));
        args.addAll(Arrays.asList(extra));
        Job.main(args.toArray(new String[0]));
        Map<String, Res> res = new HashMap<>();
        for (String line : csvLines(out.resolve("results"))) {
            String[] p = line.split(",", -1);
            assertEquals(5, p.length, "result line: " + line);
            Res prev = res.put(p[0] + "," + p[1], new Res(Double.parseDouble(p[2]), Double.parseDouble(p[3]), p[4]));
            assertTrue(prev == null, "duplicate window in output: " + line);
        }
        return res;
    }

    @BeforeAll
    static void runJob() throws Exception {
        Path input = writeInput();
        literalP1 = run(input, "literal_p1", "--parallelism", "1", "--late-output");
        lateDir = tmp.resolve("literal_p1").resolve("late");
        literalP2 = run(input, "literal_p2", "--parallelism", "2");
        strictP1 = run(input, "strict_p1", "--parallelism", "1", "--strict-crossover");
    }

    // ---------------------------------------------------------------- assertions

    static void assertSameResults(Map<String, Res> exp, Map<String, Res> act) {
        assertEquals(new TreeMap<>(exp).keySet(), new TreeMap<>(act).keySet(), "set of (symbol, window) results");
        for (Map.Entry<String, Res> e : exp.entrySet()) {
            Res a = act.get(e.getKey());
            assertEquals(e.getValue().e38, a.e38, 1e-9, "EMA38 " + e.getKey());
            assertEquals(e.getValue().e100, a.e100, 1e-9, "EMA100 " + e.getKey());
            assertEquals(e.getValue().adv, a.adv, "advisory " + e.getKey());
        }
    }

    static long count(Map<String, Res> r, String sym, String adv) {
        return r.entrySet().stream().filter(e -> e.getKey().startsWith(sym + ",") && e.getValue().adv.equals(adv)).count();
    }

    @Test
    void testDataExercisesTheRules() {
        Map<String, Res> lit = expected(false);
        Map<String, Res> str = expected(true);
        assertTrue(count(str, "OSC.FR", "BUY") >= 2, "at least 2 real BUY");
        assertTrue(count(str, "OSC.FR", "SELL") >= 2, "at least 2 real SELL");
        assertEquals(count(str, "OSC.FR", "SELL") + 1, count(lit, "OSC.FR", "SELL"), "one extra equal-start SELL under the literal rule");
    }

    @Test
    void literalRuleMatchesReferenceAtParallelism1() {
        assertSameResults(expected(false), literalP1);
    }

    @Test
    void literalRuleMatchesReferenceAtParallelism2() {
        assertSameResults(expected(false), literalP2);
    }

    @Test
    void strictRuleMatchesReference() {
        assertSameResults(expected(true), strictP1);
        assertEquals("", strictP1.get("OSC.FR," + (millis(DAY1) + 2 * WINDOW)).adv, "no equal-start signal under strict");
        assertEquals("SELL", literalP1.get("OSC.FR," + (millis(DAY1) + 2 * WINDOW)).adv, "equal-start SELL under literal");
    }

    @Test
    void parallelism1And2GiveIdenticalOutput() {
        assertEquals(new TreeMap<>(literalP1).keySet(), new TreeMap<>(literalP2).keySet());
        for (Map.Entry<String, Res> e : literalP1.entrySet()) {
            Res b = literalP2.get(e.getKey());
            assertEquals(e.getValue().e38, b.e38, 0.0, e.getKey());
            assertEquals(e.getValue().e100, b.e100, 0.0, e.getKey());
            assertEquals(e.getValue().adv, b.adv, e.getKey());
        }
    }

    /**
     * Constant price 50: mathematically both EMAs stay 50, but in floating point
     * 50*a + 50*(1-a) = 49.999999999999986 for a = 2/39 while EMA100 stays 50.0. The literal rule
     * therefore emits one SELL in window 2 (previous EMAs equal, now EMA38 a hair below EMA100);
     * the strict rule does not. This documents the job's actual behaviour (it matches the reference).
     */
    @Test
    void constantPriceEmasStayEqualUpToRoundingAndStrictGivesNoSignal() {
        long w2 = millis(DAY1) + 2 * WINDOW;
        List<Map.Entry<String, Res>> flat = literalP1.entrySet().stream().filter(e -> e.getKey().startsWith("FLAT.NL,"))
                .collect(Collectors.toList());
        assertEquals(40, flat.size());
        for (Map.Entry<String, Res> e : flat) {
            assertEquals(50.0, e.getValue().e38, 50 * 1e-9, e.getKey());
            assertEquals(50.0, e.getValue().e100, 50 * 1e-9, e.getKey());
            String expectedAdv = e.getKey().equals("FLAT.NL," + w2) ? "SELL" : "";
            assertEquals(expectedAdv, e.getValue().adv, "literal rule, rounding artefact only in window 2: " + e.getKey());
            assertEquals("", strictP1.get(e.getKey()).adv, "strict rule: no signal " + e.getKey());
        }
    }

    @Test
    void zeroAndEmptyLastRowsAreDropped() {
        List<Long> windows = literalP1.keySet().stream().filter(k -> k.startsWith("ZERO.FR,"))
                .map(k -> Long.parseLong(k.split(",")[1])).sorted().collect(Collectors.toList());
        assertEquals(10, windows.size(), "only the 10 even windows exist");
        for (long end : windows) {
            long w = (end - millis(DAY1)) / WINDOW - 1;
            assertEquals(0, w % 2, "window " + w + " should not exist");
        }
    }

    @Test
    void emptyTradingDateUsesSystemDate() {
        List<Long> windows = literalP1.keySet().stream().filter(k -> k.startsWith("IDX.ETR,"))
                .map(k -> Long.parseLong(k.split(",")[1])).collect(Collectors.toList());
        assertEquals(10, windows.size());
        for (long end : windows) {
            assertTrue(end > millis(DAY2) && end <= millis(DAY2) + 11 * WINDOW, "IDX.ETR window on the system date: " + end);
        }
    }

    /**
     * Documented tie rule: equal trading timestamps in one (symbol, window) -> the event on the later
     * line of the file wins (one reader per file, FIFO channels), at parallelism 1 and 2.
     */
    @Test
    void equalTimestampsTakeTheLaterLinesPrice() {
        for (Map<String, Res> r : Arrays.asList(literalP1, literalP2)) {
            double prev38 = 0;
            double prev100 = 0;
            for (int w = 0; w < TIE_WINDOWS; w++) {
                double p = tieWinner(w);
                double e38 = w == 0 ? p : p * A38 + prev38 * (1 - A38);
                double e100 = w == 0 ? p : p * A100 + prev100 * (1 - A100);
                Res a = r.get("TIE.NL," + (millis(DAY1) + (w + 1) * WINDOW));
                assertEquals(e38, a.e38, 0.0, "TIE.NL window " + w + " EMA38 from the later line's price");
                assertEquals(e100, a.e100, 0.0, "TIE.NL window " + w + " EMA100 from the later line's price");
                prev38 = e38;
                prev100 = e100;
            }
        }
    }

    @Test
    void outOfOrderEventIsNotLateWithDefaultTolerance() throws IOException {
        // Documented behaviour: with the default 200 h tolerance no event is late, so the event for
        // window 10 that arrives after 16 h of newer event time still becomes that window's close.
        Res w10 = literalP1.get("OSC.FR," + outOfOrderWindowEnd);
        double e38 = literalP1.get("OSC.FR," + (outOfOrderWindowEnd - WINDOW)).e38;
        assertEquals(95 * A38 + e38 * (1 - A38), w10.e38, 1e-9, "window 10 closes at the out-of-order price 95");
        assertTrue(csvLines(lateDir).isEmpty(), "--late-output must write no late events");
        assertFalse(literalP1.isEmpty());
    }
}
