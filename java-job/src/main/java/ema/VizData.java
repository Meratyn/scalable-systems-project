package ema;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Prepares the data for the web visualisation (viz/index.html).
 *
 *   java -cp target/ema-crossover-1.0.jar ema.VizData <results-dir> <out.js> [--top 60] [--min-windows 300]
 *        [--per-exchange 15] [--min-move 0.005] [--symbols A.FR,B.NL]
 *
 * Reads the job's result CSV files (symbol,window_end_ms,ema38,ema100,advisory) and writes a
 * JavaScript file (window.VIZ_DATA = {...}) with: totals, per-exchange and per-symbol counts,
 * a day x hour heat map of crossovers, and the full EMA series of the selected symbols
 * (the --top busiest by crossovers, plus any named with --symbols). Plain .js, not .json,
 * so the page works when opened straight from disk.
 *
 * "Equal-start" signals: a signal whose previous window had exactly equal EMAs (always the case on a
 * symbol's second window, since both EMAs are seeded with the first price, and again after any flat
 * stretch). The literal spec rule fires there although nothing crossed. They are counted and drawn
 * separately; the heat map uses real crossings only (what --strict-crossover keeps).
 */
public class VizData {
    static final long STEP = 300_000L;
    static final long DAY = 86_400_000L;

    static class Agg {
        String ex;
        int windows, buy, sell;
        double emaMin = Double.MAX_VALUE, emaMax = -Double.MAX_VALUE, emaSum; // EMA38 over the week
        boolean flat;                // movement (max-min)/mean of EMA38 below --min-move

        double move() {
            double mean = emaSum / windows;
            return mean == 0 ? 0 : (emaMax - emaMin) / Math.abs(mean);
        }
        long[] enc = new long[64];   // (time << 1) | (ema38 == ema100 ? 1 : 0)
        int n;
        long[] sigT = new long[4];   // (time << 1) | (BUY ? 1 : 0) of signal rows
        int ns;
        java.util.HashSet<Long> eqNext = new java.util.HashSet<>(); // times whose previous row had equal EMAs
        int eq;                      // signals that start from equal EMAs
        int eqBuy;                   // ... of which BUY (the rest are SELL)

        void add(long t, boolean equal) {
            if (n == enc.length) {
                enc = Arrays.copyOf(enc, n * 2);
            }
            enc[n++] = (t << 1) | (equal ? 1 : 0);
        }

        void addSignal(long t, boolean isBuy) {
            if (ns == sigT.length) {
                sigT = Arrays.copyOf(sigT, ns * 2);
            }
            sigT[ns++] = (t << 1) | (isBuy ? 1 : 0);
        }

        boolean isEqualStart(long t) {
            return eqNext.contains(t);
        }
    }

    static class Row {
        long t; double a, b; String adv;
    }

    static String[] parts(String line) {
        String[] p = new String[5];
        int n = 0, start = 0;
        for (int i = 0; i <= line.length() && n < 5; i++) {
            if (i == line.length() || line.charAt(i) == ',') {
                p[n++] = line.substring(start, i);
                start = i + 1;
            }
        }
        return n == 5 ? p : null;
    }

    static List<Path> csvFiles(Path dir) throws IOException {
        try (Stream<Path> s = Files.walk(dir)) {
            return s.filter(Files::isRegularFile).filter(f -> f.getFileName().toString().endsWith(".csv"))
                    .sorted().collect(Collectors.toList());
        }
    }

    static String exchangeOf(String sym) {
        int d = sym.lastIndexOf('.');
        return d >= 0 ? sym.substring(d + 1) : "?";
    }

    /** 9 significant digits, trailing zeros removed: enough to tell EMAs apart right at a crossing of a 0.79 EUR stock. */
    static String num(double v) {
        return new java.math.BigDecimal(v).round(new java.math.MathContext(9)).stripTrailingZeros().toPlainString();
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("usage: VizData <results-dir> <out.js> [--top N] [--min-windows N] [--per-exchange K] [--min-move F] [--symbols A.FR,B.NL]");
            System.exit(2);
        }
        Path dir = Paths.get(args[0]);
        Path out = Paths.get(args[1]);
        int top = 60;
        int minWindows = 300;   // only symbols with at least this many windows are drawn by default
        int perExchange = 15;   // ... and at least this many from each exchange
        double minMove = 0.005; // ... and whose EMA38 moves at least 0.5 % of its mean over the week
        Set<String> extra = new HashSet<>();
        for (int i = 2; i < args.length; i++) {
            if (args[i].equals("--top")) {
                top = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--min-windows")) {
                minWindows = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--min-move")) {
                minMove = Double.parseDouble(args[++i]);
            } else if (args[i].equals("--per-exchange")) {
                perExchange = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--symbols")) {
                extra.addAll(Arrays.asList(args[++i].split(",")));
            }
        }
        List<Path> files = csvFiles(dir);
        if (files.isEmpty()) {
            System.err.println("no .csv files under " + dir);
            System.exit(1);
        }

        // ---- pass 1: per-symbol aggregates ----
        Map<String, Agg> aggs = new HashMap<>();
        long tMin = Long.MAX_VALUE, tMax = Long.MIN_VALUE;
        long rows = 0;
        for (Path f : files) {
            try (Stream<String> lines = Files.lines(f, StandardCharsets.UTF_8)) {
                for (String line : (Iterable<String>) lines::iterator) {
                    String[] p = parts(line);
                    if (p == null) {
                        continue;
                    }
                    long t = Long.parseLong(p[1]);
                    double a = Double.parseDouble(p[2]);
                    double b = Double.parseDouble(p[3]);
                    Agg g = aggs.computeIfAbsent(p[0], k -> {
                        Agg x = new Agg();
                        x.ex = exchangeOf(k);
                        return x;
                    });
                    g.windows++;
                    g.emaMin = Math.min(g.emaMin, a);
                    g.emaMax = Math.max(g.emaMax, a);
                    g.emaSum += a;
                    if (p[4].equals("BUY")) {
                        g.buy++;
                    } else if (p[4].equals("SELL")) {
                        g.sell++;
                    }
                    g.add(t, a == b);
                    if (!p[4].isEmpty()) {
                        g.addSignal(t, p[4].equals("BUY"));
                    }
                    tMin = Math.min(tMin, t);
                    tMax = Math.max(tMax, t);
                    rows++;
                }
            }
        }
        for (Agg g : aggs.values()) {
            Arrays.sort(g.enc, 0, g.n);
            for (int i = 0; i + 1 < g.n; i++) {
                if ((g.enc[i] & 1L) != 0) {
                    g.eqNext.add(g.enc[i + 1] >> 1);
                }
            }
            for (int i = 0; i < g.ns; i++) {
                if (g.isEqualStart(g.sigT[i] >> 1)) {
                    g.eq++;
                    if ((g.sigT[i] & 1L) != 0) {
                        g.eqBuy++;
                    }
                }
            }
            g.enc = null;
        }

        // ---- choose symbols to include ----
        List<String> order = new ArrayList<>(aggs.keySet());
        order.sort((x, y) -> {
            Agg a = aggs.get(x), b = aggs.get(y);
            int ca = a.buy + a.sell - a.eq, cb = b.buy + b.sell - b.eq;
            if (ca != cb) {
                return Integer.compare(cb, ca);
            }
            return x.compareTo(y);
        });
        // Top crossers among symbols with at least minWindows windows, with at least perExchange
        // symbols from every exchange, filled up to `top` in crossing order; plus --symbols.
        // Flat symbols (EMA38 moves less than minMove of its mean, e.g. a 0.795 EUR stock that only
        // ticks by 0.0001) are not drawn by default: their crossings are tick noise and the chart is unreadable.
        List<String> eligible = new ArrayList<>();
        int flatAll = 0, flatSkipped = 0;
        for (String s : order) {
            Agg g = aggs.get(s);
            g.flat = g.move() < minMove;
            if (g.flat) {
                flatAll++;
            }
            if (g.windows >= minWindows) {
                if (g.flat) {
                    flatSkipped++;
                } else {
                    eligible.add(s);
                }
            }
        }
        Set<String> withSeries = new java.util.LinkedHashSet<>();
        Set<String> exchanges = new java.util.TreeSet<>();
        for (Agg g : aggs.values()) {
            exchanges.add(g.ex);
        }
        for (String ex : exchanges) {
            int k = 0;
            for (String s : eligible) {
                if (k >= perExchange) {
                    break;
                }
                if (aggs.get(s).ex.equals(ex)) {
                    withSeries.add(s);
                    k++;
                }
            }
            if (k < perExchange) {
                System.err.println("warning: only " + k + " eligible symbols on " + ex);
            }
        }
        for (String s : eligible) {
            if (withSeries.size() >= top) {
                break;
            }
            withSeries.add(s);
        }
        for (String s : extra) {
            if (aggs.containsKey(s)) {
                withSeries.add(s);
            } else {
                System.err.println("warning: symbol not in results: " + s);
            }
        }
        System.out.printf("drawn series: %d (top %d, min-windows %d, per-exchange %d, min-move %.4f, %d eligible symbols)%n",
                withSeries.size(), top, minWindows, perExchange, minMove, eligible.size());
        System.out.printf("flat symbols (move < %.2f%%): %d of %d; skipped as flat among symbols with >= %d windows: %d%n",
                minMove * 100, flatAll, aggs.size(), minWindows, flatSkipped);
        for (String ex : exchanges) {
            List<String> names = new ArrayList<>();
            for (String s : order) {
                if (withSeries.contains(s) && aggs.get(s).ex.equals(ex)) {
                    Agg g = aggs.get(s);
                    names.add(s + "(" + (g.buy + g.sell - g.eq) + ")");
                }
            }
            System.out.println("  " + ex + " " + names.size() + ": " + String.join(" ", names));
        }

        // ---- pass 2: heat map of real crossings + series of chosen symbols ----
        long day0 = Math.floorDiv(tMin, DAY);
        int nDays = (int) (Math.floorDiv(tMax, DAY) - day0 + 1);
        int[][] heat = new int[nDays][24];
        Map<String, List<Row>> series = new HashMap<>();
        for (String s : withSeries) {
            series.put(s, new ArrayList<>());
        }
        for (Path f : files) {
            try (Stream<String> lines = Files.lines(f, StandardCharsets.UTF_8)) {
                for (String line : (Iterable<String>) lines::iterator) {
                    String[] p = parts(line);
                    if (p == null) {
                        continue;
                    }
                    long t = Long.parseLong(p[1]);
                    Agg g = aggs.get(p[0]);
                    boolean eqSignal = !p[4].isEmpty() && g.isEqualStart(t);
                    if (!p[4].isEmpty() && !eqSignal) {
                        int d = (int) (Math.floorDiv(t, DAY) - day0);
                        int h = (int) (Math.floorMod(t, DAY) / 3_600_000L);
                        heat[d][h]++;
                    }
                    List<Row> sr = series.get(p[0]);
                    if (sr != null) {
                        Row r = new Row();
                        r.t = t;
                        r.a = Double.parseDouble(p[2]);
                        r.b = Double.parseDouble(p[3]);
                        r.adv = p[4];
                        sr.add(r);
                    }
                }
            }
        }

        // ---- totals ----
        long buy = 0, sell = 0, eq = 0, eqBuy = 0;
        Map<String, long[]> byEx = new TreeMap<>(); // symbols, windows, buy, sell, eq, eqBuy
        for (Agg g : aggs.values()) {
            buy += g.buy;
            sell += g.sell;
            eq += g.eq;
            eqBuy += g.eqBuy;
            long[] e = byEx.computeIfAbsent(g.ex, k -> new long[6]);
            e[0]++;
            e[1] += g.windows;
            e[2] += g.buy;
            e[3] += g.sell;
            e[4] += g.eq;
            e[5] += g.eqBuy;
        }

        // ---- write ----
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out, StandardCharsets.UTF_8))) {
            w.print("window.VIZ_DATA = {\n");
            w.printf("\"meta\":{\"t0\":%d,\"step\":%d,\"tMax\":%d,\"top\":%d,\"minWindows\":%d,\"perExchange\":%d,\"minMove\":%s},\n",
                    tMin, STEP, tMax, top, minWindows, perExchange, num(minMove));
            w.printf("\"totals\":{\"symbols\":%d,\"windows\":%d,\"buy\":%d,\"sell\":%d,\"equalStart\":%d,\"equalStartBuy\":%d},\n",
                    aggs.size(), rows, buy, sell, eq, eqBuy);
            w.print("\"exchanges\":[");
            boolean first = true;
            for (Map.Entry<String, long[]> e : byEx.entrySet()) {
                long[] v = e.getValue();
                w.printf("%s{\"ex\":\"%s\",\"symbols\":%d,\"windows\":%d,\"buy\":%d,\"sell\":%d,\"equalStart\":%d,\"equalStartBuy\":%d}",
                        first ? "" : ",", e.getKey(), v[0], v[1], v[2], v[3], v[4], v[5]);
                first = false;
            }
            w.print("],\n\"days\":[");
            for (int d = 0; d < nDays; d++) {
                w.printf("%s\"%s\"", d == 0 ? "" : ",", LocalDate.ofEpochDay(day0 + d));
            }
            w.print("],\n\"heat\":[");
            for (int d = 0; d < nDays; d++) {
                w.print((d == 0 ? "" : ",") + Arrays.toString(heat[d]).replace(" ", ""));
            }
            w.print("],\n\"symbols\":[\n");
            for (int i = 0; i < order.size(); i++) {
                String s = order.get(i);
                Agg g = aggs.get(s);
                // [name, exchange, windows, buy, sell, equalStart, hasSeries, equalStartBuy, flat, move]
                w.printf("%s[\"%s\",\"%s\",%d,%d,%d,%d,%d,%d,%d,%s]\n", i == 0 ? "" : ",", s, g.ex, g.windows, g.buy, g.sell, g.eq,
                        withSeries.contains(s) ? 1 : 0, g.eqBuy, g.flat ? 1 : 0, String.format(Locale.ROOT, "%.5f", g.move()));
            }
            w.print("],\n\"series\":{\n");
            first = true;
            for (String s : order) {
                if (!withSeries.contains(s)) {
                    continue;
                }
                Agg g = aggs.get(s);
                List<Row> sr = series.get(s);
                sr.sort((x, y) -> Long.compare(x.t, y.t));
                StringBuilder ts = new StringBuilder(), as = new StringBuilder(), bs = new StringBuilder(), cs = new StringBuilder();
                for (int i = 0; i < sr.size(); i++) {
                    Row r = sr.get(i);
                    if (i > 0) {
                        ts.append(',');
                        as.append(',');
                        bs.append(',');
                    }
                    ts.append((r.t - tMin) / STEP);
                    as.append(num(r.a));
                    bs.append(num(r.b));
                    boolean eqSignal = !r.adv.isEmpty() && g.isEqualStart(r.t);
                    char c = '0';
                    if (r.adv.equals("BUY")) {
                        c = eqSignal ? '3' : '1';
                    } else if (r.adv.equals("SELL")) {
                        c = eqSignal ? '4' : '2';
                    }
                    cs.append(c);
                }
                w.printf("%s\"%s\":{\"t\":[%s],\"a\":[%s],\"b\":[%s],\"c\":\"%s\"}\n", first ? "" : ",", s, ts, as, bs, cs);
                first = false;
            }
            w.print("}\n};\n");
        }
        System.out.printf("rows=%d symbols=%d buy=%d sell=%d equal-start=%d (buy %d, sell %d) (real crossings=%d) series=%d -> %s%n",
                rows, aggs.size(), buy, sell, eq, eqBuy, eq - eqBuy, buy + sell - eq, withSeries.size(), out);
    }
}
