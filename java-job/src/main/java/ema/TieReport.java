package ema;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Tie report for the window price: for every (symbol, 5-minute window) it finds the
 * maximum trading timestamp and checks how many events share it, whether their Last
 * prices differ, and whether they come from one file or several.
 *
 *   java -Xmx3g -cp target/ema-crossover-1.0.jar ema.TieReport ../data/raw [--require-trading-date]
 *
 * Uses the job's own parser (Parser.parse), so the events are exactly the job's price
 * events. Files are read in name order (= day order), one pass, single thread.
 */
public class TieReport {
    static final long WINDOW_MS = 5 * 60_000L;

    static final class Group {
        long maxTs;
        int count;          // events at maxTs
        double firstPrice;  // price of the first event at maxTs (input order)
        double lastPrice;   // price of the last event at maxTs (input order) = tie-break winner
        boolean pricesDiffer;
        int fileMask;       // files that contributed an event at maxTs
        int lastFile;
        long lastLine;
    }

    public static void main(String[] args) throws IOException {
        Path dir = Paths.get(args[0]);
        boolean fallback = !(args.length > 1 && args[1].equals("--require-trading-date"));
        List<Path> files;
        try (Stream<Path> s = Files.list(dir)) {
            files = s.filter(p -> p.getFileName().toString().endsWith(".csv")).sorted()
                    .collect(Collectors.toList());
        }
        Map<String, Group> groups = new HashMap<>(4_000_000);
        long events = 0;
        long t0 = System.currentTimeMillis();
        for (int fi = 0; fi < files.size(); fi++) {
            Path f = files.get(fi);
            long lineNo = 0;
            try (BufferedReader r = Files.newBufferedReader(f, StandardCharsets.ISO_8859_1)) {
                String line;
                while ((line = r.readLine()) != null) {
                    lineNo++;
                    Tuple4<String, String, Double, Long> e = Parser.parse(line, fallback);
                    if (e == null) {
                        continue;
                    }
                    events++;
                    long ts = e.f3;
                    long wStart = Math.floorDiv(ts, WINDOW_MS) * WINDOW_MS;
                    String key = e.f0 + '|' + wStart;
                    Group g = groups.get(key);
                    double price = e.f2;
                    if (g == null) {
                        g = new Group();
                        groups.put(key, g);
                        g.maxTs = Long.MIN_VALUE;
                    }
                    if (ts > g.maxTs) {
                        g.maxTs = ts;
                        g.count = 1;
                        g.firstPrice = price;
                        g.lastPrice = price;
                        g.pricesDiffer = false;
                        g.fileMask = 1 << fi;
                    } else if (ts == g.maxTs) {
                        g.count++;
                        if (price != g.firstPrice) {
                            g.pricesDiffer = true;
                        }
                        g.lastPrice = price;
                        g.fileMask |= 1 << fi;
                    } else {
                        continue;
                    }
                    g.lastFile = fi;
                    g.lastLine = lineNo;
                }
            }
            System.err.printf("%s: %d lines, %d events so far, %d groups, %.0f s%n",
                    f.getFileName(), lineNo, events, groups.size(),
                    (System.currentTimeMillis() - t0) / 1000.0);
        }

        long tied = 0, tiedDiff = 0, tiedSameFile = 0, tiedMultiFile = 0;
        long diffSameFile = 0, diffMultiFile = 0;
        long maxCount = 0;
        long[] byCount = new long[6]; // 2,3,4,5,6+ tied events
        Map<String, long[]> byExchange = new HashMap<>(); // tied, tiedDiff, diffMultiFile
        int shown = 0;
        for (Map.Entry<String, Group> en : groups.entrySet()) {
            Group g = en.getValue();
            if (g.count < 2) {
                continue;
            }
            tied++;
            maxCount = Math.max(maxCount, g.count);
            byCount[Math.min(g.count, 6) - 2]++;
            boolean multi = Integer.bitCount(g.fileMask) > 1;
            if (multi) {
                tiedMultiFile++;
            } else {
                tiedSameFile++;
            }
            String sym = en.getKey().substring(0, en.getKey().indexOf('|'));
            String ex = sym.substring(sym.lastIndexOf('.') + 1);
            long[] x = byExchange.computeIfAbsent(ex, k -> new long[3]);
            x[0]++;
            if (g.pricesDiffer) {
                tiedDiff++;
                x[1]++;
                if (multi) {
                    diffMultiFile++;
                    x[2]++;
                    if (shown++ < 10) {
                        System.out.printf("  example multi-file diff: %s ts=%d count=%d files=%s%n",
                                en.getKey(), g.maxTs, g.count, Integer.toBinaryString(g.fileMask));
                    }
                } else {
                    diffSameFile++;
                }
            }
        }
        System.out.printf("files=%d price_events=%d groups=%d%n", files.size(), events, groups.size());
        System.out.printf("tied_groups=%d (max timestamp shared by >=2 events)%n", tied);
        System.out.printf("  same_file=%d multi_file=%d%n", tiedSameFile, tiedMultiFile);
        System.out.printf("  tied events per group: 2=%d 3=%d 4=%d 5=%d 6+=%d (max %d)%n",
                byCount[0], byCount[1], byCount[2], byCount[3], byCount[4], maxCount);
        System.out.printf("tied_groups_with_different_prices=%d%n", tiedDiff);
        System.out.printf("  same_file=%d multi_file=%d%n", diffSameFile, diffMultiFile);
        for (Map.Entry<String, long[]> en : byExchange.entrySet()) {
            long[] x = en.getValue();
            System.out.printf("exchange %s: tied=%d tied_diff_price=%d diff_price_multi_file=%d%n",
                    en.getKey(), x[0], x[1], x[2]);
        }
        System.out.printf("seconds=%.1f%n", (System.currentTimeMillis() - t0) / 1000.0);
    }
}
