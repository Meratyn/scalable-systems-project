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
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Per-symbol order check: within each file, compares each price row of a symbol with the
 * previous price row of the same symbol (event time = Trading date + Trading time as the job
 * computes it, system-date fallback on). A backward step is a row whose event time is earlier
 * than the previous row's. Placeholder rows (empty Trading date, Trading time 01:00:00.000) are
 * excluded. Reports per exchange: symbols with any backward step, number of backward steps,
 * steps over 6 minutes, and the largest step.
 *
 *   java -Xmx3g -cp target/ema-crossover-1.0.jar ema.OrderReport ../data/raw > ../logs/order_report.txt
 */
public class OrderReport {
    static final long SIX_MIN = 6 * 60_000L;

    static final class Stat {
        long symbols;
        long symbolsWithBack;
        long steps;      // consecutive pairs compared
        long back;       // backward steps
        long backOver6;  // backward steps > 6 min
        long maxBack;    // ms
        String maxWhere = "";
    }

    public static void main(String[] args) throws IOException {
        List<Path> files;
        try (Stream<Path> s = Files.list(Paths.get(args[0]))) {
            files = s.filter(p -> p.getFileName().toString().endsWith(".csv")).sorted().collect(Collectors.toList());
        }
        Map<String, Stat> byEx = new TreeMap<>();
        Map<String, Boolean> symbolHasBack = new HashMap<>(); // over all files
        long excluded = 0;
        long t0 = System.currentTimeMillis();
        for (Path f : files) {
            Map<String, Long> prev = new HashMap<>(); // per file: symbol -> previous event time
            try (BufferedReader r = Files.newBufferedReader(f, StandardCharsets.ISO_8859_1)) {
                String line;
                long lineNo = 0;
                while ((line = r.readLine()) != null) {
                    lineNo++;
                    Tuple4<String, String, Double, Long> e = Parser.parse(line, true);
                    if (e == null) {
                        continue;
                    }
                    if (isPlaceholder(line)) {
                        excluded++;
                        continue;
                    }
                    Stat st = byEx.computeIfAbsent(e.f1, k -> new Stat());
                    symbolHasBack.putIfAbsent(e.f0, Boolean.FALSE);
                    Long p = prev.put(e.f0, e.f3);
                    if (p == null) {
                        continue;
                    }
                    st.steps++;
                    long d = p - e.f3;
                    if (d > 0) {
                        st.back++;
                        symbolHasBack.put(e.f0, Boolean.TRUE);
                        if (d > SIX_MIN) {
                            st.backOver6++;
                        }
                        if (d > st.maxBack) {
                            st.maxBack = d;
                            st.maxWhere = e.f0 + " " + f.getFileName() + " line " + lineNo;
                        }
                    }
                }
            }
            System.err.printf("%s done, %.0f s%n", f.getFileName(), (System.currentTimeMillis() - t0) / 1000.0);
        }
        for (Map.Entry<String, Boolean> en : symbolHasBack.entrySet()) {
            String sym = en.getKey();
            Stat st = byEx.get(sym.substring(sym.lastIndexOf('.') + 1));
            st.symbols++;
            if (en.getValue()) {
                st.symbolsWithBack++;
            }
        }
        System.out.printf("placeholder rows excluded: %d%n", excluded);
        System.out.println("exchange,symbols,symbols_with_backward_step,pairs_compared,backward_steps,"
                + "backward_steps_over_6min,max_backward_s,max_at");
        Stat all = new Stat();
        for (Map.Entry<String, Stat> en : byEx.entrySet()) {
            Stat s = en.getValue();
            print(en.getKey(), s);
            all.symbols += s.symbols;
            all.symbolsWithBack += s.symbolsWithBack;
            all.steps += s.steps;
            all.back += s.back;
            all.backOver6 += s.backOver6;
            if (s.maxBack > all.maxBack) {
                all.maxBack = s.maxBack;
                all.maxWhere = s.maxWhere;
            }
        }
        print("ALL", all);
        System.out.printf("seconds=%.1f%n", (System.currentTimeMillis() - t0) / 1000.0);
    }

    static void print(String name, Stat s) {
        System.out.printf("%s,%d,%d,%d,%d,%d,%.3f,%s%n", name, s.symbols, s.symbolsWithBack, s.steps, s.back,
                s.backOver6, s.maxBack / 1000.0, s.maxWhere);
    }

    /** Empty Trading date (field 26) and Trading time (field 23) exactly 01:00:00.000. */
    static boolean isPlaceholder(String line) {
        String[] p = line.trim().split(",", -1);
        return p.length >= 27 && p[26].isEmpty() && p[23].equals("01:00:00.000");
    }
}
