package ema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

/**
 * Long-tail and key-skew report from the "symbol,count" CSV written by
 * "--stage symbolstats".
 *
 *   java -cp target/ema-crossover-1.0.jar ema.SkewReport output/symbolstats
 *
 * For each parallelism it uses Flink's own key-to-worker assignment (128 key
 * groups, the default max parallelism) to compute how many price events each
 * worker of the keyed window operator would receive, and the best speedup the
 * keyed stage could reach given that imbalance (total / busiest worker).
 */
public class SkewReport {
    static final int MAX_PARALLELISM = 128;

    public static void main(String[] args) throws IOException {
        Path dir = Paths.get(args[0]);
        List<String> symbols = new ArrayList<>();
        List<Long> counts = new ArrayList<>();
        try (Stream<Path> files = Files.walk(dir)) {
            for (Path f : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
                if (!f.getFileName().toString().endsWith(".csv")) {
                    continue;
                }
                for (String line : Files.readAllLines(f)) {
                    int c = line.lastIndexOf(',');
                    if (c < 0) {
                        continue;
                    }
                    symbols.add(line.substring(0, c));
                    counts.add(Long.parseLong(line.substring(c + 1).trim()));
                }
            }
        }
        int n = symbols.size();
        long total = 0;
        for (long c : counts) {
            total += c;
        }
        System.out.printf("symbols with price events: %d, total price events: %d%n", n, total);

        // long tail: sort by count, descending
        Integer[] order = new Integer[n];
        for (int i = 0; i < n; i++) {
            order[i] = i;
        }
        java.util.Arrays.sort(order, Comparator.comparingLong((Integer i) -> counts.get(i)).reversed());

        System.out.println("\nTop 10 symbols:");
        for (int r = 0; r < Math.min(10, n); r++) {
            int i = order[r];
            System.out.printf("  %-14s %12d  %5.2f%%%n", symbols.get(i), counts.get(i), 100.0 * counts.get(i) / total);
        }
        long cum = 0;
        int[] marks = {1, 10, 50, 100, 500};
        int mi = 0;
        System.out.println("\nShare of all price events held by the busiest k symbols:");
        for (int r = 0; r < n && mi < marks.length; r++) {
            cum += counts.get(order[r]);
            if (r + 1 == marks[mi]) {
                System.out.printf("  top %-4d symbols: %6.2f%%%n", marks[mi], 100.0 * cum / total);
                mi++;
            }
        }
        long half = 0;
        int k = 0;
        for (; k < n && half * 2 < total; k++) {
            half += counts.get(order[k]);
        }
        System.out.printf("  symbols needed to cover 50%% of events: %d (%.2f%% of symbols)%n", k, 100.0 * k / n);

        // per exchange (suffix after the last dot)
        Map<String, long[]> byEx = new TreeMap<>();
        for (int i = 0; i < n; i++) {
            String s = symbols.get(i);
            int d = s.lastIndexOf('.');
            String ex = d >= 0 ? s.substring(d + 1) : "?";
            long[] v = byEx.computeIfAbsent(ex, x -> new long[2]);
            v[0]++;
            v[1] += counts.get(i);
        }
        System.out.println("\nPer exchange: symbols, price events, share");
        for (Map.Entry<String, long[]> e : byEx.entrySet()) {
            System.out.printf("  %-4s %6d symbols  %12d events  %5.1f%%%n",
                    e.getKey(), e.getValue()[0], e.getValue()[1], 100.0 * e.getValue()[1] / total);
        }

        // key skew per parallelism
        System.out.println("\nKey skew of the keyed window stage (Flink key-group assignment):");
        for (int p : new int[] {2, 4, 8, 12}) {
            long[] load = new long[p];
            for (int i = 0; i < n; i++) {
                load[KeyGroupRangeAssignment.assignKeyToParallelOperator(symbols.get(i), MAX_PARALLELISM, p)] += counts.get(i);
            }
            long max = 0;
            for (long l : load) {
                max = Math.max(max, l);
            }
            StringBuilder sb = new StringBuilder();
            for (long l : load) {
                sb.append(String.format("%.1f%% ", 100.0 * l / total));
            }
            System.out.printf("  p=%-2d busiest worker %.1f%% (ideal %.1f%%)  best possible speedup %.2fx%n      loads: %s%n",
                    p, 100.0 * max / total, 100.0 / p, (double) total / max, sb);
        }
    }
}
