package ema;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Cross-symbol disorder per file: for each price row (event time as the job computes it, system-date
 * fallback on), lag = largest event time seen so far in the same file (over all symbols) - the row's
 * event time. Placeholder rows (empty Trading date, Trading time 01:00:00.000) are excluded, both as
 * rows and from the running maximum. Reports per file and overall: price rows, max lag,
 * 99.9th percentile (nearest rank), rows with lag > 6 min, > 1 min, > 1 s.
 *
 *   java -Xmx4g -cp target/ema-crossover-1.0.jar ema.DisorderReport ../data/raw > ../logs/disorder_report.txt
 */
public class DisorderReport {

    public static void main(String[] args) throws IOException {
        List<Path> files;
        try (Stream<Path> s = Files.list(Paths.get(args[0]))) {
            files = s.filter(p -> p.getFileName().toString().endsWith(".csv")).sorted().collect(Collectors.toList());
        }
        System.out.println("file,price_rows,max_lag_s,p999_lag_s,rows_over_6min,rows_over_1min,rows_over_1s");
        long[] total = new long[4];
        long totalMax = 0;
        int[] all = new int[0];
        int allN = 0;
        long excluded = 0;
        long t0 = System.currentTimeMillis();
        for (Path f : files) {
            int[] lags = new int[1 << 22];
            int n = 0;
            long max = Long.MIN_VALUE;
            long over6 = 0;
            long over60 = 0;
            long over1 = 0;
            try (BufferedReader r = Files.newBufferedReader(f, StandardCharsets.ISO_8859_1)) {
                String line;
                while ((line = r.readLine()) != null) {
                    Tuple4<String, String, Double, Long> e = Parser.parse(line, true);
                    if (e == null) {
                        continue;
                    }
                    if (Parser.parse(line, true, true) == null) {
                        excluded++; // placeholder row
                        continue;
                    }
                    long t = e.f3;
                    long lag = 0;
                    if (t > max) {
                        max = t;
                    } else {
                        lag = max - t;
                    }
                    if (lag > 360_000) {
                        over6++;
                    }
                    if (lag > 60_000) {
                        over60++;
                    }
                    if (lag > 1_000) {
                        over1++;
                    }
                    if (n == lags.length) {
                        lags = Arrays.copyOf(lags, n * 2);
                    }
                    lags[n++] = (int) Math.min(lag, Integer.MAX_VALUE);
                }
            }
            int[] s = Arrays.copyOf(lags, n);
            Arrays.sort(s);
            long fileMax = n == 0 ? 0 : s[n - 1];
            System.out.printf("%s,%d,%.3f,%.3f,%d,%d,%d%n", f.getFileName(), n, fileMax / 1000.0,
                    n == 0 ? 0 : p999(s) / 1000.0, over6, over60, over1);
            total[0] += n;
            total[1] += over6;
            total[2] += over60;
            total[3] += over1;
            totalMax = Math.max(totalMax, fileMax);
            if (allN + n > all.length) {
                all = Arrays.copyOf(all, Math.max(all.length * 2, allN + n));
            }
            System.arraycopy(s, 0, all, allN, n);
            allN += n;
            System.err.printf("%s done, %.0f s%n", f.getFileName(), (System.currentTimeMillis() - t0) / 1000.0);
        }
        int[] a = Arrays.copyOf(all, allN);
        Arrays.sort(a);
        System.out.printf("ALL,%d,%.3f,%.3f,%d,%d,%d%n", total[0], totalMax / 1000.0, p999(a) / 1000.0, total[1],
                total[2], total[3]);
        System.out.printf("placeholder rows excluded: %d%n", excluded);
    }

    static long p999(int[] sorted) {
        return sorted[Math.max(0, (int) Math.ceil(0.999 * sorted.length) - 1)];
    }
}
