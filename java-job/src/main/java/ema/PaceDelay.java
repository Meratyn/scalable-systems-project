package ema;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Paced-replay analysis for ONE day file (one reader, so one Pacer). Rebuilds the reader's virtual
 * clock exactly as Pacer does (parser defaults, file order, forward jumps capped at 10 min).
 *
 *   scan:    java -cp ema.jar ema.PaceDelay scan <day.csv>
 *            -> price events, first/last event time, raw span, capped virtual duration
 *   analyze: java -cp ema.jar ema.PaceDelay analyze <day.csv> <speed> <tolerance_h> <latency_log_prefix>
 *            -> measured latency and excess, overall / last 30 min before the day's last event / others
 *
 * Virtual clock V(t) = virtual time of the largest max m <= t, plus min(t - m, 10 min).
 * Virtual delay of a window = min(V(end + tol), V_final) - V(last event time): the watermark
 * passes the window when the event time reaches end + tol, or at end of input (V_final) if the
 * day ends first. Excess = measured latency - virtual delay / speed.
 * excess_trigger: as excess, but the virtual delay ends at the event that actually advances the
 * watermark past end + tol (the first event with event time >= end + tol, or end of input), so the
 * paced wait for that next event is not counted either.
 */
public class PaceDelay {
    static final long CAP = Pacer.MAX_JUMP_MS;
    static final long LAST30 = 30 * 60_000L;

    long[] m = new long[1 << 20]; // increasing maxima (event time)
    long[] v = new long[1 << 20]; // virtual time at each maximum
    int n;
    long events;
    long cappedJumps;

    void scanFile(Path f) throws IOException {
        long vt = 0;
        try (BufferedReader r = Files.newBufferedReader(f, StandardCharsets.ISO_8859_1)) {
            String line;
            while ((line = r.readLine()) != null) {
                Tuple4<String, String, Double, Long> e = Parser.parse(line, true);
                if (e == null) {
                    continue;
                }
                events++;
                long t = e.f3;
                if (n == 0) {
                    add(t, 0);
                } else if (t > m[n - 1]) {
                    long d = t - m[n - 1];
                    if (d > CAP) {
                        cappedJumps++;
                    }
                    vt += Math.min(d, CAP);
                    add(t, vt);
                }
            }
        }
    }

    void add(long t, long vt) {
        if (n == m.length) {
            m = Arrays.copyOf(m, n * 2);
            v = Arrays.copyOf(v, n * 2);
        }
        m[n] = t;
        v[n] = vt;
        n++;
    }

    long virtualAt(long t) {
        int i = Arrays.binarySearch(m, 0, n, t);
        if (i < 0) {
            i = -i - 2; // largest index with m[i] < t
        }
        if (i < 0) {
            return 0;
        }
        return v[i] + Math.min(t - m[i], CAP);
    }

    /** Virtual time of the first event with event time >= t (it advances the watermark past t), or V_final. */
    long virtualTrigger(long t) {
        int i = Arrays.binarySearch(m, 0, n, t);
        if (i < 0) {
            i = -i - 1; // first index with m[i] > t
        }
        return i < n ? v[i] : vFinal();
    }

    long vFinal() {
        return v[n - 1];
    }

    public static void main(String[] args) throws IOException {
        PaceDelay pd = new PaceDelay();
        pd.scanFile(Paths.get(args[1]));
        long first = pd.m[0];
        long last = pd.m[pd.n - 1];
        if (args[0].equals("scan")) {
            System.out.printf("file=%s%nprice_events=%d%nfirst_event_ms=%d%nlast_event_ms=%d%n", args[1],
                    pd.events, first, last);
            System.out.printf("raw_span_h=%.3f%nvirtual_h=%.3f%ncapped_jumps=%d%n", (last - first) / 3.6e6,
                    pd.vFinal() / 3.6e6, pd.cappedJumps);
            return;
        }
        double speed = Double.parseDouble(args[2]);
        long tol = (long) (Double.parseDouble(args[3]) * 3600_000L);
        Path prefix = Paths.get(args[4]);
        long[] lat = new long[1 << 18];
        long[] exc = new long[1 << 18];
        long[] exT = new long[1 << 18];
        boolean[] late = new boolean[1 << 18];
        int k = 0;
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(prefix.getParent(),
                prefix.getFileName() + ".*")) {
            for (Path p : ds) {
                for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                    String[] f = line.split(",");
                    long end = Long.parseLong(f[1]);
                    long us = Long.parseLong(f[2]);
                    long tLast = Long.parseLong(f[3]);
                    long vd = Math.min(pd.virtualAt(end + tol), pd.vFinal()) - pd.virtualAt(tLast);
                    if (k == lat.length) {
                        lat = Arrays.copyOf(lat, k * 2);
                        exc = Arrays.copyOf(exc, k * 2);
                        exT = Arrays.copyOf(exT, k * 2);
                        late = Arrays.copyOf(late, k * 2);
                    }
                    lat[k] = us;
                    exc[k] = us - (long) (vd * 1000.0 / speed);
                    long vdT = pd.virtualTrigger(end + tol) - pd.virtualAt(tLast);
                    exT[k] = us - (long) (vdT * 1000.0 / speed);
                    late[k] = end > last - LAST30;
                    k++;
                }
            }
        }
        System.out.printf("speed=%s tolerance_h=%s results=%d price_events=%d%n", args[2], args[3], k, pd.events);
        System.out.println("group,metric,count,median_ms,p95_ms,p99_ms,max_ms");
        for (int g = 0; g < 3; g++) {
            String name = g == 0 ? "all" : g == 1 ? "last30min" : "others";
            long[] a = new long[k];
            long[] b = new long[k];
            long[] bt = new long[k];
            int c = 0;
            for (int i = 0; i < k; i++) {
                if (g == 0 || (g == 1) == late[i]) {
                    a[c] = lat[i];
                    b[c] = exc[i];
                    bt[c] = exT[i];
                    c++;
                }
            }
            print(name, "measured", Arrays.copyOf(a, c));
            print(name, "excess", Arrays.copyOf(b, c));
            print(name, "excess_trigger", Arrays.copyOf(bt, c));
        }
    }

    /** Nearest-rank percentiles, as scripts/latency_stats.py. */
    static void print(String group, String metric, long[] us) {
        Arrays.sort(us);
        int c = us.length;
        if (c == 0) {
            System.out.printf("%s,%s,0,,,,%n", group, metric);
            return;
        }
        System.out.printf("%s,%s,%d,%.1f,%.1f,%.1f,%.1f%n", group, metric, c, pct(us, 0.5), pct(us, 0.95),
                pct(us, 0.99), us[c - 1] / 1000.0);
    }

    static double pct(long[] s, double q) {
        return s[Math.max(0, (int) Math.ceil(q * s.length) - 1)] / 1000.0;
    }
}
