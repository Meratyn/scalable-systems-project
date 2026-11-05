package ema;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Turns the benchmark logs into java-job/viz/perf.js (window.PERF_DATA = {...}) for the
 * Performance section of the web visualisation.
 *
 *   java -cp target/ema-crossover-1.0.jar ema.PerfData [--bench logs/bench_times.csv]
 *        [--ablation logs/ablation_times.csv] [--out java-job/viz/perf.js] [--exclude baseline:2]
 *
 * bench_times.csv:    dataset,parallelism,rep,seconds
 * ablation_times.csv: config,rep,seconds,cpu_percent,max_rss_kb,check
 * Values are medians over the runs of each group. --exclude config:rep[,config:rep] drops
 * ablation runs; the default drops baseline rep 2, a machine stall (583.7 s at 73 % CPU)
 * that was re-run as rep 4.
 */
public class PerfData {

    static double median(List<Double> v) {
        List<Double> s = new ArrayList<>(v);
        Collections.sort(s);
        int n = s.size();
        return n % 2 == 1 ? s.get(n / 2) : (s.get(n / 2 - 1) + s.get(n / 2)) / 2;
    }

    static String f(double v, int d) {
        return String.format(Locale.ROOT, "%." + d + "f", v);
    }

    /** Reads a CSV with a header row; checks that the expected columns are there. */
    static List<Map<String, String>> read(Path p, String... cols) throws IOException {
        List<String> lines = Files.readAllLines(p, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new IOException(p + " is empty");
        }
        List<String> head = Arrays.asList(lines.get(0).trim().split(","));
        for (String c : cols) {
            if (!head.contains(c)) {
                throw new IOException(p + ": missing column '" + c + "' (header: " + lines.get(0) + ")");
            }
        }
        List<Map<String, String>> rows = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty()) {
                continue;
            }
            String[] v = line.split(",", -1);
            if (v.length != head.size()) {
                throw new IOException(p + " line " + (i + 1) + ": expected " + head.size() + " fields: " + line);
            }
            Map<String, String> r = new LinkedHashMap<>();
            for (int k = 0; k < v.length; k++) {
                r.put(head.get(k), v[k]);
            }
            rows.add(r);
        }
        return rows;
    }

    public static void main(String[] args) throws IOException {
        Path bench = Paths.get("logs/bench_times.csv");
        Path abl = Paths.get("logs/ablation_times.csv");
        Path out = Paths.get("java-job/viz/perf.js");
        Set<String> exclude = new HashSet<>(Collections.singletonList("baseline:2"));
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--bench": bench = Paths.get(args[++i]); break;
                case "--ablation": abl = Paths.get(args[++i]); break;
                case "--out": out = Paths.get(args[++i]); break;
                case "--exclude":
                    exclude.clear();
                    for (String e : args[++i].split(",")) {
                        if (!e.isEmpty()) {
                            exclude.add(e);
                        }
                    }
                    break;
                default:
                    System.err.println("usage: PerfData [--bench csv] [--ablation csv] [--out perf.js] [--exclude config:rep,...]");
                    System.exit(2);
            }
        }

        // ---- runtime vs parallelism: dataset -> parallelism -> seconds ----
        Map<String, TreeMap<Integer, List<Double>>> b = new TreeMap<>();
        for (Map<String, String> r : read(bench, "dataset", "parallelism", "rep", "seconds")) {
            b.computeIfAbsent(r.get("dataset"), k -> new TreeMap<>())
                    .computeIfAbsent(Integer.parseInt(r.get("parallelism")), k -> new ArrayList<>())
                    .add(Double.parseDouble(r.get("seconds")));
        }

        // ---- ablation: config (file order) -> runs ----
        Map<String, List<double[]>> a = new LinkedHashMap<>();
        Map<String, String> check = new LinkedHashMap<>();
        List<String> dropped = new ArrayList<>();
        for (Map<String, String> r : read(abl, "config", "rep", "seconds", "cpu_percent", "max_rss_kb", "check")) {
            String key = r.get("config") + ":" + r.get("rep");
            if (exclude.contains(key)) {
                dropped.add(key + " (" + r.get("seconds") + " s)");
                continue;
            }
            a.computeIfAbsent(r.get("config"), k -> new ArrayList<>()).add(new double[] {
                    Double.parseDouble(r.get("seconds")), Double.parseDouble(r.get("cpu_percent")),
                    Double.parseDouble(r.get("max_rss_kb")) / 1e6});
            String c = r.get("check");
            if (!c.equals("-") && !check.containsKey(r.get("config"))) {
                check.put(r.get("config"), c);
            }
        }
        if (!a.containsKey("baseline")) {
            throw new IOException(abl + ": no 'baseline' config to compare against");
        }

        StringBuilder sb = new StringBuilder("window.PERF_DATA = {\n\"bench\":{");
        boolean first = true;
        for (Map.Entry<String, TreeMap<Integer, List<Double>>> e : b.entrySet()) {
            sb.append(first ? "" : ",").append("\n\"").append(e.getKey()).append("\":[");
            boolean f2 = true;
            for (Map.Entry<Integer, List<Double>> q : e.getValue().entrySet()) {
                List<Double> v = q.getValue();
                sb.append(f2 ? "" : ",").append("{\"p\":").append(q.getKey())
                        .append(",\"median\":").append(f(median(v), 2))
                        .append(",\"min\":").append(f(Collections.min(v), 2))
                        .append(",\"max\":").append(f(Collections.max(v), 2))
                        .append(",\"runs\":").append(v.size()).append('}');
                f2 = false;
            }
            sb.append(']');
            first = false;
        }
        double base = 0;
        {
            List<Double> s = new ArrayList<>();
            for (double[] r : a.get("baseline")) {
                s.add(r[0]);
            }
            base = median(s);
        }
        sb.append("\n},\n\"ablation\":[");
        first = true;
        for (Map.Entry<String, List<double[]>> e : a.entrySet()) {
            List<Double> t = new ArrayList<>(), cpu = new ArrayList<>(), mem = new ArrayList<>();
            for (double[] r : e.getValue()) {
                t.add(r[0]);
                cpu.add(r[1]);
                mem.add(r[2]);
            }
            double m = median(t);
            sb.append(first ? "" : ",").append("\n{\"config\":\"").append(e.getKey())
                    .append("\",\"median\":").append(f(m, 2))
                    .append(",\"rel\":").append(f(m / base, 3))
                    .append(",\"cpu\":").append(f(median(cpu), 0))
                    .append(",\"memGB\":").append(f(median(mem), 2))
                    .append(",\"runs\":").append(t.size())
                    .append(",\"check\":\"").append(check.getOrDefault(e.getKey(), "")).append("\"}");
            first = false;
            System.out.printf("ablation %-18s median %6.1f s  %.2fx  runs %d%n", e.getKey(), m, m / base, t.size());
        }
        sb.append("\n],\n\"excluded\":[");
        for (int i = 0; i < dropped.size(); i++) {
            sb.append(i == 0 ? "" : ",").append('"').append(dropped.get(i)).append('"');
        }
        sb.append("]\n};\n");
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out, StandardCharsets.UTF_8))) {
            w.print(sb);
        }
        for (Map.Entry<String, TreeMap<Integer, List<Double>>> e : b.entrySet()) {
            StringBuilder line = new StringBuilder("bench " + e.getKey() + ":");
            for (Map.Entry<Integer, List<Double>> q : e.getValue().entrySet()) {
                line.append(String.format(Locale.ROOT, "  p%d %.1f s", q.getKey(), median(q.getValue())));
            }
            System.out.println(line);
        }
        System.out.println("excluded: " + (dropped.isEmpty() ? "none" : String.join(", ", dropped)) + " -> " + out);
    }
}
