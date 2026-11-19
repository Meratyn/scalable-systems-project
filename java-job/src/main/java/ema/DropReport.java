package ema;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Why lines are not price events: counts every input line under exactly one reason, checked in
 * the same order as Parser.parse (system-date fallback on, the default):
 *
 *   comment_or_header   line starts with "#" or "ID,"
 *   short_row           fewer than 27 fields
 *   empty_last          Last is empty
 *   empty_trading_time  Last present, Trading time empty
 *   empty_date          Trading date and system date both empty
 *   bad_last            Last is not a number
 *   zero_last           Last = 0
 *   bad_timestamp       date/time cannot be parsed
 *   price_event         kept (must equal the job's price events)
 * plus, among price events: empty_trading_date (system-date fallback used) and
 * placeholder (empty Trading date and Trading time 01:00:00.000).
 * As a check, Parser.parse is also called on every line; its count must equal price_event.
 *
 *   java -cp target/ema-crossover-1.0.jar ema.DropReport ../data/raw
 */
public class DropReport {
    public static void main(String[] args) throws IOException {
        List<Path> files;
        try (Stream<Path> s = Files.list(Paths.get(args[0]))) {
            files = s.filter(p -> p.getFileName().toString().endsWith(".csv")).sorted().collect(Collectors.toList());
        }
        String[] names = {"comment_or_header", "short_row", "empty_last", "empty_trading_time", "empty_date",
            "bad_last", "zero_last", "bad_timestamp", "price_event"};
        long[] n = new long[names.length];
        long lines = 0;
        long emptyTradingDate = 0;
        long placeholder = 0;
        long parserEvents = 0;
        long t0 = System.currentTimeMillis();
        for (Path f : files) {
            try (BufferedReader r = Files.newBufferedReader(f, StandardCharsets.ISO_8859_1)) {
                String line;
                while ((line = r.readLine()) != null) {
                    lines++;
                    if (Parser.parse(line, true) != null) {
                        parserEvents++;
                    }
                    int reason = classify(line);
                    n[reason]++;
                    if (reason == 8) {
                        String[] p = line.trim().split(",", -1);
                        if (p[26].isEmpty()) {
                            emptyTradingDate++;
                            if (p[23].equals("01:00:00.000")) {
                                placeholder++;
                            }
                        }
                    }
                }
            }
            System.err.printf("%s done, %.0f s%n", f.getFileName(), (System.currentTimeMillis() - t0) / 1000.0);
        }
        System.out.println("reason,lines,share_of_all_lines");
        for (int i = 0; i < names.length; i++) {
            System.out.printf("%s,%d,%.4f%n", names[i], n[i], 100.0 * n[i] / lines);
        }
        System.out.printf("total,%d,100.0000%n", lines);
        System.out.printf("price_event_with_empty_trading_date,%d,%.4f%n", emptyTradingDate, 100.0 * emptyTradingDate / lines);
        System.out.printf("price_event_placeholder_0100,%d,%.4f%n", placeholder, 100.0 * placeholder / lines);
        System.out.printf("check_parser_events,%d,%s%n", parserEvents, parserEvents == n[8] ? "MATCH" : "MISMATCH");
    }

    /** Index into names; mirrors Parser.parse(line, true) step by step. */
    static int classify(String line) {
        if (line.startsWith("#") || line.startsWith("ID,")) {
            return 0;
        }
        String[] p = line.trim().split(",", -1);
        if (p.length < 27) {
            return 1;
        }
        String last = p[21];
        String time = p[23];
        String date = p[26].isEmpty() ? p[2] : p[26];
        if (last.isEmpty()) {
            return 2;
        }
        if (time.isEmpty()) {
            return 3;
        }
        if (date.isEmpty()) {
            return 4;
        }
        double price;
        try {
            price = Double.parseDouble(last);
        } catch (RuntimeException e) {
            return 5;
        }
        if (price == 0.0) {
            return 6;
        }
        try {
            Parser.parseMillisReference(date, time);
        } catch (RuntimeException e) {
            return 7;
        }
        return 8;
    }
}
