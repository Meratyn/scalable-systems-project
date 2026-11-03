package ema;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Line parser. parse() is the fast version used by the job: it scans for the
 * four needed fields (symbol=0, Last=21, Trading time=23, Trading date=26)
 * instead of splitting the whole very wide row, and parses the timestamp by
 * hand. parseReference() is the original straightforward version, kept so the
 * two can be compared on any input; they must always agree.
 */
public final class Parser {
    private Parser() { }

    /**
     * Event = (symbol, exchange, last_price, ts_millis). Null when the line is not a price event.
     *
     * A price event has a non-zero Last and a Trading time. Its date is the Trading date; index
     * rows and Frankfurt (ETR) rows have no Trading date, so when fallbackToSystemDate is true
     * the system Date (column 3) is used instead. With false, a row without Trading date is
     * dropped (the behaviour of the first version of the job).
     */
    public static Tuple4<String, String, Double, Long> parse(String line, boolean fallbackToSystemDate) {
        if (line.startsWith("#") || line.startsWith("ID,")) {
            return null;
        }
        line = line.trim();
        int n = line.length();
        int field = 0;
        int start = 0;
        String symbol = null;
        String systemDate = null;
        String last = null;
        String tradingTime = null;
        String tradingDate = null;
        boolean complete = false;
        for (int i = 0; i <= n; i++) {
            if (i == n || line.charAt(i) == ',') {
                switch (field) {
                    case 0: symbol = line.substring(start, i); break;
                    case 2: systemDate = line.substring(start, i); break;
                    case 21: last = line.substring(start, i); break;
                    case 23: tradingTime = line.substring(start, i); break;
                    case 26: tradingDate = line.substring(start, i); break;
                    default: break;
                }
                if (field == 26) {
                    complete = true;
                    break;
                }
                field++;
                start = i + 1;
            }
        }
        if (!complete) {
            return null; // fewer than 27 fields
        }
        String date = (tradingDate.isEmpty() && fallbackToSystemDate) ? systemDate : tradingDate;
        if (last.isEmpty() || tradingTime.isEmpty() || date.isEmpty()) {
            return null;
        }
        try {
            double price = Double.parseDouble(last);
            if (price == 0.0) {
                return null;
            }
            long ts = parseMillis(date, tradingTime);
            int dot = symbol.lastIndexOf('.');
            String exchange = dot >= 0 ? symbol.substring(dot + 1) : symbol;
            return Tuple4.of(symbol, exchange, price, ts);
        } catch (RuntimeException e) {
            return null;
        }
    }

    // ---------- timestamp ----------

    /** Fast path for the normal fixed-width format, otherwise the reference parser. */
    static long parseMillis(String date, String time) {
        int tl = time.length();
        if (date.length() == 10 && date.charAt(2) == '-' && date.charAt(5) == '-'
                && tl >= 8 && time.charAt(2) == ':' && time.charAt(5) == ':'
                && (tl == 8 || time.charAt(8) == '.')) {
            int day = d2(date, 0);
            int month = d2(date, 3);
            int year = d2(date, 6) * 100 + d2(date, 8);
            int hh = d2(time, 0);
            int mm = d2(time, 3);
            int ss = d2(time, 6);
            int f0 = tl > 9 ? digit(time.charAt(9)) : 0;
            int f1 = tl > 10 ? digit(time.charAt(10)) : 0;
            int f2 = tl > 11 ? digit(time.charAt(11)) : 0;
            boolean ok = day >= 1 && day <= 28 && month >= 1 && month <= 12
                    && d2(date, 6) >= 0 && d2(date, 8) >= 0
                    && hh >= 0 && hh < 24 && mm >= 0 && mm < 60 && ss >= 0 && ss < 60
                    && f0 >= 0 && f1 >= 0 && f2 >= 0;
            if (ok) {
                long days = epochDay(year, month, day);
                return ((days * 24 + hh) * 60 + mm) * 60_000L + ss * 1000L
                        + f0 * 100 + f1 * 10 + f2;
            }
        }
        return parseMillisReference(date, time);
    }

    private static int digit(char c) {
        return (c >= '0' && c <= '9') ? c - '0' : -1;
    }

    private static int d2(String s, int i) {
        int a = digit(s.charAt(i));
        int b = digit(s.charAt(i + 1));
        return (a < 0 || b < 0) ? -1 : a * 10 + b;
    }

    /** Days since 1970-01-01 for a proleptic Gregorian date (day already validated as 1..28). */
    private static long epochDay(int y, int m, int d) {
        long yy = m <= 2 ? y - 1 : y;
        long era = Math.floorDiv(yy, 400);
        long yoe = yy - era * 400;
        long doy = (153L * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
        long doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        return era * 146097 + doe - 719468;
    }

    // ---------- original reference implementation (also the fallback) ----------

    static long parseMillisReference(String date, String time) {
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

    /** The straightforward split-based version of parse(), for comparison. */
    public static Tuple4<String, String, Double, Long> parseReference(String line, boolean fallbackToSystemDate) {
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
        String date = (tradingDate.isEmpty() && fallbackToSystemDate) ? parts[2] : tradingDate;
        if (last.isEmpty() || tradingTime.isEmpty() || date.isEmpty()) {
            return null;
        }
        try {
            double price = Double.parseDouble(last);
            if (price == 0.0) {
                return null;
            }
            long ts = parseMillisReference(date, tradingTime);
            int dot = symbol.lastIndexOf('.');
            String exchange = dot >= 0 ? symbol.substring(dot + 1) : symbol;
            return Tuple4.of(symbol, exchange, price, ts);
        } catch (RuntimeException e) {
            return null;
        }
    }
}
