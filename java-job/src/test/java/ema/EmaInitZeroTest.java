package ema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * --ema-init zero versus the default --ema-init first, on one symbol with three windows
 * (prices 100, 120, 40; 8 Nov 2021 08:00-08:15 UTC). Expected values computed by hand with
 * a38 = 2/39, a100 = 2/101, EMA = P*a + prev*(1-a):
 *
 *   zero:  w0 EMA38 = 100*2/39 = 5.128205128205128, EMA100 = 100*2/101 = 1.9801980198019802;
 *          previous EMAs both 0 (equal) and now EMA38 > EMA100 -> BUY (literal rule);
 *          w1 EMA38 = 120*2/39 + 5.1282*37/39 = 11.019066403681787,
 *             EMA100 = 120*2/101 + 1.9802*99/101 = 4.31722380158808 (no signal);
 *          w2 EMA38 = 12.5052681265699, EMA100 = 5.0238134292794046 (no signal).
 *          With --strict-crossover the first window has no signal (previous EMAs equal).
 *   first: w0 both EMAs = 100 (seed), no signal;
 *          w1 EMA38 = 120*2/39 + 100*37/39 = 101.02564102564102 > EMA100 = 120*2/101 + 100*99/101
 *             = 100.3960396039604, previous EMAs equal -> BUY (literal equal-start signal);
 *          w2 EMA38 = 97.89612097304405 < EMA100 = 99.20007842368396 -> SELL (real crossing).
 * (A repeated price is avoided: 100*a + 100*(1-a) rounds to 99.99999999999999.)
 */
class EmaInitZeroTest {

    static final long W0_END = 1636358700000L; // 2021-11-08 08:05 UTC

    @TempDir
    Path tmp;

    Path writeInput() throws Exception {
        List<String> lines = new ArrayList<>();
        lines.add(JobCorrectnessTest.row("ZI.FR", "08-11-2021", "100.000000", "08:01:00.000", "08-11-2021"));
        lines.add(JobCorrectnessTest.row("ZI.FR", "08-11-2021", "120.000000", "08:06:00.000", "08-11-2021"));
        lines.add(JobCorrectnessTest.row("ZI.FR", "08-11-2021", "40.000000", "08:11:00.000", "08-11-2021"));
        Path dir = tmp.resolve("in");
        Files.createDirectories(dir);
        Files.write(dir.resolve("day.csv"), lines, StandardCharsets.UTF_8);
        return dir;
    }

    /** Runs the job and returns the result lines sorted by window end. */
    List<String[]> run(Path in, String name, String... extra) throws Exception {
        Path out = tmp.resolve(name);
        List<String> args = new ArrayList<>(Arrays.asList("--input", in.toString(), "--output", out.toString()));
        args.addAll(Arrays.asList(extra));
        Job.main(args.toArray(new String[0]));
        List<String[]> rows = new ArrayList<>();
        for (String line : JobCorrectnessTest.csvLines(out.resolve("results"))) {
            rows.add(line.split(",", -1));
        }
        rows.sort((a, b) -> Long.compare(Long.parseLong(a[1]), Long.parseLong(b[1])));
        assertEquals(3, rows.size());
        for (int w = 0; w < 3; w++) {
            assertEquals(String.valueOf(W0_END + w * 300_000L), rows.get(w)[1], "window end " + w);
        }
        return rows;
    }

    static void check(String[] row, double e38, double e100, String adv) {
        assertEquals(e38, Double.parseDouble(row[2]), 1e-12, "EMA38 " + String.join(",", row));
        assertEquals(e100, Double.parseDouble(row[3]), 1e-12, "EMA100 " + String.join(",", row));
        assertEquals(adv, row[4], "advisory " + String.join(",", row));
    }

    @Test
    void zeroStartGivesBuyInFirstWindow() throws Exception {
        Path in = writeInput();
        for (String p : new String[] {"1", "2"}) {
            List<String[]> r = run(in, "zero_p" + p, "--ema-init", "zero", "--parallelism", p);
            check(r.get(0), 5.128205128205128, 1.9801980198019802, "BUY");
            check(r.get(1), 11.019066403681787, 4.31722380158808, "");
            check(r.get(2), 12.5052681265699, 5.0238134292794046, "");
        }
    }

    @Test
    void zeroStartStrictGivesNoFirstWindowSignal() throws Exception {
        List<String[]> r = run(writeInput(), "zero_strict", "--ema-init", "zero", "--strict-crossover");
        check(r.get(0), 5.128205128205128, 1.9801980198019802, "");
    }

    @Test
    void firstPriceSeedIsTheDefault() throws Exception {
        Path in = writeInput();
        for (List<String[]> r : Arrays.asList(run(in, "default"), run(in, "first", "--ema-init", "first"))) {
            check(r.get(0), 100, 100, "");
            check(r.get(1), 101.02564102564102, 100.3960396039604, "BUY");
            check(r.get(2), 97.89612097304405, 99.20007842368396, "SELL");
        }
    }
}
