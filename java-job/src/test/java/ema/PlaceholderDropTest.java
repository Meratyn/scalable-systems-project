package ema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * --drop-placeholder-time drops exactly the rows with an empty Trading date AND Trading time
 * 01:00:00.000 (the dirty index rows of 10 Nov 2021); nothing else.
 *
 *   PH.FR     placeholder row (empty date, 01:00:00.000, system date 10-11-2021, price 500)
 *             + a normal row at 09:01 (price 100): with the option only the 09:05 window
 *             remains and it seeds at 100.
 *   CTRL1.FR  empty date but 01:00:00.001 -> kept.
 *   CTRL2.FR  Trading date set and 01:00:00.000 -> kept.
 */
class PlaceholderDropTest {

    static final long W0105 = 1636506300000L; // 2021-11-10 01:05 UTC
    static final long W0905 = 1636535100000L; // 2021-11-10 09:05 UTC

    @TempDir
    Path tmp;

    Path writeInput() throws Exception {
        String d = "10-11-2021";
        List<String> lines = new ArrayList<>();
        lines.add(JobCorrectnessTest.row("PH.FR", d, "100.000000", "09:01:00.000", d));
        lines.add(JobCorrectnessTest.row("CTRL1.FR", d, "20.000000", "01:00:00.001", ""));
        lines.add(JobCorrectnessTest.row("CTRL2.FR", d, "30.000000", "01:00:00.000", d));
        lines.add(JobCorrectnessTest.row("PH.FR", d, "500.000000", "01:00:00.000", "")); // published late
        Path dir = tmp.resolve("in");
        Files.createDirectories(dir);
        Files.write(dir.resolve("day.csv"), lines, StandardCharsets.UTF_8);
        return dir;
    }

    /** symbol,window_end -> "ema38,ema100,advisory" */
    Map<String, String> run(Path in, String name, String... extra) throws Exception {
        Path out = tmp.resolve(name);
        List<String> args = new ArrayList<>(Arrays.asList("--input", in.toString(), "--output", out.toString()));
        args.addAll(Arrays.asList(extra));
        Job.main(args.toArray(new String[0]));
        Map<String, String> res = new HashMap<>();
        for (String line : JobCorrectnessTest.csvLines(out.resolve("results"))) {
            String[] p = line.split(",", 3);
            res.put(p[0] + "," + p[1], p[2]);
        }
        return res;
    }

    @Test
    void defaultKeepsThePlaceholderRow() throws Exception {
        Map<String, String> r = run(writeInput(), "default");
        assertEquals(4, r.size());
        assertEquals("500.0,500.0,", r.get("PH.FR," + W0105), "placeholder window seeds PH.FR at 500");
        assertTrue(r.containsKey("PH.FR," + W0905));
    }

    @Test
    void optionDropsOnlyThePlaceholderRow() throws Exception {
        Path in = writeInput();
        for (String p : new String[] {"1", "2"}) {
            Map<String, String> r = run(in, "drop_p" + p, "--drop-placeholder-time", "--parallelism", p);
            assertEquals(3, r.size(), "parallelism " + p);
            assertFalse(r.containsKey("PH.FR," + W0105), "placeholder window dropped");
            assertEquals("100.0,100.0,", r.get("PH.FR," + W0905), "PH.FR now seeds at its real first price");
            assertEquals("20.0,20.0,", r.get("CTRL1.FR," + W0105), "01:00:00.001 with empty date is kept");
            assertEquals("30.0,30.0,", r.get("CTRL2.FR," + W0105), "01:00:00.000 with a Trading date is kept");
        }
    }

    @Test
    void parserAgreesWithReferenceParser() {
        String d = "10-11-2021";
        String[] rows = {
            JobCorrectnessTest.row("PH.FR", d, "500.000000", "01:00:00.000", ""),
            JobCorrectnessTest.row("CTRL1.FR", d, "20.000000", "01:00:00.001", ""),
            JobCorrectnessTest.row("CTRL2.FR", d, "30.000000", "01:00:00.000", d),
        };
        for (String row : rows) {
            for (boolean drop : new boolean[] {false, true}) {
                assertEquals(Parser.parseReference(row, true, drop), Parser.parse(row, true, drop), row + " drop=" + drop);
            }
        }
        assertEquals(null, Parser.parse(rows[0], true, true));
        assertFalse(Parser.parse(rows[0], true, false) == null);
    }
}
