package ema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.java.tuple.Tuple5;
import org.junit.jupiter.api.Test;

/** --replay-speed: virtual clock with capped jumps, release time, renewed arrival stamp. */
class PacerTest {

    static Tuple5<String, String, Double, Long, Long> ev(long t) {
        return Tuple5.of("X.FR", "FR", 1.0, t, 0L);
    }

    @Test
    void virtualClockCapsJumpsAndIgnoresBackwardEvents() {
        Pacer p = new Pacer(1.0, false);
        long t0 = 1_636_617_600_000L;
        assertEquals(0, p.advance(t0));                       // first event fixes the clock
        assertEquals(30_000, p.advance(t0 + 30_000));         // +30 s
        assertEquals(30_000, p.advance(t0 + 10_000));         // backward: clock does not move
        assertEquals(30_000, p.advance(t0 + 30_000));         // equal: no move
        assertEquals(30_000 + 600_000, p.advance(t0 + 30_000 + 3_600_000)); // 1 h jump counts as 10 min
        assertEquals(30_000 + 600_000 + 600_000,
                p.advance(t0 + 30_000 + 3_600_000 + 600_000)); // exactly 10 min: not capped
    }

    @Test
    void releasesNoEarlierThanStartPlusVirtualOverSpeed() {
        Pacer p = new Pacer(10.0, false);                     // 2 s virtual -> 200 ms wall
        long t0 = 1_636_617_600_000L;
        long start = System.nanoTime();
        p.map(ev(t0));
        assertTrue(System.nanoTime() - start < 100_000_000L, "first event released at once");
        p.map(ev(t0 + 2_000));
        long ms = (System.nanoTime() - start) / 1_000_000;
        assertTrue(ms >= 200, "held until start + 2 s / 10, waited " + ms + " ms");
        assertTrue(ms < 1_000, "not held much longer, waited " + ms + " ms");
    }

    @Test
    void renewsArrivalStampOnlyWhenProbeIsOn() {
        long t0 = 1_636_617_600_000L;
        Tuple5<String, String, Double, Long, Long> off = new Pacer(10.0, false).map(ev(t0));
        assertEquals(0L, off.f4);
        long before = System.nanoTime();
        Pacer on = new Pacer(10.0, true);
        on.map(ev(t0));
        Tuple5<String, String, Double, Long, Long> e = on.map(ev(t0 + 1_000)); // waits ~100 ms
        assertTrue(e.f4 - before >= 100_000_000L, "stamp taken at release, after the wait");
    }

    @Test
    void rejectsNonPositiveSpeed() {
        assertThrows(IllegalArgumentException.class, () -> new Pacer(0, false));
        assertThrows(IllegalArgumentException.class, () -> new Pacer(-1, false));
    }
}
