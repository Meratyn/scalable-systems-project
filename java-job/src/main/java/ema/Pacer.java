package ema;

import java.util.concurrent.locks.LockSupport;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Paced replay (--replay-speed S). Runs chained right after the parser, so each reader paces its
 * own events. The first event is released at once and fixes the start (wall clock) and the virtual
 * clock (its event time). For every later event, the virtual elapsed time grows by the forward
 * step of the largest event time seen so far, with jumps over 10 minutes counted as 10 minutes
 * (night gaps, the 01:00 placeholder rows). The event is held until
 *     wall clock >= start + virtual_elapsed / S.
 * Backward (out-of-order) events do not move the clock and are released at once.
 * stamp=true (--latency-log): the arrival stamp f4 is renewed at release, so the deliberate wait
 * is not counted as latency.
 */
public class Pacer implements MapFunction<Tuple5<String, String, Double, Long, Long>,
        Tuple5<String, String, Double, Long, Long>> {
    static final long MAX_JUMP_MS = 10 * 60_000L;

    private final double speed;
    private final boolean stamp;
    private transient boolean started;
    private transient long startNanos;
    private transient long maxEventTime;
    private transient long virtualMs; // virtual elapsed event time, capped jumps

    public Pacer(double speed, boolean stamp) {
        if (!(speed > 0)) {
            throw new IllegalArgumentException("--replay-speed must be > 0, got " + speed);
        }
        this.speed = speed;
        this.stamp = stamp;
    }

    /** Virtual elapsed time after an event with event time t (exposed for the unit test). */
    long advance(long t) {
        if (!started) {
            started = true;
            startNanos = System.nanoTime();
            maxEventTime = t;
            virtualMs = 0;
        } else if (t > maxEventTime) {
            virtualMs += Math.min(t - maxEventTime, MAX_JUMP_MS);
            maxEventTime = t;
        }
        return virtualMs;
    }

    @Override
    public Tuple5<String, String, Double, Long, Long> map(Tuple5<String, String, Double, Long, Long> e) {
        long v = advance(e.f3);
        long due = startNanos + (long) (v * 1_000_000.0 / speed);
        long wait;
        while ((wait = due - System.nanoTime()) > 0) {
            LockSupport.parkNanos(wait);
        }
        if (stamp) {
            e.f4 = System.nanoTime();
        }
        return e;
    }
}
