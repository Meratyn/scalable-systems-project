"""
CS-E4780 course project: EMA38/EMA100 per symbol over 5-minute tumbling
windows (Query 1) and BUY/SELL crossover advisories (Query 2).

Usage (all options optional):
    python job.py --input data/small --output output/p1 --parallelism 1
"""
import argparse
from datetime import datetime, timezone

from pyflink.common import WatermarkStrategy, Time, Duration, Configuration
from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (StreamExecutionEnvironment,
                                ProcessWindowFunction, OutputTag)
from pyflink.datastream.connectors.file_system import (
    FileSource, StreamFormat, FileSink, OutputFileConfig)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

def event_type():
    # (symbol, exchange, last_price, ts_millis). A fresh object on every call:
    # PyFlink caches a Java handle inside a TypeInformation once it is used,
    # and such an object can no longer be pickled.
    return Types.TUPLE(
        [Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.LONG()])


def result_type():
    # (symbol, window_end_ts, ema38, ema100, advisory)
    return Types.TUPLE(
        [Types.STRING(), Types.LONG(), Types.DOUBLE(), Types.DOUBLE(),
         Types.STRING()])


# ---------- Step A: parse a raw CSV line into a clean price event ----------
def parse(line: str):
    """Return (symbol, exchange, last_price, ts_millis) or None.

    A row is a price event only if Last, Trading time and Trading date are
    all filled in. Last == 0 is a housekeeping/reset row, not a trade.
    """
    if line.startswith("#") or line.startswith("ID,"):
        return None
    parts = line.strip().split(",")
    if len(parts) < 27:
        return None
    symbol, last, trading_time, trading_date = (
        parts[0], parts[21], parts[23], parts[26])
    if last == "" or trading_time == "" or trading_date == "":
        return None
    try:
        price = float(last)
        if price == 0.0:
            return None
        # DD-MM-YYYY + HH:MM:SS.ssss -> epoch millis. The CEST wall-clock time
        # is used as-is (no timezone shift), so 5-minute windows line up with
        # local midnight as the spec requires.
        dt = datetime.strptime(
            f"{trading_date} {trading_time}", "%d-%m-%Y %H:%M:%S.%f"
        ).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return (symbol, symbol.split(".")[-1], price, int(dt.timestamp() * 1000))


class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_timestamp):
        return event[3]


def pick_latest(a, b):
    """Incremental reduce: keep only the event with the latest trading
    timestamp, so each (symbol, window) holds ONE event instead of all of
    them. On equal timestamps the later arrival wins."""
    return b if b[3] >= a[3] else a


# ---------- Step B: stateful EMA + crossover per symbol per window ----------
class EmaCrossoverFunction(ProcessWindowFunction):
    """Runs once per (symbol, window) when the window closes.

    Decisions (see report):
      * Close = the price event with the LATEST trading timestamp in the
        window, not the last one to arrive (arrival order is not reliable).
      * EMA is seeded with the first real price of a symbol (professor
        confirmed this is allowed), so EMA38 == EMA100 in the first window.
      * Windows with no price event for a symbol are skipped: EMA is not
        updated and no crossover is evaluated for them (a stated assumption).
      * Crossover rules follow the spec equations (1)-(4) exactly.
    """
    ALPHA38 = 2.0 / (1 + 38)
    ALPHA100 = 2.0 / (1 + 100)

    def __init__(self, strict=False):
        # strict=False: spec equations literally (previous EMAs may be equal).
        # strict=True: a crossover needs the previous EMAs to differ, so the
        # equal EMAs right after seeding cannot trigger an advisory.
        self.strict = strict

    def open(self, runtime_context):
        self.ema38_state = runtime_context.get_state(
            ValueStateDescriptor("ema38", Types.DOUBLE()))
        self.ema100_state = runtime_context.get_state(
            ValueStateDescriptor("ema100", Types.DOUBLE()))

    def process(self, key, context, elements):
        close = None
        for e in elements:
            if close is None or e[3] >= close[3]:
                close = e
        if close is None:
            return
        price = close[2]

        prev38 = self.ema38_state.value()
        prev100 = self.ema100_state.value()

        advisory = ""
        if prev38 is None:
            ema38 = ema100 = price  # first window of this symbol: seed
        else:
            ema38 = price * self.ALPHA38 + prev38 * (1 - self.ALPHA38)
            ema100 = price * self.ALPHA100 + prev100 * (1 - self.ALPHA100)
            if self.strict:
                was_below, was_above = prev38 < prev100, prev38 > prev100
            else:
                was_below, was_above = prev38 <= prev100, prev38 >= prev100
            if ema38 > ema100 and was_below:
                advisory = "BUY"
            elif ema38 < ema100 and was_above:
                advisory = "SELL"

        self.ema38_state.update(ema38)
        self.ema100_state.update(ema100)
        yield (key, context.window().end, ema38, ema100, advisory)


def csv_sink(path):
    return (FileSink.for_row_format(path, Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder().with_part_suffix(".csv").build())
            .build())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/raw_test/")
    ap.add_argument("--output", default="output")
    ap.add_argument("--parallelism", type=int, default=1)
    ap.add_argument("--tolerance-hours", type=float, default=200.0,
                    help="watermark out-of-orderness tolerance. The default "
                         "(200h, longer than the dataset) means no event can "
                         "be late, so results do not depend on file read "
                         "order or parallelism; windows then fire when the "
                         "input ends. Use e.g. 2 for streaming-like behaviour "
                         "(events from days read out of order may be dropped).")
    ap.add_argument("--strict-crossover", action="store_true",
                    help="require the previous EMA38 and EMA100 to differ "
                         "before a crossover can fire")
    ap.add_argument("--late-output", action="store_true",
                    help="also write events that arrive after their window "
                         "fired to <output>/late (diagnostic)")
    args = ap.parse_args()
    print(f"input={args.input} output={args.output} "
          f"parallelism={args.parallelism} tolerance_h={args.tolerance_hours} "
          f"strict={args.strict_crossover}")

    config = Configuration()
    config.set_string("heartbeat.timeout", "180000")
    config.set_string("heartbeat.interval", "20000")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(args.parallelism)

    source = FileSource.for_record_stream_format(
        StreamFormat.text_line_format(), args.input).build()
    raw_lines = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "csv-source")

    events = (raw_lines
              .map(parse, output_type=event_type())
              .filter(lambda x: x is not None))

    watermarked = events.assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_bounded_out_of_orderness(
            Duration.of_seconds(int(args.tolerance_hours * 3600)))
        .with_timestamp_assigner(EventTimestampAssigner()))

    windowed = (watermarked
                .key_by(lambda e: e[0])
                .window(TumblingEventTimeWindows.of(Time.minutes(5))))

    # Optional diagnostic: events that arrive after their window already fired
    # are normally dropped by Flink; with --late-output they are written out
    # so they can be counted per run.
    late_tag = None
    if args.late_output:
        late_tag = OutputTag("late-events", event_type())
        windowed = windowed.side_output_late_data(late_tag)

    result = windowed.reduce(
        pick_latest,
        window_function=EmaCrossoverFunction(strict=args.strict_crossover),
        output_type=result_type())

    result.map(lambda r: f"{r[0]},{r[1]},{r[2]},{r[3]},{r[4]}",
               output_type=Types.STRING()).sink_to(
        csv_sink(f"{args.output}/results"))

    if late_tag is not None:
        result.get_side_output(late_tag).map(
            lambda e: f"{e[0]},{e[3]},{e[2]}",
            output_type=Types.STRING()).sink_to(
            csv_sink(f"{args.output}/late"))

    env.execute("ema-crossover")


if __name__ == "__main__":
    main()
