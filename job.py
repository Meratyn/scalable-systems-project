import re
from datetime import datetime, timezone

from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.common.serialization import Encoder
from pyflink.common import WatermarkStrategy, Time, Duration
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.watermark_strategy import TimestampAssigner

from pyflink.common import Configuration

config = Configuration()
config.set_string("heartbeat.timeout", "180000")
config.set_string("heartbeat.interval", "20000")

env = StreamExecutionEnvironment.get_execution_environment(config)
env.set_parallelism(2)

# ---------- Step A: parse a raw CSV line into a clean event ----------
def parse(line: str):
    if line.startswith("#") or line.startswith("ID,"):
        return None
    parts = line.strip().split(",")
    if len(parts) < 27:
        return None
    symbol = parts[0]
    last = parts[21]
    trading_time = parts[23]
    trading_date = parts[26]
    if last == "" or trading_time == "" or trading_date == "":
        return None
    if float(last) == 0.0:
        return None

    # DD-MM-YYYY + HH:MM:SS.ssss -> epoch millis (treat as-is, no TZ shift)
    dt = datetime.strptime(
        f"{trading_date} {trading_time}", "%d-%m-%Y %H:%M:%S.%f"
    ).replace(tzinfo=timezone.utc)
    ts_millis = int(dt.timestamp() * 1000)

    exchange = symbol.split(".")[-1]
    return (symbol, exchange, float(last), ts_millis)

# ---------- Step B: stateful EMA + crossover per symbol per window ----------
class EmaCrossoverFunction(ProcessWindowFunction):
    def open(self, runtime_context):
        self.ema38_state = runtime_context.get_state(
            ValueStateDescriptor("ema38", Types.FLOAT()))
        self.ema100_state = runtime_context.get_state(
            ValueStateDescriptor("ema100", Types.FLOAT()))
        # remember last window's relative position (38 above/below 100)
        self.prev_signal_state = runtime_context.get_state(
            ValueStateDescriptor("prev_signal", Types.STRING()))

    def process(self, key, context, elements):
        events = list(elements)
        if not events:
            return
        last_price = events[-1][2]  # last event in the window, by arrival order

        prev_ema38 = self.ema38_state.value()
        prev_ema100 = self.ema100_state.value()
        prev_ema38 = 0.0 if prev_ema38 is None else prev_ema38
        prev_ema100 = 0.0 if prev_ema100 is None else prev_ema100

        alpha38 = 2 / (1 + 38)
        alpha100 = 2 / (1 + 100)
        ema38 = last_price * alpha38 + prev_ema38 * (1 - alpha38)
        ema100 = last_price * alpha100 + prev_ema100 * (1 - alpha100)

        self.ema38_state.update(ema38)
        self.ema100_state.update(ema100)

        prev_signal = self.prev_signal_state.value()  # "above" / "below" / None
        signal = "above" if ema38 > ema100 else "below"

        advisory = ""
        if prev_signal == "below" and signal == "above":
            advisory = "BUY"
        elif prev_signal == "above" and signal == "below":
            advisory = "SELL"

        self.prev_signal_state.update(signal)

        window_end = context.window().end
        yield (key, window_end, ema38, ema100, advisory)

class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_timestamp):
        return event[3]

# ---------- Wiring it together ----------





source = FileSource.for_record_stream_format(
    StreamFormat.text_line_format(), "data/raw_test/"
).build()

raw_lines = env.from_source(source, WatermarkStrategy.no_watermarks(), "csv-source")

events = raw_lines.map(
    parse,
    output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT(), Types.LONG()])
).filter(lambda x: x is not None)

watermarked = events.assign_timestamps_and_watermarks(
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_hours(2))
    .with_timestamp_assigner(EventTimestampAssigner())
)
result = (
    watermarked
    .key_by(lambda e: e[0])
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .process(EmaCrossoverFunction(),
             output_type=Types.TUPLE([Types.STRING(), Types.LONG(), Types.FLOAT(), Types.FLOAT(), Types.STRING()]))
)

result.map(lambda r: f"{r[0]},{r[1]},{r[2]},{r[3]},{r[4]}", output_type=Types.STRING()) \
    .sink_to(
        FileSink.for_row_format("output/results", Encoder.simple_string_encoder())
        .with_output_file_config(OutputFileConfig.builder().with_part_suffix(".csv").build())
        .build()
    )
env.execute("ema-crossover")