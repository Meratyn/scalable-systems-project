from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Time
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy

# reuse the exact same EmaCrossoverFunction and EventTimestampAssigner from job.py
exec(open("job.py").read().split("# ---------- Wiring it together ----------")[0])

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Symbol TEST.FR: price rises steadily for 40 windows, then crashes hard for 40 more.
# This should force EMA38 above EMA100 early (bullish), then flip below (bearish) later.
base_ts = 1636362000000  # arbitrary start
synthetic = []
price = 10.0
for i in range(40):
    price += 2.0
    synthetic.append(("TEST.FR", "FR", price, base_ts + i * 5 * 60 * 1000))
for i in range(40, 80):
    price -= 5.0
    synthetic.append(("TEST.FR", "FR", max(price, 0.01), base_ts + i * 5 * 60 * 1000))

ds = env.from_collection(
    synthetic,
    type_info=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT(), Types.LONG()])
)

watermarked = ds.assign_timestamps_and_watermarks(
    WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
    .with_timestamp_assigner(EventTimestampAssigner())
)

result = (
    watermarked
    .key_by(lambda e: e[0])
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .process(EmaCrossoverFunction(),
             output_type=Types.TUPLE([Types.STRING(), Types.LONG(), Types.FLOAT(), Types.FLOAT(), Types.STRING()]))
)

result.print()
env.execute("synthetic-crossover-test")