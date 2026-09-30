from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

ds = env.from_collection(collection=[1, 2, 3, 4, 5])
ds.map(lambda x: x * 10).print()

env.execute("smoke-test")