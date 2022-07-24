import os
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.execution_mode import RuntimeExecutionMode 
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(execution_mode=RuntimeExecutionMode.STREAMING)
env.enable_checkpointing(1000)
env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
t_env = StreamTableEnvironment.create(env)

# config에 kafka connector 등록하기
kafka_jar_path = os.path.join(
  os.path.abspath(os.path.dirname(__file__)), "../",
  "flink-sql-connector-kafka_2.11-1.14.0.jar"
)
t_env.get_config().get_configuration().set_string(
  "pipeline.jars", f"file://{kafka_jar_path}"
)

# kafka를 바라보고 있는 source table 만들기
schema = SimpleStringSchema()
kafka_consumer = FlinkKafkaConsumer(
  topics="flink-test",
  deserialization_schema=schema,
  properties={
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test_group'
  })

# blackhole이라는 source table을 하나 만들기 
# 만든 곳으로 데이터 보내는 sink table 만들기
ds = env.add_source(kafka_consumer)
t_env.execute_sql("""
  CREATE TABLE blackhole (
    data STRING
  ) WITH (
    'connector' = 'blackhole'
  )
""")

table = t_env.from_data_stream(ds)
table.insert_into("blackhole")
t_env.execute("flink_kafka_consumer")