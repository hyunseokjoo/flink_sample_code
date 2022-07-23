"""
하드 코딩으로 table 만드는 법
1. Environment를 만든다.
2. TableEnvironment를 만든다.
3. Table환경으로 from_elements함수를 이용하여 소스를 만든다.
"""
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes

# 환경 만들기
settings = EnvironmentSettings.new_instance()\
                                .in_batch_mode()\
                                .use_blink_planner()\
                                .build()

# Table 환경 만들기
table_env = TableEnvironment.create(settings)

# 하드 코딩 데이터 
sample_data = [
    ("Spark", 1),
    ("Airflow", 2),
    ("Kafka", 3),
    ("Flink", 4)
]

# table 만들기 
tbl_1 = table_env.from_elements(sample_data)
print(tbl_1)
tbl_1.print_schema()

# dataFrame으로 만들기
df = tbl_1.to_pandas()

# schema를 이용하기 
col_names = ["framework", "chapter"]
tbl_2 = table_env.from_elements(sample_data, col_names)
print(tbl_2.to_pandas())

# DataType을 이용하여 만들기 
schema = DataTypes.ROW([
  DataTypes.FIELD("framework", DataTypes.STRING()),
  DataTypes.FIELD("chapter", DataTypes.BIGINT()),
])
src3 = table_env.from_elements(sample_data, schema)
print(src3.to_pandas())
