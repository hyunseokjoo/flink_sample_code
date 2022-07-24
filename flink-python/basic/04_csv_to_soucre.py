"""
CSV파일 Table로 만들기 순서 
1. Table환경 만들기 
2. Source정보로 Soucrce만들기 
3. Source를 테이블환경에 등록하기 
4. 테이블 환경에서 테이블 가져와 활용하기
"""
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes,CsvTableSource

batch_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
batch_table_new = TableEnvironment.create(batch_settings)

field_names = ["framework", "chapter"]
field_types = [DataTypes.STRING(), DataTypes.BIGINT()]

# CSV 파일 Source로 만들기 
source = CsvTableSource(
    "./sample.csv",
    field_names,
    field_types,
    ignore_first_line=False,
)

# Source를 chapters라는 테이블로 테이블 환경에 만들기 
table_env.register_table_source("chapters", source)
# 테이블 환경에서 Chapters라는 테이블 불러와 table로 만들기
table = table_env.from_path("chapters")
# 출력
print(table.to_pandas())
