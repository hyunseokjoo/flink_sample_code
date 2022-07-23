from pyflink.table import EnvironmentSettings, TableEnvironment,Schema, DataTypes, TableDescriptor

t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
t_env.get_config().get_configuration().set_string("parallelism.default", "1")

input_path = "./sample.csv"

# 테이블 생성 - Source라는 명칭에 framework, chapter라는 컬럼을 가진 테이블
t_env.create_temporary_table(
  "source",  TableDescriptor.for_connector("filesystem")
                            .schema(Schema.new_builder()
                                          .column("framework", DataTypes.STRING())
                                          .column("chapter", DataTypes.INT())
                                          .build())
                            .option("path", input_path)
                            .format("csv")
                            .build())

# 환경에서 table가져오기
src = t_env.from_path("source")
print(src.to_pandas())