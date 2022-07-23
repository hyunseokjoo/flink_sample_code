from pyflink.table import EnvironmentSettings, TableEnvironment

# batch 
batch_settings = EnvironmentSettings.new_instance()\
                                    .in_batch_mode()\
                                    .use_blink_planner().build()

batch_table_new = TableEnvironment.create(batch_settings)


# stream 
stream_settings = EnvironmentSettings.new_instance()\
                                    .in_streaming_mode()\
                                    .user_blink_planner().build()
                                    
stream_table_env = TableEnvironment.create(stream_settings)