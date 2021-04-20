from pyspark.sql.functions import col, date_trunc

# define streams receiving signals from the SendSignalTCP programs

df_stream1 = spark.readStream.format("socket").option("host", "127.0.0.1").option("port", REPLACE_BY_PORT_NUMBER_1).option("includeTimestamp","true").load()
df_stream2 = spark.readStream.format("socket").option("host", "127.0.0.1").option("port", REPLACE_BY_PORT_NUMBER_2).option("includeTimestamp","true").load()

# apply transformation to data streams : truc date to the second
# the purpose of this operation is to make time aggregation and join possible by second

df_stream1 = df_stream1.select(df_stream1.value, date_trunc('second', df_stream1.timestamp).alias("timestamp"))
df_stream2 = df_stream2.select(df_stream2.value, date_trunc('second', df_stream2.timestamp).alias("timestamp"))

# combine both streaming dataframes 

df_stream3 = df_stream1.join(df_stream2,df_stream1.timestamp ==  df_stream2.timestamp,"inner")

# start queries, in reverse order of appearance, opening the sinks from the bottom up
# CAUTION : 
#   - write paths default to hdfs and should be previously created through hdfs commands and having the right permissions
#   - checkpoint locations should as well be created upfront, and cleaned up before every relaunch of the streams
#   - if checkpoint folders are not cleaned up, the streams will try to pick up where they left and you will have an error message

query3 = df_stream3.writeStream.format("csv").option("path", "/user/root/data/signal_c").option("checkpointLocation", "/user/root/data/check3").start()
query2 = df_stream2.writeStream.format("csv").option("path", "/user/root/data/signal_2").option("checkpointLocation", "/user/root/data/check2").start()
query1 = df_stream1.writeStream.format("csv").option("path", "/user/root/data/signal_1").option("checkpointLocation", "/user/root/data/check1").start()

# BEFORE ANY STREAM RESTART
# CLEAN CHEKPOINT TO START FRESH & AVOID ERROR MSG
# hdfs dfs -rm -r /user/root/data/check1
# hdfs dfs -rm -r /user/root/data/check2
# hdfs dfs -rm -r /user/root/data/check3