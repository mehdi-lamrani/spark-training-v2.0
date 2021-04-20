val df = spark.readStream.format("socket").option("host", "127.0.0.1").option("port", YOUR_PORT_NUMBER_HERE).option("includeTimestamp","true").load()

//OPTION 1 : COUNT
val dfc = df.groupBy(window($"timestamp", "10 second")).count()

//OPTION 2 : AVG
val dfc = df.groupBy(window($"timestamp", "10 second")).avg()

//OPTION 3 : SLIDING WINDOW
val dfc = df.groupBy(window($"timestamp", "10 second", "1 second")).avg()

dfc.writeStream.format("console").option("checkpointLocation", "/user/root/data/check").outputMode("Complete").start()
