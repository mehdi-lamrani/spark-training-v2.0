# Spark Advanced Optimization Workshop 

Movie Lens DataSet :


(1 MB)
http://files.grouplens.org/datasets/movielens/ml-latest-small.zip

(265 MB)
http://files.grouplens.org/datasets/movielens/ml-latest.zip

- Use wget to retrieves files : 
```
wget http://linktozipfilehere
```

- Load them into HDFS : 

- working in pair is advised here
- In case you are working individually, you need to adjust personal subfolders and permissions accordingly (hdfs dfs -chmod)
- In that case mind your database contexte creation as well

````
hdfs dfs -put *.csv /user/hadoop/movielens  
hdfs dfs -ls /user/hadoop/movielens
````


**DATA PREPARATION**

-  In Jupyter :  
Load and execute the following preparation notebook :  
[spark-tables-prep.ipynb](https://github.com/mehdi-lamrani/spark-training-v2.0/blob/master/day%203/part%202/exercises/spark-tables-prep.ipynb)  



**In Scala : (spark-shell)**

- On a Linux Terminal : Launch the following command :

```
spark-shell
```

**FIRST : WITHOUT BUCKETING - Non optimized**

```
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

val joinDf = spark.table("movies").join(spark.table("ratings"), "movieId")
```

```
scala> joinDf.explain()
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [movieId#0, title#1, genres#2, userId#6, rating#8, timestamp#9]
   +- SortMergeJoin [movieId#0], [movieId#7], Inner
      :- Sort [movieId#0 ASC NULLS FIRST], false, 0
      :  +- Exchange hashpartitioning(movieId#0, 1000), true, [id=#24]
      :     +- Project [movieId#0, title#1, genres#2]
      :        +- Filter isnotnull(movieId#0)
      :           +- FileScan parquet default.movies[movieId#0,title#1,genres#2] Batched: true, DataFilters: [isnotnull(movieId#0)], Format: Parquet, Location: InMemoryFileIndex[hdfs://ip-172-31-33-143.eu-west-3.compute.internal:8020/tmp/movies], PartitionFilters: [], PushedFilters: [IsNotNull(movieId)], ReadSchema: struct<movieId:int,title:string,genres:string>
      +- Sort [movieId#7 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(movieId#7, 1000), true, [id=#25]
            +- Project [userId#6, movieId#7, rating#8, timestamp#9]
               +- Filter isnotnull(movieId#7)
                  +- FileScan parquet default.ratings[userId#6,movieId#7,rating#8,timestamp#9] Batched: true, DataFilters: [isnotnull(movieId#7)], Format: Parquet, Location: InMemoryFileIndex[hdfs://ip-172-31-33-143.eu-west-3.compute.internal:8020/tmp/ratings], PartitionFilters: [], PushedFilters: [IsNotNull(movieId)], ReadSchema: struct<userId:int,movieId:int,rating:double,timestamp:int>
```
```
scala> spark.time(joinDf.count())
Time taken: 2725 ms                                                             
res3: Long = 100836
```

**OPTIMISATION I : WITH BUCKETING**  
**RESULT : BOTH SIDES SHUFFLE FREE JOIN**  

```
import org.apache.spark.sql.SaveMode

val moviesDf = spark.sql("select * from movies")

moviesDf.write
  .bucketBy(50, "movieId")
  .sortBy("movieId")
  .mode(SaveMode.Overwrite)
  .saveAsTable("bucketed_movies_50")

val ratingsDf = spark.sql("select * from ratings")

ratingsDf.write
  .bucketBy(50, "movieId")
  .sortBy("movieId")
  .mode(SaveMode.Overwrite)
  .saveAsTable("bucketed_ratings_50")
```

```
scala> val joinDf2 = spark.table("bucketed_movies_50").join(spark.table("bucketed_ratings_50"), "movieId")

joinDf2: org.apache.spark.sql.DataFrame = [movieId: int, title: string ... 4 more fields]

scala> joinDf2.explain()
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [movieId#90, title#91, genres#92, userId#96, rating#98, timestamp#99]
   +- SortMergeJoin [movieId#90], [movieId#97], Inner
      :- Sort [movieId#90 ASC NULLS FIRST], false, 0
      :  +- Project [movieId#90, title#91, genres#92]
      :     +- Filter isnotnull(movieId#90)
      :        +- FileScan parquet default.bucketed_movies_4[movieId#90,title#91,genres#92] Batched: true, DataFilters: [isnotnull(movieId#90)], Format: Parquet, Location: InMemoryFileIndex[hdfs://ip-172-31-33-143.eu-west-3.compute.internal:8020/user/spark/warehouse/bu..., PartitionFilters: [], PushedFilters: [IsNotNull(movieId)], ReadSchema: struct<movieId:int,title:string,genres:string>, SelectedBucketsCount: 4 out of 4
      +- Sort [movieId#97 ASC NULLS FIRST], false, 0
         +- Project [userId#96, movieId#97, rating#98, timestamp#99]
            +- Filter isnotnull(movieId#97)
               +- FileScan parquet default.bucketed_ratings_4[userId#96,movieId#97,rating#98,timestamp#99] Batched: true, DataFilters: [isnotnull(movieId#97)], Format: Parquet, Location: InMemoryFileIndex[hdfs://ip-172-31-33-143.eu-west-3.compute.internal:8020/user/spark/warehouse/bu..., PartitionFilters: [], PushedFilters: [IsNotNull(movieId)], ReadSchema: struct<userId:int,movieId:int,rating:double,timestamp:int>, SelectedBucketsCount: 4 out of 4

scala> spark.time(joinDf2.count())
Time taken: 347 ms
res32: Long = 100836

scala> val describeSQL = sql(s"DESCRIBE EXTENDED bucketed_movies_50")
scala> describeSQL.show(numRows = 21, truncate = false)
```

**We can see the number of buckets and the bucket columns :**

```
+----------------------------+-----------------------------------------------------------------------------------------------+-------+
|col_name                    |data_type                                                                                      |comment|
+----------------------------+-----------------------------------------------------------------------------------------------+-------+
|movieId                     |int                                                                                            |null   |
|title                       |string                                                                                         |null   |
|genres                      |string                                                                                         |null   |
|                            |                                                                                               |       |
|# Detailed Table Information|                                                                                               |       |
|Database                    |default                                                                                        |       |
|Table                       |bucketed_movies_50                                                                             |       |
|Owner                       |root                                                                                           |       |
|Created Time                |Fri Oct 16 18:12:17 UTC 2020                                                                   |       |
|Last Access                 |UNKNOWN                                                                                        |       |
|Created By                  |Spark 3.0.0-amzn-0                                                                             |       |
|Type                        |MANAGED                                                                                        |       |
|Provider                    |parquet                                                                                        |       |
|Num Buckets                 |50                                                                                             |       |
|Bucket Columns              |[`movieId`]                                                                                    |       |
|Sort Columns                |[`movieId`]                                                                                    |       |
|Statistics                  |358335 bytes                                                                                   |       |
|Location                    |hdfs://ip-172-31-33-143.eu-west-3.compute.internal:8020/user/spark/warehouse/bucketed_movies_50|       |
|Serde Library               |org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe                                    |       |
|InputFormat                 |org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat                                  |       |
|OutputFormat                |org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat                                 |       |
+----------------------------+-----------------------------------------------------------------------------------------------+-------+
```

**Another way to see table's bucketing information :**

```
scala> val metadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier("bucketed_movies_50"))

Num Buckets: 50
Bucket Columns: [`movieId`]
```

There are requirements that have to be met before Spark Optimizer gives a no-Exchange query plan:
    - The number of partitions on both sides of a join has to be exactly the same.
    - Both join operators have to use HashPartitioning partitioning scheme.


**OPTIMISATION II : 1 BUCKETED TABLE WITH ONE UNBUCKETED TABLE :**  
**RESULT : ONE SIDE SHUFFLE FREE JOIN**

```

val joinDf3 = spark.table("movies").join(spark.table("bucketed_ratings_50"), "movieId")

scala> joinDf3.explain()
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [movieId#0, title#1, genres#2, userId#409, rating#411, timestamp#412]
   +- SortMergeJoin [movieId#0], [movieId#410], Inner
      :- Sort [movieId#0 ASC NULLS FIRST], false, 0
      :  +- Exchange hashpartitioning(movieId#0, 200), true, [id=#4277]
      :     +- Project [movieId#0, title#1, genres#2]
      :        +- Filter isnotnull(movieId#0)
      :           +- FileScan parquet default.movies[movieId#0,title#1,genres#2] Batched: true, DataFilters: [isnotnull(movieId#0)], Format: Parquet, Location: InMemoryFileIndex[hdfs://ip-172-31-33-143.eu-west-3.compute.internal:8020/tmp/movies], PartitionFilters: [], PushedFilters: [IsNotNull(movieId)], ReadSchema: struct<movieId:int,title:string,genres:string>
      +- Sort [movieId#410 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(movieId#410, 200), true, [id=#4278]
            +- Project [userId#409, movieId#410, rating#411, timestamp#412]
               +- Filter isnotnull(movieId#410)
                  +- FileScan parquet default.bucketed_ratings_50[userId#409,movieId#410,rating#411,timestamp#412] Batched: true, DataFilters: [isnotnull(movieId#410)], Format: Parquet, Location: InMemoryFileIndex[hdfs://ip-172-31-33-143.eu-west-3.compute.internal:8020/user/spark/warehouse/bu..., PartitionFilters: [], PushedFilters: [IsNotNull(movieId)], ReadSchema: struct<userId:int,movieId:int,rating:double,timestamp:int>, SelectedBucketsCount: 50 out of 50

scala> spark.time(joinDf3.count())
Time taken: 1241 ms                                                             
res52: Long = 100836
```


**OPTIMISATION III : AGGREGATION FOLLOWED BY A JOIN**

```
val ratingsDf = spark.table("ratings")
val linksDf = spark.table("links")
val joinDf4 = ratingsDf.groupBy("movieId", "rating").agg(count("*")).join(linksDf, "movieId")

scala> joinDf4.explain()
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [movieId#1, rating#2, count(1)#210L, imdbId#9, tmdbId#10]
   +- SortMergeJoin [movieId#1], [movieId#8], Inner
      :- Sort [movieId#1 ASC NULLS FIRST], false, 0
      :  +- Exchange hashpartitioning(movieId#1, 1000), true, [id=#581]
      :     +- HashAggregate(keys=[movieId#1, rating#2], functions=[count(1)])
      :        +- Exchange hashpartitioning(movieId#1, rating#2, 1000), true, [id=#577]
      :           +- HashAggregate(keys=[movieId#1, knownfloatingpointnormalized(normalizenanandzero(rating#2)) AS rating#2], functions=[partial_count(1)])
      :              +- Project [movieId#1, rating#2]
      :                 +- Filter isnotnull(movieId#1)
      :                    +- FileScan parquet default.ratings[movieId#1,rating#2] Batched: true, DataFilters: [isnotnull(movieId#1)], Format: Parquet, Location: InMemoryFileIndex[hdfs://ip-172-31-33-143.eu-west-3.compute.internal:8020/tmp/ratings], PartitionFilters: [], PushedFilters: [IsNotNull(movieId)], ReadSchema: struct<movieId:int,rating:double>
      +- Sort [movieId#8 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(movieId#8, 1000), true, [id=#582]
            +- Project [movieId#8, imdbId#9, tmdbId#10]
               +- Filter isnotnull(movieId#8)
                  +- FileScan parquet default.links[movieId#8,imdbId#9,tmdbId#10] Batched: true, DataFilters: [isnotnull(movieId#8)], Format: Parquet, Location: InMemoryFileIndex[hdfs://ip-172-31-33-143.eu-west-3.compute.internal:8020/tmp/links], PartitionFilters: [], PushedFilters: [IsNotNull(movieId)], ReadSchema: struct<movieId:int,imdbId:int,tmdbId:int>
```

```
scala> spark.time(joinDf4.count())
Time taken: 11182 ms                                                            
res18: Long = 30417
```

```
val ratingsDf = spark.table("ratings").repartition(col("movieId"))
val linksDf = spark.table("links")
val joinDf5 = ratingsDf.groupBy("movieId", "rating").agg(count("*")).join(linksDf, "movieId")
```

```
scala> joinDf5.explain()
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [movieId#1, rating#2, count(1)#262L, imdbId#9, tmdbId#10]
   +- SortMergeJoin [movieId#1], [movieId#8], Inner
      :- Sort [movieId#1 ASC NULLS FIRST], false, 0
      :  +- HashAggregate(keys=[movieId#1, rating#2], functions=[count(1)])
      :     +- HashAggregate(keys=[movieId#1, knownfloatingpointnormalized(normalizenanandzero(rating#2)) AS rating#2], functions=[partial_count(1)])
      :        +- Exchange hashpartitioning(movieId#1, 200), false, [id=#926]
      :           +- Project [movieId#1, rating#2]
      :              +- Filter isnotnull(movieId#1)
      :                 +- FileScan parquet default.ratings[movieId#1,rating#2] Batched: true, DataFilters: [isnotnull(movieId#1)], Format: Parquet, Location: InMemoryFileIndex[hdfs://ip-172-31-33-143.eu-west-3.compute.internal:8020/tmp/ratings], PartitionFilters: [], PushedFilters: [IsNotNull(movieId)], ReadSchema: struct<movieId:int,rating:double>
      +- Sort [movieId#8 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(movieId#8, 200), true, [id=#936]
            +- Project [movieId#8, imdbId#9, tmdbId#10]
               +- Filter isnotnull(movieId#8)
                  +- FileScan parquet default.links[movieId#8,imdbId#9,tmdbId#10] Batched: true, DataFilters: [isnotnull(movieId#8)], Format: Parquet, Location: InMemoryFileIndex[hdfs://ip-172-31-33-143.eu-west-3.compute.internal:8020/tmp/links], PartitionFilters: [], PushedFilters: [IsNotNull(movieId)], ReadSchema: struct<movieId:int,imdbId:int,tmdbId:int>
```

```
scala> spark.time(joinDf5.count())
Time taken: 12343 ms                                                            
res20: Long = 30417
```
