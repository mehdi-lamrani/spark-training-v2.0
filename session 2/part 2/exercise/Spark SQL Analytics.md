
### Spark SQL Analytics

- For this workshop, you can use the provided Bureau of Transportation Dataset

- Reminder : 

    * By now, you should have Downloaded the DataSets from S3 & stored them with a proper schema as Parquet files on HDFS :

/user/root/data/BOTS/PARQUET

- Reference Data : 

In the provided S3 bucket, you have a data & a ref folder
The ref folder contains reference tables 
You should save & load that as well in parquet format.


- This is an open ended & creative workshop
- The goal is to leverage dataframe & SQL API to explore the data and get actionable insights from it
- Create your main dataframes, containing Data & Relevant Reference tables
- Explore these dataframes, and make sense of the data
   * How many airlines, How many airports ? 
   * What are the origins / destinations ?
   
- Fix insight goals that might be of interest 
   * Examples :   
   * What Airlines are mostly on Time. 
   * In which Airports planes are more likey to arrive late ?  

- A complete Example Solution Zeppelin Notebook will be provided 

- ToolBox : 

    * Joining Dataframes (Dataframe API): 

```
joinDF = DF_ONE.join(DF_TWO, DF_ONE.join_field ==  DF_TWO.join_field)
```

    * Creating Temporary Views : 

```
DF_ONE.createOrReplaceTempView("EMPLOYEES")
DF_TWO.createOrReplaceTempView("DEPT")
```
    * Joining Dataframes (SQL API): 

```
joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id") \
  .show(truncate=False)
```

- Filter + Distinct : 


```
custDF = salesDF.select("customerId").distinct()
```

- Aggregation : 

```
salesDF.agg({"productId": "max"})
```

- Complete Request Example : 

Most Rated Comedy Movies, grouped by Movie & Rating

```
moviesDf.filter(moviesDf.genres.contains('Comedy')) \
    .join(ratingsDf, "movieId") \
    .groupBy(col("movieId"),col("title"), col("rating")).count().orderBy("count", ascending=False) \
    .show(20, truncate=False)
```

