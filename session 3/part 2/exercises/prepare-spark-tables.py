from pyspark.sql.types import *

# Movies

# Define a Schema 

moviesStruct = [StructField("movieId", IntegerType(), True),
	StructField("title", StringType(), True),
	StructField("genres", StringType(), True)]

moviesSchema = StructType(moviesStruct)

# Define Movies Dataframe

moviesDf = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(moviesSchema) \
    .load("hdfs:///user/hadoop/movielens/movies.csv")

# Save as Table

moviesDf.write.option("path", "/tmp/movies").saveAsTable("movies")

# Play Around 

spark.sql("select * from movies").show()
spark.sql("select * from movies").show(20, False)


df = spark.sql("select * from movies")
df.filter(df.genres.contains('Drama')).show(20, False)

# Save as Table 

# spark.sql("Drop table movies")
# hdfs dfs -rm -r /tmp/movies

# Define Ratings Schema 

ratingsStruct = [StructField("userId", IntegerType(), True),
	StructField("movieId", IntegerType(), True),
	StructField("rating", DoubleType(), True),
	StructField("timestamp", IntegerType(), True)]

ratingsSchema = StructType(ratingsStruct)

# Define Ratings Dataframe 

ratingsDf = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(ratingsSchema) \
    .load("hdfs:///user/hadoop/movielens/ratings.csv")

ratingsDf.write.option("path", "/tmp/ratings").saveAsTable("ratings")

spark.sql("select * from ratings").show()

# Define Tag Schema 

tagsStruct = [StructField("userId", IntegerType(), True),
	StructField("movieId", IntegerType(), True),
	StructField("tag", StringType(), True),
	StructField("timestamp", IntegerType(), True)]


tagsSchema = StructType(tagsStruct)

# Define Tags DataFrame 

tagsDf = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(tagsSchema) \
    .load("hdfs:///user/hadoop/movielens/tags.csv")

tagsDf.write.option("path", "/tmp/tags").saveAsTable("tags")

spark.sql("select * from tags").show()

# Define Link Schema

linksStruct = [StructField("movieId", IntegerType(), True),
	StructField("imdbId", IntegerType(), True),
	StructField("tmdbId", IntegerType(), True)]

linksSchema = StructType(linksStruct)

# Define Link DataFrame (REF)

linksDf = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(linksSchema) \
    .load("hdfs:///user/hadoop/movielens/links.csv")

linksDf.write.option("path", "/tmp/links").saveAsTable("links")

spark.sql("select * from links").show()
