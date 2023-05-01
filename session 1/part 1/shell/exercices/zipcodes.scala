import org.apache.spark.sql.SparkSession

object ZipcodesApp {
  def main(args: Array[String]): Unit = {
    // Initialize the Spark session
    val spark = SparkSession.builder
      .appName("ZipcodesApp")
      .getOrCreate()

    // Read the CSV file from HDFS
    val df = spark.read
      .option("header", true)
      .format("csv")
      .load("hdfs:///user/hadoop/zipcodes.csv")

    // Perform any DataFrame operations you need, e.g., show the DataFrame
    df.show()

    // Stop the Spark session
    spark.stop()
  }
}
