import org.apache.spark.sql.SparkSession

object mySparkProgram {

        def main(args: Array[String]) {
                val file = "/user/root/data/zipcodes.csv"

                val spark = SparkSession
                        .builder
                        .appName("spark-deploy")
                        .getOrCreate()

                val df = spark.read.csv(file)
                df.write.parquet("/user/root/data/zipcodes.parquet")
        }
}