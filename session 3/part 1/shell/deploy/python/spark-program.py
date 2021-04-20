
import sys, time

from pyspark.sql import SparkSession

if __name__ == "__main__":

  print("")
  print("")
  print("")
  print("")

  print("----------------    WARNING   -----------------------------------")
  print("")
  print("THERE WILL BE TONS ANS TONS AND TONS OF LOGS BEFORE YOU SEE THE RESULT")
  print("We will see tomorrow how to adust LOG LEVELS. It is NOT Trivial...")
  print("FOR NOW, JUST WAIT FOR IT.....")
  print("")
  print("----------------    WARNING   -----------------------------------")
  print("")
  print("")
  print("")
  print("")
  time.sleep(10)

  #create Spark Session with Spark configuration
  spark = SparkSession\
        .builder\
        .appName("My Pyspark Program")\
        .getOrCreate()

  print("")
  print("")
  print("")
  print("")
  print("---------------- DRUM ROLLS FIRST ROUND -----------------------------------")
  print("")
  print("")
  print("")
  df = spark.read.option("header", True).csv("/user/root/data/zipcodes.csv")
  print("")
  print("")
  print("")
  print("")
  print("---------------- DRUM ROLLS SECOND ROUND-----------------------------------")
  print("")
  print("")
  print("")
  print("")
  df.show()
  print("")
  print("")
  print("")
  print("")
  print("-----------------------------------------------------------------------------")
  print("")
  print("")
  print("")
  print("")
