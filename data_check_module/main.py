from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pydeequ
import sys


def init_spark_session():
    spark = (SparkSession
        .builder
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate())

    return spark

def read_file_from_args():
  print("Args: ", sys.argv[1:])
  filepath = ".\\data\\{}".format(sys.argv[1])
  return filepath


def data_cleaning(df_init):
  df_init 



if __name__ == "__main__":
  
  print("Setting up spark and variables...")
  spark = init_spark_session()

  csv_filepath= read_file_from_args()

  df = spark.read \
          .option("inferSchema",True) \
          .option("header", True) \
          .option("delimiter",",") \
      .csv(csv_filepath)

  df.toPandas()