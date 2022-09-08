from traceback import print_tb
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, regexp_replace, split, concat_ws
import sys
import os
import data_validation


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
  location_data = ".\\data\\{}".format(sys.argv[2])
  output_filepath = sys.argv[3]

  return filepath, location_data, output_filepath


def data_cleaning(df, df_location, output_filepath):

  #Dropping unused columns
  df_drop_columns = df.drop('online_order', 'book_table', 'approx_cost(for two people)', 'menu_item', 'listed_in(type)', 'listed_in(city)')

  #Cast String columns to Arrays
  df_column_castted = df_drop_columns.withColumn("dish_liked_array", split(col('dish_liked'), ',')) \
                        .withColumn("review_list_array", split(col('reviews_list'), ',')) \
                        .withColumn("cuisines_array", split(col("cuisines"), ',')) \
                        .drop("dish_liked", "reviews_list", "cuisines")

  #Extracting valid URLs pattern
  df_regex_url = df_column_castted.withColumn("valid_url", regexp_extract(col('url'), 
                          r"[(http(s)?):\/\/(www\.)?a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)", 0))
  
  #Extracting valid Phone pattern
  df_regex_columns = df_regex_url.withColumn("phone_valid", 
                                    regexp_replace( regexp_extract(col('phone'), r"\d[\d -]{8,12}\d", 0), " ", ''))

  #Extracting valid votes pattern
  df_regex_columns_v2 = df_regex_columns.withColumn("valid_votes", regexp_extract(col('votes'), r"^[0-9]*$", 0))

  #Extracing valid address pattern
  df_replace_comma_address = df_regex_columns_v2.withColumn("address_cleaned", regexp_replace(regexp_extract(col('address'), r"^[#.0-9a-zA-Z\s,-]+$", 0), ',', ""))

  #Extracing valid rate pattern
  df_replace_rates = df_replace_comma_address.withColumn("rate_valid", regexp_extract(col('rate'), r"(\d+\.?\d+)\/(\d*\.?\d*)",0))

  #Join df with df_location to validate location column
  df_join_location = df_replace_rates.join(df_location, df_replace_comma_address['location'] == df_location['Area'], "left")

  #Filter for valid location only
  df_valid_location = df_join_location.filter(col("Area").isNotNull())

  #From valid location rows, filter not nullable columns
  df_nulls_filtered = df_valid_location.filter( (col('name').isNotNull()  
                                               & col('phone').isNotNull() 
                                               & col('location').isNotNull()) )

  #Drop unused columns
  df_drop_unused_columns = df_nulls_filtered.drop('url', 'address', 'phone', 'votes', 'rate', 'Area', 'Taluk', 'District', 'State', 'Pincode')

  #Renamed columns
  df_renamed_columns = df_drop_unused_columns.withColumnRenamed('valid_url','url') \
                                        .withColumnRenamed('address_cleaned', 'address') \
                                        .withColumnRenamed('phone_valid', 'phone') \
                                        .withColumnRenamed('valid_votes', 'votes') \
                                        .withColumnRenamed('rate_valid', 'rate')

  #Casting Array column to String and dropping old columns
  df_arrays_to_string = df_renamed_columns.withColumn('dish_liked', concat_ws(' ', col('dish_liked_array'))) \
                                    .withColumn('review_list', concat_ws(' ', col('review_list_array'))) \
                                    .withColumn('cuisines', concat_ws(' ', col('cuisines_array'))) \
                                .drop('dish_liked_array', 'review_list_array', 'cuisines_array')
  
  #Cleaning junk/special characters from review_list
  df_special_char_cleaned = df_arrays_to_string.withColumn('review_list', regexp_replace(col('review_list'), "[^a-zA-Z0-9]", " "))

  #Cast Phone, votes from String to Integer
  df_casted_int_columns = df_special_char_cleaned.withColumn("phone", df_arrays_to_string.phone.cast('integer')) \
                                            .withColumn("votes", df_arrays_to_string.votes.cast('integer'))

  #Reorder columns
  df_columns_sorted = df_casted_int_columns.select(col('url'),
                                             col('address'),
                                             col('name'),
                                             col('rate'),
                                             col('votes'),
                                             col('phone'),
                                             col('location'),
                                             col('rest_type'),
                                             col('dish_liked'),
                                             col('cuisines'),
                                             col('review_list'))


  #Writting 'good' data to output/ folder
  print('Writting "good" data to {0}'.format(output_filepath))
  df_columns_sorted.toPandas().to_csv('{0}/record_out.csv'.format(output_filepath), index=False)


  #Getting bad data (those that didn't pass the initials checks like valid location)
  df_bad_data = df_join_location.filter(col('Area').isNull()) \
                              .withColumn('dish_liked', concat_ws(' ', col('dish_liked_array'))) \
                              .withColumn('review_list', concat_ws(' ', col('review_list_array'))) \
                              .withColumn('cuisines', concat_ws(' ', col('cuisines_array'))) \
                            .drop('2url', 'address', 'phone', 'votes', 'rate', 'Area', 'Taluk', 'District', 'State', 'Pincode','dish_liked_array', 'review_list_array', 'cuisines_array') \
                            .withColumnRenamed('valid_url','url') \
                            .withColumnRenamed('address_cleaned', 'address') \
                            .withColumnRenamed('phone_valid', 'phone') \
                            .withColumnRenamed('valid_votes', 'votes') \
                            .withColumnRenamed('rate_valid', 'rate') \
                        .select(col('url'),
                                col('address'),
                                col('name'),
                                col('rate'),
                                col('votes'),
                                col('phone'),
                                col('location'),
                                col('rest_type'),
                                col('dish_liked'),
                                col('cuisines'),
                                col('review_list'))
  
  print('Writting "bad" data to {0}'.format(output_filepath))
  df_bad_data.toPandas().to_csv("{0}/record_bad.csv".format(output_filepath), index=False)

  return df_columns_sorted

if __name__ == "__main__":
  
  print("Setting up spark and variables...")
  spark = init_spark_session()

  csv_filepath, location_data_filepath, output_filepath = read_file_from_args()

  print("Creating Spark dataframe from {0}".format(csv_filepath))
  df = spark.read \
          .option("inferSchema",True) \
          .option("header", True) \
          .option("delimiter",",") \
      .csv(csv_filepath)

  print('Creating Spark dataframe from {0}'.format(location_data_filepath))
  df_location = spark.read \
          .option("inferSchema",True) \
          .option("header", True) \
          .option("delimiter",",") \
      .csv(location_data_filepath)

  print('Starting cleaning process...')
  df_cleaned = data_cleaning(df=df, df_location=df_location, output_filepath=output_filepath)

  print('Data Cleaning process finished...')
  print('Starting Data Validation...')
  data_validation.validate_data(df=df_cleaned)