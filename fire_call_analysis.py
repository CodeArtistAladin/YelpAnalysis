# In Python, define a schema
from pyspark.sql.functions import count, col, countDistinct, to_timestamp, year, month, weekofyear, avg, min, max
from pyspark.sql.types import *
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Spark Example") \
    .master("local") \
    .getOrCreate()

# Programmatic way to define a schema
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
 StructField('UnitID', StringType(), True),
 StructField('IncidentNumber', IntegerType(), True),
 StructField('CallType', StringType(), True),
 StructField('CallDate', StringType(), True),
 StructField('WatchDate', StringType(), True),
 StructField('CallFinalDisposition', StringType(), True),
 StructField('AvailableDtTm', StringType(), True),
 StructField('Address', StringType(), True),
 StructField('City', StringType(), True),
 StructField('Zipcode', IntegerType(), True),
 StructField('Battalion', StringType(), True),
 StructField('StationArea', StringType(), True),
 StructField('Box', StringType(), True),
 StructField('OriginalPriority', StringType(), True),
 StructField('Priority', StringType(), True),
 StructField('FinalPriority', IntegerType(), True),
 StructField('ALSUnit', BooleanType(), True),
 StructField('CallTypeGroup', StringType(), True),
 StructField('NumAlarms', IntegerType(), True),
 StructField('UnitType', StringType(), True),
 StructField('UnitSequenceInCallDispatch', IntegerType(), True),
 StructField('FirePreventionDistrict', StringType(), True),
 StructField('SupervisorDistrict', StringType(), True),
 StructField('Neighborhood', StringType(), True),
 StructField('Location', StringType(), True),
 StructField('RowID', StringType(), True),
 StructField('Delay', FloatType(), True)])

df = spark.read\
    .option('header', True)\
    .schema(fire_schema)\
    .csv('dataset/sf-fire-calls.txt')



# Convert date columns to timestamps
cleaned_df = df.withColumn('IncidentDate', to_timestamp(col('CallDate'), 'MM/dd/yyyy')) \
    .drop('CallDate') \
    .withColumn('OnWatchDate', to_timestamp(col('WatchDate'), 'MM/dd/yyyy')) \
    .drop('WatchDate') \
    .withColumn('AvailableDtTS', to_timestamp(col('AvailableDtTm'), 'MM/dd/yyyy hh:mm:ss a')) \
    .drop('AvailableDtTm')



# Storing data as Parquet file
cleaned_df.write.mode("overwrite").parquet("fire_data.parquet")

# Reading from Parquet
parquet_df = spark.read.parquet("fire_data.parquet")
parquet_df.show(5, False)

