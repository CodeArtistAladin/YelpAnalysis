from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName('DataFrameIntro') \
    .master('local') \
    .getOrCreate()

# 1) Spark session is the entrance for Spark SQL
# 2) Spark context is for RDD operations

# Get Spark context from the Spark session
sc = spark.sparkContext

# Parallelize to create an RDD
rdd = sc.parallelize([("tom", 3), ("jerry", 1)])

# Convert RDD to DataFrame and specify column names
df = rdd.toDF(["name", "age"])

# 1) Print the schema definition
df.printSchema()

# Print the DataFrame rows
df.show()

# Read a CSV file into a DataFrame
df = spark.read\
    .option('header', True)\
    .csv('dataset/BeijingPM20100101_20151231.csv')

# Print the schema of the DataFrame
df.printSchema()

# Show the first few rows of the DataFrame
df.show()

# Clean the data: select 'year', 'month', and 'PM_Dongsi', and filter out rows with 'NA' in 'PM_Dongsi'
clean_df = df.select('year', 'month', 'PM_Dongsi')\
    .where("PM_Dongsi != 'NA'")

# Show the cleaned DataFrame
clean_df.show()

# Group the data by 'month' and count the number of rows per month
result = clean_df.groupBy('month')\
    .count()\
    .orderBy('count')

# Show the grouped result
result.show()
