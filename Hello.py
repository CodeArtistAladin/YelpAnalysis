# Step 1: Create Spark session
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Spark Example") \
    .master("local") \
    .getOrCreate()

# Step 2: Submit analysis to Spark session created in Step 1
# Create a DataFrame
df = spark.createDataFrame([('tom', 20), ('jack', 40)], ['name', 'age'])

print("test")
# Print the number of rows in the DataFrame
print(df.count())

# Step 3: Close Spark session
spark.stop()