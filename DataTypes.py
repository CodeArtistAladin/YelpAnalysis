from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, ArrayType

# 1. Fixed SparkSession initialization
spark = SparkSession.builder \
    .appName('DataTypeExample') \
    .master('local') \
    .getOrCreate()  # Corrected method name (camelCase)

# 2. Fixed schema definition
schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("First", StringType(), True),
    StructField("Last", StringType(), True),
    StructField("Url", StringType(), True),
    StructField("Published", StringType(), True),
    StructField("Hits", LongType(), True),
    StructField("Campaigns", ArrayType(StringType()), True)  # Removed space
])

# 3. Fixed DataFrame creation
df = spark.read \
    .schema(schema) \
    .json('dataset/blogs.txt')  # Ensure this file exists and is valid JSON

# Show results
df.printSchema()
df.show()