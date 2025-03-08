from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, ArrayType
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder \
    .appName('DataTypeExample') \
    .master('local') \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("Last", StringType(), True),
    StructField("Id", IntegerType(), True),
    StructField("First", StringType(), True),
    StructField("Url", StringType(), True),
    StructField("Published", StringType(), True),
    StructField("Hits", LongType(), True),
    StructField("Campaigns", ArrayType(StringType()), True)
])

# Read JSON file using the schema
df = spark.read \
    .schema(schema) \
    .json('dataset/blogs.txt')

# 1) Get column by column name (using column access in df)
print(df['First'])

# 2) Using col() function (preferred approach for better readability)
print(col('First'))

# 3) Select columns 'First', 'Last', and 'Hits'
df.select(col('First'), col('Last'), col('Hits')).show()

# 4) Double the 'Hits' column and show the result
df.select(col('First'), col('Last'), col('Hits'), col('Hits') * 2).show()

# 5) Order by 'Hits' in descending order
df.orderBy(col('Hits').desc()).show()

# 6) Check if 'Hits' > 10000 (True/False condition)
df.select(col('First'), col('Last'), col('Hits'), col('Hits') > 10000).show()

# 7) Search for rows where 'First' column is 'Denny'
df.where(col('First') == 'Denny').show()

# 8) Shorter way to select columns (without using col())
df.select('First', 'Last', 'Hits').show()

df.select('First', 'Last', 'Hits', col('Hits') * 2).show()

