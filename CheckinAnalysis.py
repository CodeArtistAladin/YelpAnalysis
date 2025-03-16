from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import avg

spark = SparkSession.builder \
    .appName("Spark Example") \
    .master("local") \
    .getOrCreate()

hcx = HiveContext(spark.sparkContext)

# Load the checkin table into a DataFrame
df = hcx.table('checkin')
business_view = df.select(col('business_id'), explode(split(col('checkin_dates'), ',')).alias('datetime'))

business_df = hcx.table('business')

# Create the business_view
business_view = df.select(
    col('business_id'),
    explode(split(col('checkin_dates'), ',')).alias('datetime')
)

# Register the view so it can be reused
business_view.createOrReplaceTempView("business_view")
# 1.Count the number of check-ins per year.

result = df.select(col('business_id'), explode(split(col('checkin_dates'), ',')).alias('datetime')) \
    .select('business_id', year(to_timestamp(trim(col('datetime')))).alias('year')) \
    .groupBy('year') \
    .count()

print("Checkin per year")

# 2.Count the number of check-ins per hour within a 24-hour period.

result = df.select(col('business_id'), explode(split(col('checkin_dates'), ',')).alias('datetime')) \
    .select('business_id', hour(to_timestamp(trim(col('datetime')))).alias('hour')) \
    .groupBy('hour') \
    .count() \
    .orderBy('hour')
print("check-ins per hour within a 24-hour period")


# 3.Identify the most popular city for check-ins.
result = df.select(col('business_id'), explode(split(col('checkin_dates'), ',')).alias('datetime')) \
    .join(business_df, df['business_id'] == business_df['business_id']) \
    .select(df['business_id'], business_df['city']) \
    .groupBy('city') \
    .agg(count('city').alias('cnt')) \
    .orderBy(col('cnt').desc())

print("most popular city for check-ins:")


# 4.Rank all businesses based on check-in counts.

result = business_view.join(business_df, business_view['business_id'] == business_df['business_id']) \
    .groupBy(business_df['business_id'], business_df['name']) \
    .agg(count(business_view['business_id']).alias('checkin_count')) \
    .orderBy(col('checkin_count').desc())

print("Business rank based on check-in counts:")
