from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, size, split, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Spark Example") \
    .master("local") \
    .getOrCreate()

hcx = HiveContext(spark.sparkContext)

df_business = hcx.table('business')
df_checkin = hcx.table('checkin')

# Process check-in data by counting check-in occurrences per business
df_checkin_processed = df_checkin.withColumn("checkin_count", size(split(col("checkin_dates"), ",")))

# Join business and check-in data on business_id
df_merged = df_business.join(df_checkin_processed, "business_id", "left_outer") \
                       .select("business_id", "name", "city", "stars", "review_count", "checkin_count")

# Fill null check-in counts with 0 (for businesses with no check-ins)
df_merged = df_merged.fillna({"checkin_count": 0})

# window partitioned by city and ordered by rating frequency, average rating and check-in count
window_spec = Window.partitionBy("city") \
                    .orderBy(col("review_count").desc(), col("stars").desc(), col("checkin_count").desc())

df_top_merchants = df_merged.withColumn("rank", row_number().over(window_spec)) \
                            .filter(col("rank") <= 5) \
                            .select("city", "name", "stars", "review_count", "checkin_count", "rank")

print("Top 5 merchants in each city based on rating frequency, average rating, and check-in frequency:")
