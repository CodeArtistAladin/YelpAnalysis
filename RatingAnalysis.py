from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, date_format, when

spark = SparkSession.builder \
    .appName("Spark Example") \
    .master("local") \
    .getOrCreate()

hcx = HiveContext(spark.sparkContext)

# 1: Analyze the distribution of ratings (1-5 stars)
df_review = hcx.table('review')

print("The distribution of ratings (1-5 stars):")
result = df_review.groupBy(col("rev_stars").alias("rating")) \
                  .agg(count("rev_stars").alias("count")) \
                  .orderBy("rating")

# 2 Analyze the weekly rating frequency (Monday to Sunday).
print("Weekly rating frequency (Monday to Sunday):")
result = df_review.withColumn("day_of_week", date_format(col("rev_date"), "EEEE")) \
                  .withColumn("day_order", when(col("day_of_week") == "Monday", 1)
                                         .when(col("day_of_week") == "Tuesday", 2)
                                         .when(col("day_of_week") == "Wednesday", 3)
                                         .when(col("day_of_week") == "Thursday", 4)
                                         .when(col("day_of_week") == "Friday", 5)
                                         .when(col("day_of_week") == "Saturday", 6)
                                         .when(col("day_of_week") == "Sunday", 7)) \
                  .groupBy("day_of_week", "day_order") \
                  .agg(count("rev_date").alias("count")) \
                  .orderBy("day_order")

# 3 Identify the top businesses with the most five-star ratings.
df_review = hcx.table('review')
df_business = hcx.table('business')

print("Top businesses with names and most five-star ratings:")
result = df_review.filter(col("rev_stars") == 5.0) \
                  .groupBy("rev_business_id") \
                  .agg(count("rev_stars").alias("five_star_count")) \
                  .join(df_business, df_review.rev_business_id == df_business.business_id) \
                  .select("business.name", "five_star_count") \
                  .orderBy(col("five_star_count").desc()) \
                  .limit(10)