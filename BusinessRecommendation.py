# Step 1: Import necessary libraries
from pyspark.sql import HiveContext
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Step 2: Initialize Hive Context
hive_context = HiveContext(sc)

# Step 3: Load the first 200 rows from Hive tables
business_df = hive_context.table("default.business").limit(10000)
review_df = hive_context.table("default.review").limit(10000)
users_df = hive_context.table("default.users").limit(10000)

# Prepare interaction data (user_id, business_id, rating)
interaction_df = review_df.join(
    business_df,
    review_df.rev_business_id == business_df.business_id,
    "inner"
).select(
    review_df.rev_user_id.alias("user_id"),
    business_df.business_id.alias("business_id"),
    review_df.rev_stars.alias("rating")
)

# Create mappings for user and business indices
user_mapping = interaction_df.select("user_id").distinct().withColumn("user_index", F.monotonically_increasing_id())
business_mapping = interaction_df.select("business_id").distinct().withColumn("business_index", F.monotonically_increasing_id())

# Map user_id and business_id to numeric indices
interaction_df = interaction_df.join(user_mapping, "user_id").join(business_mapping, "business_id")

# Split data into training and testing
(training_data, test_data) = interaction_df.randomSplit([0.8, 0.2])

# Build ALS model
als = ALS(
    userCol="user_index",
    itemCol="business_index",
    ratingCol="rating",
    coldStartStrategy="drop",
    nonnegative=True
)
model = als.fit(training_data)

# Generate recommendations (5 recommendations per user)
user_recs = model.recommendForAllUsers(5)

# Explode recommendations to separate rows
recs_exploded = user_recs.withColumn("recommendations", F.explode("recommendations")) \
    .select("user_index", "recommendations.business_index", "recommendations.rating")

# Join recommendations with original mappings and business/user details
final_recs = recs_exploded \
    .join(user_mapping, "user_index") \
    .join(business_mapping, "business_index") \
    .join(business_df, "business_id") \
    .join(users_df, "user_id") \
    .select("user_name", "name", "rating")

# Get the top-rated business for each user
window_spec = Window.partitionBy("user_name").orderBy(F.desc("rating"))

top_business_per_user = final_recs.withColumn("rank", F.row_number().over(window_spec)) \
    .filter(F.col("rank") == 1) \
    .drop("rank") \
    .withColumnRenamed("name", "top_recommended_business")

# Limit to the top 20 rows
top_20_rows = top_business_per_user.limit(20)



