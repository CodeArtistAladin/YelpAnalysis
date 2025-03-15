%pyspark
# Convert user_friends from string to list of friends
users_df = users_df.withColumn("friends_list", F.split(F.col("user_friends"), ","))

# Explode the list of friends so each row represents a user and their friend
exploded_users_df = users_df.withColumn("friend", F.explode(F.col("friends_list")))

%pyspark
# Alias the DataFrame and rename columns to avoid ambiguity
user_ratings_a = user_ratings.alias("a").select(
    F.col("a.user_index").alias("user_a"),
    F.col("a.business_index").alias("business_index_a"),
    F.col("a.rating").alias("rating_a")
)

%pyspark
user_ratings_b = user_ratings.alias("b").select(
    F.col("b.user_index").alias("user_b"),
    F.col("b.business_index").alias("business_index_b"),
    F.col("b.rating").alias("rating_b")
)

%pyspark
from pyspark.sql import functions as F

# Filter out cases where user_a and user_b are the same (no self-recommendation)
similar_users_df = similar_users_df.filter(similar_users_df["user_a"] != similar_users_df["user_b"])

%pyspark
# Join with users_df to get the user names
friend_recommendations = similar_users_df.join(
    users_df,
    similar_users_df["user_b"] == users_df["user_id"],
    "inner"
).select(
    similar_users_df["user_a"],
    users_df["user_name"].alias("user_b_name"),
    similar_users_df["similarity"]
)

# Display the output
friend_recommendations.show(truncate=False)

%pyspark
# Sort by similarity in descending order and limit to the first 10,000 rows
friend_recommendations = friend_recommendations.orderBy(F.col("similarity").desc()).limit(10000)

# Show the first 20 rows with user names only (no user ids)
friend_recommendations.select("user_b_name", "similarity").show(20, truncate=False)

