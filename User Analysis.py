from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .appName("Spark Example") \
    .master("local") \
    .getOrCreate()

hc = HiveContext(spark.sparkContext)

# 1. Analyze the number of users joining each year

result = hc.sql("""
    SELECT YEAR(user_yelping_since) as joining_year, COUNT(user_id) as user_count 
    FROM users 
    GROUP BY YEAR(user_yelping_since) 
    ORDER BY joining_year
""")
print("Analyzing the number of users joining each year...")



# 2.Identify top reviewers based on review_count
result = hc.sql("""
    SELECT user_name, user_review_count 
    FROM users 
    ORDER BY user_review_count DESC 
    LIMIT 20
""")
print("Top reviewers based on review count:")


# 3. Identify the most popular users based on fans
result = hc.sql("""
    SELECT user_name, user_fans 
    FROM users 
    ORDER BY user_fans DESC 
    LIMIT 20
""")
print("Most popular users based on fans:")


# 4. Calculate the ratio of elite users to regular users each year

result = hc.sql("""
    SELECT YEAR(user_yelping_since) as join_year, 
           SUM(CASE WHEN user_elite != '' THEN 1 ELSE 0 END) as elite_count, 
           SUM(CASE WHEN user_elite = '' THEN 1 ELSE 0 END) as regular_count, 
           (SUM(CASE WHEN user_elite != '' THEN 1 ELSE 0 END) / COUNT(*)) as elite_ratio 
    FROM users 
    GROUP BY YEAR(user_yelping_since) 
    ORDER BY join_year
""")
print("Ratio of elite users to regular users each year:")


# 5. Display the proportion of total users and silent users each year

result = hc.sql("""
    SELECT YEAR(user_yelping_since) as join_year, 
           COUNT(user_id) as total_users, 
           SUM(CASE WHEN user_review_count = 0 THEN 1 ELSE 0 END) as silent_users, 
           (SUM(CASE WHEN user_review_count = 0 THEN 1 ELSE 0 END) / COUNT(*)) as silent_user_ratio 
    FROM users 
    GROUP BY YEAR(user_yelping_since) 
    ORDER BY join_year
""")
print("Proportion of total users and silent users each year:")


# 6. Compute yearly statistics of new users, number of reviews, elite users, tips, and check-ins

result = hc.sql("""
    WITH user_stats AS (
        SELECT YEAR(u.user_yelping_since) AS join_year, 
               COUNT(DISTINCT u.user_id) AS new_users, 
               SUM(u.user_review_count) AS total_reviews, 
               SUM(CASE WHEN u.user_elite != '' THEN 1 ELSE 0 END) AS elite_users, 
               SUM(u.user_compliment_hot + u.user_compliment_more + u.user_compliment_profile + 
                   u.user_compliment_cute + u.user_compliment_list + u.user_compliment_note + 
                   u.user_compliment_plain + u.user_compliment_cool + u.user_compliment_funny + 
                   u.user_compliment_writer + u.user_compliment_photos) AS total_tips 
        FROM users u 
        GROUP BY YEAR(u.user_yelping_since)
    ), 
    checkin_stats AS (
        SELECT 
            YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(split_date), 'yyyy-MM-dd HH:mm:ss'))) AS checkin_year, 
            COUNT(*) AS total_checkins 
        FROM checkin 
        LATERAL VIEW EXPLODE(SPLIT(checkin_dates, ',')) exploded_table AS split_date 
        GROUP BY YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(split_date), 'yyyy-MM-dd HH:mm:ss')))
    )
    SELECT 
        u.join_year, 
        u.new_users, 
        u.total_reviews, 
        u.elite_users, 
        u.total_tips, 
        COALESCE(c.total_checkins, 0) AS total_checkins 
    FROM user_stats u 
    LEFT JOIN checkin_stats c ON u.join_year = c.checkin_year 
    ORDER BY u.join_year
""")

print("Yearly statistics of new users, reviews, elite users, tips, and check-ins:")
