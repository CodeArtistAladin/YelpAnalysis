from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder \
    .appName("Spark Example") \
    .master("local") \
    .getOrCreate()

hc = HiveContext(spark.sparkContext)

# 1. Identify the 20 most common merchants in the U.S.
result = hc.sql("SELECT name, COUNT(name) as name_count FROM business GROUP BY name ORDER BY name_count DESC LIMIT 20")


# 2. top 10 cities with the most merchants in the U.S.
result = hc.sql("SELECT city, COUNT(*) as merchant_count FROM business GROUP BY city ORDER BY merchant_count DESC LIMIT 10")
print("Top 10 cities with the most merchants in the U.S.")

# 3. Identify the top 5 states with the most merchants in the U.S.
result = hc.sql("SELECT state, COUNT(*) as merchant_count FROM business GROUP BY state ORDER BY merchant_count DESC LIMIT 5")
print("Top 5 states with the most merchants in the U.S.")

# 4. Identify the 20 most common merchants in the U.S. and display their average ratings.
result = hc.sql("""
    SELECT name, COUNT(*) as merchant_count, AVG(stars) as avg_rating 
    FROM business 
    GROUP BY name 
    ORDER BY merchant_count DESC LIMIT 20 """)
print("Top 20 most common merchants in the U.S. and their average ratings:")

# 5. Identify the top 10 cities with the highest ratings.
result = hc.sql("""
    SELECT city, AVG(stars) as avg_rating FROM business GROUP BY city 
    ORDER BY avg_rating DESC LIMIT 10 """)
print("Top 10 cities with the highest ratings:")

# 6. Count the number of different categories.
result = hc.sql("SELECT COUNT(DISTINCT categories) as unique_categories FROM business ")

print("Number of different categories:")

# 7. Identify the top 10 most frequent categories and their count.
result = hc.sql("""
    SELECT categories, COUNT(*) as category_count FROM business GROUP BY categories 
    ORDER BY category_count DESC LIMIT 10 """)

print("Top 10 most frequent categories and their count:")

# 8. Identify the top 20 merchants that received the most five-star reviews.
result = hc.sql("""
    SELECT name, COUNT(*) as five_star_count FROM business WHERE stars = 5 
    GROUP BY name ORDER BY five_star_count DESC LIMIT 20 """)

print("Top 20 merchants with the most five-star reviews:")

# 9. Count the number of restaurant types (Chinese, American, Mexican).
result = hc.sql("""
    SELECT 
        SUM(CASE WHEN categories LIKE '%Chinese%' THEN 1 ELSE 0 END) as chinese_count,
        SUM(CASE WHEN categories LIKE '%American%' THEN 1 ELSE 0 END) as american_count,
        SUM(CASE WHEN categories LIKE '%Mexican%' THEN 1 ELSE 0 END) as mexican_count
    FROM business
    WHERE categories LIKE '%Restaurants%' """)

print("Number of restaurant types (Chinese, American, Mexican):")

# 10. Count the number of reviews for each restaurant type (Chinese, American, Mexican).
result = hc.sql("""
    SELECT 
        SUM(CASE WHEN categories LIKE '%Chinese%' THEN review_count ELSE 0 END) as chinese_reviews,
        SUM(CASE WHEN categories LIKE '%American%' THEN review_count ELSE 0 END) as american_reviews,
        SUM(CASE WHEN categories LIKE '%Mexican%' THEN review_count ELSE 0 END) as mexican_reviews
    FROM business
    WHERE categories LIKE '%Restaurants%' """)

print("Number of reviews for each restaurant type (Chinese, American, Mexican):")

# 11. Analyze the rating distribution for different restaurant types (Chinese, American, Mexican).
result = hc.sql("""
    SELECT 
        CASE 
            WHEN categories LIKE '%Chinese%' THEN 'Chinese'
            WHEN categories LIKE '%American%' THEN 'American'
            WHEN categories LIKE '%Mexican%' THEN 'Mexican'
        END as restaurant_type,
        stars,
        COUNT(*) as rating_count
    FROM business
    WHERE categories LIKE '%Restaurants%'
    GROUP BY restaurant_type, stars
    ORDER BY restaurant_type, stars """)

print("Rating distribution for different restaurant types (Chinese, American, Mexican):")

print("Added by Minat Lasani")