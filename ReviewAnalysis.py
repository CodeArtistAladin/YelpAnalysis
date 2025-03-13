from pyspark.ml.feature import NGram
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, explode, split, col

spark = SparkSession.builder \
    .appName("Spark Example") \
    .master("local") \
    .getOrCreate()

hc = HiveContext(spark.sparkContext)

#1 Count the number of reviews per year.

result = hc.sql("""
    SELECT YEAR(rev_date) as year, COUNT(*) as review_count 
    FROM review 
    GROUP BY YEAR(rev_date) 
    ORDER BY year
""")


#2 Count the number of useful (helpful), funny (funny), and cool (cool) reviews.

result = hc.sql("""
    SELECT 
        SUM(rev_useful) as total_useful, 
        SUM(rev_funny) as total_funny, 
        SUM(rev_cool) as total_cool 
    FROM review
""")
print("Total useful, funny, and cool reviews:")

#3 Rank users by the total number of reviews each year.

result = hc.sql("""
    SELECT 
        rev_user_id, 
        YEAR(rev_date) as year, 
        COUNT(*) as review_count 
    FROM review 
    GROUP BY rev_user_id, YEAR(rev_date) 
    ORDER BY year, review_count DESC
""")

print("Rank users by the total number of reviews each year:")

#4 Extract the Top 20 most common words from all reviews.

words_df = hc.sql("SELECT rev_text FROM review")
words_df = words_df.select(explode(split(words_df.rev_text, " ")).alias("word"))
result = words_df.groupBy("word").count().orderBy("count", ascending=False).limit(20)

print("Top 20 most common words from all reviews:")

#5 Extract the Top 10 words from positive reviews (rating > 3).

positive_reviews = hc.sql("SELECT rev_text FROM review WHERE rev_stars > 3")
words_df = positive_reviews.select(explode(split(positive_reviews.rev_text, " ")).alias("word"))
result = words_df.groupBy("word").count().orderBy("count", ascending=False).limit(10)

print("Top 10 words from positive reviews (rating > 3):")


#6 Extract the Top 10 words from negative reviews (rating ≤ 3).
negative_words_df = hc.sql("SELECT explode(split(lower(text), '\\s+')) as word FROM review WHERE stars <= 3")
negative_word_count = negative_words_df.groupBy("word").count().orderBy(col("count").desc()).limit(10)
print("Top 10 words from negative reviews (rating ≤ 3):")



# 8. Construct a word association graph.

words_df = hc.sql("SELECT rev_text FROM review").withColumn("words", split("rev_text", " "))

# Create bigrams (word pairs)
ngram = NGram(n=2, inputCol="words", outputCol="bigrams")
ngram_df = ngram.transform(words_df)

# Count bigrams
bigram_counts = ngram_df.select(explode("bigrams").alias("bigram")).groupBy("bigram").count().orderBy("count", ascending=False).limit(20)

print("Top 20 word associations (bigrams):")

print("edited by Surjo")