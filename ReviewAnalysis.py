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

# 7. word cloud
import nltk
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from nltk.tokenize import word_tokenize
from nltk import pos_tag

# Initialize a Spark session and HiveContext
spark = SparkSession.builder \
    .appName("WordCloudAnalysis") \
    .enableHiveSupport() \
    .getOrCreate()

hc = HiveContext(spark)

nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')

# Step 1: Query the `review` table from Hive to get the review text
reviews_df = hc.sql("SELECT rev_text FROM review")
# Sample 10% of the reviews and limit the review text to 500 characters
sampled_reviews_df = reviews_df.sample(fraction=0.1)
# Limit the review text length to 500 characters to reduce data size
trimmed_reviews_df = sampled_reviews_df.rdd.map(lambda row: row['rev_text'][:500])
# Collect the data in smaller chunks to avoid exceeding spark.driver.maxResultSize
review_texts = trimmed_reviews_df.collect()

# Function to extract and filter nouns and adjectives (for word cloud)
def filter_nouns_adjectives(text):
    tokens = nltk.word_tokenize(text)                      # Tokenize the text
    tagged_tokens = nltk.pos_tag(tokens)                    # Perform POS tagging
    filtered_words = [word for word, pos in tagged_tokens   # Keep nouns & adjectives
                      if pos in ['NN', 'NNS', 'JJ', 'JJR', 'JJS']]
    return ' '.join(filtered_words)

# Apply the function to filter nouns and adjectives from the collected review texts
filtered_texts = ' '.join([filter_nouns_adjectives(text) for text in review_texts])
# Generate a word cloud from the filtered text
wordcloud = WordCloud(width=800, height=400, background_color='white').generate(filtered_texts)

# Display the word cloud
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')  # Hide axis for cleaner display
plt.show()


# 8. Construct a word association graph.

import networkx as nx
from nltk.tokenize import word_tokenize
from collections import Counter
import matplotlib.pyplot as plt

review = hc.sql("SELECT rev_text FROM review")


# Tokenize and build word pairs (co-occurrences)
word_pairs = []
for review in reviews:
    tokens = word_tokenize(review.lower())
    for i in range(len(tokens)-1):
        word_pairs.append((tokens[i], tokens[i+1]))

# Create a co-occurrence graph
G = nx.Graph()
for pair in word_pairs:
    if not G.has_edge(pair[0], pair[1]):
        G.add_edge(pair[0], pair[1], weight=1)
    else:
        G[pair[0]][pair[1]]['weight'] += 1

# Draw the graph
pos = nx.spring_layout(G)
nx.draw(G, pos, with_labels=True, node_size=5000, node_color="lightblue", font_size=10)
edge_labels = nx.get_edge_attributes(G, 'weight')
nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
plt.show()


print("edited by Surjo")
