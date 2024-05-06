from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json
from pyspark.sql.types import StringType, StructType, StructField
from textblob import TextBlob

# Define the sentiment analysis function
def sentiment_analysis(text):
    """ Simple sentiment analysis function using TextBlob. """
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return 'Positive'
    elif analysis.sentiment.polarity == 0:
        return 'Neutral'
    else:
        return 'Negative'

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("SentimentAnalysis") \
    .getOrCreate()

# Define UDF for sentiment analysis
sentiment_udf = udf(sentiment_analysis, StringType())

# Define the schema for the JSON data
json_schema = StructType([
    StructField("tweet_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("text", StringType(), True),
    StructField("airline", StringType(), True),
    StructField("tweet_created", StringType(), True)
])

# Read data from Kafka and parse the JSON data
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "rawdata") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")
df = df.withColumn("jsonData", from_json(col("value"), json_schema)).select("jsonData.*")

# Perform sentiment analysis
df_with_sentiment = df.withColumn("sentiment", sentiment_udf(col("text")))

# Select relevant columns (e.g., tweet_id and sentiment)
output_df = df_with_sentiment.selectExpr("CAST(tweet_id AS STRING) as key", "to_json(struct(*)) as value")

# Write to Kafka
query = output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "sentiment_data") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .start()

query.awaitTermination()
