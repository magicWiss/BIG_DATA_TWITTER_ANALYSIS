

from pyspark.sql import SparkSession

import argparse
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json
import ast







parser = argparse.ArgumentParser()
parser.add_argument("--output_path",type=str,help="Output file paths")


args = parser.parse_args()
input_filepath = args.input_path
output_filepath=args.output_path

spark = SparkSession.builder \
    .appName("HashtagAggregationMongoDB") \
    .config("spark.mongodb.input.uri", "mongodb://your_mongodb_uri/Twitter/Tweets") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Define the schema for the DataFrame
# Adjust the data types according to your data in MongoDB
schema = StructType([
    StructField("hashtag", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("pos", DoubleType(), True),
    StructField("med", DoubleType(), True),
    StructField("neg", DoubleType(), True)
])

# Load the data from MongoDB into a DataFrame
df = spark.read \
    .format("mongo") \
    .schema(schema) \
    .load()
# Create a Spark sessione, inferSchema=True)

# Group by hashtag and compute the average for pos, med, and neg
aggregated_df = df.groupBy("hashtag").agg(
    F.avg("Positive").alias("avg_pos"),
    F.avg("Medium").alias("avg_med"),
    F.avg("Negative").alias("avg_neg")
)
aggregated_df = aggregated_df.withColumn(
    "Overal_Sentiment",
    F.when(
        (F.col("avg_pos") >= F.col("avg_med")) & (F.col("avg_pos") >= F.col("avg_neg")),
        "positive"
    ).when(
        (F.col("avg_med") >= F.col("avg_pos")) & (F.col("avg_med") >= F.col("avg_neg")),
        "medium"
    ).otherwise("negative")
)

aggregated_df = aggregated_df.filter(~F.isnull("hashtag") & F.col("hashtag").cast("string").isNotNull())

aggregated_df.write.csv(output_filepath, header=True)