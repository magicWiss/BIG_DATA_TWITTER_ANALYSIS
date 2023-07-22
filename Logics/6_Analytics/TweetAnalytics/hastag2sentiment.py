

import argparse
import json
import ast

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file paths")
parser.add_argument("--output_path",type=str,help="Output file paths")


args = parser.parse_args()
input_filepath = args.input_path
output_filepath=args.output_path
# Create a Spark session
spark = SparkSession.builder.appName("HashtagAggregation").getOrCreate()

df = spark.read.csv(input_filepath, header=True, inferSchema=True)

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