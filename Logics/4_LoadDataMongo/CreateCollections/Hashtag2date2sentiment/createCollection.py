#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, concat, lit, udf
import argparse
import ast
from pyspark.sql.functions import from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, ArrayType
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,avg
from datetime import datetime



parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file paths")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path
# Step 1: Create a SparkSession
spark = SparkSession.builder \
    .appName("ExtractColumnsAndExplodeHashtags") \
    .getOrCreate()
df = spark.read.csv(input_filepath, header=True, inferSchema=True)

df = df.withColumn("date", from_unixtime("timestamp"))
convert_to_list = udf(lambda x: ast.literal_eval(x) if x else [], ArrayType(StringType()))

# Step 3: Extract columns and explode hashtags
output_df = df.select("sentiment", "hashtags", "date") \
    .withColumn("hashtag_list", convert_to_list(df.hashtags)) \
    .withColumn("sentiment_list",convert_to_list(df.sentiment)) \
    .select("hashtag_list", "date", "sentiment_list")

# Step 4: Split sentiment column into three separate columns
output_df = output_df.select(
    output_df.hashtag_list,
    output_df.date,
    output_df.sentiment_list[0].alias("positive"),
    output_df.sentiment_list[1].alias("medium"),
    output_df.sentiment_list[2].alias("negative")
)

# Step 5: Show the resulting DataFrame
output_df.show()

#normalizzazione
#creazione dell'output
schema = StructType([
    #StructField("id", StringType(),nullable=False),
    StructField("Hashtag", StringType(), nullable=False),
    StructField("Timestamp", TimestampType(), nullable=True),
    StructField("Positive", FloatType(), nullable=True),
    StructField("Medium", FloatType(), nullable=True),
    StructField("Negative", FloatType(), nullable=True)])

data=[]
for row in output_df.collect():
    hastags=row["hashtag_list"]
    timestamp=datetime.strptime(row["date"], '%Y-%m-%d %H:%M:%S')
    pos=float(row["positive"])
    med=float(row["medium"])
    neg=float(row["negative"])
    if len(hastags)>0:
        for h in hastags:
            data.append([h,timestamp,pos,med,neg])


df = spark.createDataFrame(data, schema)
agg_df = df.groupBy("Hashtag", "Timestamp").agg(avg("Positive").alias("avg_pos"),
                                           avg("Medium").alias("avg_med"),
                                           avg("Negative").alias("avg_neg"))
df.write.csv(output_filepath, header=True)
# Step 6: Stop the SparkSession
spark.stop()

