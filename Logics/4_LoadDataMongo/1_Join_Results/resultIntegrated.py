#!/usr/bin/env python3
from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import col
import json





#create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file paths")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

with open(input_filepath,"r") as f:
    files=json.load(f)


file_paths=list(files.values())




spark = SparkSession.builder.appName("CSV Integration").getOrCreate()

for i in file_paths:
    print("FILE")
    print(i)

joined_df = spark.read.csv(file_paths[0], header=True, inferSchema=True)
# Read the first CSV file into a DataFrame
print("FIRST")
joined_df.show()



# Perform the join operation based on a common key or column
for path in file_paths[1:]:
    print("FILEEEEEEEEEEEEEEEEE")
    print(path)
    df = spark.read.csv(path, header=True, inferSchema=True)
    df.show()
    
    
    joined_df = joined_df.join(df, on=["tweet_id"], how="inner")

new_column_order = ["tweet_id","author_id","conversation_author_id","conversation_id","timestamp","hashtags","parsed_text","prediction","limitedTopics","sentiment_bert","sentiment_spark"]
joined_df = joined_df.select(*new_column_order)
# Save the frequent itemsets as a CSV file
joined_df.write.csv(output_filepath, header=True)
# Close the SparkSession
spark.stop()