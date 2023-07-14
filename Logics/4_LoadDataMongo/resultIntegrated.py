#!/usr/bin/env python3
from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import col

#create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", nargs="+", help="Input file paths")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path




# Define a list of file paths
file_paths = input_filepath #tutti i file che verranno combinati

spark = SparkSession.builder.appName("CSV Integration").getOrCreate()

# Read the first CSV file into a DataFrame
df = spark.read.csv(file_paths[0], header=True, inferSchema=True)

joined_df = spark.read.csv(input_filepath, header=True, inferSchema=True)
# Read the first CSV file into a DataFrame



print("FILE PATHS:",str(file_paths))
# Perform the join operation based on a common key or column
for path in file_paths[1:]:
    
    df = spark.read.csv(path, header=True, inferSchema=True)
    df.show()
    joined_df = joined_df.join(df, jon=["tweet_id"], how="inner")


# Save the frequent itemsets as a CSV file
joined_df.write.csv(output_filepath, header=True)
# Close the SparkSession
spark.stop()