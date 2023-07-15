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

for path in file_paths:
    print("FILE")
    print(path)
spark = SparkSession.builder.appName("CSV Integration").getOrCreate()

# Read the first CSV file into a DataFrame
df = spark.read.csv(file_paths[0], header=True, inferSchema=True)


# Iterate over the remaining file paths and union the DataFrames
for path in file_paths[1:]:
    temp_df = spark.read.csv(path, header=True, inferSchema=True)
    df = df.union(temp_df)


total_rows_integrated=df.count()

# Find rows with the same value on a specific column
duplicate_rows = df.dropDuplicates(["tweet_id"])

# Drop one of the duplicate rows



total_rows_after_deduplication=duplicate_rows.count()

print("[Before deduplication]:",total_rows_integrated)
print("[After deduplication]:",total_rows_after_deduplication)
print("[DELTA]",total_rows_integrated-total_rows_after_deduplication)
# Show the integrated DataFrame
# Write the processed DataFrame to a new CSV file

specific_values = ["","0","1","2","3","4","5","6","7","8","9","0"]  # Example numeric values

# Delete rows with specific values in a column
filtered_df = duplicate_rows.filter(~col("author_id").isin(specific_values))

# Delete rows with specific values in a column


filtered_df.write.csv(output_filepath, header=True)
# Stop the SparkSession
spark.stop()

