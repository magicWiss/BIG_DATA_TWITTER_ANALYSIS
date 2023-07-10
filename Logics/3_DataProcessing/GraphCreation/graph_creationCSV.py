#!/usr/bin/env python3
"""spark application"""
import argparse
from pyspark.sql import SparkSession

USER_ID=0
TWEET_ID=1
CONV_AUTH_ID=2
CONV_ID=3
TIMESTAMP=4
TEXT=5
SENTIMENTO=6

def create_entry(data):
    try:
        user_id=data[USER_ID]
        conv_auth=data[CONV_AUTH_ID]
        tweet_id=data[TWEET_ID]

        if conv_auth!="":
        
            return (user_id,tweet_id,conv_auth)
    except IndexError:
            print(data)

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path


# Create a SparkSession
spark = SparkSession.builder.appName("CSV as Text Processing").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv(input_filepath, header=True, inferSchema=True)

#filtro i dati che non hanno conv_auth_id
filtered_df = df.filter(df[CONV_AUTH_ID].isNotNull() | (df[CONV_AUTH_ID] != ""))

# Select the desired columns (1, 3, and 6)
selected_df = filtered_df.select(df.columns[USER_ID], df.columns[TWEET_ID], df.columns[CONV_AUTH_ID])

# Write the selected DataFrame to a new CSV file
selected_df.write.csv(output_filepath, header=True)

# Stop the SparkSession
spark.stop()
