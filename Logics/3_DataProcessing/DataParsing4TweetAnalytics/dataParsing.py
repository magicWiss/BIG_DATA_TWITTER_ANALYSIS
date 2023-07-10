#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, concat, lit, udf
import argparse
import ast
from nltk.tokenize import word_tokenize
from pyspark.sql.types import ArrayType, StringType

from pyspark.sql import Row
from pyspark.sql.types import StringType
import nltk

nltk.download('punkt') 

#funzione di parsing mediante nltk
def parse_tweet(text):
    
    tokens = word_tokenize(text)  # Tokenize the tweet text
    # Apply further parsing or processing as needed
    output=' '.join(tokens)

#create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path


# Create a SparkSession
spark = SparkSession.builder.appName("CSV as Text Processing").getOrCreate()

# restituisce un RDD dopo avere caricato il file dato in input
df = spark.read.csv(input_filepath, header=True, inferSchema=True)

parse_tweet_udf = udf(parse_tweet, StringType())

parsed_df = df.withColumn("parsed_text", parse_tweet_udf("text"))

# Write the processed DataFrame to a new CSV file
parsed_df.write.csv(output_filepath, header=True)

