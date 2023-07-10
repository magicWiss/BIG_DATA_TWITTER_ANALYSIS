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

# restituisce un RDD dopo avere caricato il file dato in input
input_RDD = spark.sparkContext.textFile(input_filepath)


header=input_RDD.first()

input2_RDD = input_RDD.filter(lambda row: row != header)

# splitta i record in campi sulla base della virgola 
rows = input2_RDD.map(lambda line: line.split(","))


#creazione delle relazioni
relationshis_allRels=rows.map(create_entry)

relationshis_allRels.saveAsTextFile(output_filepath)
