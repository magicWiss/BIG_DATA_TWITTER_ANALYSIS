#!/usr/bin/env python3

#Allineamento dello schema del dataset UkraineRussia allo schema qatar.
#Eliminazione dei tweet non inglese (campo len)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, concat, lit, udf
import argparse
import ast

from pyspark.sql import Row
from pyspark.sql.types import StringType



    
    


    



#create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# Create a SparkSession
spark = SparkSession.builder.appName("CSV Integration").getOrCreate()


# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

#Lettura del file csv
df = spark.read.csv(input_filepath, header=True, inferSchema=True)


df=df.drop(*["sentiment"])

# Write the processed DataFrame to a new CSV file
df.write.csv(output_filepath, header=True)



# Stop the SparkSession
spark.stop()