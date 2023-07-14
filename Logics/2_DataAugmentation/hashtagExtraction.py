#!/usr/bin/env python3


import re
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col,split, explode, regexp_extract, udf
from pyspark.sql.types import StringType

from pyspark.sql import Row


import argparse
import ast

#====================================
#==========NLP======================
#===================================
import nltk
from nltk.tokenize import word_tokenize, PunktSentenceTokenizer
from nltk.corpus import stopwords
from nltk.tag import pos_tag

#versione nuova (modello pre addestrato spacy)

import spacy
nlp = spacy.load("en_core_web_sm")
# Download the required NLTK resources



#metodo di estrazione degli hashtag
def hastag_extraction(text):
    if type(text)==type("tweets"):
        hashtag_pattern = r"#(\w+)"  
        hashtags = re.findall(hashtag_pattern, text)
        out=[]
        for k in hashtags:
            out.append(k.lower())
        return str(out)
    else:
        return "[]"

#extrazione nouns (parole significative)
#NON FUNZIONA IN QUANTO CI SONO DEI PROBLEMI CON UDF e NLTK
def nouns_extraction_OLD(tweet):
    if type(tweet)==type("tweet"):
        sentence_tokenizer = PunktSentenceTokenizer()
        sentences = sentence_tokenizer.tokenize(tweet)
        words = [word_tokenize(sentence) for sentence in sentences]

        # Perform POS tagging to identify nouns
        tagged_words = [pos_tag(sentence) for sentence in words]
        nouns = []
        for sentence in tagged_words:
            nouns += [word for word, pos in sentence if pos.startswith('N')]

        # Remove stopwords
        stopwords_list = set(stopwords.words('english'))
        nouns = [noun for noun in nouns if noun.lower() not in stopwords_list]
        return str(nouns)
    else:
        return "[]"


#create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")


# Create a SparkSession
spark = SparkSession.builder.appName("Hashtag Extraction").getOrCreate()
# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

#Lettura del file csv
df = spark.read.csv(input_filepath, header=True, inferSchema=True)

# Register the function as a UDF
extract_hastags = udf(hastag_extraction, StringType())

# Perform the operation and add the result as a new column
result = df.withColumn("hashtags", extract_hastags(col("text")))

# Write the processed DataFrame to a new CSV file
result.write.csv(output_filepath, header=True)

# Stop the SparkSession
spark.stop()