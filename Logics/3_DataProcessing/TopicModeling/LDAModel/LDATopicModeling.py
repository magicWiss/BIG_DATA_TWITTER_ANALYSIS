
from pyspark.ml.feature import Tokenizer

import json
from pyspark.sql import SparkSession
from pyspark.ml.feature import CountVectorizer, IDF
from pyspark.ml.clustering import LDA
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from functools import partial
import argparse
import re

# Define a custom function to extract words from text
def extract_words(text):
    if text!=None:
        words = re.findall(r'\w+', text.lower())
        return words
    else:
        return []
    

def get_top_topics(topics,maxnumber):
    
    topics=sorted(topics)
    return str(topics[:maxnumber])
    
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file paths")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path


# Create a Spark session
spark = SparkSession.builder.appName("TopicModeling").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv(input_filepath, header=True, inferSchema=True)


    


# Define a UDF for the custom function
extract_words_udf = udf(extract_words, ArrayType(StringType()))

# Apply the UDF to extract words from the "text" column
df = df.withColumn("words", extract_words_udf(df["parsed_text"]))

# Create a CountVectorizer to convert text into a vector of token counts
countVectorizer = CountVectorizer(inputCol="words", outputCol="rawFeatures")
countVectorizerModel = countVectorizer.fit(df)
df = countVectorizerModel.transform(df)

# Compute the IDF (Inverse Document Frequency)
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(df)
df = idfModel.transform(df)

# Read hyperparameters from JSON file
with open(r"/home/matteowissel/BigDataFinal/BIG_DATA_TWITTER_ANALYSIS/Logics/3_DataProcessing/TopicModeling/LDAModel/hyperparam.json") as json_file:
    hyperparameters = json.load(json_file)

# Extract hyperparameters
numTopics = hyperparameters["numTopics"]
maxIter = hyperparameters["maxIter"]
maxTopicsPerRow = hyperparameters["maxTopicsPerRow"]

# Train the LDA model
lda = LDA(k=numTopics, maxIter=maxIter)
ldaModel = lda.fit(df)

# Get the topics and their corresponding weights
topics = ldaModel.describeTopics().select("termIndices", "termWeights")

# Show the topics and their corresponding terms
topics.show(truncate=False)

# Transform the input DataFrame to get the topic distributions
transformed = ldaModel.transform(df)

custom_partial = partial(get_top_topics, maxnumber=maxTopicsPerRow)

topics_udf = udf(custom_partial, StringType())


# Apply the UDF to limit the number of topics per row
transformed = transformed.withColumn("limitedTopics", topics_udf(transformed["topicDistribution"]))



# Select the desired columns for the final result
result = transformed.select("tweet_id","limitedTopics")

df_result = df.join(result, on="tweet_id").select(df["*"], result["limitedTopics"])

# Save the result to a CSV file
result.write.csv(output_filepath, header=True)

# Stop the Spark session
spark.stop()

