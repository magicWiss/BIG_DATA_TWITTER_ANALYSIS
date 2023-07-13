from pyspark.sql import SparkSession
from pyspark.ml.feature import CountVectorizer, IDF
from pyspark.ml.clustering import KMeans
from pyspark.sql.types import ArrayType, StringType
import json


from pyspark.sql.functions import udf


import argparse
import re

#Define a custom function to extract words from text
def extract_words(text):
    if text!=None:
        words = re.findall(r'\w+', text.lower())
        return words
    else:
        return []


parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file paths")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# Create a Spark session
spark = SparkSession.builder.appName("KMeansClustering").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv(input_filepath, header=True, inferSchema=True)

# Define the column to perform clustering on
text_column = "parsed_text"


# Define a UDF for the custom function
extract_words_udf = udf(extract_words, ArrayType(StringType()))

# Apply the UDF to extract words from the "text" column
df_train = df.withColumn("tokens", extract_words_udf(df["parsed_text"]))

# Compute the term frequencies
countVectorizer = CountVectorizer(inputCol="tokens", outputCol="term_freq")
countVectorizerModel = countVectorizer.fit(df_train)
df_train = countVectorizerModel.transform(df_train)

# Compute the IDF (Inverse Document Frequency)
idf = IDF(inputCol="term_freq", outputCol="features")
idfModel = idf.fit(df_train)
df_train = idfModel.transform(df_train)


# Read hyperparameters from JSON file
with open(r"/home/matteowissel/BigDataFinal/BIG_DATA_TWITTER_ANALYSIS/Logics/3_DataProcessing/TopicModeling/BOWModel/hyperparam.json") as json_file:
    hyperparameters = json.load(json_file)

# Extract hyperparameters
k = hyperparameters["k"]
seed = hyperparameters["seed"]


# Perform K-means clustering

kmeans = KMeans(k=k, seed=seed)
kmeansModel = kmeans.fit(df_train)

# Get the cluster labels for each row
predictions = kmeansModel.transform(df_train)

# Select the desired columns for the final result
result = predictions.select("tweet_id","prediction")

df_result = df.join(result, on="tweet_id").select(df["*"], result["prediction"])

df_result.write.csv(output_filepath, header=True)
# Stop the Spark session
spark.stop()