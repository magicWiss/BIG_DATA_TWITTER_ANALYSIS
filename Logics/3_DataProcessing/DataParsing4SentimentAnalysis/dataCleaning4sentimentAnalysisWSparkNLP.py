


import re
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, concat_ws
from functools import partial
from sparknlp.pretrained import PretrainedPipeline

def sentiment_analysis(df):
    try:
        return pipeline.transform(df)
    except:
        return df

def delete_hashtag_symbol(text):
    return re.sub(r'#', '', text)

def find_none_spaced_words(text):          
    text_with_space = re.sub(r"(\w)([A-Z])", r"\1 \2", text)
    return text_with_space

def delete_double_spaces(text):
    text_without_double_spaces = re.sub(r"\s+", " ", text)
    return text_without_double_spaces

def to_lower(text):
    return " ".join(x.lower() for x in text.split())

# Define a custom function to delete URLs from text
def delete_urls(text):
    url_pattern = r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
    cleaned_text = re.sub(url_pattern, "", text)
    return cleaned_text

def clean_text(text):
    # in this case we still need stopwords because they could be useful for sentiment analysis. The same states for punctuation and emojis
    if type(text)==type("s"):
        parsed_text=delete_urls(text)
        parsed_text=delete_hashtag_symbol(parsed_text)
        parsed_text=to_lower(parsed_text)
        parsed_text=delete_double_spaces(parsed_text)                
    else:
        parsed_text=text

    return parsed_text

#create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file paths")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# Define a list of file paths
file_paths = input_filepath #tutti i file che verranno combinati

spark = SparkSession.builder.appName("CSV Integration")\
    .master("local[*]")\
    .config("spark.driver.memory","16G")\
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.kryoserializer.buffer.max", "2000M")\
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.0.0")\
        .getOrCreate()

# load pipeline from disk or SparkNLP's model's hub
pipeline = PretrainedPipeline('analyze_sentimentdl_use_twitter', lang = 'en')
# pipeline = PretrainedPipeline.from_disk("file:///home/federico/BD/BIG_DATA_TWITTER_ANALYSIS/analyze_sentimentdl_use_twitter_en/")

# Read the first CSV file into a DataFrame
df = spark.read.csv(input_filepath, header=True, inferSchema=True)

clean_text_partial = partial(clean_text)

parse_text_udf = udf(clean_text_partial, StringType())

# Apply the custom parsing function to the "text" column and create a new column "parsed_text"
df_with_parsed_text = df.withColumn("text", parse_text_udf("text"))

# sentiment_analysis_partial = partial(sentiment_analysis)
# sentiment_analysis_udf = udf(sentiment_analysis_partial, StringType())
df_with_parsed_text = sentiment_analysis(df_with_parsed_text)

df_with_parsed_text = df_with_parsed_text.drop(*["document","sentence_embeddings"]).withColumn("sentiment",  concat_ws(',', 'sentiment.result'))

# Write the processed DataFrame to a new CSV file
df_with_parsed_text.write.csv(output_filepath, header=True)

# Stop the SparkSession
spark.stop()