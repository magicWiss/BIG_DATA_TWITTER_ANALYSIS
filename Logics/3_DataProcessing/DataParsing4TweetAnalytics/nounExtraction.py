from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
import spacy

# Create a SparkSession
spark = SparkSession.builder.appName("Noun Extraction").getOrCreate()

# Load the spaCy English language model
nlp = spacy.load("en_core_web_sm")

# Read the CSV file into a DataFrame
df = spark.read.csv("input.csv", header=True, inferSchema=True)
# Replace "input.csv" with the actual path and filename of your input CSV file

# Define a custom function for noun extraction
def extract_nouns(text):
    doc = nlp(text)
    nouns = [token.text for token in doc if token.pos_ == "NOUN"]
    return nouns

# Register the function as a UDF
extract_nouns_udf = udf(extract_nouns, ArrayType(StringType()))

# Apply the custom function to the "text" column and create a new column "nouns"
df_with_nouns = df.withColumn("nouns", extract_nouns_udf("text"))

# Show the resulting DataFrame
df_with_nouns.show()

# Stop the SparkSession
spark.stop()
