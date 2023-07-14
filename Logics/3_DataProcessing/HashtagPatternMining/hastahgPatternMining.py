import ast
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import concat_ws
from pyspark.ml.fpm import FPGrowth
import argparse


# Define a custom function to transform hashtags column into a list
def parse_hashtags(hashtags):
    try:
        hashtags_list = ast.literal_eval(hashtags)
        return set(hashtags_list)
    except (SyntaxError, ValueError):
        return set()



parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file paths")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# Create a Spark session
spark = SparkSession.builder.appName("FrequentPatternMining").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv(input_filepath, header=True, inferSchema=True)



# Register the custom function as a UDF
parse_hashtags_udf = udf(parse_hashtags, ArrayType(StringType()))

# Apply the UDF to transform the hashtags column
df = df.withColumn("hashtags_list", parse_hashtags_udf("hashtags"))

# Select the transformed hashtags column
hashtags_df = df.select("hashtags_list")

# Apply FP-Growth to find frequent itemsets
fpGrowth = FPGrowth(itemsCol="hashtags_list", minSupport=0.1, minConfidence=0.2)
model = fpGrowth.fit(hashtags_df)

# Get the frequent itemsets
frequent_itemsets = model.freqItemsets
frequent_itemsets = frequent_itemsets.withColumn("items_str", concat_ws(",", "items")).drop("items")

# Save the frequent itemsets as a CSV file
frequent_itemsets.write.csv(output_filepath, header=True)

# Stop the Spark session
spark.stop()
