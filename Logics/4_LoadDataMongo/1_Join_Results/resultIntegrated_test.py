from pyspark.sql import SparkSession
import argparse
# Create a SparkSession
spark = SparkSession.builder \
    .appName("CSVJoinExample") \
    .getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file paths")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# Load CSV files into PySpark DataFrames
df1 = spark.read.csv(input_filepath, header=True, inferSchema=True)
df2 = spark.read.csv(output_filepath, header=True, inferSchema=True)
# Load additional CSV files if needed
df2.show()
# Perform the join operation based on a common key or column
joined_df = df1.join(df2, on="tweet_id",how="inner")
# Adjust join condition and type as per your requirements
joined_df.show()
# Optionally, perform additional transformations or filter the joined DataFrame as needed

# Display the joined DataFrame
joined_df.write.csv(r"file:///home/matteowissel/BigDataFinal/BIG_DATA_TWITTER_ANALYSIS/Dataset/CuratedData/IntegrateResult/TGT/out.csv", header=True)

# Close the SparkSession
spark.stop()