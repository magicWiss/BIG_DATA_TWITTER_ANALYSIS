import ast
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import concat_ws
from pyspark.ml.fpm import FPGrowth
import argparse
from hastagClass import Hastag
import ast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


hastag_col="hashtags"
user_col="author_id"
topics_col="limitedTopics"
cluster_col="prediction"
#sentiment_col="sentiment"

def get_cols():
        return ["hastag","total","users","clusters","topics","sentiment"]

def create_entry(id,total,user,topics,clusters):
    obj=Hastag(id,total,user,topics,clusters)
    return (id,obj)
    

def process_row(row):
    
    output=[]   #lista di dizionari {id:hastag,value:oggetto Hastag}
    hastags=ast.literal_eval(row[hastag_col])
    user=row[user_col]
    topics=ast.literal_eval(row[topics_col])
    clusters=row[cluster_col]

    


    
    
    
    return [hastags,user,topics,clusters]
    
def update_output(output,elements):
    hashtags=elements[0]
    user=elements[1]
    topics=elements[2]
    clusters=elements[3]

    for h in hashtags:
        current_hastag=h
        if current_hastag not in output:
            entry=create_entry(current_hastag,1,user,topics,clusters)
            output[entry[0]]=entry[1]
        
        else:
            old_obj=output[current_hastag]
            old_obj.update_Hastag(1,user,topics,clusters)
            output[current_hastag]=old_obj
    
    return output



parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file paths")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# Create a Spark session
spark = SparkSession.builder.appName("Hashtag collection creation").getOrCreate()

#VERSIONE LOCALE, I DATI DEVONO ESSERE PRESI DA MONGO
# Read the CSV file into a DataFrame
df = spark.read.csv(input_filepath, header=True, inferSchema=True)




output=dict()
# Step 4: Apply the custom function to each row using map
for row in df.collect():
    elements=process_row(row)
    output=update_output(output,elements)


#creazione dell'output
schema = StructType([
    #StructField("id", StringType(),nullable=False),
    StructField("Hastag", StringType(), nullable=False),
    StructField("Total", IntegerType(), nullable=True),
    StructField("Users", StringType(), nullable=True),
    StructField("Clusters", StringType(), nullable=True),
    StructField("Topics", StringType(), nullable=True),
    StructField("Sentiment", StringType(), nullable=True)
])
data=[]
for k in output.keys():
     obj=output[k]
     row=obj.to_row()
     data.append(row)





df = spark.createDataFrame(data, schema)
df.write.csv(output_filepath, header=True)
# Stop the Spark session
spark.stop()
