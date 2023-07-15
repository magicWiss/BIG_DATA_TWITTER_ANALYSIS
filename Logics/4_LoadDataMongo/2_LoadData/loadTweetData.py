from pyspark.sql import SparkSession
from pymongo import MongoClient
import json
import argparse

MONGO_DB_FORMAT="com.mongodb.spark.sql.DefaultSource"
def get_config(coll_name):
    path=r"/home/matteowissel/BigDataFinal/BIG_DATA_TWITTER_ANALYSIS/Logics/4_LoadDataMongo/2_LoadData/config.json"
    with open(path,"r")as f:
        data=json.load(f)

    uri=data["uri"]
    db=data["dbName"]
    collection=data[coll_name]

    return [uri,db,collection]



#Connettore

spark = SparkSession.builder.appName("MongoDbIntegration").getOrCreate()







parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file paths")
parser.add_argument("--collection",type=str,help="Collection file paths")


args = parser.parse_args()
input_filepath = args.input_path
collection_name=args.output_path

#lettura del file file di info
configuration=get_config(collection_name)

df = spark.read.csv(input_filepath, header=True, inferSchema=True)


df.write \
    .format(MONGO_DB_FORMAT) \
        .option("uri", configuration[0]+"/"+configuration[1]+"."+configuration[2]) \
        .option("database", configuration[1]) \
        .option("collection", configuration[2]) \
        .mode("append") \
        .save()






spark.stop()