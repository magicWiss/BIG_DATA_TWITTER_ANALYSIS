from pyspark.sql import SparkSession
from pymongo import MongoClient
import json

with open(r"/home/matteowissel/BigDataFinal/BIG_DATA_TWITTER_ANALYSIS/Logics/4_LoadDataMongo/config.json","r") as f:
    config=json.load(f)

uri=config["uri"]
db=config["dbName"]
coll=config["coll_tweets"]

spark = SparkSession.builder \
    .appName("MongoDBExample") \
    .getOrCreate()

client = MongoClient(uri)
db = client[db]
collection = db[coll]

df = spark.createDataFrame(collection.find())

df.show()

client.close()
spark.stop()