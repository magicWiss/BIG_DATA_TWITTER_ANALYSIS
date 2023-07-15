from pyspark.sql import SparkSession


#Connettore
my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Twitter_Analytics.Tweets") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Twitter_Analytics.Tweets") \
    .getOrCreate()

