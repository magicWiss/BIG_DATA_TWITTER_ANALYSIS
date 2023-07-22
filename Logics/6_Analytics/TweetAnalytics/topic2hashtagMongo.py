from pyspark.sql import SparkSession

import argparse
import json
import ast

TOP_K_WORDS=20
#filtraggio del wordcount
def filter_wordcount(wordCount):
    sorted_hashtags_dict = dict(sorted(wordCount.items(), key=lambda item: item[1], reverse=True))

    # Get the first 20 entries
    first_20_entries = {k: sorted_hashtags_dict[k] for k in list(sorted_hashtags_dict)[:TOP_K_WORDS]}

    return first_20_entries



prediction_col="limitedTopics"
hashtag_col="hashtags"

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file paths")
parser.add_argument("--output_path",type=str,help="Output file paths")


args = parser.parse_args()
input_filepath = args.input_path
output_filepath=args.output_path

# Create a Spark session
spark = SparkSession.builder \
    .appName("HashtagAggregationMongoDB") \
    .config("spark.mongodb.input.uri", "mongodb://your_mongodb_uri/Twitter/Tweets") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

#fields_to_read = ["hashtag", "pos", "med", "neg"]

fields_to_read=["limitedTopics","hastags"]

df = spark.read \
    .format("mongo") \
    .option("uri", "bolt://localhost:7687/Twitter.Tweets") \
    .option("pipeline", '[{ "$project": { ' + ', '.join(f'"{field}": 1' for field in fields_to_read) + ' }}]') \
    .load()

prediction_map=dict()

#creo la struttura dati
for row in df.collect():
    prediction=ast.literal_eval(row[prediction_col])
    if prediction!=None:
        prediction=prediction[:3]
        for k in prediction:
            hashtags=row[hashtag_col]
            if hashtags!=None:
                hashtags=ast.literal_eval(hashtags)
            else:
                hashtags=[]

            if k not in prediction_map:
                prediction_map[k]={"total":0,"hashtags":dict()}

            prediction_map[k]["total"]+=1
            
            for word in hashtags:
                
                if word not in prediction_map[k]["hashtags"]:
                    prediction_map[k]["hashtags"][word]=0
                
                prediction_map[k]["hashtags"][word]+=1
  


#filtraggio dei risultati
#ordinamento del wordcount
for k in prediction_map.keys():
    current_word_count=prediction_map[k]["hashtags"]
    filtered_word_count=filter_wordcount(current_word_count)
    prediction_map[k]["hashtags"]=str(filtered_word_count)

print(prediction_map)



with open(output_filepath,"w") as f:
    json_data=json.dumps(prediction_map,indent=4)
    f.write(json_data)

