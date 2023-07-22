from pyspark.sql import SparkSession

import argparse
import json

TOP_K_WORDS=20
#filtraggio del wordcount
def filter_wordcount(wordCount):
    sorted_text_dict = dict(sorted(wordCount.items(), key=lambda item: item[1], reverse=True))

    # Get the first 20 entries
    first_20_entries = {k: sorted_text_dict[k] for k in list(sorted_text_dict)[:TOP_K_WORDS]}

    return first_20_entries



prediction_col="prediction"
text_parsed_col="parsed_text"

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file paths")
parser.add_argument("--output_path",type=str,help="Output file paths")


args = parser.parse_args()
input_filepath = args.input_path
output_filepath=args.output_path

parser = argparse.ArgumentParser()
parser.add_argument("--output_path",type=str,help="Output file paths")


args = parser.parse_args()
input_filepath = args.input_path
output_filepath=args.output_path

spark = SparkSession.builder \
    .appName("HashtagAggregationMongoDB") \
    .config("spark.mongodb.input.uri", "mongodb://your_mongodb_uri/Twitter/Tweets") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

#fields_to_read = ["hashtag", "pos", "med", "neg"]

fields_to_read=["prediction","parsed_text"]

df = spark.read \
    .format("mongo") \
    .option("uri", "bolt://localhost:7687/Twitter.Tweets") \
    .option("pipeline", '[{ "$project": { ' + ', '.join(f'"{field}": 1' for field in fields_to_read) + ' }}]') \
    .load()

prediction_map=dict()

#creo la struttura dati
for row in df.collect():
    prediction=str(row[prediction_col])
    text=row[text_parsed_col]
    if text!=None:
        text=text.split()
    else:
        text=""

    if prediction not in prediction_map:
        prediction_map[prediction]={"total":0,"wordcount":dict()}

    prediction_map[prediction]["total"]+=1
    
    for word in text:
        
        if word not in prediction_map[prediction]["wordcount"]:
            prediction_map[prediction]["wordcount"][word]=0
        
        prediction_map[prediction]["wordcount"][word]+=1

  


#filtraggio dei risultati
#ordinamento del wordcount
for k in prediction_map.keys():
    current_word_count=prediction_map[k]["wordcount"]
    filtered_word_count=filter_wordcount(current_word_count)
    prediction_map[k]["wordcount"]=str(filtered_word_count)

print(prediction_map)



with open(output_filepath,"w") as f:
    json_data=json.dumps(prediction_map,indent=4)
    f.write(json_data)

