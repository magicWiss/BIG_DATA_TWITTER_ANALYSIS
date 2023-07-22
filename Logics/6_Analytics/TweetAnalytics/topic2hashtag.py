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
spark = SparkSession.builder.appName("Word2Count").getOrCreate()

# Load the CSV data into a DataFrame
# Replace 'path_to_csv_file' with the actual path to your CSV file
df = spark.read.csv(input_filepath, header=True, inferSchema=True)

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

