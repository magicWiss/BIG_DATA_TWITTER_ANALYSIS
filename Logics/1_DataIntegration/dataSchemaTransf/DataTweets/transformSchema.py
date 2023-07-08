#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, concat, lit, udf
import argparse
import ast

from pyspark.sql import Row
from pyspark.sql.types import StringType



    
def create_timestamp_col(df):
    df = df.withColumn("datetime", concat(col("date"), lit(" "), col("time")))

    # Convert the "datetime" column to Unix timestamp
    df = df.withColumn("timestamp", unix_timestamp("datetime"))

    # Drop the original "date", "time", and "datetime" columns if needed
    df = df.drop("date", "time", "datetime")

    return df



    
    
def extract_conv_author_id(column_value):

    
    if column_value==None:
        return None
    else:
        try:
            elements=ast.literal_eval(column_value)
            
            if type(elements)==type([1,2]):
                if len(elements)>=1:
                    elements=elements[0]["id"]
                    return int(elements)
                else:
                    return None

            else:
                return None

        except:
            return None
    
    


    



#create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# Create a SparkSession
spark = SparkSession.builder.appName("CSV Integration").getOrCreate()


# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

#Lettura del file csv
df = spark.read.csv(input_filepath, header=True, inferSchema=True)


#creazione del timestamp, unificando i valori date e time presenti
df=create_timestamp_col(df)



# Rearange delle colonne
columns1 = df[["user_id", "id", "reply_to","conversation_id","timestamp","tweet"]]

#definizione delle colonne di output
output_coulms=["author_id","tweet_id","conversation_author_id","conversation_id","timestamp","text"]


#registrazione funzione custom per l'estrazione di conv_auth_id
custom_logic_udf = udf(extract_conv_author_id, StringType())


#invocazione funzione custom
df= df.withColumn("conversation_author_id", custom_logic_udf("reply_to"))


#ridefinizione dei nomi delle colonne
df = df.withColumnRenamed("user_id", "author_id").withColumnRenamed("id", "tweet_id").withColumnRenamed("tweet","text")

#ridefinizione ordine delle colonne

df = df.select("author_id","tweet_id","conversation_author_id","conversation_id","timestamp","text")







# Write the processed DataFrame to a new CSV file
df.write.csv(output_filepath, header=True)



# Stop the SparkSession
spark.stop()