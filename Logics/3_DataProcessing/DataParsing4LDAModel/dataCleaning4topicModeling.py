


import string

from nltk.corpus import stopwords
import json
import glob
import re
import spacy
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, unix_timestamp, concat, lit, udf
from functools import partial




def remove_stops(text,stops):
        
        pattern = r'\b\d{1,2}/\d{1,2}/\d{4}\b'
        text= re.sub(pattern, '', text)
        text=text.replace("\n"," ")
        text=text.replace("-"," ")
        text=text.strip()
        text=text.lower()
        words=text.split()
        
        
        
        #rimozione stopwords
        final=[]
        for word in words:
            if word not in stops:
                
                    final.append(word)
        final=" ".join(final)

        #punti
        final=final.translate(str.maketrans("","",string.punctuation))
        #rimozione numeri
        final="".join([i for i in final if not i.isdigit()])

        #eliminazione doppi " "
        while "  " in final:
            final=final.replace("  "," ")
        
        return (final)



        
def lemming_text( text):
        # Load the Italian language model
        nlp = spacy.load("it_core_news_sm")
        # Process the text with spaCy
        doc = nlp(text)
        # Lemmatize each token in the text
        lemmas = [token.lemma_ for token in doc]
        # Print the lemmas
        text=" ".join(lemmas)
        return text
    
            
def find_none_spaced_words(text):
       
       
            
        text_with_space = re.sub(r"(\w)([A-Z])", r"\1 \2", text)

        
        return text_with_space

def delete_double_spaces(text):
        text_without_double_spaces = re.sub(r"\s+", " ", text)
        return text_without_double_spaces

def clean_text_from_emoj(tweet):

    # Remove emojis
    tweet = re.sub(r"\s+", " ", tweet.encode("ascii", "ignore").decode("utf-8"))

    # Remove emoticons
    emoticons = re.findall(r"(?::|;|=)(?:-)?(?:\)|\(|D|P)", tweet)
    tweet = re.sub(r"\s+", " ", tweet)
    for emoticon in emoticons:
        tweet = tweet.replace(emoticon, "")

    # Remove URLs
    tweet = re.sub(r"http\S+|www\S+|https\S+", "", tweet, flags=re.MULTILINE)

    return tweet.strip() 

# Define a custom function to delete URLs from text
def delete_urls(text):
    url_pattern = r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
    cleaned_text = re.sub(url_pattern, "", text)
    return cleaned_text

def clean_text( text,stops):
        
        
        
       

        if type(text)==type("s"):
                #rimozione di tutte le parole che non hanno lunghezza maggiore di 2
                parsed_text=clean_text_from_emoj(text)
                parsed_text=delete_urls(parsed_text)
                parsed_text=remove_stops(parsed_text,stops)                  #rimozione stopwords
                parsed_text=delete_double_spaces(parsed_text)
                #parsed_text=lemming_text(parsed_text)              #lemming delle parole
                
        else:
                parsed_text=text
                
        

        return parsed_text

#create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file paths")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path


#stop words inglesi di nltk
stops=stopwords.words("english")
# Define a list of file paths
file_paths = input_filepath #tutti i file che verranno combinati

spark = SparkSession.builder.appName("CSV Integration").getOrCreate()

# Read the first CSV file into a DataFrame
df = spark.read.csv(input_filepath, header=True, inferSchema=True)

custom_partial = partial(clean_text, stops=stops)

parse_text_udf = udf(custom_partial, StringType())

# Apply the custom parsing function to the "text" column and create a new column "parsed_text"
df_with_parsed_text = df.withColumn("parsed_text", parse_text_udf("text"))




final=df_with_parsed_text.drop(*["text"])
# Write the processed DataFrame to a new CSV file
final.write.csv(output_filepath, header=True)

# Stop the SparkSession
spark.stop()



