# gestione dei file json in input (generalmente derivanti dalle Twitter APIs)
import numpy as np
import ijson
import pandas as pd
from datetime import datetime
import time

import argparse
import csv


#create parser and set its arguments
argparser = argparse.ArgumentParser()
argparser.add_argument("--input_path", type=str, help="Input file path")
argparser.add_argument("--output_path", type=str, help="Output folder path")
argparser.add_argument("--save_usernames", type=str, help="Save Usernames")
# parse arguments
args = argparser.parse_args()
input_path, output_path, save_usernames = args.input_path, args.output_path, args.save_usernames

# conversione data in stringa -> timestamp
def dateToTimestampFromTwitter(date):
    dt = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
    return time.mktime(dt.timetuple())

tweetCount = 0
df = pd.DataFrame(columns=["author_id", "tweet_id", "conversation_id", "timestamp", "text"])

with open(input_path, 'rb') as input_file:
    parser = ijson.parse(input_file, multiple_values=True)
    
    curr_author = 0
    curr_tweet = 0
    curr_text = ''
    curr_date = ''
    curr_conversation = 0
    
    curr_user_id = 0   
    curr_username = ''

    conversation2tweets = {}
    tweet2author = {}
    tweet2text = {}
    tweet2date = {}
    tweetIds = set()
    authors = set()
       
    for prefix, event, value in parser:
        if prefix=="includes.tweets.item.text":# or prefix=="data.item.text":
            if curr_text != '' and curr_tweet!=0:
                df.loc[len(df.index)] = [curr_author, curr_tweet, curr_conversation, curr_date, curr_text]
            curr_text = value
        if prefix=="includes.tweets.item.author_id":# or prefix=="data.item.author_id":
            curr_author = value
            authors.add(value)
        if prefix=="includes.tweets.item.id":# or prefix=="data.item.id":
            #tweetIds.add(value)
            tweetCount += 1
            curr_tweet = value
            tweet2author[value] = curr_author
            tweet2text[value] = curr_text
        if prefix=="includes.tweets.item.conversation_id":# or prefix=="data.item.conversation_id":
            #tweetIds.add(value)
            curr_conversation = value
            """if value != curr_tweet:
                if conversation2tweets.get(value) == None:
                    values = []
                    values.append(curr_tweet)
                    conversation2tweets[value] = values
                else:
                    values = conversation2tweets.get(value)
                    values.append(curr_tweet)"""
        if prefix=="includes.tweets.item.created_at":# or prefix=="data.item.created_at":
            curr_date = dateToTimestampFromTwitter(value)
            tweet2date[curr_tweet] = curr_date

if(save_usernames):
    # salvataggio delle corrispondenze user_id <-> username per non effettuare altre chiamate per recuperare questa informazione
    df_users = pd.DataFrame(columns=["user_id", "username"])
    with open(input_path, 'rb') as input_file:
        parser = ijson.parse(input_file, multiple_values=True)

        curr_id = 0   
        curr_username = ''
        
        for prefix, event, value in parser:
            if prefix=="data.item.entities.mentions.item.username":# or prefix=="data.item.text":
                if curr_id != 0 and curr_username !='':
                    df_users.loc[len(df.index)] = [curr_id, curr_username]
                curr_username = value
            if prefix=="data.item.entities.mentions.item.id":# or prefix=="data.item.text":
                curr_id = value
    df_users.to_json(str.split(output_path, '.json')[0] + '_usernames' + '.json')

# generazione lista dei tweet ricavati per filtrare quelli di cui non si hanno abbastanza informazioni
tweets_ids_list = []
for tweet_id in df['tweet_id']:
    tweets_ids_list.append(tweet_id)

# filtraggio
df_filtered = df[df['conversation_id'].isin(tweets_ids_list)]

# corrispondenza tweet <-> autore
tweet2author = {}
c = 0
for user_id, tweet_id in zip(df_filtered['user_id'], df_filtered['tweet_id']):
    tweet2author[tweet_id] = user_id
    
# salvataggio del dataset finale contenente id_autore_tweet_risposta, id_autore_tweet_iniziale, timestamp_tweet, testo_tweet
df_final = pd.DataFrame(columns=["author_id",  "tweet_id", "conversation_author_id", "conversation_id", "timestamp", "text"])
errors = 0
for index, row in df_filtered.iterrows():
    try:
        df_final.loc[len(df_final.index)] = [row['author_id'], row["tweet_id"], tweet2author[row['conversation_id']], row["conversation_id"], row['timestamp'], row['text'].replace('\n', ' ')]
    except Exception as e:
        errors+=1
print(str(errors))

df_final.to_csv(output_path, index=False)

# rimozione righe vuote
with open(output_path) as input, open(output_path, 'w', newline='') as output:
    writer = csv.writer(output)
    for row in csv.reader(input):
        if any(field.strip() for field in row):
            writer.writerow(row)