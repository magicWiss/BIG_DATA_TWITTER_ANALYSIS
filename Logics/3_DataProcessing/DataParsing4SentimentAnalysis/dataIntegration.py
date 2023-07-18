import pandas as pd
import glob

def string_to_array(text):
    if(isinstance(text, str)):
        return str.split(text, ',')
    return text

all_csv_spark = glob.glob('D:\Projects\BIG_DATA_TWITTER_ANALYSIS\Dataset\CuratedData\DataForSentimentAnalysis\parsed_data_SPARKNLP\*.csv')
all_csv_bert = glob.glob('D:\Projects\BIG_DATA_TWITTER_ANALYSIS\Dataset\CuratedData\DataForSentimentAnalysis\parsed_data_BERT\*.csv')

df_spark = pd.read_csv(all_csv_spark[0], error_bad_lines=False, dtype=str)
df_bert = pd.read_csv(all_csv_bert[0], error_bad_lines=False, dtype=str)

for csv in all_csv_spark[1:]:
    tmp = pd.read_csv(csv, error_bad_lines=False, dtype=str)
    df_spark = pd.concat([df_spark, tmp])

for csv in all_csv_bert[1:]:
    tmp = pd.read_csv(csv, error_bad_lines=False, dtype=str)
    df_bert = pd.concat([df_bert, tmp])

print(len(df_spark))
print(len(df_bert))

df_spark = df_spark.rename(columns={'sentiment': 'sentiment_spark'}).drop(['author_id','conversation_author_id','conversation_id', 'timestamp', 'text'], axis=1)
df_bert = df_bert.rename(columns={'sentiment': 'sentiment_bert'}).drop(['author_id','conversation_author_id','conversation_id', 'timestamp', 'parsed_text'], axis=1)
df_bert['sentiment_bert'] = df_bert['sentiment_bert'].apply(string_to_array)
df_tot = df_bert.merge(df_spark, how='inner', on='tweet_id')
print(df_bert)
print(df_tot)
df_tot.to_csv('D:\Projects\BIG_DATA_TWITTER_ANALYSIS\Dataset\CuratedData\DataForSentimentAnalysis\\all_sentiment.csv', index=False)