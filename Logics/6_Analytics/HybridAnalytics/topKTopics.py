import pandas as pd

topic2wordcount = pd.read_json('D:\Projects\BIG_DATA_TWITTER_ANALYSIS\TweetAnalytics\\topic2Wordcount\\result.json').T
topic2wordcount['total'] = pd.to_numeric(topic2wordcount['total'])
top_topics = topic2wordcount.nlargest(3, 'total').index.tolist()

tot_tweets_df = pd.read_csv('D:\Projects\BIG_DATA_TWITTER_ANALYSIS\Dataset\Collections\Tweets\Tweets.csv').drop(['timestamp','hashtags','parsed_text','prediction','sentiment','sentiment_spark'], axis=1).rename(columns={"author_id": "Source", "conversation_author_id": "Target"})

tweets_dfs = dict()
for topic in top_topics:
    tweets_dfs[topic] = tot_tweets_df[tot_tweets_df['limitedTopics'].apply(lambda arr: str(topic) in arr[:3])]

for topic in tweets_dfs.keys():
    tweets_dfs[topic].to_csv(f'D:\Projects\BIG_DATA_TWITTER_ANALYSIS\GephiViews\Raw Graph\edgesByTopics\edges{topic}.csv', index=False)

