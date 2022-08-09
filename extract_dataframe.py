import json
import pandas as pd
from textblob import TextBlob


def read_json(json_file: str)->list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file
    
    Returns
    -------
    length of the json file and a list of json
    """
    
    tweets_data = []
    for tweets in open(json_file,'r'):
        tweets_data.append(json.loads(tweets))
    
    
    return len(tweets_data), tweets_data

class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe
    
    Return
    ------
    dataframe
    """
    def __init__(self, tweets_list):
        
        self.tweets_list = tweets_list

    # an example function
    def find_statuses_count(self)->list:
        statuses_count = [tweets['statuses_count'] for tweets in self.tweets_list]
        
        return statuses_count
        
    def find_full_text(self)->list:
        
        try:
            text = [tweets['full_text'] for tweets in self.tweets_list]
        except KeyError:
            text = None
        return text
    
    def find_sentiments(self, text)->list:
        
        #def sentiment_analysis(tweet):
        def getSubjectivity(text1)->list:
            return TextBlob(text1).sentiment.subjectivity
        
        #Create a function to get the polarity
        def getPolarity(text1)->list:
            return TextBlob(text1).sentiment.polarity
  
        #Create two new columns ‘Subjectivity’ & ‘Polarity’
        text['TextBlob_Subjectivity'] = text['text'].apply(getSubjectivity)
        text ['TextBlob_Polarity'] = text['text'].apply(getPolarity)
        def getAnalysis(score):
            if score < 0:
                return 'Negative'
            elif score == 0:
                return 'Neutral'
            else:
                return 'Positive'
        tweet ['TextBlob_Analysis'] = text['TextBlob_Polarity'].apply(getAnalysis )
        return text
        #polarity = [tweets['polarity'] for tweets in self.tweets_list]
        
        #return polarity, self.subjectivity

    def find_created_time(self)->list:
        created_at = [tweets['created_at'] for tweets in self.tweets_list]
        #for tweets in self.tweets_list:
            #created_at.append(tweets['created_at'])

       
        return created_at

    def find_source(self)->list:
        source = []
        for tweets in self.tweets_list:
            source.append(tweets['source'])
            

        return source

    def find_screen_name(self)->list:
        screen_name = []
        for tweets in self.tweets_list:
            screen_name.append(tweets['screen_name'])
            
        return screen_name

    def find_followers_count(self)->list:
        followers_count = []
        for tweets in self.tweets_list:
            followers_count.append(tweets['followers_count'])
        return followers_count

    def find_friends_count(self)->list:
        friends_count = []
        for tweets in self.tweets_list:
            friends_count.append(tweets['friends_count'])
        
        return friends_count
    
    def is_sensitive(self)->list:
        try:
            is_sensitive = [x['possibly_sensitive'] for x in self.tweets_list]
        except KeyError:
            is_sensitive = None

        return is_sensitive

    def find_favourite_count(self)->list:
        fav_count = [tweets['fav_count'] for tweets in self.tweets_list]
        
        return fav_count
    
    def find_retweet_count(self)->list:
        retweet_count = [tweets['retweet_count'] for tweets in self.tweets_list]
        
        return retweet_count
    
    def find_hashtags(self)->list:
        hashtags = [tweets['hashtags'] for tweets in self.tweets_list]
        
        return hashtags
    
    def find_mentions(self)->list:
        mentions = [tweets['mentions'] for tweets in self.tweets_list]
        
        return mentions

    def find_location(self)->list:
        try:
            location = self.tweets_list['user']['location']
        except TypeError:
            location = ''
        
        return location

    
        
        
    def get_tweet_df(self, save=False)->pd.DataFrame:
        """required column to be generated you should be creative and add more features"""
        
        columns = ['created_at', 'source', 'original_text','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
            'original_author', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place']
        
        created_at = self.find_created_time()
        source = self.find_source()
        text = self.find_full_text()
        polarity, subjectivity = self.find_sentiments(text)
        lang = self.find_lang()
        fav_count = self.find_favourite_count()
        retweet_count = self.find_retweet_count()
        screen_name = self.find_screen_name()
        follower_count = self.find_followers_count()
        friends_count = self.find_friends_count()
        sensitivity = self.is_sensitive()
        hashtags = self.find_hashtags()
        mentions = self.find_mentions()
        location = self.find_location()
        data = zip(created_at, source, text, polarity, subjectivity, lang, fav_count, retweet_count, screen_name, follower_count, friends_count, sensitivity, hashtags, mentions, location)
        df = pd.DataFrame(data=data, columns=columns)

        if save:
            df.to_csv('processed_tweet_data.csv', index=False)
            print('File Successfully Saved.!!!')
        
        return df

                
if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    #tg = 'C:\Users\Me\Desktop\10_Academy\data\global_twitter_data.json'
    columns = ['created_at', 'source', 'original_text','clean_text', 'sentiment','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
    'original_author', 'screen_count', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place', 'place_coord_boundaries']
    _, tweet_list = read_json('global_twitter_data.json')
    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df() 

    # use all defined functions to generate a dataframe with the specified columns above
