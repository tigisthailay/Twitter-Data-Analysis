import pandas as pd

class Clean_Tweets:
    """
    The PEP8 Standard AMAZING!!!
    """
    def __init__(self, df:pd.DataFrame):
        self.df = df
        print('Automation in Action...!!!')
        
    def drop_unwanted_column(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove rows that has column names. This error originated from
        the data collection stage.  
        """
        unwanted_rows = df[df['retweet_count'] == 'retweet_count' ].index
        df.drop(unwanted_rows , inplace=True)
        df = df[df['polarity'] != 'polarity']
        
        return df
    
    def drop_duplicate(self, df:pd.DataFrame)->pd.DataFrame:
        """
        drop duplicate rows
        """
        
        #df = df.loc[:,~df.T.duplicated(keep='first')]
        #df = df.loc[:,~df.columns.duplicated()].copy()
        #df = df.T.drop_duplicates().T
        df = self.df.drop_duplicates(subset ='original_text')
        
        return df
    
    def convert_to_datetime(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert column to datetime
        """
        
        self.df['created_at']= pd.to_datetime(self.df['created_at'], errors='coerce')
        self.df = self.df[self.df['created_at'] >= '2020-12-31' ]
        
        return self.df
    
    def convert_to_numbers(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert columns like polarity, subjectivity, retweet_count
        favorite_count etc to numbers
        """
    
        
        self.df['polarity'] = pd.to_numeric(self.df["polarity"],errors = 'coerce')
        self.df['subjectivity'] = pd.to_numeric(self.df["subjectivity"],errors = 'coerce')
        self.df['retweet_count'] = pd.to_numeric(self.df["retweet_count"],errors = 'coerce')
        self.df['favorite_count'] = pd.to_numeric(self.df["favorite_count"],errors = 'coerce')
        
        return self.df
    
    def remove_non_english_tweets(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove non english tweets from lang
        """
        
        self.df = self.df.drop(self.df[self.df['lang'] != 'en'].index)
        
        return self.df
    

if __name__ == "__main__":
    df = pd.read_csv("processed_tweet_data.csv")
    tweets = Clean_Tweets(df)
    df = tweets.drop_duplicate(df)
    df = tweets.remove_non_english_tweets(df)
    df = tweets.convert_to_datetime(df)
    df = tweets.drop_unwanted_column(df)
    df = tweets.convert_to_numbers(df)
    print(df['polarity'][0:5])
    
    df.to_csv('clean_processed_data.csv')
    print('File Successfully Saved.!!!')
