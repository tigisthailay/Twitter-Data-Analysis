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
        df = df.T.drop_duplicates().T
        
        return df
    def convert_to_datetime(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert column to datetime
        """
        df['created_at']= pd.to_datetime(df['created_at'])
        
        df = df[df['created_at'] >= '2020-12-31' ]
        
        return df
    
    def convert_to_numbers(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert columns like polarity, subjectivity, retweet_count
        favorite_count etc to numbers
        """
        #df = df.astype({"polarity":"int", "subjectivity":"int", "retweet_count":"int", "favorite_count":"int"})        

        df['polarity'] = pd.to_numeric(df["polarity"])
        df['subjectivity'] = pd.to_numeric(df["subjectivity"])
        df['retweet_count'] = pd.to_numeric(df["retweet_count"])
        df['favorite_count'] = pd.to_numeric(df["favorite_count"])
        
        df = df[df['polarity'], df['subjectivity'], df['retweet_count'], df['favorite_count']]

        
        return df
    
    def remove_non_english_tweets(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove non english tweets from lang
        """
        
        df = ----
        
        return df
