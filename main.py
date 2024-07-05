
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
        
    
def clean_data(data):
    df=pd.DataFrame(data)
    df.is_fraud=-1
    df.transaction_datetime=pd.to_datetime(df.transaction_datetime)
    df.sort_values(by=["transaction_datetime"],axis=0,inplace=True)
    df=df[['ssn', 'cc_num', 'first', 'last', 'gender','lat', 'long', 'city_pop', 'job', 'dob', 'acct_num',
       'trans_num','category', 'amt', 'is_fraud', 'merchant','merch_lat', 'merch_long', 'transaction_datetime', 'user_loc_id',
       'merch_loc_id']]
    return df
    
    
def load_lottiefile(filepath: str):
    with open(filepath, "r") as f:
        return json.load(f)
    

def streamdata(df,start_time,end_time):
    df= df[(df['transaction_datetime'] >= start_time) & (df['transaction_datetime'] < end_time)]
    df.transaction_datetime=df.transaction_datetime.astype("str")
    return df