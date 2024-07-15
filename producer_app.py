import streamlit as st 
from main import load_lottiefile ,clean_data
from streamlit_lottie import st_lottie_spinner
import pandas as pd
import time
import json
from kafka import KafkaProducer
from main import clean_data,streamdata,load_lottiefile
import time
import json
from datetime import  timedelta

lottie_file = "Animation-1716966760564.json"
lottie_animation = load_lottiefile(lottie_file)

with st.sidebar:
    st.image("NVlogo.png",width=150)
    st.header("Credit Card Fraud")
    

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

def login():
    st.session_state.logged_in = True

def logout():
    st.session_state.logged_in = False

if st.session_state.logged_in:
    if st.sidebar.button("Log out"):
        logout()
else:
    if st.sidebar.button("Log in"):
        login()

if not st.session_state.logged_in:
    st.title("Please log in to access the application.")
    st.stop()

@st.cache_resource
def connect_to_producer(ip="172.16.20.71:9092"):
    Producer = KafkaProducer(bootstrap_servers=[ip],
                         value_serializer=lambda x:json.dumps(x).encode('utf-8')
                         ) 
    return Producer

lottie_file = "Animation-1716966760564.json"
lottie_animation = load_lottiefile(lottie_file)

Producer=connect_to_producer()


st.title("Kafka Data Streamming")
st.write("This application will stream data using kafka producer to mimic near-realtime scenario!")

with st_lottie_spinner(lottie_animation, height=200, key="loading_animation"):
    time.sleep(2)
    uploaded_file=st.file_uploader("Choose a JSONL file", type="jsonl")

streaming = False
if uploaded_file is not None:
    try:
        df = pd.read_json(uploaded_file, lines=True)
        df=clean_data(df)
        st.subheader("Uploaded Data!")
        st.table(df.head())
        streaming=st.button("Start streaming!")
        
    except Exception as e:
        st.write(e)
transactions_placeholder = st.empty()

if streaming:
    start_time=df.transaction_datetime.loc[0]
    count=0
    try:
        st.write("Data Streaming started!")
        with st_lottie_spinner(lottie_animation, height=200, key="loading_animation1"):
            while count<=len(df):
                end_time=start_time+timedelta(minutes=1)
                data=streamdata(df,start_time,end_time)
                count=count+len(data)
                start_time=end_time
                if len(data)>0:
                    Producer.send("creditcardfraud", value=data.to_dict())
                    with transactions_placeholder:
                        placeholder=st.empty()
                        placeholder.text("Transaction Deails")
                        st.table(data[['cc_num','trans_num', 'amt', 'merchant', 'transaction_datetime']])
                    time.sleep(30)
                else:
                    continue
                
                
        st.write("Data streaming completed!")    
    except Exception as e:
        st.write(f"Uable to streamdata due to {e}")


    

