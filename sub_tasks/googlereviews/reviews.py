from re import X
import sys
from numpy import nan
sys.path.append(".")
import requests
import psycopg2
import pandas as pd
from airflow.models import Variable 
from sub_tasks.data.connect import (pg_execute, engine) 
from sub_tasks.googlereviews.refresh_token import (refresh_tokens)
conn = psycopg2.connect(host="10.40.16.19",database="mabawa", user="postgres", password="@Akb@rp@$$w0rtf31n")

def fetch_reviews():

    token = refresh_tokens()

    locations = pd.read_sql("""
        SELECT distinct location_id
        FROM mabawa_staging.source_locations;""", con=engine)

    payload={}
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }

    reviews_df = pd.DataFrame()
    for index, row in locations.iterrows():
        text = row['location_id']
        print(text)
        url = f"https://mybusiness.googleapis.com/v4/{text}/reviews"
        response = requests.request("GET", url, headers=headers, data=payload)
        data = response.json()
        if bool(data) == False:
            continue
        data1 = data['reviews']
        df = pd.DataFrame.from_dict(data1)
        df['reviewer']= df['reviewer'].apply(pd.Series)['displayName']
        reviews_df = reviews_df.append(df, ignore_index=True)
        
        reviewcount= data['totalReviewCount']
        if reviewcount > 50:
            nextPageToken = data['nextPageToken']
            #getting other pages
            while bool(nextPageToken)==True:
                token = nextPageToken
                print(token)
                urll = f"https://mybusiness.googleapis.com/v4/{text}/reviews?pageToken={token}"
                reply = requests.request("GET", urll, headers=headers, data=payload)
                api_data = reply.json()
                print(api_data)
                try:
                    api_data1 = api_data['reviews']
                    api_df = pd.DataFrame.from_dict(api_data1)
                    api_df['reviewer']= api_df['reviewer'].apply(pd.Series)['displayName']
                    reviews_df = reviews_df.append(api_df, ignore_index=True)
                except:
                    print('error')

                print('eof')
                
                try:
                    newPageToken = api_data['nextPageToken']
                except:
                    print("end")
                    break
                print(newPageToken)
                nextPageToken = newPageToken
        else:
            continue

    reviews_df['reply_time'] = reviews_df['reviewReply'].apply(pd.Series)['updateTime']
    reviews_df['reviewReply'] = reviews_df['reviewReply'].apply(pd.Series)['comment']
    reviews_df.rename (columns = {'reviewId':'review_id', 
                       'reviewer':'reviewer', 
                       'starRating':'star_rating',
                       'comment':'review_comment',
                       'createTime':'createdat',
                       'updateTime':'updatedat',
                       'name':'location_id'}
            ,inplace=True)

    query="""truncate mabawa_staging.source_google_reviews;"""
    query=pg_execute(query)

    reviews_df.to_sql('source_google_reviews', con = engine, schema='mabawa_staging', if_exists = 'append', index=False)
    
    return 'something' 

def create_source_google_reviews():

    query = """
    truncate mabawa_staging.google_reviews;
    insert into mabawa_staging.google_reviews
    SELECT location_id, location_name, store_code, 
    review_id, reviewer, star_rating, review_comment, 
    createdat, updatedat, review_reply, reply_time
    FROM mabawa_staging.v_google_reviews;
    insert into mabawa_dw.update_log(table_name, update_time) values('google_reviews', default);
    """

    query = pg_execute(query)

    return "Table Created"


# create_source_google_reviews()