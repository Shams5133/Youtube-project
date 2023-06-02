import os
import streamlit as st
import pymongo
import mysql.connector
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.tools import argparser
import pandas as pd
import isodate

API_KEY = "AIzaSyB-xg8RyJe9Bd95t2oz4ugAQtCI2UmDv9w"

MONGODB_CONNECTION_URL = "mongodb://localhost:27017"
MONGODB_DATABASE_NAME = "youtube_data"
MONGODB_COLLECTION_NAME = "channel_data"

MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"
MYSQL_DATABASE = "youtube_data"
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
query = ""


youtube = build('youtube', 'v3', developerKey=API_KEY)

mongo_client = pymongo.MongoClient(MONGODB_CONNECTION_URL)
mongo_db = mongo_client[MONGODB_DATABASE_NAME]
mongo_collection = mongo_db[MONGODB_COLLECTION_NAME]

mysql_conn = mysql.connector.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    database=MYSQL_DATABASE,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD
)
mysql_cursor = mysql_conn.cursor()


def get_channel_data(channel_id):
    try:
        channels_response = youtube.channels().list(
            part='snippet,statistics,contentDetails',
            id=channel_id
        ).execute()

        channel_info = channels_response['items'][0]['snippet']
        statistics = channels_response['items'][0]['statistics']
        content_details = channels_response['items'][0]['contentDetails']

        channel_data = {
            'channel_id': channel_id,
            'channel_name': channels_response['items'][0]['snippet']['title'],
            'channel_views': statistics['viewCount'],
            'channel_videocount': statistics['videoCount'],
            'channel_description': channel_info['description'],
            'playlist_id': content_details['relatedPlaylists']['uploads']
        }

        return channel_data

    except HttpError as e:
        st.error(f'An HTTP error in get_channel_data{e.resp.status} occurred: {e.content}')


def get_video_data(playlist_id):
    try:
        playlist_response = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=5
        ).execute()
        videos = []
        for playlist_item in playlist_response['items']:
            video_info = playlist_item['snippet']
            video_id = video_info['resourceId']['videoId'],
            video_response = youtube.videos().list(
                part = "snippet, contentDetails, statistics",
                id = video_info['resourceId']['videoId']
                ).execute()
            print(video_response)
            print('\n\n')
            comment_count = video_response['items'][0]['statistics']['commentCount']
            comments = []
            # print(comment_count)
            if (comment_count!='0'):
                # print("fetching comments")
                comment_response = youtube.commentThreads().list(
                    part = "snippet,replies",
                    videoId = video_info['resourceId']['videoId']
                    ).execute()

                for commentData in comment_response['items']:
                    comment_data = {
                        'comment_id': commentData['id'],
                        'video_id': commentData['snippet']['videoId'],
                        'comment_text': commentData['snippet']['topLevelComment']['snippet']['textOriginal'],
                        'comment_author': commentData['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'comment_published_date': commentData['snippet']['topLevelComment']['snippet']['publishedAt'],
                    }
                    comments.append(comment_data)
            duration = isodate.parse_duration(video_response['items'][0]['contentDetails']['duration'])
            duration_seconds = duration.total_seconds()
            video_data = {
                'video_id': video_info['resourceId']['videoId'],
                'channel_id': video_response['items'][0]['snippet']['channelId'],
                'video_name': video_response['items'][0]['snippet']['title'], 
                'video_description': video_response['items'][0]['snippet']['description'], 
                'published_date': video_response['items'][0]['snippet']['publishedAt'], 
                'view_count': video_response['items'][0]['statistics']['viewCount'], 
                'like_count': video_response['items'][0]['statistics']['likeCount'], 
                'favorite_count': video_response['items'][0]['statistics']['favoriteCount'], 
                'comment_count': video_response['items'][0]['statistics']['commentCount'], 
                'duration': duration_seconds, 
                'comments': comments
            }
            videos.append(video_data)
            # print(video_data)
            # print('\n\n')


        return videos

    except HttpError as e:
        st.error(f'An HTTP error in get_video_data{e.resp.status} occurred: {e.content}')


def save_to_mongodb(data):
    mongo_collection.insert_one(data)
    st.success("Data saved to MongoDB!")


def migrate_to_mysql(channel_name):
  
    mysql_cursor.execute("""
        CREATE TABLE IF NOT EXISTS channel_data (
            channel_id VARCHAR(255) PRIMARY KEY,
            channel_name VARCHAR(255),
            playlist_id VARCHAR(255),
            channel_description VARCHAR(255),
            channel_views INT,
            channel_videocount INT
        );
    """)

    mysql_cursor.execute("""
        CREATE TABLE IF NOT EXISTS video_data (
            video_id VARCHAR(255) PRIMARY KEY,
            channel_id VARCHAR(255),
            video_name VARCHAR(255),
            video_description VARCHAR(255),
            published_date DATETIME,
            view_count INT,
            like_count INT,
            favourite_count INT,
            comment_count INT,
            duration VARCHAR(255),
            FOREIGN KEY (channel_id) REFERENCES channel_data(channel_id)
        );
    """)

    mysql_cursor.execute("""
        CREATE TABLE IF NOT EXISTS comment_data (
            comment_id VARCHAR(255) PRIMARY KEY,
            video_id VARCHAR(255),
            comment_text VARCHAR(255),
            comment_author VARCHAR(255),
            comment_published_date DATETIME,
            FOREIGN KEY (video_id) REFERENCES video_data(video_id)
        );
    """)
    channel_data = mongo_collection.find_one({'channel_name': channel_name})

    mysql_cursor.execute("""
        INSERT INTO channel_data (channel_id, channel_name, channel_views, channel_description, channel_videocount, playlist_id)
        VALUES (%s, %s, %s, %s, %s, %s);
    """, (
        channel_data['channel_id'],
        channel_data['channel_name'],
        channel_data['channel_views'],
        channel_data['channel_description'],
        channel_data['channel_videocount'],
        channel_data['playlist_id']
    ))
    
    video_data = channel_data['video_data']
    for video in video_data:
        mysql_cursor.execute("""
            INSERT INTO video_data (video_id, channel_id, video_name, video_description, published_date, view_count, like_count,favourite_count,comment_count,duration)
            VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            video['video_id'],
            video['channel_id'],
            video['video_name'],
            video['video_description'],
            video['published_date'],
            video['view_count'],
            video['like_count'],
            video['favorite_count'],
            video['comment_count'],
            video['duration'],
        ))
        
        for comment in video['comments']:
            mysql_cursor.execute("""
                INSERT INTO comment_data (comment_id, video_id, comment_text, comment_author, comment_published_date)
                VALUES ( %s, %s, %s, %s, %s);
            """, (
                comment['comment_id'],
                comment['video_id'],
                comment['comment_text'],
                comment['comment_author'],
                comment['comment_published_date'],
            ))
        mysql_conn.commit()
    mysql_cursor.close()
    mysql_conn.close()
    st.success("Data migrated to MySQL database!")


def display_sqltable(query_option):
    if(query_option == "What are the names of all the videos and their corresponding channels?"):
        mysql_cursor.execute("""
            SELECT  video_data.video_name,channel_data.channel_name
            FROM video_data
            JOIN channel_data ON channel_data.channel_id = video_data.channel_id;
        """)   
        results = mysql_cursor.fetchall()
        new_frame = pd.DataFrame(results,columns = ["videos","channel name"])
        st.table(new_frame)

    if(query_option == "Which channels have the most number of videos, and how many videos do they have?"):
        mysql_cursor.execute("""
            SELECT  channel_name,channel_videocount
            FROM channel_data 
            WHERE channel_videocount = (SELECT MAX(channel_videocount) FROM channel_data);
        """)   
        results = mysql_cursor.fetchall()
        new_frame = pd.DataFrame(results,columns = ["channel name","video count"])
        st.table(new_frame)

    if(query_option == "What are the top 10 most viewed videos and their respective channels?"):
        mysql_cursor.execute("""
            SELECT  video_data.video_name,video_data.view_count,channel_data.channel_name
            FROM video_data
            JOIN channel_data ON channel_data.channel_id = video_data.channel_id
            ORDER BY video_data.view_count DESC
            LIMIT 10;
        """)           
        results = mysql_cursor.fetchall()
        new_frame = pd.DataFrame(results,columns = ["video","view count","channel name"])
        st.table(new_frame)

    if(query_option == "How many comments were made on each video, and what are their corresponding video names?"):
        mysql_cursor.execute("""
            SELECT  video_name,comment_count
            FROM video_data;
        """)             
        results = mysql_cursor.fetchall()
        new_frame = pd.DataFrame(results,columns = ["video","comment count"])
        st.table(new_frame)

    if(query_option == "Which videos have the highest number of likes, and what are their corresponding channel names?"):
        mysql_cursor.execute("""
            SELECT  video_data.video_name,video_data.like_count,channel_data.channel_name
            FROM video_data
            JOIN channel_data ON channel_data.channel_id = video_data.channel_id
            ORDER BY video_data.like_count DESC
            LIMIT 1;
        """)           
        results = mysql_cursor.fetchall()
        new_frame = pd.DataFrame(results,columns = ["video","like count","channel name"])
        st.table(new_frame)

    if(query_option == "What is the total number of likes and dislikes for each video, and what are their corresponding video names?"):
        mysql_cursor.execute("""
            SELECT  video_data.video_name,video_data.like_count,channel_data.channel_name
            FROM video_data
            JOIN channel_data ON channel_data.channel_id = video_data.channel_id;
        """)           
        results = mysql_cursor.fetchall()
        new_frame = pd.DataFrame(results,columns = ["video","like count","channel name"])
        st.table(new_frame)

    if(query_option == "What is the total number of views for each channel, and what are their corresponding channel names?"):
        mysql_cursor.execute("""
            SELECT  channel_name,channel_views
            FROM channel_data;
        """)   
        results = mysql_cursor.fetchall()
        new_frame = pd.DataFrame(results,columns = ["channel name","channel_views"])
        st.table(new_frame)
    if(query_option == "What are the names of all the channels that have published videos in the year 2022?"):
        mysql_cursor.execute("""
            SELECT  DISTINCT channel_data.channel_name
            FROM channel_data
            JOIN video_data ON channel_data.channel_id = video_data.channel_id
            WHERE video_data.published_date BETWEEN '2022-01-01' AND '2022-12-31';
        """)           
        results = mysql_cursor.fetchall()
        new_frame = pd.DataFrame(results,columns = ["channel name"])
        st.table(new_frame)

    if(query_option == "What is the average duration of all videos in each channel, and what are their corresponding channel names?"):
        mysql_cursor.execute("""
            SELECT channel_data.channel_name,AVG(video_data.duration) as average
            FROM channel_data
            JOIN video_data ON channel_data.channel_id = video_data.channel_id
            GROUP BY channel_data.channel_name;
        """)           
        results = mysql_cursor.fetchall()
        new_frame = pd.DataFrame(results,columns = ["channel","average duration in seconds"])
        st.table(new_frame)

    if(query_option == "Which videos have the highest number of comments, and what are their corresponding channel names?"):
        mysql_cursor.execute("""
            SELECT  video_data.video_name,video_data.comment_count,channel_data.channel_name
            FROM video_data
            JOIN channel_data ON channel_data.channel_id = video_data.channel_id
            ORDER BY video_data.comment_count DESC
            LIMIT 1;
        """)           
        results = mysql_cursor.fetchall()
        new_frame = pd.DataFrame(results,columns = ["video","comment count","channel name"])
        st.table(new_frame)
def main():
    st.title("Search from SQL Table")

    st.sidebar.header("Data Collection")
    channel_id = st.sidebar.text_input("Enter YouTube Channel ID:")
    if st.sidebar.button("Collect Data"):
        channel_data = get_channel_data(channel_id)
        if channel_data:
            video_data = get_video_data(channel_data['playlist_id'])
            channel_data['video_data'] = video_data
            save_to_mongodb(channel_data)

    st.sidebar.header("Data Migration")
    selected_channel = st.sidebar.selectbox(key = 1,label = "Select Channel Name:", options =  [data['channel_name'] for data in mongo_collection.find()])
    if st.sidebar.button("Migrate to MySQL"):
        migrate_to_mysql(selected_channel)

    query_list = ["What are the names of all the videos and their corresponding channels?",
    "Which channels have the most number of videos, and how many videos do they have?",
    "What are the top 10 most viewed videos and their respective channels?",
    "How many comments were made on each video, and what are their corresponding video names?",
    "Which videos have the highest number of likes, and what are their corresponding channel names?",
    "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "What is the total number of views for each channel, and what are their corresponding channel names?",
    "What are the names of all the channels that have published videos in the year 2022?",
    "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "Which videos have the highest number of comments, and what are their corresponding channel names?",
    ]

    query_option = st.selectbox("Search from sql table",query_list)
    if(st.button("Search")):
        print(query_option)
        display_sqltable(query_option)

if __name__ == '__main__':
    main()
