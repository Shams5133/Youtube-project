import os
import streamlit as st
import pymongo
import mysql.connector
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.tools import argparser

# Set API key
API_KEY = "AIzaSyB-xg8RyJe9Bd95t2oz4ugAQtCI2UmDv9w"

# Set MongoDB connection details
MONGODB_CONNECTION_URL = "mongodb://localhost:27017"
MONGODB_DATABASE_NAME = "youtube_data"
MONGODB_COLLECTION_NAME = "channel_data"

# Set MySQL connection details
MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"
MYSQL_DATABASE = "youtube_data"
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
query = ""


# Set up YouTube API service
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Connect to MongoDB
mongo_client = pymongo.MongoClient(MONGODB_CONNECTION_URL)
mongo_db = mongo_client[MONGODB_DATABASE_NAME]
mongo_collection = mongo_db[MONGODB_COLLECTION_NAME]

# Connect to MySQL
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
        # Make API request to retrieve channel data
        channels_response = youtube.channels().list(
            part='snippet,statistics,contentDetails',
            id=channel_id
        ).execute()

        # Extract relevant channel information
        channel_info = channels_response['items'][0]['snippet']
        statistics = channels_response['items'][0]['statistics']
        content_details = channels_response['items'][0]['contentDetails']

        channel_data = {
            'channel_id': channel_id,
            'channel_name': channel_info['title'],
            'channel_views': statistics['viewCount'],
            'channel_description': channel_info['description'],
            'playlist_id': content_details['relatedPlaylists']['uploads']
        }

        return channel_data

    except HttpError as e:
        st.error(f'An HTTP error {e.resp.status} occurred: {e.content}')


def get_video_data(playlist_id):
    try:
        # Make API request to retrieve videos from playlist
        playlist_response = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=5
        ).execute()
        # Extract video information
        videos = []
        for playlist_item in playlist_response['items']:
            video_info = playlist_item['snippet']
            video_id = video_info['resourceId']['videoId'],
            video_response = youtube.videos().list(
                part = "snippet, contentDetails, statistics",
                id = video_info['resourceId']['videoId']
                ).execute()
            print(video_response['items'][0])

            comment_response = youtube.commentThreads().list(
                part = "snippet,replies",
                videoId = video_info['resourceId']['videoId']
                ).execute()
            comments = []

            for commentData in comment_response['items']:
                comment_data = {
                    'comment_id': commentData['id'],
                    'video_id': commentData['snippet']['videoId'],
                    'comment_text': commentData['snippet']['topLevelComment']['snippet']['textOriginal'],
                    'comment_author': commentData['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'comment_published_date': commentData['snippet']['topLevelComment']['snippet']['publishedAt'],
                }
                comments.append(comment_data)
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
                'duration': video_response['items'][0]['contentDetails']['duration'], 
                'duration': video_response['items'][0]['contentDetails']['duration'], 
                'comments': comments 
            }
            videos.append(video_data)

        return videos

    except HttpError as e:
        st.error(f'An HTTP error {e.resp.status} occurred: {e.content}')


def save_to_mongodb(data):
    mongo_collection.insert_one(data)
    st.success("Data saved to MongoDB!")


def migrate_to_mysql(channel_name):
    # Create table for channel data
    # mysql_cursor.execute("""
    #     CREATE TABLE IF NOT EXISTS channel_data (
    #         channel_id VARCHAR(255) PRIMARY KEY,
    #         channel_name VARCHAR(255),
    #         subscribers INT,
    #         total_videos INT
    #     );
    # """)   
    mysql_cursor.execute("""
        CREATE TABLE IF NOT EXISTS channel_data (
            channel_id VARCHAR(255) PRIMARY KEY,
            channel_name VARCHAR(255),
            playlist_id VARCHAR(255),
            channel_description VARCHAR(255),
            channel_views INT
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
            duration INT,
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
    # Retrieve channel data from MongoDB
    channel_data = mongo_collection.find_one({'channel_name': channel_name})

    # Insert channel data into MySQL
    mysql_cursor.execute("""
        INSERT INTO channel_data (channel_id, channel_name, channel_views, channel_description,playlist_id)
        VALUES (%s, %s, %s, %s, %s);
    """, (
        channel_data['channel_id'],
        channel_data['channel_name'],
        channel_data['channel_views'],
        channel_data['channel_description'],
        channel_data['playlist_id']
    ))

    # Retrieve video data from MongoDB
    video_data = channel_data['video_data']
    # videodata = video_data['video_data']
    print(video_data)
    # Insert video data into MySQL
    for video in video_data:
        mysql_cursor.execute("""
            INSERT INTO video_data (video_id, channel_id, video_name, video_description, published_date, view_count,like_count,favourite_count,comment_count,duration)
            VALUES ( %s, %s, %s, %s, %s,%s,%s,%s,%s,%s);
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
        
        # print(video2['video_id'])

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
        # Commit changes and close the connection
        mysql_conn.commit()
    mysql_cursor.close()
    mysql_conn.close()
    st.success("Data migrated to MySQL database!")

# Streamlit app
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

 
    query = st.text_input("Enter your search query:")
    if(st.button("Search now")):
        search_query = query  # Replace with your search query
        mysql_cursor = mysql_conn.cursor()
        # Execute the search query
        mysql_cursor.execute("""
            SELECT video_name FROM video_data
            WHERE video_description LIKE %s
        """, ('%' + search_query + '%',))

        # Fetch all the rows that match the search query
        results = mysql_cursor.fetchall()
        print(results)
        # Process and display the search results
        for row in results:
            st.write(row[0])

        # Close the cursor and connection
        mysql_cursor.close()
        mysql_conn.close()


if __name__ == '__main__':
    main()
