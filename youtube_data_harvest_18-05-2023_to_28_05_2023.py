import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymongo import MongoClient
import mysql.connector
import sqlalchemy
from sqlalchemy.exc import IntegrityError
from datetime import datetime
import pandas as pd
import isodate


def get_channel_details(y_tube, channel_id):
    fields = 'items(snippet(title, description, publishedAt, thumbnails(default(url))), contentDetails(' \
             'relatedPlaylists(uploads)), statistics(videoCount, viewCount, subscriberCount))'

    response = y_tube.channels().list(
        part='snippet, contentDetails, statistics',
        id=channel_id,
        fields=fields
    ).execute()
    chnl = response['items'][0]
    channel_details = {
        'Channel_Name': chnl['snippet']['title'],
        'Channel_ID': channel_id,
        'Description': chnl['snippet']['description'],
        'Published_At': chnl['snippet']['publishedAt'],
        'Thumbnail': chnl['snippet']['thumbnails']['default']['url'],
        'Playlist_ID': chnl['contentDetails']['relatedPlaylists']['uploads'],
        'Video_Count': int(chnl['statistics']['videoCount']),
        'View_Count': int(chnl['statistics']['viewCount']),
        'Subscribers_Count': int(chnl['statistics']['subscriberCount']),
    }
    return channel_details


def get_all_playlist_ids(y_tube, channel_id):
    playlists = {}
    next_page_token = None

    while True:
        response = y_tube.playlists().list(
            part='snippet',
            fields='items(id, snippet(title))',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        i = 1
        for item in response['items']:
            playlist = {'playlist_id': item['id'], 'playlist_name': item['snippet']['title'],
                        'video_ids': get_all_video_ids(youtube, item['id'])}
            playlists['playlist_' + str(i)] = playlist
            i += 1

        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break

    return playlists


def get_all_video_ids(y_tube, playlist_id):
    video_ids = []
    next_page_token = None

    while True:
        response = y_tube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for item in response['items']:
            video_id = item['snippet']['resourceId']['videoId']
            video_ids.append(video_id)

        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break

    return video_ids


# Get video details
def get_video_details(y_tube, video_ids):
    v_ids = video_ids
    all_videos = []
    fields = 'items(snippet(title, description,publishedAt, thumbnails(default(url))), contentDetails(caption, ' \
             'duration), statistics(viewCount, likeCount, dislikeCount, favoriteCount, commentCount))'
    for v_id in v_ids:
        response = y_tube.videos().list(
            part='snippet,contentDetails,statistics',
            id=v_id['vid_id'],
            fields=fields
        ).execute()

        video = response['items'][0]
        duration = isodate.parse_duration(video['contentDetails']['duration'])
        duration = duration.__str__()
        video_details = {
            'Video_ID': v_id['vid_id'],
            'Playlist_ID': v_id['pl_id'],  # playlist_id,
            'Video_Title': video['snippet']['title'],
            'Playlist_Name': v_id['pl_title'],  # playlist_name,
            'Description': video['snippet']['description'],
            'Published_At': video['snippet']['publishedAt'],
            'Duration': duration,  # video_duration,
            'Thumbnail': video['snippet']['thumbnails']['default']['url'],
            'Caption': video['contentDetails']['caption'],  # video_caption,
            'View_Count': int(video['statistics'].get('viewCount', 0)),
            'Like_Count': int(video['statistics'].get('likeCount', 0)),  # int(like_count),
            'Dislike_Count': int(video['statistics'].get('dislikeCount', 0)),  # int(dislike_count),
            'Favorite_Count': int(video['statistics'].get('favoriteCount', 0)),  # int(favorite_count),
            'Comment_Count': int(video['statistics'].get('commentCount', 0)),  # int(comment_count),
            'Comments': get_comments(y_tube, v_id['vid_id'])
        }
        all_videos.append(video_details)
    return all_videos


# To get Comments of each video

def get_comments(y_tube, video_id):
    next_page_token = None
    fields = 'items(id, snippet(topLevelComment(snippet(textDisplay, authorDisplayName, publishedAt))))'
    comments = {}
    try:
        while True:
            response = y_tube.commentThreads().list(
                part='snippet',
                textFormat='plainText',
                maxResults=100,
                pageToken=next_page_token,
                videoId=video_id,
                fields=fields).execute()

            for item in response['items']:
                comment_id = item['id']
                comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
                comment_published_at = item['snippet']['topLevelComment']['snippet']['publishedAt']

                comment_details = {'Comment_ID': comment_id,
                                   'Comment_Text': comment_text,
                                   'Comment_Author': comment_author,
                                   'Published_At': comment_published_at}

                comments[f'Comment_{len(comments) + 1}'] = comment_details

            next_page_token = response.get('nextPageToken')

            if not next_page_token:
                break

        return comments
    except HttpError as e:
        if e.resp.status == 403:
            return comments


def get_channel_data(channel_ids):
    channl_ids = channel_ids.split(', ')
    channels = []
    for channel_id in channl_ids:
        channel_data = {}
        channel_details = get_channel_details(youtube, channel_id)
        playlists = get_all_playlist_ids(youtube, channel_id)
        uploads_playlist_id = channel_details['Playlist_ID']
        video_ids = get_all_video_ids(youtube, uploads_playlist_id)
        vp_ids = []
        for video_id in video_ids:
            vp_details = {'vid_id': video_id,
                          'pl_id': uploads_playlist_id,
                          'pl_title': 'Uploads'}
            for i in range(1, len(playlists) + 1):
                if video_id in playlists['playlist_' + str(i)]['video_ids']:
                    vp_details = {'vid_id': video_id,
                                  'pl_id': playlists['playlist_' + str(i)]['playlist_id'],
                                  'pl_title': playlists['playlist_' + str(i)]['playlist_name']}
                    break
            vp_ids.append(vp_details)

        video_details_list = get_video_details(youtube, vp_ids)  # , playlists
        channel_data["Channel"] = channel_details
        for video_details in video_details_list:
            channel_data[f'Video_{len(channel_data)}'] = video_details
        channels.append(channel_data)

    return channels


def migrate_to_sql(selected_chnls):
    ch_df = pd.DataFrame()
    pl_df = pd.DataFrame()
    vdo_df = pd.DataFrame()
    cmt_df = pd.DataFrame()
    chs = []
    p_lists = []
    vds = []
    cmts = []
    # Unstructured to structured dataframe
    for selected_chnl in selected_chnls:
        yt_data = mycol.find_one({'Channel.Channel_Name': selected_chnl})

        # Channels Dataframe
        ch = yt_data['Channel']
        chs.append(ch)
        ch_df = pd.DataFrame(chs)
        ch_df['Published_At'] = ch_df['Published_At'].apply(lambda x: to_datetime(x))

        # Playlists Dataframe
        for i in range(1, yt_data['Channel']['Video_Count'] + 1):
            p_list = {'Playlist_Name': yt_data['Video_' + str(i)]['Playlist_Name'],
                      'Playlist_ID': yt_data['Video_' + str(i)]['Playlist_ID'],
                      'Channel_ID': yt_data['Channel']['Channel_ID']}
            p_lists.append(p_list)
        pl_df = pd.DataFrame(p_lists)
        pl_df = pl_df.drop_duplicates()

        # Videos Dataframe
        for i in range(yt_data['Channel']['Video_Count']):
            vdo = {}
            i += 1
            video = 'Video_' + str(i)
            vdo['Video_ID'] = yt_data[video]['Video_ID']
            vdo['Playlist_ID'] = yt_data[video]['Playlist_ID']
            vdo['Video_Title'] = yt_data[video]['Video_Title']
            vdo['Description'] = yt_data[video]['Description']
            vdo['Published_At'] = to_datetime(yt_data[video]['Published_At'])
            vdo['Duration'] = duration_to_seconds(yt_data[video]['Duration'])
            vdo['Thumbnail'] = yt_data[video]['Thumbnail']
            vdo['Caption'] = yt_data[video]['Caption']
            vdo['View_Count'] = yt_data[video]['View_Count']
            vdo['Like_Count'] = yt_data[video]['Like_Count']
            vdo['Dislike_Count'] = yt_data[video]['Dislike_Count']
            vdo['Favorite_Count'] = yt_data[video]['Favorite_Count']
            vdo['Comment_Count'] = yt_data[video]['Comment_Count']
            vds.append(vdo)
        vdo_df = pd.DataFrame(vds)

        # Comments Dataframe
        for i in range(yt_data['Channel']['Video_Count']):
            i += 1
            video = 'Video_' + str(i)
            for j in range(len(yt_data[video]['Comments'])):
                cmt = {}
                j += 1
                comment = 'Comment_' + str(j)
                cmt['Comment_ID'] = yt_data[video]['Comments'][comment]['Comment_ID']
                cmt['Video_ID'] = yt_data[video]['Video_ID']
                cmt['Comment_Text'] = yt_data[video]['Comments'][comment]['Comment_Text']
                cmt['Comment_Author'] = yt_data[video]['Comments'][comment]['Comment_Author']
                cmt['Published_At'] = to_datetime(yt_data[video]['Comments'][comment]['Published_At'])
                cmts.append(cmt)
        cmt_df = pd.DataFrame(cmts)

    # Migrating data from dataframe to SQL
    engine = sqlalchemy.create_engine('<mysql url>')
    try:
        # Migrating channel details into channel table
        ch_df.to_sql('ychannel', engine, if_exists='append', index=False)

        # Migrating playlist details into playlist table
        pl_df.to_sql('yplaylist', engine, if_exists='append', index=False)

        # Migrating video details into video table
        vdo_df.to_sql('yvideo', engine, if_exists='append', index=False)

        # Migrating comment details into comment table
        cmt_df.to_sql('ycomment', engine, if_exists='append', index=False)
        return 1
    except IntegrityError:
        return 0


def duration_to_seconds(duration):
    duration = str(duration)
    sec = 3600
    time_duration = 0
    for j in duration.split(":"):
        j = j.lstrip('0')
        if j.isdigit():
            s = int(j)
        else:
            s = 0
        time_duration += s * sec
        sec //= 60
    return time_duration


def to_datetime(published):
    if len(published) > 20:
        dt = datetime.strptime(published, '%Y-%m-%dT%H:%M:%S.%fZ')
    else:
        dt = datetime.strptime(published, '%Y-%m-%dT%H:%M:%SZ')
    d_time = dt.strftime('%Y-%m-%d %H:%M:%S')
    return d_time


def data_query(chosen_query):
    query_df = ''
    if chosen_query == 'Names of all the videos and their corresponding channels':
        cursor.execute('''select v.Video_Title, c.Channel_Name from yvideo v  
                          join yplaylist p on v.Playlist_ID = p.Playlist_ID
                          join ychannel c on p.Channel_ID = c.Channel_ID''')

        result = cursor.fetchall()
        query_df = pd.DataFrame.from_records(result, columns=['Video_Title', 'Channel_Name'])

    elif chosen_query == 'Channel with most number of videos and its video count':
        query = 'select Channel_name, Video_Count from ychannel ORDER BY Video_Count DESC LIMIT 3'
        query_df = pd.read_sql_query(query, ytdb)

    elif chosen_query == 'Top 10 most viewed videos with their channel name':
        query = '''select v.Video_Title, c.Channel_name from yvideo v 
                   join yplaylist p on v.Playlist_ID = p.Playlist_ID
                   join ychannel c on p.Channel_ID = c.Channel_ID
                   ORDER BY v.View_Count DESC LIMIT 10'''
        query_df = pd.read_sql_query(query, ytdb)

    elif chosen_query == 'Number of comments on each video with channel name':
        query = '''select v.Video_Title, v.Comment_Count, c.Channel_Name from yvideo v 
                   join yplaylist p on v.Playlist_ID = p.Playlist_ID
                   join ychannel c on p.Channel_ID = c.Channel_ID'''
        query_df = pd.read_sql_query(query, ytdb)

    elif chosen_query == 'Videos with highest number of likes with channel name':
        query = '''select v.Video_Title, v.Like_Count, c.Channel_name from yvideo v
                   join yplaylist p on v.Playlist_ID = p.Playlist_ID
                   join ychannel c on p.Channel_ID = c.Channel_ID 
                   ORDER BY v.Like_Count DESC LIMIT 10'''

        query_df = pd.read_sql_query(query, ytdb)

    elif chosen_query == 'Number of likes and dislikes of each video':
        query = 'select yvideo.Video_Title, yvideo.Like_Count, yvideo.Dislike_Count from yvideo'
        query_df = pd.read_sql_query(query, ytdb)

    elif chosen_query == 'Total views of each channel':
        query = 'select ychannel.Channel_Name, ychannel.View_Count from ychannel'
        query_df = pd.read_sql_query(query, ytdb)

    elif chosen_query == 'Names of all the channels that have published videos in the year 2022':
        query = '''select distinct c.Channel_Name from ychannel c 
                   join yplaylist p on p.Channel_ID = c.Channel_ID
                   join yvideo v on v.Playlist_ID = p.Playlist_ID
                   where year(v.Published_At)=2022'''
        query_df = pd.read_sql_query(query, ytdb)

    elif chosen_query == 'Average duration of videos in each channel':
        query = '''select c.Channel_Name, AVG(v.Duration) as avg_duration_seconds from ychannel c 
                   join yplaylist p on p.Channel_ID = c.Channel_ID
                   join yvideo v on v.Playlist_ID = p.Playlist_ID
                   GROUP BY c.Channel_Name'''
        query_df = pd.read_sql_query(query, ytdb)

    elif chosen_query == 'Videos with most comments with channel name':
        query = '''select v.Video_Title, v.Comment_Count, c.Channel_Name from yvideo v 
                   join yplaylist p on v.Playlist_ID = p.Playlist_ID
                   join ychannel c on p.Channel_ID = c.Channel_ID 
                   ORDER BY v.Comment_Count DESC LIMIT 10'''
        query_df = pd.read_sql_query(query, ytdb)

    return query_df


# CONNECTION SEGMENT
# YouTube API connection
api_key = "<youtube api key>"
youtube = build("youtube", "v3", developerKey=api_key)
# mongodb
mongo_connection = "<mongodb connection url>"
database_name = "Youtube_data_harvesting"
collection_name = "channels"

client = MongoClient(mongo_connection)
mydb = client[database_name]
mycol = mydb[collection_name]
# SQL
ytdb = mysql.connector.connect(host='localhost',
                               user='root',
                               password='<password>',
                               database='ytdata')
cursor = ytdb.cursor()


# cursor.execute("create database ytdata")
# ytdb.commit()
#
# cursor.execute("""create table yChannel (Channel_Name varchar(255),
#                                         Channel_ID varchar(255) PRIMARY KEY,
#                                         Description TEXT,
#                                         Published_At DATETIME,
#                                         Thumbnail varchar(255),
#                                         Playlist_ID varchar(255),
#                                         Video_Count INT,
#                                         View_Count INT,
#                                         Subscribers_Count INT)""")
# ytdb.commit()
#
# cursor.execute("""create table yPlaylist (Playlist_Name varchar(255),
#                                           Playlist_ID varchar(255) PRIMARY KEY,
#                                           Channel_ID varchar(255),
#                                           FOREIGN KEY (Channel_ID) REFERENCES yChannel(Channel_ID)
#                                           ON DELETE CASCADE)""")
# ytdb.commit()
#
# cursor.execute("""create table yVideo (Video_ID varchar(255) PRIMARY KEY,
#                                       Playlist_ID varchar(255),
#                                       FOREIGN KEY (Playlist_ID) REFERENCES yPlaylist(Playlist_ID) ON DELETE CASCADE,
#                                       Video_Title varchar(255),
#                                       Description TEXT,
#                                       Published_At DATETIME,
#                                       Duration INT,
#                                       Thumbnail varchar(255),
#                                       Caption varchar(255),
#                                       View_Count INT,
#                                       Like_Count INT,
#                                       Dislike_Count INT,
#                                       Favorite_Count INT,
#                                       Comment_Count INT)""")
# ytdb.commit()
#
# cursor.execute("""create table yComment(Comment_ID varchar(255) PRIMARY KEY,
#                                        Video_ID varchar(255),
#                                        FOREIGN KEY (Video_ID) REFERENCES yVideo(Video_ID) ON DELETE CASCADE,
#                                        Comment_Text TEXT,
#                                        Comment_Author varchar(255),
#                                        Published_At DATETIME)""")
# ytdb.commit()

# STREAMLIT interactive
def main():
    st.set_page_config(page_title='Youtube data harvesting')
    st.header('Youtube data harvesting using youtube API ')
    st.subheader('Data Collection through API')
    c1, c2 = st.columns(2)
    with c1:
        channel_ids = st.text_input('Please provide the comma separated channel ID(s) and press enter')
        if st.button('Find'):
            channels = get_channel_data(channel_ids)
            st.session_state.channels = channels
            st.write('Channel Details:', *channels)  # To display the fetched channel details
    with c2:
        channel_names = mycol.distinct('Channel.Channel_Name')
        if st.button('Store Data in MongoDB'):
            channels = st.session_state.channels
            if channels is not None:
                for channel in channels:
                    if channel['Channel']['Channel_Name'] not in channel_names:
                        mycol.insert_one(channel)
                st.write('Data stored in MongoDB')
            else:
                st.write('Channel details not fetched')

        channel_names = mycol.distinct('Channel.Channel_Name')  # Dropdown list of channels added to MongoDB

        if channel_names:
            selected_channels = st.multiselect('Select a channel', channel_names)  # Dropdown selection box

            # Migrating selected channel to SQL Database
            if st.button('Migrate to SQL Database'):
                if migrate_to_sql(selected_channels):
                    st.write('Data migrated to SQL Database')
                else:
                    st.write('Data already in SQL Database')
        # Performing SQL queries over the migrated data
        cursor.execute('select * from ychannel')
        if cursor.fetchall():
            query_list = ['Names of all the videos and their corresponding channels',
                          'Channel with most number of videos and its video count',
                          'Top 10 most viewed videos with their channel name',
                          'Number of comments on each video with channel name',
                          'Videos with highest number of likes with channel name',
                          'Number of likes and dislikes of each video', 'Total views of each channel',
                          'Names of all the channels that have published videos in the year 2022',
                          'Average duration of videos in each channel',
                          'Videos with most comments with channel name']

            selected_query = st.selectbox('Select a query', query_list)
            if st.button('Get Report'):
                if selected_query:
                    df = data_query(selected_query)
                    st.dataframe(df)
                else:
                    st.write('Select a query and then press find')

        if st.button('Clear MongoDB Collection'):
            mycol.delete_many({})
            st.write('Cleared MongoDB Collection')

        if st.button('Clear sql tables'):
            cursor.execute('delete from ychannel')
            ytdb.commit()
            st.write('SQL Tables Cleared')


if __name__ == '__main__':
    main()
