#pip install pymongo

from googleapiclient.discovery import build
import pymongo
import pandas as pd
import psycopg2
import isodate
import streamlit as st

st.title("**YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit**")

# Connecting with MongoDB Atlas 

client = pymongo.MongoClient('mongodb+srv://m_sangeetha_27:sangeetha@cluster0.dieiilu.mongodb.net/?retryWrites=true&w=majority')
db = client['youtube']

# Connecting with PostgresSQL
mydb = psycopg2.connect(host = 'localhost',user = 'postgres',password = 'Tkkrathna26@',port = 5432,database = 'YouTube')
mycursor = mydb.cursor()

# Getting API Key 
API_Key = "AIzaSyAMbS1nRgldp4ysbLMoLA9oa0gRPu60Egw"
service_name = "youtube"
version = "v3"
youtube = build(service_name,version,developerKey=API_Key)

#Define a function to convert duration
def duration(data):
    dur = isodate.parse_duration(data)
    sec = dur.total_seconds()
    hours = float(int(sec) / 3600)
    return(hours)

#Getting channel id
def channel_details(user_inp):
  chl_id = []
  request = youtube.search().list(
      part = "snippet",
      channelType = "any",
      q = user_inp)
  response = request.execute()

  channel_id =  response["items"][0]["snippet"]["channelId"]
  chl_id.append(channel_id)
  return channel_id

# Getting channel details using Channel Id
def get_channel_details(youtube,channel_id):
   chl_details = []
   request = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id = channel_id )
   response = request.execute()

   for i in range(len(response['items'])):
      chl_data = dict(Channel_name = response['items'][i]['snippet']['title'],
                      Channel_Id=response['items'][i]["id"],
                      Total_videos = int(response['items'][i]['statistics']['videoCount']),
                      Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                      Subscribercount = int(response['items'][i]['statistics']['subscriberCount']),
                      Views = int(response['items'][i]['statistics']['viewCount']))

      chl_details.append(chl_data)
      return chl_data

# Getting Playlist Id
def get_playlist_id(youtube,channel_id):
  request = youtube.channels().list(part="contentDetails",
                                    id= channel_id)
  response = request.execute()

  playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  return playlist_id

# Getiing Video Ids
def video_ids(youtube,playlist_id):
  vdo_ids = []
  next_page_token = None

  while True:
    request = youtube.playlistItems().list(part='snippet',
                                           playlistId=playlist_id,
                                           maxResults = 50,
                                           pageToken = next_page_token)
    response = request.execute()

    for i in range(len(response['items'])):
       vdo_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
    if next_page_token is None:
      break
    next_page_token = response.get('nextpageToken')

  return vdo_ids


#Getting Video Details
def get_video_details(youtube,vdo_ids):
   vd_ids = []
   for i in range(0,len(vdo_ids),50):
     request = youtube.videos().list(part='snippet,statistics,contentDetails',
                                     id =','.join(vdo_ids[i:i+50]))
     response=request.execute()

     for i in range(len(response['items'])):
       data = dict(Channel_name=response['items'][i]['snippet']['channelTitle'],
                   Channel_id=response['items'][i]['snippet']['channelId'],
                   Video_id=response['items'][i]['id'],
                   Title=response['items'][i]['snippet']['title'],
                   video_view = int(response['items'][i]['statistics']['viewCount']),
                   Duration=duration(response['items'][i]['contentDetails']['duration']),
                   Published = response['items'][i]['snippet']['publishedAt'],
                   Like_count=int(response['items'][i]['statistics']['likeCount']),
                   Dislike_count = int(response['items'][i]['statistics'].get('dislikeCount',0)),
                   Comment_count=int(response['items'][i]['statistics']['commentCount']))
       vd_ids.append(data)

   return vd_ids

#Getting Comment Details
def get_comments_details(youtube,vdo_ids):
    comment_data = []
    for v in vdo_ids:
      try:
          request = youtube.commentThreads().list(part="snippet,replies",
                                                 videoId=v,
                                                 maxResults = 15)
          response = request.execute()

          for i in range(len(response['items'])):
            data = dict(Comment_id = response['items'][i]['id'],
                        Video_id = response['items'][i]['snippet']['videoId'],
                        Comment_text = response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_author = response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_posted_date = response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'],
                        Like_count = response['items'][i]['snippet']['topLevelComment']['snippet']['likeCount'],
                        Reply_count = response['items'][i]['snippet']['totalReplyCount'])
            comment_data.append(data)
          return comment_data
      except:
        pass

# Select option from user
option = st.selectbox('Select any one options',
    ('View Data','Migration','Questions'))
st.button('ENTER')

#Getting channel Names
if option == "Migration" or option == "View Data":
      
      ch_name = []
      for i in db.channels.find():
        ch_name.append(i['Channel_name'])      
 
      user_inp = st.selectbox("Select channel", options=ch_name)


      try:
          if option == "Migration" and option != 'View Data':
            mig_option = st.selectbox('Select any one options',
            ('Migrate to MongoDB','Migrate to SQL'))
      except:
          st.error("ready to go migration")

       
      if option =='View Data' and option != 'Migration':
          try:  
            channel_id = channel_details(user_inp)    
            channel_details = get_channel_details(youtube,channel_id)
            st.write("CHANNEL DETAILS")
            st.write(channel_details)
    
            playlist_id = get_playlist_id(youtube,channel_id)
            vdo_ids = video_ids(youtube,playlist_id)
            video_details = get_video_details(youtube,vdo_ids)
            st.write("VIDEO DETAILS")
            st.write(video_details)
          
            cmt_details = get_comments_details(youtube,vdo_ids)
            st.write("COMMENT DETAILS")
            st.write(cmt_details)
          except:
            st.error("Run Successfully.")

if option == 'Migration' and mig_option == "Migrate to MongoDB" :
      
    if user_inp in ch_name:
      st.info(":black[Data already exists....!]")
    else: 
      choice = st.radio("Can you enter the data in MongoDB?",["yes","no"])
      submit = st.button("submit")

# Calling a function to collect channel details for MongoDB
      channel_id = channel_details(user_inp)
      channel_details = get_channel_details(youtube,channel_id)
  
      col1 = db["channels"]
      col1.insert_one(channel_details)

      playlist_id = get_playlist_id(youtube,channel_id)
      vdo_ids = video_ids(youtube,playlist_id)
      video_details = get_video_details(youtube,vdo_ids)
   
#create a collection to insert video datas in mongoDB
      col2 = db["videos"]
      col2.insert_many(video_details)

#calling a function to collect comment details for mongoDB
      cmt_details = get_comments_details(youtube,vdo_ids)

#create a collection to insert comment datas in mongoDB
      col3 = db["comments"]
      col3.insert_many(cmt_details)   
      st.info("Data is successfully stored in MongoDB")

# Define a function to create table in SQL 

def create_sqlschema():

 # Create a table to insert channel data   
  
    query_channels = """create table if not exists channels(
        Channel_name varchar,
        Channel_Id varchar PRIMARY KEY,
        Total_videos int,
        playlist_id varchar,
        subscribercount int,
        view_count int)"""
  mycursor.execute(query_channels)
  mydb.commit()

# Create a table to insert Video Data

    query_videos = """create table if not exists videos(
        channel_name varchar,
        channel_id varchar,
        video_id varchar PRIMARY KEY,
        video_Title varchar,
        video_view int,
        Duration float,
        published  varchar,
        like_count int,
        dilike_count int,
        comment_count int,
        CONSTRAINT fk_playlist FOREIGN KEY(channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE)"""
  mycursor.execute(query_videos)
  mydb.commit()

 # Create a table to insert Comment Data

    query_comment = """create table if not exists comment(
        comment_id varchar PRIMARY KEY,
        video_id varchar,
        comment_text varchar,
        comment_author varchar,
        commment_posted_date varchar,
        comment_likes int,
        comment_replies int,
  CONSTRAINT fk_video FOREIGN KEY(video_id) REFERENCES video(video_id) ON DELETE CASCADE)"""
  mycursor.execute(query_comment)
  mydb.commit() 

create_sqlschema()

# Insert values in SQL table
 # Insert Channel Details

def insert_into_channels():
    col1 = db.channels
  query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s)"""
  for i in col1.find({'Channel_name':user_inp},{"_id":0}):
     t = tuple(i.values())
     mycursor.execute(query,t)
     mydb.commit()

# Insert Video Details
def insert_into_video():
    col2 = db.videos
    query1 = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    for i in col2.find({'Channel_name':user_inp},{"_id":0}):
      mycursor.execute(query1, tuple(i.values()))
      mydb.commit()

#Insert Comment Details
def insert_into_comment():
  col2 = db.videos
  col3 = db.comments
  query2 = """INSERT INTO comment VALUES(%s,%s,%s,%s,%s,%s,%s)"""

  for vdo in col2.find({'Channel_name':user_inp},{'_id' : 0,}):
    for i in col3.find({'Video_id': vdo['Video_id']},{'_id' : 0}):
      t=tuple(i.values())
      mycursor.execute(query2,t)
      mydb.commit()


#Migrate datas from MongoDB to SQL
if option == "Migration" and  mig_option == "Migrate to SQL":       

#Define a function to migrate datas from MongoDB to SQL  
  def insert_sql(user_inp):
    try:
      insert_into_channels(user_inp) 
      insert_into_video(user_inp)
      insert_into_comment(user_inp) 
      st.success("Migrate Successfully.....!")
    except:
      st.error("Already exists this channel information")  

#Calling a function to migrate datas from MongoDB to SQL      
  insert_sql(user_inp)

if option == "Questions":
    questions = st.selectbox(
        'what question you want to choose?',
        (  "What are the names of all the videos and their corresponding channels?",
        "Which channels have the most number of videos, and how many videos do they have?",
        "What are the top 10 most viewed videos and their respective channels?",
        "How many comments were made on each video, and what are their corresponding video names?",
        "Which videos have the highest number of likes, and what are their corresponding channel names?",
        "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
        "What is the total number of views for each channel, and what are their corresponding channel names?",
        "What are the names of all the channels that have published videos in the year 2022?",
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "Which videos have the highest number of comments, and what are their corresponding channel names?"))

    st.write('You user_inp:', questions)

    def execute_query(questions):
        if questions ==  "What are the names of all the videos and their corresponding channels?":
                mycursor.execute("select video_Title , channel_name from videos")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'video_Title',1:'channel_name'})
                st.dataframe(df_re)
                

        elif questions == "Which channels have the most number of videos, and how many videos do they have?":
                mycursor.execute("select Channel_name,Total_videos from channels order by Total_videos desc limit 5")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'channel_name',1:'Total_videos'})
                st.dataframe(df_re)
        

        elif questions == "What are the top 10 most viewed videos and their respective channels?":
                mycursor.execute("select channel_name ,video_view from videos order by video_view desc limit 10")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'channel_name',1:'Viewers'})
                st.dataframe(df_re)


        elif questions =="How many comments were made on each video, and what are their corresponding video names?":
                mycursor.execute("select channel_name, video_Title,comment_count from videos order by channel_name,video_Title" )
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'channel_name',1:'video_Title',2:'comment_count'})
                st.dataframe(df_re)
        

        elif questions == "Which videos have the highest number of likes, and what are their corresponding channel names?":
                mycursor.execute("select channel_name,video_Title,like_count from videos order by like_count desc limit 5")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'channel_name',1:'video_title',2:'like_count'})
                st.dataframe(df_re)         
        
            
        elif questions == "What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
                mycursor.execute("select channel_name,video_Title,like_count,dilike_count from videos order by channel_name ")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'channel_name',1:'video_title',2:'like_count',3:'dislike_count'})
                st.dataframe(df_re)


        elif questions == "What is the total number of views for each channel, and what are their corresponding channel names?":
                mycursor.execute("select Channel_name,view_count from channels order by Channel_name")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'channel_name',1:'view_count'})
                st.dataframe(df_re)


        elif questions == "What are the names of all the channels that have published videos in the year 2022?":
                mycursor.execute("select distinct channel_name from videos where published like '2022%' ")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'channel_name',1:'video_Title',2:'published'})
                st.dataframe(df_re)

            
        elif questions == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
                mycursor.execute("select channel_name, avg(Duration) from videos group by channel_name")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={1:'channel_name',1:'average Duration '})
                st.dataframe(df_re)
       

        elif questions == "Which videos have the highest number of comments, and what are their corresponding channel names?" :
                mycursor.execute("select channel_name,video_Title,comment_count from videos order by comment_count desc limit 10")
                df = pd.DataFrame(mycursor.fetchall())
                df_re = df.rename(columns={0:'channel_name',1:'video_Title',2:'comment_count'})
                st.dataframe(df_re)
                     
            
    execute_query(questions)      

    df = pd.DataFrame(mycursor.fetchall())
    st.success("Successfully Done",icon="✅")
