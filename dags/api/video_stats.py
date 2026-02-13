import requests
import json
from datetime import date, datetime

#import os 
#from dotenv import load_dotenv
#load_dotenv(dotenv_path="./.env")

from airflow.decorators import task 
from airflow.models import Variable



API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
maxResults = 50

@task
def get_playlist_id():  
    
    try:
    
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        response = requests.get(url)
        
        response.raise_for_status

        data = response.json()

        json.dumps(data, indent=4)

        #print(json.dumps(data, indent=4))

        channel_items = data["items"][0]
        channel_playlistId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        
        return channel_playlistId
        
    except requests.exceptions.RequestException as e:
        raise e
       
       
@task
def get_video_ids(playlistId):
    
    video_ids = [] 
    pageToken = None 
    
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=ContentDetails&maxResults={maxResults}&playlistId=UUX6OQ3DkcsbYNE6H8uQQuVA&key={API_KEY}"       

    try: 
        while True:
            url = base_url
            if pageToken:
                url += f"&pageToken={pageToken}"
                
            response = requests.get(url)
            response.raise_for_status()
            
            data = response.json()
            json.dumps(data, indent=4)
            
           
            for item in data["items"]:
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)
                
            pageToken = data.get("nextPageToken")
            if not pageToken:
                break
                
        return video_ids
        
    except requests.exceptions.RequestException as e:
            raise e
        

def batch_list(video_ids, batch_size=50):
    for video_id in range(0, len(video_ids), batch_size):
        yield video_ids[video_id:video_id + batch_size]
        
@task
def extract_video_data(video_ids):
    extracted_data = []

    try:
        for batch in batch_list(video_ids, maxResults):
            video_id_str = ",".join(batch)
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails,snippet,statistics&id={video_id_str}&key={API_KEY}"

            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            for item in data.get("items", []):
                snippet = item["snippet"]
                contentDetails = item["contentDetails"]
                statistics = item.get("statistics", {})

                video_data = {
                    "video_id": item["id"],
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "viewCount": statistics.get("viewCount"),
                    "likeCount": statistics.get("likeCount"),
                    "commentCount": statistics.get("commentCount"),
                    "duration": contentDetails["duration"]
                }
                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e

@task
def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}.json"
    
    with open(file_path,"w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)
       
if __name__ == "__main__":
    playlistId = get_playlist_id()
    video_ids = get_video_ids(playlistId)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)