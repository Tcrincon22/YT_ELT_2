from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json


local_tz = pendulum.timezone("America/Bogota")

default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email":"data@enginners.com",
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(minutes=60),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    #"'end_date': datetime(2024, 6, 30, tzinfo=local_tz)",
    
}

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='A DAG to extract video stats from YouTube API and save it as JSON',
    schedule='0 6 * * *',
    catchup=False
) as dag:
    
    #Define task
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(video_data)
    
    #define dependencies
    playlist_id >> video_ids >> video_data >> save_to_json_task