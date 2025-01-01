# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # YouTube Bulk Download with Additional Columns for Next Steps
# MAGIC 
# MAGIC 1. **Creates** (or uses) a Delta table `channel_processed_videos` in the same schema `yt-deslopify`.default.
# MAGIC 2. Adds extra columns to track additional metadata about each download:
# MAGIC    - `channel_id`: from the channel URL  
# MAGIC    - `channel_name`  
# MAGIC    - `channel_url`  
# MAGIC    - `video_id`  
# MAGIC    - `video_title`  
# MAGIC    - `video_duration` (seconds)  
# MAGIC    - `azure_blob_name` (filename in container)  
# MAGIC    - `downloaded_timestamp`  
# MAGIC 3. **Skips** already-processed videos.
# MAGIC 4. Skips shorts (<60s).
# MAGIC 5. **Random sleeps** between channels to reduce potential bot triggers.
# MAGIC 6. **Uploads** files to Azure Blob Storage, using a fresh cookies file for YouTube authentication.

# COMMAND ----------
import os
import re
import time
import random
import tempfile
from datetime import datetime

import yt_dlp
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from googleapiclient.discovery import build

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Copy Cookies File from DBFS

# COMMAND ----------
dbutils.fs.cp(
    "dbfs:/Volumes/yt-deslopify/default/youtube/cookies/www.youtube.com_cookies.txt",
    "file:/tmp/youtube_cookies.txt",
    recurse=False
)

COOKIE_FILE = "/tmp/youtube_cookies.txt"
if not os.path.exists(COOKIE_FILE):
    raise FileNotFoundError(
        f"Cookies file not found at {COOKIE_FILE}. Ensure it was copied successfully!"
    )
print(f"Cookie file is at {COOKIE_FILE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Create/Alter the Processed Table with Additional Columns

# COMMAND ----------
# We'll store these fields in `channel_processed_videos`.
# If you already have an older table, we'll do an ALTER to add missing columns.

spark.sql("""
    CREATE TABLE IF NOT EXISTS `yt-deslopify`.default.channel_processed_videos (
        channel_id          STRING,
        channel_name        STRING,
        channel_url         STRING,
        video_id            STRING,
        video_title         STRING,
        video_duration      INT,
        azure_blob_name     STRING,
        downloaded_timestamp TIMESTAMP
    )
    USING delta
""")

# Optionally, ensure we have all columns (just in case).
# For example, if the table already existed, you can do:
existing_cols = set(row['col_name'].lower() for row in spark.sql(
    "DESCRIBE TABLE `yt-deslopify`.default.channel_processed_videos"
).collect())

expected_cols = {
    'channel_id', 'channel_name', 'channel_url',
    'video_id', 'video_title', 'video_duration',
    'azure_blob_name', 'downloaded_timestamp'
}

missing_cols = expected_cols - existing_cols
for col in missing_cols:
    # We'll pick simple data types for missing columns
    if col == 'video_duration':
        spark.sql(f"ALTER TABLE `yt-deslopify`.default.channel_processed_videos ADD COLUMNS ({col} INT)")
    elif col == 'downloaded_timestamp':
        spark.sql(f"ALTER TABLE `yt-deslopify`.default.channel_processed_videos ADD COLUMNS ({col} TIMESTAMP)")
    else:
        spark.sql(f"ALTER TABLE `yt-deslopify`.default.channel_processed_videos ADD COLUMNS ({col} STRING)")

print("Ensured table `yt-deslopify`.default.channel_processed_videos with new columns")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Grab Channels from `channel_statistics`

# COMMAND ----------
channel_df = spark.sql("""
    SELECT name, url
    FROM `yt-deslopify`.default.channel_statistics
    WHERE url IS NOT NULL
""")

channels = channel_df.collect()
channel_count = len(channels)
if channel_count == 0:
    raise Exception("No channels found in `channel_statistics` with a valid URL")

print(f"Found {channel_count} channels to process.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. YouTube Helpers (API + Short-Skipping)

# COMMAND ----------
def get_youtube_api_key():
    credential = DefaultAzureCredential()
    key_vault_url = "https://gattaca-keys.vault.azure.net/"
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
    return secret_client.get_secret("youtube-v3-api-key-yt-deslopify").value

def parse_iso8601_duration(duration_str):
    """
    e.g. 'PT1H2M3S' -> int seconds
    """
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration_str)
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    mins  = int(match.group(2) or 0)
    secs  = int(match.group(3) or 0)
    return hours * 3600 + mins * 60 + secs

def get_nonshort_videos(channel_url, max_results=20):
    """
    1) Find channel ID from channel_url
    2) Get up to max_results from 'uploads' playlist
    3) Calls videos().list to get contentDetails.duration
    4) Skip <60s
    5) Return list of {id, title, url, duration_in_sec}
    """
    youtube = build('youtube', 'v3', developerKey=get_youtube_api_key())
    
    channel_id = channel_url.strip().split('/')[-1]
    ch_resp = youtube.channels().list(part='contentDetails', id=channel_id).execute()
    if not ch_resp.get('items'):
        print(f"No channel found for ID={channel_id}")
        return []
    
    playlist_id = ch_resp['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    videos = []
    req = youtube.playlistItems().list(
        part='snippet',
        playlistId=playlist_id,
        maxResults=max_results
    )
    while req and len(videos) < max_results:
        resp = req.execute()
        for item in resp.get('items', []):
            vid_id = item['snippet']['resourceId']['videoId']
            title  = item['snippet']['title']
            videos.append({
                'id': vid_id,
                'title': title,
                'url': f'https://www.youtube.com/watch?v={vid_id}',
            })
        req = youtube.playlistItems().list_next(req, resp)
    
    # Now get durations
    if not videos:
        return []
    
    chunked = []
    for i in range(0, len(videos), 50):
        chunk = videos[i:i+50]
        chunked.append(chunk)
    
    filtered = []
    for chunk in chunked:
        ids_str = ",".join(v['id'] for v in chunk)
        det_resp = youtube.videos().list(part='contentDetails', id=ids_str).execute()
        
        dur_map = {}
        for vi in det_resp.get('items', []):
            v_id = vi['id']
            iso_str = vi['contentDetails']['duration']
            dur_map[v_id] = parse_iso8601_duration(iso_str)
        
        for v in chunk:
            dur_s = dur_map.get(v['id'], 0)
            if dur_s >= 60:
                v['duration_in_sec'] = dur_s
                filtered.append(v)
            else:
                print(f"Skipping short (<60s): {v['title']} (ID={v['id']}, {dur_s}s)")
    
    return filtered

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. YT-DLP Download + Upload Helper

# COMMAND ----------
def download_and_upload_video(video_info, channel_name, channel_id, cookiefile):
    """
    Try multiple mp4-based formats, normal JS extraction,
    then upload to Azure. Return the Azure blob name.
    """
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # Prepare a safe local filename
            now = datetime.now().strftime('%Y%m%d_%H%M%S')
            sc = "".join(c for c in channel_name if c.isalnum() or c in (' ', '-', '_')).strip()
            sv = "".join(c for c in video_info['title'] if c.isalnum() or c in (' ', '-', '_')).strip()
            filename = f"{sc}_{sv}_{now}.mp4"
            out_path = os.path.join(tmpdir, filename)
            
            HEADERS = {
                'User-Agent': (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/115.0.0.0 Safari/537.36"
                ),
                'Accept-Language': 'en-US,en;q=0.9'
            }
            
            fmt_opts = [
                'best[ext=mp4]',
                'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                'worstvideo[ext=mp4]+worstaudio[ext=m4a]',
            ]
            
            # Example advanced extractor args
            ex_args = {
                'youtube': {
                    'player_client': ['web','mweb','ios','android','tv']
                }
            }
            
            base_opts = {
                'cookiefile': cookiefile,
                'outtmpl': out_path,
                'quiet': False,
                'no_warnings': False,
                'progress': True,
                'geo_bypass': True,
                'sleep_interval': 2,
                'max_sleep_interval': 6,
                'sleep_interval_requests': 2,
                'retries': 5,
                'fragment_retries': 5,
                'http_headers': HEADERS,
                'extractor_args': ex_args
            }
            
            success = False
            last_err = None
            for fm in fmt_opts:
                try:
                    print(f"\nAttempting format: {fm}")
                    ydl_opts = dict(base_opts)
                    ydl_opts['format'] = fm
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        ydl.download([video_info['url']])
                    success = True
                    break
                except Exception as ex:
                    last_err = ex
                    print(f"Format {fm} failed: {ex}")
            
            if not success:
                raise RuntimeError(
                    f"All format attempts failed for {video_info['url']}.\n{last_err}"
                )
            
            # Upload to azure
            account_url = "https://gattacav2.blob.core.windows.net"
            credential = DefaultAzureCredential()
            service_client = BlobServiceClient(account_url=account_url, credential=credential)
            container_name = "yt-deslopify"
            blob_client = service_client.get_container_client(container_name)
            
            print(f"\nUploading => {filename}")
            with open(out_path, 'rb') as fh:
                blob_client.upload_blob(name=filename, data=fh, overwrite=True)
            
            print(f"Upload done: {filename}")
            return filename
    except Exception as e:
        print(f"Error in download_and_upload_video: {e}")
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Main Loop Over Channels with Crash Resistance

# COMMAND ----------
import random
import time

try:
    # load existing processed
    # note that we only fetch video_id because we also want to track channel_id etc. 
    # but for duplicates, we care about video_id
    processed_df = spark.sql("SELECT video_id FROM `yt-deslopify`.default.channel_processed_videos")
    processed_ids = set(row['video_id'] for row in processed_df.collect())
    
    for idx, row in enumerate(channels, start=1):
        ch_name = row["name"]
        ch_url  = row["url"]
        ch_id   = ch_url.strip().split('/')[-1]  # simple parsing from channel URL
        
        print(f"\n=== Channel {idx}/{channel_count} ===")
        print(f"Name: {ch_name}\nURL : {ch_url}\nID  : {ch_id}")
        
        all_nonshort = get_nonshort_videos(ch_url, max_results=20)
        if not all_nonshort:
            print("No non-short videos or channel invalid. Skipping.\n")
            continue
        
        # skip duplicates
        new_videos = [v for v in all_nonshort if v['id'] not in processed_ids]
        if not new_videos:
            print("All found videos are already processed. Skipping.\n")
            continue
        
        # take up to 10
        to_download = new_videos[:3]
        print(f"Found {len(all_nonshort)} non-short videos, {len(new_videos)} are new. Will download {len(to_download)}.\n")
        
        for i, vid_info in enumerate(to_download, start=1):
            print(f"--- Video {i}/{len(to_download)} for '{ch_name}' ---")
            print(f"Title: {vid_info['title']}")
            print(f"URL  : {vid_info['url']}")
            
            azure_filename = download_and_upload_video(vid_info, ch_name, ch_id, COOKIE_FILE)
            print(f" -> Done: {azure_filename}\n")
            
            # Insert row in `channel_processed_videos`
            safe_title  = vid_info['title'].replace("'", "''")
            safe_name   = ch_name.replace("'", "''")
            safe_ch_url = ch_url.replace("'", "''")
            safe_ch_id  = ch_id.replace("'", "''")
            safe_vid_id = vid_info['id'].replace("'", "''")
            
            spark.sql(f"""
                INSERT INTO `yt-deslopify`.default.channel_processed_videos
                (
                    channel_id,
                    channel_name,
                    channel_url,
                    video_id,
                    video_title,
                    video_duration,
                    azure_blob_name,
                    downloaded_timestamp
                )
                VALUES (
                    '{safe_ch_id}',
                    '{safe_name}',
                    '{safe_ch_url}',
                    '{safe_vid_id}',
                    '{safe_title}',
                     {vid_info['duration_in_sec']},
                    '{azure_filename}',
                     current_timestamp()
                )
            """)
            
            # Add to local set so we skip if the job continues
            processed_ids.add(vid_info['id'])
        
        # random sleep between channels
        if idx < channel_count:
            snooze = random.randint(10, 40)
            print(f"Sleeping {snooze}s before next channel...\n")
            time.sleep(snooze)
    
    print("\nAll channels processed successfully!")
except Exception as err:
    print(f"Failed to process channels: {err}")
    raise