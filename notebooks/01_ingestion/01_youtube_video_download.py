# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # YouTube Download: Skip Shorts (< 60 sec)

# COMMAND ----------
import os
import tempfile
from datetime import datetime, timedelta
import re

import yt_dlp
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from googleapiclient.discovery import build

# COMMAND ----------
# MAGIC %md
# MAGIC ## Copy Cookies File from DBFS to Local Path

# COMMAND ----------
dbutils.fs.cp(
    "dbfs:/Volumes/yt-deslopify/default/youtube/cookies/www.youtube.com_cookies.txt",
    "file:/tmp/youtube_cookies.txt",
    recurse=False
)

cookiefile_path = "/tmp/youtube_cookies.txt"
if not os.path.exists(cookiefile_path):
    raise FileNotFoundError(
        f"Cookies file not found at {cookiefile_path}. "
        "Ensure it was copied successfully from DBFS!"
    )
print(f"Cookie file is at {cookiefile_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Grab Channel Info from Table

# COMMAND ----------
channel_df = spark.sql("""
    SELECT name, url
    FROM `yt-deslopify`.default.channel_statistics
    ORDER BY views DESC
    LIMIT 1
""")

if channel_df.count() == 0:
    raise Exception("No channel found in the table.")

channel_info = channel_df.collect()[0]
channel_name = channel_info['name']
channel_url = channel_info['url']

print(f"Selected channel: {channel_name}\nChannel URL: {channel_url}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## YouTube Utilities

# COMMAND ----------
def get_youtube_api_key():
    credential = DefaultAzureCredential()
    key_vault_url = "https://gattaca-keys.vault.azure.net/"
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
    return secret_client.get_secret("youtube-v3-api-key-yt-deslopify").value

def parse_iso8601_duration(duration_str):
    """
    Convert an ISO8601 duration (e.g. 'PT5M33S', 'PT15S', 'PT1H2M3S') to seconds (int).
    """
    # Remove 'PT' prefix
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration_str)
    if not match:
        # If something's off, return 0
        return 0
    hours = int(match.group(1) or 0)
    mins = int(match.group(2) or 0)
    secs = int(match.group(3) or 0)
    total_seconds = hours * 3600 + mins * 60 + secs
    return total_seconds

def get_channel_videos_excluding_shorts(channel_url, max_results=50):
    """
    1) Finds up to 'max_results' videos from the channel's 'uploads' playlist (using snippet)
    2) Calls videos().list to get each video's duration from contentDetails
    3) Skips any videos with duration < 60 seconds
    """
    youtube = build('youtube', 'v3', developerKey=get_youtube_api_key())
    
    # Extract channel ID from URL
    channel_id = channel_url.strip().split('/')[-1]
    
    # 1) retrieve the channel's 'uploads' playlist
    channel_resp = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    ).execute()
    if not channel_resp.get('items'):
        raise Exception(f"No channel found with ID: {channel_id}")
    
    playlist_id = channel_resp['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    # 2) fetch items from that playlist
    videos = []
    request = youtube.playlistItems().list(
        part='snippet',
        playlistId=playlist_id,
        maxResults=max_results
    )
    while request and len(videos) < max_results:
        response = request.execute()
        for item in response['items']:
            vid_id = item['snippet']['resourceId']['videoId']
            title = item['snippet']['title']
            videos.append({
                'id': vid_id,
                'title': title,
                'url': f'https://www.youtube.com/watch?v={vid_id}'
            })
        request = youtube.playlistItems().list_next(request, response)
    
    if not videos:
        return []
    
    # 3) Now get durations for each
    # We'll chunk them in up to 50 IDs per call for efficiency
    filtered_videos = []
    chunk_size = 50
    for i in range(0, len(videos), chunk_size):
        sublist = videos[i:i+chunk_size]
        ids_str = ",".join([v['id'] for v in sublist])
        
        vids_resp = youtube.videos().list(
            part='contentDetails',
            id=ids_str
        ).execute()
        
        # Map from ID -> duration in seconds
        id_to_duration = {}
        for v in vids_resp.get('items', []):
            this_id = v['id']
            iso_dur = v['contentDetails']['duration']
            seconds = parse_iso8601_duration(iso_dur)
            id_to_duration[this_id] = seconds
        
        # Filter out any < 60
        for v in sublist:
            vid_sec = id_to_duration.get(v['id'], 0)
            if vid_sec >= 60:
                # keep it
                filtered_videos.append(v)
            else:
                # skip short
                print(f"Skipping SHORT video < 60s: {v['title']} (ID={v['id']}, {vid_sec}s)")
    
    return filtered_videos

def download_and_upload_video(video_info, channel_name, cookiefile):
    """
    Attempts multiple MP4-based formats, with normal JS loading (no skip).
    """
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Prepare a safe filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            safe_channel_name = "".join(c for c in channel_name if c.isalnum() or c in (' ', '-', '_')).strip()
            safe_video_title = "".join(c for c in video_info['title'] if c.isalnum() or c in (' ', '-', '_')).strip()
            filename = f"{safe_channel_name}_{safe_video_title}_{timestamp}.mp4"
            output_path = os.path.join(temp_dir, filename)
            
            # For normal "browser" appearance
            SPOOF_HEADERS = {
                'User-Agent':
                    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/115.0.0.0 Safari/537.36"),
                'Accept-Language': 'en-US,en;q=0.9'
            }
            
            # Basic format combos
            format_options = [
                'best[ext=mp4]',
                'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                'worstvideo[ext=mp4]+worstaudio[ext=m4a]',
            ]
            
            # ydl base opts
            base_opts = {
                'cookiefile': cookiefile,
                'outtmpl': output_path,
                'quiet': False,
                'no_warnings': False,
                'progress': True,
                'geo_bypass': True,
                'sleep_interval': 2,
                'max_sleep_interval': 4,
                'ignore_no_formats_error': False,
                'http_headers': SPOOF_HEADERS,
                
                # We do NOT set "allow_unplayable_formats" now,
                # so we won't see that "developer option" warning.
            }
            
            success = False
            last_err = None
            
            for fm in format_options:
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
                    f"All format attempts failed. Last error:\n{last_err}"
                )
            
            # Upload to Azure Blob
            account_url = "https://gattacav2.blob.core.windows.net"
            credential = DefaultAzureCredential()
            service_client = BlobServiceClient(account_url=account_url, credential=credential)
            container_name = "yt-deslopify"
            blob_client = service_client.get_container_client(container_name)
            
            print(f"\nUploading to Azure: {filename}")
            with open(output_path, 'rb') as fh:
                blob_client.upload_blob(name=filename, data=fh, overwrite=True)
            
            print(f"Upload complete: {filename}")
            return filename
    except Exception as e:
        print(f"Error in download_and_upload_video: {e}")
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Main Logic

# COMMAND ----------
try:
    # fetch up to 10 videos from that channel (to have some buffer),
    # then exclude the ones <60s
    vids = get_channel_videos_excluding_shorts(channel_url, max_results=10)
    if not vids:
        raise Exception("No suitable (non-short) videos found. All are under 60s or none available.")
    
    # We'll just download first 2
    print(f"Found {len(vids)} non-short videos. Will attempt up to 2.\n")
    
    for idx, vid_info in enumerate(vids[:2], start=1):
        print(f"=== Video {idx} of {len(vids)} ===")
        print(f"Title: {vid_info['title']}")
        print(f"URL  : {vid_info['url']}")
        final_name = download_and_upload_video(vid_info, channel_name, cookiefile_path)
        print(f" -> Done: {final_name}\n")
        
    print("All finished!")
except Exception as err:
    print(f"Failed to process videos: {err}")
    raise