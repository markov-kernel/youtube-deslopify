# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Download Non-Short YouTube Videos For All Channels
# MAGIC 
# MAGIC This code:
# MAGIC 1. Gets cookies from DBFS
# MAGIC 2. For each channel in `channel_statistics`, fetches up to 20 recent uploads
# MAGIC 3. Skips those <60s (Shorts)
# MAGIC 4. Downloads up to 10 of them using fresh cookies and standard JS extraction
# MAGIC 5. Uploads the video file to Azure Blob
# MAGIC 
# MAGIC **No impersonation** used, so we avoid the `Impersonate target not available` error.

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
# MAGIC ## Copy Cookies File from DBFS

# COMMAND ----------
# Adjust path as needed if your cookies are in a different location
dbutils.fs.cp(
    "dbfs:/Volumes/yt-deslopify/default/youtube/cookies/www.youtube.com_cookies.txt",
    "file:/tmp/youtube_cookies.txt",
    recurse=False
)

COOKIE_FILE = "/tmp/youtube_cookies.txt"
if not os.path.exists(COOKIE_FILE):
    raise FileNotFoundError(
        f"Cookies file not found at {COOKIE_FILE}. "
        "Ensure it was copied successfully from DBFS!"
    )
print(f"Cookie file is at {COOKIE_FILE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Grab All Channels from Table

# COMMAND ----------
channel_df = spark.sql("""
    SELECT name, url
    FROM `yt-deslopify`.default.channel_statistics
    WHERE url IS NOT NULL
""")

channels = channel_df.collect()
channel_count = len(channels)
if channel_count == 0:
    raise Exception("No channels found in table with valid URL")

print(f"Found {channel_count} channels.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## YouTube Data API Helpers

# COMMAND ----------
def get_youtube_api_key():
    """Fetch your YouTube Data API key from Azure Key Vault."""
    credential = DefaultAzureCredential()
    key_vault_url = "https://gattaca-keys.vault.azure.net/"
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
    return secret_client.get_secret("youtube-v3-api-key-yt-deslopify").value

def parse_iso8601_duration(duration_str):
    """
    Convert e.g. 'PT5M33S', 'PT1H2M3S' → int seconds.
    """
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration_str)
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    mins = int(match.group(2) or 0)
    secs = int(match.group(3) or 0)
    return hours * 3600 + mins * 60 + secs

def get_nonshort_videos(channel_url, max_results=20):
    """
    For a channel's 'uploads' playlist:
      - Grab up to 'max_results' items
      - Get each video's duration from videos().list
      - Return only those >=60s
    """
    youtube = build('youtube', 'v3', developerKey=get_youtube_api_key())
    
    # Extract channel ID from the URL
    channel_id = channel_url.strip().split('/')[-1]
    ch_resp = youtube.channels().list(part='contentDetails', id=channel_id).execute()
    if not ch_resp.get('items'):
        print(f"Channel not found: ID={channel_id}")
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
            title = item['snippet']['title']
            videos.append({
                'id': vid_id,
                'title': title,
                'url': f'https://www.youtube.com/watch?v={vid_id}',
            })
        req = youtube.playlistItems().list_next(req, resp)
    
    if not videos:
        return []
    
    # chunk in up to 50
    filtered = []
    for i in range(0, len(videos), 50):
        chunk = videos[i:i+50]
        ids_str = ",".join(v['id'] for v in chunk)
        det_resp = youtube.videos().list(part='contentDetails', id=ids_str).execute()
        dur_map = {}
        for vi in det_resp.get('items', []):
            vid = vi['id']
            iso_dur = vi['contentDetails']['duration']
            dur_map[vid] = parse_iso8601_duration(iso_dur)
        
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
# MAGIC ## YT-DLP Download + Upload Helper

# COMMAND ----------
def download_and_upload_video(video_info, channel_name, cookiefile):
    """
    Try multiple mp4-based formats, with standard headers/cookies,
    and upload to Azure.
    """
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # Safe local filename
            now = datetime.now().strftime('%Y%m%d_%H%M%S')
            sc = "".join(c for c in channel_name if c.isalnum() or c in (' ', '-', '_')).strip()
            sv = "".join(c for c in video_info['title'] if c.isalnum() or c in (' ', '-', '_')).strip()
            filename = f"{sc}_{sv}_{now}.mp4"
            out_path = os.path.join(tmpdir, filename)
            
            # Use normal browser-like headers
            HEADERS = {
                'User-Agent': (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/115.0.0.0 Safari/537.36"
                ),
                'Accept-Language': 'en-US,en;q=0.9'
            }
            
            # Just do the basic 3 fallback format combos
            fmt_opts = [
                'best[ext=mp4]',
                'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                'worstvideo[ext=mp4]+worstaudio[ext=m4a]',
            ]
            
            # We'll keep the advanced "extractor_args" minimal
            # if we want to force multiple clients:
            # 'youtube': { 'player_client': ['web','ios','mweb','tv','android'] }
            ex_args = {
                'youtube': {
                    # This is optional. If it doesn't help,
                    # you can remove it entirely.
                    'player_client': ['web','mweb','android','ios','tv']
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
                'extractor_args': ex_args,
                
                # No impersonate argument
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
            
            # Upload to Azure Blob
            account_url = "https://gattacav2.blob.core.windows.net"
            credential = DefaultAzureCredential()
            service_client = BlobServiceClient(account_url=account_url, credential=credential)
            container_name = "yt-deslopify"
            blob_client = service_client.get_container_client(container_name)
            
            print(f"\nUploading => {filename}")
            with open(out_path, 'rb') as fh:
                blob_client.upload_blob(name=filename, data=fh, overwrite=True)
            
            print(f"Uploaded: {filename}")
            return filename
    except Exception as e:
        print(f"Error in download_and_upload_video: {e}")
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Main Loop Over Channels

# COMMAND ----------
import random
import time

try:
    for idx, row in enumerate(channels, start=1):
        ch_name = row["name"]
        ch_url  = row["url"]
        
        print(f"\n=== Channel {idx}/{channel_count} ===")
        print(f"Name: {ch_name}\nURL : {ch_url}")
        
        # fetch up to 20 uploads, skip <60s
        all_nonshort = get_nonshort_videos(ch_url, max_results=20)
        if not all_nonshort:
            print("No non-short videos found or channel invalid. Skipping.\n")
            continue
        
        to_download = all_nonshort[:10]  # up to 10
        print(f"Found {len(all_nonshort)} non-short videos. Will download up to {len(to_download)}.\n")
        
        for i, vid_info in enumerate(to_download, start=1):
            print(f"--- Video {i}/{len(to_download)} for '{ch_name}' ---")
            print(f"Title: {vid_info['title']}")
            print(f"URL  : {vid_info['url']}")
            fn = download_and_upload_video(vid_info, ch_name, COOKIE_FILE)
            print(f" -> Done: {fn}\n")
        
        if idx < channel_count:
            # Sleep random 10-40 seconds between channels
            snooze = random.randint(10, 40)
            print(f"Sleeping {snooze}s before next channel...\n")
            time.sleep(snooze)
    
    print("\nAll channels processed successfully!")
except Exception as err:
    print(f"Failed to process channels: {err}")
    raise