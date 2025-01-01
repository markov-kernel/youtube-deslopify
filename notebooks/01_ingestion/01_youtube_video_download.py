# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # YouTube Download: Skip Shorts, MP3 Only (64k)

# COMMAND ----------
import os
import tempfile
import isodate
from datetime import datetime
import subprocess
import requests
import tarfile
import shutil

# Azure / Google / yt_dlp imports
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import yt_dlp

# Spark, DBUtils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Setup ffmpeg

# COMMAND ----------
# Download and setup static ffmpeg binaries
FFMPEG_DIR = "/tmp/ffmpeg_bin"
if not os.path.exists(FFMPEG_DIR):
    os.makedirs(FFMPEG_DIR)

# Download static ffmpeg build for Linux x64
FFMPEG_URL = "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"
FFMPEG_TAR = "/tmp/ffmpeg.tar.xz"

if not os.path.exists(os.path.join(FFMPEG_DIR, "ffmpeg")):
    print("Downloading ffmpeg static binaries...")
    response = requests.get(FFMPEG_URL)
    with open(FFMPEG_TAR, 'wb') as f:
        f.write(response.content)
    
    print("Extracting ffmpeg binaries...")
    with tarfile.open(FFMPEG_TAR) as tar:
        # Extract only ffmpeg and ffprobe executables
        for member in tar.getmembers():
            if member.name.endswith(('/ffmpeg', '/ffprobe')):
                member.name = os.path.basename(member.name)  # Remove directory structure
                tar.extract(member, FFMPEG_DIR)
    
    # Make executables
    os.chmod(os.path.join(FFMPEG_DIR, "ffmpeg"), 0o755)
    os.chmod(os.path.join(FFMPEG_DIR, "ffprobe"), 0o755)
    
    # Cleanup
    os.remove(FFMPEG_TAR)

# Verify ffmpeg works
try:
    result = subprocess.run(
        [os.path.join(FFMPEG_DIR, "ffmpeg"), "-version"],
        capture_output=True,
        text=True
    )
    print("ffmpeg version info:", result.stdout.split('\n')[0])
except Exception as e:
    raise RuntimeError(f"Failed to verify ffmpeg installation: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Copy Cookies File from DBFS to Local

# COMMAND ----------
# Ensure cookies file exists and is accessible
cookie_dbfs_path = "dbfs:/Volumes/yt-deslopify/default/youtube/cookies/www.youtube.com_cookies.txt"
local_cookiefile = "/tmp/youtube_cookies.txt"

# Copy cookies file from DBFS to local
dbutils.fs.cp(cookie_dbfs_path, f"file:{local_cookiefile}", recurse=False)

if not os.path.exists(local_cookiefile):
    raise FileNotFoundError(f"Cookies file not found at {local_cookiefile}")

# Verify file has content
with open(local_cookiefile, 'r') as f:
    cookie_content = f.read().strip()
    if not cookie_content:
        raise ValueError("Cookie file exists but is empty!")
    
print(f"Cookie file present and verified at {local_cookiefile}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Helper Functions

# COMMAND ----------
def get_youtube_api_key():
    """
    Get YouTube API key from Azure Key Vault using Managed Identity
    """
    credential = DefaultAzureCredential()
    key_vault_url = "https://gattaca-keys.vault.azure.net/"
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
    return secret_client.get_secret("youtube-v3-api-key-yt-deslopify").value

def get_channel_uploads(channel_id, max_results=50):
    """
    Returns 'uploads' playlist items for the channel_id,
    each containing video_id and snippet info
    """
    youtube = build('youtube', 'v3', developerKey=get_youtube_api_key())
    
    channel_resp = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    ).execute()
    
    if not channel_resp.get('items'):
        raise Exception(f"No channel found with ID: {channel_id}")
    playlist_id = channel_resp['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    vids = []
    req = youtube.playlistItems().list(
        part='snippet',
        playlistId=playlist_id,
        maxResults=min(max_results, 50)
    )
    while req and len(vids) < max_results:
        r = req.execute()
        for item in r['items']:
            vid_id = item['snippet']['resourceId']['videoId']
            vid_title = item['snippet']['title']
            vids.append({'id': vid_id, 'title': vid_title})
        req = youtube.playlistItems().list_next(req, r)
    
    return vids

def get_video_durations(video_ids):
    """
    Call videos.list(contentDetails) to get durations for each of the video_ids
    Returns a dict {video_id -> duration_in_seconds}
    """
    if not video_ids:
        return {}
    youtube = build('youtube', 'v3', developerKey=get_youtube_api_key())
    
    durations_map = {}
    try:
        r = youtube.videos().list(
            part='contentDetails',
            id=",".join(video_ids)
        ).execute()
        for item in r.get('items', []):
            vid_id = item['id']
            iso_dur = item['contentDetails']['duration']
            td = isodate.parse_duration(iso_dur)
            durations_map[vid_id] = td.total_seconds()
    except HttpError as e:
        print(f"Error retrieving durations: {e}")
    return durations_map

def get_videos_nonshort(channel_url, max_results=50):
    """
    1) Extract channel ID from channel_url
    2) Get uploads
    3) Filter out videos < 60s
    """
    channel_id = channel_url.split('/')[-1]
    vids = get_channel_uploads(channel_id, max_results=max_results)
    if not vids:
        return []
    # fetch durations
    durations_map = get_video_durations([v['id'] for v in vids])
    
    final = []
    for v in vids:
        dur = durations_map.get(v['id'], None)
        if dur is not None and dur >= 60:
            final.append({
                'url': f"https://www.youtube.com/watch?v={v['id']}",
                'title': v['title'],
                'duration': dur
            })
    return final

def download_and_upload_audio_mp3_lowq(video_info, channel_name, cookies_path, ffmpeg_path):
    """
    Download 'bestaudio/best', re-encode to 64 kbps mp3
    Then upload to Azure Blob
    """
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            safe_ch = "".join(c for c in channel_name if c.isalnum() or c in (' ', '-', '_')).strip()
            safe_ti = "".join(c for c in video_info['title'] if c.isalnum() or c in (' ', '-', '_')).strip()
            filename = f"{safe_ch}_{safe_ti}_{stamp}.mp3"
            out_path = os.path.join(tmpdir, filename)
            
            ydl_opts = {
                'outtmpl': out_path[:-4],  # Remove .mp3 as yt-dlp will add it
                'cookiefile': cookies_path,
                'cookiesfrombrowser': None,  # Don't try browser cookies
                'format': 'bestaudio/best',
                # Re-encode audio to mp3 64 kbps
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '64'
                }],
                'ffmpeg_location': ffmpeg_path,
                'quiet': False,
                'no_warnings': False,
                # Additional options to help bypass bot detection
                'sleep_interval': 5,  # Increased sleep between requests
                'max_sleep_interval': 10,
                'sleep_interval_requests': 3,
                'geo_bypass': True,
                'geo_bypass_country': 'US',
                'extractor_retries': 5,  # Retry more times
                'http_headers': {  # Mimic a real browser
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-us,en;q=0.5',
                    'Sec-Fetch-Mode': 'navigate'
                }
            }

            print(f"Downloading and converting to MP3: {video_info['title']}")
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_info['url']])
            
            # The actual output file has .mp3 extension added by yt-dlp
            final_out_path = out_path[:-4] + '.mp3'
            if not os.path.exists(final_out_path):
                raise FileNotFoundError(f"Expected output file not found at: {final_out_path}")
            
            # Now upload to Azure
            account_url = "https://gattacav2.blob.core.windows.net"
            cred = DefaultAzureCredential()
            blob_service_client = BlobServiceClient(account_url=account_url, credential=cred)
            container_name = "yt-deslopify"
            container_client = blob_service_client.get_container_client(container_name)
            
            print(f"Uploading {filename} to container '{container_name}'...")
            with open(final_out_path, 'rb') as f:
                container_client.upload_blob(name=filename, data=f)
            
            print(f"Upload of {filename} complete!")
            return filename

    except Exception as e:
        print(f"Error in download_and_upload_audio_mp3_lowq: {e}")
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Orchestrate the Download Flow

# COMMAND ----------
try:
    # Get top 50 channels by views
    channel_df = spark.sql("""
        SELECT name, url
        FROM `yt-deslopify`.default.channel_statistics
        ORDER BY views DESC
        LIMIT 50
    """)
    
    if not channel_df.count():
        raise Exception("No channels found in table!")
    
    channels = channel_df.collect()
    print(f"Processing {len(channels)} channels...")
    
    # Track overall stats
    total_processed = 0
    failed_downloads = []
    successful_downloads = []
    
    ffmpeg_dir = "/tmp/ffmpeg_bin"  # where we placed the static binaries
    
    # Process each channel
    for channel_idx, row in enumerate(channels, 1):
        channel_name = row['name']
        channel_url = row['url']
        print(f"\n[{channel_idx}/50] Processing channel: {channel_name}")
        print(f"URL: {channel_url}")
        
        try:
            # Get non-short videos
            videos = get_videos_nonshort(channel_url, max_results=50)  # fetch more to ensure we have enough after filtering
            if not videos:
                print(f"No non-short videos found for channel {channel_name}, skipping...")
                continue
                
            print(f"Found {len(videos)} non-short videos, will process latest 10")
            
            # Take the 10 most recent videos (they're already in chronological order, most recent first)
            to_download = videos[:10]
            
            # Process videos for this channel
            for vid_idx, vid in enumerate(to_download, 1):
                print(f"\nProcessing video {vid_idx}/10: '{vid['title']}' ({vid['duration']} seconds)")
                try:
                    output_name = download_and_upload_audio_mp3_lowq(
                        vid,
                        channel_name,
                        cookies_path=local_cookiefile,
                        ffmpeg_path=ffmpeg_dir
                    )
                    print(f"Successfully processed: {output_name}")
                    successful_downloads.append({
                        'channel': channel_name,
                        'video': vid['title'],
                        'output': output_name
                    })
                    total_processed += 1
                    
                except Exception as e:
                    print(f"Failed to process video: {e}")
                    failed_downloads.append({
                        'channel': channel_name,
                        'video': vid['title'],
                        'error': str(e)
                    })
                    continue
            
        except Exception as e:
            print(f"Error processing channel {channel_name}: {e}")
            continue
            
        # After each channel, print a progress update
        print(f"\nChannel {channel_idx}/50 complete.")
        print(f"Running totals:")
        print(f"- Successfully processed: {total_processed}")
        print(f"- Failed downloads: {len(failed_downloads)}")
    
    # Final summary
    print("\n=== Final Processing Summary ===")
    print(f"Total channels processed: {len(channels)}")
    print(f"Total successful downloads: {total_processed}")
    print(f"Total failed downloads: {len(failed_downloads)}")
    
    if failed_downloads:
        print("\nFailed Downloads:")
        for fail in failed_downloads:
            print(f"- Channel: {fail['channel']}")
            print(f"  Video: {fail['video']}")
            print(f"  Error: {fail['error']}")
            print()
    
    print("\nWorkflow complete!")

except Exception as e:
    print(f"Workflow failed: {e}")
    raise