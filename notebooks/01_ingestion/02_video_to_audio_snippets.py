# Databricks notebook source

# COMMAND ----------
# MAGIC %sh
# MAGIC apt-get update && apt-get install -y ffmpeg

# COMMAND ----------
# MAGIC %pip install pydub azure-storage-blob==12.* urllib3==1.26.15

# COMMAND ----------
# MAGIC %md
# MAGIC # Convert Videos to 3-Minute Audio Snippets
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. Creates a new Delta table `channel_audio_snippets` to track audio processing
# MAGIC 2. Processes videos using Spark
# MAGIC 3. Uploads snippets to a new directory in the same container
# MAGIC 4. Records metadata about the snippets

# COMMAND ----------
import os
import tempfile
from datetime import datetime
import subprocess
import time

from pydub import AudioSegment
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Create/Update Audio Snippets Table

# COMMAND ----------
# Check if table exists and get its schema
table_exists = spark.sql("SHOW TABLES IN `yt-deslopify`.default").filter("tableName = 'channel_audio_snippets'").count() > 0

if not table_exists:
    # Create new table with all columns
    spark.sql("""
        CREATE TABLE `yt-deslopify`.default.channel_audio_snippets (
            channel_id              STRING,
            channel_name           STRING,
            video_id               STRING,
            video_title           STRING,
            original_video_blob    STRING,
            audio_snippet_blob     STRING,
            snippet_duration_sec   INT,
            processed_timestamp    TIMESTAMP,
            status                STRING,
            error_message         STRING
        )
        USING delta
    """)
    print("Created new table channel_audio_snippets with all columns")
else:
    # Get existing columns
    existing_cols = set(row['col_name'].lower() for row in spark.sql(
        "DESCRIBE TABLE `yt-deslopify`.default.channel_audio_snippets"
    ).collect())
    
    # Add missing columns if needed
    if 'status' not in existing_cols:
        spark.sql("ALTER TABLE `yt-deslopify`.default.channel_audio_snippets ADD COLUMN status STRING")
    if 'error_message' not in existing_cols:
        spark.sql("ALTER TABLE `yt-deslopify`.default.channel_audio_snippets ADD COLUMN error_message STRING")
    
    print("Updated existing table channel_audio_snippets with any missing columns")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Get List of Videos to Process

# COMMAND ----------
# Get videos that haven't been processed yet
videos_df = spark.sql("""
    SELECT 
        v.channel_id,
        v.channel_name,
        v.video_id,
        v.video_title,
        v.azure_blob_name as original_video_blob
    FROM `yt-deslopify`.default.channel_processed_videos v
    LEFT JOIN `yt-deslopify`.default.channel_audio_snippets a
        ON v.video_id = a.video_id
    WHERE a.video_id IS NULL
""")

print(f"Found {videos_df.count()} videos to process")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Process Videos to Audio Snippets

# COMMAND ----------
def get_blob_service_client():
    """Get blob service client with retry logic"""
    for attempt in range(3):
        try:
            account_url = "https://gattacav2.blob.core.windows.net"
            credential = DefaultAzureCredential()
            return BlobServiceClient(account_url=account_url, credential=credential)
        except Exception as e:
            if attempt == 2:  # Last attempt
                raise
            time.sleep(2 ** attempt)  # Exponential backoff

def process_video(video_info):
    """Process a single video and return the result"""
    try:
        print(f"Starting to process video: {video_info.video_title}")
        blob_service_client = get_blob_service_client()
        container_client = blob_service_client.get_container_client("yt-deslopify")
        
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = os.path.join(tmpdir, "video.mp4")
            
            # Download video
            print(f"Downloading: {video_info.video_title}")
            with open(video_path, "wb") as video_file:
                blob_client = container_client.get_blob_client(video_info.original_video_blob)
                blob_data = blob_client.download_blob()
                for chunk in blob_data.chunks():
                    video_file.write(chunk)
            
            # Create audio snippet
            now = datetime.now().strftime('%Y%m%d_%H%M%S')
            audio_filename = f"audio_snippets/{video_info.channel_id}_{video_info.video_id}_{now}.mp3"
            audio_path = os.path.join(tmpdir, "snippet.mp3")
            temp_audio = audio_path + ".temp.mp3"
            
            # Extract audio
            print(f"Extracting audio: {video_info.video_title}")
            subprocess.run([
                'ffmpeg', '-i', video_path,
                '-t', '180',
                '-q:a', '0',
                '-map', 'a',
                temp_audio
            ], check=True, capture_output=True)
            
            # Process with pydub
            print(f"Processing with pydub: {video_info.video_title}")
            audio = AudioSegment.from_mp3(temp_audio)
            if len(audio) > 180000:  # 3 minutes in milliseconds
                audio = audio[:180000]
            audio.export(audio_path, format='mp3')
            os.remove(temp_audio)
            
            # Upload audio snippet
            print(f"Uploading audio: {video_info.video_title}")
            with open(audio_path, "rb") as audio_file:
                container_client.upload_blob(name=audio_filename, data=audio_file, overwrite=True)
            
            print(f"Successfully processed: {video_info.video_title}")
            
            # Insert into Delta table
            spark.sql(f"""
                INSERT INTO `yt-deslopify`.default.channel_audio_snippets
                VALUES (
                    '{video_info.channel_id}',
                    '{video_info.channel_name}',
                    '{video_info.video_id}',
                    '{video_info.video_title}',
                    '{video_info.original_video_blob}',
                    '{audio_filename}',
                    180,
                    current_timestamp(),
                    'success',
                    NULL
                )
            """)
            
    except Exception as e:
        error_msg = str(e).replace("'", "''")  # Escape single quotes for SQL
        print(f"Error processing {video_info.video_title}: {error_msg}")
        
        spark.sql(f"""
            INSERT INTO `yt-deslopify`.default.channel_audio_snippets
            VALUES (
                '{video_info.channel_id}',
                '{video_info.channel_name}',
                '{video_info.video_id}',
                '{video_info.video_title}',
                '{video_info.original_video_blob}',
                NULL,
                NULL,
                current_timestamp(),
                'error',
                '{error_msg}'
            )
        """)

# COMMAND ----------
# Process videos
videos = videos_df.collect()
for video in videos:
    process_video(video)

# COMMAND ----------
# Get processing summary
spark.sql("""
    SELECT 
        status,
        COUNT(*) as count
    FROM `yt-deslopify`.default.channel_audio_snippets
    GROUP BY status
""").show()

# Show any errors
spark.sql("""
    SELECT 
        video_title,
        error_message
    FROM `yt-deslopify`.default.channel_audio_snippets
    WHERE status = 'error'
""").show(truncate=False) 