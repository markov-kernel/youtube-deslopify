# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Whisper Transcription with Delta Tables
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. Creates a Delta table to track transcription progress
# MAGIC 2. Processes audio snippets using Whisper
# MAGIC 3. Stores transcriptions in Delta format
# MAGIC 4. Ensures crash resistance and no duplication

# COMMAND ----------
# MAGIC %pip install transformers torch torchaudio librosa

# COMMAND ----------
import os
import logging
import time
import warnings
import tempfile
from datetime import datetime
from typing import Optional, Dict, Any

import torch
import librosa
import numpy as np
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor, pipeline

# Optional imports for flash-attn
try:
    from torch.nn.attention import SDPBackend, sdpa_kernel
    FLASH_SUPPORT = True
except ImportError:
    FLASH_SUPPORT = False

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message=".*past_key_values.*")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Create/Update Transcription Tables

# COMMAND ----------
# Check if transcription tracking table exists
table_exists = spark.sql("SHOW TABLES IN `yt-deslopify`.default").filter("tableName = 'channel_transcriptions'").count() > 0

if not table_exists:
    # Create new table with all columns
    spark.sql("""
        CREATE TABLE `yt-deslopify`.default.channel_transcriptions (
            channel_id              STRING,
            channel_name           STRING,
            video_id               STRING,
            video_title           STRING,
            audio_snippet_blob     STRING,
            transcription_text     STRING,
            chunk_timestamps       ARRAY<STRUCT<start: DOUBLE, end: DOUBLE, text: STRING>>,
            detected_language      STRING,
            processing_time_sec    DOUBLE,
            processed_timestamp    TIMESTAMP,
            status                STRING,
            error_message         STRING
        )
        USING delta
    """)
    print("Created new table channel_transcriptions")
else:
    print("Table channel_transcriptions already exists")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Initialize Whisper Model

# COMMAND ----------
def initialize_whisper(
    model_id: str = "openai/whisper-large-v3-turbo",
    chunk_length_s: int = 60,
    batch_size: int = 16,
    use_flash_attn: bool = False,
    use_torch_compile: bool = False
) -> pipeline:
    """Initialize the Whisper model with optimized settings.
    
    Args:
        model_id: HuggingFace model ID (default: whisper-large-v3-turbo)
        chunk_length_s: Length of each audio chunk in seconds (30-60 recommended)
        batch_size: Number of chunks to process in parallel (tune for GPU memory)
        use_flash_attn: Whether to try flash-attn2 (requires 'flash-attn' package)
        use_torch_compile: Whether to use torch.compile for speed-ups (PyTorch 2+)
    """
    logger.info(f"Initializing Whisper model {model_id}")
    
    # Check for GPU
    if torch.cuda.is_available():
        device = "cuda"
        logger.info("Using CUDA device for inference.")
        torch_dtype = torch.float16
    else:
        device = "cpu"
        logger.info("Using CPU device for inference.")
        torch_dtype = torch.float32
    
    # Set matmul precision to 'high'
    torch.set_float32_matmul_precision("high")
    
    # Decide on attention implementation
    attn_impl = "flash_attention_2" if use_flash_attn else "sdpa"
    logger.info(f"Using attention implementation: {attn_impl}")
    
    # Try loading model with attn_implementation
    try:
        model = AutoModelForSpeechSeq2Seq.from_pretrained(
            model_id,
            torch_dtype=torch_dtype,
            low_cpu_mem_usage=True,
            use_safetensors=True,
            attn_implementation=attn_impl
        ).to(device)
    except TypeError as e:
        logger.warning(f"Could not set attn_implementation={attn_impl}. Fallback. Error: {e}")
        model = AutoModelForSpeechSeq2Seq.from_pretrained(
            model_id,
            torch_dtype=torch_dtype,
            low_cpu_mem_usage=True,
            use_safetensors=True
        ).to(device)
    
    # Optionally compile model
    if use_torch_compile and torch.__version__ >= "2.0":
        logger.info("Compiling model with torch.compile(...) for potential speed-ups.")
        model.forward = torch.compile(
            model.forward, mode="reduce-overhead", fullgraph=True
        )
    
    # Load processor
    processor = AutoProcessor.from_pretrained(model_id)
    
    # Create optimized pipeline
    pipe = pipeline(
        "automatic-speech-recognition",
        model=model,
        tokenizer=processor.tokenizer,
        feature_extractor=processor.feature_extractor,
        torch_dtype=torch_dtype,
        device=0 if device == "cuda" else -1,
        chunk_length_s=chunk_length_s,
        batch_size=batch_size,
        return_timestamps=True
    )
    
    logger.info(f"Model initialized (chunk_length_s={chunk_length_s}, batch_size={batch_size}).")
    return pipe

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Process Audio Snippets

# COMMAND ----------
def get_pending_snippets():
    """Get audio snippets that haven't been transcribed yet."""
    return spark.sql("""
        SELECT 
            a.channel_id,
            a.channel_name,
            a.video_id,
            a.video_title,
            a.audio_snippet_blob
        FROM `yt-deslopify`.default.channel_audio_snippets a
        LEFT JOIN `yt-deslopify`.default.channel_transcriptions t
            ON a.video_id = t.video_id
            AND a.audio_snippet_blob = t.audio_snippet_blob
        WHERE a.status = 'success'
            AND t.video_id IS NULL
    """)

def process_audio_snippet(pipe, snippet_info, container_client):
    """Process a single audio snippet and return the transcription result."""
    try:
        logger.info(f"Processing snippet for video: {snippet_info.video_title}")
        
        # Download audio snippet to temporary file
        with tempfile.NamedTemporaryFile(suffix='.mp3') as temp_audio:
            blob_client = container_client.get_blob_client(snippet_info.audio_snippet_blob)
            blob_data = blob_client.download_blob()
            blob_data.readinto(temp_audio)
            temp_audio.flush()
            
            # Load audio with librosa
            audio_array, sr = librosa.load(temp_audio.name, sr=16000)
        
        # Generation config for optimal results
        generate_kwargs = {
            "task": "transcribe",
            "language": None,  # Set to None for auto-detection
            "max_new_tokens": 440,
            "num_beams": 1,
            "condition_on_prev_tokens": False,
            "compression_ratio_threshold": 1.35,
            "temperature": (0.0, 0.2, 0.4, 0.6, 0.8, 1.0),
            "logprob_threshold": -1.0,
            "no_speech_threshold": 0.6,
            "return_timestamps": True
        }
        
        # Transcribe with timestamps
        start_time = time.time()
        
        # Use flash-attn2 or sdpa context if available
        if FLASH_SUPPORT:
            with torch.inference_mode(), sdpa_kernel(SDPBackend.MATH):
                result = pipe(audio_array, generate_kwargs=generate_kwargs)
        else:
            with torch.inference_mode():
                result = pipe(audio_array, generate_kwargs=generate_kwargs)
        
        processing_time = time.time() - start_time
        
        # Extract detected language
        detected_language = result.get("language", "auto")
        logger.info(f"Detected language: {detected_language}")
        
        # Format chunks for storage
        chunks = []
        if "chunks" in result:
            for chunk in result["chunks"]:
                start, end = chunk["timestamp"]
                chunks.append({
                    "start": float(start),
                    "end": float(end),
                    "text": chunk["text"].strip()
                })
        
        # Insert into Delta table
        chunk_array = spark.createDataFrame(chunks).collect()
        spark.sql(f"""
            INSERT INTO `yt-deslopify`.default.channel_transcriptions
            VALUES (
                '{snippet_info.channel_id}',
                '{snippet_info.channel_name}',
                '{snippet_info.video_id}',
                '{snippet_info.video_title}',
                '{snippet_info.audio_snippet_blob}',
                '{result["text"].replace("'", "''")}',
                array({','.join([f"named_struct('start', {c.start}, 'end', {c.end}, 'text', '{c.text.replace(chr(39), chr(39)+chr(39))}')" for c in chunk_array])}),
                '{detected_language}',
                {processing_time},
                current_timestamp(),
                'success',
                NULL
            )
        """)
        
        logger.info(f"Successfully transcribed: {snippet_info.video_title} in {processing_time:.2f}s")
        
    except Exception as e:
        error_msg = str(e).replace("'", "''")
        logger.error(f"Error processing {snippet_info.video_title}: {error_msg}")
        
        spark.sql(f"""
            INSERT INTO `yt-deslopify`.default.channel_transcriptions
            VALUES (
                '{snippet_info.channel_id}',
                '{snippet_info.channel_name}',
                '{snippet_info.video_id}',
                '{snippet_info.video_title}',
                '{snippet_info.audio_snippet_blob}',
                NULL,
                NULL,
                NULL,
                NULL,
                current_timestamp(),
                'error',
                '{error_msg}'
            )
        """)

# COMMAND ----------
# Initialize Azure blob client
blob_service_client = get_blob_service_client()  # Reuse function from audio processing
container_client = blob_service_client.get_container_client("yt-deslopify")

# Initialize Whisper pipeline with optimized settings
pipe = initialize_whisper(
    model_id="openai/whisper-large-v3-turbo",
    chunk_length_s=60,
    batch_size=16,
    use_flash_attn=True,
    use_torch_compile=True
)

# Get pending snippets
pending_df = get_pending_snippets()
pending_snippets = pending_df.collect()
logger.info(f"Found {len(pending_snippets)} snippets to process")

# Process snippets
for snippet in pending_snippets:
    process_audio_snippet(pipe, snippet, container_client)

# COMMAND ----------
# Get processing summary
spark.sql("""
    SELECT 
        status,
        COUNT(*) as count,
        AVG(processing_time_sec) as avg_processing_time,
        COUNT(DISTINCT detected_language) as unique_languages
    FROM `yt-deslopify`.default.channel_transcriptions
    GROUP BY status
""").show()

# Show any errors
spark.sql("""
    SELECT 
        video_title,
        error_message
    FROM `yt-deslopify`.default.channel_transcriptions
    WHERE status = 'error'
""").show(truncate=False)

# Show language distribution
spark.sql("""
    SELECT 
        detected_language,
        COUNT(*) as count,
        AVG(processing_time_sec) as avg_processing_time
    FROM `yt-deslopify`.default.channel_transcriptions
    WHERE status = 'success'
    GROUP BY detected_language
    ORDER BY count DESC
""").show()