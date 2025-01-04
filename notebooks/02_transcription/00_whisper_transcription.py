# =========================================
# WHISPER_LONG_OPTIMIZED_UC.PY
# =========================================

# Databricks notebook source

import torch
import logging
import os
import csv
import time
import warnings
import librosa
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor, pipeline
from pyspark.sql import SparkSession

# Optional imports for flash-attn or torch.compile (handled gracefully if not present):
try:
    from torch.nn.attention import SDPBackend, sdpa_kernel
    FLASH_SUPPORT = True
except ImportError:
    FLASH_SUPPORT = False

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message=".*past_key_values.*")

class MeetingTranscriber:
    """
    A class to handle very long-form speech transcription using a Whisper-based model.
    Optimised for GPU usage, chunked inference, large batch size, optional flash-attn, and torch.compile,
    and compatible with Unity Catalog volumes on Databricks Runtime 13.3+.
    """

    def __init__(
        self,
        model_id: str = "openai/whisper-large-v3-turbo",
        chunk_length_s: int = 60,
        batch_size: int = 16,
        use_flash_attn: bool = False,
        use_torch_compile: bool = False
    ):
        """
        Args:
            model_id (str): Hugging Face model ID or local path.
            chunk_length_s (int): Length of each audio chunk, in seconds (30-60 recommended).
            batch_size (int): Number of chunks to process in parallel (tune for GPU memory).
            use_flash_attn (bool): Whether to try flash-attn2. Requires 'flash-attn' installed.
            use_torch_compile (bool): Whether to compile the model with torch.compile for speed-ups (PyTorch 2+).
        """
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        self.chunk_length_s = chunk_length_s
        self.batch_size = batch_size
        self.use_flash_attn = use_flash_attn
        self.use_torch_compile = use_torch_compile

        # Check for GPU
        if torch.cuda.is_available():
            self.device = "cuda"
            self.logger.info("Using CUDA device for inference.")
            self.torch_dtype = torch.float16
        else:
            self.device = "cpu"
            self.logger.info("Using CPU device for inference.")
            self.torch_dtype = torch.float32

        # Set matmul precision to 'high'
        torch.set_float32_matmul_precision("high")

        self.logger.info("Loading model and processor...")

        # Decide on attention implementation
        attn_impl = None
        if self.use_flash_attn:
            attn_impl = "flash_attention_2"  # flash-attn2
            self.logger.info("Attempting to use Flash Attention 2.")
        else:
            attn_impl = "sdpa"  # scaled dot-product attention

        # Try loading model with attn_implementation
        try:
            self.logger.info(f"Loading model with attn_implementation={attn_impl}.")
            self.model = AutoModelForSpeechSeq2Seq.from_pretrained(
                model_id,
                torch_dtype=self.torch_dtype,
                low_cpu_mem_usage=True,
                use_safetensors=True,
                attn_implementation=attn_impl
            ).to(self.device)
        except TypeError as e:
            self.logger.warning(f"Could not set attn_implementation={attn_impl}. Fallback. Error: {e}")
            self.model = AutoModelForSpeechSeq2Seq.from_pretrained(
                model_id,
                torch_dtype=self.torch_dtype,
                low_cpu_mem_usage=True,
                use_safetensors=True
            ).to(self.device)

        # Optionally compile
        if self.use_torch_compile and torch.__version__ >= "2.0":
            self.logger.info("Compiling model with torch.compile(...) for potential speed-ups.")
            self.model.forward = torch.compile(
                self.model.forward, mode="reduce-overhead", fullgraph=True
            )
        else:
            self.logger.info("Skipping torch.compile (either disabled or PyTorch < 2.0).")

        # Load processor
        self.processor = AutoProcessor.from_pretrained(model_id)

        # Create pipeline for chunked long-form audio
        self.pipe = pipeline(
            "automatic-speech-recognition",
            model=self.model,
            tokenizer=self.processor.tokenizer,
            feature_extractor=self.processor.feature_extractor,
            torch_dtype=self.torch_dtype,
            device=0 if self.device == "cuda" else -1,
            chunk_length_s=self.chunk_length_s,
            batch_size=self.batch_size,
            return_timestamps=True
        )

        self.logger.info(
            f"Model initialized (chunk_length_s={self.chunk_length_s}, batch_size={self.batch_size})."
        )

    def _load_audio(self, audio_path: str) -> np.ndarray:
        """
        Load and preprocess audio file to 16kHz sample rate using Spark to read from Unity Catalog.
        """
        self.logger.info(f"Original audio path: {audio_path}")
        
        # Create Spark session if not exists
        spark = SparkSession.builder.getOrCreate()
        
        # Handle Unity Catalog volume paths
        if '/Volumes/' in audio_path:
            # Remove any dbfs: prefix and ensure path starts with /
            spark_path = audio_path.replace('dbfs:', '').strip('/')
            spark_path = f"/{spark_path}"
            
            self.logger.info(f"Using Unity Catalog path: {spark_path}")
            
            try:
                self.logger.info("Reading using Spark binary file reader...")
                df = spark.read.format("binaryFile").load(spark_path)
                audio_binary = df.first().content
                
                # Save to temporary file with correct extension
                temp_path = "/tmp/temp_audio.m4a"
                with open(temp_path, "wb") as f:
                    f.write(audio_binary)
                
                # Load with librosa
                self.logger.info(f"Loading audio from temporary file: {temp_path}")
                audio_array, sr = librosa.load(temp_path, sr=16000)
                
                # Clean up
                os.remove(temp_path)
                
                return audio_array
                
            except Exception as e:
                raise RuntimeError(f"Failed to load audio from Unity Catalog: {e}")
        else:
            # For regular DBFS paths
            if not audio_path.startswith('dbfs:'):
                spark_path = f"dbfs:{audio_path}" if audio_path.startswith('/') else f"dbfs:/{audio_path}"
            else:
                spark_path = audio_path
                
            self.logger.info(f"Using DBFS path: {spark_path}")
            
            # Convert dbfs: path to /dbfs path for local file access
            local_path = spark_path.replace('dbfs:', '/dbfs')
            self.logger.info(f"Converting to local path: {local_path}")
            
            try:
                # Load with librosa directly from the /dbfs path
                self.logger.info(f"Loading audio from local path: {local_path}")
                audio_array, sr = librosa.load(local_path, sr=16000)
                return audio_array
                
            except Exception as e:
                self.logger.error(f"Error loading audio file: {e}")
                # Fallback to using Spark if direct access fails
                try:
                    self.logger.info("Falling back to Spark binary file reading...")
                    df = spark.read.format("binaryFile").load(spark_path)
                    audio_binary = df.first().content
                    
                    # Save to temporary file
                    temp_path = "/tmp/temp_audio.m4a"
                    with open(temp_path, "wb") as f:
                        f.write(audio_binary)
                    
                    # Load with librosa
                    self.logger.info(f"Loading audio from temporary file: {temp_path}")
                    audio_array, sr = librosa.load(temp_path, sr=16000)
                    
                    # Clean up
                    os.remove(temp_path)
                    
                    return audio_array
                    
                except Exception as e2:
                    raise RuntimeError(f"Failed to load audio file using both methods. Errors: {e}, {e2}")

    def transcribe(
        self,
        audio_path: str,
        output_path: Optional[str] = None,
        language: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Transcribe a long-form audio file with optional forced language.

        Args:
            audio_path: Path to the audio file in Unity Catalog volume or DBFS.
            output_path: Optional path to save the transcription (.txt).
            language: Optional language code (e.g. "en", "nl").

        Returns:
            dict: The transcription result, including "transcription_time" in seconds.
        """
        self.logger.info(f"Starting transcription of {audio_path}.")

        # Handle Unity Catalog paths
        if '/Volumes/' in audio_path:
            # Remove any dbfs: prefix and ensure path starts with /
            spark_path = audio_path.replace('dbfs:', '').strip('/')
            spark_path = f"/{spark_path}"
        else:
            # For regular DBFS paths
            if not audio_path.startswith('dbfs:'):
                spark_path = f"dbfs:{audio_path}" if audio_path.startswith('/') else f"dbfs:/{audio_path}"
            else:
                spark_path = audio_path

        # Check file existence using Spark
        spark = SparkSession.builder.getOrCreate()
        try:
            df = spark.read.format("binaryFile").load(spark_path)
            df.first()  # This will raise an error if the file doesn't exist
        except Exception as e:
            raise FileNotFoundError(f"Audio file not found or not accessible: {spark_path}. Error: {e}")

        # Possibly convert language name to code
        lang_code = None
        if language:
            lc = language.lower()
            if lc == "dutch":
                lang_code = "nl"
            elif lc == "english":
                lang_code = "en"
            else:
                lang_code = language

        # forced_decoder_ids if language is given
        forced_decoder_ids = None
        if lang_code:
            forced_decoder_ids = self.processor.get_decoder_prompt_ids(
                language=lang_code,
                task="transcribe",
                no_timestamps=False
            )

        # Generation config
        generate_kwargs = {
            "task": "transcribe",  # do not translate
            "language": None,  # Set to None for auto-detection
            "max_new_tokens": 440,
            "num_beams": 1,
            "condition_on_prev_tokens": False,
            "compression_ratio_threshold": 1.35,
            "temperature": (0.0, 0.2, 0.4, 0.6, 0.8, 1.0),
            "logprob_threshold": -1.0,
            "no_speech_threshold": 0.6,
            "return_timestamps": True,
        }
        if forced_decoder_ids:
            generate_kwargs["forced_decoder_ids"] = forced_decoder_ids

        # Load the audio
        audio_array = self._load_audio(audio_path)

        self.logger.info(
            f"Processing audio in {self.chunk_length_s}-sec chunks (batch_size={self.batch_size})."
        )

        start_time = time.time()

        # For flash-attn2 or sdpa context (optional):
        if FLASH_SUPPORT:
            with torch.inference_mode(), sdpa_kernel(SDPBackend.MATH):
                result = self.pipe(audio_array, generate_kwargs=generate_kwargs)
        else:
            with torch.inference_mode():
                result = self.pipe(audio_array, generate_kwargs=generate_kwargs)

        # Extract detected language if available
        detected_language = result.get("language", "auto")
        self.logger.info(f"Detected language: {detected_language}")

        end_time = time.time()
        result["transcription_time"] = end_time - start_time
        result["detected_language"] = detected_language
        self.logger.info(f"Finished transcription in {result['transcription_time']:.2f}s.")

        # Save transcript
        if output_path:
            output_path = output_path if output_path.endswith(".txt") else f"{output_path}.txt"
            self._save_transcription(result, output_path)
            self.logger.info(f"Transcription saved to {output_path}")

        return result

    def _save_transcription(self, result: Dict[str, Any], output_path: str):
        """
        Save the transcription, including timestamps if available.
        Uses dbutils.fs.put for Unity Catalog compatibility.
        """
        self.logger.info(f"Writing transcription to {output_path}")
        
        # Prepare the content as a string
        content = []
        if isinstance(result, dict) and "chunks" in result:
            for chunk in result["chunks"]:
                start, end = chunk["timestamp"]
                timestamp = f"[{start:.2f} - {end:.2f}] "
                content.append(f"{timestamp}{chunk['text']}")
        else:
            content.append(result.get("text", ""))
            
        # Join all lines with newlines
        content_str = "\n".join(content)
        
        try:
            # Use dbutils.fs.put to write the file
            dbutils.fs.put(output_path, content_str, overwrite=True)
        except Exception as e:
            self.logger.error(f"Failed to save transcription to {output_path}: {e}")
            raise

    def process_directory(
        self,
        input_dir: str,
        processed_dir: str,
        logs_path: str,
        language: Optional[str] = None
    ) -> None:
        """
        Transcribe all audio files in input_dir, then move them to processed_dir,
        and log stats to a CSV in logs_path. Uses Unity Catalog volume paths.
        """
        self.logger.info(f"Starting processing of directory: {input_dir}")
        
        # For Unity Catalog volumes, ensure directories exist using dbutils
        for path in [processed_dir, os.path.dirname(logs_path)]:
            try:
                dbutils.fs.ls(path)
            except Exception:
                dbutils.fs.mkdirs(path)
                self.logger.info(f"Created directory: {path}")

        # Check if log file exists and read existing content if it does
        file_exists = True
        try:
            dbutils.fs.ls(logs_path)
        except Exception:
            file_exists = False

        # Prepare CSV content
        csv_content = []
        if not file_exists:
            csv_content.append([
                "timestamp",
                "filename",
                "audio_duration_sec",
                "transcription_time_sec",
                "chunk_count",
                "language",
                "status",
                "message",
                "output_transcript"
            ])

        # Iter  ate over audio files using dbutils
        try:
            self.logger.info(f"Listing files in directory: {input_dir}")
            files = dbutils.fs.ls(input_dir)
            self.logger.info(f"Found {len(files)} items in {input_dir}")
            for f in files:
                self.logger.info(f"Found item: {f.path} (name: {f.name})")
        except Exception as e:
            self.logger.error(f"Error listing directory: {e}")
            raise RuntimeError(f"Failed to list files in input directory: {e}")

        audio_files_found = 0
        for file_info in files:
            file_path = file_info.path
            file_name = file_info.name
            self.logger.info(f"Checking file: {file_path} (name: {file_name})")
            
            # Check for audio files specifically
            is_audio = file_name.lower().endswith(('.m4a', '.mp3', '.wav', '.aac'))
            self.logger.info(f"Is audio file? {is_audio} (path: {file_path})")
            
            # Check if it's a directory by looking at the size attribute
            is_directory = getattr(file_info, 'size', 0) == 0
            
            if not is_directory and is_audio:
                audio_files_found += 1
                try:
                    self.logger.info(f"Processing audio file: {file_path}")
                    
                    # Use the dbfs: path directly for audio processing
                    if not file_path.startswith('dbfs:'):
                        file_path = f"dbfs:{file_path}" if file_path.startswith('/') else f"dbfs:/{file_path}"
                    
                    self.logger.info(f"Loading audio file: {file_path}")
                    audio_array = self._load_audio(file_path)
                    audio_duration = librosa.get_duration(y=audio_array, sr=16000)
                    self.logger.info(f"Audio duration: {audio_duration} seconds")

                    # Output .txt name
                    output_transcript_name = f"{Path(file_name).stem}_transcript.txt"
                    output_transcript_path = f"{processed_dir}/{output_transcript_name}"
                    
                    # Ensure the path is in the correct format for Unity Catalog
                    if output_transcript_path.startswith('dbfs:'):
                        output_transcript_path = output_transcript_path.replace('dbfs:', '')
                    if not output_transcript_path.startswith('/'):
                        output_transcript_path = f"/{output_transcript_path}"
                    
                    self.logger.info(f"Output path: {output_transcript_path}")

                    # Transcribe
                    self.logger.info("Starting transcription...")
                    transcription_result = self.transcribe(
                        audio_path=file_path,
                        output_path=output_transcript_path,
                        language=language
                    )
                    self.logger.info("Transcription completed")

                    # Move file to processed directory using dbutils
                    destination_path = f"{processed_dir}/{file_name}"
                    self.logger.info(f"Moving file to: {destination_path}")
                    dbutils.fs.mv(file_path, destination_path)

                    # Summarize transcription stats
                    chunk_count = 0
                    if isinstance(transcription_result, dict) and "chunks" in transcription_result:
                        chunk_count = len(transcription_result["chunks"])
                    transcription_time = transcription_result.get("transcription_time", 0.0)

                    # Add row to CSV content
                    csv_content.append([
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        file_name,
                        f"{audio_duration:.2f}",
                        f"{transcription_time:.2f}",
                        chunk_count,
                        transcription_result.get("detected_language", "auto"),
                        "SUCCESS",
                        "",
                        output_transcript_name
                    ])
                    self.logger.info(f"File {file_name} processed successfully.")

                except Exception as e:
                    self.logger.error(f"Error processing file {file_path}: {str(e)}")
                    # Add error row to CSV content
                    csv_content.append([
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        file_name,
                        "",
                        "",
                        "",
                        "auto",
                        "FAILURE",
                        str(e),
                        ""
                    ])
                    self.logger.error(f"Failed to process {file_path}: {e}")
            else:
                self.logger.info(f"Skipping non-audio file or directory: {file_path}")

        self.logger.info(f"Found {audio_files_found} audio files in total")
        
        # Write CSV content using dbutils
        if csv_content:
            self.logger.info("Writing CSV content...")
            import io
            import csv as csv_lib
            output = io.StringIO()
            writer = csv_lib.writer(output)
            writer.writerows(csv_content)
            
            if file_exists:
                # If file exists, append to it
                existing_content = dbutils.fs.head(logs_path)
                output_content = existing_content + output.getvalue()
            else:
                output_content = output.getvalue()
            
            # Write the content to the file
            dbutils.fs.put(logs_path, output_content, overwrite=True)
            self.logger.info("Updated log file successfully")
        else:
            self.logger.warning("No content to write to CSV file")


def test_file_listing():
    """Test function to check file listing"""
    input_dir = "/Volumes/cooper/modalities/research/media_input"
    print("\nTesting file listing:")
    print("-" * 50)
    try:
        files = dbutils.fs.ls(input_dir)
        print(f"Number of files found: {len(files)}")
        print("\nFile details:")
        for f in files:
            print(f"Name: {f.name}")
            print(f"Path: {f.path}")
            print(f"Size: {f.size}")
            print(f"ModificationTime: {f.modificationTime}")
            print(f"IsDir: {f.isDir}")
            print("-" * 30)
    except Exception as e:
        print(f"Error listing files: {e}")

def main():
    """
    Example usage for transcribing audio in Azure Databricks using Unity Catalog volumes.
    Make sure you have:
      - The appropriate access permissions to the Unity Catalog volume
      - WRITE VOLUME privilege for the volume
      - Enough GPU memory to handle chunk_length_s & batch_size together
    """
    
    # Unity Catalog volume paths (format: /Volumes/<catalog>/<schema>/<volume_name>/path)
    CATALOG = "cooper"        # catalog name from the path
    SCHEMA = "modalities"     # schema name from the path
    VOLUME = "research"       # volume name from the path
    
    # Construct the full paths using forward slashes
    base_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"  # Changed from dots to slashes
    input_dir = f"{base_path}/media_input"
    processed_dir = f"{base_path}/processed_audio"
    log_file = f"{base_path}/transcription_log.csv"

    # Validate paths exist and are accessible using dbutils
    for path in [input_dir, processed_dir]:
        try:
            dbutils.fs.ls(path)
            print(f"Directory exists: {path}")
        except Exception:
            try:
                dbutils.fs.mkdirs(path)
                print(f"Created directory: {path}")
            except Exception as e:
                raise RuntimeError(f"Failed to create or access directory {path}. Error: {e}")

    transcriber = MeetingTranscriber(
        model_id="openai/whisper-large-v3-turbo",
        chunk_length_s=60,   # chunk size in seconds
        batch_size=16,       # increase if memory allows, reduce if OOM
        use_flash_attn=False, # set to True if flash-attn is installed
        use_torch_compile=False # set to True if you want torch.compile
    )

    transcriber.process_directory(
        input_dir=input_dir,
        processed_dir=processed_dir,
        logs_path=log_file,
        language=None  # Set to None for auto-detection
    )

    print("All files processed. Check the CSV log for details.")

if __name__ == "__main__":
    test_file_listing()  # Run the test first
    main()  # Then run the main program