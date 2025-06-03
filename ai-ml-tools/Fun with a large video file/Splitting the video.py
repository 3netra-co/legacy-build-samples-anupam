import csv
import subprocess
import os

def cleanup_intermediate_files(base_filename, output_dir, remove_video=True):
    extensions_to_remove = ['.tsv', '.vtt', '.json', '.srt']
    
    for ext in extensions_to_remove:
        file_path = os.path.join(output_dir, f"{base_filename}{ext}")
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Removed {file_path}")

    # Remove original .mp4 if it does not contain 'subtitled'
    if remove_video:
        video_path = os.path.join(output_dir, f"{base_filename}.mp4")
        if "subtitled" not in base_filename and os.path.exists(video_path):
            os.remove(video_path)
            print(f"Removed original video: {video_path}")

# Input & Output Paths
INPUT_VIDEO = "PATH TO LARGE VIDEO.MP4"
OUTPUT_DIR = "OUTPUT DIRECTORY"
#This CSV file will require 3 columns - start_time, end_time & filename
CLIPS_CSV = "PATH TO YOUR CSV TO DEFINE START AND END TIMES FOR SPLITS"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Read CSV and process each clip
with open(CLIPS_CSV, newline='', encoding='utf-8-sig') as csvfile:
    reader = csv.DictReader(csvfile)
    print("Column names:", reader.fieldnames)
    for row in reader: 
        start = row["start"]
        end = row["end"]
        name = row["name"]

        base_path = os.path.join(OUTPUT_DIR, name)
        clip_path = base_path + ".mp4"
        srt_path = base_path + ".srt"
        subtitled_path = base_path + "_subtitled.mp4"

        # Step 1: Extract clip using ffmpeg (no re-encoding)
        subprocess.run([
            "ffmpeg", "-y", "-ss", start, "-to", end,
            "-i", INPUT_VIDEO, "-c", "copy", clip_path
        ])

        # Step 2: Generate subtitle using whisper
        subprocess.run([
            "whisper", clip_path,
            "--model", "medium",
            "--output_format", "all",
            "--output_dir", OUTPUT_DIR
        ])

        # Step 3: Burn subtitle into the clip
        subprocess.run([
            "ffmpeg", "-y", "-i", clip_path,
            "-vf", f"subtitles={srt_path}",
            "-c:a", "copy", subtitled_path
        ])

        # Extract base filename from clip_path
        base_filename = os.path.splitext(os.path.basename(clip_path))[0]

        # Clean up unwanted files
        cleanup_intermediate_files(base_filename, OUTPUT_DIR)

print("All clips have been subtitled and saved to your Desktop.")
