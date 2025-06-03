import os
import pandas as pd
from openai import OpenAI

client = OpenAI(api_key="sk") #Your Open AI Key

color = 'green'

# --- CONFIG ---
csv_path = f"PATH TO YOUR SPLIT TIMES CSV"
output_csv_path = f"FILENAME AND PATH TO OUR OUTPUT"
transcript_folder = f"FOLDER WHERE YOU SAVED THE SPILT VIDEOS AND TRANSCRIPTS"

# --- PROMPT TEMPLATE ---
base_prompt = (
    "You are helping write YouTube titles and descriptions that are solutions-based and emphasize viewer agency.\n"
    "Please suggest 3 potential video titles for the following transcript.\n"
    "Please suggest 2 potential video descriptions for the following transcript.\n"
    "Each title should be under 65 characters long.\n\nTranscript.\n"
    "Keep the description specific to the transcript and about 200 words or less.\n"
)

# --- LOAD CSV ---
# --- LOAD CSV ---
df = pd.read_csv(csv_path, encoding='utf-8-sig')
df["transcript_text"] = ""           # <-- Add this line
df["title_option_1"] = ""
df["title_option_2"] = ""
df["title_option_3"] = ""
df["description_option_1"] = ""
df["description_option_2"] = ""

# --- LOOP THROUGH CLIPS ---
for idx, row in df.iterrows():
    name = row["name"]
    txt_path = os.path.join(transcript_folder, f"{name}.txt")

    if not os.path.exists(txt_path):
        print(f"[!] Transcript file not found for {name}")
        continue

    with open(txt_path, "r", encoding="utf-8") as file:
        transcript = file.read().strip()

    full_prompt = base_prompt + transcript

    try:
        txt_path = os.path.join(transcript_folder, f"{name}.txt")
        with open(txt_path, "r", encoding="utf-8") as file:
            transcript = file.read().strip()

        # Store transcript for QC
        df.at[idx, "transcript_text"] = transcript

        # Build prompt and call API
        full_prompt = base_prompt + transcript
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": full_prompt}],
            temperature=0.7,
            max_tokens=150
        )

        # Parse response
        content = response.choices[0].message.content.strip()
        suggestions = content.split("\n")
        titles = [line.strip("1234567890.- ").strip() for line in suggestions if line.strip()]

        df.at[idx, "title_option_1"] = titles[0] if len(titles) > 0 else ""
        df.at[idx, "title_option_2"] = titles[1] if len(titles) > 1 else ""
        df.at[idx, "title_option_3"] = titles[2] if len(titles) > 2 else ""

    except Exception as e:
        print(f"[!] Error for {name}: {e}")

# --- SAVE OUTPUT ---
df.to_csv(output_csv_path, index=False)
print(f"Done. Output saved to {output_csv_path}")
