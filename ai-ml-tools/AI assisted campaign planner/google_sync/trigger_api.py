from google.oauth2 import service_account
from googleapiclient.discovery import build
from openai import OpenAI
import os
import json
import numpy as np
import pandas as pd
import faiss
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer
import re
import json
from datetime import datetime


# Set up OpenAI client
# Set your OpenAI API Key
client = OpenAI(api_key='YOUR API KEY')

embedding_model = SentenceTransformer("BAAI/bge-base-en-v1.5")

CHAT_MODEL = "gpt-4o"

# Resolve data folder path relative to this file
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")

def log_run(brief, content_text, guidelines_text, campaign_output, token_usage, log_dir="/home/ec2-user/logs"):
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(log_dir, f"campaign_log_{timestamp}.csv")

    data = {
        "timestamp": [timestamp],
        "brief": [brief],
        "content_text": [content_text],
        "guideline_text": [guidelines_text],
        "openai_output_json": [campaign_output],
        "token_usage": [token_usage]
    }

    df = pd.DataFrame(data)
    df.to_csv(log_file, index=False)
    print(f"Log saved to: {log_file}")

def load_vector_index(name):
    index = faiss.read_index(os.path.join(DATA_DIR, f"{name}_index.faiss"))
    with open(os.path.join(DATA_DIR, f"{name}_metadata.json"), "r") as f:
        metadata = json.load(f)
    df = pd.DataFrame(metadata)
    return index, df

def get_top_chunks(query, model, index, metadata_df, top_n=2):
    query_vec = model.encode(query, convert_to_numpy=True).reshape(1, -1)
    similarities = cosine_similarity(query_vec, index.reconstruct_n(0, index.ntotal))[0]
    top_indices = similarities.argsort()[-top_n:][::-1]
    return metadata_df.iloc[top_indices]["text"].tolist()


def get_prompt_from_sheet(spreadsheet_id, range_name, credentials_path):
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=scopes)
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get("values", [])
    return values[0][0] if values else None

GOOGLE_CREDS_PATH = os.path.join(BASE_DIR, "google_sync", "config", "credentials.json") #Add your Service Account Json Here
SHEET_ID = "YOUR SHEET ID" #Sheet id from Google Drive
PROMPT_CELL_RANGE = "Prompt Sheet!A1" #Tab in the sheet where you have stored your prompt for that ask. All in one Cell
GENERAL_INSTRUCTIONS = "Instructions!A1" #Tab in the sheet where you have stored system prompts. All in once cell
MEDIA_INSTRUCTIONS = "Media Types!A1" #Tab in the sheet you have saved your platform and media type. I.E. Facebook-Reels, Videos. Again all in one cell

#brief = get_prompt_from_sheet(SHEET_ID, PROMPT_CELL_RANGE, GOOGLE_CREDS_PATH)

def update_output_sheet(spreadsheet_id, sheet_name, data, credentials_path):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=scopes)
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    # Clear the sheet
    sheet.values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1:Z1000"  # Clear a safe range
    ).execute()

    # Prepare data to write (including headers)
    values = [data.columns.tolist()] + data.values.tolist()

    # Write data
    sheet.values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

def clean_openai_json(raw_str):
    """
    Cleans the OpenAI response to extract valid JSON.
    Strips Markdown formatting, triple quotes, etc.
    """
    raw_str = raw_str.strip()

    if raw_str.startswith("```"):
        raw_str = re.sub(r"^```[\w]*\n?", "", raw_str)
        raw_str = re.sub(r"\n?```$", "", raw_str)
    elif raw_str.startswith("'''"):
        raw_str = raw_str.strip("'''")

    # Ensure array structure if needed
    if raw_str.startswith("{") and not raw_str.startswith("["):
        raw_str = f"[{raw_str}]"

    return raw_str

def run_full_process():
    brief = get_prompt_from_sheet(SHEET_ID, PROMPT_CELL_RANGE, GOOGLE_CREDS_PATH)
    system_instructions = get_prompt_from_sheet(SHEET_ID, GENERAL_INSTRUCTIONS, GOOGLE_CREDS_PATH)
    platform_instructions = get_prompt_from_sheet(SHEET_ID, MEDIA_INSTRUCTIONS, GOOGLE_CREDS_PATH)

    if not brief:
        return {"status": "Failed", "error": "Prompt not found in sheet."}

    brief = brief.strip()
    system_instructions = system_instructions.strip()
    platform_instructions = platform_instructions.strip()

    # Load FAISS indices
    content_index, content_df = load_vector_index("processed_content")
    guidelines_index, guidelines_df = load_vector_index("guidelines")

    # Get most relevant chunks
    top_content = get_top_chunks(brief, embedding_model, content_index, content_df, top_n=2)
    top_guidelines = get_top_chunks(brief, embedding_model, guidelines_index, guidelines_df, top_n=1)

    # Assemble content grounding text safely
    content_text = "\n\n".join(top_content[:3])  # Safe even if fewer than 3

    # Assemble guidelines grounding text safely
    guidelines_text = "\n\n".join(top_guidelines[:2])
    # Assemble prompts
    system_prompt = system_instructions
    guideline_summary = (
        "Use brand voice: gentle, thoughtful, conversational, reflective.\n"
        "Avoid being prescriptive, avoid jargon, use everyday language.\n"
    )

    user_prompt = (
        f"Brief:\n{brief}\n\n"
        f"Use these excerpts to create your content strategy:\n{content_text}\n\n"
        f"Use this for applicable Platform, Media Type and CTAs:\n{platform_instructions}\n\n"
        f"Use media and format that serve the content. Does not have to include everything.\n\n"
        f"Guidelines Summary:\n{guideline_summary}\n\n"
        f"Also consider these additional communication guidelines:\n{guidelines_text}"
    )

    try:
        response = client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )

        campaign_output = response.choices[0].message.content.strip()
        if not campaign_output:
            return {"status": "Failed", "error": "OpenAI returned an empty response."}
        
        token_usage = response.usage.total_tokens

        cleaned = clean_openai_json(campaign_output)
        parsed_dicts = json.loads(cleaned)

        df_campaign = pd.DataFrame(parsed_dicts)

        update_output_sheet(
            spreadsheet_id=SHEET_ID,
            sheet_name="Output Tab",
            data=df_campaign,
            credentials_path=GOOGLE_CREDS_PATH
        )
        log_run(brief, content_text, guidelines_text, campaign_output, token_usage)
        return {"status": "Success"}

    except Exception as e:
        print("Parsing or sheet update failed:", str(e))
        return {"status": "Failed", "error": str(e)}
