import pandas as pd
import textwrap
import gspread
from google.oauth2.service_account import Credentials

# Auth
SERVICE_ACCOUNT_FILE = "PATH TO YOUR SERVICE ACCOUNT JSON/credentials.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)

# Open the sheet and worksheet from Google Drive where
# You would have saved all your prior marketing or communication content
# File should be contain following columns - source_type, source_id, source_text, text_len

spreadsheet = client.open("All Content")
worksheet = spreadsheet.worksheet("Sheet1")  # Adjust name if needed

# Get data into a DataFrame
data = worksheet.get_all_records()
df = pd.DataFrame(data)

# Chunking setup
MAX_CHAR_LENGTH = 2000
OVERLAP = 100
processed_rows = []

for _, row in df.iterrows():
    text = row["source_text"]
    if len(text) <= MAX_CHAR_LENGTH:
        processed_rows.append(row.to_dict())
    else:
        chunks = textwrap.wrap(text, width=MAX_CHAR_LENGTH - OVERLAP)
        for i, chunk in enumerate(chunks):
            new_row = row.copy()
            new_row["source_id"] = f"{row['source_id']}_chunk{i+1:02}"
            new_row["source_text"] = chunk
            new_row["text_len"] = len(chunk)
            processed_rows.append(new_row.to_dict())

df_chunks = pd.DataFrame(processed_rows)

# Save the chunked output
output_path = "PATH TO YOUR OUTPUT FOLDER/processed_content_chunks.csv"
df_chunks.to_csv(output_path, index=False)

print(f"Saved to {output_path}")
