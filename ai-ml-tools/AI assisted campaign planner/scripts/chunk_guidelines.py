from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import textwrap
import pandas as pd

# Auth setup
SERVICE_ACCOUNT_FILE = "PATH TO YOUR SERVICE ACCOUNT JSON/credentials.json"
SCOPES = ["https://www.googleapis.com/auth/documents.readonly"]
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Google Docs API
service = build("docs", "v1", credentials=creds)

# Replace with your actual Google Doc ID (from the URL)
# It is assumed that the corporate communication guidelines are in a Google Doc
DOCUMENT_ID = "XXX" #YOUR GOOGLE DOC ID

# Get the doc content
doc = service.documents().get(documentId=DOCUMENT_ID).execute()
elements = doc.get("body").get("content")

# Extract plain text
text = ""
for value in elements:
    paragraph = value.get("paragraph")
    if paragraph:
        for elem in paragraph.get("elements"):
            if "textRun" in elem:
                text += elem["textRun"]["content"]

# Chunking
MAX_CHAR_LENGTH = 2000
OVERLAP = 100
chunks = textwrap.wrap(text, width=MAX_CHAR_LENGTH - OVERLAP)

# Build dataframe
chunk_rows = [{
    "source_type": f"Guidelines",
    "source_id": f"guideline_chunk{i+1:02}",
    "source_text": chunk,
    "text_len": len(chunk)
} for i, chunk in enumerate(chunks)]

df_chunks = pd.DataFrame(chunk_rows)

# Save it
output_path = "OUTPUT PATH/processed_guidelines_chunks.csv"
df_chunks.to_csv(output_path, index=False)

print(f"Saved to {output_path}")
