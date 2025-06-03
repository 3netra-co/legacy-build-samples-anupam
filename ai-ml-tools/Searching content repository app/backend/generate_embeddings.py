import pandas as pd
import requests
import json
import pickle

OPENAI_KEY = "sk-" #Your openAI Key

def create_embedding(texts):
    headers = {
        'Authorization': f'Bearer ' + OPENAI_KEY,
        'Content-Type': 'application/json',
    }
    data = {
        'input': texts,
        'model': "text-embedding-ada-002",
        'encoding_format': "float"
    }
    response = requests.post('https://api.openai.com/v1/embeddings', headers=headers, data=json.dumps(data))
    
    if response.status_code == 200:
        return [item['embedding'] for item in response.json()['data']]
    else:
        print("API Error:", response.status_code, response.text)  # Add this
        return []

def process_and_save_embeddings_in_batches(texts, batch_size=10):
    all_embeddings = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        embeddings = create_embedding(batch)
        if not embeddings:
            print(f"Failed at batch {i}")
            return
        all_embeddings.extend(embeddings)
        print(f"Batch {i // batch_size + 1} processed")
    return all_embeddings

# Load your chapter data
df = pd.read_csv('your_content_file.csv')
texts = df['Text'].tolist()

# Generate embeddings
embeddings = process_and_save_embeddings_in_batches(texts)

# Assign to DataFrame
df['Embeddings'] = embeddings

# Save embeddings only (as a list)
with open('embeddings.pkl', 'wb') as f:
    pickle.dump(embeddings, f)

print("embeddings.pkl created.")
