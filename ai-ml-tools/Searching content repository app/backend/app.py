from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import requests
import json
import pickle

app = Flask(__name__)
CORS(app)  # Allow requests from React frontend

OPENAI_KEY = "sk-" ###Your open AI Key

# Load data
df = pd.read_csv('extracted_chapters.csv')
with open('embeddings.pkl', 'rb') as f:
    embeddings = pickle.load(f)
df['Embeddings'] = embeddings

def create_embedding(texts):
    headers = {
        'Authorization': f'Bearer {OPENAI_KEY}',
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
    return []

@app.route('/query', methods=['POST'])
def query():
    q = request.json['question']
    embedding = create_embedding([q])
    if not embedding:
        return jsonify({'answer': 'Failed to create embedding.'})

    q_vec = np.array(embedding[0]).reshape(1, -1)
    df['sim'] = df['Embeddings'].apply(lambda x: cosine_similarity(np.array(x).reshape(1, -1), q_vec)[0][0])
    best = df.loc[df['sim'].idxmax()]
    return jsonify({'answer': best['Text']})

if __name__ == '__main__':
    app.run(debug=True)
