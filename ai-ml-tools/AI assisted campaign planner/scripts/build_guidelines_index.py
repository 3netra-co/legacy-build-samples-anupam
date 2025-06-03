from sentence_transformers import SentenceTransformer
import pandas as pd
import faiss
import numpy as np
import json

# Load your preprocessed data
df = pd.read_csv("../data/processed_guidelines_chunks.csv")

# Use the BGE embedding model
model = SentenceTransformer('BAAI/bge-base-en-v1.5')
embedding_dim = 768  # BGE base uses 768 dimensions

# Initialize FAISS index
index = faiss.IndexFlatL2(embedding_dim)
metadata = []

for i, row in df.iterrows():
    text = row["source_text"]
    print(f"Embedding chunk {i + 1}/{len(df)}")
    vector = model.encode(text, convert_to_numpy=True)
    index.add(np.array([vector], dtype="float32"))
    metadata.append({
        "source_id": row["source_id"],
        "source_type": row["source_type"],
        "text": text
    })

# Save FAISS index and metadata
faiss.write_index(index, "../data/guidelines_index.faiss")
with open("../data/guidelines_metadata.json", "w") as f:
    json.dump(metadata, f)

# Save vector matrix
np.save("../data/guidelines_vectors.npy", index.reconstruct_n(0, index.ntotal))