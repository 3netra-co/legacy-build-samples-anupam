import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

# Load models and data
model = SentenceTransformer('BAAI/bge-base-en-v1.5')
chapters_df = pd.read_csv("YOUR MAIN CSV TO WHICH YOU WANT TO MATCH")
questions_df = pd.read_csv("YOUR KEY OR QUESTIONS THAT YOU NEED TO MATCH TO")

# Combine relevant fields into one text column
def combine_chapter_text(row):
    fields = ['source_id', 'source_text'] #Your columns
    return ' '.join(str(row[field]) for field in fields if pd.notnull(row[field]))

chapters_df["full_text"] = chapters_df.apply(combine_chapter_text, axis=1)

# Compute chapter embeddings
chapter_embeddings = model.encode(chapters_df["full_text"].tolist(), normalize_embeddings=True)

# Compute question embeddings
question_embeddings = model.encode(questions_df["question"].tolist(), normalize_embeddings=True)

# Match each question to the best chapter
best_titles = []
best_text = []
best_scores = []

for q_embed in question_embeddings:
    sims = cosine_similarity([q_embed], chapter_embeddings)[0]
    best_idx = sims.argmax()
    best_titles.append(chapters_df.loc[best_idx, "source_id"])
    best_text.append(chapters_df.loc[best_idx, "source_text"])
    best_scores.append(sims[best_idx])

# Append results to questions DataFrame
questions_df["best_match_title"] = best_titles
questions_df["best_match_text"] = best_text
questions_df["matching_score"] = best_scores

# Save the results
questions_df.to_csv("OUTPUT PATH TO CSV", index=False)
