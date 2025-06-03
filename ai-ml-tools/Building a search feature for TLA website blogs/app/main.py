import pprint
import time
from typing import List

import pandas as pd
from annoy import AnnoyIndex
from flask import Flask, request, jsonify
from transformers import AutoTokenizer, AutoModel
import os
import file_utils
from project_path import PROJECT_PATH
from flask_cors import CORS
app = Flask(__name__)
CORS(app=app)
model_name = "sentence-transformers/all-MiniLM-L6-v2"
# Load model and tokenizer
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModel.from_pretrained(model_name)

dimension = model.config.hidden_size  # Dimension of embeddings from DistilBERT

# index = faiss.IndexFlatL2(dimension)  # L2 distance for similarity search
index = AnnoyIndex(dimension, 'dot')  # Using 'angular' distance
# i checked different distances:- dot,educlidan, anglur are best:-> dot should be first choice
ID_COLUMN = 'Serial Number'

index_name2_index = {}
file_name2_data = {}


# Example usage
# db_manager = document_db.DocumentManager('my_documents.db')


def texts_to_embeddings(texts):
    inputs = tokenizer(texts, return_tensors="pt", padding=True, truncation=True, max_length=128)
    outputs = model(**inputs)
    return outputs.last_hidden_state.mean(dim=1).detach().numpy()


def text_to_vector(text):
    """Converts input text to a vector using DistilBERT."""
    inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True, max_length=128)
    outputs = model(**inputs)
    # Use mean pooling to convert the output to a single vector per input text
    embeddings = outputs.last_hidden_state.mean(1).detach().numpy()
    return embeddings[0]


def delete_file(file_path):
    """
    Deletes a file from the specified file path.

    Args:
        file_path (str): The path to the file to be deleted.

    Returns:
        bool: True if the file was successfully deleted, False if the file does not exist.
    """
    try:
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"File {file_path} deleted successfully.")
            return True
        else:
            print(f"File {file_path} does not exist.")
            return False
    except Exception as e:
        print(f"Error deleting file {file_path}: {e}")
        return False


def save_index_based_on_descriptions(
        texts,
        user_id,
        # index_path='my_index.ann'
):
    index_path = PROJECT_PATH.joinpath(f"data/vector_dbs/{user_id}.nns")
    # combined_texts = [f"{title} {description if not pd.isna(description) else ''}"
    #                   for title, description in
    #                   zip(titles, descriptions)]

    # Convert texts to embeddings in a batch
    vectors = texts_to_embeddings(texts=texts)

    for i, vec in enumerate(vectors):
        index.add_item(i, vec)
    # Convert vectors to correct dtype and shape for FAISS

    # Add to FAISS index in a single batch
    # index.add(vectors_np)
    index.build(30)
    print(index_path)
    try:
        index.save(str(index_path))
    except OSError:
        delete_file(index_path)
        time.sleep(3)
        index.save(str(index_path))
    print("Index is saved", index_path)
    return index


# @app.route('/upload_bulk', methods=['POST'])
# def upload_texts():
#     data = request.json
#     titles = data['titles']
#     user_id = data['user_id']
#     descriptions = data['descriptions']
#
#     save_title_descriptions(titles)
#
#     # # Generate hashes for each text and create response data
#     response_data = []
#     # for i, text in enumerate(combined_texts):
#     #     hash_id = hashlib.sha256(text.encode()).hexdigest()
#     #     index_id = index.ntotal - len(combined_texts) + i  # Compute index id
#     #     response_data.append({"hash_id": hash_id, "index_id": index_id})
#     #
#
#     return jsonify(response_data)

def text_df_from_df(df: pd.DataFrame, text_columns: List[str], id_column: str):
    cols2_df = []
    for str_col in text_columns:
        df["col_name"] = str_col
        str_col2_indexes = df.groupby([str_col, 'col_name'])[id_column].apply(list).reset_index()
        str_col2_indexes = str_col2_indexes.rename(columns={str_col: 'text', 'index': 'document_ids'})
        cols2_df.append(str_col2_indexes)
    text_indexes_df = pd.concat(cols2_df)
    return text_indexes_df


@app.route('/status', methods=['GET'])
def get_status():
    return jsonify({"message": "Hello World!!"}), 200


@app.route('/upload', methods=['POST'])
def upload_file():
    user_id = request.form.get('user_id')

    if 'file' in request.files:
        file = request.files['file']
        if file:
            print("file is found")
            # Read the file as a DataFrame
            df = pd.read_csv(file)
            print("file read is done")
            text_columns = file_utils.get_text_columns(df)
            print("text columns reading is done")
            text_2_ids_df = text_df_from_df(df, text_columns=text_columns, id_column=ID_COLUMN)
            print("index 2 text columns is done")
            text_2_ids_df.to_parquet(PROJECT_PATH.joinpath(f"data/{user_id}.parquet"))
            print("File saving is done")

            save_index_based_on_descriptions(text_2_ids_df['text'].to_list(), user_id)
            print("Index saving is done")
            # You can now process the DataFrame as needed
            return jsonify({"message": "File received and processed", "data": df.to_json()}), 200
    return jsonify({"message": "No file part"}), 400


@app.route('/userdata/<string:user_id>', methods=['GET'])
def get_user_data(user_id):
    """Fetch and return all records for a given user_id."""
    try:
        print(f"user id {user_id}")

        if user_id in file_name2_data:
            return jsonify({'user_data': file_name2_data[user_id]})

        user_data = pd.read_parquet(PROJECT_PATH.joinpath(f"data/{user_id}.parquet"))

        file_name2_data[user_id] = user_data
        # #db_manager.fetch_documents_by_user_id(user_id)
        # print(user_data)
        if user_data:
            return jsonify({"user_data": user_data}), 200
        else:
            return jsonify({"message": "No data found for the specified user"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/find_similar', methods=['POST'])
def find_similar():
    global index
    data = request.json
    text = data['text']
    user_id = data['user_id']
    # print(text)
    vector = text_to_vector(text)  # Ensure vector is the right shape and dtype
    # print(vector)
    index_path = PROJECT_PATH.joinpath(f"data/vector_dbs/{user_id}.nns")

    if ((index_path not in index_name2_index) or
            (index_path in index_name2_index and not isinstance(index_name2_index[index_path], bool))
    ):

        index.load(str(index_path))
        index_name2_index[index_path] = index
    else:
        index = index_name2_index[index_path]

    indexes, distances = index.get_nns_by_vector(vector, 10, include_distances=True)  # Find 10 nearest neighbors

    if user_id not in file_name2_data:
        df = pd.read_parquet(PROJECT_PATH.joinpath(f"data/{user_id}.parquet"))
    else:
        df = file_name2_data[user_id]
    df_f = df.iloc[indexes, :]
    df_f: pd.DataFrame
    # df_f['document_ids'].apply(list)
    df_f['similarity'] = distances
    id_column_valuess = [str(i[0]) for i in df_f[ID_COLUMN]]
    unique_id_column_values_list = []
    _unique_id_column_values_set = set()
    for id_column in id_column_valuess:
        if id_column not in _unique_id_column_values_set:
            _unique_id_column_values_set.add(id_column)
            unique_id_column_values_list.append(id_column)
    # print(df_f.to_dict())
    result = {
        'data': df_f.to_json(orient='records'),
        ID_COLUMN: unique_id_column_values_list
    }
    # pprint.pprint(result)
    return jsonify(result)


if __name__ == '__main__':
    app.run(debug=0, host="0.0.0.0",ssl_context=('search.thelenders.app/fullchain.pem', 'search.thelenders.app/privkey.pem'))
    # app.run(debug=0, host="0.0.0.0")
