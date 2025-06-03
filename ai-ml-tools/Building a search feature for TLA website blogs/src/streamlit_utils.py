import pandas as pd
import requests
import streamlit as st

main_url = "http://127.0.0.1:5000"


def fetch_user_data(user_id):
    """Fetch user data from the Flask API."""
    url = f"{main_url}/userdata/{user_id}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()['user_data']
        return pd.DataFrame(data)
    else:
        st.error('Failed to fetch data or no data available for the specified User ID')
        return pd.DataFrame()


def show_user_data(user_id):
    if st.button("Show User Data"):
        if user_id:
            df = fetch_user_data(user_id)
            if not df.empty:
                st.write("User Uploaded Data:")
                st.dataframe(df)
            else:
                st.write("No data available for this user.")
        else:
            st.warning("Please enter a valid User ID.")


def query_flask_api(text,user_id):
    """Send the input text to the Flask API and get the nearest vectors."""
    url = f"{main_url}/find_similar"
    payload = {"text": text,'user_id':user_id}
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def text_similar(user_id):
    st.title("Text Similarity Finder")
    st.write(
        "This application sends text to a Flask API which processes the text using a DistilBERT model and searches for similar vectors in a FAISS index.")

    # Text input
    user_input = st.text_area("Enter text to find similar vectors:", height=150)

    if st.button("Find Similar"):
        if user_input:
            result = query_flask_api(user_input,user_id)
            if result:
                st.write("Similarity Search Results:")
                st.dataframe(pd.read_json(result['data']))
            else:
                st.error("Failed to get a response from the API. Check if the API is running and accessible.")
        else:
            st.warning("Please enter some text to process.")