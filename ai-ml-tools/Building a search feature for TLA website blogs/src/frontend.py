import streamlit as st
import requests
from streamlit_utils import show_user_data,text_similar

st.title("Text Vector Embedding Interface")
# title = st.text_input("Title")
# description = st.text_area("Description")
#

# User input for user_id or user_name
user_id = st.text_input("Enter User ID or User Name")

# if st.button("Upload"):
#     response = requests.post("http://localhost:5000/upload", json={"title": title, "description": description})
#     if response.status_code == 200:
#         st.success("Uploaded successfully!")
#         st.json(response.json())
#     else:
#         st.error("Failed to upload")

uploaded_file = st.file_uploader("Choose a file")
if uploaded_file is not None and user_id:
    # Display details of the uploaded file
    st.write("Filename:", uploaded_file.name)

    # Include user_id in the form data sent to the API
    files = {'file': (uploaded_file.name, uploaded_file, uploaded_file.type)}
    data = {'user_id': user_id}  # Data including user_id

    # Send the file and user_id to the Flask API
    response = requests.post("http://localhost:5000/upload", files=files, data=data)

    # Handling response from Flask
    if response.ok:
        st.write("File successfully sent to Flask API")
        # st.write(response.text)
    else:

        st.write("Failed to send file")
else:
    if uploaded_file and not user_id:
        st.warning("Please enter a User ID or User Name to proceed.")

if user_id:
    show_user_data(user_id)
# # Streamlit interface
# st.title('File Upload to Flask API')
#
# uploaded_file = st.file_uploader("Choose a file")
# if uploaded_file is not None:
#     # Display details of the uploaded file
#     st.write("Filename:", uploaded_file.name)
#
#     # Send the file to Flask API
#     files = {'file': (uploaded_file.name, uploaded_file, uploaded_file.type)}
#     response = requests.post("http://localhost:5000/upload", files=files)
#
#     # Handling response from Flask
#     if response.ok:
#         st.write("File successfully sent to Flask API")
#         st.write(response.text)
#     else:
#         st.write("Failed to send file")

text_similar(user_id)