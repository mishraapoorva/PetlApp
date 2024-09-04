import boto3
import streamlit as st
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from transformers import pipeline

# Step 1: Retrieve Data from S3
def get_s3_data(bucket_name, file_key, aws_access_key_id, aws_secret_access_key):
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    content = response['Body'].read().decode('utf-8')
    return content

# Step 2: Preprocess the Data
def preprocess_data(data, chunk_size=200):
    words = data.split()
    chunks = [' '.join(words[i:i+chunk_size]) for i in range(0, len(words), chunk_size)]
    return chunks

# Step 3: Implement a Retriever
def retrieve_relevant_chunks(query, chunks):
    vectorizer = TfidfVectorizer().fit(chunks)
    chunk_vectors = vectorizer.transform(chunks)
    query_vector = vectorizer.transform([query])
    
    similarities = cosine_similarity(query_vector, chunk_vectors).flatten()
    most_relevant_idx = similarities.argmax()
    return chunks[most_relevant_idx]

# Step 4: Generate an Answer
def generate_answer(query, context):
    qa_pipeline = pipeline("question-answering")
    answer = qa_pipeline(question=query, context=context)
    return answer['answer']

# Streamlit App
def main_app():
    st.title("RAG-Based Question Answering on S3 Data")

    bucket_name = st.text_input("S3 Bucket Name", value="your-bucket-name")
    file_key = st.text_input("S3 File Key", value="path/to/your/data.txt")
    aws_access_key_id = st.text_input("AWS Access Key ID", type="password")
    aws_secret_access_key = st.text_input("AWS Secret Access Key", type="password")
    query = st.text_input("Enter your question")

    if st.button("Get Answer"):
        if bucket_name and file_key and query and aws_access_key_id and aws_secret_access_key:
            with st.spinner('Fetching data from S3...'):
                data = get_s3_data(bucket_name, file_key, aws_access_key_id, aws_secret_access_key)
            with st.spinner('Processing data...'):
                chunks = preprocess_data(data)
            with st.spinner('Retrieving relevant information...'):
                relevant_chunk = retrieve_relevant_chunks(query, chunks)
            with st.spinner('Generating answer...'):
                answer = generate_answer(query, relevant_chunk)
            
            st.write(f"**Question:** {query}")
            st.write(f"**Answer:** {answer}")
        else:
            st.error("Please provide all inputs!")

if __name__ == "__main__":
    main_app()
