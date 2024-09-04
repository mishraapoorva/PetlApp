
import streamlit as st
import pandas as pd
import boto3
from io import StringIO

st.set_page_config(page_title="AWS Data Ingestion", page_icon=":cloud:")

st.title("AWS Data Ingestion App")

# 


st.write("Welcome to the AWS Data Ingestion App. Use the navigation sidebar to choose the operation you want to perform. For simple aws upload please continue to choose your file.")


# AWS credentials configuration
aws_access_key_id = ""
aws_secret_access_key = ""
bucket_name = ""

# Initialize S3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key)

# Streamlit UI
st.title("ETL Application: Load Data to AWS S3")

# File uploader
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    # Read the uploaded CSV file into a pandas DataFrame
    df = pd.read_csv(uploaded_file)
    st.write("Original Data:")
    st.dataframe(df.head())  # Show first few rows

    # Simple transformation: Add a new column with transformed data
    df['new_sal'] = df['salary'].apply(lambda x: x * 2 if pd.notna(x) else x)

    st.write("Transformed Data:")
    st.dataframe(df.head())  # Show first few rows of transformed data

    # Define the S3 path
    s3_path = "transformed_data/" + uploaded_file.name

    # Button to trigger the load process
    if st.button("Load Data to S3"):
        # Convert DataFrame to CSV format
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload CSV to S3
        s3.put_object(Bucket=bucket_name, Key=s3_path, Body=csv_buffer.getvalue())
        st.success(f"Data successfully loaded to S3 at {s3_path}")