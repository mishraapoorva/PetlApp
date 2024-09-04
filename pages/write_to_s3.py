import streamlit as st
import boto3
from botocore.exceptions import NoCredentialsError

st.title("Write Data to AWS S3")

# AWS credentials (should be stored securely, such as in environment variables or AWS IAM roles)
aws_access_key_id = st.text_input("AWS Access Key ID", type="password")
aws_secret_access_key = st.text_input("AWS Secret Access Key", type="password")
region_name = st.text_input("AWS Region Name")

bucket_name = st.text_input("S3 Bucket Name")
file_to_upload = st.file_uploader("Choose a file")

# AWS credentials configuration
aws_access_key_id = ""
aws_secret_access_key = ""
bucket_name = ""
#s3_key = 'transformed_data/employees.csv'
region_name = "us-east-1" 

if st.button("Upload to S3"):
    if aws_access_key_id and aws_secret_access_key and region_name and bucket_name and file_to_upload:
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name
            )
            s3_client.upload_fileobj(file_to_upload, bucket_name, file_to_upload.name)
            st.success(f"File '{file_to_upload.name}' successfully uploaded to S3 bucket '{bucket_name}'.")
        except NoCredentialsError:
            st.error("Credentials are not available.")
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
    else:
        st.warning("Please fill in all the fields.")
