import streamlit as st
import boto3
import pandas as pd
import psycopg2
import mysql.connector
from io import StringIO

# Streamlit app title
st.title("S3 to RDS SQL Data Transfer")

# AWS S3 Configuration Inputs
st.header("AWS S3 Configuration")
s3_bucket_name = st.text_input("S3 Bucket Name", "your-s3-bucket-name")
s3_file_key = st.text_input("S3 File Key", "path/to/your-file.csv")
aws_access_key_id = st.text_input("AWS Access Key ID", "your-access-key-id")
aws_secret_access_key = st.text_input("AWS Secret Access Key", "your-secret-access-key", type="password")

# RDS Configuration Inputs
st.header("RDS Configuration")
db_engine = st.selectbox("Database Engine", ["PostgreSQL", "MySQL"])
rds_host = st.text_input("RDS Host (Endpoint)", "your-rds-endpoint.amazonaws.com")
rds_port = st.text_input("RDS Port", "5432" if db_engine == "PostgreSQL" else "3306")
rds_dbname = st.text_input("Database Name", "your_database_name")
rds_user = st.text_input("Database Username", "your_username")
rds_password = st.text_input("Database Password", "your_password", type="password")
table_name = st.text_input("Table Name", "your_table_name")

# Button to trigger the transfer
if st.button("Transfer Data"):
    if not all([s3_bucket_name, s3_file_key, aws_access_key_id, aws_secret_access_key, rds_host, rds_dbname, rds_user, rds_password, table_name]):
        st.error("Please fill in all the required fields.")
    else:
        try:
            # Download file from S3
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
            response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_file_key)
            csv_content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))

            # Connect to the RDS database
            if db_engine == "PostgreSQL":
                conn = psycopg2.connect(
                    host=rds_host,
                    port=rds_port,
                    dbname=rds_dbname,
                    user=rds_user,
                    password=rds_password
                )
            elif db_engine == "MySQL":
                conn = mysql.connector.connect(
                    host=rds_host,
                    port=int(rds_port),
                    database=rds_dbname,
                    user=rds_user,
                    password=rds_password
                )

            cur = conn.cursor()

            # SQL query to create a table if it doesn't exist
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (EMPLOYEE_ID VARCHAR(50) PRIMARY KEY,FIRST_NAME VARCHAR(50),LAST_NAME VARCHAR(50),EMAIL VARCHAR(100) UNIQUE NOT NULL,PHONE_NUMBER VARCHAR(20),HIRE_DATE VARCHAR(50) NOT NULL,JOB_ID VARCHAR(10) NOT NULL,SALARY VARCHAR(50),COMMISSION_PCT VARCHAR(50),MANAGER_ID VARCHAR(50),DEPARTMENT_ID VARCHAR(50));"

            # Execute the SQL query
            cur.execute(create_table_query)
            conn.commit()

            print("Table created successfully (if it didn't exist).")

            # Create SQL insert statement
            columns = df.columns.tolist()
            columns_str = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))
            insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

            # Insert data into the RDS database
            for row in df.itertuples(index=False):
                cur.execute(insert_query, row)

            conn.commit()
            st.success("Data has been successfully written to the database.")

        except Exception as e:
            st.error(f"An error occurred: {e}")
        finally:
            if conn:
                cur.close()
                conn.close()
