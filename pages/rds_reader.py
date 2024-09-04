import streamlit as st
import psycopg2
import cx_Oracle
import mysql.connector

# Function to read from PostgreSQL
def read_postgres_data(host, port, dbname, user, password, query):
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
    except Exception as e:
        return f"An error occurred: {e}"
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Function to read from Oracle
def read_oracle_data(dsn, user, password, query):
    try:
        conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
    except Exception as e:
        return f"An error occurred: {e}"
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Function to read from Oracle
def read_sql_data(host, port, dbname, user, password, query):
    try:
        conn = mysql.connector.connect(
                    host=host,
                    port=int(port),
                    database=dbname,
                    user=user,
                    password=password
                )
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
    except Exception as e:
        return f"An error occurred: {e}"
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Streamlit app
def main():
    st.title("AWS RDS Data Reader")
    # Options for the dropdown menu
    options = ["Select an option","Postgres SQL", "MySQL", "Oracle"]


    # Create the dropdown menu
    selected_option = st.selectbox("Choose an option:", options)   

    # Check if the selected option is not the placeholder
    if selected_option != "Select an option":
        if selected_option == "Postgres SQL":                           

            # PostgreSQL Section
            st.header("PostgreSQL")
            pg_host = st.text_input("PostgreSQL Host", "your-rds-endpoint")
            pg_port = st.text_input("PostgreSQL Port", "5432")
            pg_dbname = st.text_input("PostgreSQL Database Name", "your-dbname")
            pg_user = st.text_input("PostgreSQL Username", "your-username")
            pg_password = st.text_input("PostgreSQL Password", type="password")
            pg_query = st.text_area("PostgreSQL Query", "SELECT * FROM your_table LIMIT 10;")
            
            if st.button("Run PostgreSQL Query"):
                if pg_host and pg_port and pg_dbname and pg_user and pg_password and pg_query:
                    with st.spinner('Reading data from PostgreSQL...'):
                        pg_result = read_postgres_data(pg_host, pg_port, pg_dbname, pg_user, pg_password, pg_query)
                    st.write("**PostgreSQL Query Result:**")
                    st.write(pg_result)
                else:
                    st.error("Please fill in all PostgreSQL fields!")
        elif selected_option == "Oracle":
            # Oracle Section
            st.header("Oracle")
            ora_dsn = st.text_input("Oracle DSN", "your-rds-endpoint:1521/your-service-name")
            ora_user = st.text_input("Oracle Username", "your-username")
            ora_password = st.text_input("Oracle Password", type="password")
            ora_query = st.text_area("Oracle Query", "SELECT * FROM your_table WHERE ROWNUM <= 10")
            
            if st.button("Run Oracle Query"):
                if ora_dsn and ora_user and ora_password and ora_query:
                    with st.spinner('Reading data from Oracle...'):
                        ora_result = read_oracle_data(ora_dsn, ora_user, ora_password, ora_query)
                    st.write("**Oracle Query Result:**")
                    st.write(ora_result)
                else:
                    st.error("Please fill in all Oracle fields!")

        elif selected_option == "MySQL":
            # PostgreSQL Section
            st.header("PostgreSQL")
            host = st.text_input("MySQL Host", "your-rds-endpoint")
            port = st.text_input("MySQL Port", "3306")
            dbname = st.text_input("MySQL Database Name", "your-dbname")
            user = st.text_input("MySQL Username", "your-username")
            password = st.text_input("MySQL Password", type="password")
            query = st.text_area("MySQL Query", "SELECT * FROM your_table LIMIT 10;")
            
            if st.button("Run MySQL Query"):
                if host and port and dbname and user and password and query:
                    with st.spinner('Reading data from PostgreSQL...'):
                        pg_result = read_sql_data(host, port, dbname, user, password, query)
                    st.write("**MySQL Query Result:**")
                    st.write(pg_result)
                else:
                    st.error("Please fill in all MySQL fields!")

    else:
        st.write("Please select a valid option from the dropdown.")

if __name__ == "__main__":
    main()



# Query to fing the table names in Postgres
# SELECT table_name
# FROM information_schema.tables
# WHERE table_schema = 'public'
# ORDER BY table_name;
# "emp_bckup"



