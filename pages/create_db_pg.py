import streamlit as st
import psycopg2
from psycopg2 import sql 

# Streamlit app title
st.title("Create Database in AWS RDS")

# Input fields for RDS connection details
rds_host = st.text_input("RDS Host (Endpoint)", "your-rds-endpoint.amazonaws.com")
rds_port = st.text_input("RDS Port", "5432")
rds_user = st.text_input("RDS Username", "your_username")
rds_password = st.text_input("RDS Password", "your_password", type="password")
new_db_name = st.text_input("New Database Name", "new_database")

# Button to trigger database creation
if st.button("Create Database"):
    if not rds_host or not rds_user or not rds_password or not new_db_name:
        st.error("Please fill all the required fields")
    else:
        try:
            # Connect to the PostgreSQL instance
            conn = psycopg2.connect(
                host=rds_host,
                port=rds_port,
                dbname='postgres',  # Connect to default 'postgres' database
                user=rds_user,
                password=rds_password
            )
            conn.autocommit = True  # Enable autocommit mode to create database

            # Create a cursor and execute the SQL command to create the database
            cur = conn.cursor()
            create_db_query = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(new_db_name))
            cur.execute(create_db_query)

            st.success(f"Database '{new_db_name}' created successfully!")

        except psycopg2.Error as e:
            st.error(f"Error: {e}")
        finally:
            if conn:
                cur.close()
                conn.close()


if st.button("Create Hub & Sat tables"):                                               

    # SQL statements to create tables and populate data
    sql_statements = [
        """
        CREATE TABLE IF NOT EXISTS employee_hub (
            employee_id INT PRIMARY KEY
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS employee_satellite (
            employee_id INT,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100),
            phone_number VARCHAR(15),
            hire_date DATE,
            job_id VARCHAR(10),
            salary DECIMAL(10, 2),
            commission_pct VARCHAR(10),
            manager_id VARCHAR(10),
            department_id VARCHAR(10),
            FOREIGN KEY (employee_id) REFERENCES employee_hub(employee_id)
        );
        """,
        """
        INSERT INTO employee_hub (employee_id)
        SELECT DISTINCT CAST(employee_id AS INT)
        FROM emp_bckup
        ON CONFLICT (employee_id) DO NOTHING;
        """,
        """
        INSERT INTO employee_satellite (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
        SELECT CAST(employee_id AS INT), first_name, last_name, email, phone_number, 
        CAST(hire_date AS DATE), job_id, 
           CASE 
               WHEN REPLACE(salary, ',', '') ~ '^[-+]?[0-9]*\.?[0-9]+$' THEN CAST(REPLACE(salary, ',', '') AS FLOAT8)
               ELSE NULL 
           END AS salary,
           CASE 
            WHEN commission_pct ~ '^[-+]?[0-9]*\.?[0-9]+$' THEN CAST(commission_pct AS DECIMAL(5, 2))
           ELSE 0.5
            END AS commission_pct, 
           CASE 
           WHEN manager_id ~ '^[0-9]+$' THEN CAST(manager_id AS INT)
           ELSE NULL  -- or 0 or some other default value
            END AS manager_id,
           CASE 
           WHEN department_id ~ '^[0-9]+$' THEN CAST(department_id AS INT)
           ELSE NULL  -- or 0 or some other default value
       END AS department_id
        FROM emp_bckup;
        """
    ]

    # Connect to the PostgreSQL database and execute SQL statements
    try:
        conn = psycopg2.connect(
            host=rds_host,
            port=rds_port,
            dbname=new_db_name,
            user=rds_user,
            password=rds_password
        )
        cursor = conn.cursor()
        
        for statement in sql_statements:
            cursor.execute(statement)
            conn.commit()
        
        print("Tables created and data inserted successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

