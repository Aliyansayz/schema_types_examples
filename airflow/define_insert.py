from datetime import datetime, timedelta
import pandas as pd
import sqlite3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'example_sqlite_dag',
    default_args=default_args,
    description='An example ETL DAG using star schema with SQLite and Pandas',
    schedule_interval=timedelta(days=1),
)

# Define the tasks
def create_tables():
    # Connect to SQLite database
    conn = sqlite3.connect('/tmp/example_star_schema.db')

    # Create dimension table
    create_dim_person_table = """
    CREATE TABLE IF NOT EXISTS dim_person (
        person_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        age INTEGER NOT NULL,
        gender TEXT NOT NULL,
        city TEXT NOT NULL
    );
    """
    
    create_fact_people_table = """
    CREATE TABLE IF NOT EXISTS fact_people (
        fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
        person_id INTEGER,
        purchase_amount REAL,
        purchase_category TEXT,
        FOREIGN KEY(person_id) REFERENCES dim_person(person_id)
    );
    """

    # Execute SQL commands to create tables
    conn.execute(create_dim_person_table)
    conn.execute(create_fact_people_table)
    
    conn.commit()
    conn.close()

def insert_data():
    # Connect to SQLite database
    conn = sqlite3.connect('/tmp/example_star_schema.db')

    # Create DataFrames for dimension and fact tables
    dim_person_data = {
        'name': ['John', 'Jane', 'Doe', 'Alice', 'Bob'],
        'age': [28, 34, 29, 22, 45],
        'gender': ['Male', 'Female', 'Male', 'Female', 'Male'],
        'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    }
    
    fact_people_data = {
        'person_id': [1, 2, 3, 4, 5],
        'purchase_amount': [250.50, 300.00, 150.75, 200.00, 400.25],
        'purchase_category': ['Medium', 'High', 'Low', 'Medium', 'High']
    }

    # Convert dictionaries to DataFrames
    dim_person_df = pd.DataFrame(dim_person_data)
    
    # Insert data into dimension table
    dim_person_df.to_sql('dim_person', conn, if_exists='append', index=False)

    # Insert data into fact table
    fact_people_df = pd.DataFrame(fact_people_data)
    
    # Ensure person_id matches with the dimension table (this step is usually handled in a real ETL process)
    fact_people_df.to_sql('fact_people', conn, if_exists='append', index=False)

    conn.close()

# Define the PythonOperator tasks
create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    dag=dag,
)

# Set task dependencies
create_tables_task >> insert_data_task

