# DAG to insert data from CSV files
# =============================================================

from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from sqlalchemy import create_engine

dag = DAG(                                                    
   dag_id="csv_to_postgres_historical_data",                       
   start_date=datetime(2026, 10, 27),                 
   schedule_interval=None,  
   catchup=False,                                 
)

##############################
# function to read data from the CSV file 
##############################
def _read_csv_data(ti):
    # read the csv file that is accessible by the container
    airline_df = pd.read_csv("/opt/airflow/data/airline-historical-data.csv")

    # feature transformation
    # convert the Weather, CrewAvailable, BusyRunways and DelayedYN fields into numeric. 
    # For example, airline_df['CrewAvailable'] = airline_df['CrewAvailable'].apply(lambda x: 0 if x == 'N' else 1)
    airline_df['CrewAvailable'] = airline_df['CrewAvailable'].apply(lambda x: 1 if str(x).strip().upper() == 'Y' else 0)
    airline_df['BusyRunways'] = airline_df['BusyRunways'].apply(lambda x: 1 if str(x).strip().upper() == 'Y' else 0)
    airline_df['DelayedYN'] = airline_df['DelayedYN'].apply(lambda x: 1 if str(x).strip().upper() == 'YES' else 0)

    # Mapping weather statuses to categorical flags matching database integers
    weather_map = {'calm': 0, 'sunny': 1, 'cloudy': 2, 'windy': 3, 'rain': 4, 'snow': 5, 'ice': 6, 'tstorms': 7}
    airline_df['Weather'] = airline_df['Weather'].apply(lambda x: weather_map.get(str(x).strip().lower(), -1))
    ti.xcom_push(key='airline_key', value=airline_df)    
    return airline_df

##############################
# function to write the CSV data into the database
##############################
def _write_data_to_DB(ti):
    # pull the data value from the previous task
    airline_df = ti.xcom_pull(key='airline_key', task_ids="read_csv_data")
    #print(airline_df)  # for debugging 

    # Connect to postgres container
    conn_string = 'postgresql://postgres:password@57700-postgres:5432/postgres'

    db = create_engine(conn_string)
    airline_df.columns = airline_df.columns.str.lower()

    with db.connect() as conn:
        airline_df.to_sql('airline_weather_data', conn, if_exists='append', index=False)

read_csv = PythonOperator(                                
   task_id="read_csv_data",
   python_callable=_read_csv_data,                             
   dag=dag,
)

write_data = PythonOperator(                                
   task_id="write_data_to_DB",
   python_callable=_write_data_to_DB,                             
   dag=dag,
)

read_csv >> write_data

