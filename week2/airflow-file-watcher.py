
# DAG to poll a directory and load data from CSV files that show up in that directory
# =============================================================

from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import psycopg2
from sqlalchemy import create_engine


# to keep the DAG from running constantly and keep watching files being dropped into the folder, change schedule to "@continuous".
dag = DAG(                                                    
   dag_id="csv_to_postgres_incremental",          # Provide a different and valid DAG name               
   start_date=datetime(2026, 10, 27),                 # provide a valid Year, Month and Day (you can use the date you run this DAG)
   schedule="@continuous",
   max_active_runs=1,    
   catchup=False,                                 
)

##############################
# function to wait for a file to show up into a folder 
##############################
wait_for_file = FileSensor(
    task_id = "wait_for_file",
    filepath="incremental-data.csv",
    # for example:  /opt/airflow/data/incremental-data.csv
    fs_conn_id='FileWatcher',
    poke_interval=5,
    timeout=600,
    recursive=True,
    dag=dag,
)

##############################
# function to read data from the CSV file 
##############################
def _read_csv_data(ti):
    airline_df = pd.read_csv("/opt/airflow/data/incremental-data.csv")  
    ## for example, /opt/airflow/data/incremental-data.csv
    #print(airline_df.head())  # uncomment for debugging purposes

    # feature transformation
    # convert the Weather, CrewAvailable, BusyRunways and DelayedYN fields into numeric. 
    # For example, airline_df['CrewAvailable'] = airline_df['CrewAvailable'].apply(lambda x: 0 if x == 'N' else 1)
    airline_df['CrewAvailable'] = airline_df['CrewAvailable'].apply(lambda x: 1 if str(x).strip().upper() == 'Y' else 0)
    airline_df['BusyRunways'] = airline_df['BusyRunways'].apply(lambda x: 1 if str(x).strip().upper() == 'Y' else 0)
    airline_df['DelayedYN'] = airline_df['DelayedYN'].apply(lambda x: 1 if str(x).strip().upper() == 'YES' else 0)

    # Mapping weather statuses cleanly to categorical flags matching database integers
    weather_map = {'calm': 0, 'sunny': 1, 'cloudy': 2, 'windy': 3, 'rain': 4, 'snow': 5, 'ice': 6, 'tstorms': 7}
    airline_df['Weather'] = airline_df['Weather'].apply(lambda x: weather_map.get(str(x).strip().lower(), -1))
    ti.xcom_push(key='airline_key', value=airline_df)    
    return airline_df

##############################
# function to write the CSV data into the database
##############################
def _write_data_to_DB(ti):
    airline_df = ti.xcom_pull(key='airline_key', task_ids="read_csv_data")

    #print("*** in write_data ***")
    #print(airline_df.head())

    # Establish engine routing pool connection via internal Docker network hostname
    conn_string = 'postgresql://postgres:password@57700-postgres:5432/postgres'

    db = create_engine(conn_string)
    
    airline_df.columns = airline_df.columns.str.lower()
    conn = db.connect()

    
    conn1 = psycopg2.connect(database="postgres", user='postgres', password='password', host='57700-postgres', port= '5432')

    conn1.autocommit = True
    cursor = conn1.cursor()

    # save data into the DB
    airline_df.to_sql('airline-incremental-data', conn, if_exists= 'append',index=False)

    conn1.commit()
    conn1.close()

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

# this task will move the processed file into an archive folder.  
archive_file = BashOperator (
    task_id = "archive_file",
    bash_command = 'mv /opt/airflow/data/incremental-data.csv /opt/airflow/archive/incremental-data-{{ run_id }}.csv',   # verify the folder and filenames
    dag=dag,
)
wait_for_file >> read_csv >> write_data >> archive_file

