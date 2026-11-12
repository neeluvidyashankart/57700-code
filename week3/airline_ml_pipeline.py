
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score,f1_score,roc_auc_score

from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from joblib import dump, load
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature

# name of the model file
model_filename = "/tmp/airline_model.pickle"


dag = DAG(                                                    
   dag_id="airflow-airline-pipeline",                      
   start_date=datetime(2025, 1, 1),               
   schedule=None,  
   catchup=False,                                 
)

# DAG Step 1: Get data
def _get_data(ti):
    airline_df = pd.read_csv("/opt/airflow/data/airline-historical-data.csv")
    airline_df.columns = airline_df.columns.str.strip()

    # Encode Yes/No and Y/N columns to 1/0
    airline_df['CrewAvailable'] = airline_df['CrewAvailable'].apply(lambda x: 1 if str(x).strip().upper() == 'Y' else 0)
    airline_df['BusyRunways'] = airline_df['BusyRunways'].apply(lambda x: 1 if str(x).strip().upper() == 'Y' else 0)
    airline_df['DelayedYN'] = airline_df['DelayedYN'].apply(lambda x: 1 if str(x).strip().upper() == 'YES' else 0)

    # Encode Weather text to numbers
    weather_map = {'calm': 0, 'sunny': 1, 'cloudy': 2, 'windy': 3, 'rain': 4, 'snow': 5, 'ice': 6, 'tstorms': 7}
    airline_df['Weather'] = airline_df['Weather'].apply(lambda x: weather_map.get(str(x).strip().lower(), -1))

    # One-hot encode categorical text columns (Airline, Origin, Destination)
    feature_cols = ['FlightNumber', 'Airline', 'Origin', 'Destination', 'Duration', 'Weather', 'CrewAvailable', 'BusyRunways']
    df_processed = pd.get_dummies(airline_df[feature_cols], drop_first=True)
    df_processed['DelayedYN'] = airline_df['DelayedYN']

    #Push the data into XCOM for the next step
    ti.xcom_push(key='airline_data', value=df_processed)
    return df_processed


# DAG Step 2: Train the model
def _train_model(ti):
    #Get the data from XCOM
    df_processed = ti.xcom_pull(key='airline_data', task_ids="get_data")

    #Create the X and y using the RandomForestClassifier algorithm’s train-test-split function
    X = df_processed.drop(columns=['DelayedYN'])
    y = df_processed['DelayedYN']
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0)

    model = RandomForestClassifier(random_state=0)
    model.fit(X_train, y_train)

    dump(model, model_filename)

    ti.xcom_push(key='X_train', value=X_train)
    ti.xcom_push(key='X_test', value=X_test)
    ti.xcom_push(key='y_test', value=y_test.to_list())
    return X_train, X_test, y_test.to_list()


# DAG step 3: Load the saved model and run a prediction
def _load_and_predict_model(ti):
    # lGet the model from the pickle file
    model = load(model_filename)

    # Get the X and y data from XCOM
    X_train = ti.xcom_pull(key='X_train', task_ids="train_model")
    X_test = ti.xcom_pull(key='X_test', task_ids="train_model")
    y_test = ti.xcom_pull(key='y_test', task_ids="train_model")

    # run the prediction
    y_pred_rfc = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]
    roc_auc = roc_auc_score(y_test, y_prob)
    print(f'ROC-AUC : {roc_auc * 100:.2f}%')

    #Compute the Accuracy, Precision, F1 and Roc-auc metrics
    accuracy = accuracy_score(y_test, y_pred_rfc)
    print(f'Accuracy: {accuracy * 100:.2f}%')
    precision = precision_score(y_test, y_pred_rfc, average='weighted')
    print(f'Precision : {precision * 100:.2f}%')
    f1 = f1_score(y_test, y_pred_rfc, average='weighted')
    print(f'F1 : {f1 * 100:.2f}%')

    # Push the metrics into XCOM for the next step
    ti.xcom_push(key='X_train', value=X_train)    
    ti.xcom_push(key='accuracy', value=accuracy)    
    ti.xcom_push(key='precision', value=precision)    
    ti.xcom_push(key='f1', value=f1)
    ti.xcom_push(key='roc_auc', value=roc_auc)
    return X_train, accuracy, precision, f1, roc_auc

# DAG Step 4: Log the model in MLFlow
def _log_model_in_mlflow(ti):
    # Load the model from the pickle file
    model = load(model_filename)

    # Get the data from XCOM
    X_train = ti.xcom_pull(key='X_train',      task_ids="load_and_predict_model")
    accuracy = ti.xcom_pull(key='accuracy',    task_ids="load_and_predict_model")
    precision = ti.xcom_pull(key='precision',  task_ids="load_and_predict_model")
    f1 = ti.xcom_pull(key='f1',                task_ids="load_and_predict_model")
    roc_auc = ti.xcom_pull(key='roc_auc',      task_ids="load_and_predict_model")

    exp_name = "neelu_week3_airline_ml_experiment"    
    model_name = "neelu_week3_airline_model"         
    url = "http://192.168.58.105:5000"
        
    # Set the MLFlow tracking URL ,MLFlow experiment and model names
    mlflow.set_tracking_uri(uri=url)
    mlflow.set_experiment(exp_name)

    with mlflow.start_run():
      signature = infer_signature(X_train, model.predict(X_train))

      # Log the model and the metrics in MLFlow
      mlflow.sklearn.log_model(model, artifact_path=model_name, registered_model_name=model_name, signature=signature)

      mlflow.log_metric("Accuracy", accuracy)
      mlflow.log_metric("Precision", precision)
      mlflow.log_metric("F1", f1)
      mlflow.log_metric("ROC_AUC", roc_auc)

      mlflow.end_run()

################################################
# configure the tasks

get_data = PythonOperator(                                
   task_id="get_data",
   python_callable=_get_data,                             
   dag=dag,
)

train_model = PythonOperator(                                
   task_id="train_model",
   python_callable=_train_model,                             
   dag=dag,
)

load_and_predict_model = PythonOperator(                                
   task_id="load_and_predict_model",
   python_callable=_load_and_predict_model,                             
   dag=dag,
)

log_model_in_mlflow = PythonOperator(                                
   task_id="log_model_in_mlflow",
   python_callable=_log_model_in_mlflow,                             
   dag=dag,
)

# specify the pipeline steps
get_data >> train_model >> load_and_predict_model >> log_model_in_mlflow


