import requests
import time
from datetime import datetime, timedelta
import logging
import json 
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

def fetch_user_data(**kwargs):
    url="https://randomuser.me/api/"

    response=requests.get(url=url)


    if response.status_code == 200:
        response_data=json.loads(response.text)

        kwargs["ti"].xcom_push(key="responce_data", value=response_data)

    else:
        raise RuntimeError
        

    

def format_data(**kwargs):
    json_data=kwargs["ti"].xcom_pull(key="responce_data", task_ids="fetch_user_data")
    
    formated_data={}

    formated_data["name"] = str(json_data["results"][0]["name"]["first"]) + str(json_data["results"][0]["name"]["last"])
    formated_data["city"] = str(json_data["results"][0]["location"]["city"]) 
    formated_data["country"] = str(json_data["results"][0]["location"]["country"]) 
    formated_data["email"] = str(json_data["results"][0]["email"]) 
    formated_data["dob"] = str(json_data["results"][0]["dob"]["date"])
    formated_data["age"] = str(json_data["results"][0]["dob"]["age"]) 
    formated_data["phone"] = str(json_data["results"][0]["phone"])
    formated_data["picture"] = str(json_data["results"][0]["picture"]["thumbnail"])


    kwargs["ti"].xcom_push(key="format_data", value=formated_data)



def stream_data(**kwargs):
    BOOSTRAP_SERVER="localhost:9092"
    data_to_stream=kwargs["ti"].xcom_pull(key="format_data", task_ids="format_data")
    
    data_to_stream=json.dumps(data_to_stream)
    
    kafka_producer=KafkaProducer(bootstrap_servers='localhost:9092')
    
    kafka_producer.send(topic="stream", value=data_to_stream.encode())



default_args={
    "owner": "Pinil Dissanyaka",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 7),
    "email": ["pinildissanayaka@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}



with DAG(
    'Real_Time_Streaming',
    default_args=default_args,
    description="Real_time_data_streaming ",
    schedule_interval="@daily") as dag:
    
    
    fetch_data =PythonOperator(
        task_id="fetch_user_data",
        python_callable=fetch_user_data,    
        provide_context=True
    )
    
    _format_data = PythonOperator(
        task_id="format_data",
        python_callable=format_data,
        provide_context=True
    )
    
    _stream_data=PythonOperator(
        task_id="stream_data_to_kafka",
        python_callable=stream_data,
        provide_context=True
    )
    
    
    fetch_data >> _format_data >> _stream_data