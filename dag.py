import requests
import time
from datetime import datetime
import logging
import json 


def fetch_user_data(**kwargs):
    try:
        url="https://randomuser.me/api/"

        response=requests.get(url=url)
    except Exception as e:
        print(f"{e.args}")


    if response.status_code == 200:
        response_data=json.loads(response.text)

        kwargs["ti"].xcom_push(key="responce_data", value=response_data)

    else:
        raise RuntimeError
        

    

def format_data(**kwargs):
    json_data=kwargs["ti"].xcom_pull(task_id="fetch_user_data", key="response_data")
    
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
    data_to_stream=kwargs["ti"].xcom_pull(task_id="format_data", key="format_data")

    






format_data(fetch_user_data())

