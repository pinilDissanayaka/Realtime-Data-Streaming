import json
import requests
from time import sleep
from uuid import uuid4
from kafka import KafkaProducer



def fetch_user_data():
    url="https://randomuser.me/api/"

    response=requests.get(url=url)


    if response.status_code == 200:
        response_data=json.loads(response.text)

        return response_data

    else:
        raise RuntimeError
        

def format_data(json_data:dict):
    
    formated_data={}

    formated_data["id"] = str(uuid4())
    formated_data["name"] = str(json_data["results"][0]["name"]["first"]) + " " + str(json_data["results"][0]["name"]["last"])
    formated_data["city"] = str(json_data["results"][0]["location"]["city"]) 
    formated_data["country"] = str(json_data["results"][0]["location"]["country"]) 
    formated_data["email"] = str(json_data["results"][0]["email"]) 
    formated_data["dob"] = str(json_data["results"][0]["dob"]["date"])
    formated_data["age"] = str(json_data["results"][0]["dob"]["age"]) 
    formated_data["phone"] = str(json_data["results"][0]["phone"])
    formated_data["picture"] = str(json_data["results"][0]["picture"]["thumbnail"])


    return formated_data





def stream_data(data_to_stream):
    server="localhost"
    port="9092"

    
    data_to_stream=json.dumps(data_to_stream)
    
    kafka_producer=KafkaProducer(bootstrap_servers=f"{server}:{port}")
    
    kafka_producer.send(topic="stream", value=data_to_stream.encode("utf-8"))



if __name__ == "__main__":
    while True:
        sleep(5)
        json_data=fetch_user_data()
        formated_data=format_data(json_data)
        stream_data(formated_data)