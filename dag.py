import requests
import time
from datetime import datetime
import logging
import json


def fetch_user_data():
    try:
        url="https://randomuser.me/api/"

        response=requests.get(url=url)
    except Exception as e:
        print(f"{e.args}")


    if response.status_code == 200:
        response_data=json.loads(response.text)
        return response_data
    else:
        return
        






fetch_user_data()

