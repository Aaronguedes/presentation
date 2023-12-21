import requests
from databricks.sdk.runtime import *
import random
from time import sleep

def get_token(environment,token_table):
    
    url = "**********************"
    secret_configuration = {
        "scope": f"secret_scope_{environment}",
        "user": f"centreon-apirest-user",
        "key": f"centreon-apirest-pwd"
    }
    username = dbutils.secrets.get(scope=secret_configuration['scope'], key=secret_configuration['user'])
    password = dbutils.secrets.get(scope=secret_configuration['scope'], key=secret_configuration['key'])
    payload = {
        "security": {
            "credentials": {
                "login": username,
                "password": password
            }
        }
    }
    response = requests.post(url+'login',json=payload)
    response.raise_for_status()
    token = response.json().get('security', {}).get('token')
    sleep(random.random())
    spark.sql(f"""
            
    INSERT INTO {token_table} (token,date_generation)
    VALUES ('{token}', current_timestamp())
    
             """
)
    return token