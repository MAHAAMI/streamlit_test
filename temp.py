# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import streamlit as st
import requests
import json
import getpass
from sqlalchemy import create_engine
import pandas as pd
from snowflake.sqlalchemy import URL
import sqlalchemy
import numpy as np
pd.set_option('display.max_columns', None)
# environment is either 'nonprod' or 'prod'
environment = 'prod'
seven_letter = 'MAHAAMI'
sf_role = 'GF_CONSUMER_NONEXEMPT'
sf_warehouse = 'GF_WAREHOUSE'

base_url = f'https://vault-{ environment }.centralus.chrazure.cloud'

# Some helper functions

def get_client_token(username, password):
    payload = {
        'password': password
    }

    response = requests.post(f'{ base_url }/v1/auth/okta/login/{ seven_letter.lower() }', data=payload, verify=False)

    client_token = response.json()['auth']['client_token']

    return client_token

def get_leased_password(username, client_token):
    CONN = 'snowflake'
    RL = username.lower()

    if environment == 'prod':
        DB = 'prod'
    else:
        DB = 'dev'

    headers = {
        'X-Vault-Token': client_token
    }

    response = requests.get(f'{ base_url }/v1/database/static-creds/{ CONN }_{ DB }_{ RL }', headers=headers, verify=False)
    print(response.json())
    leased_password = response.json()['data']['password']

    return leased_password

def get_sqlalchemy_engine(username, leased_password):
    USER = f'{ username.upper() }@CHROBINSON.COM'
    PASSWORD = leased_password

    if environment == 'prod':
        ACCOUNT = 'prod_chrobinson.east-us-2.azure'
    else:
        ACCOUNT = 'dev_chrobinson.east-us-2.azure'
        
    url = URL(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        role=sf_role,
        warehouse=sf_warehouse
    )
    
    engine = create_engine(url)

    return engine

client_token = get_client_token(seven_letter, getpass.getpass(prompt='CHR password... '))
leased_password = get_leased_password(seven_letter, client_token)
engine = get_sqlalchemy_engine(seven_letter, leased_password)
sw_conn = engine.connect()

with open("test.sql", "r") as f:
        appt_query = f.read() 
# print(appt_query)        
df = pd.read_sql(appt_query, sw_conn)

df