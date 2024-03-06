#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 10:22:45 2024

@author: zugmana2
"""

#import psycopg
from sqlalchemy import create_engine, URL
import pandas as pd
import pandas.io.sql as psql
from os import environ
import json
import os

def get_home_directory():
    # Get the user's home directory
    return os.path.expanduser("~")

def read_config():
    home_dir = get_home_directory()
    config_file = os.path.join(home_dir, '.config.robin')

    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            config = json.load(f)
    else:
        config = {}
        config['URL'] = input("Enter value for DBURL: ")
        config['uname'] = input("Enter value for username: ")
        config['dbname'] = input("Enter value for database: ")
        with open(config_file, 'w') as f:
            json.dump(config, f)

    return config


def checkrobin(sdanid,allsubj=False):

    c = read_config()
    url_object = URL.create(
    drivername="postgresql",
    username= c["uname"],
#    #password="password",
    host=c["URL"],
    database=c["dbname"]
    )

    engine = create_engine(url=url_object)
    if not allsubj:
        my_table    = pd.read_sql(f'select * from core where sdan_id = {sdanid}', engine)
    else :
        my_table    = pd.read_sql('select * from core', engine)
    return my_table
