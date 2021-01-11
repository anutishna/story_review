import psycopg2
from time import sleep
import os
import pandas as pd


def read_query(file_path):
    query_path = os.path.dirname(os.path.realpath(__file__)) + "/" + file_path
    with open(query_path) as query_file:
        q = query_file.read()
    return q


def update_query(query: str, params={}):
    for param in params:
        query = query.replace("{{"+param+"}}", str(params[param]))
    return query


def db_query(db_settings, query):
    connected = False
    while not connected:
        try:
            conn = psycopg2.connect(db_settings)
            cur = conn.cursor()
            print("Database connected")

            cur.execute(query)
            rs = cur.fetchall()
            cur.close()
            conn.close()
            connected = True
        except psycopg2.OperationalError:
            print("Couldn't establish connection.")
            print("Next connetion attempt in 15 seconds.")
            sleep(15)
    return rs


def db_query_dataframe(db_settings, query):
    connected = False
    while not connected:
        try:
            conn = psycopg2.connect(db_settings)
            cur = conn.cursor()
            print("Database connected")
            cur.execute(query)
            df = pd.DataFrame(cur.fetchall())
            if df.shape[0] > 0:
                df.columns = [desc[0] for desc in cur.description]
            cur.close()
            conn.close()
            connected = True
        except psycopg2.OperationalError:
            print("Couldn't establish connection.")
            print("Next connetion attempt in 15 seconds.")
            sleep(15)
    return df
