import pandas as pd
import requests
import psycopg2
import psycopg2.extras as extras
import numpy as np
import configparser

config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8-sig')


def extract(rows_count):
    response = requests.get(url=f'https://random-data-api.com/api/cannabis/random_cannabis?size={rows_count}')
    data = pd.json_normalize(response.json())
    return data


def transform(data):
    data = data.replace({np.nan: None})
    return data


def load(data):
    def execute_values(cursor, df_to_insert, table_name):
        tuples = [tuple(x) for x in df_to_insert.to_numpy()]
        cols = ','.join(list(df_to_insert.columns)).lower()
        query = f'INSERT INTO {table_name} ({cols}) ' \
                f'VALUES %s'
        extras.execute_values(cursor, sql=query, argslist=tuples)

    conn = psycopg2.connect(user=config.get('db', 'user'),
                            password=config.get('db', 'password'),
                            host=config.get('db', 'host'),
                            port=config.get('db', 'port'),
                            database=config.get('db', 'database'))
    with conn:
        with conn.cursor() as curs:
            curs.execute('CREATE TABLE IF NOT EXISTS api_cannabis (\
                              id int,\
                              uid text,\
                             strain text, \
                             cannabinoid_abbreviation text,\
                            cannabinoid text,\
                             terpene text, \
                             medical_use text,\
                             health_benefit text,\
                             category text,\
                             type text,\
                             buzzword text,\
                             brand text)')
            execute_values(curs, data, 'api_cannabis')
    conn.close()


if __name__ == '__main__':
    df = extract(10)
    df = transform(df)
    load(df)
