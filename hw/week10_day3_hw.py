from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging

import requests


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def extract_transform(api_endpoint):
    res = requests.get(api_endpoint)
    data = res.json() # .json() -> 딕셔너리
    records = []

    for country in data:
        records.append([country["name"]["official"], country["population"], country["area"]])

    return records

@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()

    # DROP TABLE을 먼저 수행 -> FULL REFRESH을 하는 형태
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
                        CREATE TABLE {schema}.{table} (
                            country varchar(256) primary key,
                            population int,
                            area float
                        );""")

      # 테이블에 레코드 적재
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES ('{r[0]}', {r[1]}, {r[2]});"
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;")

    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")


with DAG(
    dag_id = 'CountryInfo',
    start_date = datetime(2023,5,30),
    catchup=False,
    tags=['API'],
    schedule = '30 6 * * 6'
) as dag:
    api_endpoint = "https://restcountries.com/v3/all"
    results = extract_transform(api_endpoint)
    load("ydj8952", "country_info", results)
