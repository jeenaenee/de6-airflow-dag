from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

def fetch_and_load_to_redshift():
    url = 'https://restcountries.com/v3/all'
    response = requests.get(url)
    data = response.json()

    rows = []
    for item in data:
        name = item.get("name", {}).get("official")
        population = item.get("population")
        area = item.get("area")
        if name and population is not None and area is not None:
            rows.append((name, population, area))

    # Redshift 연결
    conn = psycopg2.connect(
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        host=os.getenv("REDSHIFT_HOST"),
        port=os.getenv("REDSHIFT_PORT")
    )

    cur = conn.cursor()

    # 기존 테이블 제거 및 새로 생성 (public 스키마에 생성)
    cur.execute("DROP TABLE IF EXISTS restcountries;")
    cur.execute("""
        CREATE TABLE restcountries (
            country VARCHAR(255),
            population BIGINT,
            area FLOAT
        );
    """)

    # 데이터 삽입
    for row in rows:
        cur.execute("INSERT INTO restcountries (country, population, area) VALUES (%s, %s, %s);", row)

    conn.commit()
    cur.close()
    conn.close()

# DAG 정의
with DAG(
    dag_id='country_info_to_redshift',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='30 6 * * 6',  # 매주 토요일 6:30 UTC
    catchup=False
) as dag:

    load_task = PythonOperator(
        task_id='fetch_and_insert_data',
        python_callable=fetch_and_load_to_redshift
    )
