from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

import datetime
import requests
import pandas as pd
import os
import psycopg2, psycopg2.extras

dag = DAG(
    dag_id='552_postgresql_export_fuction',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt':'2022-05-06'}


# file_names = ['customer_research.csv', 'user_order_log.csv', 'user_activity_log.csv']

def load_file_to_pg(file_names):
    
    df = pd.read_csv(f"/lessons/5. Реализация ETL в Airflow/4. Extract как подключиться к хранилищу, чтобы получить файл/Задание 2/{file_names}", index_col=0 )

    cols = ','.join(list(df.columns))
    insert_stmt = f"INSERT INTO stage.{file_names.split('.')[0]} ({cols}) VALUES %s"

    pg_conn = psycopg2.connect("host='localhost' port='5432' dbname='de' user='jovyan' password='jovyan'")
    cur = pg_conn.cursor()

    psycopg2.extras.execute_values(cur, insert_stmt, df.values)
    pg_conn.commit()

    cur.close()
    pg_conn.close()

t_load_customer_research = PythonOperator(task_id='load_file_to_pg',
                                        python_callable= load_file_to_pg,
                                        op_kwargs={'file_names' : ['customer_research.csv'] },
                                        dag=dag)


t_load_user_activity_log = PythonOperator(task_id='user_activity_log',
                                        python_callable= load_file_to_pg,
                                        op_kwargs={'file_names' : ['user_activity_log.csv'] },
                                        dag=dag)


t_load_user_order_log = PythonOperator(task_id='user_order_log',
                                        python_callable= load_file_to_pg,
                                        op_kwargs={'file_names' : ['user_order_log.csv'] },
                                        dag=dag)

# t_load_file_to_pg = PythonOperator(task_id='load_file_to_pg',
#                                         python_callable=load_file_to_pg,
#                                         op_kwargs={'file_names' : ['customer_research.csv'
#                                                                 ,'user_activity_log.csv'
#                                                                 ,'user_order_log.csv']
#                                         },
#                                         dag=dag)