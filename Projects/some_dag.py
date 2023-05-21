from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

import datetime
import requests
import json

dag = DAG(
    dag_id='533_api_generate_report',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt':'2022-05-06'}


#запрос выгрузки файлов;
#в итоге вы получите строковый идентификатор задачи выгрузки - task_id;
#через некоторое время по другому пути сформируется ссылка на выгруженные файлы.
nickname = "dobrov"
cohort = "14"
api_token = "5f55e6c0-e9e5-4a9c-b313-63c01fc31460"

headers = {
    "X-API-KEY": api_token,
    "X-Nickname": nickname,
    "X-Cohort": cohort
}


def create_files_request( headers):
    api_conn = 'create_files_api'
    api_endpoint = 'd5dg1j9kt695d30blp03.apigw.yandexcloud.net'

    method_url = '/generate_report'
    r = requests.post('https://' + api_endpoint + method_url, headers=headers)
    response_dict = json.loads(r.content)


    print(f"task_id is {response_dict['task_id']}")
    return response_dict['task_id']

task = PythonOperator(task_id='create_files_request',
                                        python_callable= create_files_request,
                                        op_kwargs= None,
                                        dag=dag)


task