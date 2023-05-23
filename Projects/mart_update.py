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
    dag_id='583_postgresql_mart_update',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt':'2022-05-06'}

###POSTGRESQL settings###
#set postgresql connectionfrom basehook
pg_conn = BaseHook.get_connection('pg_connection')

##init test connection
conn = psycopg2.connect(f"dbname='de' port='{pg_conn.port}' user='{pg_conn.login}' host='{pg_conn.host}' password='{pg_conn.password}'")
cur = conn.cursor()
cur.close()
conn.close()


#3. обновление таблиц d по загруженным данным в staging-слой

def update_mart_d_tables(ti):
    #connection to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()
#
    #d_calendar
    cur.execute(''' 
                    DELETE FROM mart.d_calendar;
                    CREATE SEQUENCE date_id_sequence START 1 ;
                    INSERT INTO mart.d_calendar (
                        date_id, 
                        fact_date, 
                        day_num, 
                        month_num, 
                        month_name, 
                        year_num )
                    WITH all_dates AS (
                        SELECT date_id AS date
                        FROM stage.customer_research
                        UNION
                        SELECT date_time
                        FROM stage.user_activity_log 
                        UNION
                        SELECT date_time
                        FROM stage.user_order_log
                    )
                        SELECT 
                            NEXTVAL ('date_id_sequence')::int as date_id,
                            date::date,
                            DATE_PART ('day', date) as day_num,
                            DATE_PART ('month', date) as month_num,
                            TO_CHAR (date,'month') as month_name,
                            DATE_PART ('year', date) as year_num
                            FROM all_dates
                    --      FROM generate_series(date '2023-04-15 00:00:00.000',
                    --                           date '2023-05-07 00:00:00.000',
                    --                           interval '1 day')
                    --           AS t(date)
                    ;
                    DROP SEQUENCE date_id_sequence;
    ''')
    conn.commit()
# 
    #d_customer
    cur.execute(''' 
                    DELETE FROM mart.d_customer;
                    INSERT INTO mart.d_customer (
                        customer_id, 
                        first_name, 
                        last_name, 
                        city_id )
                    SELECT DISTINCT
                        customer_id, 
                        first_name, 
                        last_name, 
                        MAX(city_id) AS city_id
                    FROM stage.user_order_log
                    GROUP BY customer_id, first_name, last_name 
                    ;
    ''')
    conn.commit()
# 
    #d_item
    cur.execute(''' 
                    DELETE FROM mart.d_item;
                    INSERT INTO mart.d_item ( 
                        item_id, 
                        item_name)
                    SELECT DISTINCT
                        item_id, 
                        item_name
                    FROM stage.user_order_log
                    ;
    ''')
    conn.commit()
# 
    cur.close()
    conn.close()
# 
    return 200

#4. обновление витрин (таблицы f)

def update_mart_f_tables(ti):
    #connection to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()
# 
    #f_activity
    cur.execute('''
                    DELETE FROM mart.f_activity;
                    INSERT INTO mart.f_activity (
                        activity_id, 
                        date_id, 
                        click_number)
                    SELECT
                        ual.action_id,
                        dc.date_id,
                        SUM(quantity) AS click_number
                    FROM stage.user_activity_log AS ual
                    LEFT JOIN mart.d_calendar as dc
                        ON ual.date_time = dc.fact_date
                    GROUP BY ual.action_id,	dc.date_id
                    ;
    ''')
    conn.commit()
# 
    #f_daily_sales
    cur.execute('''
                    DELETE FROM mart.f_daily_sales;
                    INSERT INTO mart.f_daily_sales (
                        date_id, 
                        item_id, 
                        customer_id, 
                        price, 
                        quantity, 
                        payment_amount)
                    SELECT
                        dc.date_id,
                        item_id,
                        customer_id,
                        SUM (payment_amount) / SUM (quantity) AS price,
                        SUM (quantity),
                        SUM (payment_amount)
                    FROM stage.user_order_log AS uol
                    LEFT JOIN mart.d_calendar as dc
                        ON uol.date_time = dc.fact_date
                    GROUP BY dc.date_id, item_id, customer_id
                    ;
    ''')
    conn.commit()
# 
    cur.close()
    conn.close()
# 
    return 200

t_update_mart_d_tables = PythonOperator(task_id='update_mart_d_tables',
                                        python_callable=update_mart_d_tables,
                                        dag=dag)


t_update_mart_f_tables = PythonOperator(task_id='update_mart_f_tables',
                                        python_callable=update_mart_f_tables,
                                        dag=dag)


t_update_mart_d_tables >> t_update_mart_f_tables