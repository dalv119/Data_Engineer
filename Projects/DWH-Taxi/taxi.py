#!/usr/bin/python3

import psycopg2
import pandas as pd
import os
from datetime import datetime

# Создаем переменные с путями к папкам с данными

waybills_files_path = '/mnt/files/waybills'
payments_files_path = '/mnt/files/payments'

# Создание подключения к PostgreSQL

conn_src = psycopg2.connect(database = 'taxi',
                            host = 'de-edu-db.chronosavant.ru',
                            user = 'etl_tech_user',
                            password = 'etl_tech_user_password',
                            port = '5432')

conn_dwh = psycopg2.connect(database = 'dwh',
                            host = 'de-edu-db.chronosavant.ru',
                            user = 'dwh_oryol',
                            password = 'dwh_oryol_TQGyR5kq',
                            port = '5432')

# Отключение автокоммита
conn_src.autocommit = False
conn_dwh.autocommit = False

# Создание курсора
cursor_src = conn_src.cursor()
cursor_dwh = conn_dwh.cursor()

###>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
### 1. Очистка стейджинговых таблиц

cursor_dwh.execute( '''
                    DELETE FROM dwh_oryol.stg_drivers;
                    DELETE FROM dwh_oryol.stg_drivers_del;
                    
                    DELETE FROM dwh_oryol.stg_rides;
                    DELETE FROM dwh_oryol.stg_fact_rides;

                    DELETE FROM dwh_oryol.stg_car_pool;

                    DELETE FROM dwh_oryol.stg_movement;

                    DELETE FROM dwh_oryol.stg_payments;

                    DELETE FROM dwh_oryol.stg_waybills;
                    DELETE FROM dwh_oryol.stg_fact_waybills;

                    DELETE FROM dwh_oryol.stg_cars;
                    DELETE FROM dwh_oryol.stg_cars_del;

                    DELETE FROM dwh_oryol.stg_clients;
                    DELETE FROM dwh_oryol.stg_clients_del;

                    DELETE table dwh_oryol.stg_rep_clients_hist;
                    DELETE table dwh_oryol.stg_rep_clients_hist_del;

''')

#######################################################################################################
###  2. Загрузка stg (Захват данных из источника (измененных с момента последней загрузки) в стейджинг)

#----> stg_drivers -> Insert stg >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# Получаем дату последнего изменения из meta
cursor_dwh.execute( ''' SELECT max_UPDATE_dt
                        FROM dwh_oryol.meta
                        WHERE schema_name = 'dwh_oryol' 
                        AND table_name = 'dim_drivers_hist' ''' )

meta_drivers_date = cursor_dwh.fetchone()[0]

# Чтение данных из БД источника src в DataFrame
cursor_src.execute( ''' SELECT
                            driver_license,
                            first_name,
                            last_name,
                            middle_name,
                            driver_valid_to,
                            card_num,
                            update_dt,
                            birth_dt 
                        FROM main.drivers
                        WHERE update_dt > ( %s )''', [meta_drivers_date] )

records = cursor_src.fetchall()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )

# Запись DataFrame в таблицу базы данных dwh
cursor_dwh.executemany( ''' INSERT INTO dwh_oryol.stg_drivers (
                                driver_license,
                                first_name,
                                last_name,
                                middle_name,
                                driver_valid_to,
                                card_num,
                                update_dt,
                                birth_dt )
                            VALUES( %s, %s, %s, %s, %s, %s, %s, %s ) ''', df.values.tolist() )

#----> stg_rides -> Insert stg >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# Получаем дату последнего изменения из meta
cursor_dwh.execute( ''' SELECT max_UPDATE_dt
                        FROM dwh_oryol.meta
                        WHERE schema_name = 'dwh_oryol'
                        AND table_name = 'fact_rides' ''' )

meta_rides_date = cursor_dwh.fetchone()[0]

# Чтение данных из БД источника src в DataFrame
cursor_src.execute( ''' SELECT
                            ride_id,
                            dt,
                            client_phone,
                            card_num,
                            point_from,
                            point_to,
                            distance,
                            price
                        FROM main.rides
                        WHERE  dt > ( %s )''', [meta_rides_date] )

records = cursor_src.fetchall()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )

# Запись DataFrame в таблицу базы данных dwh
cursor_dwh.executemany( ''' INSERT INTO dwh_oryol.stg_rides (
                                ride_id,
                                dt,
                                client_phone,
                                card_num,
                                point_from,
                                point_to,
                                distance,
                                price )
                            VALUES( %s, %s, %s, %s, %s, %s, %s, %s ) ''', df.values.tolist() )

#----> stg_movement -> Insert stg >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# Получаем дату последнего изменения из meta
cursor_dwh.execute( ''' SELECT max_UPDATE_dt
                        FROM dwh_oryol.meta
                        WHERE schema_name = 'dwh_oryol'
                        AND table_name = 'stg_movement' ''')

meta_movement_date = cursor_dwh.fetchone()[0]

# Чтение данных из БД источника src в DataFrame
cursor_src.execute( ''' SELECT
                            movement_id,
                            car_plate_num,
                            ride,
                            event,
                            dt 
                        FROM main.movement
                        WHERE dt > ( %s ) ''', [meta_movement_date] )

records = cursor_src.fetchall()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )

# Запись DataFrame в таблицу базы данных dwh
cursor_dwh.executemany( ''' INSERT INTO dwh_oryol.stg_movement(
                                movement_id,
                                car_plate_num,
                                ride,
                                event,
                                dt )
                            VALUES ( %s, %s, %s, %s, %s ) ''', df.values.tolist() )

#----> stg_car_pool -> Insert stg >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# Получаем дату последнего изменения из meta
cursor_dwh.execute( ''' SELECT max_UPDATE_dt
                        FROM dwh_oryol.meta
                        WHERE schema_name = 'dwh_oryol'
                        AND table_name = 'stg_car_pool' ''' )

meta_car_pool_date = cursor_dwh.fetchone()[0]

# Чтение данных из БД источника src в DataFrame
cursor_src.execute( ''' SELECT
                            plate_num,
                            model,
                            revision_dt,
                            register_dt,
                            finished_flg,
                            update_dt 
                        FROM main.car_pool
                        WHERE update_dt > ( %s) ''', [meta_car_pool_date] )

records = cursor_src.fetchall()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )

# Запись DataFrame в таблицу базы данных dwh
cursor_dwh.executemany( ''' INSERT INTO dwh_oryol.stg_car_pool(
                                plate_num,
                                model,
                                revision_dt,
                                register_dt,
                                finished_flg,
                                update_dt )
                             VALUES( %s, %s, %s, %s, %s, %s ) ''', df.values.tolist() )

#----> stg_waybills -> Insert stg >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>	

# Ищем файл для обработки и читаем его в DataFrame
for f in os.listdir(waybills_files_path):
    df = pd.read_xml( f'{waybills_files_path}/{f}',
                     xpath='//waybills/waybill|//waybills/waybill/driver|//waybills/waybill/period')
    df['number'] = df['number'][0] # df.number.fillna(df.number.unique()[0]) заполняем 3 строки накладной её номером
    df = df[['number', 'license', 'car', 'start', 'stop', 'issuedt']]
# Запись DataFrame в таблицу базы данных dwh
    cursor_dwh.executemany( ''' INSERT INTO dwh_oryol.stg_waybills (
                                    waybill_num,
                                    driver_license,
                                    car_plate_num,
                                    work_start_dt,
                                    work_end_dt,
                                    issue_dt )
                                VALUES( %s, %s, %s, %s, %s, %s ) ''', df.values.tolist() )

#----> stg_payments -> Insert stg >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>	

# Получаем дату последнего изменения из meta
cursor_dwh.execute( ''' SELECT max_UPDATE_dt
                        FROM dwh_oryol.meta
                        WHERE schema_name = 'dwh_oryol'
                        AND table_name = 'fact_payments' ''')

meta_payments_date = cursor_dwh.fetchone()[0]

    # Отбираем файл для загрузки
payment_file_date = None
payment_file = None   
for f in sorted(os.listdir(payments_files_path)): # сортируем файлы по возрастанию
    file_date = ''.join( [n for n in f if n.isdigit()] ) # выбираем из названия цифры обозначающие дату в виде списка и преобразуем в строку
    payment_file_date = datetime.strptime(file_date, '%Y%m%d%H%M') # форматируем строку в дату
    if payment_file_date > meta_payments_date: # отбираем файлы с датой > даты последнего обновления в meta
        payment_file = f
        df = pd.read_csv(f'{payments_files_path}/{f}', sep = '\t', header = None)
        df[0] = pd.to_datetime(df[0], format="%d.%m.%Y %H:%M:%S") 
    # Определяем имена полей        
        # df = df[['transaction_dt', 'card_num', 'transaction_amt']]
    # Фильтрация вновь появившихся в источнике строк
        # df = df[df['transaction_dt'] > meta_payments_date]
    # Запись DataFrame в таблицу базы данных
        cursor_dwh.executemany( ''' INSERT INTO dwh_oryol.stg_payments(
                                    transaction_dt,
                                    card_num,
                                    transaction_amt 
                                    )
                                VALUES( %s, %s, %s ) ''', df.values.tolist() )


#####################################################################################################
### 3. Загрузка  в стейджинг ключей из источника полным срезом для вычисления удалений.

#----> stg_drivers_del -> Insert del >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>    

cursor_src.execute( ''' SELECT driver_license
                        FROM main.drivers ''' )

records = cursor_src.fetchall()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )

cursor_dwh.executemany( ''' INSERT INTO dwh_oryol.stg_drivers_del( 
	                            driver_license)
                            VALUES( %s ) ''', df.values.tolist() )

#----> stg_cars_del -> Insert del >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>    

cursor_src.execute( ''' SELECT DISTINCT plate_num
                        FROM main.car_pool ''' )

records = cursor_src.fetchall()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )

cursor_dwh.executemany( ''' 
                    INSERT INTO dwh_oryol.stg_cars_del ( 
                        plate_num )
                    VALUES( %s ) ''', df.values.tolist() )
 
#----> stg_clients_del -> Insert del >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>    

cursor_src.execute( ''' SELECT DISTINCT client_phone
                        FROM main.rides ''' )

records = cursor_src.fetchmany()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )

cursor_dwh.executemany( ''' INSERT INTO dwh_oryol.stg_clients_del ( phone_num )
                            VALUES( %s ) ''', df.values.tolist() )

#####################################################################################################
### 4. Загрузка dim ( Загрузка в приемник новых позиций (формат SCD2) )

#----> dim_drivers_hist -> Insert new positions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.dim_drivers_hist (
                        start_dt,
                        last_name,
                        first_name,
                        middle_name,
                        birth_dt,
                        card_num,
                        driver_license_num,
                        driver_license_dt,
                        deleted_flag,
                        end_dt,
                        processed_dt
                    )
                    SELECT 
                        stg.update_dt AS start_dt,
                        stg.last_name,
                        stg.first_name,
                        stg.middle_name,
                        stg.birth_dt, 
                        stg.card_num,
                        stg.driver_license,
                        stg.driver_valid_to,
                        'N' AS deleted_flag,
                        to_date('2999-12-31', 'YYYY-MM-DD') AS end_dt,
                        NOW() AS processed_dt
                    FROM dwh_oryol.stg_drivers AS stg
                    LEFT JOIN dwh_oryol.dim_drivers_hist AS dim
                    ON stg.driver_license = dim.driver_license_num
                        AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                        AND dim.deleted_flag = 'N'
                    WHERE dim.driver_license_num IS NULL 
                    ;
''' )

#----> dim_cars -> Insert new positions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( ''' 
                    INSERT INTO dwh_oryol.dim_cars_hist (
                        plate_num,
                        start_dt,
                        model_name,
                        revision_dt,
                        deleted_flag,
                        end_dt,
                        processed_dt
                    )
                    SELECT 
                        stg.plate_num,
                        stg.update_dt AS start_dt,
                        stg.model,
                        stg.revision_dt,
                        'N' AS deleted_flag,
                        to_date('2999-12-31', 'YYYY-MM-DD') AS end_dt,
                        NOW() AS processed_dt
                    FROM dwh_oryol.stg_car_pool AS stg
                    LEFT JOIN dwh_oryol.dim_cars_hist AS dim
                    ON 1 = 1 
                        AND stg.plate_num = dim.plate_num 
                        AND stg.model = dim.model_name
                        AND stg.update_dt = dim.start_dt
                        AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                        AND dim.deleted_flag = 'N'
                    WHERE dim.plate_num IS NULL 
                    ;
''' )                    

#----> dim_clients -> Insert new positions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.dim_clients_hist (
                        phone_num,
                        start_dt,
                        card_num,
                        deleted_flag,
                        end_dt,
                        processed_dt
                    )
                    SELECT DISTINCT
                        stg.client_phone,
                        NOW() AS start_dt,
                        REPLACE(stg.card_num, ' ',''),
                        'N' AS deleted_flag,
                        to_date('2999-12-31', 'YYYY-MM-DD') AS end_dt,
                        NOW() AS processed_dt
                    FROM dwh_oryol.stg_rides AS stg
                    LEFT JOIN dwh_oryol.dim_clients_hist AS dim
                    ON 1 = 1 
                        AND stg.client_phone = dim.phone_num 
                        AND stg.card_num = dim.card_num
                        AND stg.dt = dim.start_dt
                        AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                        AND dim.deleted_flag = 'N'
                    WHERE dim.phone_num IS NULL
                    ;
''' )

#####################################################################################################
### 5. Обновление dim  ( Фиксация даты end_dt для измененной строки и загрузка обновленной строки 
#                        с актуальной датой end_dt (формат SCD2) )

#----> dim_drivers_hist -> Update dim (fix + add) >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( ''' 
                    UPDATE dwh_oryol.dim_drivers_hist AS d
                    SET 
                        end_dt = tmp.update_dt - INTERVAL '15 second',
                        processed_dt = tmp.processed_dt
                    FROM (
                        SELECT 
                            stg.last_name,
                            stg.first_name,
                            stg.middle_name,
                            stg.birth_dt, 
                            stg.driver_license,
                            stg.update_dt,
                            NOW() AS processed_dt
                        FROM dwh_oryol.stg_drivers AS stg
                        INNER JOIN dwh_oryol.dim_drivers_hist AS dim
                        ON stg.driver_license = dim.driver_license_num
                            AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                            AND dim.deleted_flag = 'N'
                        WHERE 1 = 0
                            OR stg.last_name <> dim.last_name OR (stg.last_name IS NULL AND dim.last_name IS NOT NULL)
                                                              OR (stg.last_name IS NOT NULL AND dim.last_name IS NULL)
                            OR stg.first_name <> dim.first_name OR (stg.first_name IS NULL AND dim.first_name IS NOT NULL)
                                                                OR (stg.first_name IS NOT NULL AND dim.first_name IS NULL)
                            OR stg.middle_name <> dim.middle_name OR (stg.middle_name IS NULL AND dim.middle_name IS NOT NULL)
                                                                  OR (stg.middle_name IS NOT NULL AND dim.middle_name IS NULL)
                            OR stg.birth_dt <> dim.birth_dt OR (stg.birth_dt IS NULL AND dim.birth_dt IS NOT NULL)
                                                            OR (stg.birth_dt IS NOT NULL AND dim.birth_dt IS NULL)
                            OR stg.card_num <> dim.card_num OR (stg.card_num IS NULL AND dim.card_num IS NOT NULL)
                                                            OR (stg.card_num IS NOT NULL AND dim.card_num IS NULL)
                            OR stg.driver_license <> dim.driver_license_num OR (stg.driver_license IS NULL AND dim.driver_license_num IS NOT NULL)
                                                                        OR (stg.driver_license IS NOT NULL AND dim.driver_license_num IS NULL)                                  
                            OR stg.driver_valid_to <> dim.driver_license_dt OR (stg.driver_valid_to IS NULL AND dim.driver_license_dt IS NOT NULL)
                                                                          OR (stg.driver_valid_to IS NOT NULL AND dim.driver_license_dt IS NULL)
                        ) tmp
                    WHERE 1 = 1 
                        AND d.first_name = tmp.first_name
                        AND d.last_name = tmp.last_name
                        AND d.middle_name = tmp.middle_name
                        AND d.birth_dt = tmp.birth_dt
                        AND d.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                    ;
''' )

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.dim_drivers_hist (
                        personnel_num,
                        start_dt,
                        last_name,
                        first_name,
                        middle_name,
                        birth_dt,
                        card_num,
                        driver_license_num,
                        driver_license_dt,
                        deleted_flag,
                        end_dt,
                        processed_dt )
                    SELECT DISTINCT
                        dim.personnel_num,
                        stg.update_dt AS start_dt,
                        stg.last_name,
                        stg.first_name,
                        stg.middle_name,
                        stg.birth_dt, 
                        stg.card_num,
                        stg.driver_license,
                        stg.driver_valid_to,
                        'N' AS deleted_flag,
                        to_date('2999-12-31', 'YYYY-MM-DD') AS end_dt,
                        NOW() AS processed_dt
                    FROM dwh_oryol.stg_drivers AS stg
                    INNER JOIN dwh_oryol.dim_drivers_hist AS dim
                    ON stg.driver_license = dim.driver_license_num
                        AND dim.end_dt = stg.update_dt - INTERVAL '15 second'
                        AND dim.deleted_flag = 'N'
                    WHERE 1 = 0
                        OR stg.last_name <> dim.last_name OR (stg.last_name IS NULL AND dim.last_name IS NOT NULL)
                                                            OR (stg.last_name IS NOT NULL AND dim.last_name IS NULL)
                        OR stg.first_name <> dim.first_name OR (stg.first_name IS NULL AND dim.first_name IS NOT NULL)
                                                            OR (stg.first_name IS NOT NULL AND dim.first_name IS NULL)
                        OR stg.middle_name <> dim.middle_name OR (stg.middle_name IS NULL AND dim.middle_name IS NOT NULL)
                                                                OR (stg.middle_name IS NOT NULL AND dim.middle_name IS NULL)
                        OR stg.birth_dt <> dim.birth_dt OR (stg.birth_dt IS NULL AND dim.birth_dt IS NOT NULL)
                                                        OR (stg.birth_dt IS NOT NULL AND dim.birth_dt IS NULL)
                        OR stg.card_num <> dim.card_num OR (stg.card_num IS NULL AND dim.card_num IS NOT NULL)
                                                        OR (stg.card_num IS NOT NULL AND dim.card_num IS NULL)
                        OR stg.driver_license <> dim.driver_license_num OR (stg.driver_license IS NULL AND dim.driver_license_num IS NOT NULL)
                                                                    OR (stg.driver_license IS NOT NULL AND dim.driver_license_num IS NULL)                                  
                        OR stg.driver_valid_to <> dim.driver_license_dt OR (stg.driver_valid_to IS NULL AND dim.driver_license_dt IS NOT NULL)
                                                                        OR (stg.driver_valid_to IS NOT NULL AND dim.driver_license_dt IS NULL)
                    ;
''')

#----> dim_cars_hist -> Update dim (fix + add) >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    UPDATE dwh_oryol.dim_cars_hist AS d
                    SET
                        end_dt = tmp.start_dt - INTERVAL '15 second'
                    FROM (
                        SELECT 
                            stg.plate_num,
                            stg.update_dt AS start_dt,
                            stg.model,
                            stg.revision_dt,
                            'N' AS deleted_flag,
                            to_date('2999-12-31', 'YYYY-MM-DD') AS end_dt,
                            NOW() AS processed_dt
                        FROM dwh_oryol.stg_car_pool AS stg
                        INNER JOIN dwh_oryol.dim_cars_hist AS dim
                        ON 1 = 1 
                            AND stg.plate_num = dim.plate_num 
                            AND stg.model = dim.model_name
                            AND stg.update_dt = dim.start_dt
                            AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                            AND dim.deleted_flag = 'N'
                        WHERE 1 = 0
                            OR stg.plate_num <> dim.plate_num OR (stg.plate_num IS NULL AND dim.plate_num IS NOT NULL)
                                                              OR (stg.plate_num IS NOT NULL AND dim.plate_num IS NULL)
                            OR stg.model <> dim.model_name OR (stg.model IS NULL AND dim.model_name IS NOT NULL)
                                                            OR (stg.model IS NOT NULL AND dim.model_name IS NULL)
                            OR stg.revision_dt <> dim.revision_dt OR (stg.revision_dt IS NULL AND dim.revision_dt IS NOT NULL)
                                                            OR (stg.revision_dt IS NOT NULL AND dim.revision_dt IS NULL)
                        ) tmp
                    WHERE 1 = 1
                            AND d.plate_num = tmp.plate_num 
                            AND d.model_name = tmp.model
                            AND d.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                    ;
''')

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.dim_cars_hist (
                        plate_num,
                        start_dt,
                        model_name,
                        revision_dt,
                        deleted_flag,
                        end_dt,
                        processed_dt )
                    SELECT DISTINCT
                        stg.plate_num,
                        stg.update_dt AS start_dt,
                        stg.model,
                        stg.revision_dt,
                        'N' AS deleted_flag,
                        to_date('2999-12-31', 'YYYY-MM-DD') AS end_dt,
                        NOW() AS processed_dt
                    FROM dwh_oryol.stg_car_pool AS stg
                    INNER JOIN dwh_oryol.dim_cars_hist AS dim
                    ON 1 = 1 
                        AND stg.plate_num = dim.plate_num 
                        AND stg.model = dim.model_name
                        AND stg.update_dt = dim.start_dt
                        AND dim.end_dt = stg.update_dt - INTERVAL '15 second'
                        AND dim.deleted_flag = 'N'
                    WHERE 1 = 0
                        OR stg.plate_num <> dim.plate_num OR (stg.plate_num IS NULL AND dim.plate_num IS NOT NULL)
                                                            OR (stg.plate_num IS NOT NULL AND dim.plate_num IS NULL)
                        OR stg.model <> dim.model_name OR (stg.model IS NULL AND dim.model_name IS NOT NULL)
                                                            OR (stg.model IS NOT NULL AND dim.model_name IS NULL)
                        OR stg.revision_dt <> dim.revision_dt OR (stg.revision_dt IS NULL AND dim.revision_dt IS NOT NULL)
                                                        OR (stg.revision_dt IS NOT NULL AND dim.revision_dt IS NULL)
                    ;
''')

#----> dim_clients_hist -> Update dim (fix + add) >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    UPDATE dwh_oryol.dim_clients_hist  AS d
                    SET 
                        end_dt = tmp.start_dt - INTERVAL '15 second'
                    FROM (
                         SELECT DISTINCT
                            stg.client_phone,
                            NOW() AS start_dt,
                            stg.card_num,
                            'N' AS deleted_flag,
                            to_date('2999-12-31', 'YYYY-MM-DD') AS end_dt,
                            NOW() AS processed_dt
                        FROM dwh_oryol.stg_rides AS stg
                        INNER JOIN dwh_oryol.dim_clients_hist AS dim
                        ON 1 = 1 
                            AND stg.client_phone = dim.phone_num 
                            AND stg.card_num = dim.card_num
                            AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                            AND dim.deleted_flag = 'N'
                        WHERE 1 = 0 
                            OR stg.client_phone <> dim.phone_num OR (stg.client_phone IS NULL AND dim.phone_num IS NOT NULL)
                                                            OR (stg.client_phone IS NOT NULL AND dim.phone_num IS NULL)
                            OR stg.card_num <> dim.card_num OR (stg.card_num IS NULL AND dim.card_num IS NOT NULL)
                                                            OR (stg.card_num IS NOT NULL AND dim.card_num IS NULL) 
                        ) tmp
                    WHERE 1 = 1
                        AND d.phone_num = tmp.client_phone 
                        AND d.card_num = tmp.card_num
                        AND d.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                        ;
''')

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.dim_clients_hist (
                        phone_num,
                        start_dt,
                        card_num,
                        deleted_flag,
                        end_dt,
                        processed_dt )
                     SELECT DISTINCT
                        stg.client_phone,
                        NOW() AS start_dt,
                        stg.card_num,
                        'N' AS deleted_flag,
                        to_date('2999-12-31', 'YYYY-MM-DD') AS end_dt,
                        NOW() AS processed_dt
                    FROM dwh_oryol.stg_rides AS stg
                    INNER JOIN dwh_oryol.dim_clients_hist AS dim
                    ON 1 = 1 
                        AND stg.client_phone = dim.phone_num 
                        AND stg.card_num = dim.card_num
                        AND dim.end_dt = stg.dt - INTERVAL '15 second'
                        AND dim.deleted_flag = 'N'
                    WHERE 1 = 0 
                        OR stg.client_phone <> dim.phone_num OR (stg.client_phone IS NULL AND dim.phone_num IS NOT NULL)
                                                        OR (stg.client_phone IS NOT NULL AND dim.phone_num IS NULL)
                        OR stg.card_num <> dim.card_num OR (stg.card_num IS NULL AND dim.card_num IS NOT NULL)
                                                        OR (stg.card_num IS NOT NULL AND dim.card_num IS NULL) 
                    ;
''' )

#####################################################################################################
### 6. Загрузка fact

#----> stg_fact_rides -> INSERT stg_fact >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.stg_fact_rides (
                        ride_id,
                        point_from_txt,
                        point_to_txt,
                        distance_val,
                        price_amt,
                        client_phone_num,
                        car_plate_num,
                        ride_arrival_dt,
                        ride_start_dt,
                        ride_end_dt )
                    SELECT 
                        grp.ride_id,
                        grp.point_from_txt,
                        grp.point_to_txt,
                        grp.distance_val,
                        grp.price_amt,
                        grp.client_phone_num,
                        grp.car_plate_num,
                        grp.ride_arrival_dt,
                        grp.ride_start_dt,
                        grp.ride_end_dt 
                    FROM (
                        SELECT 
                            r.ride_id,
                            MAX ( r.point_from ) AS point_from_txt,
                            MAX ( r.point_to ) AS point_to_txt,
                            MAX ( r.distance ) AS distance_val,
                            MAX ( r.price ) AS price_amt,
                            MAX ( r.client_phone ) AS client_phone_num,
                            MAX ( m.car_plate_num ) AS car_plate_num,
                            MAX ( CASE WHEN event = 'READY' THEN m.dt END ) AS ride_arrival_dt,
                            MAX ( CASE WHEN event = 'BEGIN' THEN m.dt END ) AS ride_start_dt,
                            MAX ( CASE WHEN (event = 'END' OR event = 'CANCEL') THEN m.dt END ) AS ride_end_dt
                        FROM dwh_oryol.stg_rides AS r
                        LEFT JOIN dwh_oryol.stg_movement AS m
                        ON r.ride_id = m.ride 
                        GROUP BY r.ride_id
                        ) grp
                    LEFT JOIN dwh_oryol.stg_fact_rides AS rf
                        ON grp.ride_id = rf.ride_id  
                    WHERE 
                        rf.ride_id IS NULL AND 
                        grp.ride_end_dt IS NOT NULL
                    ;
''')

#----> stg_fact_waybills -> INSERT stg_fact >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.stg_fact_waybills (
                        waybill_num,
                        driver_pers_num,
                        car_plate_num,
                        work_start_dt,
                        work_end_dt,
                        issue_dt )
                    SELECT 
                        grp.waybill_num,
                        grp.driver_pers_num,
                        grp.car_plate_num,
                        grp.work_start_dt,
                        grp.work_end_dt,
                        grp.issue_dt 
                    FROM (
                        SELECT 
                            w.waybill_num,
                            MAX ( d.personnel_num ) AS driver_pers_num,
                            MAX ( w.car_plate_num ) AS car_plate_num,
                            MAX ( w.work_start_dt ) AS work_start_dt,
                            MAX ( w.work_end_dt ) AS work_end_dt,
                            MAX ( w.issue_dt ) AS issue_dt
                        FROM dwh_oryol.stg_waybills AS w
                        LEFT JOIN dwh_oryol.dim_drivers_hist d
                            ON w.driver_license = d.driver_license_num
                        GROUP BY w.waybill_num
                        ) AS grp
                    LEFT JOIN dwh_oryol.stg_fact_waybills AS sf
                        ON grp.waybill_num = sf.waybill_num
                    WHERE sf.waybill_num IS NULL
                    ;
''')

#----> fact_waybills -> INSERT fact >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.fact_waybills (
                        waybill_num,
                        driver_pers_num,
                        car_plate_num,
                        work_start_dt,
                        work_end_dt,
                        issue_dt,
                        processed_dt )
                    SELECT
                        grp.waybill_num,
                        grp.driver_pers_num,
                        grp.car_plate_num,
                        grp.work_start_dt,
                        grp.work_end_dt,
                        grp.issue_dt,
                        NOW () AS processed_dt
                    FROM dwh_oryol.stg_fact_waybills AS grp
                    LEFT JOIN dwh_oryol.fact_waybills AS fact
                        ON grp.waybill_num = fact.waybill_num
                    WHERE fact.waybill_num IS NULL
                    ;
''')

#----> fact_rides -> INSERT fact >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.fact_rides (
                        ride_id,
                        point_from_txt,
                        point_to_txt,
                        distance_val,
                        price_amt,
                        client_phone_num,
                        driver_pers_num,
                        car_plate_num,
                        ride_arrival_dt,
                        ride_start_dt,
                        ride_end_dt,
                        processed_dt )
                    SELECT 
                        grp.ride_id,
                        grp.point_from_txt,
                        grp.point_to_txt,
                        grp.distance_val,
                        grp.price_amt,
                        grp.client_phone_num,
                        dpn.driver_pers_num,
                        grp.car_plate_num,
                        grp.ride_arrival_dt,
                        grp.ride_start_dt,
                        grp.ride_end_dt,
                        NOW () AS processed_dt
                    FROM dwh_oryol.stg_fact_rides AS grp
                    LEFT JOIN (
                        SELECT
                            driver_pers_num,
                            ride_id
                        FROM (
                            SELECT
                                driver_pers_num,
                                ride_id,
                                ROW_NUMBER () OVER ( PARTITION BY ride_id ORDER BY work_start_dt) AS rk
                            FROM dwh_oryol.stg_fact_rides AS sfr
                            LEFT JOIN dwh_oryol.fact_waybills AS fw
                                ON sfr.car_plate_num = fw.car_plate_num
                                AND sfr.ride_arrival_dt BETWEEN fw.work_start_dt AND fw.work_end_dt
                            ) AS ranked
                        WHERE ranked.rk <= 1
                        ) AS dpn
                        ON grp.ride_id = dpn.ride_id
                    LEFT JOIN dwh_oryol.fact_rides AS fact
                        ON grp.ride_id = fact.ride_id
                    WHERE 
                        fact.ride_id IS NULL AND 
                        grp.ride_end_dt IS NOT NULL
                    ;
''')

#----> fact_payments -> INSERT fact >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.fact_payments (
                        card_num,
                        transaction_amt,
                        transaction_dt,
                        processed_dt )
                    SELECT DISTINCT
                        stg.card_num,
                        stg.transaction_amt,
                        stg.transaction_dt,
                        NOW () AS processed_dt
                    FROM dwh_oryol.stg_payments AS stg
                    LEFT JOIN dwh_oryol.fact_payments AS fact
                        ON stg.card_num = fact.card_num AND
                           stg.transaction_dt = fact.transaction_dt
                    WHERE fact.transaction_dt IS NULL
                    ;
''')

#####################################################################################################
### 7. Загрузка Deleted (Загрузка в приемник "удаленных" строк на источнике (формат SCD2))

#---->  dim_drivers_hist -> Insert Deleted >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.dim_drivers_hist (
                        personnel_num,
                        start_dt,
                        last_name,
                        first_name,
                        middle_name,
                        birth_dt,
                        card_num,
                        driver_license_num,
                        driver_license_dt,
                        deleted_flag,
                        end_dt,
                        processed_dt )
                    SELECT
                        dim.personnel_num,
                        NOW () AS start_dt,
                        dim.last_name,
                        dim.first_name,
                        dim.middle_name,
                        dim.birth_dt,
                        dim.card_num,
                        dim.driver_license_num,
                        dim.driver_license_dt,
                        'Y' AS deleted_flag,
                        to_date('2999-12-31', 'YYYY-MM-DD') AS end_dt,
                        NOW () AS processed_dt
                    FROM dwh_oryol.dim_drivers_hist AS dim
                    WHERE 
                        dim.personnel_num IN (
                        SELECT
                            dim_2.personnel_num
                        FROM dwh_oryol.dim_drivers_hist AS dim_2
                        LEFT JOIN dwh_oryol.stg_drivers_del AS stg_d
	                        ON dim_2.driver_license_num = stg_d.driver_license 
                        WHERE dim_2.driver_license_num IS NULL
                            AND  dim_2.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                            AND dim_2.deleted_flag = 'N'
                        )
                    AND  dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                    AND dim.deleted_flag = 'N'
                    ;
''')

cursor_dwh.execute( '''
                    UPDATE dwh_oryol.dim_drivers_hist AS dim
                    SET
                        end_dt = NOW () - INTERVAL '15 second',
                        processed_dt = NOW ()
                    WHERE 
                        dim.personnel_num IN (
                        SELECT
                            dim_2.personnel_num
                        FROM dwh_oryol.dim_drivers_hist AS dim_2
                        LEFT JOIN dwh_oryol.stg_drivers_del AS stg_d
	                        ON dim_2.driver_license_num = stg_d.driver_license 
                        WHERE dim_2.driver_license_num IS NULL
                            AND  dim_2.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                            AND dim_2.deleted_flag = 'N'
                        )
                    AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                    AND dim.deleted_flag = 'N'
                    ; 
''')

#---->  dim_cars_hist -> Insert Deleted >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.dim_cars_hist (
                        plate_num,
                        start_dt,
                        model_name,
                        revision_dt, 
                        deleted_flag,
                        end_dt,
                        processed_dt )
                    SELECT 
                        dim.plate_num,
                        NOW () AS start_dt,
                        dim.model_name,
                        dim.revision_dt,
                        'Y' AS deleted_flag,
                        to_date('2999-12-31', 'YYYY-MM-DD') AS end_dt,
                        NOW () AS processed_dt
                    FROM dwh_oryol.dim_cars_hist AS dim
                    WHERE 
                        dim. plate_num IN (
                        SELECT
                            dim_2.plate_num
                        FROM dwh_oryol.dim_cars_hist AS dim_2
                        LEFT JOIN dwh_oryol.stg_cars_del AS stg_d
                            ON dim_2.plate_num = stg_d.plate_num
                        WHERE
                            dim_2.plate_num IS NULL 
                            AND dim_2.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                            AND dim_2.deleted_flag = 'N'
                        )
                    AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                    AND dim.deleted_flag = 'N'
                    ;
''')	 

cursor_dwh.execute( '''
                    UPDATE dwh_oryol.dim_cars_hist AS dim
                    SET
                        end_dt = NOW () - INTERVAL '15 second',
                        processed_dt = NOW ()
                    WHERE 
                        dim. plate_num IN (
                        SELECT
                            dim_2.plate_num
                        FROM dwh_oryol.dim_cars_hist AS dim_2
                        LEFT JOIN dwh_oryol.stg_cars_del AS stg_d
                            ON dim_2.plate_num = stg_d.plate_num
                        WHERE
                            dim_2.plate_num IS NULL 
                            AND dim_2.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                            AND dim_2.deleted_flag = 'N'
                        )
                    AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                    AND dim.deleted_flag = 'N'
                    ;
''')

#---->  dim_clients_hist -> Insert Deleted >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.dim_clients_hist (
                        phone_num,
                        start_dt,
                        card_num,
                        deleted_flag,
                        end_dt,
                        processed_dt )
                    SELECT 
                        dim.phone_num,
                        NOW () AS start_dt,
                        dim.card_num,
                        'Y' AS deleted_flag,
                        to_date('2999-12-31', 'YYYY-MM-DD') AS end_dt,
                        NOW () AS processed_dt
                    FROM dwh_oryol.dim_clients_hist AS dim
                    WHERE phone_num IN (
                        SELECT
                            dim_2.phone_num
                        FROM dwh_oryol.dim_clients_hist AS dim_2
                        LEFT JOIN dwh_oryol.stg_clients_del AS stg_d
                            ON  dim_2.phone_num = stg_d.phone_num
                        WHERE
                            dim_2.phone_num IS NULL
                            AND dim_2.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                            AND dim_2.deleted_flag = 'N'
                        )
                    AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                    AND dim.deleted_flag = 'N'
                    ;
''')

cursor_dwh.execute( '''
                    UPDATE dwh_oryol.dim_clients_hist AS dim
                    SET
                        end_dt = now () - INTERVAL '15 second'
                    WHERE dim.phone_num IN (
                        SELECT
                            dim_2.phone_num
                        FROM dwh_oryol.dim_clients_hist AS dim_2
                        LEFT JOIN dwh_oryol.stg_clients_del AS stg_d
                            ON  dim_2.phone_num = stg_d.phone_num
                        WHERE
                            dim_2.phone_num IS NULL
                            AND dim_2.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                            AND dim_2.deleted_flag = 'N'
                        )
                    AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                    AND dim.deleted_flag = 'N'
                    ;
''')

#####################################################################################################
### 8. Обновление метаданных

#---->  drivers -> Meta >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    UPDATE dwh_oryol.meta
                    SET max_UPDATE_dt = COALESCE (
                        ( SELECT MAX ( update_dt ) FROM dwh_oryol.stg_drivers ),
                        ( SELECT max_UPDATE_dt FROM dwh_oryol.meta 
                          WHERE schema_name = 'dwh_oryol' AND table_name = 'dim_drivers_hist')
                    )
                    WHERE schema_name = 'dwh_oryol' AND table_name = 'dim_drivers_hist'
                    ;
''')

#---->  cars -> Meta >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    UPDATE dwh_oryol.meta
                    SET max_UPDATE_dt = COALESCE (
                        ( SELECT MAX ( update_dt ) FROM dwh_oryol.stg_car_pool),
                        ( SELECT max_UPDATE_dt FROM dwh_oryol.meta
                          WHERE schema_name = 'dwh_oryol' AND table_name = 'stg_car_pool')
                    )
                    WHERE schema_name = 'dwh_oryol' AND table_name = 'stg_car_pool'
                    ;
''')

#---->  clients & rides -> Meta >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    UPDATE dwh_oryol.meta
                    SET max_UPDATE_dt = COALESCE (
                        ( SELECT MAX ( dt ) FROM dwh_oryol.stg_rides),
                        ( SELECT max_UPDATE_dt FROM dwh_oryol.meta 
                          WHERE schema_name = 'dwh_oryol' AND table_name = 'fact_rides')
                    )
                    WHERE schema_name = 'dwh_oryol' AND table_name = 'fact_rides'
                    ;
''')
                   
#---->  movement -> Meta >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    UPDATE dwh_oryol.meta
                    SET max_UPDATE_dt = COALESCE (
                        ( SELECT MAX ( dt ) FROM stg_movement ),
                        ( SELECT max_UPDATE_dt FROM dwh_oryol.meta 
                          WHERE schema_name = 'dwh_oryol' AND table_name = 'stg_movement') 
                    )
                    WHERE schema_name = 'dwh_oryol' AND table_name = 'stg_movement'
                    ;
''')
                   
#---->  payments -> Meta >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    UPDATE dwh_oryol.meta
                    SET max_UPDATE_dt = COALESCE (                   
                        ( SELECT MAX ( transaction_dt ) FROM dwh_oryol.stg_payments ),
                        ( SELECT max_UPDATE_dt FROM dwh_oryol.meta 
                          WHERE schema_name = 'dwh_oryol' AND table_name = 'fact_payments') 
                    )
                    WHERE schema_name = 'dwh_oryol' AND table_name = 'fact_payments'
                    ;
''')
                   
#---->  waybills -> Meta >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    UPDATE dwh_oryol.meta
                    SET max_UPDATE_dt = COALESCE (                   
                        ( SELECT MAX ( issue_dt ) FROM dwh_oryol.stg_waybills ),
                        ( SELECT max_UPDATE_dt FROM dwh_oryol.meta 
                          WHERE schema_name = 'dwh_oryol' AND table_name = 'fact_waybills') 
                    )
                    WHERE schema_name = 'dwh_oryol' AND table_name = 'fact_waybills'
                    ;
''')

#####################################################################################################
###  9. Создание ветрины отчетов по мошенническим операциям 

# определяем дату составления отчетов как дату загруженной исходной таблицы 'rides' на основе которой составлен отчет

cursor_dwh.execute( ''' SELECT max_UPDATE_dt FROM dwh_oryol.meta 
                        WHERE schema_name = 'dwh_oryol' AND table_name = 'fact_rides' ''')

report_dt = cursor_dwh.fetchone()[0]

#----> 9.1 Выплата водителям
#       -- услуги сервиса - 20% от стоимости поездки,
#       -- стоимость топливо - 47,26 * 7 * дистанция / 100,
#       -- амортизация машины 5 руб / км * дистанция,
#       -- остаток достается водителю. 
#          ● Атрибуты таблицы:
#          ○ personnel_num - табельный номер водителя,
#          ○ last_name - фамилия,
#          ○ first_name - имя,
#          ○ middle_name - отчество,
#          ○ card_num - номер карты водителя,
#          ○ amount - сумма к выплате,
#          ○ report_dt - дата, на которую построен отчет.

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.rep_drivers_payments (
                        personnel_num,
                        last_name,
                        first_name,
                        middle_name,
                        card_num,
                        amount,
                        report_dt)
                    SELECT
                        d.personnel_num,
                        MAX(d.last_name) AS last_name,
                        MAX(d.first_name) AS first_name,
                        MAX(d.middle_name) AS middle_name,
                        MAX(d.card_num) AS card_num,
                        SUM(r.price_amt) * 0.8 - (47.26 * 7 * SUM(r.distance_val) / 100) - 5 * SUM(r.distance_val) AS amount,
                        DATE_TRUNC('day', r.ride_arrival_dt) AS report_dt
                    FROM dwh_oryol.fact_rides AS r
                    LEFT JOIN dwh_oryol.dim_drivers_hist AS d
                        ON d.personnel_num  = r.driver_pers_num
                    WHERE 
                        ride_start_dt IS NOT NULL AND -- усовие: не было отмены поездки
                        ride_arrival_dt::date = TO_DATE('2023-01-18', 'YYYY-MM-DD') --> дата для проекта, т.к. данные в базе < now()
                    --	ride_arrival_dt BETWEEN NOW()::date - INTERVAL '1 day' AND NOW()::date - INTERVAL '1 second' --> для актуальных данных, если они будут в источнике
                    GROUP BY d.personnel_num, DATE_TRUNC('day', r.ride_arrival_dt)
                    ;	
''')

#----> 9.2 Водители-нарушители
#       -- завершил поездку со средней скоростью более 85 км/ч.
#			● Атрибуты:
#			○ personnel_num - табельный номер водителя,
#			○ ride - идентификатор поездки,
#			○ speed - средняя скорость,
#			○ violations_cnt - количество таких нарушений, выявленных ранее,
#			  накопленной суммой. Первое нарушение - значение 1
#			○ report_dt - дата, на которую построен отчет.

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.rep_drivers_violations (
                        personnel_num,
                        ride,
                        speed,
                        violations_cnt,
                        report_dt 
                    )
                    WITH cte_drive AS (	
                        SELECT
                            driver_pers_num AS personnel_num,
                            ride_id AS ride,
                            distance_val,
                            ride_arrival_dt,
                            ride_start_dt,
                            ride_end_dt,
                            (date_part('hour', ride_end_dt::timestamp - ride_start_dt::timestamp)*60 +
                            date_part('minute', ride_end_dt::timestamp - ride_start_dt::timestamp))*60 +
                            date_part('second', ride_end_dt::timestamp - ride_start_dt::timestamp) as ride_sec,
                            r.ride_arrival_dt::date AS report_dt	
                        FROM dwh_oryol.fact_rides AS r
                        WHERE
                            r.ride_start_dt IS NOT NULL -- # т.е. не было отмены поездки
                    ),
                    cte_viol_hist AS (  --> # определение количества нарушений до даты отчета
                        SELECT DISTINCT
                            personnel_num,
                            COUNT(ride) OVER (PARTITION BY personnel_num ) AS violations_cnt,
                            TO_DATE('2022-12-29', 'YYYY-MM-DD') AS report_dt -- # дата отчета высталяется вручную, т.к. на текущий период нет данных в БД
                        FROM cte_drive
                        WHERE 
                            ROUND(distance_val / (ride_sec)*60*60) > 85 AND 
                            ride_sec > 0 AND
                            ride_arrival_dt::date < TO_DATE('2022-12-29', 'YYYY-MM-DD') --> # нарушений до даты отчета
                    )
                    SELECT
                        d.personnel_num,
                        d.ride,
                        ROUND(distance_val / (ride_sec)*60*60) AS speed,
                        coalesce(h.violations_cnt, 0) AS violations_cnt,
                        TO_DATE('2022-12-29', 'YYYY-MM-DD') AS report_dt --> # дата отчета на период наличия данных в БД
                    FROM cte_drive AS d
                    LEFT JOIN cte_viol_hist AS h 
                        ON d.personnel_num = h.personnel_num
                    WHERE 
                        ROUND(d.distance_val / (d.ride_sec)*60*60) > 85 AND 
                        d.ride_sec > 0 AND
                        d.ride_arrival_dt::date = TO_DATE('2022-12-29', 'YYYY-MM-DD')
                    ;
''')

#----> 9.3 Перерабатывающие водители
#       -- водитель работал больше 7 часов за 24 часа (по путевым листам).
#       -- нарушением считается тот путевой лист, который вышел за пределы 8
#       -- часов. Таких путевых листов может быть много.
#       -- дата нарушения - по дате начала работы в путевом листе.
#			● Атрибуты:
#			○ personnel_num - табельный номер водителя,
#			○ дата-время старта 24-часового периода, где он работал больше 8 часов
#			○ суммарная наработка
#			○ report_dt - дата, на которую построен отчет.

cursor_dwh.execute( ''' 
                    INSERT INTO dwh_oryol.rep_drivers_overtime (
                        personnel_num,
                        start_work_dt,
                        working_time,
                        report_dt)
                    WITH end_work_24 AS (
                    SELECT
                        driver_pers_num,
                        work_start_dt,
                        work_end_dt,
                        first_value(work_start_dt) over( partition by driver_pers_num order by work_start_dt ) 
                            + INTERVAL '24 hour' as work_day_24
                    FROM dwh_oryol.fact_waybills AS w
                    WHERE work_start_dt::date = TO_DATE('2023-01-01', 'YYYY-MM-DD') --> дата отчета
                    ),
                    sum_overtime AS (
                        SELECT
                            driver_pers_num,
                            work_start_dt,
                            work_end_dt,
                            work_day_24,
                            case 
                                when work_end_dt > work_day_24 
                                    then date_part('hour',work_day_24::timestamp - work_start_dt::timestamp)
                                else 
                                date_part('hour', work_end_dt::timestamp - work_start_dt::timestamp)
                            end as sum_work_time
                        FROM end_work_24
                    )
                    SELECT
                        driver_pers_num AS personnel_num,
                        MIN(work_start_dt) AS start_work_dt,
                        SUM(sum_work_time) AS working_time,
                        MIN(work_start_dt)::date AS report_dt
                    FROM sum_overtime
                    GROUP BY personnel_num, work_day_24
                    HAVING SUM(sum_work_time) > 8
                    ;
''')

#----> 9.4 “Знай своего клиента”
#			● Атрибуты:
#			○ client_id - идентификатор клиента (суррогатный ключ),
#			○ phone_num - номер телефона клиента,
#			○ rides_cnt - количество поездок,
#			○ cancelled_cnt - сколько поездок отменено,
#			○ spent_amt - сколько денег потрачено,
#			○ debt_amt - задолженность,
#			○ start_dt - начало версии,
#			○ end_dt - конец версии,
#			○ deleted_flag - признак логического удаления.


#----> 9.4.1 stg_clients_hist -> Insert stg >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Загрузка в промежуточную (staging) таблицу текущего резельтата отчета

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.stg_rep_clients_hist  (
                        phone_num,
                        rides_cnt,
                        cancelled_cnt,
                        spent_amt, 
                        debt_amt,
                        update_dt
                        )
                    WITH cte_rides AS (
                        SELECT
                            r.client_phone_num,
                            COUNT(r.ride_start_dt) AS rides_cnt,
                            COUNT(CASE WHEN r.ride_start_dt IS NULL THEN r.ride_id END) AS cancelled_cnt,
                            SUM(CASE WHEN r.ride_start_dt IS NOT NULL THEN r.price_amt END) AS spent_amt
                        FROM dwh_oryol.fact_rides AS r
                        GROUP BY r.client_phone_num
                    ),
                    cte_pay AS (
                        SELECT
                            card_num,
                            SUM(transaction_amt) AS recieved_amt
                        FROM dwh_oryol.fact_payments
                        GROUP BY card_num
                    )
                    SELECT
                        c.phone_num,
                        r.rides_cnt,
                        r.cancelled_cnt,
                        r.spent_amt,
                        SUM(spent_amt - recieved_amt) AS debt_amt,
                        NOW()::timestamp as update_dt
                    FROM dwh_oryol.dim_clients_hist AS c
                    LEFT JOIN cte_rides AS r
                        ON c.phone_num = r.client_phone_num 
                    LEFT JOIN cte_pay AS p 
                        ON REPLACE(c.card_num, ' ', '') = p.card_num
                    GROUP BY c.phone_num, r.rides_cnt, r.cancelled_cnt, r.spent_amt
                    ;	
''')

#----> 9.4.2 stg_rep_clients_hist_del -> Insert del >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>    
# Загрузка  в стейджинг ключей из источника полным срезом для вычисления удалений.

cursor_src.execute( ''' SELECT DISTINCT phone_num
                        FROM dwh_oryol.stg_rep_clients_hist ''' )

records = cursor_src.fetchmany()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )

cursor_dwh.executemany( ''' INSERT INTO dwh_oryol.stg_rep_clients_hist_del ( phone_num )
                            VALUES( %s ) ''', df.values.tolist() )

#----> 9.4.3  rep_clients_hist -> Insert new positions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Загрузка dim ( Загрузка в приемник новых позиций (формат SCD2) )

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.rep_clients_hist (
                        phone_num,
                        rides_cnt,
                        cancelled_cnt,
                        spent_amt, 
                        debt_amt,
                        start_dt,
			            end_dt,
			            deleted_flag
                    )
                    SELECT DISTINCT
                        stg.phone_num,
                        stg.rides_cnt,
                        stg.cancelled_cnt,
                        stg.spent_amt, 
                        stg.debt_amt,
                        NOW() AS start_dt,
                        to_date('2999-12-31', 'YYYY-MM-DD') AS end_dt,
                        'N' AS deleted_flag
                    FROM dwh_oryol.stg_rep_clients_hist AS stg
                    LEFT JOIN dwh_oryol.rep_clients_hist AS dim
                    ON 1 = 1 
                        AND stg.phone_num = dim.phone_num 
                        AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                        AND dim.deleted_flag = 'N'
                    WHERE dim.phone_num IS NULL
                    ;
''' )

#----> 9.4.4  rep_clients_hist -> Update dim (fix + add) >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Обновление dim  ( Фиксация даты end_dt для измененной строки и  
#                  загрузка обновленной строки с актуальной датой end_dt (SCD2) )

cursor_dwh.execute( '''
                    UPDATE dwh_oryol.rep_clients_hist  AS d
                    SET 
                        end_dt = tmp.start_dt - INTERVAL '15 second'
                    FROM (
                         SELECT DISTINCT
                            stg.phone_num,
                            NOW() AS start_dt,
                            'N' AS deleted_flag,
                            to_date('2999-12-31', 'YYYY-MM-DD') AS end_dt
                        FROM dwh_oryol.stg_rep_clients_hist AS stg
                        INNER JOIN dwh_oryol.rep_clients_hist AS dim
                        ON 1 = 1 
                            AND stg.phone_num = dim.phone_num 
                            AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                            AND dim.deleted_flag = 'N'
                        WHERE 1 = 0 
                            OR stg.phone_num <> dim.phone_num OR (stg.phone_num IS NULL AND dim.phone_num IS NOT NULL)
                                                            OR (stg.phone_num IS NOT NULL AND dim.phone_num IS NULL)
                            OR stg.rides_cnt <> dim.rides_cnt OR (stg.rides_cnt IS NULL AND dim.rides_cnt IS NOT NULL)
                                                            OR (stg.rides_cnt IS NOT NULL AND dim.rides_cnt IS NULL) 
                            OR stg.cancelled_cnt <> dim.cancelled_cnt OR (stg.cancelled_cnt IS NULL AND dim.cancelled_cnt IS NOT NULL)
                                                            OR (stg.cancelled_cnt IS NOT NULL AND dim.cancelled_cnt IS NULL) 
                            OR stg.spent_amt <> dim.spent_amt OR (stg.spent_amt IS NULL AND dim.spent_amt IS NOT NULL)
                                                            OR (stg.spent_amt IS NOT NULL AND dim.spent_amt IS NULL) 
                            OR stg.debt_amt <> dim.debt_amt OR (stg.debt_amt IS NULL AND dim.debt_amt IS NOT NULL)
                                                            OR (stg.debt_amt IS NOT NULL AND dim.debt_amt IS NULL)                                                                                             
                        ) tmp
                    WHERE 1 = 1
                        AND d.phone_num = tmp.phone_num 
                        AND d.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                        ;
''')

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.rep_clients_hist (
                        phone_num,
                        rides_cnt,
                        cancelled_cnt,
                        spent_amt, 
                        debt_amt,
                        start_dt,
			            end_dt,
			            deleted_flag )
                     SELECT DISTINCT
                        stg.phone_num,
                        stg.rides_cnt,
                        stg.cancelled_cnt,
                        stg.spent_amt, 
                        stg.debt_amt,
                        NOW() AS start_dt,
                        to_date('2999-12-31', 'YYYY-MM-DD') AS end_dt,
                        'N' AS deleted_flag
                    FROM dwh_oryol.stg_rep_clients_hist AS stg
                    INNER JOIN dwh_oryol.rep_clients_hist AS dim
                    ON 1 = 1 
                        AND stg.phone_num = dim.phone_num 
                        AND dim.end_dt = stg.update_dt - INTERVAL '15 second'
                        AND dim.deleted_flag = 'N'
                    WHERE 1 = 0 
                            OR stg.phone_num <> dim.phone_num OR (stg.phone_num IS NULL AND dim.phone_num IS NOT NULL)
                                                            OR (stg.phone_num IS NOT NULL AND dim.phone_num IS NULL)
                            OR stg.rides_cnt <> dim.rides_cnt OR (stg.rides_cnt IS NULL AND dim.rides_cnt IS NOT NULL)
                                                            OR (stg.rides_cnt IS NOT NULL AND dim.rides_cnt IS NULL) 
                            OR stg.cancelled_cnt <> dim.cancelled_cnt OR (stg.cancelled_cnt IS NULL AND dim.cancelled_cnt IS NOT NULL)
                                                            OR (stg.cancelled_cnt IS NOT NULL AND dim.cancelled_cnt IS NULL) 
                            OR stg.spent_amt <> dim.spent_amt OR (stg.spent_amt IS NULL AND dim.spent_amt IS NOT NULL)
                                                            OR (stg.spent_amt IS NOT NULL AND dim.spent_amt IS NULL) 
                            OR stg.debt_amt <> dim.debt_amt OR (stg.debt_amt IS NULL AND dim.debt_amt IS NOT NULL)
                                                            OR (stg.debt_amt IS NOT NULL AND dim.debt_amt IS NULL)                                                                                             
                    ;
''' )

#----> 9.4.5  dim_clients_hist -> Insert Deleted (insert + fix) >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Загрузка Deleted (Загрузка в отчет "удаленных" строк с актуальной датой end_dt и deleted_flag,
#                   фиксация даты end_dt для "удаленной" строки (SCD2))

cursor_dwh.execute( '''
                    INSERT INTO dwh_oryol.rep_clients_hist (
                        phone_num,
                        rides_cnt,
                        cancelled_cnt,
                        spent_amt, 
                        debt_amt,
                        start_dt,
			            end_dt,
			            deleted_flag )
                    SELECT 
                        dim.phone_num,
                        dim.rides_cnt,
                        dim.cancelled_cnt,
                        dim.spent_amt, 
                        dim.debt_amt,
                        NOW () AS start_dt,
                        to_date('2999-12-31', 'YYYY-MM-DD') AS end_dt,
                        'Y' AS deleted_flag
                    FROM dwh_oryol.rep_clients_hist AS dim
                    WHERE phone_num IN (
                        SELECT
                            dim.phone_num
                        FROM dwh_oryol.rep_clients_hist AS dim
                        LEFT JOIN dwh_oryol.stg_rep_clients_hist_del AS stg_d
                            ON  dim.phone_num = stg_d.phone_num
                        WHERE
                            dim.phone_num IS NULL
                            AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                            AND dim.deleted_flag = 'N'
                        )
                    AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                    AND dim.deleted_flag = 'N'
                    ;
''')

cursor_dwh.execute( '''
                    UPDATE dwh_oryol.rep_clients_hist AS dim
                    SET
                        end_dt = now () - INTERVAL '15 second'
                    WHERE dim.phone_num IN (
                        SELECT
                            dim.phone_num
                        FROM dwh_oryol.rep_clients_hist AS dim
                        LEFT JOIN dwh_oryol.stg_rep_clients_hist_del AS stg_d
                            ON  dim.phone_num = stg_d.phone_num
                        WHERE
                            dim.phone_num IS NULL
                            AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                            AND dim.deleted_flag = 'N'
                        )
                    AND dim.end_dt = to_date('2999-12-31', 'YYYY-MM-DD')
                    AND dim.deleted_flag = 'N'
                    ;
''')
#----> 9.4.6  clients -> Meta >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Обновление метаданных

cursor_dwh.execute( '''
                    UPDATE dwh_oryol.meta
                    SET max_UPDATE_dt = COALESCE (
                        ( SELECT MAX ( update_dt ) FROM dwh_oryol.stg_rep_clients_hist),
                        ( SELECT max_UPDATE_dt FROM dwh_oryol.meta 
                          WHERE schema_name = 'dwh_oryol' AND table_name = 'rep_clients_hist')
                    )
                    WHERE schema_name = 'dwh_oryol' AND table_name = 'fact_rides'
                    ;
''')

# Сохранение изменений
conn_dwh.commit()

# Закрываем соединение
cursor_dwh.close()
cursor_src.close()
conn_dwh.close()
conn_src.close()

print (f'Загружены и обновлены данные за: {report_dt}')

# ---- END --------------------------------------------------------



















