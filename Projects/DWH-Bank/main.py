#!/usr/bin/python3

import os
import shutil
from datetime import datetime
import psycopg2
import pandas as pd

# Создаем переменные с путями к папкам с данными
project_path = '/home/de11an/dobr/project'
data_path = '/home/de11an/dobr/project'
archive_data_parh = '/home/de11an/dobr/project/archive'

# Создание подключения к PostgreSQL
conn_src = psycopg2.connect(database = "bank",
                            host =     "de-edu-db.chronosavant.ru",
                            user =     "bank_etl",
                            password = "bank_etl_password",
                            port =     "5432")
conn_dwh = psycopg2.connect(database = "edu",
                            host =     "de-edu-db.chronosavant.ru",
                            user =     "de11an",
                            password = "peregrintook",
                            port =     "5432")

# Отключение автокоммита
conn_src.autocommit = False
conn_dwh.autocommit = False

# Создание курсора
cursor_src = conn_src.cursor()
cursor_dwh = conn_dwh.cursor()

####################################################################################################
### 1. Очистка стейджинговых таблиц

cursor_dwh.execute( """
                        DELETE FROM de11an.dobr_stg_accounts;
                        DELETE FROM de11an.dobr_stg_cards;
                        DELETE FROM de11an.dobr_stg_clients;
                        DELETE FROM de11an.dobr_stg_terminals;
                        DELETE FROM de11an.dobr_stg_transactions;
                        DELETE FROM de11an.dobr_stg_passport_blacklist;
                        DELETE FROM de11an.dobr_stg_del_accounts;
                        DELETE FROM de11an.dobr_stg_del_cards;
                        DELETE FROM de11an.dobr_stg_del_clients;
                        DELETE FROM de11an.dobr_stg_del_terminals;
""")


#######################################################################################################
###  2. Загрузка stg (Захват данных из источника (измененных с момента последней загрузки) в стейджинг)

#----> stg_accounts -> Insert stg >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# Получаем дату последнего изменения из meta
cursor_dwh.execute( """ SELECT max_update_dt
                        FROM de11an.dobr_meta_dwh
                        WHERE schema_name='info' 
                        AND table_name = 'accounts' """ )

meta_accounts_date = cursor_dwh.fetchone()[0]

# Чтение данных из БД источника src в DataFrame
cursor_src.execute( ''' SELECT 
                            account,
                            valid_to,
                            client,
                            create_dt,
                            update_dt 
                        FROM info.accounts 
                        WHERE COALESCE(update_dt, create_dt) > ( %s ) ''', [meta_accounts_date] ) 

records = cursor_src.fetchall()
names = [x[0] for x in cursor_src.description]
df = pd.DataFrame(records, columns = names)

# Запись DataFrame в таблицу базы данных dwh
cursor_dwh.executemany( ''' INSERT INTO de11an.dobr_stg_accounts (
                                account,
                                valid_to,
                                client,
                                create_dt,
                                update_dt     
                            ) VALUES( %s, %s, %s, %s, %s ) ''', df.values.tolist() )


#----> stg_cards -> Insert stg >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# Получаем дату последнего изменения из meta
cursor_dwh.execute( """ SELECT max_update_dt
                    FROM de11an.dobr_meta_dwh
                    WHERE schema_name='info' 
                    AND table_name = 'cards' """ )

meta_cards_date = cursor_dwh.fetchone()[0]

# Чтение данных из БД источника src в DataFrame
cursor_src.execute( ''' SELECT 
                            card_num,
                            account,
                            create_dt,
                            update_dt 
                        FROM info.cards 
                        WHERE COALESCE(update_dt, create_dt) > ( %s ) ''', [meta_cards_date] ) 

records = cursor_src.fetchall()
names = [x[0] for x in cursor_src.description]
df = pd.DataFrame(records, columns = names)

# Запись DataFrame в таблицу базы данных dwh
cursor_dwh.executemany( ''' INSERT INTO de11an.dobr_stg_cards (
                                card_num,
                                account,
                                create_dt,
                                update_dt    
                            ) VALUES( %s, %s, %s, %s ) ''', df.values.tolist() )



#---- stg_clients -> Insert stg >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# Получаем дату последнего изменения из meta
cursor_dwh.execute( """ SELECT max_update_dt
                    FROM de11an.dobr_meta_dwh
                    WHERE schema_name='info' 
                    AND table_name = 'clients' """ )

meta_clients_date = cursor_dwh.fetchone()[0]

# Чтение данных из БД источника src в DataFrame
cursor_src.execute( ''' SELECT 
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            create_dt,
                            update_dt 
                        FROM info.clients 
                        WHERE COALESCE(update_dt, create_dt) > ( %s ) ''', [meta_clients_date] ) 

records = cursor_src.fetchall()
names = [x[0] for x in cursor_src.description]
df = pd.DataFrame(records, columns = names)

# Запись DataFrame в таблицу базы данных dwh
cursor_dwh.executemany( ''' INSERT INTO de11an.dobr_stg_clients (
                                client_id,
                                last_name,
                                first_name,
                                patronymic,
                                date_of_birth,
                                passport_num,
                                passport_valid_to,
                                phone,
                                create_dt,
                                update_dt 
                            ) VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) ''', df.values.tolist() )


#----> stg_terminals -> Insert stg >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# Получаем дату из меты - последние изменения
cursor_dwh.execute( """
                        SELECT
                            max_update_dt
                        FROM de11an.dobr_meta_dwh
                        WHERE schema_name='xlsx' 
                        AND table_name='terminals' 
""" )

meta_terminals_date = cursor_dwh.fetchone()[0]

# Ищем файл для обработки
terminals_file = None
terminals_file_dt = None
for f in sorted(os.listdir(data_path)): # сортируем по возрастанию
    if not f.startswith('terminals_'): # отбираем файлы с названием начинающимся с terminals_
        continue
    file_date = ''.join([n for n in f if n.isdigit()])  # выбираем из названия цифры обозначающие дату в виде списока и преобразуем в строку
    terminals_file_dt = datetime.strptime(file_date, '%d%m%Y') # форматируем строку в дату
    if terminals_file_dt > meta_terminals_date: # отбираем файл с датой > даты последнего обновления в meta
        terminals_file = f
        break

# Чтение данных из файлов xlsx в DataFrame
df = pd.read_excel(f'{data_path}/{terminals_file}', sheet_name='terminals', header=0, index_col=None)

# Присваиваем полю update_dt значение даты из имени файла
df['update_dt'] = terminals_file_dt.strftime('%Y-%m-%d') 

# Определяем имена полей
df = df[['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address', 'update_dt']]

# Запись DataFrame в таблицу базы данных
cursor_dwh.executemany( """ INSERT INTO de11an.dobr_stg_terminals(
                                terminal_id,
                                terminal_type,
                                terminal_city,
                                terminal_address,
                                update_dt 
                            ) VALUES( %s, %s, %s, %s, %s ) """, df.values.tolist() )


#----> stg_transactions -> Insert stg >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# Получаем дату из меты - последние изменения
cursor_dwh.execute( """
                    SELECT
                        max_update_dt
                    FROM de11an.dobr_meta_dwh
                    WHERE schema_name='txt' 
                    AND table_name='transactions'
""" )
meta_transactions_date = cursor_dwh.fetchone()[0]

    # Ищем файл для обработки
transactions_file = None
transactions_file_dt = None
for f in sorted(os.listdir(data_path)): # сортируем по возрастанию
    if not f.startswith('transactions_'): # отбираем файлы с названием начинающимся с transactions_
        continue
    file_date = ''.join([n for n in f if n.isdigit()]) # выбираем из названия цифры обозначающие дату в виде списока и преобразуем в строку
    transactions_file_dt = datetime.strptime(file_date, '%d%m%Y') # форматируем строку в дату
    if transactions_file_dt > meta_transactions_date: # отбираем файл с датой > даты последнего обновления в meta
        transactions_file = f
        break

    # Чтение данных из файлов xlsx в DataFrame
df = pd.read_csv(f'{data_path}/{transactions_file}', sep=';', header=0)

    # Замена запятой на точку в поле amt_fixed
df['amt_fixed'] = df.amount.apply(lambda x: x.replace(',', '.'))

    # Определяем имена полей
df = df[['transaction_id', 'transaction_date', 'amt_fixed', 'card_num', 'oper_type', 'oper_result', 'terminal']]

    # Запись DataFrame в таблицу базы данных
cursor_dwh.executemany( """ INSERT INTO de11an.dobr_stg_transactions( 
                                transaction_id, 
                                transaction_date, 
                                amount, 
                                card_num, 
                                oper_type, 
                                oper_result, 
                                terminal 
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s) """, df.values.tolist() )



#----> stg_passport_blacklist -> Insert stg >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# Получаем дату из меты - последние изменения
cursor_dwh.execute( """
                    SELECT
                        max_update_dt
                    FROM de11an.dobr_meta_dwh
                    WHERE schema_name='xlsx' 
                    AND table_name='passport_blacklist'
""" )
meta_passport_blacklist_date = cursor_dwh.fetchone()[0]

    # Ищем файл для обработки
passport_blacklist_file = None
passport_blacklist_file_dt = None
    # сортируем по возрастанию и находим первый 
for f in sorted(os.listdir(data_path)): 
    # отбираем файлы с названием начинающимся с passport_blacklist_
    if not f.startswith('passport_blacklist_'): 
        continue
    # выбираем из названия цифры обозначающие дату в виде списока и преобразуем в строку
    file_date = ''.join([n for n in f if n.isdigit()]) 
    # форматируем цифры из названия файла в дату
    passport_blacklist_file_dt = datetime.strptime(file_date, '%d%m%Y') 
    # отбираем файл с датой > даты последнего обновления в meta
    if passport_blacklist_file_dt > meta_passport_blacklist_date: 
        passport_blacklist_file = f
        break

    # Чтение данных из файлов xlsx в DataFrame
df = pd.read_excel(f'{data_path}/{passport_blacklist_file}', sheet_name='blacklist', header=0, index_col=None)

    # Отбираем только вновь поступившие данные
rslt_df = df[df['date'] > meta_passport_blacklist_date] 

    # Запись DataFrame в таблицу базы данных
cursor_dwh.executemany( """ INSERT INTO de11an.dobr_stg_passport_blacklist( 
                                date, 
                                passport )
                            VALUES (%s, %s) """, rslt_df.values.tolist() )    

#####################################################################################################
### 3. Загрузка  в стейджинг ключей из источника полным срезом для вычисления удалений.

#----> stg_del_accounts -> Insert del >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_src.execute( ''' SELECT 
                            account
                        FROM info.accounts  ''' ) 

records = cursor_src.fetchall()
names = [x[0] for x in cursor_src.description]
df = pd.DataFrame(records, columns = names)

cursor_dwh.executemany( ''' INSERT INTO de11an.dobr_stg_del_accounts (
                                account  
                            ) VALUES( %s ) ''', df.values.tolist() )

#----> stg_del_cards -> Insert del >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_src.execute( ''' SELECT 
                            card_num
                        FROM info.cards  ''' ) 

records = cursor_src.fetchall()
names = [x[0] for x in cursor_src.description]
df = pd.DataFrame(records, columns = names)

cursor_dwh.executemany( ''' INSERT INTO de11an.dobr_stg_del_cards (
                                card_num  
                            ) VALUES( %s ) ''', df.values.tolist() )

#----> stg_del_clients -> Insert del >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_src.execute( ''' SELECT 
                            client_id
                        FROM info.clients  ''' ) 

records = cursor_src.fetchall()
names = [x[0] for x in cursor_src.description]
df = pd.DataFrame(records, columns = names)

cursor_dwh.executemany( ''' INSERT INTO de11an.dobr_stg_del_clients (
                                client_id  
                            ) VALUES( %s ) ''', df.values.tolist() )

#----> stg_del_terminals -> Insert del >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# Чтение данных из файлов xlsx в DataFrame
df = pd.read_excel(f'{data_path}/{terminals_file}', sheet_name='terminals', header=0, index_col=None)

cursor_dwh.executemany( """ INSERT INTO de11an.dobr_stg_del_terminals(
                                terminal_id
                            ) VALUES( %s ) """, df[['terminal_id']].values.tolist() )




#####################################################################################################
### 3. Загрузка dim ( Загрузка в приемник "вставок" на источнике (формат SCD2))

#----> dim_accounts -> Insert dim >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                        INSERT INTO de11an.dobr_dwh_dim_accounts_hist ( 
                            account_num,
                            valid_to,
                            client,
                            effective_from,
                            effective_to,
                            del_flg )
                        SELECT 
                            account,
                            stg.valid_to,
                            stg.client,
                            COALESCE( update_dt, create_dt ) AS effective_from,
                            TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) AS effective_to,
                            0 AS del_flg
                        FROM de11an.dobr_stg_accounts stg
                        LEFT JOIN de11an.dobr_dwh_dim_accounts_hist dim
                            ON stg.account = dim.account_num
                            AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                            AND dim.del_flg = 0
                        WHERE dim.account_num IS NULL
                        ;
''')
#----> dim_cards -> INSERT dim >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute('''
                        INSERT INTO de11an.dobr_dwh_dim_cards_hist ( 
                            card_num,
                            account_num,
                            effective_from,
                            effective_to,
                            del_flg )
                        SELECT 
                            stg.card_num,
                            account,
                            COALESCE( update_dt, create_dt ) AS effective_from,
                            TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) AS effective_to,
                            0 AS del_flg
                        FROM de11an.dobr_stg_cards stg
                        LEFT JOIN de11an.dobr_dwh_dim_cards_hist dim
                            ON stg.card_num = dim.card_num
                            AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                            AND dim.del_flg = 0
                        WHERE dim.card_num IS NULL
                       ;
''')

#----> dim_clients -> INSERT dim >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute('''
                        INSERT INTO de11an.dobr_dwh_dim_clients_hist ( 
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            effective_from,
                            effective_to,
                            del_flg )
                        SELECT 
                            stg.client_id,
                            stg.last_name,
                            stg.first_name,
                            stg.patronymic,
                            stg.date_of_birth,
                            stg.passport_num,
                            stg.passport_valid_to,
                            stg.phone,
                            COALESCE( update_dt, create_dt ) AS effective_from,
                            TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) AS effective_to,
                            0 AS del_flg
                        FROM de11an.dobr_stg_clients stg
                        LEFT JOIN de11an.dobr_dwh_dim_clients_hist dim
                            ON stg.client_id = dim.client_id
                            AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                            AND dim.del_flg = 0
                        WHERE dim.client_id IS NULL
                       ;
''')

#----> dim_terminals -> INSERT dim >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                        INSERT INTO de11an.dobr_dwh_dim_terminals_hist ( 
                            terminal_id, 
                            terminal_type, 
                            terminal_city, 
                            terminal_address,
                            effective_from,
                            effective_to,
                            del_flg )
                        SELECT 
                            stg.terminal_id, 
                            stg.terminal_type, 
                            stg.terminal_city, 
                            stg.terminal_address,
                            update_dt AS effective_from,
                            TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) AS effective_to,
                            0 AS del_flg
                        FROM de11an.dobr_stg_terminals stg
                        LEFT JOIN de11an.dobr_dwh_dim_terminals_hist dim
                            ON stg.terminal_id = dim.terminal_id
                            AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                            AND dim.del_flg = 0
                        WHERE dim.terminal_id IS NULL
                       ;
''')

#####################################################################################################
### 4. Загрузка fact

#----> fact_transactions -> INSERT fact >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                        INSERT INTO de11an.dobr_dwh_fact_transactions ( 
                            trans_id, 
                            trans_date,
                            card_num,
                            oper_type,
                            amt, 
                            oper_result, 
                            terminal)
                        SELECT 
                            transaction_id, 
                            transaction_date, 
                            stg.card_num,
                            stg.oper_type, 
                            amount, 
                            stg.oper_result, 
                            stg.terminal
                        FROM de11an.dobr_stg_transactions stg
                        ;
''')

#----> fact_passport_blacklist -> INSERT fact >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                        INSERT INTO de11an.dobr_dwh_fact_passport_blacklist ( 
                            passport_num,
                            entry_dt )
                        SELECT 
                            passport,
                            date
                        FROM de11an.dobr_stg_passport_blacklist
                        ;
''')



#####################################################################################################
### 5. Обновление dim  ( Обновление в приемнике "обновлений" на источнике (формат SCD2) )

#----> dim_accounts_hist -> Update dim >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                        UPDATE de11an.dobr_dwh_dim_accounts_hist
                        SET 
                            effective_to = tmp.update_dt - INTERVAL '15 second'
                        FROM (
                            SELECT 
                                stg.account, 
                                stg.update_dt
                            FROM de11an.dobr_stg_accounts stg
                            INNER JOIN de11an.dobr_dwh_dim_accounts_hist dim
                                ON stg.account = dim.account_num
                                AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                                AND dim.del_flg = 0
                            WHERE 1=0
                                OR stg.valid_to <> dim.valid_to OR ( stg.valid_to IS NULL AND dim.valid_to IS NOT NULL ) 
				                                                OR ( stg.valid_to IS NOT NULL and dim.valid_to is null )
                                OR stg.client <> dim.client OR ( stg.client IS NULL AND dim.client IS NOT NULL ) 
				                                            OR ( stg.client IS NOT NULL and dim.client IS NULL )
                        ) tmp
                        WHERE dobr_dwh_dim_accounts_hist.account_num = tmp.account
                          AND dobr_dwh_dim_accounts_hist.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                        ; 
''')

cursor_dwh.execute( '''
                        INSERT INTO de11an.dobr_dwh_dim_accounts_hist ( 
                            account_num,
                            valid_to,
                            client,
                            effective_from,
                            effective_to,
                            del_flg )
                        SELECT DISTINCT
                            account,
                            stg.valid_to,
                            stg.client,
                            COALESCE( update_dt, create_dt ) AS effective_from,
                            TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) AS effective_to,
                            0 AS del_flg
                        FROM de11an.dobr_stg_accounts stg
                        INNER JOIN de11an.dobr_dwh_dim_accounts_hist dim
                            ON stg.account = dim.account_num
                            AND dim.effective_to = stg.update_dt - INTERVAL '15 second'
                            AND dim.del_flg = 0
                        WHERE 1=0
                            OR stg.valid_to <> dim.valid_to OR ( stg.valid_to IS NULL AND dim.valid_to IS NOT NULL ) 
			                                                OR ( stg.valid_to IS NOT NULL and dim.valid_to IS NULL )
                            OR stg.client <> dim.client OR ( stg.client IS NULL AND dim.client IS NOT NULL ) 
			                                                OR ( stg.client IS NOT NULL and dim.client IS NULL )
                        ;
''')
#---->  dim_cards_hist -> Update dim >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>-

cursor_dwh.execute( '''
                        UPDATE de11an.dobr_dwh_dim_cards_hist
                        SET 
                            effective_to = tmp.update_dt - INTERVAL '15 second'
                        FROM (
                            SELECT 
                                stg.card_num, 
                                stg.update_dt
                            FROM de11an.dobr_stg_cards stg
                            INNER JOIN de11an.dobr_dwh_dim_cards_hist dim
                                ON stg.card_num = dim.card_num
                                AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                                AND dim.del_flg = 0
                            WHERE 1=0
                                OR stg.account <> dim.account_num OR ( stg.account IS NULL AND dim.account_num IS NOT NULL ) 
				                                                  OR ( stg.account IS NOT NULL and dim.account_num IS NULL )
                        ) tmp
                        WHERE dobr_dwh_dim_cards_hist.card_num = tmp.card_num
                          AND dobr_dwh_dim_cards_hist.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                        ; 
''')
cursor_dwh.execute( '''
                        INSERT INTO de11an.dobr_dwh_dim_cards_hist ( 
                            card_num,
                            account_num,
                            effective_from,
                            effective_to,
                            del_flg )
                        SELECT DISTINCT
                            stg.card_num,
                            stg.account,
                            COALESCE( update_dt, create_dt ) AS effective_from,
                            TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) AS effective_to,
                            0 AS del_flg
                        FROM de11an.dobr_stg_cards stg
                        INNER JOIN de11an.dobr_dwh_dim_cards_hist dim
                            ON stg.card_num = dim.card_num
                            AND dim.effective_to = stg.update_dt - INTERVAL '15 second'
                            AND dim.del_flg = 0
                        WHERE 1=0
                            OR stg.account <> dim.account_num OR ( stg.account IS NULL AND dim.account_num IS NOT NULL ) 
			                                                  OR ( stg.account IS NOT NULL and dim.account_num is null )
                        ;		
''')

#---->  dim_clients_hist -> Update dim >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                        UPDATE de11an.dobr_dwh_dim_clients_hist
                        SET 
                            effective_to = tmp.update_dt - INTERVAL '15 second'
                        FROM (
                            SELECT 
                                stg.client_id, 
                                stg.update_dt
                            FROM de11an.dobr_stg_clients stg
                            INNER JOIN de11an.dobr_dwh_dim_clients_hist dim
                                ON stg.client_id = dim.client_id
                                AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                                AND dim.del_flg = 0
                            WHERE 1=0
                                OR stg.last_name <> dim.last_name OR ( stg.last_name IS NULL AND dim.last_name IS NOT NULL ) 
				                                                  OR ( stg.last_name IS NOT NULL and dim.last_name IS NULL )
                                OR stg.first_name <> dim.first_name OR ( stg.first_name IS NULL AND dim.first_name IS NOT NULL ) 
				                                                    OR ( stg.first_name IS NOT NULL and dim.first_name IS NULL )
                                OR stg.patronymic <> dim.patronymic OR ( stg.patronymic IS NULL AND dim.patronymic IS NOT NULL ) 
				                                                    OR ( stg.patronymic IS NOT NULL and dim.patronymic IS NULL )
                                OR stg.date_of_birth <> dim.date_of_birth OR ( stg.date_of_birth IS NULL AND dim.date_of_birth IS NOT NULL ) 
				                                                          OR ( stg.date_of_birth IS NOT NULL and dim.date_of_birth IS NULL )
                                OR stg.passport_num <> dim.passport_num OR ( stg.passport_num IS NULL AND dim.passport_num IS NOT NULL ) 
				                                                        OR ( stg.passport_num IS NOT NULL and dim.passport_num IS NULL )
                                OR stg.passport_valid_to <> dim.passport_valid_to OR ( stg.passport_valid_to IS NULL AND dim.passport_valid_to IS NOT NULL ) 
				                                                                  OR ( stg.passport_valid_to IS NOT NULL and dim.passport_valid_to IS NULL )
                                OR stg.phone <> dim.phone OR ( stg.phone IS NULL AND dim.phone IS NOT NULL ) 
				                                          OR ( stg.phone IS NOT NULL and dim.phone IS NULL )
                        ) tmp
                        WHERE dobr_dwh_dim_clients_hist.client_id = tmp.client_id
                          AND dobr_dwh_dim_clients_hist.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                        ; 
''')

cursor_dwh.execute( '''
                    INSERT INTO de11an.dobr_dwh_dim_clients_hist ( 
                        client_id,
                        last_name,
                        first_name,
                        patronymic,
                        date_of_birth,
                        passport_num,
                        passport_valid_to,
                        phone,
                        effective_from,
                        effective_to,
                        del_flg )
                    SELECT DISTINCT
                        stg.client_id,
                        stg.last_name,
                        stg.first_name,
                        stg.patronymic,
                        stg.date_of_birth,
                        stg.passport_num,
                        stg.passport_valid_to,
                        stg.phone,
                        COALESCE( update_dt, create_dt ) AS effective_from,
                        TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) AS effective_to,
                        0 AS del_flg
                    FROM de11an.dobr_stg_clients stg
                    INNER JOIN de11an.dobr_dwh_dim_clients_hist dim
                    ON stg.client_id = dim.client_id
                        AND dim.effective_to = stg.update_dt - INTERVAL '15 second'
                        AND dim.del_flg = 0
                    WHERE 1=0
                        OR stg.last_name <> dim.last_name OR ( stg.last_name IS NULL AND dim.last_name IS NOT NULL ) 
                                                            OR ( stg.last_name IS NOT NULL and dim.last_name IS NULL )
                        OR stg.first_name <> dim.first_name OR ( stg.first_name IS NULL AND dim.first_name IS NOT NULL ) 
                                                            OR ( stg.first_name IS NOT NULL and dim.first_name IS NULL )
                        OR stg.patronymic <> dim.patronymic OR ( stg.patronymic IS NULL AND dim.patronymic IS NOT NULL ) 
                                                            OR ( stg.patronymic IS NOT NULL and dim.patronymic IS NULL )
                        OR stg.date_of_birth <> dim.date_of_birth OR ( stg.date_of_birth IS NULL AND dim.date_of_birth IS NOT NULL ) 
                                                                    OR ( stg.date_of_birth IS NOT NULL and dim.date_of_birth IS NULL )
                        OR stg.passport_num <> dim.passport_num OR ( stg.passport_num IS NULL AND dim.passport_num IS NOT NULL ) 
                                                                OR ( stg.passport_num IS NOT NULL and dim.passport_num IS NULL )
                        OR stg.passport_valid_to <> dim.passport_valid_to OR ( stg.passport_valid_to IS NULL AND dim.passport_valid_to IS NOT NULL ) 
                                                                            OR ( stg.passport_valid_to IS NOT NULL and dim.passport_valid_to IS NULL )
                        OR stg.phone <> dim.phone OR ( stg.phone IS NULL AND dim.phone IS NOT NULL ) 
                                                    OR ( stg.phone IS NOT NULL and dim.phone IS NULL )
                    ;
''')

#---->  dim_terminals_hist -> Update dim >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    UPDATE de11an.dobr_dwh_dim_terminals_hist
                    SET 
                        effective_to = tmp.update_dt - INTERVAL '15 second'
                    FROM (
                        SELECT 
                            stg.terminal_id, 
                            stg.update_dt
                        FROM de11an.dobr_stg_terminals stg
                        INNER JOIN de11an.dobr_dwh_dim_terminals_hist dim
                            ON stg.terminal_id = dim.terminal_id
                            AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                            AND dim.del_flg = 0
                        WHERE 1=0
                            OR stg.terminal_type <> dim.terminal_type OR ( stg.terminal_type IS NULL AND dim.terminal_type IS NOT NULL ) 
                                                                    OR ( stg.terminal_type IS NOT NULL and dim.terminal_type IS NULL )
                            OR stg.terminal_city <> dim.terminal_city OR ( stg.terminal_city IS NULL AND dim.terminal_city IS NOT NULL ) 
                                                                    OR ( stg.terminal_city IS NOT NULL and dim.terminal_city IS NULL )
                            OR stg.terminal_address <> dim.terminal_address OR ( stg.terminal_address IS NULL AND dim.terminal_address IS NOT NULL ) 
                                                                    OR ( stg.terminal_address IS NOT NULL and dim.terminal_address IS NULL )
                    ) tmp
                    WHERE dobr_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id
                      AND dobr_dwh_dim_terminals_hist.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                    ; 
''')

cursor_dwh.execute( '''
                    INSERT INTO de11an.dobr_dwh_dim_terminals_hist ( 
                        terminal_id, 
                        terminal_type, 
                        terminal_city, 
                        terminal_address,
                        effective_from,
                        effective_to,
                        del_flg )
                    SELECT DISTINCT
                        stg.terminal_id, 
                        stg.terminal_type, 
                        stg.terminal_city, 
                        stg.terminal_address,
                        update_dt AS effective_from,
                        TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) AS effective_to,
                        0 AS del_flg
                    FROM de11an.dobr_stg_terminals stg
                    INNER JOIN de11an.dobr_dwh_dim_terminals_hist dim
                        ON stg.terminal_id = dim.terminal_id
                        AND dim.effective_to = stg.update_dt - INTERVAL '15 second'
                        AND dim.del_flg = 0
                    WHERE 1=0
                        OR stg.terminal_type <> dim.terminal_type OR ( stg.terminal_type IS NULL AND dim.terminal_type IS NOT NULL ) 
                                                                OR ( stg.terminal_type IS NOT NULL and dim.terminal_type IS NULL )
                        OR stg.terminal_city <> dim.terminal_city OR ( stg.terminal_city IS NULL AND dim.terminal_city IS NOT NULL ) 
                                                                OR ( stg.terminal_city IS NOT NULL and dim.terminal_city IS NULL )
                        OR stg.terminal_address <> dim.terminal_address OR ( stg.terminal_address IS NULL AND dim.terminal_address IS NOT NULL ) 
                                                                        OR ( stg.terminal_address IS NOT NULL and dim.terminal_address IS NULL )
                    ;	
''')

#####################################################################################################
### 6. Загрузка Deleted (Загрузка в приемник "удаленных" строк на источнике (формат SCD2))

#---->  dim_accounts_hist -> Insert Deleted >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>-	

cursor_dwh.execute( '''		
                    INSERT INTO de11an.dobr_dwh_dim_accounts_hist ( 
                        account_num,
                        valid_to,
                        client,
                        effective_from,
                        effective_to,
                        del_flg )		
                    SELECT 
                        dim.account_num,
                        dim.valid_to,
                        dim.client,
                        NOW() AS effective_from,
                        TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) AS effective_to,
                        1 AS del_flg
                    FROM de11an.dobr_dwh_dim_accounts_hist dim
                    WHERE 
                        dim.account_num IN (
                        SELECT dim.account_num
                        FROM de11an.dobr_dwh_dim_accounts_hist dim
                        LEFT JOIN de11an.dobr_stg_del_accounts std
                            ON std.account = dim.account_num
                        WHERE std.account IS NULL
                            AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                            AND dim.del_flg = 0
                        )
                        AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                        AND dim.del_flg = 0
                    ;
''')

cursor_dwh.execute( '''	
                    UPDATE de11an.dobr_dwh_dim_accounts_hist
                    SET 
                        effective_to = NOW() - INTERVAL '15 second'
                    WHERE 
                        dobr_dwh_dim_accounts_hist.account_num IN (
                        SELECT dim.account_num
                        FROM de11an.dobr_dwh_dim_accounts_hist dim
                        LEFT JOIN de11an.dobr_stg_del_accounts std
                            ON std.account = dim.account_num
                        WHERE std.account IS NULL
                            AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                            AND dim.del_flg = 0
                        )
                        AND dobr_dwh_dim_accounts_hist.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                        AND dobr_dwh_dim_accounts_hist.del_flg = 0
                    ;
''')

#---->  dim_cards_hist -> Insert Deleted >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>		

cursor_dwh.execute( '''		
                    INSERT INTO de11an.dobr_dwh_dim_cards_hist ( 
                        card_num,
                        account_num,
                        effective_from,
                        effective_to,
                        del_flg )		
                    SELECT 
                        dim.card_num,
                        dim.account_num,
                        NOW() AS effective_from,
                        TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) AS effective_to,
                        1 AS del_flg
                    FROM de11an.dobr_dwh_dim_cards_hist dim
                    WHERE dim.card_num IN (
                        SELECT dim.card_num
                        FROM de11an.dobr_dwh_dim_cards_hist dim
                        LEFT JOIN de11an.dobr_stg_del_cards std
                            ON std.card_num = dim.card_num
                        WHERE std.card_num IS NULL
                            AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                            AND dim.del_flg = 0
                        )
                        AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                        AND dim.del_flg = 0
                    ;
''')

cursor_dwh.execute( '''	
                    UPDATE de11an.dobr_dwh_dim_cards_hist
                    SET 
                        effective_to = NOW() - INTERVAL '15 second'
                    WHERE dobr_dwh_dim_cards_hist.card_num IN (
                        SELECT dim.card_num
                        FROM de11an.dobr_dwh_dim_cards_hist dim
                        LEFT JOIN de11an.dobr_stg_del_cards std
                            ON std.card_num = dim.card_num
                        WHERE std.card_num IS NULL
                            and dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                            AND dim.del_flg = 0
                        )
                        AND dobr_dwh_dim_cards_hist.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                        AND dobr_dwh_dim_cards_hist.del_flg = 0
                    ;
''')

#---->  dim_clients_hist -> Insert Deleted >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>	

cursor_dwh.execute( '''			
                    INSERT INTO de11an.dobr_dwh_dim_clients_hist ( 
                        client_id,
                        last_name,
                        first_name,
                        patronymic,
                        date_of_birth,
                        passport_num,
                        passport_valid_to,
                        phone,
                        effective_from,
                        effective_to,
                        del_flg )
                    SELECT 
                        dim.client_id,
                        dim.last_name,
                        dim.first_name,
                        dim.patronymic,
                        dim.date_of_birth,
                        dim.passport_num,
                        dim.passport_valid_to,
                        dim.phone,
                        NOW() AS effective_from,
                        TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) AS effective_to,
                        1 AS del_flg
                    FROM de11an.dobr_dwh_dim_clients_hist dim
                    WHERE dim.client_id IN (
                        SELECT dim.client_id
                        FROM de11an.dobr_dwh_dim_clients_hist dim
                        LEFT JOIN de11an.dobr_stg_del_clients std
                            ON std.client_id = dim.client_id
                        WHERE std.client_id IS NULL
                            AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                            AND dim.del_flg = 0
                        )
                        AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                        AND dim.del_flg = 0
                    ;
''')

cursor_dwh.execute( '''	
                    UPDATE de11an.dobr_dwh_dim_clients_hist
                    SET 
                        effective_to = NOW() - INTERVAL '15 second'
                    WHERE dobr_dwh_dim_clients_hist.client_id IN (
                        SELECT dim.client_id
                        FROM de11an.dobr_dwh_dim_clients_hist dim
                        LEFT JOIN de11an.dobr_stg_del_clients std
                            ON std.client_id = dim.client_id
                        WHERE std.client_id IS NULL
                            AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                            AND dim.del_flg = 0
                        )
                        AND dobr_dwh_dim_clients_hist.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                        AND dobr_dwh_dim_clients_hist.del_flg = 0
                    ;
''')

#---->  dim_terminals_hist -> Insert Deleted  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>	

cursor_dwh.execute( '''		
                    INSERT INTO de11an.dobr_dwh_dim_terminals_hist ( 
                        terminal_id, 
                        terminal_type, 
                        terminal_city, 
                        terminal_address,
                        effective_from,
                        effective_to,
                        del_flg )
                    SELECT 
                        dim.terminal_id, 
                        dim.terminal_type, 
                        dim.terminal_city, 
                        dim.terminal_address,
                        NOW() AS effective_from,
                        TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) AS effective_to,
                        1 AS del_flg
                    FROM de11an.dobr_dwh_dim_terminals_hist dim
                    WHERE dim.terminal_id IN (
                        SELECT dim.terminal_id
                        FROM de11an.dobr_dwh_dim_terminals_hist dim
                        LEFT JOIN de11an.dobr_stg_del_terminals std
                            ON std.terminal_id = dim.terminal_id
                        WHERE std.terminal_id IS NULL
                            AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                            AND dim.del_flg = 0
                        )
                        AND dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                        AND dim.del_flg = 0
                    ;
''')

cursor_dwh.execute( '''	
                    UPDATE de11an.dobr_dwh_dim_terminals_hist
                    SET 
                        effective_to = NOW() - INTERVAL '15 second'
                    WHERE dobr_dwh_dim_terminals_hist.terminal_id IN (
                        SELECT dim.terminal_id
                        FROM de11an.dobr_dwh_dim_terminals_hist dim
                        LEFT JOIN de11an.dobr_stg_del_terminals std
                            ON std.terminal_id = dim.terminal_id
                        WHERE std.terminal_id IS NULL
                            and dim.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                            AND dim.del_flg = 0
                        )
                        AND dobr_dwh_dim_terminals_hist.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                        AND dobr_dwh_dim_terminals_hist.del_flg = 0
                    ;
''')

#####################################################################################################
### 7. Обновление метаданных

#---->  accounts -> Meta >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''	
                    UPDATE de11an.dobr_meta_dwh
                    SET max_update_dt = COALESCE (
                        ( SELECT MAX( COALESCE(update_dt, create_dt ) ) 
                        FROM de11an.dobr_stg_accounts ),
                        ( SELECT max_update_dt 
                        FROM de11an.dobr_meta_dwh
                        WHERE schema_name='info' AND table_name='accounts' )
                        )
                    WHERE schema_name='info' AND table_name = 'accounts';
''')

#---->  cards -> Meta  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    UPDATE de11an.dobr_meta_dwh
                    SET max_update_dt = COALESCE(
                        ( SELECT MAX( COALESCE(update_dt, create_dt ) ) 
                        FROM de11an.dobr_stg_cards ),
                        ( SELECT max_update_dt 
                        FROM de11an.dobr_meta_dwh
                        WHERE schema_name='info' AND table_name='cards' )
                        )
                    WHERE schema_name='info' AND table_name = 'cards';
''')

#----> clients -> Meta  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    UPDATE de11an.dobr_meta_dwh
                    SET max_update_dt = COALESCE(
                        ( SELECT MAX( COALESCE(update_dt, create_dt ) ) 
                        FROM de11an.dobr_stg_clients ),
                        ( SELECT max_update_dt 
                        FROM de11an.dobr_meta_dwh
                        WHERE schema_name='info' AND table_name='clients' )
                        )
                    WHERE schema_name='info' AND table_name = 'clients';
''')

#----> terminals ->  Meta >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    UPDATE de11an.dobr_meta_dwh
                    SET max_update_dt = COALESCE(
                        ( SELECT max( update_dt ) 
                        FROM de11an.dobr_stg_terminals ),
                        ( SELECT max_update_dt 
                        FROM de11an.dobr_meta_dwh
                        WHERE schema_name='info' AND table_name='terminals' )
                        )
                    WHERE schema_name='xlsx' AND table_name = 'terminals';
''')

#----> transactions -> Meta  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    UPDATE de11an.dobr_meta_dwh
                    SET max_update_dt = COALESCE(
                        ( SELECT MAX( transaction_date ) 
                        FROM de11an.dobr_stg_transactions ),
                        ( SELECT max_update_dt 
                        FROM de11an.dobr_meta_dwh
                        WHERE schema_name='info' AND table_name='transactions' )
                        )
                    WHERE schema_name='txt' AND table_name = 'transactions';
''')

#----> passport_blacklist -> Meta  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cursor_dwh.execute( '''
                    UPDATE de11an.dobr_meta_dwh
                    SET max_update_dt = COALESCE(
                        ( SELECT MAX( date ) 
                        FROM de11an.dobr_stg_passport_blacklist ),
                        ( SELECT max_update_dt 
                        FROM de11an.dobr_meta_dwh
                        WHERE schema_name='info' AND table_name='passport_blacklist' )
                        )
                    WHERE schema_name='xlsx' AND table_name = 'passport_blacklist';
''')

#####################################################################################################
###  8. Создание ветрины отчетов по мошенническим операциям 

# определяем дату составления отчетов как дату загруженной исходной таблицы 'transactions' на основе которой составлен отчет
report_dt = transactions_file_dt

#----> 8.1 Совершение операции при просроченном или заблокированном паспорте 

cursor_dwh.execute( '''
                    INSERT INTO de11an.dobr_rep_fraud (
                        event_dt,
                        passport,
                        fio,
                        phone,
                        event_type,
                        report_dt 
                    )
                    WITH client_acc AS (
                        SELECT 
                            TRIM(cr.card_num) AS card_num,
                            CONCAT(last_name, ' ', first_name, ' ', patronymic) AS fio,
                            passport_num, 
                            passport_valid_to, 
                            phone
                        FROM de11an.dobr_dwh_dim_accounts_hist AS acc
                        INNER JOIN dobr_dwh_dim_clients_hist AS cl
                            ON acc.client  = cl.client_id 
                            AND acc.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) 
                            AND cl.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                        INNER JOIN dobr_dwh_dim_cards_hist AS cr 
                            ON acc.account_num = cr.account_num 
                            AND acc.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) 
                            AND cr.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                    )
                    SELECT DISTINCT
                        trans_date AS event_dt, 
                        COALESCE(ca.passport_num, bl.passport_num) AS passport,
                        ca.fio,
                        phone,
                        1 AS event_type,
                        trans_date::date AS report_dt
                    FROM  client_acc AS ca
                    INNER JOIN dobr_dwh_fact_transactions AS tr
                        ON ca.card_num = tr.card_num
                    LEFT JOIN dobr_dwh_fact_passport_blacklist AS bl
                        ON ca.passport_num = bl.passport_num
                    WHERE ( trans_date::date > passport_valid_to 
                        OR ( bl.passport_num IS NOT NULL AND trans_date > bl.entry_dt ) ) 
                        AND trans_date >= ( %s ) ''', [report_dt] ) 

#----> 8.2 Совершение операции при недействующем договоре

cursor_dwh.execute( '''
                    INSERT INTO de11an.dobr_rep_fraud (
                        event_dt,
                        passport,
                        fio,
                        phone,
                        event_type,
                        report_dt 
                    )
                    WITH client_acc AS (
                        SELECT 
                            TRIM(cr.card_num) AS card_num,
                            CONCAT(last_name, ' ', first_name, ' ', patronymic) AS fio,
                            passport_num, 
                            acc.valid_to,
                            phone
                        FROM de11an.dobr_dwh_dim_accounts_hist AS acc
                        INNER JOIN dobr_dwh_dim_clients_hist AS cl
                            ON acc.client  = cl.client_id 
                            AND acc.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) AND cl.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                        INNER JOIN dobr_dwh_dim_cards_hist AS cr 
                            ON acc.account_num = cr.account_num 
                            AND acc.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) AND cr.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                    )
                    SELECT DISTINCT
                        trans_date AS event_dt,
                        COALESCE(ca.passport_num, bl.passport_num) AS passport,
                        ca.fio,
                        phone,
                        2 AS event_type,
                        trans_date::date AS report_dt
                    FROM  client_acc AS ca
                    INNER JOIN dobr_dwh_fact_transactions AS tr
                        ON ca.card_num = tr.card_num
                    LEFT JOIN dobr_dwh_fact_passport_blacklist AS bl
                        ON ca.passport_num = bl.passport_num
                    WHERE ca.valid_to < tr.trans_date::date AND trans_date >= ( %s )''', [report_dt] ) 

#----> 8.3  Совершение операций в разных городах в течение одного часа.

cursor_dwh.execute( '''
                    INSERT INTO de11an.dobr_rep_fraud (
                        event_dt,
                        passport,
                        fio,
                        phone,
                        event_type,
                        report_dt 
                    )
                    WITH client_acc AS (
                        SELECT 
                            TRIM(cr.card_num) AS card_num,
                            CONCAT(last_name, ' ', first_name, ' ', patronymic) AS fio,
                            passport_num AS passport, 
                            phone
                        FROM de11an.dobr_dwh_dim_accounts_hist AS acc
                        INNER JOIN dobr_dwh_dim_clients_hist AS cl
                            ON acc.client  = cl.client_id 
                            AND acc.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) AND cl.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                        INNER JOIN dobr_dwh_dim_cards_hist AS cr 
                            ON acc.account_num = cr.account_num 
                            AND acc.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) AND cr.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                    ),
                    client_trans AS (
                        SELECT DISTINCT
                            passport,
                            ca.fio,
                            phone,
                            terminal_city,
                            COALESCE(lead (terminal_city) OVER (PARTITION BY fio), terminal_city) AS lead_city_1,
                            trans_date,
                            COALESCE(lead (trans_date) OVER (PARTITION BY fio ORDER BY fio, trans_date), trans_date) AS lead_dt_1
                        FROM dobr_dwh_fact_transactions AS tr
                        INNER JOIN  client_acc AS ca
                            ON tr.card_num = ca.card_num
                        LEFT JOIN dobr_dwh_dim_terminals_hist AS term
                            ON tr.terminal = term.terminal_id
                    )
                    SELECT 	
                        trans_date AS event_dt,
                        passport,
                        fio,
                        phone,
                        3 AS event_type,
                        trans_date::date AS report_dt
                    FROM client_trans
                    WHERE (trans_date - lead_dt_1) <= '1 hour'::INTERVAL AND trans_date >= ( %s )
                        AND (terminal_city <> lead_city_1)
                    ORDER BY fio ''', [report_dt] ) 

#----> 8.4 Попытка подбора суммы. В течение 20 минут проходит более 3х
#           операций со следующим шаблоном – каждая последующая меньше предыдущей,
#           при этом отклонены все кроме последней. Последняя операция (успешная) в
#           такой цепочке считается мошеннической -->

cursor_dwh.execute( '''
                    INSERT INTO de11an.dobr_rep_fraud (
                        event_dt,
                        passport,
                        fio,
                        phone,
                        event_type,
                        report_dt 
                    )
                    WITH client_acc AS (
                        SELECT 
                            TRIM(cr.card_num) AS card_num,
                            CONCAT(last_name, ' ', first_name, ' ', patronymic) AS fio,
                            passport_num AS passport, 
                            phone
                        FROM de11an.dobr_dwh_dim_accounts_hist AS acc
                        INNER JOIN dobr_dwh_dim_clients_hist AS cl
                            ON acc.client  = cl.client_id 
                            AND acc.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) 
                            AND cl.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                        INNER JOIN dobr_dwh_dim_cards_hist AS cr 
                            ON acc.account_num = cr.account_num 
                            AND acc.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' ) 
                            AND cr.effective_to = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
                    ),
                    client_trans AS (
                        SELECT DISTINCT
                            passport,
                            ca.fio,
                            phone,
                            trans_date,
                            COALESCE(lead (trans_date, 1) OVER (PARTITION BY fio ORDER BY fio, trans_date), trans_date) AS lead_dt_1,
                            COALESCE(lead (trans_date, 2) OVER (PARTITION BY fio ORDER BY fio, trans_date), trans_date) AS lead_dt_2,
                            COALESCE(lead (trans_date, 3) OVER (PARTITION BY fio ORDER BY fio, trans_date), trans_date) AS lead_dt_3,
                            amt,
                            COALESCE(lead (amt, 1) OVER (PARTITION BY fio ORDER BY fio, trans_date), amt) AS lead_amt_1,
                            COALESCE(lead (amt, 2) OVER (PARTITION BY fio ORDER BY fio, trans_date), amt) AS lead_amt_2,
                            COALESCE(lead (amt, 3) OVER (PARTITION BY fio ORDER BY fio, trans_date), amt) AS lead_amt_3,
                            oper_result,
                            COALESCE(lead (oper_result, 1) OVER (PARTITION BY fio ORDER BY fio, trans_date), oper_result) AS lead_oper_res_1,
                            COALESCE(lead (oper_result, 2) OVER (PARTITION BY fio ORDER BY fio, trans_date), oper_result) AS lead_oper_res_2,
                            COALESCE(lead (oper_result, 3) OVER (PARTITION BY fio ORDER BY fio, trans_date), oper_result) AS lead_oper_res_3
                        FROM dobr_dwh_fact_transactions AS tr
                        INNER JOIN  client_acc AS ca
                            ON tr.card_num = ca.card_num
                    )
                    SELECT 
                        trans_date AS event_dt,
                        passport,
                        fio,
                        phone,
                        4 AS event_type,
                        trans_date::date AS report_dt
                    FROM client_trans
                    WHERE (trans_date - lead_dt_3) <= '20 minute'::INTERVAL AND trans_date >= ( %s )
                        AND oper_result = 'REJECT' AND lead_oper_res_1 = 'REJECT' 
                        AND lead_oper_res_2 = 'REJECT' AND lead_oper_res_3 = 'SUCCESS'
                        AND amt > lead_amt_1 AND lead_amt_1 > lead_amt_2 AND lead_amt_2 > lead_amt_3
                    ORDER BY fio ''', [report_dt] )

# Сохранение изменений
conn_dwh.commit()

# Закрываем соединение
cursor_src.close()
cursor_dwh.close()
conn_src.close()
conn_dwh.close()

###########################################################################
### 9. Перемещение файлов в архив --> Move

#----> terminals ->  Meta -----------------------------------------
shutil.move(
    f'{project_path}/{terminals_file}',
    f'{archive_data_parh}/{terminals_file}.backup' )

#----> transactions ->  Meta --------------------------------------
shutil.move(
    f'{project_path}/{transactions_file}',
    f'{archive_data_parh}/{transactions_file}.backup' )

#----> passport_blacklist ->  Meta --------------------------------
shutil.move(
    f'{project_path}/{passport_blacklist_file}',
    f'{archive_data_parh}/{passport_blacklist_file}.backup' )

print (f'Загружены и обновлены данные за: {report_dt}')

# ---- END --------------------------------------------------------
