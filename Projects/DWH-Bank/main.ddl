-- ! Исполнение данного скрипта осуществляется запуском bash скрипта
--   из папки где он находится командой :

-- ./start.sh

-- !!! для проверки в базе данных edu предварительно уже созданы таблицы с помощью запуска main.ddl 

-- Подготовка таблиц ------------------------------------------

--> 1. Создание промежуточных (staging) таблиц 

CREATE TABLE de11an.dobr_stg_accounts (
    account varchar(20),
	valid_to date,
	client varchar(10),
	create_dt timestamp(0),
	update_dt timestamp(0)
 );

CREATE TABLE de11an.dobr_stg_cards (
    card_num varchar(20),
	account varchar(20),
	create_dt timestamp(0),
	update_dt timestamp(0) 
);   

CREATE TABLE de11an.dobr_stg_clients (
    client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(16),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

CREATE TABLE de11an.dobr_stg_terminals ( 
    terminal_id varchar(5), 
    terminal_type varchar(10), 
    terminal_city varchar(30), 
    terminal_address varchar(100),
    update_dt timestamp(0)
 );

CREATE TABLE de11an.dobr_stg_transactions ( 
    transaction_id varchar(11), 
    transaction_date timestamp(0), 
    amount decimal(10, 2), 
    card_num varchar(20), 
    oper_type varchar(20), 
    oper_result varchar(20), 
    terminal varchar(5)
);

CREATE TABLE de11an.dobr_stg_passport_blacklist ( 
    date date, 
    passport varchar(11)
);   


--> 2. Создание таблиц измерений (dimensions) в SCD2 формате

CREATE TABLE de11an.dobr_dwh_dim_accounts_hist (
    account_num varchar(20),
	valid_to date,
	client varchar(10),
	effective_from timestamp(0),
	effective_to timestamp(0),
	del_flg integer    
); 

CREATE TABLE de11an.dobr_dwh_dim_cards_hist (
    card_num varchar(20),
	account_num varchar(20),
	effective_from timestamp(0),
	effective_to timestamp(0),
	del_flg integer     
);  

CREATE TABLE de11an.dobr_dwh_dim_clients_hist (
    client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(16),
	effective_from timestamp(0),
	effective_to timestamp(0),
	del_flg integer     
);

CREATE TABLE de11an.dobr_dwh_dim_terminals_hist ( 
    terminal_id varchar(5), 
    terminal_type varchar(10), 
    terminal_city varchar(30), 
    terminal_address varchar(100),
    effective_from timestamp(0),
	effective_to timestamp(0),
	del_flg integer     
 );

--> 3. Создание фактовых (fact) таблиц 

CREATE TABLE de11an.dobr_dwh_fact_transactions ( 
    trans_id varchar(11), 
    trans_date timestamp(0),
    card_num varchar(20),
    oper_type varchar(20),
    amt decimal(10, 2), 
    oper_result varchar(20), 
    terminal varchar(5)
);

CREATE TABLE de11an.dobr_dwh_fact_passport_blacklist ( 
    passport_num varchar(11),
    entry_dt date   
);   

--> 4. Создание таблиц удаленных данных

CREATE TABLE de11an.dobr_stg_del_accounts( 
	account varchar(20)
);

CREATE TABLE de11an.dobr_stg_del_cards (
    card_num varchar(20) 
);

CREATE TABLE de11an.dobr_stg_del_clients (
    client_id varchar(10)
);

CREATE TABLE de11an.dobr_stg_del_terminals ( 
    terminal_id varchar(5) 
 );

--> 5. Создание таблицы для хранения метаданных

CREATE TABLE de11an.dobr_meta_dwh (
    schema_name varchar(30),
    table_name varchar(30),
    max_UPDATE_dt timestamp(0)
);


INSERT INTO de11an.dobr_meta_dwh ( 
    schema_name, 
    table_name, 
    max_UPDATE_dt )
VALUES
    ( 'info','cards', to_timestamp('1900-01-01','YYYY-MM-DD') ),
    ( 'info','clients', to_timestamp('1899-01-01','YYYY-MM-DD') ),
    ( 'info','accounts', to_timestamp('1899-01-01','YYYY-MM-DD') ),
    ( 'xlsx','terminals', to_timestamp('1900-01-01','YYYY-MM-DD') ),
    ( 'txt','transactions', to_timestamp('1900-01-01','YYYY-MM-DD') ),
    ( 'xlsx','passport_blacklist', to_timestamp('1900-01-01','YYYY-MM-DD') )  
;

--> 6. Создание таблицы отчета 

CREATE TABLE de11an.dobr_rep_fraud (
    event_dt timestamp(0),
    passport varchar(11),
    fio varchar(50),
    phone varchar(16),
    event_type integer,
    report_dt timestamp(0)
);

