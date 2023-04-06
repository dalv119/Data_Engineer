-- ! Исполнение данного скрипта осуществляется запуском bash скрипта
--   из папки где он находится командой :

-- ./taxi.sh


-- Подготовка таблиц ------------------------------------------

--> Создание промежуточных (staging) таблиц хранилища

CREATE TABLE dwh_oryol.stg_drivers (
	driver_license char(12),
	first_name varchar(20),
	last_name varchar(20),
	middle_name varchar(20),
	driver_valid_to date,
	card_num char(19),
	update_dt timestamp(0),
	birth_dt date
);

CREATE TABLE dwh_oryol.stg_rides (
	ride_id integer,
	dt timestamp(0),
	client_phone char(18),
	card_num char(19),
	point_from varchar(200),
	point_to varchar(200),
	distance numeric(5, 2),
	price numeric(7, 2)
);

CREATE TABLE dwh_oryol.stg_car_pool (
	plate_num char(9),
	model varchar(30),
	revision_dt date,
	register_dt timestamp(0),
	finished_flg char(1),
	update_dt timestamp(0)
);

CREATE TABLE dwh_oryol.stg_movement (
	movement_id integer,
	car_plate_num char(9),
	ride integer,
	event varchar(6),
	dt timestamp(0)
);

CREATE TABLE dwh_oryol.stg_payments (
    transaction_dt timestamp(0),
    card_num char(19),
    transaction_amt numeric(7, 2)
);

CREATE TABLE dwh_oryol.stg_waybills (  
    waybill_num varchar(10),
    driver_license char(12),
    car_plate_num varchar(10),
    work_start_dt timestamp(0),
    work_end_dt timestamp(0),
    issue_dt timestamp(0)
);

--> Создание промежуточных (staging) фактовых таблиц хранилища 

CREATE TABLE dwh_oryol.stg_fact_rides (
    ride_id integer,
    point_from_txt varchar(200),
    point_to_txt varchar(200),
    distance_val numeric(5, 2),
    price_amt numeric(7, 2),
    client_phone_num varchar(20),
    car_plate_num varchar(10),
    ride_arrival_dt timestamp(0),
    ride_start_dt timestamp(0),
    ride_end_dt timestamp(0)
);

CREATE TABLE dwh_oryol.stg_fact_waybills (
    waybill_num varchar(10),
    driver_pers_num integer,
    car_plate_num varchar(10),
    work_start_dt timestamp(0),
    work_end_dt timestamp(0),
    issue_dt timestamp(0)
);

--> Создание таблиц измерений (dimentions) SCD2

CREATE TABLE dwh_oryol.dim_drivers_hist (
    personnel_num serial,
    start_dt timestamp(0),
    last_name varchar(20),
    first_name varchar(20),
    middle_name varchar(20),
    birth_dt date,
    card_num varchar(20),
    driver_license_num varchar(20),
    driver_license_dt date,
    deleted_flag char(1),
    end_dt timestamp(0),
    processed_dt timestamp(0)
);

CREATE TABLE dwh_oryol.dim_cars_hist (
    plate_num varchar(10),
    start_dt timestamp(0),
    model_name varchar(30),
    revision_dt timestamp(0),
    deleted_flag char(1),
    end_dt timestamp(0),
    processed_dt timestamp(0)
);

CREATE TABLE dwh_oryol.dim_clients_hist (
    phone_num varchar(20),
    start_dt timestamp(0),
    card_num varchar(20),
    deleted_flag char(1),
    end_dt timestamp(0),
    processed_dt timestamp(0)
);

--> Создание фактовых таблиц 

CREATE TABLE dwh_oryol.fact_rides (
    ride_id integer,
    point_from_txt varchar(200),
    point_to_txt varchar(200),
    distance_val numeric(5, 2),
    price_amt numeric(7, 2),
    client_phone_num varchar(20),
    driver_pers_num integer,
    car_plate_num varchar(10),
    ride_arrival_dt timestamp(0),
    ride_start_dt timestamp(0),
    ride_end_dt timestamp(0),
    processed_dt timestamp(0)
);

CREATE TABLE dwh_oryol.fact_waybills (
    waybill_num varchar(10),
    driver_pers_num integer,
    car_plate_num varchar(10),
    work_start_dt timestamp(0),
    work_end_dt timestamp(0),
    issue_dt timestamp(0),
    processed_dt timestamp(0)
);

CREATE TABLE dwh_oryol.fact_payments (
    transaction_id serial,
    card_num varchar(20),
    transaction_amt numeric(7, 2),
    transaction_dt timestamp(0),
    processed_dt timestamp(0)
);

--> Создание таблиц удаленных данных

CREATE TABLE dwh_oryol.stg_drivers_del( 
	driver_license char(12)
);

CREATE TABLE dwh_oryol.stg_clients_del (
    phone_num varchar(20)
);

CREATE TABLE dwh_oryol.stg_cars_del (
	plate_num char(9)
 );


--> Создание таблицы для хранения метаданных

CREATE TABLE dwh_oryol.meta (
    schema_name varchar(30),
    table_name varchar(30),
    max_UPDATE_dt timestamp(0)
);


INSERT INTO dwh_oryol.meta ( 
    schema_name, 
    table_name, 
    max_UPDATE_dt )
VALUES
    ( 'dwh_oryol','fact_rides', to_timestamp('1900-01-01','YYYY-MM-DD') ),
    ( 'dwh_oryol','dim_drivers_hist', to_timestamp('1900-01-01','YYYY-MM-DD') ),
    ( 'dwh_oryol','stg_car_pool', to_timestamp('1900-01-01','YYYY-MM-DD') ),
    ( 'dwh_oryol','stg_movement', to_timestamp('1900-01-01','YYYY-MM-DD') ),
    ( 'dwh_oryol','fact_payments', to_timestamp('1900-01-01','YYYY-MM-DD') ),
    ( 'dwh_oryol','fact_waybills', to_timestamp('1900-01-01','YYYY-MM-DD') ),
    ( 'dwh_oryol','rep_clients_hist', to_timestamp('1900-01-01','YYYY-MM-DD') )  
;


--> Создание таблиц ветрины отчетов:

--> 1. Выплата водителям
    -- услуги сервиса - 20% от стоимости поездки,
    -- стоимость топливо - 47,26 * 7 * дистанция / 100,
    -- амортизация машины 5 руб / км * дистанция,
    -- остаток достается водителю.

CREATE TABLE dwh_oryol.rep_drivers_payments (
    personnel_num integer,
    last_name varchar(20),
    first_name varchar(20),
    middle_name varchar(20),
    card_num varchar(20),
    amount numeric(11, 2),
    report_dt date
);

--> 2. Водители-нарушители
    -- завершил поездку со средней скоростью более 85 км/ч.

CREATE TABLE dwh_oryol.rep_drivers_violations (
    personnel_num integer,
    ride varchar(10),
    speed integer,
    violations_cnt integer,
    report_dt date
);

--> 3. Перерабатывающие водители
    -- водитель работал больше 7 часов за 24 часа (по путевым листам).
    -- нарушением считается тот путевой лист, который вышел за пределы 8
    -- часов. Таких путевых листов может быть много.
    -- дата нарушения - по дате начала работы в путевом листе.

CREATE TABLE dwh_oryol.rep_drivers_overtime (
    personnel_num integer,
    start_work_dt timestamp(0),
    working_time integer,
    report_dt date
);

--> 4. “Знай своего клиента”

CREATE TABLE dwh_oryol.stg_rep_clients_hist (
    phone_num varchar(20),
    rides_cnt integer,
    cancelled_cnt integer,
    spent_amt numeric(9, 2),
    debt_amt numeric(9, 2),
    update_dt timestamp(0)
);

CREATE TABLE dwh_oryol.stg_rep_clients_hist_del( 
	phone_num varchar(20)
);

CREATE TABLE dwh_oryol.rep_clients_hist (
    client_id serial,
    phone_num varchar(20),
    rides_cnt integer,
    cancelled_cnt integer,
    spent_amt numeric(9, 2),
    debt_amt numeric(9, 2),
    start_dt timestamp(0),
    end_dt timestamp(0),
    deleted_flag char(1)
);

-->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
