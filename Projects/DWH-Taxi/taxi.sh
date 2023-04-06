#!/bin/bash

# -> Даем права на исполнение файла скрипта DDL
chmod ugo+x ./taxi.ddl

# Запуск SQL скрипта taxi.dll создания таблиц DWH в базе данных 

PGPASSWORD=dwh_oryol_TQGyR5kq psql -U dwh_oryol -d dwh -p 5432 -h de-edu-db.chronosavant.ru -f ./taxi.ddl

# -> Даем права на исполнение файла скрипта PY
# chmod ugo+rx ./taxi.py

# Запуск скрипта taxi.py загрузки и обновления данных DWH (SCD-2)
# echo 'Please wait ...'
# ./taxi.py 