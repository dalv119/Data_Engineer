## Техническое задание на разработку хранилища данных. Этап 1.
### Введение.
Сегодня ваш первый день работы инженером по данным в компании такси
“Везу и точка”. К вам приходит начальник и говорит тоном, не терпящим возражений:
“Что это мы живём без нормальных отчётов? Мне нужно, чтобы ты закончил работу
по их созданию”. После чего подмигивает и уходит. От коллеги справа, который
работает с операционной системой, в которой ведётся деятельность компании, вы
узнаёте, что ваш предшественник начал делать эту задачу, но внезапно решил
уволиться.
Открыв папку с документацией, где предшественник всё хранил, вы находите
документ, который описывает источник, актуальность которого подтверждает всё тот
же коллега справа, а также описание целевой структуры хранилища. Чтобы не терять
времени вы решаете использовать её.
Ваша первая задача - настроить ETL процесс, который будет забирать данные
из источника и раскладывать их по целевым таблицам в описанную в документации
структуру в хранилище.
### Описание источников данных.
#### 1. Источник данных - СУБД PostgreSQL
Реквизиты подключения к источнику техническим пользователем:
●
host: de-edu-db.chronosavant.ru
●
port: 5432
●
database: taxi
●
schema: main
●
user: etl_tech_user
●
password: etl_tech_user_password