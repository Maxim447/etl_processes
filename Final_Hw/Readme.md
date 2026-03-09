# Запуск проекта
1. Поднять контейнеры

    `docker-compose up -d`


2. Открыть UI
    
    `URL:      http://localhost:8080`

    `Login:    admin`

    `Password: admin`


3. Запустить даги в таком порядке: 

    3.1. generate_mongo_data

    3.2. transform_to_postgres

    3.3. create_analytics_marts


4. Посмотреть результаты

`docker exec -it etl_postgres psql -U airflow -d etl_db`

`select count(*) from user_sessions;`

`select count(*) from support_tickets;`

`select * from mart_user_activity limit 10;`

`select * from mart_support_efficiency limit 10;`
`
