# **Итоговое задание 3 модуля курса ETL-процессы**

## **Студент:** Амосов Вячеслав

## **Курс:** ETL-процессы
---
## Проект реализует полный ETL-цикл:
- **Генерация тестовых данных** в MongoDB (коллекции: `user_sessions`, `event_logs`, `support_tickets`, `user_recommendations`, `moderation_queue`).
- **Репликация данных** из MongoDB в PostgreSQL с помощью Apache Airflow (DAG `nosql_to_sql_replication`).  
  На этапе репликации применяются трансформации: приведение типов, разворачивание вложенных структур, очистка.
- **Построение двух аналитических витрин** в PostgreSQL на основе реплицированных данных (DAG `analytics_marts`):
  - **Витрина активности пользователей** (`user_activity_mart`) – агрегированные показатели по каждому пользователю.
  - **Витрина эффективности работы поддержки** (`support_ticket_mart`) – информация по обращениям с расчётом времени решения.

## Архитектура ETL-процесса

- **Источник**: MongoDB (контейнер `mongodb`).
- **Оркестратор**: Apache Airflow (контейнеры `airflow-webserver`, `airflow-scheduler`).
- **Целевое хранилище**: PostgreSQL (контейнер `postgres`) – единая база для метаданных Airflow, сырых данных и витрин.
- **Генерация данных**: Jupyter Notebook для заполнения MongoDB.

## Структура репозитория

```plaintext
.
├── docker-compose.yml               # Конфигурация всех сервисов
├── postgres-init/
│   └── init-dbs.sql                 # Скрипт инициализации баз данных PostgreSQL
├── dags/
│   ├── nosql_to_sql_replication.py  # DAG репликации
│   └── analytics_marts.py           # DAG построения витрин
├── notebooks/
│   └── generate.ipynb               # Jupyter ноутбук для генерации данных
├── scripts/                         # Вспомогательные скрипты (опционально)
├── logs/                            # Логи Airflow (монтируются)
└── README.md                        # Документация

