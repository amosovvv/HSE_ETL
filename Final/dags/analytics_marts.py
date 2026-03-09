from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_mart_tables():
    """Создаёт таблицы витрин, если они не существуют."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_target')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    create_user_activity_mart = """
    CREATE TABLE IF NOT EXISTS user_activity_mart (
        user_id TEXT PRIMARY KEY,
        total_sessions INTEGER,
        total_time_spent INTEGER,
        avg_session_duration FLOAT,
        most_visited_pages TEXT,
        distinct_actions INTEGER,
        last_activity_date TIMESTAMP
    );
    """

    create_support_ticket_mart = """
    CREATE TABLE IF NOT EXISTS support_ticket_mart (
        ticket_id TEXT PRIMARY KEY,
        user_id TEXT,
        status TEXT,
        issue_type TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        resolution_time_seconds INTEGER,
        message_count INTEGER
    );
    """

    cursor.execute(create_user_activity_mart)
    cursor.execute(create_support_ticket_mart)
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Mart tables created or already exist.")

def refresh_user_activity_mart():
    pg_hook = PostgresHook(postgres_conn_id='postgres_target')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE user_activity_mart;")

    insert_sql = """
    INSERT INTO user_activity_mart (user_id, total_sessions, total_time_spent, avg_session_duration, most_visited_pages, distinct_actions, last_activity_date)
    SELECT
        user_id,
        COUNT(*) AS total_sessions,
        SUM(EXTRACT(EPOCH FROM (end_time::timestamp - start_time::timestamp))) AS total_time_spent,
        AVG(EXTRACT(EPOCH FROM (end_time::timestamp - start_time::timestamp))) AS avg_session_duration,
        (
            SELECT page
            FROM (
                SELECT unnest(string_to_array(us2.pages_visited, ',')) AS page
                FROM user_sessions us2
                WHERE us2.user_id = us.user_id
            ) p
            GROUP BY page
            ORDER BY COUNT(*) DESC
            LIMIT 1
        ) AS most_visited_pages,
        (
            SELECT COUNT(DISTINCT action)
            FROM (
                SELECT unnest(string_to_array(us2.actions, ',')) AS action
                FROM user_sessions us2
                WHERE us2.user_id = us.user_id
            ) a
        ) AS distinct_actions,
        MAX(start_time::timestamp) AS last_activity_date
    FROM user_sessions us
    GROUP BY user_id;
    """
    cursor.execute(insert_sql)
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("User activity mart refreshed.")
def refresh_support_ticket_mart():
    """Обновляет витрину поддержки."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_target')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE support_ticket_mart;")

    insert_sql = """
    INSERT INTO support_ticket_mart (ticket_id, user_id, status, issue_type, created_at, updated_at, resolution_time_seconds, message_count)
    SELECT
        ticket_id,
        user_id,
        status,
        issue_type,
        created_at::timestamp,
        updated_at::timestamp,
        CASE
            WHEN status IN ('resolved', 'closed') THEN EXTRACT(EPOCH FROM (updated_at::timestamp - created_at::timestamp))
            ELSE NULL
        END AS resolution_time_seconds,
        message_count
    FROM support_tickets;
    """
    cursor.execute(insert_sql)
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Support ticket mart refreshed.")

with DAG(
    dag_id='analytics_marts',
    default_args=default_args,
    description='Build analytics marts from replicated data',
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['analytics'],
) as dag:

    create_tables = PythonOperator(
        task_id='create_mart_tables',
        python_callable=create_mart_tables,
    )

    refresh_user_mart = PythonOperator(
        task_id='refresh_user_activity_mart',
        python_callable=refresh_user_activity_mart,
    )

    refresh_support_mart = PythonOperator(
        task_id='refresh_support_ticket_mart',
        python_callable=refresh_support_ticket_mart,
    )

    create_tables >> [refresh_user_mart, refresh_support_mart]
    