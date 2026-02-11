from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import json

# Подключение к PostgreSQL
POSTGRES_CONN = {
    'host': '172.18.0.3',
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'demo_load2',
    default_args=default_args,
    description='ETL-процесс загрузки JSON в PostgreSQL',
    schedule_interval='0 4 * * *',
    catchup=False,
    tags=['demo', 'json', 'scd2']
)

def get_postgres_connection():
    """Создание подключения к PostgreSQL"""
    return psycopg2.connect(**POSTGRES_CONN)

def extract_and_flatten(**context):
    """
    Этап 1: Извлечение новых JSON-записей
    и преобразование в плоский вид
    """
    execution_date = context['execution_date'].date()
#    execution_date = ''

    conn = get_postgres_connection()
    cursor = conn.cursor()

    # Извлечение необработанных записей
    select_query = """
        SELECT id, json_data
        FROM demo_load.orders_data
    """
    cursor.execute(select_query)
    records = cursor.fetchall()

        # Преобразование JSON в плоский вид и загрузка в staging
    for record_id, json_data in records:
        # прямой доступ к полям JSON без pandas
        flat_data = {
            'order_id': json_data['order_id'],
            'customer_id': json_data['customer_id'],
            'order_date': json_data['order_date'],
            'amount': float(json_data['amount']),
            'status': json_data['status'],
            'product_name': json_data['product_name'],
            'customer_name': json_data['customer']['name'],
            'customer_email': json_data['customer']['email'],
            'customer_city': json_data['customer']['city'],
            'processed_at': execution_date
        }

            # Вставка в промежуточную таблицу
        insert_query = """  
                INSERT INTO demo_load.staging_orders_flat (
                    order_id, customer_id, order_date, amount, status,
                    product_name, customer_name, customer_email, customer_city,    processed_at
                ) VALUES (  
                    %(order_id)s, %(customer_id)s, %(order_date)s, %(amount)s,  
                    %(status)s, %(product_name)s, %(customer_name)s,  
                    %(customer_email)s, %(customer_city)s, %(processed_at)s
                )  
            """
        
        cursor.execute(insert_query, flat_data)
    conn.commit()
    cursor.close()
    conn.close()

def load_clients_scd2(**context):
    """
    Этап 2: Обновление SCD2-таблицы клиентов
    Полное обновление с отслеживанием истории изменений
    """
    execution_date = context['execution_date'].date()
#    execution_date = date(2024, 1, 15)
    
    conn = get_postgres_connection()
    cursor = conn.cursor()

    # 1. Закрытие старых версий для измененных записей
    update_old_versions = """
            UPDATE demo_load.dim_clients_scd2
            SET valid_to = %s - INTERVAL '1 day',
                is_current = FALSE
            WHERE customer_id IN (
                SELECT DISTINCT s.customer_id
                FROM demo_load.staging_orders_flat s
                JOIN demo_load.dim_clients_scd2 d ON s.customer_id = d.customer_id
                WHERE s.processed_at::date = %s
                    AND d.is_current = TRUE
                    AND (
                        s.customer_name != d.customer_name
                        OR s.customer_email != d.customer_email
                        OR s.customer_city != d.customer_city
                    )   
            )
        """
    cursor.execute(update_old_versions, (execution_date, execution_date))

    # 2. Вставка новых версий для изменения записей
    insert_new_versions = """
        INSERT INTO demo_load.dim_clients_scd2 (
            customer_id, customer_name, customer_email,
            customer_city, valid_from, valid_to, is_current
        )
        SELECT DISTINCT
            s.customer_id,
            s.customer_name,
            s.customer_email,
            s.customer_city,
            %s,
            cast('9999-12-31' as date),
            TRUE
        FROM demo_load.staging_orders_flat s
        WHERE s.processed_at::date = %s
            AND s.customer_id IN (
                SELECT customer_id
                FROM demo_load.dim_clients_scd2
                WHERE valid_to = %s - INTERVAL '1 day'
            )
    """
    cursor.execute(insert_new_versions, (execution_date, execution_date, execution_date))

    # 3. Вставка совершенно новых клиентов
    insert_new_customers = """
            INSERT INTO demo_load.dim_clients_scd2 (
                customer_id, customer_name, customer_email,
                customer_city, valid_from, valid_to, is_current
            )
            SELECT DISTINCT
                s.customer_id,
                s.customer_name,
                s.customer_email,
                s.customer_city,
                %s,
                cast('9999-12-31' as date),
                TRUE
            FROM demo_load.staging_orders_flat s
            LEFT JOIN demo_load.dim_clients_scd2 d ON s.customer_id = d.customer_id
            WHERE s.processed_at::date = %s
                AND d.customer_id IS NULL
        """
    cursor.execute(insert_new_customers, (execution_date, execution_date))

    conn.commit()
    cursor.close()
    conn.close()

def load_analytics_mart(**context):
    """
    Этап 3: Инкрементальное обновление аналитической витрины
    Добавление агрегатов за новый день без пересчета истории
    """
    execution_date = context['execution_date'].date()
#   execution_date = date(2024, 1, 15)

    conn = get_postgres_connection()
    cursor = conn.cursor()

    # Расчет агрегатов за день на staging-рабочий
    daily_aggregates = """
            SELECT
                order_date,
                COUNT(*) as total_orders,
                SUM(amount) as total_amount,
                AVG(amount) as avg_order_amount,
                COUNT(DISTINCT customer_id) as unique_customers
            FROM demo_load.staging_orders_flat
            WHERE processed_at::date = %s
            GROUP BY order_date
            """
    cursor.execute(daily_aggregates, (execution_date,))
    result = cursor.fetchone()

    # Вставка или обновление записи за день
    insert_mart = """
            INSERT INTO demo_load.mart_daily_sales (
                report_date, total_orders, total_amount,
                avg_order_amount, unique_customers
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (report_date) DO UPDATE SET
                total_orders = EXCLUDED.total_orders,
                total_amount = EXCLUDED.total_amount,
                avg_order_amount = EXCLUDED.avg_order_amount,
                unique_customers = EXCLUDED.unique_customers,
                created_at = NOW()
            """
    cursor.execute(insert_mart, result)
    conn.commit()
    cursor.close()
    conn.close()

# Определение задач в DAG
task_extract_flatten = PythonOperator(
    task_id='extract_and_flatten',
    python_callable=extract_and_flatten,
    dag=dag
)

task_load_scd2 = PythonOperator(
    task_id='load_clients_scd2',
    python_callable=load_clients_scd2,
    dag=dag
)

task_load_mart = PythonOperator(
    task_id='load_analytics_mart',
    python_callable=load_analytics_mart,
    dag=dag
)

# Порядок выполнения задач
task_extract_flatten >> task_load_scd2 >> task_load_mart


