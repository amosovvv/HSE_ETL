from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pymongo
import json
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

def get_mongo_client():
    conn = BaseHook.get_connection('mongo_source')
    uri = f"mongodb://{conn.host}:{conn.port}/"
    client = pymongo.MongoClient(uri)
    return client

def extract_collection(collection_name):
    client = get_mongo_client()
    db = client['etl_database']
    collection = db[collection_name]
    docs = list(collection.find({}))
    client.close()
    return docs

def transform_user_session(doc):
    device = doc.get('device', {})
    return {
        'session_id': doc['session_id'],
        'user_id': doc['user_id'],
        'start_time': doc['start_time'],
        'end_time': doc['end_time'],
        'device_type': device.get('type'),
        'device_browser': device.get('browser'),
        'device_os': device.get('os'),
        'pages_visited': ','.join(doc.get('pages_visited', [])),
        'actions': ','.join(doc.get('actions', []))
    }

def transform_event_log(doc):
    return {
        'event_id': doc['event_id'],
        'timestamp': doc['timestamp'],
        'event_type': doc['event_type'],
        'details': json.dumps(doc.get('details', {}))
    }

def transform_support_ticket(doc):
    messages = doc.get('messages', [])
    return {
        'ticket_id': doc['ticket_id'],
        'user_id': doc['user_id'],
        'status': doc['status'],
        'issue_type': doc['issue_type'],
        'messages': json.dumps(messages),
        'message_count': len(messages),
        'created_at': doc['created_at'],
        'updated_at': doc['updated_at']
    }

def transform_user_recommendation(doc):
    return {
        'user_id': doc['user_id'],
        'recommended_products': ','.join(doc.get('recommended_products', [])),
        'last_updated': doc['last_updated']
    }

def transform_moderation_queue(doc):
    return {
        'review_id': doc['review_id'],
        'user_id': doc['user_id'],
        'product_id': doc['product_id'],
        'review_text': doc['review_text'],
        'rating': doc['rating'],
        'moderation_status': doc['moderation_status'],
        'flags': ','.join(doc.get('flags', [])),
        'submitted_at': doc['submitted_at']
    }

def process_collection(collection_name, transform_func, table_name, conflict_column, **kwargs):
    logging.info(f"Starting replication for {collection_name}")
    docs = extract_collection(collection_name)
    logging.info(f"Extracted {len(docs)} documents from {collection_name}")

    if not docs:
        logging.warning(f"No documents found in {collection_name}")
        return

    transformed = [transform_func(doc) for doc in docs]

    pg_hook = PostgresHook(postgres_conn_id='postgres_target')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    columns = list(transformed[0].keys())
    col_names = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    update_set = ', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col != conflict_column])

    insert_sql = f"""
        INSERT INTO {table_name} ({col_names}) 
        VALUES ({placeholders})
        ON CONFLICT ({conflict_column}) DO UPDATE SET {update_set}
    """

    rows = [tuple(doc[col] for col in columns) for doc in transformed]

    try:
        cursor.executemany(insert_sql, rows)
        conn.commit()
        logging.info(f"Inserted/updated {len(rows)} rows into {table_name}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error loading into {table_name}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def create_tables():
    pg_hook = PostgresHook(postgres_conn_id='postgres_target')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    create_queries = [
        """
        CREATE TABLE IF NOT EXISTS user_sessions (
            session_id TEXT PRIMARY KEY,
            user_id TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            device_type TEXT,
            device_browser TEXT,
            device_os TEXT,
            pages_visited TEXT,
            actions TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS event_logs (
            event_id TEXT PRIMARY KEY,
            timestamp TIMESTAMP,
            event_type TEXT,
            details JSONB
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS support_tickets (
            ticket_id TEXT PRIMARY KEY,
            user_id TEXT,
            status TEXT,
            issue_type TEXT,
            messages JSONB,
            message_count INTEGER,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS user_recommendations (
            user_id TEXT PRIMARY KEY,
            recommended_products TEXT,
            last_updated TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS moderation_queue (
            review_id TEXT PRIMARY KEY,
            user_id TEXT,
            product_id TEXT,
            review_text TEXT,
            rating INTEGER,
            moderation_status TEXT,
            flags TEXT,
            submitted_at TIMESTAMP
        );
        """
    ]

    for query in create_queries:
        cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Tables created or already exist.")

with DAG(
    dag_id='nosql_to_sql_replication',
    default_args=default_args,
    description='Replicate data from MongoDB to PostgreSQL',
    schedule_interval='@daily',
    catchup=False,
    tags=['replication'],
) as dag:

    create_table_task = PythonOperator(
        task_id='create_tables_if_not_exist',
        python_callable=create_tables,
    )

    t1 = PythonOperator(
        task_id='replicate_user_sessions',
        python_callable=process_collection,
        op_kwargs={
            'collection_name': 'user_sessions',
            'transform_func': transform_user_session,
            'table_name': 'user_sessions',
            'conflict_column': 'session_id'
        }
    )

    t2 = PythonOperator(
        task_id='replicate_event_logs',
        python_callable=process_collection,
        op_kwargs={
            'collection_name': 'event_logs',
            'transform_func': transform_event_log,
            'table_name': 'event_logs',
            'conflict_column': 'event_id'
        }
    )

    t3 = PythonOperator(
        task_id='replicate_support_tickets',
        python_callable=process_collection,
        op_kwargs={
            'collection_name': 'support_tickets',
            'transform_func': transform_support_ticket,
            'table_name': 'support_tickets',
            'conflict_column': 'ticket_id'
        }
    )

    t4 = PythonOperator(
        task_id='replicate_user_recommendations',
        python_callable=process_collection,
        op_kwargs={
            'collection_name': 'user_recommendations',
            'transform_func': transform_user_recommendation,
            'table_name': 'user_recommendations',
            'conflict_column': 'user_id'
        }
    )

    t5 = PythonOperator(
        task_id='replicate_moderation_queue',
        python_callable=process_collection,
        op_kwargs={
            'collection_name': 'moderation_queue',
            'transform_func': transform_moderation_queue,
            'table_name': 'moderation_queue',
            'conflict_column': 'review_id'
        }
    )

    create_table_task >> [t1, t2, t3, t4, t5]
    