from __future__ import annotations

import json
import pendulum
import requests
import psycopg2
from psycopg2.extras import execute_values

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def create_pets_table():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    with conn.cursor() as cursor:
        cursor.execute("""
            create table if not exists pets (
                id serial primary key,
                name varchar(255) not null,
                favFoods varchar(255)[] not null,
                birthYear smallint not null,
                photo varchar(255) not null
            );
        """)
        conn.commit()
    conn.close()
    print("Pets table created successfully")

def fetch_and_insert_pets():
    url = "https://raw.githubusercontent.com/LearnWebCode/json-example/refs/heads/master/pets-data.json"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    pets = data.get("pets", [])

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    
    try:
        with conn.cursor() as cursor:
            insert_data = []
            for pet in pets:
                fav_foods = pet.get("favFoods", [])
                if not fav_foods:
                    fav_foods = []
                
                insert_data.append((
                    pet.get("name"),
                    fav_foods,  # Python list will be converted to PostgreSQL array
                    pet.get("birthYear"),
                    pet.get("photo")
                ))
            
            insert_query = """
                INSERT INTO pets (name, favfoods, birthyear, photo)
                VALUES %s
            """
            
            execute_values(cursor, insert_query, insert_data)
        conn.commit()
        
        print(f"Successfully inserted {len(insert_data)} pets into pets table")
        
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data: {str(e)}")
        raise e
    finally:
        cursor.close()
        conn.close()


with DAG(
    dag_id="json_to_sql",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["etl", "pets"],
) as dag:
    fetch_and_insert_task = PythonOperator(
        task_id="fetch_and_insert_pets",
        python_callable=fetch_and_insert_pets,
    )

    create_pets_table_task = PythonOperator(
        task_id="create_pets_table",
        python_callable=create_pets_table,
    )

    create_pets_table_task >> fetch_and_insert_task
