from __future__ import annotations

import xml.etree.ElementTree as ET
import pendulum
import requests
import psycopg2
from psycopg2.extras import execute_values

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def create_nutrition_table():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    with conn.cursor() as cursor:
        cursor.execute("""
            create table if not exists nutrilon (
                id serial primary key,
                total_fat numeric not null,
                total_fat_units varchar(10) not null default 'g',
                saturated_fat numeric not null,
                saturated_fat_units varchar(10) not null default 'g',
                cholesterol numeric not null,
                cholesterol_units varchar(10) not null default 'mg',
                sodium numeric not null,
                sodium_units varchar(10) not null default 'mg',
                carb numeric not null,
                carb_units varchar(10) not null default 'g',
                fiber numeric not null,
                fiber_units varchar(10) not null default 'g',
                protein numeric not null,
                protein_units varchar(10) not null default 'g'
            );

            create table if not exists food (
                id serial primary key,
                name varchar(255) not null,
                mfr varchar(255),
                serving numeric,
                serving_units varchar(10) default 'g',
                calories_total numeric,
                calories_fat numeric,
                total_fat numeric,
                saturated_fat numeric,
                cholesterol numeric,
                sodium numeric,
                carb numeric,
                fiber numeric,
                protein numeric,
                vitamin_a numeric,
                vitamin_c numeric,
                mineral_ca numeric,
                mineral_fe numeric
            );
        """)
        conn.commit()
    conn.close()
    print("nutrition table created successfully")

def fetch_and_insert_nutrition_data():
    url = "https://gist.githubusercontent.com/pamelafox/3000322/raw/6cc03bccf04ede0e16564926956675794efe5191/nutrition.xml"
    response = requests.get(url)
    response.raise_for_status()
    xml_content = response.content
    
    root = ET.fromstring(xml_content)
    
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    
    try:
        with conn.cursor() as cursor:
            nutrilon_elem = root.find('daily-values')
            if nutrilon_elem is not None:
                nutrilon_data = (
                    float(nutrilon_elem.find('total-fat').text) if nutrilon_elem.find('total-fat') is not None else None,
                    nutrilon_elem.find('total-fat').get('units', 'g') if nutrilon_elem.find('total-fat') is not None else 'g',
                    float(nutrilon_elem.find('saturated-fat').text) if nutrilon_elem.find('saturated-fat') is not None else None,
                    nutrilon_elem.find('saturated-fat').get('units', 'g') if nutrilon_elem.find('saturated-fat') is not None else 'g',
                    float(nutrilon_elem.find('cholesterol').text) if nutrilon_elem.find('cholesterol') is not None else None,
                    nutrilon_elem.find('cholesterol').get('units', 'mg') if nutrilon_elem.find('cholesterol') is not None else 'mg',
                    float(nutrilon_elem.find('sodium').text) if nutrilon_elem.find('sodium') is not None else None,
                    nutrilon_elem.find('sodium').get('units', 'mg') if nutrilon_elem.find('sodium') is not None else 'mg',
                    float(nutrilon_elem.find('carb').text) if nutrilon_elem.find('carb') is not None else None,
                    nutrilon_elem.find('carb').get('units', 'g') if nutrilon_elem.find('carb') is not None else 'g',
                    float(nutrilon_elem.find('fiber').text) if nutrilon_elem.find('fiber') is not None else None,
                    nutrilon_elem.find('fiber').get('units', 'g') if nutrilon_elem.find('fiber') is not None else 'g',
                    float(nutrilon_elem.find('protein').text) if nutrilon_elem.find('protein') is not None else None,
                    nutrilon_elem.find('protein').get('units', 'g') if nutrilon_elem.find('protein') is not None else 'g',
                )
                
                nutrilon_query = """
                    INSERT INTO nutrilon (
                        total_fat, total_fat_units,
                        saturated_fat, saturated_fat_units,
                        cholesterol, cholesterol_units,
                        sodium, sodium_units,
                        carb, carb_units,
                        fiber, fiber_units,
                        protein, protein_units
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(nutrilon_query, nutrilon_data)
                print("Successfully inserted nutrilon")
            
            food_items = root.findall('food')
            food_data = []
            
            for food_elem in food_items:
                vitamins_elem = food_elem.find('vitamins')
                vitamin_a = float(vitamins_elem.find('a').text) if vitamins_elem is not None and vitamins_elem.find('a') is not None else None
                vitamin_c = float(vitamins_elem.find('c').text) if vitamins_elem is not None and vitamins_elem.find('c') is not None else None
                
                minerals_elem = food_elem.find('minerals')
                mineral_ca = float(minerals_elem.find('ca').text) if minerals_elem is not None and minerals_elem.find('ca') is not None else None
                mineral_fe = float(minerals_elem.find('fe').text) if minerals_elem is not None and minerals_elem.find('fe') is not None else None
                
                calories_elem = food_elem.find('calories')
                calories_total = float(calories_elem.get('total')) if calories_elem is not None and calories_elem.get('total') is not None else None
                calories_fat = float(calories_elem.get('fat')) if calories_elem is not None and calories_elem.get('fat') is not None else None
                
                food_data.append((
                    food_elem.find('name').text if food_elem.find('name') is not None else None,
                    food_elem.find('mfr').text if food_elem.find('mfr') is not None else None,
                    float(food_elem.find('serving').text) if food_elem.find('serving') is not None else None,
                    food_elem.find('serving').get('units', 'g') if food_elem.find('serving') is not None else 'g',
                    calories_total,
                    calories_fat,
                    float(food_elem.find('total-fat').text) if food_elem.find('total-fat') is not None else None,
                    float(food_elem.find('saturated-fat').text) if food_elem.find('saturated-fat') is not None else None,
                    float(food_elem.find('cholesterol').text) if food_elem.find('cholesterol') is not None else None,
                    float(food_elem.find('sodium').text) if food_elem.find('sodium') is not None else None,
                    float(food_elem.find('carb').text) if food_elem.find('carb') is not None else None,
                    float(food_elem.find('fiber').text) if food_elem.find('fiber') is not None else None,
                    float(food_elem.find('protein').text) if food_elem.find('protein') is not None else None,
                    vitamin_a,
                    vitamin_c,
                    mineral_ca,
                    mineral_fe,
                ))
            
            if food_data:
                food_query = """
                    INSERT INTO food (
                        name, mfr, serving, serving_units,
                        calories_total, calories_fat,
                        total_fat, saturated_fat, cholesterol, sodium,
                        carb, fiber, protein,
                        vitamin_a, vitamin_c, mineral_ca, mineral_fe
                    ) VALUES %s
                """
                execute_values(cursor, food_query, food_data)
                print(f"Successfully inserted {len(food_data)} food items")
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data: {str(e)}")
        raise e
    finally:
        conn.close()


with DAG(
    dag_id="xml_to_sql",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["etl", "nutrition"],
) as dag:
    fetch_and_insert_task = PythonOperator(
        task_id="fetch_and_insert_nutrition_data",
        python_callable=fetch_and_insert_nutrition_data,
    )

    create_nutrition_table_task = PythonOperator(
        task_id="create_nutrition_table",
        python_callable=create_nutrition_table,
    )

    create_nutrition_table_task >> fetch_and_insert_task
