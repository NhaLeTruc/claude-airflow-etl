from datetime import datetime, timedelta
from airflow import DAG
import requests
import json
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer

def fetch_weather_data():
    try:
        api_key = ""  # Reemplaza con tu API key
        lat=40.4165
        lon=-3.70256
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"

        response = requests.get(url)
        data = response.json()
        return data
    
    except Exception as e:
        print(f"Error fetching weather data: {e}")
        return


def print_json(**kwargs):
    ti = kwargs['ti']
    weather_data = ti.xcom_pull(task_ids='fetch_weather_task') # Obtener datos de la tarea anterior
    if weather_data:
        print(json.dumps(weather_data, indent=4))
    else:
        print("No weather data available.")

def json_serializer(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_weather_task')  # Obtener datos de la tarea anterior
    if data:
        producer_config = {
            'bootstrap.servers': 'kafka:29092',  # Cambia esto si tu broker Kafka está en otra dirección
            'client.id': 'airflow'
        }
        producer = Producer(producer_config)
        json_message = json.dumps(data)
        producer.produce('topic_prueba', value=json_message)
        producer.flush()
    else:
        print("No data to serialize.")  


dag = DAG(
    "DAG_API_CLIMA",
    schedule=timedelta(seconds=5),
    start_date=datetime(2025, 10, 10),

)

task1 = PythonOperator(
    task_id='fetch_weather_task',
    python_callable=fetch_weather_data,
    dag=dag,

    
    )   

task2 = PythonOperator(
    task_id='print_log',
    python_callable=print_json,
    dag=dag,

    
    ) 

task3 = PythonOperator(
    task_id='send_task_kafka-spark',
    python_callable=json_serializer,
    dag=dag,

    
    ) 


task1 >> task2 >> task3