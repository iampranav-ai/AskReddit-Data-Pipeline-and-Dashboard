from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta, timezone
import requests
import time
import csv
import os
from unidecode import unidecode

default_args = {
    'owner': 'pranavVerma',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'reddit_data_pipeline_continuous',
    default_args=default_args,
    description='A DAG to continuously fetch Reddit data, save to CSV, and store in PostgreSQL',
    schedule_interval=timedelta(seconds=5),
    catchup=False,
    tags=['reddit', 'api', 'postgres', 'csv'],
)

def fetch_reddit_data(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    return response.json()

def extract_post_data(post):
    title = unidecode(post['data']['title'])
    return {
        'subreddit': post['data']['subreddit'],
        'title': title,
        'created_utc': post['data']['created_utc'],
        'word_count': len(title.split()),
        'char_count': len(title),
        'id': post['data']['id']
    }

def convert_utc_to_ist(utc_time):
    utc_dt = datetime.fromtimestamp(utc_time, timezone.utc)
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    ist_dt = utc_dt.astimezone(ist_tz)
    return ist_dt.strftime('%Y-%m-%d %H:%M:%S IST')

def write_to_csv(data, filename):
    file_exists = os.path.isfile(filename)
    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['S.No.', 'Subreddit', 'Title', 'Words', 'Chars', 'Created At']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(data)

def fetch_and_process_reddit_data(**kwargs):
    url = "https://www.reddit.com/r/AskReddit/new.json"
    subreddit = url.split('/r/')[1].split('/')[0]
    csv_filename = f"/opt/airflow/personalData/{subreddit}_new.csv"

    postgres_hook = PostgresHook(postgres_conn_id='airflow_connection')
    
    # Fetch existing post IDs from the database
    existing_ids_query = "SELECT id FROM reddit_api_askReddit"
    existing_ids = set(row[0] for row in postgres_hook.get_records(existing_ids_query))

    data = fetch_reddit_data(url)
    posts = data['data']['children']
    sorted_posts = sorted(posts, key=lambda x: x['data']['created_utc'])

    processed_posts = []

    for post in sorted_posts:
        post_data = extract_post_data(post)
        post_id = post_data['id']

        if post_id not in existing_ids:
            created_at = convert_utc_to_ist(post_data['created_utc'])
            
            csv_data = {
                'S.No.': len(existing_ids) + len(processed_posts) + 1,
                'Subreddit': post_data['subreddit'],
                'Title': post_data['title'],
                'Words': post_data['word_count'],
                'Chars': post_data['char_count'],
                'Created At': created_at
            }
            write_to_csv(csv_data, csv_filename)
            
            processed_posts.append({
                'subreddit': post_data['subreddit'],
                'title': post_data['title'],
                'word_count': post_data['word_count'],
                'char_count': post_data['char_count'],
                'created_at': created_at,
                'id': post_id
            })
            
            existing_ids.add(post_id)

    return processed_posts

def insert_to_postgres(**kwargs):
    ti = kwargs['ti']
    processed_posts = ti.xcom_pull(task_ids='fetch_and_process_reddit_data')
    
    postgres_hook = PostgresHook(postgres_conn_id='airflow_connection')
    
    insert_query = """
    INSERT INTO reddit_api_askReddit (subreddit, title, word_count, char_count, created_at, id)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    for post in processed_posts:
        postgres_hook.run(insert_query, parameters=(
            post['subreddit'],
            post['title'],
            post['word_count'],
            post['char_count'],
            post['created_at'],
            post['id']
        ))

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='airflow_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS reddit_api_askReddit (
        id TEXT PRIMARY KEY,
        subreddit TEXT,
        title TEXT,
        word_count INTEGER,
        char_count INTEGER,
        created_at TIMESTAMP WITH TIME ZONE
    );
    """,
    dag=dag,
)

fetch_and_process_task = PythonOperator(
    task_id='fetch_and_process_reddit_data',
    python_callable=fetch_and_process_reddit_data,
    dag=dag,
)

insert_to_postgres_task = PythonOperator(
    task_id='insert_to_postgres',
    python_callable=insert_to_postgres,
    dag=dag,
)

create_table_task >> fetch_and_process_task >> insert_to_postgres_task