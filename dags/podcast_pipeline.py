""" Airflow Podcast ETL DAG"""
# system import
# import os
# import re
# import json

import requests
# import xmltodict

# airflow import
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

# DAG defination
default_dag = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2
}

with DAG(
    'podcast_processing_pipeline',
    default_args=default_dag,
    description="A Simple Podcast Processing Pipeline.",
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['podcast']
) as dag:

    @task
    def extract_data():
        """Extract podcast data from the URL."""
        podcast_url = 'https://www.marketplace.org/feed/podcast/'
        podcast_category = 'marketplace-tech'
        resp = requests.get(podcast_url + podcast_category, timeout=500)
        return resp.status_code, resp

extract_data()
