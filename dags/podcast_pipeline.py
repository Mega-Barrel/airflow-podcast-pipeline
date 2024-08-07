""" Airflow Podcast ETL DAG"""
# system import
# import os
import re
# import json

import requests
import xmltodict

# airflow import
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
# from airflow.operators.empty import EmptyOperator

# DAG defination
default_dag = {
    'owner': 'saurabh.jo',
    'depends_on_past': False,
    'retries': 2
}

@dag(
    'podcast_processing_pipeline',
    default_args=default_dag,
    description="A Simple Podcast Processing Pipeline.",
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['podcast'],
)
def podcast_summary():
    """ Podcast Dag which performs ETL tasks"""
    @task
    def extract_data():
        """Extract podcast data from the URL."""
        podcast_url = 'https://www.marketplace.org/feed/podcast/'
        podcast_category = 'marketplace-tech'
        response = requests.get(podcast_url + podcast_category, timeout=500)

        if response.status_code == 200:
            print(response.text)
            # return xmltodict.parse(response.text)
        return f'Error occured while fetching data. More info on the error {response.status_code} - {response.text}'

    # def transformd_data(response):
    #     items = response["rss"]["channel"]["item"]
    #     for item in items:
    #         description = item['description']
    #         if description is not None:
    #             # Remove HTML tags using regex
    #             clean_text = re.sub(r'<.*?>', '', description)
    #             # Replace HTML character references with their corresponding characters
    #             clean_text = re.sub(r'&#(\d+);', lambda x: chr(int(x.group(1))), clean_text)
    #             clean_text = re.sub(r'&(#x[\da-fA-F]+);', lambda x: chr(int(x.group(1)[2:], 16)), clean_text)
    #             clean_text = re.sub(r'&(\w+);', '', clean_text)
    #             print(clean_text)

    raw_data = extract_data()
    # transformd_data(raw_data)

podcast_summary()
