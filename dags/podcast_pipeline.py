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
            return xmltodict.parse(response.text)
        else:
            return f'Error occured while fetching data. More info on the error {response.status_code} - {response}'

    @task
    def transformd_data(response):
        items = response["rss"]["channel"]["item"]
        for item in items:
            epi_dict = {
                "title": item["title"],
                "page_link": item["link"],
                "episode_type": item["itunes:episodeType"],
                "author": item["itunes:author"],
                "published_date": item["pubDate"],
                "clean_description": item["description"],
                "is_explicit": item["itunes:explicit"],
                "podcast_url": item["enclosure"]["@url"],
                "podcast_type": item["enclosure"]["@type"],
                "podcast_length": item["enclosure"]["@length"],
                "podcast_duration": item["itunes:duration"],
                "podcast_id": item["post-id"]["#text"]
            }
            print(epi_dict)

    raw_data = extract_data()
    transformd_data(response=raw_data)

podcast_summary()
