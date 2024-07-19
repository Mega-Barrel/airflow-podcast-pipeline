""" Python code which contains Extract task."""

import re
import requests
import xmltodict

class PodcastScraper:
    """Podcast Scraper class which handles Data Extraction Part"""
    def __init__(self):
        self._podcast_url = 'https://www.marketplace.org/feed/podcast/'
        self._podcast_categories = 'marketplace-tech'

    def extract_data(self):
        """Method which Extracts podcast data"""
        resp = requests.get(self._podcast_url + self._podcast_categories, timeout=500)
        return (resp.status_code, resp.text)

    def clean_xml_data(self, raw_data) -> list:
        """ Method to parse/clean raw xml data"""
        feed = xmltodict.parse(raw_data)
        episodes = feed["rss"]["channel"]["item"]
        print(f"Found {len(episodes)} episodes.")
        return episodes

class PodcastTransformer:
    """Podcast Transformer class which handles Data Cleaning Part"""
    def __init__(self) -> None:
        self._episode_data = []

    def get_episode_data(self) -> list:
        """Method to return the transformed data"""
        return self._episode_data

    def transform_data(self, episodes) -> dict:
        """Method which transforms the data and returns dict object"""
        for episode in episodes:
            epi_dict = {
                "title": episode["title"],
                "page_link": episode["link"],
                "episode_type": episode["itunes:episodeType"],
                "author": episode["itunes:author"],
                "published_date": episode["pubDate"],
                "clean_description": self.clean_description(description=episode["description"]),
                "is_explicit": episode["itunes:explicit"],
                "podcast_url": episode["enclosure"]["@url"],
                "podcast_type": episode["enclosure"]["@type"],
                "podcast_length": episode["enclosure"]["@length"],
                "podcast_duration": episode["itunes:duration"],
                "podcast_id": episode["post-id"]["#text"]
            }
            self._episode_data.append(epi_dict)
            # TODO: push data to queue to be inserted.

    def clean_description(self, description) -> str:
        """Method to clean HTML contents using regex"""
        if description is not None:
            # Remove HTML tags using regex
            clean_text = re.sub(r'<.*?>', '', description)
            # Replace HTML character references with their corresponding characters
            clean_text = re.sub(r'&#(\d+);', lambda x: chr(int(x.group(1))), clean_text)
            clean_text = re.sub(r'&(#x[\da-fA-F]+);', lambda x: chr(int(x.group(1)[2:], 16)), clean_text)
            clean_text = re.sub(r'&(\w+);', '', clean_text)  # Remove named entities
            return clean_text

ps = PodcastScraper()
pt = PodcastTransformer()

status_code, xml_data = ps.extract_data()[0], ps.extract_data()[1]
xml_data = ps.clean_xml_data(raw_data=xml_data)
pt.transform_data(episodes=xml_data)
print(pt.get_episode_data())
