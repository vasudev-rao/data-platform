import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
import json
import time
from datetime import datetime, UTC

URL = "https://news.ycombinator.com"
TOPIC = "hn_news"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5
)

def scrape():
    try:
        response = requests.get(URL, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        posts = soup.select(".titleline a")

        for post in posts:
            data = {
                "title": post.text.strip(),
                "url": post.get("href"),
                "scraped_at": datetime.now(UTC).isoformat()
            }

            future = producer.send(TOPIC, data)
            future.get(timeout=10)  # wait for Kafka acknowledgement

            print("Sent:", data)

        producer.flush()

    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":
    while True:
        scrape()
        time.sleep(30)
