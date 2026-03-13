import json
import time
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

url = "https://news.ycombinator.com/"

while True:
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    titles = soup.select(".titleline > a")

    for title in titles:
        data = {
            "title": title.text,
            "link": title.get("href"),
            "timestamp": time.time()
        }

        producer.send("scraped-data", data)
        print("Sent:", data)

    producer.flush()

    time.sleep(30)
