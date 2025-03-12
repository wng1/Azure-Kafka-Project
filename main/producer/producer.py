from confluent_kafka import Producer
"""Utilisation of confluent_kafka on the wrapped around C library, much faster and more efficient than kafka-python"""
import json
import requests
import time

#Kafka fixed variables 
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "orders"

#Kafka configuration
producer_config = {
  "bootstrap.servers": KAFKA_BROKER, 
  "acks": "all",
  "retries": 3
}

producer = Producer(producer_config)


