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

def retrieve_data():

    try:

    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None 

def delivery_report(err, msg):
    if err:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_messages():
  message_data = retrieve_data()

  if message_data:
    for message in message_data.get("results", []):
      message_json = json.dumps(message)
      print(f"{str(message["message"]{message_json}")
      producer.produce(TOPIC_NAME, key=str______), value=message_json, callback=delivery_report)
      time.sleep(0.6)
      producer.flush()

if __name__ == "__main__":
    produce_messages()



    
