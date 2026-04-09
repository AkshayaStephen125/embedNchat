import os
import json
import asyncio
from logger import logger
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer
from connection_manager import manager
import time


load_dotenv()


KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
KAFKA_TOPIC_REQ = os.environ.get("KAFKA_TOPIC_REQ")
KAFKA_TOPIC_RES = os.environ.get("KAFKA_TOPIC_RES")
KAFKA_TOPIC_AGENT = os.environ.get("KAFKA_TOPIC_AGENT")
STORE_MESSAGE_KAFKA_TOPIC = os.environ.get("STORE_MESSAGE_KAFKA_TOPIC")




def get_producer():
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8')
                         )
            return producer
        except:
            print("Waiting for Kafka...")
            time.sleep(5)


producer = get_producer()

def publish_message_to_kafka(message:dict):
    result = False
    try:
        logger.info("----------sending to fastapi----------------")
        producer.send(KAFKA_TOPIC_RES, value=message)
        producer.flush()
        result = True
    except Exception as e:
        logger.exception(f"Kafka error: {e}")
    return result

def publish_message_to_db(message:dict):
    result = False
    try:
        logger.info("----------sending to db_service----------------")
        producer.send(STORE_MESSAGE_KAFKA_TOPIC, value=message)
        producer.flush()
        result = True
    except Exception as e:
        logger.exception(f"Kafka error: {e}")
    return result



