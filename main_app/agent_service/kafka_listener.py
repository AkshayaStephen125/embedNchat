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
KAFKA_TOPIC_AGENT = os.environ.get("KAFKA_TOPIC_AGENT")

def get_consumer(kafka_topic):
    while True:
        try:
            consumer = KafkaConsumer(
                            kafka_topic, 
                            bootstrap_servers=KAFKA_BROKER_URL,
                            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                            auto_offset_reset='earliest',
                            enable_auto_commit=True
                        )
            return consumer
        except:
            print("Waiting for Kafka...")
            time.sleep(5)

def consume_kafka_messages(loop):

    consumer = get_consumer(KAFKA_TOPIC_AGENT)

    for message_session in consumer:

        message_info = message_session.value

        print("Received:", message_info)

        session_id = message_info.get("session_id")

        if session_id:
            asyncio.run_coroutine_threadsafe(
                manager.broadcast(message_info),
                loop
            )

async def response_listener():

    loop = asyncio.get_running_loop()

    loop.run_in_executor(
        None,
        consume_kafka_messages,
        loop   
    )