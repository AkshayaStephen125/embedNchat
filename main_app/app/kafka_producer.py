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
KAFKA_TOPIC_FILE_UPLOAD = os.environ.get("KAFKA_TOPIC_FILE_UPLOAD")
KAFKA_TOPIC_FILE_UPLOAD_STATUS = os.environ.get("KAFKA_TOPIC_FILE_UPLOAD_STATUS")
STORE_MESSAGE_KAFKA_TOPIC = os.environ.get("STORE_MESSAGE_KAFKA_TOPIC")
KAFKA_TOPIC_AGENT = os.environ.get("KAFKA_TOPIC_AGENT")




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



producer = get_producer()
def publish_message_to_ai_service(message_info:dict):
    result = False
    message = ''
    try:
        message = "sending to bot service"
        producer.send(KAFKA_TOPIC_REQ, value=message_info)
        producer.flush()
        result = True
    except Exception as e:
        logger.exception(f"Kafka error: {e}")
    return result, message

def publish_message_to_agent_service(message_info:dict):
    result = False
    message = ''
    try:
        message = "sending to agent service"
        producer.send(KAFKA_TOPIC_AGENT, value=message_info)
        producer.flush()
        result = True
    except Exception as e:
        logger.exception(f"Kafka error: {e}")
    return result, message

def publish_message_to_db(message_info:dict):
    result = False
    message = ''
    try:
        print("send to kafka")
        producer.send(STORE_MESSAGE_KAFKA_TOPIC, value=message_info)
        producer.flush()
        result = True
        message = "Message stored successfully"
    except Exception as e:
        logger.exception(f"Kafka error: {e}")
    return result, message

def publish_file_to_kafka(file_info:dict):
    result = False
    try:
        print("send to kafka")
        producer.send(KAFKA_TOPIC_FILE_UPLOAD, value=file_info)
        producer.flush()
        result = True
    except Exception as e:
        logger.exception(f"Kafka error: {e}")
    return result


def consume_kafka_messages(loop):

    consumer = get_consumer(KAFKA_TOPIC_RES)

    for message_session in consumer:

        message_info = message_session.value

        logger.info(f"Received/////////////////////////////////: {message_info}")

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


