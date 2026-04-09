import os
import json
import rag
import time
from logger import logger
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer


load_dotenv()

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
KAFKA_TOPIC_FILE_UPLOAD = os.environ.get("KAFKA_TOPIC_FILE_UPLOAD")
KAFKA_TOPIC_FILE_UPLOAD_STATUS = os.environ.get("KAFKA_TOPIC_FILE_UPLOAD_STATUS")


def get_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                            KAFKA_TOPIC_FILE_UPLOAD, 
                            bootstrap_servers=KAFKA_BROKER_URL,
                            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                            auto_offset_reset='earliest',
                            enable_auto_commit=True
                        )
            return consumer
        except:
            print("Waiting for Kafka...")
            time.sleep(5)

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


consumer = get_consumer()
producer = get_producer()

def get_from_kafka():
    logger.info("Listening on file_upload_topic")
    result=False
    file_context = dict()
    try:
        for file_info in consumer:
            file_context=file_info.value
            logger.info(f"file_context : {file_context}")
            result, message = rag.file_to_vector(**file_context)
            producer.send(KAFKA_TOPIC_FILE_UPLOAD_STATUS, value=result)
            logger.info(f"Status : {result} - {message}")
    except Exception as e:
        logger.exception(f"Error in recieving from kafka : {e}")
    return result


get_from_kafka()

