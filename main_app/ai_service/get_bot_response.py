import os
import json
import rag
import time
from logger import logger
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer


load_dotenv()

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
KAFKA_TOPIC_REQ = os.environ.get("KAFKA_TOPIC_REQ")
KAFKA_TOPIC_RES = os.environ.get("KAFKA_TOPIC_RES")
KAFKA_TOPIC_AGENT = os.environ.get("KAFKA_TOPIC_AGENT")
STORE_MESSAGE_KAFKA_TOPIC = os.environ.get("STORE_MESSAGE_KAFKA_TOPIC")



def get_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                            KAFKA_TOPIC_REQ, 
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
    logger.info("Listening on bot_message_topic")

    result=False
    message_query = dict()
    try:
        for message_session in consumer:
            message_query=message_session.value

            print('message_query ',message_query)
            message_response = dict()
            message_response['session_id'] = message_query['session_id']
            message_response['tenant_id'] = message_query['tenant_id']
            message_response['tenant_brand_id'] = message_query['tenant_brand_id']
            message_response['brand_tone'] = message_query['brand_tone']
            message_response["sender"]="Bot"
            message_response["event"]="BOT_MESSAGE"
            message_response["message"]=""

            if 'human' in message_query.get("message").lower() or 'agent' in message_query.get("message").lower():
                message_response["message"]="The agent will contact you shortly. Please keep the patience."
                producer.send(KAFKA_TOPIC_AGENT, value=message_query)
            else:
                relevant_chunks = rag.retrieve_relevant_chunks(int(message_query['tenant_id']), int(message_query['tenant_brand_id']), message_query.get("message"))
                print('relevant_chunks      ',relevant_chunks)
                answer = rag.generate_answer(message_query.get("message"), message_query['brand_tone'], relevant_chunks)
                if answer:
                    message_response["message"]=answer
                producer.send(STORE_MESSAGE_KAFKA_TOPIC, value=message_response)
            print('message_response', message_response)
            producer.send(KAFKA_TOPIC_RES, value=message_response)

        result=True
    except Exception as e:
        logger.exception(f"Error in recieving from kafka : {e}")
    return result


get_from_kafka()

