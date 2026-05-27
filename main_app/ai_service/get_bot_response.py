import os
import json
import rag
import time
from logger import logger
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
import random


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

            logger.info(f'Received from topic KAFKA_TOPIC_REQ :{message_query}')
            message_response = dict()
            message_response['type'] = "message"
            message_response['session_id'] = message_query['session_id']
            message_response['tenant_id'] = message_query['tenant_id']
            message_response['tenant_brand_id'] = message_query['tenant_brand_id']
            message_response['brand_tone'] = message_query['brand_tone']
            message_response["sender"]="Bot"
            message_response["message"]=""

            if 'human' in message_query.get("message").lower() or 'agent' in message_query.get("message").lower():
                message_response["event"]="AGENT_MESSAGE"
                message_response["message"]="The agent will contact you shortly. Please keep the patience."
                producer.send(KAFKA_TOPIC_AGENT, value=message_query)
            else:
                token_used = 0
                message_response["event"]="BOT_MESSAGE"
                relevant_chunks = rag.retrieve_relevant_chunks(int(message_query['tenant_id']), int(message_query['tenant_brand_id']), message_query.get("message"))
                if relevant_chunks:
                    answer, token_used = rag.generate_answer(message_query.get('session_id'), message_query.get("message"), message_query['brand_tone'], relevant_chunks)
                else:
                    answer = rag.no_context_answers[random.randint(0,len(rag.no_context_answers)-1)]
                if answer:
                    message_response["message"]=answer
                    message_response["token_used"]=token_used
                    message_response['timestamp'] = datetime.now().isoformat()
                producer.send(STORE_MESSAGE_KAFKA_TOPIC, value=message_response)
                logger.info(f'Sending to topic STORE_MESSAGE_KAFKA_TOPIC')

            producer.send(KAFKA_TOPIC_RES, value=message_response)
            logger.info(f'Sending to topic KAFKA_TOPIC_RES')


        result=True
    except Exception as e:
        logger.exception(f"Error in recieving from kafka : {e}")
    return result


get_from_kafka()

