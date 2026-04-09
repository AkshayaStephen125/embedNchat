import os
import json
import time
import uuid
from shared import models
from logger import logger
from dotenv import load_dotenv
from db_config import get_db
from sqlalchemy.orm import Session
from kafka import KafkaConsumer, KafkaProducer


load_dotenv()

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
STORE_MESSAGE_KAFKA_TOPIC = os.environ.get("STORE_MESSAGE_KAFKA_TOPIC")


def get_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                            STORE_MESSAGE_KAFKA_TOPIC, 
                            bootstrap_servers=KAFKA_BROKER_URL,
                            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                            auto_offset_reset='earliest',
                            enable_auto_commit=True
                        )
            return consumer
        except:
            print("Waiting for Kafka...")
            time.sleep(5)



consumer = get_consumer()



def save_message():
    logger.info("Listening on store_message_topic")
    db = next(get_db())

    result=False
    message_query = dict()
    try:
        for message_session in consumer:
            message_query=message_session.value

            print('message_query ',message_query)
            session_id = message_query['session_id']
            tenant_id = message_query['tenant_id']
            tenant_brand_id = message_query['tenant_brand_id']
            sender = message_query['sender']
            event = message_query['event']
            message = message_query['message']

            result = save_user_session(db, session_id, tenant_id, tenant_brand_id)
            logger.info(f"Session Upload Status: {result}")
            if result:
                if event=='BOT_MESSAGE':
                    save_to_db(session_id, message, sender, db)
                else:
                    save_to_db(session_id, message, sender, db, 'Manual')


            
        result=True
    except Exception as e:
        logger.exception(f"Error in recieving from kafka : {e}")
    return result


def save_user_session(db, session_id, tenant_id, tenant_brand_id):
    result = False
    message = ''
    try:
        db = next(get_db())
        if user_session := db.query(models.UserSession).filter(models.UserSession.id == session_id).first():
           result = True
           message = 'Session already exists'
        else:
            user_session = models.UserSession(
                id= session_id,
                tenant_id=tenant_id,
                tenant_brand_id=tenant_brand_id
            )
            db.add(user_session)
            db.commit()
            db.refresh(user_session)
            message = 'New Session created'
        if user_session:
            result = True
    except Exception as e:
        logger.exception("Saving chat history failed")
        message = str(e)
        result = False
    logger.info(f"Session Status: {result}, Message: {message}")
    return result


def save_to_db(session_id, message, sender, db, takeover='Auto'):
    result = False,
    try:
        db = next(get_db())
        chat_history = models.ChatHistory(
            session_id=uuid.UUID(session_id),
            message=message,
            sender=sender,
            takeover=takeover
        )
        db.add(chat_history)
        db.commit()
        db.refresh(chat_history)

        if sender == "Bot":
            chat_request = models.AgentChatHistory(
            session_id=uuid.UUID(session_id))
            db.add(chat_request)
            db.commit()
            db.refresh(chat_request)

        result = True
    except Exception as e:
        db.rollback()
        logger.exception("Saving chat history failed")
        message = str(e)
    return result



if __name__ == "__main__":
    save_message()

